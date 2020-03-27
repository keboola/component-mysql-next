"""Component main class for data extraction.

Executes component endpoint executions based on dependent api_mappings.json file in same path. This file should have
a set structure, see example below.

Essentially at the table table and column level, add "replication-method" of: FULL_TABLE, INCREMENTAL, or ROW_BASED.
If INCREMENTAL, you need to specify a "replication-key".

# Primary To-do items
TODO: Integrate SSH tunnel compatibility
TODO: Fix log-based because it is not working
TODO: Confirm successfully works in container including imports
TODO: Update output file names to something consistent
TODO: Write manifest files, including logic for if incremental or not.

# Secondary To-do items
TODO: Response for table mappings
TODO: Integrate SSL, if all else works and there is a need
TODO: Add testing framework
"""
import copy
import itertools
import json
import logging
import os
import paramiko
import pendulum
import pymysql
import sys

from collections import namedtuple
from contextlib import nullcontext
from io import StringIO
from sshtunnel import SSHTunnelForwarder

from kbc.env_handler import KBCEnvHandler

try:
    import core as core
    import core.metrics as metrics
    import mysql.result as result_writer

    from core import metadata
    from core.schema import Schema
    from core.catalog import Catalog, CatalogEntry

    import mysql.replication.binlog as binlog
    import mysql.replication.common as common
    import mysql.replication.full_table as full_table
    import mysql.replication.incremental as incremental
except ImportError:
    import src.core as core
    import src.core.metrics as metrics
    import src.mysql.result as result_writer

    from src.core import metadata
    from src.core.schema import Schema
    from src.core.catalog import Catalog, CatalogEntry

    import src.mysql.replication.binlog as binlog
    import src.mysql.replication.common as common
    import src.mysql.replication.full_table as full_table
    import src.mysql.replication.incremental as incremental

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

try:
    from mysql.client import connect_with_backoff, MySQLConnection
except ImportError as import_err:
    LOGGER.warning('Running component locally, some features may not reflect their behavior in Keboola as we are not'
                   'running inside of a Docker container. Err: {}'.format(import_err))
    from src.mysql.client import connect_with_backoff, MySQLConnection

current_path = os.path.dirname(__file__)
module_path = os.path.dirname(current_path)

# Define mandatory parameter constants, matching Config Schema.
KEY_OBJECTS_ONLY = 'fetchObjectsOnly'
KEY_TABLE_MAPPINGS_JSON = 'tableMappingsJson'
KEY_MYSQL_HOST = 'host'
KEY_MYSQL_PORT = 'port'
KEY_MYSQL_USER = 'username'
KEY_MYSQL_PWD = '#password'
KEY_USE_SSH_TUNNEL = 'sshTunnel'

# Define optional parameters as constants for later use.
KEY_SSH_HOST = 'sshHost'
KEY_SSH_PORT = 'sshPort'
KEY_SSH_PUBLIC_KEY = 'sshPublicKey'
KEY_SSH_PRIVATE_KEY = '#sshPrivateKey'
KEY_SSH_USERNAME = 'sshUser'

MAPPINGS_FILE = 'table_mappings.json'
SSH_BIND_PORT = 8080
LOCAL_ADDRESS = '127.0.0.1'
CONNECT_TIMEOUT = 30

# Keep for debugging
KEY_STDLOG = 'stdlogging'
KEY_DEBUG = 'debug'
MANDATORY_PARS = (KEY_OBJECTS_ONLY, KEY_MYSQL_HOST, KEY_MYSQL_PORT, KEY_MYSQL_USER, KEY_MYSQL_PWD, KEY_USE_SSH_TUNNEL)
MANDATORY_IMAGE_PARS = ()

APP_VERSION = '0.1.0'

pymysql.converters.conversions[pendulum.Pendulum] = pymysql.converters.escape_datetime

# Bin database sub-types by type.
STRING_TYPES = {'char', 'enum', 'longtext', 'mediumtext', 'text', 'varchar'}
FLOAT_TYPES = {'double', 'float'}
DATETIME_TYPES = {'date', 'datetime', 'time', 'timestamp'}
BYTES_FOR_INTEGER_TYPE = {
    'tinyint': 1,
    'smallint': 2,
    'mediumint': 3,
    'int': 4,
    'bigint': 8
}

Column = namedtuple('Column', [
    "table_schema",
    "table_name",
    "column_name",
    "data_type",
    "character_maximum_length",
    "numeric_precision",
    "numeric_scale",
    "column_type",
    "column_key"
])


def schema_for_column(c):
    """Returns the Schema object for the given Column."""
    data_type = c.data_type.lower()
    column_type = c.column_type.lower()

    inclusion = 'available'
    # We want to automatically include all primary key columns
    if c.column_key.lower() == 'pri':
        inclusion = 'automatic'

    result = Schema(inclusion=inclusion)

    if data_type == 'bit' or column_type.startswith('tinyint(1)'):
        result.type = ['null', 'boolean']

    elif data_type in BYTES_FOR_INTEGER_TYPE:
        result.type = ['null', 'integer']
        bits = BYTES_FOR_INTEGER_TYPE[data_type] * 8
        if 'unsigned' in c.column_type:
            result.minimum = 0
            result.maximum = 2 ** bits - 1
        else:
            result.minimum = 0 - 2 ** (bits - 1)
            result.maximum = 2 ** (bits - 1) - 1

    elif data_type in FLOAT_TYPES:
        result.type = ['null', 'number']

    elif data_type == 'json':
        result.type = ['null', 'string']

    elif data_type == 'decimal':
        result.type = ['null', 'number']
        result.multipleOf = 10 ** (0 - c.numeric_scale)
        return result

    elif data_type in STRING_TYPES:
        result.type = ['null', 'string']
        result.maxLength = c.character_maximum_length

    elif data_type in DATETIME_TYPES:
        result.type = ['null', 'string']
        result.format = 'date-time'

    else:
        result = Schema(None, inclusion='unsupported', description='Unsupported column type {}'.format(column_type))
    return result


def create_column_metadata(cols):
    mdata = {}
    mdata = metadata.write(mdata, (), 'selected-by-default', False)
    for c in cols:
        schema = schema_for_column(c)
        mdata = metadata.write(mdata,
                               ('properties', c.column_name),
                               'selected-by-default',
                               schema.inclusion != 'unsupported')
        mdata = metadata.write(mdata,
                               ('properties', c.column_name),
                               'sql-datatype',
                               c.column_type.lower())

    return metadata.to_list(mdata)


def discover_catalog(mysql_conn, config):
    """Returns a Catalog describing the structure of the database."""
    filter_dbs_config = config.get('filter_dbs')

    if filter_dbs_config:
        filter_dbs_clause = ",".join(["'{}'".format(db) for db in filter_dbs_config.split(",")])
        table_schema_clause = "WHERE table_schema IN ({})".format(filter_dbs_clause)
    else:
        table_schema_clause = """
        WHERE table_schema NOT IN (
        'information_schema',
        'performance_schema',
        'mysql'
        )"""

    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute("""
            SELECT table_schema,
                   table_name,
                   table_type,
                   table_rows
                FROM information_schema.tables
                {}
            """.format(table_schema_clause))

            table_info = {}

            for (db, table, table_type, rows) in cur.fetchall():
                if db not in table_info:
                    table_info[db] = {}

                table_info[db][table] = {
                    'row_count': rows,
                    'is_view': table_type == 'VIEW'
                }

            cur.execute("""
                SELECT table_schema,
                       table_name,
                       column_name,
                       data_type,
                       character_maximum_length,
                       numeric_precision,
                       numeric_scale,
                       column_type,
                       column_key
                    FROM information_schema.columns
                    {}
                    ORDER BY table_schema, table_name
            """.format(table_schema_clause))

            columns = []
            rec = cur.fetchone()
            while rec is not None:
                columns.append(Column(*rec))
                rec = cur.fetchone()

            entries = []
            for (k, cols) in itertools.groupby(columns, lambda c: (c.table_schema, c.table_name)):
                cols = list(cols)
                (table_schema, table_name) = k
                schema = Schema(type='object', properties={c.column_name: schema_for_column(c) for c in cols})
                md = create_column_metadata(cols)
                md_map = metadata.to_map(md)

                md_map = metadata.write(md_map, (), 'database-name', table_schema)

                is_view = table_info[table_schema][table_name]['is_view']

                if table_schema in table_info and table_name in table_info[table_schema]:
                    row_count = table_info[table_schema][table_name].get('row_count')

                    if row_count is not None:
                        md_map = metadata.write(md_map, (), 'row-count', row_count)

                    md_map = metadata.write(md_map, (), 'is-view', is_view)

                column_is_key_prop = lambda c, s: (  # noqa: E731
                    c.column_key == 'PRI' and s.properties[c.column_name].inclusion != 'unsupported'
                )

                key_properties = [c.column_name for c in cols if column_is_key_prop(c, schema)]

                if not is_view:
                    md_map = metadata.write(md_map, (), 'table-key-properties', key_properties)

                entry = CatalogEntry(table=table_name, stream=table_name, metadata=metadata.to_list(md_map),
                                     tap_stream_id=common.generate_tap_stream_id(table_schema, table_name),
                                     schema=schema)

                entries.append(entry)

    return Catalog(entries)


def do_discover(mysql_conn, config):
    return discover_catalog(mysql_conn, config).dumps()


# TODO: Maybe put in a singer-db-utils library.
def desired_columns(selected, table_schema):
    """Return the set of column names we need to include in the SELECT.
    selected - set of column names marked as selected in the input catalog
    table_schema - the most recently discovered Schema for the table
    """
    all_columns = set()
    available = set()
    automatic = set()
    unsupported = set()

    for column, column_schema in table_schema.properties.items():
        all_columns.add(column)
        inclusion = column_schema.inclusion
        if inclusion == 'automatic':
            automatic.add(column)
        elif inclusion == 'available':
            available.add(column)
        elif inclusion == 'unsupported':
            unsupported.add(column)
        else:
            raise Exception('Unknown inclusion ' + inclusion)

    selected_but_unsupported = selected.intersection(unsupported)
    if selected_but_unsupported:
        LOGGER.warning(
            'Columns %s were selected but are not supported. Skipping them.',
            selected_but_unsupported)

    selected_but_nonexistent = selected.difference(all_columns)
    if selected_but_nonexistent:
        LOGGER.warning(
            'Columns %s were selected but do not exist.',
            selected_but_nonexistent)

    not_selected_but_automatic = automatic.difference(selected)
    if not_selected_but_automatic:
        LOGGER.warning(
            'Columns %s are primary keys but were not selected. Adding them.',
            not_selected_but_automatic)

    return selected.intersection(available).union(automatic)


def log_engine(mysql_conn, catalog_entry):
    is_view = common.get_is_view(catalog_entry)
    database_name = common.get_database_name(catalog_entry)

    if is_view:
        LOGGER.info("Beginning sync for view %s.%s", database_name, catalog_entry.table)
    else:
        with connect_with_backoff(mysql_conn) as open_conn:
            with open_conn.cursor() as cur:
                cur.execute("""
                    SELECT engine
                      FROM information_schema.tables
                     WHERE table_schema = %s
                       AND table_name   = %s
                """, (database_name, catalog_entry.table))

                row = cur.fetchone()

                if row:
                    LOGGER.info("Beginning sync for %s table %s.%s",
                                row[0],
                                database_name,
                                catalog_entry.table)


def is_valid_currently_syncing_stream(selected_stream, state):
    stream_metadata = metadata.to_map(selected_stream.metadata)
    replication_method = stream_metadata.get((), {}).get('replication-method')

    if replication_method != 'LOG_BASED':
        return True

    if replication_method == 'LOG_BASED' and binlog_stream_requires_historical(selected_stream, state):
        return True

    return False


def binlog_stream_requires_historical(catalog_entry, state):
    log_file = core.get_bookmark(state, catalog_entry.tap_stream_id, 'log_file')
    log_pos = core.get_bookmark(state, catalog_entry.tap_stream_id, 'log_pos')
    max_pk_values = core.get_bookmark(state, catalog_entry.tap_stream_id, 'max_pk_values')
    last_pk_fetched = core.get_bookmark(state, catalog_entry.tap_stream_id, 'last_pk_fetched')

    if (log_file and log_pos) and (not max_pk_values and not last_pk_fetched):
        return False

    return True


def resolve_catalog(discovered_catalog, streams_to_sync):
    result = Catalog(streams=[])

    # Iterate over the streams in the input catalog and match each one up
    # with the same stream in the discovered catalog.
    for catalog_entry in streams_to_sync:
        catalog_metadata = metadata.to_map(catalog_entry.metadata)
        replication_key = catalog_metadata.get((), {}).get('replication-key')

        discovered_table = discovered_catalog.get_stream(catalog_entry.tap_stream_id)
        database_name = common.get_database_name(catalog_entry)

        if not discovered_table:
            LOGGER.warning('Database %s table %s was selected but does not exist',
                           database_name, catalog_entry.table)
            continue

        selected = {k for k, v in catalog_entry.schema.properties.items()
                    if common.property_is_selected(catalog_entry, k) or k == replication_key}

        # These are the columns we need to select
        columns = desired_columns(selected, discovered_table.schema)

        result.streams.append(CatalogEntry(
            tap_stream_id=catalog_entry.tap_stream_id,
            metadata=catalog_entry.metadata,
            stream=catalog_entry.stream,
            table=catalog_entry.table,
            schema=Schema(
                type='object',
                properties={col: discovered_table.schema.properties[col]
                            for col in columns}
            )
        ))

    return result


def get_non_binlog_streams(mysql_conn, catalog, config, state):
    """Returns the Catalog of data we're going to sync for all SELECT-based
    streams (i.e. INCREMENTAL, FULL_TABLE, and LOG_BASED that require a historical
    sync). LOG_BASED streams that require a historical sync are inferred from lack
    of any state.
    Using the Catalog provided from the input file, this function will return a
    Catalog representing exactly which tables and columns that will be emitted
    by SELECT-based syncs. This is achieved by comparing the input Catalog to a
    freshly discovered Catalog to determine the resulting Catalog.
    The resulting Catalog will include the following any streams marked as
    "selected" that currently exist in the database. Columns marked as "selected"
    and those labled "automatic" (e.g. primary keys and replication keys) will be
    included. Streams will be prioritized in the following order:
      1. currently_syncing if it is SELECT-based
      2. any streams that do not have state
      3. any streams that do not have a replication method of LOG_BASED
    """
    discovered = discover_catalog(mysql_conn, config)

    # Filter catalog to include only selected streams
    selected_streams = list(filter(lambda s: common.stream_is_selected(s), catalog.streams))
    streams_with_state = []
    streams_without_state = []

    for stream in selected_streams:
        stream_metadata = metadata.to_map(stream.metadata)
        replication_method = stream_metadata.get((), {}).get('replication-method')
        stream_state = state.get('bookmarks', {}).get(stream.tap_stream_id)

        if not stream_state:
            if replication_method == 'LOG_BASED':
                LOGGER.info("LOG_BASED stream %s requires full historical sync", stream.tap_stream_id)

            streams_without_state.append(stream)
        elif stream_state and replication_method == 'LOG_BASED' and binlog_stream_requires_historical(stream, state):
            is_view = common.get_is_view(stream)

            if is_view:
                raise Exception("Unable to replicate stream({}) with binlog because it's a view.".format(stream.stream))

            LOGGER.info("LOG_BASED stream %s will resume its historical sync", stream.tap_stream_id)

            streams_with_state.append(stream)
        elif stream_state and replication_method != 'LOG_BASED':
            streams_with_state.append(stream)

    # If the state says we were in the middle of processing a stream, skip
    # to that stream. Then process streams without prior state and finally
    # move onto streams with state (i.e. have been synced in the past)
    currently_syncing = core.get_currently_syncing(state)

    # prioritize streams that have not been processed
    ordered_streams = streams_without_state + streams_with_state

    if currently_syncing:
        currently_syncing_stream = list(filter(
            lambda s: s.tap_stream_id == currently_syncing and is_valid_currently_syncing_stream(s, state),
            streams_with_state))

        non_currently_syncing_streams = list(filter(lambda s: s.tap_stream_id != currently_syncing, ordered_streams))

        streams_to_sync = currently_syncing_stream + non_currently_syncing_streams
    else:
        # prioritize streams that have not been processed
        streams_to_sync = ordered_streams

    return resolve_catalog(discovered, streams_to_sync)


def get_binlog_streams(mysql_conn, catalog, config, state):
    discovered = discover_catalog(mysql_conn, config)

    selected_streams = list(filter(lambda s: common.stream_is_selected(s), catalog.streams))
    binlog_streams = []

    for stream in selected_streams:
        stream_metadata = metadata.to_map(stream.metadata)
        replication_method = stream_metadata.get((), {}).get('replication-method')
        stream_state = state.get('bookmarks', {}).get(stream.tap_stream_id)
        LOGGER.debug(stream_state)

        if replication_method == 'LOG_BASED' and not binlog_stream_requires_historical(stream, state):
            binlog_streams.append(stream)

    return resolve_catalog(discovered, binlog_streams)


def write_schema_message(catalog_entry, bookmark_properties=[]):
    key_properties = common.get_key_properties(catalog_entry)

    core.write_message(core.SchemaMessage(
        stream=catalog_entry.stream,
        schema=catalog_entry.schema.to_dict(),
        key_properties=key_properties,
        bookmark_properties=bookmark_properties
    ))


def do_sync_incremental(mysql_conn, catalog_entry, state, columns, optional_limit=None):
    LOGGER.info("Stream %s is using incremental replication", catalog_entry.stream)

    md_map = metadata.to_map(catalog_entry.metadata)
    replication_key = md_map.get((), {}).get('replication-key')

    if not replication_key:
        raise Exception("Cannot use INCREMENTAL replication for table ({}) without a replication key.".format(
            catalog_entry.stream))

    write_schema_message(catalog_entry=catalog_entry,
                         bookmark_properties=[replication_key])

    if optional_limit:
        LOGGER.info("Incremental Stream %s is using an optional limit clause of %d", catalog_entry.stream,
                    int(optional_limit))
        incremental.sync_table(mysql_conn, catalog_entry, state, columns, int(optional_limit))
    else:
        incremental.sync_table(mysql_conn, catalog_entry, state, columns)

    core.write_message(core.StateMessage(value=copy.deepcopy(state)))


def do_sync_historical_binlog(mysql_conn, config, catalog_entry, state, columns):
    binlog.verify_binlog_config(mysql_conn)

    is_view = common.get_is_view(catalog_entry)
    key_properties = common.get_key_properties(catalog_entry)

    if is_view:
        raise Exception("Unable to replicate stream({}) with binlog because it is a view.".format(catalog_entry.stream))

    log_file = core.get_bookmark(state, catalog_entry.tap_stream_id, 'log_file')

    log_pos = core.get_bookmark(state, catalog_entry.tap_stream_id, 'log_pos')

    max_pk_values = core.get_bookmark(state, catalog_entry.tap_stream_id, 'max_pk_values')

    last_pk_fetched = core.get_bookmark(state, catalog_entry.tap_stream_id, 'last_pk_fetched')

    write_schema_message(catalog_entry)

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)

    if log_file and log_pos and max_pk_values:
        LOGGER.info("Resuming initial full table sync for LOG_BASED stream %s", catalog_entry.tap_stream_id)
        full_table.sync_table(mysql_conn, catalog_entry, state, columns, stream_version)

    else:
        LOGGER.info("Performing initial full table sync for LOG_BASED stream %s", catalog_entry.tap_stream_id)

        state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'initial_binlog_complete', False)

        current_log_file, current_log_pos = binlog.fetch_current_log_file_and_pos(mysql_conn)
        state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'version', stream_version)

        if full_table.sync_is_resumable(mysql_conn, catalog_entry):
            # We must save log_file and log_pos across FULL_TABLE syncs when performing
            # a resumable full table sync
            state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'log_file', current_log_file)
            state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'log_pos', current_log_pos)

            full_table.sync_table(mysql_conn, catalog_entry, state, columns, stream_version)
        else:
            full_table.sync_table(mysql_conn, catalog_entry, state, columns, stream_version)
            state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'log_file', current_log_file)
            state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'log_pos', current_log_pos)


def do_sync_full_table(mysql_conn, config, catalog_entry, state, columns):
    LOGGER.info("Stream %s is using full table replication", catalog_entry.stream)
    key_properties = common.get_key_properties(catalog_entry)

    write_schema_message(catalog_entry)

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)

    full_table.sync_table(mysql_conn, catalog_entry, state, columns, stream_version)

    # Prefer initial_full_table_complete going forward
    core.clear_bookmark(state, catalog_entry.tap_stream_id, 'version')

    state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'initial_full_table_complete', True)

    core.write_message(core.StateMessage(value=copy.deepcopy(state)))


def sync_non_binlog_streams(mysql_conn, non_binlog_catalog, config, state):
    for catalog_entry in non_binlog_catalog.streams:
        columns = list(catalog_entry.schema.properties.keys())

        if not columns:
            LOGGER.warning('There are no columns selected for stream %s, skipping it.', catalog_entry.stream)
            continue

        state = core.set_currently_syncing(state, catalog_entry.tap_stream_id)

        # Emit a state message to indicate that we've started this stream
        core.write_message(core.StateMessage(value=copy.deepcopy(state)))

        md_map = metadata.to_map(catalog_entry.metadata)

        replication_method = md_map.get((), {}).get('replication-method')

        database_name = common.get_database_name(catalog_entry)

        with metrics.job_timer('sync_table') as timer:
            timer.tags['database'] = database_name
            timer.tags['table'] = catalog_entry.table

            log_engine(mysql_conn, catalog_entry)

            if replication_method == 'INCREMENTAL':
                optional_limit = config.get('incremental_limit')
                do_sync_incremental(mysql_conn, catalog_entry, state, columns, optional_limit)
            elif replication_method == 'LOG_BASED':
                do_sync_historical_binlog(mysql_conn, config, catalog_entry, state, columns)
            elif replication_method == 'FULL_TABLE':
                do_sync_full_table(mysql_conn, config, catalog_entry, state, columns)
            else:
                raise Exception("only INCREMENTAL, LOG_BASED, and FULL TABLE replication methods are supported")

    state = core.set_currently_syncing(state, None)
    core.write_message(core.StateMessage(value=copy.deepcopy(state)))


def sync_binlog_streams(mysql_conn, binlog_catalog, config, state):
    if binlog_catalog.streams:
        for stream in binlog_catalog.streams:
            write_schema_message(stream)

        with metrics.job_timer('sync_binlog') as timer:
            binlog.sync_binlog_stream(mysql_conn, config, binlog_catalog.streams, state)


def do_sync(mysql_conn, config, catalog, state):
    non_binlog_catalog = get_non_binlog_streams(mysql_conn, catalog, config, state)
    binlog_catalog = get_binlog_streams(mysql_conn, catalog, config, state)

    sync_non_binlog_streams(mysql_conn, non_binlog_catalog, config, state)
    sync_binlog_streams(mysql_conn, binlog_catalog, config, state)


def log_server_params(mysql_conn):
    with connect_with_backoff(mysql_conn) as open_conn:
        try:
            with open_conn.cursor() as cur:
                cur.execute('''
                SELECT VERSION() as version,
                       @@session.wait_timeout as wait_timeout,
                       @@session.innodb_lock_wait_timeout as innodb_lock_wait_timeout,
                       @@session.max_allowed_packet as max_allowed_packet,
                       @@session.interactive_timeout as interactive_timeout''')
                row = cur.fetchone()
                LOGGER.info('Server Parameters: ' +
                            'version: %s, ' +
                            'wait_timeout: %s, ' +
                            'innodb_lock_wait_timeout: %s, ' +
                            'max_allowed_packet: %s, ' +
                            'interactive_timeout: %s',
                            *row)
            with open_conn.cursor() as cur:
                cur.execute('''
                show session status where Variable_name IN ('Ssl_version', 'Ssl_cipher')''')
                rows = cur.fetchall()
                mapped_row = dict(rows)
                LOGGER.info('Server SSL Parameters (blank means SSL is not active): ' +
                            '[ssl_version: %s], ' +
                            '[ssl_cipher: %s]',
                            mapped_row['Ssl_version'],
                            mapped_row['Ssl_cipher'])

        except pymysql.err.InternalError as e:
            LOGGER.warning("Encountered error checking server params. Error: (%s) %s", *e.args)


class Component(KBCEnvHandler):
    """Keboola extractor component."""
    def __init__(self, debug: bool = False, data_path: str = None):
        KBCEnvHandler.__init__(self, MANDATORY_PARS, data_path=data_path,
                               log_level=logging.DEBUG if debug else logging.INFO)
        self.files_out_path = os.path.join(data_path, 'out', 'files')
        self.files_in_path = os.path.join(data_path, 'in', 'files')
        # override debug from config
        if self.cfg_params.get(KEY_DEBUG):
            debug = True
        if debug:
            logging.getLogger().setLevel(logging.DEBUG)
        LOGGER.info('Running version %s', APP_VERSION)
        LOGGER.info('Loading configuration...')

        try:
            self.validate_config()
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as err:
            LOGGER.exception(err)
            exit(1)

        # TODO: Update to more clear environment variable; used must set local time to UTC.
        os.environ['TZ'] = 'UTC'

    def _check_file_inputs(self) -> str:
        """Return path name of file inputs if any."""
        file_input = self.files_in_path
        has_file_inputs = any(os.path.isfile(os.path.join(file_input, file)) for file in os.listdir(file_input))

        if has_file_inputs:
            return file_input

    def write_table_mappings_file(self, table_mapping: Catalog, file_name: str = 'table_mappings.json'):
        """Write table mappings to output file destination."""
        write_destination = os.path.join(self.files_out_path, file_name)
        with open(write_destination, 'w') as mapping_file:
            mapping_file.write(table_mapping.dumps())

    @staticmethod
    def write_result(result: list, output_file: str = 'results.json'):
        """Write table mappings to output file destination."""
        with open(output_file, 'w') as mapping_file:
            mapping_file.write(json.dumps(result))

    def run(self):
        """Execute main component extraction process."""
        params = self.cfg_params
        file_input_path = self._check_file_inputs()
        table_mappings = {}

        config_params = {
            "host": LOCAL_ADDRESS,
            "port": SSH_BIND_PORT,
            "user": params[KEY_MYSQL_USER],
            "password": params[KEY_MYSQL_PWD],
            "connect_timeout": CONNECT_TIMEOUT
        }

        if self.cfg_params[KEY_USE_SSH_TUNNEL]:
            pkey_from_input = paramiko.RSAKey.from_private_key(StringIO(self.cfg_params[KEY_SSH_PRIVATE_KEY]))
            context_manager = SSHTunnelForwarder(
                (self.cfg_params[KEY_SSH_HOST], self.cfg_params[KEY_SSH_PORT]),
                ssh_username=self.cfg_params[KEY_SSH_USERNAME],
                ssh_pkey=pkey_from_input,
                remote_bind_address=(self.cfg_params[KEY_MYSQL_HOST], self.cfg_params[KEY_MYSQL_PORT]),
                local_bind_address=(LOCAL_ADDRESS, SSH_BIND_PORT),
                logger=LOGGER,
                ssh_config_file=None
            )
        else:
            context_manager = nullcontext(None)

        with context_manager as server:
            if server:  # True if set an SSH tunnel returns false if using the null context.
                LOGGER.info('Connecting via SSH tunnel over bind port {}'.format(SSH_BIND_PORT))
            else:
                LOGGER.info('Connecting directly to database via port {}'.format(self.cfg_params[KEY_MYSQL_PORT]))

            mysql_client = MySQLConnection(config_params)
            # TODO: Consider logging server details here.

            if file_input_path:
                manual_table_mappings_file = os.path.join(file_input_path, MAPPINGS_FILE)
                LOGGER.info('Fetching table mappings from file input mapping configuration: {}.'.format(
                    manual_table_mappings_file))
                with open(manual_table_mappings_file, 'r') as mappings_file:
                    table_mappings = json.load(mappings_file)
            elif KEY_TABLE_MAPPINGS_JSON:
                table_mappings = KEY_TABLE_MAPPINGS_JSON

            if params[KEY_OBJECTS_ONLY]:
                # Run only schema discovery process.
                LOGGER.info('Fetching only object and field names, not running full extraction.')
                table_mapping = discover_catalog(mysql_client, params)

                # TODO: Retain prior selected choices by user.

                self.write_table_mappings_file(table_mapping)
            elif table_mappings:
                # Run extractor data sync.
                prior_state = self.get_state_file() or {}

                if prior_state:
                    LOGGER.info('Using prior state file to execute sync.')
                else:
                    LOGGER.info('No prior state found, will need to execute full sync.')
                catalog = Catalog.from_dict(table_mappings)

                do_sync(mysql_client, config_params, catalog, prior_state)

                # with ListStream() as stdout_stream:
                #     do_sync(mysql_client, config_params, catalog, prior_state)
                #
                # result = stdout_stream.get_result()

                # final_state = stdout_stream.get_state()
                # self.write_state_file(final_state)

                # output_file = 'results.json'
                # self.write_result(result, output_file=output_file)
                result_writer.write(self.tables_out_path)
            else:
                LOGGER.error('You have either specified incorrect input parameters, or have not chosen to either '
                             'specify a table mappings file manually or via the File Input Mappings configuration.')
                exit(1)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        debug_arg = sys.argv[1]
    else:
        debug_arg = False
    try:
        # Note: If debugging, run docker-compose instead. Only use below two lines for early testing.
        debug_data_path = os.path.join(module_path, 'data')

        comp = Component(debug_arg, data_path=debug_data_path)
        comp.run()

        # TODO: Add standard call back in below once ready to move to Docker.
        # comp = Component(debug_arg)
        # comp.run()
    except Exception as generic_err:
        LOGGER.exception(generic_err)
        exit(1)
