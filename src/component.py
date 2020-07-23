#!/usr/bin/env python
"""Component main class for data extraction.

Executes component endpoint executions based on dependent api_mappings.json file in same path. This file should have
a set structure, see example below.

Essentially at the table table and column level, add "replication-method" of: FULL_TABLE, INCREMENTAL, or LOG_BASED.
If INCREMENTAL, you need to specify a "replication-key".

# Primary To-Do Items
TODO: Update Config documentation on how to fill out table mappings [Done]
TODO: Schema changes handling
TODO: Numeric vs. Int Issue [Done]
TODO: True/False vs. 1/0 Consistency [Done]

# Secondary To-do Items
TODO: Table Mappings - Handle prior user inputs
TODO: Option to do "one-time" table resync, where just one table resyncs once
TODO: Add testing framework
TODO: Confirm SSL works as expected [DONE]
TODO: Support Ticket for UI for this component (maybe they handle SSL?)
TODO: More User Options
"""
import ast
import base64
import binascii
import copy
import itertools
import json
import logging
import os
import sys

import pandas as pd
import paramiko
import pendulum
import pymysql
import pymysqlreplication
import yaml

from collections import namedtuple
from contextlib import nullcontext
from io import StringIO
from sshtunnel import SSHTunnelForwarder


try:
    import core as core
    import core.metrics as metrics
    import core.datatypes as datatypes

    from core import metadata
    from core.catalog import Catalog, CatalogEntry
    from core.env_handler import KBCEnvHandler
    from core.schema import Schema
    from core.yaml_mappings import convert_yaml_to_json_mapping, make_yaml_mapping_file

    # import mysql.result as result_writer
    import mysql.replication.binlog as binlog
    import mysql.replication.common as common
    import mysql.replication.full_table as full_table
    import mysql.replication.incremental as incremental

    from mysql.client import connect_with_backoff, MySQLConnection
except ImportError as e:
    import src.core as core
    import src.core.metrics as metrics
    import src.core.datatypes as datatypes

    from src.core import metadata
    from src.core.catalog import Catalog, CatalogEntry
    from src.core.env_handler import KBCEnvHandler
    from src.core.schema import Schema
    from src.core.yaml_mappings import convert_yaml_to_json_mapping, make_yaml_mapping_file

    # import src.mysql.result as result_writer
    import src.mysql.replication.binlog as binlog
    import src.mysql.replication.common as common
    import src.mysql.replication.full_table as full_table
    import src.mysql.replication.incremental as incremental

    from src.mysql.client import connect_with_backoff, MySQLConnection

current_path = os.path.dirname(__file__)
module_path = os.path.dirname(current_path)

# Define mandatory parameter constants, matching Config Schema.
KEY_OBJECTS_ONLY = 'fetchObjectsOnly'
KEY_TABLE_MAPPINGS_JSON = 'base64TableMappingsJson'
KEY_DATABASES = 'databases'
KEY_MYSQL_HOST = 'host'
KEY_MYSQL_PORT = 'port'
KEY_MYSQL_USER = 'username'
KEY_MYSQL_PWD = '#password'
KEY_INCREMENTAL_SYNC = 'runIncrementalSync'
KEY_OUTPUT_BUCKET = 'outputBucket'
KEY_USE_SSH_TUNNEL = 'sshTunnel'
KEY_USE_SSL = 'ssl'
KEY_MAPPINGS_FILE = 'storageMappingsFile'
KEY_INPUT_MAPPINGS_YAML = 'inputMappingsYaml'
KEY_STATE_JSON = 'base64StateJson'

# Define optional parameters as constants for later use.
KEY_SSH_HOST = 'sshHost'
KEY_SSH_PORT = 'sshPort'
KEY_SSH_PUBLIC_KEY = 'sshPublicKey'
KEY_SSH_PRIVATE_KEY = '#sshBase64PrivateKey'
KEY_SSH_USERNAME = 'sshUser'
KEY_SSL_CA = 'sslCa'
KEY_VERIFY_CERT = 'verifyCert'

MAPPINGS_FILE = 'table_mappings.json'
LOCAL_ADDRESS = '127.0.0.1'
SSH_BIND_PORT = 3307
CONNECT_TIMEOUT = 30
FLUSH_STORE_THRESHOLD = 5000

# Keep for debugging
KEY_DEBUG = 'debug'
MANDATORY_PARS = (KEY_OBJECTS_ONLY, KEY_MYSQL_HOST, KEY_MYSQL_PORT, KEY_MYSQL_USER, KEY_MYSQL_PWD,
                  KEY_USE_SSH_TUNNEL, KEY_USE_SSL)
MANDATORY_IMAGE_PARS = ()

APP_VERSION = '0.4.7'

pymysql.converters.conversions[pendulum.Pendulum] = pymysql.converters.escape_datetime

# Bin database sub-types by type.
STRING_TYPES = {'char', 'enum', 'longtext', 'mediumtext', 'text', 'varchar'}
FLOAT_TYPES = {'double', 'float'}
DATETIME_TYPES = {'date', 'datetime', 'time', 'timestamp'}
SET_TYPE = 'set'
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


def new_read_binary_json_type_inlined(self, t, large):
    if t == pymysqlreplication.packet.JSONB_TYPE_LITERAL:
        value = self.read_uint32() if large else self.read_uint16()
        if value == pymysqlreplication.packet.JSONB_LITERAL_NULL:
            return None
        if value == pymysqlreplication.packet.JSONB_LITERAL_TRUE:
            return True
        if value == pymysqlreplication.packet.JSONB_LITERAL_FALSE:
            return False
    if t == pymysqlreplication.packet.JSONB_TYPE_INT16:
        return self.read_int16()
    if t == pymysqlreplication.packet.JSONB_TYPE_UINT16:
        return self.read_uint16()
    if t == pymysqlreplication.packet.JSONB_TYPE_INT32:
        return self.read_int32()
    if t == pymysqlreplication.packet.JSONB_TYPE_UINT32:
        return self.read_uint32()
    raise ValueError('Json type %d is not handled' % t)


pymysqlreplication.packet.BinLogPacketWrapper.read_binary_json_type_inlined = new_read_binary_json_type_inlined


def new_read_offset_or_inline(packet, large):
    t = packet.read_uint8()

    if t in (pymysqlreplication.packet.JSONB_TYPE_LITERAL,
             pymysqlreplication.packet.JSONB_TYPE_INT16,
             pymysqlreplication.packet.JSONB_TYPE_UINT16):
        return (t, None, packet.read_binary_json_type_inlined(t, large))
    if large and t in (pymysqlreplication.packet.JSONB_TYPE_INT32,
                       pymysqlreplication.packet.JSONB_TYPE_UINT32):
        return (t, None, packet.read_binary_json_type_inlined(t, large))
    if large:
        return (t, packet.read_uint32(), None)
    return (t, packet.read_uint16(), None)


pymysqlreplication.packet.read_offset_or_inline = new_read_offset_or_inline


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

    elif data_type.startswith(SET_TYPE):
        result.type = ['null', 'string']

    else:
        result = Schema(None, inclusion='unsupported', description='Unsupported column type {}'.format(column_type))
    return result


def create_column_metadata(cols):
    """Write metadata to catalog entry for given columns."""
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
    filter_dbs_config = config.get(KEY_DATABASES)
    logging.debug('Filtering databases via config to: {}'.format(filter_dbs_config))

    if filter_dbs_config:
        filter_dbs_clause = ",".join(["'{}'".format(db) for db in filter_dbs_config])
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

                # Get primary keys
                pk_sql = """
                SELECT
                    k.column_name
                FROM
                    information_schema.table_constraints t
                    INNER JOIN information_schema.key_column_usage k
                        USING(constraint_name, table_schema, table_name)
                WHERE
                    t.constraint_type='PRIMARY KEY'
                    AND t.table_schema = '{}'
                    AND t.table_name='{}';""".format(db, table)
                cur.execute(pk_sql)

                rec = cur.fetchone()
                table_primary_keys = []
                while rec is not None:
                    table_primary_keys.append(rec[0])
                    rec = cur.fetchone()

                table_info[db][table]['primary_keys'] = table_primary_keys

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
                primary_keys = table_info[table_schema][table_name].get('primary_keys')

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
                                     schema=schema, primary_keys=primary_keys, database=table_schema)

                entries.append(entry)

    return Catalog(entries)


def do_discover(mysql_conn, config):
    return discover_catalog(mysql_conn, config).dumps()


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
        logging.warning('Columns %s were selected but are not supported. ping them: {}'.format(
            selected_but_unsupported))

    selected_but_nonexistent = selected.difference(all_columns)
    if selected_but_nonexistent:
        logging.warning(
            'Columns %s were selected but do not exist.',
            selected_but_nonexistent)

    not_selected_but_automatic = automatic.difference(selected)
    if not_selected_but_automatic:
        logging.warning(
            'Columns %s are primary keys but were not selected. Adding them.',
            not_selected_but_automatic)

    return selected.intersection(available).union(automatic)


def log_engine(mysql_conn, catalog_entry):
    is_view = common.get_is_view(catalog_entry)
    database_name = common.get_database_name(catalog_entry)

    if is_view:
        logging.info("Beginning sync for view %s.%s", database_name, catalog_entry.table)
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
                    logging.info("Beginning sync for %s table %s.%s", row[0], database_name, catalog_entry.table)


def is_valid_currently_syncing_stream(selected_stream, state):
    stream_metadata = metadata.to_map(selected_stream.metadata)
    replication_method = stream_metadata.get((), {}).get('replication-method')

    if replication_method.upper() != 'LOG_BASED':
        return True

    if replication_method.upper() == 'LOG_BASED' and binlog_stream_requires_historical(selected_stream, state):
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


def resolve_catalog(discovered_catalog, streams_to_sync) -> Catalog:
    result = Catalog(streams=[])

    # Iterate over the streams in the input catalog and match each one up
    # with the same stream in the discovered catalog.
    for catalog_entry in streams_to_sync:
        catalog_metadata = metadata.to_map(catalog_entry.metadata)
        replication_key = catalog_metadata.get((), {}).get('replication-key')

        discovered_table = discovered_catalog.get_stream(catalog_entry.tap_stream_id)
        database_name = common.get_database_name(catalog_entry)

        if not discovered_table:
            logging.warning('Database %s table %s was selected but does not exist',
                            database_name, catalog_entry.table)
            continue

        selected = {k for k, v in catalog_entry.schema.properties.items()
                    if common.property_is_selected(catalog_entry, k) or k == replication_key}

        # These are the columns we need to select
        columns = desired_columns(selected, discovered_table.schema)

        result.streams.append(CatalogEntry(
            tap_stream_id=catalog_entry.tap_stream_id,
            database=database_name,
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


# TODO: Add check for change in schema for new column, if so full sync that table.
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
    and those labeled "automatic" (e.g. primary keys and replication keys) will be
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
        replication_method = stream_metadata.get((), {}).get('replication-method').upper()
        stream_state = state.get('bookmarks', {}).get(stream.tap_stream_id)

        if not stream_state:
            if replication_method.upper() == 'LOG_BASED':
                logging.info("LOG_BASED stream %s requires full historical sync", stream.tap_stream_id)

            streams_without_state.append(stream)
        elif stream_state and replication_method.upper() == 'LOG_BASED' \
                and binlog_stream_requires_historical(stream, state):
            is_view = common.get_is_view(stream)

            if is_view:
                raise Exception("Unable to replicate stream({}) with binlog because it's a view.".format(stream.stream))

            logging.info("LOG_BASED stream %s will resume its historical sync", stream.tap_stream_id)

            streams_with_state.append(stream)
        elif stream_state and replication_method.upper() != 'LOG_BASED':
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
        replication_method = stream_metadata.get((), {}).get('replication-method').upper()
        stream_state = state.get('bookmarks', {}).get(stream.tap_stream_id)
        logging.debug(stream_state)

        if replication_method.upper() == 'LOG_BASED' and not binlog_stream_requires_historical(stream, state):
            binlog_streams.append(stream)

    return resolve_catalog(discovered, binlog_streams)


def write_schema_message(catalog_entry, message_store=None, bookmark_properties=[]):
    key_properties = common.get_key_properties(catalog_entry)
    core.write_message(core.SchemaMessage(stream=catalog_entry.stream, schema=catalog_entry.schema.to_dict(),
                                          key_properties=key_properties, bookmark_properties=bookmark_properties),
                       message_store=message_store, database_schema=catalog_entry.database)


class Component(KBCEnvHandler):
    """Keboola extractor component."""
    def __init__(self, debug: bool = False, data_path: str = None):
        KBCEnvHandler.__init__(self, MANDATORY_PARS, data_path=data_path,
                               log_level=logging.DEBUG if debug else logging.INFO)

        if self.cfg_params.get(KEY_DEBUG):
            debug = True

        log_level = logging.DEBUG if debug else logging.INFO
        # setup GELF if available
        if os.getenv('KBC_LOGGER_ADDR', None):
            self.set_gelf_logger(log_level)
        else:
            self.set_default_logger(log_level)

        self.files_out_path = os.path.join(self.data_path, 'out', 'files')
        self.files_in_path = os.path.join(self.data_path, 'in', 'files')
        self.state_out_file_path = os.path.join(self.data_path, 'out', 'state.json')
        self.params = self.cfg_params
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            self.validate_config()
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as err:
            logging.exception(err)
            exit(1)

        self.mysql_config_params = {
            "host": LOCAL_ADDRESS,
            "port": SSH_BIND_PORT,
            "user": self.params[KEY_MYSQL_USER],
            "password": self.params[KEY_MYSQL_PWD],
            "ssl": self.params.get(KEY_USE_SSL),
            "ssl_ca": self.params.get(KEY_SSL_CA),
            "verify_mode": self.params.get(KEY_VERIFY_CERT) or False,
            "connect_timeout": CONNECT_TIMEOUT
        }

        # TODO: Update to more clear environment variable; used must set local time to UTC.
        os.environ['TZ'] = 'UTC'

    def _check_file_inputs(self) -> str:
        """Return path name of file inputs if any."""
        file_input = self.files_in_path
        has_file_inputs = any(os.path.isfile(os.path.join(file_input, file)) for file in os.listdir(file_input))

        if has_file_inputs:
            return file_input

    # Sync Methods
    @staticmethod
    def do_sync_incremental(mysql_conn, catalog_entry, state, columns, optional_limit=None,
                            message_store: core.MessageStore = None):
        logging.info("Stream %s is using incremental replication", catalog_entry.stream)

        md_map = metadata.to_map(catalog_entry.metadata)
        replication_key = md_map.get((), {}).get('replication-key')

        if not replication_key:
            raise Exception("Cannot use INCREMENTAL replication for table ({}) without a replication key.".format(
                catalog_entry.stream))

        write_schema_message(catalog_entry=catalog_entry, bookmark_properties=[replication_key],
                             message_store=message_store)

        if optional_limit:
            logging.info("Incremental Stream %s is using an optional limit clause of %d", catalog_entry.stream,
                         int(optional_limit))
            incremental.sync_table(mysql_conn, catalog_entry, state, columns, int(optional_limit),
                                   message_store=message_store)
        else:
            incremental.sync_table(mysql_conn, catalog_entry, state, columns, message_store=message_store)

        core.write_message(core.StateMessage(value=copy.deepcopy(state)), message_store=message_store)

    @staticmethod
    def do_sync_historical_binlog(mysql_conn, config, catalog_entry, state, columns, tables_destination: str = None,
                                  message_store: core.MessageStore = None):
        binlog.verify_binlog_config(mysql_conn)

        is_view = common.get_is_view(catalog_entry)
        key_properties = common.get_key_properties(catalog_entry)

        if is_view:
            raise Exception(
                "Unable to replicate stream({}) with binlog because it is a view.".format(catalog_entry.stream))

        log_file = core.get_bookmark(state, catalog_entry.tap_stream_id, 'log_file')

        log_pos = core.get_bookmark(state, catalog_entry.tap_stream_id, 'log_pos')

        max_pk_values = core.get_bookmark(state, catalog_entry.tap_stream_id, 'max_pk_values')

        last_pk_fetched = core.get_bookmark(state, catalog_entry.tap_stream_id, 'last_pk_fetched')

        write_schema_message(catalog_entry, message_store=message_store)

        stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)

        if log_file and log_pos and max_pk_values:
            logging.info("Resuming initial full table sync for LOG_BASED stream %s", catalog_entry.tap_stream_id)
            full_table.sync_table_chunks(mysql_conn, catalog_entry, state, columns, stream_version,
                                         tables_destination=tables_destination, message_store=message_store)

        else:
            logging.info("Performing initial full table sync for LOG_BASED table {}".format(
                catalog_entry.tap_stream_id))

            state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'initial_binlog_complete', False)

            current_log_file, current_log_pos = binlog.fetch_current_log_file_and_pos(mysql_conn)
            state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'version', stream_version)

            if full_table.sync_is_resumable(mysql_conn, catalog_entry):
                # We must save log_file and log_pos across FULL_TABLE syncs when performing
                # a resumable full table sync
                state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'log_file', current_log_file)
                state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'log_pos', current_log_pos)

                full_table.sync_table_chunks(mysql_conn, catalog_entry, state, columns, stream_version,
                                             tables_destination=tables_destination, message_store=message_store)
            else:
                full_table.sync_table_chunks(mysql_conn, catalog_entry, state, columns, stream_version,
                                             tables_destination=tables_destination, message_store=message_store)
                state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'log_file', current_log_file)
                state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'log_pos', current_log_pos)

    @staticmethod
    def do_sync_full_table(mysql_conn, config, catalog_entry, state, columns, tables_destination: str = None,
                           message_store: core.MessageStore = None):
        logging.info("Stream %s is using full table replication", catalog_entry.stream)
        key_properties = common.get_key_properties(catalog_entry)

        write_schema_message(catalog_entry, message_store=message_store)

        stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)

        full_table.sync_table_chunks(mysql_conn, catalog_entry, state, columns, stream_version,
                                     tables_destination=tables_destination, message_store=message_store)

        # Prefer initial_full_table_complete going forward
        core.clear_bookmark(state, catalog_entry.tap_stream_id, 'version')

        state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'initial_full_table_complete', True)

        core.write_message(core.StateMessage(value=copy.deepcopy(state)), message_store=message_store)

    def sync_non_binlog_streams(self, mysql_conn, non_binlog_catalog, config, state, tables_destination: str = None,
                                message_store: core.MessageStore = None):
        if tables_destination is None:
            logging.info('No table destination specified, so will not work on new CSV write implementation')

        for catalog_entry in non_binlog_catalog.streams:
            columns = list(catalog_entry.schema.properties.keys())

            if not columns:
                logging.warning('There are no columns selected for stream %s, skipping it.', catalog_entry.stream)
                continue

            state = core.set_currently_syncing(state, catalog_entry.tap_stream_id)

            # Emit a state message to indicate that we've started this stream
            core.write_message(core.StateMessage(value=copy.deepcopy(state)), message_store=message_store)

            md_map = metadata.to_map(catalog_entry.metadata)

            replication_method = md_map.get((), {}).get('replication-method')

            database_name = common.get_database_name(catalog_entry)

            with metrics.job_timer('sync_table') as timer:
                timer.tags['database'] = database_name
                timer.tags['table'] = catalog_entry.table

                log_engine(mysql_conn, catalog_entry)

                if replication_method.upper() == 'INCREMENTAL':
                    optional_limit = config.get('incremental_limit')
                    self.do_sync_incremental(mysql_conn, catalog_entry, state, columns, optional_limit,
                                             message_store=message_store)
                elif replication_method.upper() == 'LOG_BASED':
                    self.do_sync_historical_binlog(mysql_conn, config, catalog_entry, state, columns,
                                                   tables_destination=tables_destination, message_store=message_store)
                elif replication_method.upper() == 'FULL_TABLE':
                    self.do_sync_full_table(mysql_conn, config, catalog_entry, state, columns,
                                            tables_destination=tables_destination, message_store=message_store)
                else:
                    raise Exception("only INCREMENTAL, LOG_BASED, and FULL TABLE replication methods are supported")

        state = core.set_currently_syncing(state, None)
        core.write_message(core.StateMessage(value=copy.deepcopy(state)), message_store=message_store)

    @staticmethod
    def sync_binlog_streams(mysql_conn, binlog_catalog, mysql_config, state,
                            message_store: core.MessageStore = None):
        if binlog_catalog.streams:
            for stream in binlog_catalog.streams:
                write_schema_message(stream, message_store=message_store)

            with metrics.job_timer('sync_binlog'):
                binlog.sync_binlog_stream(mysql_conn, mysql_config, binlog_catalog.streams, state,
                                          message_store=message_store)

    def do_sync(self, mysql_conn, config, mysql_config, catalog, state, message_store: core.MessageStore = None):
        non_binlog_catalog = get_non_binlog_streams(mysql_conn, catalog, config, state)
        logging.info('Number of non-binlog tables to process: {}'.format(len(non_binlog_catalog)))
        binlog_catalog = get_binlog_streams(mysql_conn, catalog, config, state)
        logging.info('Number of binlog catalog tables to process: {}'.format(len(binlog_catalog)))

        self.sync_non_binlog_streams(mysql_conn, non_binlog_catalog, config, state,
                                     tables_destination=self.tables_out_path, message_store=message_store)
        self.sync_binlog_streams(mysql_conn, binlog_catalog, mysql_config, state, message_store=message_store)

    @staticmethod
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
                    logging.info('Server Parameters: ' +
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
                    logging.info('Server SSL Parameters (blank means SSL is not active): ' +
                                 '[ssl_version: %s], ' +
                                 '[ssl_cipher: %s]',
                                 mapped_row['Ssl_version'],
                                 mapped_row['Ssl_cipher'])

            except pymysql.err.InternalError as ie:
                logging.warning("Encountered error checking server params. Error: (%s) %s", *ie.args)
    # End of sync methods

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

    def get_table_column_metadata(self, columns_metadata: dict):
        """Return metadata for all columns for given table stream ID."""
        table_columns_metadata = {}

        # First, determine if column is selected. Selected if "selected" is true, or "selected-by-default" is true and
        # "selected" is not specifically set to false
        for column_metadata in columns_metadata:
            column_detail = column_metadata['metadata']
            is_selected_in_detail = True if 'selected' in column_detail else False
            is_column_set_as_selected = column_detail.get('selected')
            is_selected_by_default = column_detail.get('selected-by-default')
            if is_column_set_as_selected or (is_selected_by_default and not is_selected_in_detail):
                column_name = column_metadata['breadcrumb'][1].upper()
                data_type = column_detail.get('sql-datatype')

                table_columns_metadata[column_name] = self.generate_column_metadata(data_type=data_type, nullable=True)

        # Append KBC metadata column types, hard coded for now
        table_columns_metadata[common.KBC_SYNCED] = self.generate_column_metadata(data_type='timestamp', nullable=True)
        table_columns_metadata[common.KBC_DELETED] = self.generate_column_metadata(data_type='timestamp', nullable=True)

        return table_columns_metadata

    def generate_column_metadata(self, data_type: str = None, nullable: bool = None):
        """Return metadata for given column"""
        column_metadata = []
        base_data_type = self._convert_mysql_data_types_to_kbc_types(data_type)

        # Append metadata per input parameter, if present
        if data_type:
            type_metadata = {}
            type_key, type_value = 'KBC.datatype.type', data_type
            type_metadata['key'] = type_key
            type_metadata['value'] = type_value
            column_metadata.append(type_metadata)

            base_type_metadata = {}
            base_type_key, base_type_value = 'KBC.datatype.basetype', base_data_type
            base_type_metadata['key'] = base_type_key
            base_type_metadata['value'] = base_type_value
            column_metadata.append(base_type_metadata)

            # Add length data type if String, just using max for now
            if base_data_type == 'STRING':
                string_length_metadata = {}
                length_type_key, length_type_value = 'KBC.datatype.length', 16777216
                string_length_metadata['key'] = length_type_key
                string_length_metadata['value'] = length_type_value
                column_metadata.append(string_length_metadata)

        if nullable:
            nullable_metadata = {}
            nullable_key, nullable_value = 'KBC.datatype.nullable', nullable
            nullable_metadata['key'] = nullable_key
            nullable_metadata['value'] = nullable_value
            column_metadata.append(nullable_metadata)

        return column_metadata

    @staticmethod
    def _convert_mysql_data_types_to_kbc_types(column_type: str) -> str:
        """Convert given column data type from MySQL data type to Keboola base type."""
        column_type = column_type.lower()
        for db_data_type in datatypes.BASE_STRING:
            if column_type.startswith(db_data_type):
                return 'STRING'
        for db_data_type in datatypes.BASE_INTEGER:
            if column_type.startswith(db_data_type):
                return 'INTEGER'
        for db_data_type in datatypes.BASE_TIMESTAMP:
            if column_type.startswith(db_data_type):
                return 'TIMESTAMP'
        for db_data_type in datatypes.BASE_FLOAT:
            if column_type.startswith(db_data_type):
                return 'FLOAT'
        for db_data_type in datatypes.BASE_BOOLEAN:
            if column_type.startswith(db_data_type):
                return 'BOOLEAN'
        for db_data_type in datatypes.BASE_DATE:
            if column_type.startswith(db_data_type):
                return 'DATE'
        for db_data_type in datatypes.BASE_NUMERIC:
            if column_type.startswith(db_data_type):
                return 'NUMERIC'

        logging.warning('Processed data type {} does not match any KBC base types'.format(column_type))

    def create_manifests(self, entry: dict, data_path: str, columns: list = None, column_metadata: dict = None,
                         set_incremental: bool = True, output_bucket: str = None):
        """Write manifest files for the results produced by the results writer.

        :param entry: Dict entry from catalog
        :param data_path: Path to the result output files
        :param columns: List of strings representing columns for the output table, necessary for sliced tables.
        the `.column` attribute should be
        used in manifest file.
        :param column_metadata: Dict column metadata as keys and values.
        :param set_incremental: Incremental choice true or false for whether to write incrementally to manifest file.
        :param output_bucket: The name of the output bucket to be written to Storage
        :return:
        """
        if entry.get('primary_keys'):
            primary_keys = [key.upper() for key in entry.get('primary_keys')]
        else:
            primary_keys = None
        table_name = entry.get('table_name').upper()
        result_full_path = os.path.join(data_path, table_name + '.csv')

        # for r in results:
        if not columns:
            self.write_table_manifest(result_full_path, primary_key=primary_keys, column_metadata=column_metadata,
                                      is_incremental=set_incremental, output_bucket=output_bucket)
        else:
            self.write_table_manifest(result_full_path, primary_key=primary_keys, columns=columns,
                                      column_metadata=column_metadata, is_incremental=set_incremental,
                                      output_bucket=output_bucket)

    @staticmethod
    def write_table_manifest(file_name: str, destination: str = '', primary_key: list = None, columns: list = None,
                             column_metadata: dict = None, is_incremental: bool = None, output_bucket: str = None):
        """Write manifest for output table Manifest is used for the table to be stored in KBC Storage.

        Args:
            file_name: Local file name of the CSV with table data.
            destination: String name of the table in Storage.
            primary_key: List with names of columns used for primary key.
            columns: List of columns as strings that are written to table, necessary to specify for sliced tables.
            column_metadata: Metadata keys and values about columns in table
            is_incremental: Set to true to enable incremental loading
            output_bucket: The output bucket in storage
        """
        manifest = {}
        if destination:
            if output_bucket:
                manifest['destination'] = 'in.c-' + output_bucket + '.' + destination
            else:
                manifest['destination'] = destination
        if primary_key:
            if isinstance(primary_key, list):
                manifest['primary_key'] = primary_key
            else:
                raise TypeError("Primary key must be a list")
        if columns:
            if isinstance(columns, list):
                manifest['columns'] = columns
            else:
                raise TypeError("Columns must by a list")
        if column_metadata:
            if isinstance(column_metadata, dict):
                manifest['column_metadata'] = column_metadata
            else:
                raise TypeError("Columns must by a list")
        if is_incremental:
            manifest['incremental'] = True

        with open(file_name + '.manifest', 'w') as manifest_file:
            json.dump(manifest, manifest_file)
            logging.info('Wrote manifest table {}'.format(file_name + '.manifest'))

    @staticmethod
    def write_only_latest_result_binlogs(csv_table_path: str, primary_keys: list = None) -> None:
        """For given result CSV file path, remove non-latest binlog event by primary key.

        A primary key can only have a single Write Event and a max of one Delete Event. It can have infinite Update
        Events. Each event returns the current state of the row, so we just want the latest row per extraction.
        """
        if primary_keys:
            logging.info('Keeping only latest per primary key from binary row event results for {} '
                         'based on table primary keys: {}'.format(csv_table_path, primary_keys))
        else:
            logging.warning('Table at path {} does not have primary key, so no binlog de-duplication will occur, '
                            'records must be processed downstream'.format(csv_table_path))
            return

        with metrics.job_timer('latest_binlog_results') as timer:
            timer.tags['csv_table'] = csv_table_path
            timer.tags['primary_key'] = primary_keys

            df = pd.read_csv(csv_table_path)
            df.drop_duplicates(subset=primary_keys, keep='last', inplace=True)
            df.columns = map(str.upper, df.columns)
            df.to_csv(csv_table_path, index=False)

    def get_conn_context_manager(self):
        if self.cfg_params[KEY_USE_SSH_TUNNEL]:
            b64_input_key = self.cfg_params.get(KEY_SSH_PRIVATE_KEY)
            input_key = None
            try:
                input_key = base64.b64decode(b64_input_key, validate=True).decode('utf-8')
            except binascii.Error as bin_err:
                logging.error('Failed to base64-decode the private key, confirm you have base64-encoded your private '
                              'key input variable. Detail: {}'.format(bin_err))
                exit(1)

            pkey_from_input = paramiko.RSAKey.from_private_key(StringIO(input_key))
            context_manager = SSHTunnelForwarder(
                (self.cfg_params[KEY_SSH_HOST], self.cfg_params[KEY_SSH_PORT]),
                ssh_username=self.cfg_params[KEY_SSH_USERNAME],
                ssh_pkey=pkey_from_input,
                remote_bind_address=(self.cfg_params[KEY_MYSQL_HOST], self.cfg_params[KEY_MYSQL_PORT]),
                local_bind_address=(LOCAL_ADDRESS, SSH_BIND_PORT),
                ssh_config_file=None,
                allow_agent=False
            )
        else:
            context_manager = nullcontext(None)

        return context_manager

    def walk_path(self, path: str = None, is_pre_manifest: bool = False):
        """Walk through specified path to QA files/directories/tables (an manifests, if generated)."""
        logging.info('Walking path {} to QA directories and files'.format(path))
        if not path:
            path = self.tables_out_path
        directories = []
        files = []
        for (_, dirs, file_names) in os.walk(path):
            directories.extend(dirs)
            files.extend(file_names)
        if is_pre_manifest:
            logging.info('All pre-manifest directories at walked path: {}'.format(directories))
            logging.info('All pre-manifest files at walked path: {}'.format(files))
        else:
            logging.info('All directories at walked path: {}'.format(directories))
            logging.info('All files sent at walked path: {}'.format(files))

    def run(self):
        """Execute main component extraction process."""
        table_mappings = {}
        file_input_path = self._check_file_inputs()

        # QA Input data
        self.walk_path(self.files_in_path)
        self.walk_path(self.tables_in_path)

        connection_context = self.get_conn_context_manager()

        with connection_context as server:
            if server:  # True if set an SSH tunnel returns false if using the null context.
                logging.info('Connecting via SSH tunnel over bind port {}'.format(SSH_BIND_PORT))
            else:
                logging.info('Connecting directly to database via port {}'.format(self.cfg_params[KEY_MYSQL_PORT]))

            mysql_client = MySQLConnection(self.mysql_config_params)
            self.log_server_params(mysql_client)

            # elif file_input_path:
            #     manual_table_mappings_file = os.path.join(file_input_path, MAPPINGS_FILE)
            #     logging.info('Fetching table mappings from file input mapping configuration: {}.'.format(
            #         manual_table_mappings_file))
            #     with open(manual_table_mappings_file, 'r') as mappings_file:
            #         table_mappings = json.load(mappings_file)

            # TESTING: Fetch current state of database: schemas, tables, columns, datatypes, etc.
            table_mapping = discover_catalog(mysql_client, self.params)

            # Make Raw Mapping file to allow edits
            raw_yaml_mapping = make_yaml_mapping_file(table_mapping.to_dict())

            if self.params.get(KEY_INPUT_MAPPINGS_YAML) and self.params.get(KEY_MAPPINGS_FILE):
                input_method = 'yaml'
                logging.info('Using table mappings based on input YAML mappings')
                yaml_mappings = yaml.load(self.params[KEY_INPUT_MAPPINGS_YAML])
                print('yaml mappings loaded:')
                print(yaml_mappings)
                table_mappings = json.loads(convert_yaml_to_json_mapping(yaml_mappings, table_mapping.to_dict()))
                print('converted to table mappings:')
                print(table_mappings)
            elif self.cfg_params.get(KEY_TABLE_MAPPINGS_JSON):
                input_method = 'json'
                logging.warning('Using table mappings based on Base64-encoded table mappings JSON input parameter. '
                                'Note: this option will be deprecated with version 0.5.0 of the extractor')
                table_mappings = json.loads(base64.b64decode(self.cfg_params.get(KEY_TABLE_MAPPINGS_JSON),
                                                             validate=True).decode('utf-8'))
                print('legacy table mappings:')
                print(table_mappings)
            else:
                raise AttributeError('You are missing either a YAML input mapping, or the legacy Base-64 encoded table '
                                     'mappings JSON. Please specify either to appropriately execute the extractor')

            if self.params[KEY_OBJECTS_ONLY]:
                # Run only schema discovery process.
                logging.info('Fetching only object and field names, not running full extraction.')

                # TODO: Retain prior selected choices by user despite refresh.
                input_file_name = self.params.get(KEY_MAPPINGS_FILE) or 'mappings'
                if input_method == 'json':
                    logging.info('Outputting legacy JSON to file {}.json in KBC storage'.format(input_file_name))
                    out_path = os.path.join(self.files_out_path, input_file_name + '_raw.json')
                    with open(out_path, 'w') as json_out:
                        json_out.write(table_mapping.dumps())

                logging.info('Outputting YAML to file {}.yaml in KBC storage'.format(input_file_name))
                out_path = os.path.join(self.files_out_path, input_file_name + '_raw.yaml')
                with open(out_path, 'w') as yaml_out:
                    yaml_out.write(yaml.dump(raw_yaml_mapping))

            elif table_mappings:
                # Run extractor data sync.
                manually_entered_b64_state = self.cfg_params.get(KEY_STATE_JSON)
                if manually_entered_b64_state:
                    logging.info('Manually input prior state parameter populated for incremental execution')
                    prior_state = base64.b64decode(manually_entered_b64_state, validate=True).decode('utf-8')
                elif self.cfg_params[KEY_INCREMENTAL_SYNC]:
                    prior_state = self.get_state_file() or {}
                else:
                    prior_state = {}

                if prior_state:
                    logging.info('Using prior state file to execute sync')
                elif prior_state == {}:
                    logging.info('No prior state was found, will execute full data sync')
                else:
                    logging.info('Incremental sync set to false, ignoring prior state and running full data sync')

                with core.MessageStore(state=prior_state, flush_row_threshold=FLUSH_STORE_THRESHOLD,
                                       output_table_path=self.tables_out_path) as message_store:
                    catalog = Catalog.from_dict(table_mappings)
                    self.do_sync(mysql_client, self.params, self.mysql_config_params, catalog, prior_state,
                                 message_store=message_store)

                    logging.info('Data extraction completed')

                # QA: Walk through output destination pre-manifest
                self.walk_path(path=self.tables_out_path, is_pre_manifest=True)

                # Determine Manifest file outputs
                tables_and_columns = dict()
                if os.path.exists(os.path.join(current_path, 'table_headers.csv')):
                    with open(os.path.join(current_path, 'table_headers.csv')) as headers_file:
                        tables_and_columns = {row.split('\t')[0]: row.split('\t')[1] for row in headers_file}
                        for item, value in tables_and_columns.items():
                            tables_and_columns[item] = [column.strip().upper() for column in ast.literal_eval(value)]
                        logging.debug('Tables and columns mappings for manifests set to: {}'.format(tables_and_columns))

                for entry in catalog.to_dict()['streams']:
                    entry_table_name = entry.get('table_name')
                    table_metadata = entry['metadata'][0]['metadata']
                    column_metadata = entry['metadata'][1:]

                    if self.params.get(KEY_OUTPUT_BUCKET):
                        output_bucket = self.params[KEY_OUTPUT_BUCKET]
                    else:
                        output_bucket = None

                    if table_metadata.get('selected'):
                        table_replication_method = table_metadata.get('replication-method').upper()

                        # Confirm corresponding table or folder exists
                        table_specific_sliced_path = os.path.join(self.tables_out_path,
                                                                  entry_table_name.upper() + '.csv')
                        if os.path.isdir(table_specific_sliced_path):
                            logging.info('Table {} at location {} is a directory'.format(entry_table_name,
                                                                                         table_specific_sliced_path))
                            output_is_sliced = True
                        elif os.path.isfile(table_specific_sliced_path):
                            logging.info('Table {} at location {} is a file'.format(entry_table_name,
                                                                                    table_specific_sliced_path))
                            output_is_sliced = False
                        else:
                            output_is_sliced = False
                            logging.info('NO DATA found for table {} in either a file or sliced table directory, this '
                                         'table is not being synced'.format(entry_table_name))

                        # TODO: Consider other options for writing to storage based on user choices
                        logging.info('Table has rep method {} and user incremental param is {}'.format(
                            table_replication_method, self.cfg_params[KEY_INCREMENTAL_SYNC]
                        ))
                        if table_replication_method.upper() == 'FULL_TABLE' or \
                                not self.cfg_params[KEY_INCREMENTAL_SYNC]:
                            logging.info('Manifest file will have incremental false for Full Table syncs')
                            manifest_incremental = False
                        else:
                            logging.info('Manifest file will have incremental True for {} sync'.format(
                                table_replication_method
                            ))
                            manifest_incremental = True

                        table_column_metadata = self.get_table_column_metadata(column_metadata)

                        logging.info('Table specific path {} for table {}'.format(table_specific_sliced_path,
                                                                                  entry_table_name))

                        # Write manifest files
                        if output_is_sliced:
                            if core.find_files(table_specific_sliced_path, '*.csv'):
                                logging.info('Writing manifest for {} to "{}" with columns for sliced table'.format(
                                    entry_table_name, self.tables_out_path))
                                self.create_manifests(entry, self.tables_out_path,
                                                      columns=list(tables_and_columns.get(entry_table_name)),
                                                      column_metadata=table_column_metadata,
                                                      set_incremental=manifest_incremental, output_bucket=output_bucket)
                        elif os.path.isfile(table_specific_sliced_path):
                            logging.info('Writing manifest for {} to path "{}" for non-sliced table'.format(
                                entry_table_name, self.tables_out_path))
                            self.create_manifests(entry, self.tables_out_path, column_metadata=table_column_metadata,
                                                  set_incremental=manifest_incremental, output_bucket=output_bucket)

                            # For binlogs (only binlogs are written non-sliced) rewrite CSVs de-duped to latest per PK
                            self.write_only_latest_result_binlogs(table_specific_sliced_path, entry.get('primary_keys'))
                        else:
                            logging.info('No manifest file found for selected table {}, because no data was synced '
                                         'from the database for this table. This may be expected behavior if the table '
                                         'is empty or no new rows were added (if incremental)'.format(entry_table_name))

                        # Write output state file
                        logging.info('Got final state {}'.format(message_store.get_state()))
                        self.write_state_file(message_store.get_state())
                        file_state_destination = os.path.join(self.files_out_path, 'state.json')
                        self.write_state_file(message_store.get_state(), output_path=file_state_destination)

                # QA: Walk through output destination
                self.walk_path(path=self.tables_out_path, is_pre_manifest=False)

            else:
                logging.error('You have either specified incorrect input parameters, or have not chosen to either '
                              'specify a table mappings file manually or via the File Input Mappings configuration.')
                exit(1)

        logging.info('Process execution completed')


if __name__ == "__main__":
    component_start = core.utils.now()
    if len(sys.argv) > 1:
        set_debug = sys.argv[1]
    else:
        set_debug = False

    try:
        if os.path.dirname(current_path) == '/code':
            # Running in docker, assumes volume ./code
            comp = Component(debug=set_debug)
            comp.run()
        else:
            # Running locally, not in Docker
            debug_data_path = os.path.join(module_path, 'data')
            comp = Component(debug=set_debug, data_path=debug_data_path)
            comp.run()

        component_end = core.utils.now()
        component_duration = (component_end - component_start).total_seconds()
        logging.info('Extraction completed successfully in {} seconds'.format(component_duration))

    except Exception as generic_err:
        logging.exception(generic_err)
        exit(1)
