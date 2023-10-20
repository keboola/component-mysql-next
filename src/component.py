#!/usr/bin/env python
import ast
import base64
import binascii
import copy
import csv
import glob
import itertools
import json
import logging
import os
import shutil
import sys
import tempfile
import warnings
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import nullcontext, contextmanager
from io import StringIO
from typing import List

from cryptography.utils import CryptographyDeprecationWarning
from keboola.component import ComponentBase, UserException
from keboola.component.base import sync_action
from keboola.component.sync_actions import ValidationResult, SelectElement

import configuration
import mysql
import table_metadata
from configuration import is_legacy_config, Configuration, KEY_OBJECTS_ONLY, KEY_MYSQL_HOST, KEY_MYSQL_PORT, \
    KEY_MYSQL_USER, KEY_INCLUDE_SCHEMA_NAME, \
    KEY_MYSQL_PWD, KEY_USE_SSH_TUNNEL, KEY_USE_SSL, KEY_MAX_EXECUTION_TIME, \
    KEY_SSL_CA, KEY_VERIFY_CERT, KEY_DATABASES, KEY_TABLE_MAPPINGS_JSON, \
    MANDATORY_PARS, KEY_APPEND_MODE, KEY_SSH_PRIVATE_KEY, KEY_SSH_USERNAME, KEY_SSH_HOST, KEY_SSH_PORT, LOCAL_ADDRESS, \
    ENV_COMPONENT_ID, ENV_CONFIGURATION_ID, KEY_OUTPUT_BUCKET, KEY_INCREMENTAL_SYNC

from ssh.ssh_utils import generate_ssh_key_pair
from table_metadata import column_metadata_to_schema
from workspace_client import SnowflakeClient

with warnings.catch_warnings():
    warnings.filterwarnings('ignore', category=CryptographyDeprecationWarning)
    import paramiko
import pendulum
import pymysql
from sshtunnel import SSHTunnelForwarder

from mysql.replication.stream_reader import TableColumnSchemaCache

import core as core
from core import convert_yaml_to_json_mapping, make_yaml_mapping_file
import core.metrics as metrics
import core.datatypes as datatypes
import core.utils as utils

from core import metadata
from core.catalog import Catalog, CatalogEntry
from core.schema import Schema

# import mysql.result as result_writer
import mysql.replication.binlog as binlog
import mysql.replication.common as common
import mysql.replication.full_table as full_table
import mysql.replication.incremental as incremental

from mysql.client import connect_with_backoff, MySQLConnection, get_execution_time_parameter

# Define mandatory parameter constants, matching Config Schema.

SSH_BIND_PORT = 3307
CONNECT_TIMEOUT = 30
FLUSH_STORE_THRESHOLD = 1000000

# Keep for debugging
KEY_DEBUG = 'debug'
MANDATORY_IMAGE_PARS = ()

pymysql.converters.conversions[pendulum.Pendulum] = pymysql.converters.escape_datetime

# Bin database sub-types by type.
SUPPORTED_STRING_TYPES = {'char', 'enum', 'longtext', 'mediumtext', 'text', 'varchar'}
SUPPORTED_BLOB_TYPES = {"tinyblob", "blob", "mediumblob"}
SUPPORTED_FLOAT_TYPES = {'double', 'float'}
SUPPORTED_DATETIME_TYPES = {'date', 'datetime', 'time', 'timestamp'}
SET_TYPE = 'set'
BYTES_FOR_INTEGER_TYPE = {
    'tinyint': 1,
    'smallint': 2,
    'mediumint': 3,
    'int': 4,
    'bigint': 8
}
BINARY_TYPES = {'binary', 'varbinary'}

TYPES_SUPPORTING_LENGTH = ['CHAR', 'VARCHAR', 'TEXT', 'FLOAT', 'DECIMAL', 'DEC', 'DOUBLE', 'DOUBLE PRECISION']

Column = namedtuple('Column', [
    "table_schema",
    "table_name",
    "column_name",
    "data_type",
    "character_maximum_length",
    "numeric_precision",
    "numeric_scale",
    "column_type",
    "column_key",
    "character_set_name",
    "ordinal_position"
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

    elif data_type in SUPPORTED_FLOAT_TYPES:
        result.type = ['null', 'number']

    elif data_type == 'json':
        result.type = ['null', 'string']

    elif data_type == 'decimal':
        result.type = ['null', 'number']
        result.multipleOf = 10 ** (0 - c.numeric_scale)
        return result

    elif data_type in SUPPORTED_STRING_TYPES:
        result.type = ['null', 'string']
        result.maxLength = c.character_maximum_length
        result.characterSet = c.character_set_name

    elif data_type in SUPPORTED_BLOB_TYPES:
        result.type = ['null', 'string']
        result.maxLength = c.character_maximum_length
        result.characterSet = c.character_set_name

    elif data_type in SUPPORTED_DATETIME_TYPES:
        result.type = ['null', 'string']
        result.format = 'date-time'

    elif data_type.startswith(SET_TYPE):
        result.type = ['null', 'string']

    elif data_type in BINARY_TYPES:
        result.type = ['null', 'binary']

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
        mdata = metadata.write(mdata,
                               ('properties', c.column_name),
                               'ordinal-position',
                               c.ordinal_position)

    return metadata.to_list(mdata)


def discover_catalog(mysql_conn, config, append_mode, include_schema_name: bool = True):
    """Returns a Catalog describing the structure of the database."""
    filter_dbs_config = config.get(KEY_DATABASES)
    logging.debug('Filtering databases via config to: {}'.format(filter_dbs_config))

    if filter_dbs_config:
        filter_dbs_clause = ",".join(["'{}'".format(db) for db in filter_dbs_config])
        table_schema_clause = "WHERE t.table_schema IN ({})".format(filter_dbs_clause)
    else:
        table_schema_clause = """
        WHERE t.table_schema NOT IN (
        'information_schema',
        'performance_schema',
        'mysql',
        'sys'
        )"""

    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute("""
            SELECT table_schema,
                   table_name,
                   table_type,
                   table_rows
            FROM information_schema.tables t
                {}
            AND table_type != 'VIEW'
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
                if append_mode is True:
                    table_info[db][table]['primary_keys'] = []
                else:
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
                SELECT c.table_schema,
                       c.table_name,
                       c.column_name,
                       c.data_type,
                       c.character_maximum_length,
                       c.numeric_precision,
                       c.numeric_scale,
                       c.column_type,
                       c.column_key,
                       c.character_set_name,
                       c.ordinal_position
                    FROM information_schema.columns c JOIN
                    information_schema.tables t ON c.table_schema = t.table_schema and c.table_name = t.table_name
                    {}
                    AND t.table_type != 'VIEW'
                    ORDER BY table_schema, table_name
            """.format(table_schema_clause))

            columns = []
            rec = cur.fetchone()
            while rec is not None:
                columns.append(Column(*rec))
                rec = cur.fetchone()

            entries = []
            logging.debug(f"Table info: {table_info}")
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

                def column_is_key_prop(c, s):
                    return c.column_key == 'PRI' and s.properties[c.column_name].inclusion != 'unsupported'

                key_properties = [c.column_name for c in cols if column_is_key_prop(c, schema)]

                if not is_view:
                    md_map = metadata.write(md_map, (), 'table-key-properties', key_properties)

                binary_columns = []
                for column in cols:
                    if column.data_type.lower() == 'binary':
                        binary_columns += [column.column_name]
                stream_name = table_name
                if include_schema_name:
                    stream_name = f"{table_schema}_{table_name}".replace(".", "_")
                entry = CatalogEntry(table=table_name, stream=stream_name, metadata=metadata.to_list(md_map),
                                     tap_stream_id=common.generate_tap_stream_id(table_schema, table_name),
                                     schema=schema, primary_keys=primary_keys, database=table_schema,
                                     binary_columns=binary_columns, include_schema_name=include_schema_name)

                entries.append(entry)

    return Catalog(entries)


def do_discover(mysql_conn, config, append_mode):
    return discover_catalog(mysql_conn, config, append_mode=append_mode).dumps()


def desired_columns(selected, table_schema, table_name: str = ''):
    """Return the set of column names we need to include in the SELECT.
    selected - set of column names marked as selected in the input catalog
    table_schema - the most recently discovered Schema for the table
    """
    all_columns = set()
    available = set()
    automatic = set()
    unsupported = dict()

    for column, column_schema in table_schema.properties.items():
        all_columns.add(column)
        inclusion = column_schema.inclusion
        if inclusion == 'automatic':
            automatic.add(column)
        elif inclusion == 'available':
            available.add(column)
        elif inclusion == 'unsupported':
            unsupported[column] = column_schema
        else:
            raise Exception('Unknown inclusion ' + inclusion)

    selected_but_unsupported = selected.intersection(list(unsupported.keys()))
    if selected_but_unsupported:
        logging.warning(f'Columns in table {table_name} were selected but are not supported, skipping. '
                        f'Invalid columns:  {[f"{c}:{unsupported[c]}" for c in selected_but_unsupported]}')

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

    return selected.intersection(available).union(automatic), all_columns


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

        if not discovered_table.primary_keys:
            logging.warning(f'Table {database_name}.{catalog_entry.table} '
                            'was selected but does not have primary keys defined, skipping.')

        selected = {k for k, v in catalog_entry.schema.properties.items()
                    if common.property_is_selected(catalog_entry, k) or k == replication_key}

        # These are the columns we need to select
        columns, all_columns = desired_columns(selected, discovered_table.schema, discovered_table.table)
        binary_columns = []

        for column, column_vals in discovered_table.schema.properties.items():
            if column_vals.type and 'binary' in column_vals.type:
                binary_columns += [column]

        result.streams.append(CatalogEntry(
            tap_stream_id=catalog_entry.tap_stream_id,
            database=database_name,
            metadata=catalog_entry.metadata,
            stream=catalog_entry.stream,
            table=catalog_entry.table,
            include_schema_name=catalog_entry.include_schema_name,
            schema=Schema(
                type='object',
                properties={col: discovered_table.schema.properties[col]
                            for col in columns}
            ),
            full_schema=Schema(
                type='object',
                properties={col: discovered_table.schema.properties[col]
                            for col in all_columns}
            ),
            binary_columns=binary_columns
        ))

    return result


# TODO: Add check for change in schema for new column, if so full sync that table.
def get_non_binlog_streams(mysql_conn, catalog, config, state, append_mode):
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
    discovered = discover_catalog(mysql_conn, config, append_mode)

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
                logging.warning(f"{stream.tap_stream_id} not found in {state.get('bookmarks')}")
                logging.warning(state)
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


def get_binlog_streams(mysql_conn, catalog, config, state, append_mode):
    discovered = discover_catalog(mysql_conn, config, append_mode)

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


class Component(ComponentBase):
    """Keboola extractor component."""

    def __init__(self):
        super().__init__()

        self.params: dict

        self._snowflake_client: SnowflakeClient

        if not self.configuration.parameters.get("debug"):
            logging.getLogger('snowflake.connector').setLevel(logging.WARNING)

    def _init_workspace_client(self):
        snfwlk_credentials = {
            "account": self.configuration.workspace_credentials['host'].replace('.snowflakecomputing.com', ''),
            "user": self.configuration.workspace_credentials['user'],
            "password": self.configuration.workspace_credentials['password'],
            "database": self.configuration.workspace_credentials['database'],
            "schema": self.configuration.workspace_credentials['schema'],
            "warehouse": self.configuration.workspace_credentials['warehouse']
        }
        self._snowflake_client = SnowflakeClient(**snfwlk_credentials)

    def _init_configuration(self):
        logging.info('Loading configuration...')
        if not is_legacy_config(self.configuration.parameters):
            self.validate_configuration_parameters(Configuration.get_dataclass_required_parameters())
            cfg = Configuration.load_from_dict(self.configuration.parameters)
            self.params = configuration.convert_to_legacy(cfg)
        else:
            self.validate_configuration_parameters(MANDATORY_PARS)
            self.params = self.configuration.parameters
            # old versions do not support schema prefix
            self.params[KEY_INCLUDE_SCHEMA_NAME] = False

        if self.params.get(KEY_TABLE_MAPPINGS_JSON) == '{}':
            raise UserException("You must select at least one database and table to sync!")

        # TODO: Update to more clear environment variable; used must set local time to UTC.
        os.environ['TZ'] = 'UTC'

    def _init_connection_params(self):

        if not is_legacy_config(self.configuration.parameters):
            cfg = Configuration.load_from_dict(self.configuration.parameters)
            self.params = configuration.convert_to_legacy(cfg)
        else:
            self.params = self.configuration.parameters
            # old versions do not support schema prefix
            self.params[KEY_INCLUDE_SCHEMA_NAME] = False

        max_execution_time = self.params.get(KEY_MAX_EXECUTION_TIME)
        if max_execution_time:
            max_execution_time = self.params.get(KEY_MAX_EXECUTION_TIME)
            if len(str(max_execution_time)) > 0:
                try:
                    max_execution_time = int(max_execution_time)
                    logging.info(f"Using parameter max_execution time from config: {max_execution_time}")
                except ValueError as e:
                    raise Exception(f"Cannot cast parameter {max_execution_time} to integer.") from e

        self._validate_parameters(self.params, [KEY_MYSQL_HOST, KEY_MYSQL_PORT, KEY_MYSQL_USER, KEY_MYSQL_PWD],
                                  'Db Options')
        self.mysql_config_params = {
            "host": self.params[KEY_MYSQL_HOST],
            "port": self.params[KEY_MYSQL_PORT],
            "user": self.params[KEY_MYSQL_USER],
            "password": self.params[KEY_MYSQL_PWD],
            "ssl": self.params.get(KEY_USE_SSL),
            "ssl_ca": self.params.get(KEY_SSL_CA),
            "verify_mode": self.params.get(KEY_VERIFY_CERT) or False,
            "connect_timeout": CONNECT_TIMEOUT,
            "max_execution_time": max_execution_time
        }

        if self.params[KEY_USE_SSH_TUNNEL]:
            self._validate_parameters(self.params, [KEY_SSH_PORT, KEY_SSH_USERNAME, KEY_SSH_PRIVATE_KEY, KEY_SSH_HOST],
                                      'SSH Options')
            logging.info('Connecting via SSH tunnel over bind port {}'.format(SSH_BIND_PORT))
            self.mysql_config_params['host'] = LOCAL_ADDRESS
            self.mysql_config_params['port'] = SSH_BIND_PORT
        else:
            logging.info(
                'Connecting directly to database via port {}'.format(self.params[KEY_MYSQL_PORT]))

    @contextmanager
    def init_mysql_client(self) -> MySQLConnection:
        connection_context = self.get_conn_context_manager()
        mysql_client = MySQLConnection(self.mysql_config_params)
        try:
            if not isinstance(connection_context, nullcontext):
                connection_context.start()
            yield mysql_client
        except Exception as e:
            raise UserException(f"Failed to create connection, please check you have properly set the private key "
                                f"and updated your server's ssh_keys. {e}") from e
        finally:
            if not isinstance(connection_context, nullcontext) and connection_context.is_alive:
                connection_context.close()

    def run(self):
        """Execute main component extraction process."""
        self._init_configuration()
        self._init_connection_params()
        self._init_workspace_client()

        file_input_path = self._check_file_inputs()  # noqa

        # QA Input data
        self.walk_path(self.files_in_path)
        self.walk_path(self.tables_in_path)

        with self.init_mysql_client() as mysql_client:

            self.log_server_params(mysql_client)

            catalog_mapping = discover_catalog(mysql_client, self.params, append_mode=self.params.get(KEY_APPEND_MODE),
                                               include_schema_name=self.params.get(KEY_INCLUDE_SCHEMA_NAME, False))

            # TODO: WTF???
            # Make Raw Mapping file to allow edits
            raw_yaml_mapping = make_yaml_mapping_file(catalog_mapping.to_dict())
            if (_json := self.params.get(KEY_TABLE_MAPPINGS_JSON)) and _json != '{}' and _json != '':  # noqa
                input_method = 'json'
                logging.info('Using table mappings based on input JSON mappings.')

                try:
                    input_mapping = json.loads(self.params.get(configuration.KEY_TABLE_MAPPINGS_JSON))
                    logging.debug(f"Received input schema: {input_mapping}")
                except ValueError:
                    logging.error("Invalid JSON mappins provided. Could not parse JSON.")
                    sys.exit(1)

                input_mapping, schemas_to_sync, tables_to_sync, columns_to_sync = self.parse_input_mapping(
                    input_mapping)
                table_mappings = json.loads(convert_yaml_to_json_mapping(input_mapping, catalog_mapping.to_dict()))

            else:
                raise AttributeError('No schemas or tables are selected.')

            if self.params[KEY_OBJECTS_ONLY]:
                # Run only schema discovery process.
                logging.info('Fetching only object and field names, not running full extraction.')

                # TODO: Retain prior selected choices by user despite refresh.
                input_file_name = self.params.get(configuration.KEY_MAPPINGS_FILE) or 'mappings'
                if input_method == 'json':
                    logging.info('Outputting JSON to file {}.json in KBC storage'.format(input_file_name))
                    out_path = os.path.join(self.files_out_path, input_file_name + '_raw.json')
                    with open(out_path, 'w') as json_out:
                        json.dump(raw_yaml_mapping, json_out)

            elif table_mappings:
                # Run extractor data sync.
                if self.params[KEY_INCREMENTAL_SYNC]:
                    prior_state = self.get_state_file() or {}
                else:
                    prior_state = {}

                if prior_state:
                    logging.info('Using prior state file to execute sync')
                elif prior_state == {}:
                    logging.info('No prior state was found, will execute full data sync')
                else:
                    logging.info('Incremental sync set to false, ignoring prior state and running full data sync')
                output_bucket = self.create_output_bucket(self.params.get(KEY_OUTPUT_BUCKET))
                with core.MessageStore(state=prior_state, flush_row_threshold=FLUSH_STORE_THRESHOLD,
                                       output_table_path=self.tables_out_path,
                                       binary_handler=self.params.get(configuration.KEY_HANDLE_BINARY, 'plain'),
                                       output_bucket=output_bucket,
                                       include_schema_name=self.params[KEY_INCLUDE_SCHEMA_NAME]) as message_store:
                    catalog = Catalog.from_dict(table_mappings)
                    self.do_sync(mysql_client, self.params, self.mysql_config_params, catalog, prior_state,
                                 message_store=message_store, schemas=schemas_to_sync, tables=tables_to_sync,
                                 columns=columns_to_sync)

                    logging.info('Data extraction completed')

                # upload data to stage in parallel
                # Determine Manifest file outputs (Only full load)

                # convert to uppercase to match behaviour in other places
                tables_and_columns_order = {t: [col.upper() for col in v] for t, v in
                                            message_store.full_sync_headers.items()}

                with self._snowflake_client.connect():

                    with ThreadPoolExecutor(max_workers=10) as executor:
                        futures = {
                            executor.submit(self._write_result_table, entry, message_store,
                                            tables_and_columns_order): entry for
                            entry in [entry for entry in catalog.to_dict()['streams']
                                      if entry['metadata'][0]['metadata'].get('selected')]
                        }
                        for future in as_completed(futures):
                            if future.exception():
                                raise UserException(
                                    f"Could not create table: {futures[future].get('result_table_name')},"
                                    f" reason: {future.exception()}") from future.exception()

                            future.result()

                    # write schema changes if present.
                    chc_path = os.path.join(self.tables_out_path, 'SCHEMA_CHANGES.csv')
                    if os.path.exists(chc_path):
                        self._create_table_in_stage('SCHEMA_CHANGES', chc_path,
                                                    primary_key_columns=['column_name', 'query', 'timestamp'],
                                                    table_column_metadata=self.get_schema_changes_metadata(),
                                                    is_full_sync=False,
                                                    dedupe=False)
                self.write_state_file(message_store.get_state())

                # QA: Walk through output destination
                self.walk_path(path=self.tables_out_path, is_pre_manifest=False)

            else:
                raise UserException(
                    'You have either specified incorrect input parameters, or have not chosen to either '
                    'specify a table mappings file manually or via the File Input Mappings configuration.')

        logging.info('Process execution completed')

    def get_header_from_file(self, file_path: str):
        with open(file_path) as input:
            delimiter = ','
            enclosure = '"'
            reader = csv.DictReader(input, lineterminator='\n', delimiter=delimiter, quotechar=enclosure)
            return reader.fieldnames

    def _write_result_table(self, entry: dict, message_store, tables_and_columns_order: dict):
        entry_table_name = entry.get('result_table_name')
        table_metadata = entry['metadata'][0]['metadata']
        column_metadata = entry['metadata'][1:]

        output_bucket = self.create_output_bucket(self.params.get(KEY_OUTPUT_BUCKET))

        table_replication_method = table_metadata.get('replication-method').upper()
        # Confirm corresponding table or folder exists
        table_specific_sliced_path = os.path.join(self.tables_out_path, entry_table_name.upper())

        # TODO: in platform sometimes extension was upper case - WTF fix
        if entry_table_name.upper() + '.csv' in os.listdir(self.tables_out_path):
            extension = '.csv'
        else:
            extension = '.CSV'

        table_specific_sliced_path += extension
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
            logging.warning(
                'NO DATA found for table {} in either a file or sliced table directory, this '
                'table is not being synced'.format(entry_table_name))

        # TODO: Consider other options for writing to storage based on user choices
        logging.info('Table has rep method {} and user incremental param is {}'.format(
            table_replication_method, self.params[KEY_INCREMENTAL_SYNC]
        ))

        if table_replication_method.upper() == 'FULL_TABLE' or \
                not self.params[KEY_INCREMENTAL_SYNC]:
            logging.info('Manifest file will have incremental false for Full Table syncs')
            manifest_incremental = False
        else:
            logging.info('Manifest file will have incremental True for {} sync'.format(
                table_replication_method
            ))
            manifest_incremental = True

        _table_column_metadata = self.get_table_column_metadata(column_metadata)

        try:
            if not output_is_sliced:
                with open(table_specific_sliced_path) as io:
                    rdr = csv.DictReader(io)
                    fields = rdr.fieldnames
            else:
                fields = None

        except FileNotFoundError:
            fields = []
        except IsADirectoryError:
            fields = None
        logging.info('Table specific path {} for table {}'.format(table_specific_sliced_path,
                                                                  entry_table_name))
        if fields is not None:
            table_column_metadata = dict()

            for key, val in _table_column_metadata.items():
                if key in fields:
                    table_column_metadata[key] = val

        else:
            table_column_metadata = _table_column_metadata

        # Write manifest files

        if not ((output_is_sliced and core.find_files(table_specific_sliced_path, '*.csv')) or os.path.isfile(
                table_specific_sliced_path)):
            logging.info(f'No data was synced '
                         f'from the database for {entry_table_name} table. This may be expected behavior if the table '
                         'is empty or no new rows were added (if incremental)')
            return

        if bool(self.params.get(KEY_APPEND_MODE, False)) is True:
            primary_keys = None
        elif entry.get('primary_keys'):
            primary_keys = [key.upper() for key in entry.get('primary_keys')]
        else:
            primary_keys = None

        # TODO: for backward compatibility, make configurable
        if tables_and_columns_order.get(entry_table_name):
            ordered_columns = tables_and_columns_order.get(entry_table_name)
        else:
            ordered_columns = fields
        result_table_name = entry_table_name.upper()
        logging.info("Ordering columns for sliced upload.")
        # order metadata
        table_column_metadata = self._order_metadata(table_column_metadata, ordered_columns)
        self._create_table_in_stage(result_table_name, table_specific_sliced_path,
                                    primary_keys, table_column_metadata, output_is_sliced)

        self.create_manifests(entry, self.tables_out_path,
                              columns=list(table_column_metadata.keys()),
                              column_metadata=table_column_metadata,
                              set_incremental=manifest_incremental,
                              output_bucket=output_bucket,
                              extension=extension,
                              primary_keys=primary_keys)

        # Write output state file
        logging.debug('Got final state {}'.format(message_store.get_state()))

    def _order_metadata(self, column_metadata: dict, column_order: list):
        """
        This orders metadata based on column order.
        Relevant for full sync and when schema changes. Spaghetti code patch.
        Args:
            column_metadata:
            column_order:

        Returns:

        """
        default_metadata = [
            {'key': 'KBC.datatype.basetype', 'value': 'STRING'},
            {'key': 'KBC.datatype.nullable', 'value': True}]
        ordered_metadata = dict()
        for col in column_order:
            col_name = col[1:] if col.startswith('_') else col
            if col_name in column_metadata:
                ordered_metadata[col] = column_metadata[col_name]
            else:
                # column was deleted, putting default type
                ordered_metadata[col] = default_metadata

        return ordered_metadata

    def _create_table_in_stage(self, result_table_name: str, table_path: str, primary_key_columns: list[str],
                               table_column_metadata: dict,
                               is_full_sync: bool, dedupe: bool = True):

        # rename columns to deal with KBC Storage limitations
        normalizer = utils.KBCNormalizer()
        columns = normalizer.normalize_header(list(table_column_metadata.keys()))
        renamed_metadata = {}
        for renamed, v in zip(columns, table_column_metadata.values()):
            renamed_metadata[renamed] = v

        column_types = self._convert_to_snowflake_column_definitions(renamed_metadata)

        logging.info(f"Creating table {result_table_name} in stage")
        self._snowflake_client.create_table(result_table_name, column_types)

        logging.info(f"Uploading data into table {result_table_name} in stage")
        if is_full_sync:
            # chunks if fullsync
            tables = glob.glob(os.path.join(table_path, '*.csv'))
            file_format = self._snowflake_client.DEFAULT_FILE_FORMAT.copy()
            file_format['SKIP_HEADER'] = 0
            for table in tables:
                self._snowflake_client.copy_csv_into_table_from_file(result_table_name, columns, column_types, table,
                                                                     file_format=file_format)

        else:
            self._snowflake_client.copy_csv_into_table_from_file(result_table_name, columns, column_types, table_path)
            if dedupe:
                self._dedupe_stage_table(table_name=result_table_name, id_columns=primary_key_columns)

    def _dedupe_stage_table(self, table_name: str, id_columns: list[str]):
        """
        Dedupe staging table and keep only latest records.
        Based on the internal column kbc__event_order produced by CDC engine
        Args:
            table_name:
            id_columns:

        Returns:

        """
        id_cols = self._snowflake_client.wrap_columns_in_quotes(id_columns)
        id_cols_str = ','.join([f'"{table_name}".{col}' for col in id_cols])
        unique_id_concat = f"CONCAT_WS('|',{id_cols_str},\"BINLOG_CHANGE_AT\")"

        query = f"""DELETE FROM
                            "{table_name}" USING (
                            SELECT
                                {unique_id_concat} AS "ID"
                            FROM
                                "{table_name}"
                                QUALIFY ROW_NUMBER() OVER (PARTITION BY {id_cols_str} ORDER BY
                                    "BINLOG_CHANGE_AT"::INT DESC) != 1) TO_DELETE
                        WHERE
                            TO_DELETE.ID = {unique_id_concat}
            """

        logging.debug(f'Dedupping table {table_name}: {query}')
        self._snowflake_client.execute_query(query)

        self._snowflake_client.execute_query('commit')

    def _check_file_inputs(self) -> str:
        """Return path name of file inputs if any."""
        file_input = self.files_in_path
        has_file_inputs = any(os.path.isfile(os.path.join(file_input, file)) for file in os.listdir(file_input))

        if has_file_inputs:
            return file_input

    def _convert_to_snowflake_column_definitions(self, column_metadata: dict) -> list[dict[str, str]]:
        column_types = []

        for name, md in column_metadata.items():
            c = column_metadata_to_schema(name, md, TYPES_SUPPORTING_LENGTH)
            dtype = c.base_type
            # Only NUMERIC types can have length
            length_clause = ''
            if c.length and c.base_type.upper() in ['NUMERIC', 'STRING']:
                # Postgres return full integer value if no length is specified
                if c.base_type.upper() == 'STRING' and int(c.length) > 16777216:
                    c.length = 16777216

                length_clause += str(c.length)
            if c.precision and c.base_type.upper() in ['NUMERIC']:
                length_clause += f', {c.precision}'

            if length_clause:
                dtype += f'({length_clause})'

            type_def = {"name": c.name, "type": dtype, 'convert_nulls': False}

            if c.base_type.upper() != 'STRING':
                type_def['convert_nulls'] = True

            column_types.append(type_def)

        return column_types

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
        key_properties = common.get_key_properties(catalog_entry)  # noqa

        if is_view:
            raise Exception(
                "Unable to replicate stream({}) with binlog because it is a view.".format(catalog_entry.stream))

        log_file = core.get_bookmark(state, catalog_entry.tap_stream_id, 'log_file')

        log_pos = core.get_bookmark(state, catalog_entry.tap_stream_id, 'log_pos')

        max_pk_values = core.get_bookmark(state, catalog_entry.tap_stream_id, 'max_pk_values')

        last_pk_fetched = core.get_bookmark(state, catalog_entry.tap_stream_id, 'last_pk_fetched')  # noqa

        write_schema_message(catalog_entry, message_store=message_store)

        stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)

        # Update state last_table_schema with current schema, and store KBC cols
        table_schema = Component._build_schema_cache_from_catalog_entry(catalog_entry, full=True)
        state = core.update_schema_in_state(state, {catalog_entry.tap_stream_id: table_schema})

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
    def _build_schema_cache_from_catalog_entry(catalog_entry, full=False):
        table_schema = []
        primary_keys = common.get_key_properties(catalog_entry)
        if full:
            column_properties = catalog_entry.full_schema.properties
        else:
            column_properties = catalog_entry.schema.properties

        for idx, column_metadata in enumerate(catalog_entry.metadata[1:], start=1):
            col_name = column_metadata['breadcrumb'][1]

            if col_name not in column_properties:
                logging.debug(f"Skipping columns: {col_name}")
                continue
            col_type = column_metadata['metadata']['sql-datatype']
            ordinal_position = column_metadata['metadata']['ordinal-position']
            is_pkey = col_name in primary_keys
            character_set = column_properties[col_name].characterSet
            schema = TableColumnSchemaCache.build_column_schema(col_name.upper(), ordinal_position, col_type, is_pkey,
                                                                character_set_name=character_set)
            table_schema.append(schema)

        # ensure sort by ordinal position to avoid later shift
        table_schema = sorted(table_schema, key=lambda sch: sch['ORDINAL_POSITION'])

        return table_schema

    @staticmethod
    def do_sync_full_table(mysql_conn, config, catalog_entry, state, columns, tables_destination: str = None,
                           message_store: core.MessageStore = None):
        logging.info("Stream %s is using full table replication", catalog_entry.stream)
        key_properties = common.get_key_properties(catalog_entry)  # noqa

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
            # only selected
            columns = [k for k, v in catalog_entry.schema.properties.items()]

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
                            message_store: core.MessageStore = None, schemas=[], tables=[], columns={}):
        if binlog_catalog.streams:
            for stream in binlog_catalog.streams:
                write_schema_message(stream, message_store=message_store)

            with metrics.job_timer('sync_binlog'):
                binlog.sync_binlog_stream(mysql_conn, mysql_config, binlog_catalog.streams, state,
                                          message_store=message_store, schemas=schemas, tables=tables, columns=columns)

    def do_sync(self, mysql_conn, config, mysql_config, catalog, state,
                message_store: core.MessageStore = None, schemas=[], tables=[], columns={}):
        non_binlog_catalog = get_non_binlog_streams(mysql_conn, catalog, config, state,
                                                    self.params.get(KEY_APPEND_MODE))
        logging.info('Number of non-binlog tables to process: {}'.format(len(non_binlog_catalog)))
        binlog_catalog = get_binlog_streams(mysql_conn, catalog, config, state, self.params.get(KEY_APPEND_MODE))
        logging.info('Number of binlog catalog tables to process: {}'.format(len(binlog_catalog)))

        self.sync_non_binlog_streams(mysql_conn, non_binlog_catalog, config, state,
                                     tables_destination=self.tables_out_path, message_store=message_store)
        self.sync_binlog_streams(mysql_conn, binlog_catalog, mysql_config, state,
                                 message_store=message_store, schemas=schemas, tables=tables, columns=columns)

    @staticmethod
    def log_server_params(mysql_conn):
        with connect_with_backoff(mysql_conn) as open_conn:
            try:
                with open_conn.cursor() as cur:
                    exec_time_variable = get_execution_time_parameter(cur)
                    cur.execute(f'''
                        SELECT VERSION() as version,
                               @@session.wait_timeout as wait_timeout,
                               @@session.innodb_lock_wait_timeout as innodb_lock_wait_timeout,
                               @@session.max_allowed_packet as max_allowed_packet,
                               @@session.interactive_timeout as interactive_timeout,
                               @@session.{exec_time_variable} as max_execution_time''')
                    row = cur.fetchone()
                    logging.info('Server Parameters: ' +
                                 'version: %s, ' +
                                 'wait_timeout: %s, ' +
                                 'innodb_lock_wait_timeout: %s, ' +
                                 'max_allowed_packet: %s, ' +
                                 'interactive_timeout: %s ' +
                                 'max_execution_time: %s',
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
            column_name = column_metadata['breadcrumb'][1].upper()
            data_type = column_detail.get('sql-datatype')

            table_columns_metadata[column_name] = self.generate_column_metadata(data_type=data_type, nullable=True)

        # Append KBC metadata column types, hard coded for now
        table_columns_metadata[common.KBC_SYNCED] = self.generate_column_metadata(data_type='timestamp', nullable=True)
        table_columns_metadata[common.KBC_DELETED] = self.generate_column_metadata(
            data_type='timestamp', nullable=True)
        table_columns_metadata[common.BINLOG_CHANGE_AT] = self.generate_column_metadata(data_type='integer',
                                                                                        nullable=True)
        table_columns_metadata[common.BINLOG_READ_AT] = self.generate_column_metadata(data_type='integer',
                                                                                      nullable=True)

        return table_columns_metadata

    def _get_size_and_precision(self, datatype: str):
        length = None
        precision = None
        size = ()
        if len(split_parts := datatype.split('(')) > 1:
            # remove anything after ) e.g. int(12) unsigned)
            size_str = split_parts[1].split(')')[0]
            size = ast.literal_eval(f'({size_str})')

        if size and isinstance(size, tuple):
            length = size[0]
            precision = size[1]
        elif size:
            length = size
        return length, precision

    def generate_column_metadata(self, data_type: str = None, nullable: bool = None):
        """Return metadata for given column"""
        column_metadata = []
        base_data_type = self._convert_mysql_data_types_to_kbc_types(data_type)

        # Append metadata per input parameter, if present
        if data_type:
            length, precision = self._get_size_and_precision(data_type)
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
            if table_metadata.is_type_with_length(data_type, TYPES_SUPPORTING_LENGTH):
                length_type_key = 'KBC.datatype.length'
                if length:
                    if precision:
                        length = f'{length},{precision}'
                if base_data_type in ['NUMERIC', 'FLOAT']:
                    string_length_metadata = {}

                    if length:
                        string_length_metadata['key'] = length_type_key
                        string_length_metadata['value'] = length
                        column_metadata.append(string_length_metadata)
                elif base_data_type in ['STRING']:
                    string_length_metadata = {}
                    if 'binary' in data_type.lower():
                        # store binary as TEXT size
                        length = 16777216
                    if length:
                        string_length_metadata['key'] = length_type_key
                        string_length_metadata['value'] = length
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
                         set_incremental: bool = True, output_bucket: str = None, extension='.csv',
                         primary_keys: list = None):
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

        table_name = entry.get('result_table_name').upper()
        result_full_path = os.path.join(data_path, table_name)

        # rename columns to deal with KBC Storage limitations
        normalizer = utils.KBCNormalizer()
        columns_renamed = normalizer.normalize_header(columns)
        renamed_metadata = {}
        for renamed, v in zip(columns_renamed, column_metadata.values()):
            renamed_metadata[renamed] = v

        # for r in results:
        if not columns:
            self.write_table_manifest(result_full_path, destination=table_name,
                                      primary_key=primary_keys, column_metadata=renamed_metadata,
                                      is_incremental=set_incremental, output_bucket=output_bucket)
        else:
            self.write_table_manifest(result_full_path, destination=table_name,
                                      primary_key=primary_keys, columns=columns_renamed,
                                      column_metadata=renamed_metadata, is_incremental=set_incremental,
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
        if output_bucket:
            manifest['destination'] = output_bucket + '.' + destination
        else:
            pass
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
    def deduplicate_binlog_result(table_path: str, primary_keys: List[str], buffer_size=8192):
        """
        Reads table backwards and deduplicates based on  primary key.
        Args:
            table_path:
            primary_keys:
            buffer_size

        Returns:

        """

        with open(table_path, 'r') as inp:
            header = csv.DictReader(inp).fieldnames
        pkey_hashes = set()

        def create_pkey_hash(row_record: dict):
            try:
                pkey_hash_str = '|'.join(row_record[idx] for idx in primary_keys)
                return pkey_hash_str
            except IndexError:
                # TODO: remove temp debug statement
                for idx in primary_keys:
                    try:
                        row_record[idx]
                    except IndexError:
                        logging.error(f"Pkey index {idx} not found in row: {row_record}")
                        raise Exception(f"Pkey index {idx} not found in row: {row_record} "
                                        f"for primary key: {primary_keys}")

        fd, temp_result = tempfile.mkstemp()
        # FIX line 1: field larger than field limit error
        # as proposed here https://stackoverflow.com/questions/15063936/csv-error-field-larger-than-field-limit-131072
        csv.field_size_limit(sys.maxsize)
        with open(temp_result, 'w+', newline='', encoding='utf-8') as out_file, open(table_path, 'rb') as inp:
            writer = csv.DictWriter(out_file, fieldnames=header, lineterminator='\n')
            reader = csv.DictReader(core.utils.reverse_readline(inp, buf_size=buffer_size), fieldnames=header)
            writer.writeheader()
            for row in reader:
                if not row:
                    logging.warning("Empty row in result")
                    continue
                pkey_hash = create_pkey_hash(row)
                if pkey_hash in pkey_hashes:
                    continue

                pkey_hashes.add(pkey_hash)

                if list(row.values()) != header:
                    writer.writerow(row)

        os.remove(table_path)
        shutil.move(temp_result, table_path)

    def write_only_latest_result_binlogs(self, csv_table_path: str, primary_keys: list = None,
                                         append_mode: bool = False) -> None:
        """For given result CSV file path, remove non-latest binlog event by primary key.

        A primary key can only have a single Write Event and a max of one Delete Event. It can have infinite Update
        Events. Each event returns the current state of the row, so we just want the latest row per extraction.
        """

        with metrics.job_timer('latest_binlog_results') as timer:
            timer.tags['csv_table'] = csv_table_path
            timer.tags['primary_key'] = primary_keys

            # Read DF as Strings to avoid incorrect rounding issues with conversions of ints/numerics to floats

            if primary_keys and append_mode is not True:
                logging.info('Keeping only latest per primary key from binary row event results for {} '
                             'based on table primary keys: {}'.format(csv_table_path, primary_keys))

                # TODO: remove
                shutil.copy(csv_table_path, self.files_out_path + '/test.csv')

                self.deduplicate_binlog_result(csv_table_path, [pk.upper() for pk in primary_keys])

            else:

                if append_mode is True:
                    logging.info("Append mode active, no deduplication of rows occuring.")

                else:
                    logging.warning('Table at path {} does not have primary key, '
                                    'so no binlog de-duplication will occur, '
                                    'records must be processed downstream'.format(csv_table_path))

    def get_conn_context_manager(self):
        if self.params[KEY_USE_SSH_TUNNEL]:
            b64_input_key = self.params.get(KEY_SSH_PRIVATE_KEY)
            input_key = None
            try:
                input_key = base64.b64decode(b64_input_key, validate=True).decode('utf-8')
            except binascii.Error as bin_err:
                logging.error('Failed to base64-decode the private key, confirm you have base64-encoded your private '
                              'key input variable. Detail: {}'.format(bin_err))
                exit(1)

            pkey_from_input = paramiko.RSAKey.from_private_key(StringIO(input_key))
            context_manager = SSHTunnelForwarder(
                (self.params[KEY_SSH_HOST], self.params[KEY_SSH_PORT]),
                ssh_username=self.params[KEY_SSH_USERNAME],
                ssh_pkey=pkey_from_input,
                remote_bind_address=(
                    self.params[KEY_MYSQL_HOST], self.params[KEY_MYSQL_PORT]),
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

    def parse_input_mapping(self, input_mapping):
        """Parses provided input mappings and returns a list of selected tables and schemas."""
        schemas = []
        tables = []
        output_mapping = []
        columns = {}

        for schema in input_mapping:
            # Because yaml and json mapping have different specification. YAML is an array, JSON is an object.
            # Converting this to be same as YAML mapping.
            output_mapping += [{schema: input_mapping[schema]}]
            _tables = input_mapping[schema].get('tables', [])

            if _tables == []:
                logging.warning(f"No tables specified for schema {schema}. Skipping.")
            elif isinstance(_tables, list) is False:
                logging.error(f"Tables for schema {schema} are not an array.")
                sys.exit(1)
            else:
                schemas += [schema]
                tables += [list(table.keys())[0] for table in _tables]
            for table in _tables:
                table_name = list(table.keys())[0]
                tap_stream_id = '-'.join([schema, table_name])
                desired_columns = [k for k, v in table[table_name].get('columns', {}).items() if v is True]
                columns_to_watch = table[table_name].get('columns_to_watch')
                columns_to_ignore = table[table_name].get('columns_to_ignore')
                columns[tap_stream_id] = {
                    'desired': None if desired_columns == [] else desired_columns + list(common.KBC_METADATA_COLS),
                    'watch': columns_to_watch,
                    'ignore': columns_to_ignore
                }

        logging.debug(f"Parsed following schemas: {schemas}.")
        logging.debug(f"Parsed following tables: {tables}.")

        return output_mapping, schemas, tables, columns

    @staticmethod
    def create_output_bucket(bucket_name: str = None):
        if bucket_name and bucket_name.strip() != '':
            return f'in.c-{bucket_name.strip()}'

        else:
            _component_id = os.environ.get(ENV_COMPONENT_ID)
            _configuration_id = os.environ.get(ENV_CONFIGURATION_ID)
            logging.debug(f"Env {_component_id} - {_configuration_id}")

            if _component_id is not None and _component_id is not None:
                return f"in.c-{_component_id.replace('.', '-')}-{_configuration_id}"

            else:
                return None

    # ##### SYNC ACTIONS
    @sync_action('testConnection')
    def test_connection(self):
        self._init_connection_params()
        with self.init_mysql_client() as client:
            client.ping()

    @sync_action('get_schemas')
    def get_schemas(self):
        self._init_connection_params()
        with self.init_mysql_client() as client:
            schemas = mysql.client.get_schemas(client)
            return [
                SelectElement(schema) for schema in schemas
            ]

    @sync_action('get_tables')
    def get_tables(self):
        self._init_connection_params()
        with self.init_mysql_client() as client:
            params = self.configuration.parameters
            databases = params.get(KEY_DATABASES) or params.get('source_settings', {}).get('schemas')
            if not databases:
                raise UserException("Schema must be selected first!")
            tables = mysql.client.get_tables(client, databases)
            return [SelectElement(f"{table['schema']}.{table['name']}") for table in tables]

    @sync_action("generate_ssh_key")
    def generate_ssh_key(self):
        private_key, public_key = generate_ssh_key_pair()
        md_message = f"**Private Key**  (*Copy this to the `Private Key` configuration field*):\n\n" \
                     f"```\n{private_key}\n```\n\n" \
                     f"**Public Key**  (*Add this to your servers `ssh_keys`*): \n\n```\n{public_key}\n```"

        return ValidationResult(message=md_message)

    def get_schema_changes_metadata(self):
        schema_changes_cols = ['schema', 'table', 'change_type', 'column_name', 'query', 'timestamp']
        chc_metadata = {}
        for col in schema_changes_cols:
            dtype = 'STRING' if col != 'timestamp' else 'TIMESTAMP'
            chc_metadata[col] = [{'key': 'KBC.datatype.basetype', 'value': dtype},
                                 {'key': 'KBC.datatype.nullable', 'value': True}]
        return chc_metadata


if __name__ == "__main__":
    component_start = core.utils.now()
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()

        component_end = core.utils.now()
        component_duration = (component_end - component_start).total_seconds()
        logging.info('Extraction completed successfully in {} seconds'.format(component_duration))
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as generic_err:
        logging.exception(generic_err)
        exit(1)
