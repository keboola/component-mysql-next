"""
Binary log row-based replication.
"""

import copy
import datetime
import json
import logging
import uuid
from typing import Tuple, List

import pymysql.connections
import pymysql.err
import pytz
import requests
from pymysqlreplication.constants import FIELD_TYPE
from pymysqlreplication.event import RotateEvent
from pymysqlreplication.row_event import (DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent)
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from core import bookmarks
from mysql.replication.stream_reader import BinLogStreamReaderAlterTracking, SchemaOffsyncError, \
    QueryEventWithSchemaChanges

try:
    import core as core
    from core import utils
    from core.schema import Schema

    import mysql.replication.common as common
    from mysql.client import connect_with_backoff, make_connection_wrapper
except ImportError:
    import src.core as core
    from src.core import utils
    from src.core.schema import Schema

    import src.mysql.replication.common as common
    from src.mysql.client import connect_with_backoff, make_connection_wrapper

BOOKMARK_KEYS = {'log_file', 'log_pos', 'version'}
UPDATE_BOOKMARK_PERIOD = 10000

mysql_timestamp_types = {FIELD_TYPE.TIMESTAMP, FIELD_TYPE.TIMESTAMP2}


def add_automatic_properties(catalog_entry, columns):
    catalog_entry.schema.properties[common.KBC_SYNCED] = Schema(type=["null", "string"], format="date-time")
    catalog_entry.schema.properties[common.KBC_DELETED] = Schema(type=["null", "string"], format="date-time")
    catalog_entry.schema.properties[common.BINLOG_CHANGE_AT] = Schema(type=["null", "integer"])
    catalog_entry.schema.properties[common.BINLOG_READ_AT] = Schema(type=["null", "integer"])
    columns.append(common.KBC_SYNCED)
    columns.append(common.KBC_DELETED)
    columns.append(common.BINLOG_CHANGE_AT)
    columns.append(common.BINLOG_READ_AT)

    return columns


def verify_binlog_config(mysql_conn):
    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute("SELECT  @@binlog_format")
            binlog_format = cur.fetchone()[0]

            if binlog_format != 'ROW':
                raise Exception("Unable to replicate binlog stream because binlog_format is not set to 'ROW': {}."
                                .format(binlog_format))

            try:
                cur.execute("SELECT  @@binlog_row_image")
                binlog_row_image = cur.fetchone()[0]
            except pymysql.err.InternalError as ex:
                if ex.args[0] == 1193:
                    raise Exception("Unable to replicate binlog stream because binlog_row_image system variable does"
                                    "not exist. MySQL version must be at least 5.6.2 to use binlog replication.")
                raise ex

            if binlog_row_image != 'FULL':
                raise Exception("Unable to replicate binlog stream because binlog_row_image is not set to "
                                "'FULL': {}.".format(binlog_row_image))


def verify_log_file_exists(binary_logs, log_file, log_pos):
    existing_log_file = list(filter(lambda log: log[0] == log_file, binary_logs))

    if not existing_log_file:
        raise Exception("Unable to replicate binlog stream because log file {} does not exist."
                        .format(log_file))

    current_log_pos = existing_log_file[0][1]

    if log_pos > current_log_pos:
        raise Exception("Unable to replicate binlog stream because requested position ({}) for log file {} is "
                        "greater than current position ({}).".format(log_pos, log_file, current_log_pos))


def fetch_current_log_file_and_pos(mysql_conn):
    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute("SHOW MASTER STATUS")
            result = cur.fetchone()

            if result is None:
                raise Exception("MySQL binary logging is not enabled.")
            current_log_file, current_log_pos = result[0:2]

            return current_log_file, current_log_pos


def fetch_server_id(mysql_conn):
    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute("SELECT @@server_id")
            server_id = cur.fetchone()[0]

            return server_id


def json_bytes_to_string(data):
    if isinstance(data, bytes):
        return data.decode()
    if isinstance(data, dict):
        return dict(map(json_bytes_to_string, data.items()))
    if isinstance(data, tuple):
        return tuple(map(json_bytes_to_string, data))
    if isinstance(data, list):
        return list(map(json_bytes_to_string, data))
    return data


def row_to_data_record(catalog_entry, version, db_column_map, row, time_extracted):
    row_to_persist = {}

    for column_name, val in row.items():
        db_column_type = None
        is_boolean_type = False
        try:
            # TODO: WTF is this?? aparently coming from the actual datatype name
            # property_type = catalog_entry.schema.properties[column_name].type replaced with type got from the
            # BinlogReader
            is_boolean_type = db_column_map[column_name].get('is_boolean')
            db_column_type = db_column_map[column_name].get('type')
        except KeyError:
            # skip system columns
            if column_name.startswith('_KBC') or column_name.startswith('_BINLOG'):
                pass
            else:
                raise SchemaOffsyncError(f'Schema for {column_name} is not available!')

        if isinstance(val, (datetime.datetime, datetime.date)):
            the_utc_date = common.to_utc_datetime_str(val)
            row_to_persist[column_name] = the_utc_date

        # row_event.__read_time() returns timedelta in case it is a time
        if isinstance(val, datetime.timedelta):
            row_to_persist[column_name] = str(val)

        elif db_column_type == FIELD_TYPE.JSON:
            row_to_persist[column_name] = json.dumps(json_bytes_to_string(val))
        # TODO: WTF is this??
        # elif 'boolean' in property_type or property_type == 'boolean':
        elif is_boolean_type:
            if val is None:
                boolean_representation = None
            elif val == 0:
                # boolean_representation = False
                boolean_representation = 0
            elif db_column_type == FIELD_TYPE.BIT:
                boolean_representation = 1 if int(val) != 0 else 0
                # boolean_representation = int(val) != 0
            else:
                boolean_representation = 1
                # boolean_representation = True
            row_to_persist[column_name] = boolean_representation

        else:
            row_to_persist[column_name] = val

    return core.RecordMessage(stream=catalog_entry.stream, record=row_to_persist,
                              column_map=catalog_entry.current_column_cache,
                              version=version,
                              time_extracted=time_extracted)


def get_min_log_pos_per_log_file(binlog_streams_map, state):
    min_log_pos_per_file = {}

    for tap_stream_id, bookmark in state.get('bookmarks', {}).items():
        stream = binlog_streams_map.get(tap_stream_id)

        if not stream:
            continue

        log_file = bookmark.get('log_file')
        log_pos = bookmark.get('log_pos')

        if not min_log_pos_per_file.get(log_file):
            min_log_pos_per_file[log_file] = {
                'log_pos': log_pos,
                'streams': [tap_stream_id]
            }

        elif min_log_pos_per_file[log_file]['log_pos'] > log_pos:
            min_log_pos_per_file[log_file]['log_pos'] = log_pos
            min_log_pos_per_file[log_file]['streams'].append(tap_stream_id)

        else:
            min_log_pos_per_file[log_file]['streams'].append(tap_stream_id)

    return min_log_pos_per_file


def calculate_bookmark(show_binlog_method, binlog_streams_map, state):
    min_log_pos_per_file = get_min_log_pos_per_log_file(binlog_streams_map, state)

    binary_logs = show_binlog_method()

    if binary_logs:
        server_logs_set = {log[0] for log in binary_logs}
        state_logs_set = set(min_log_pos_per_file.keys())
        expired_logs = state_logs_set.difference(server_logs_set)

        if expired_logs:
            raise Exception("Unable to replicate binlog stream because the following binary log(s) no longer "
                            "exist: {}".format(", ".join(expired_logs)))

        for log_file in sorted(server_logs_set):
            if min_log_pos_per_file.get(log_file):
                return log_file, min_log_pos_per_file[log_file]['log_pos'], binary_logs

    raise Exception("Unable to replicate binlog stream because no binary logs exist on the server.")


def update_bookmarks(state, binlog_streams_map, log_file, log_pos):
    for tap_stream_id in binlog_streams_map.keys():
        state = core.write_bookmark(state, tap_stream_id, 'log_file', log_file)
        state = core.write_bookmark(state, tap_stream_id, 'log_pos', log_pos)

    return state


def get_db_column_types(event):
    return {c.name: {"type": c.type, "is_boolean": c.type_is_bool} for c in event.columns}


def handle_write_rows_event(event, catalog_entry, state, columns, rows_saved, time_extracted,
                            message_store: core.MessageStore = None):
    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    db_column_types = get_db_column_types(event)

    for row in event.rows:
        vals = row['values']
        vals[common.KBC_DELETED] = None
        vals[common.KBC_SYNCED] = common.SYNC_STARTED_AT
        vals[common.BINLOG_CHANGE_AT] = event.timestamp
        vals[common.BINLOG_READ_AT] = utils.now(format='ts_1e6')

        if columns == [] or columns is None:
            filtered_vals = vals
        else:
            filtered_vals = {k: v for k, v in vals.items() if k in columns}

        record_message = row_to_data_record(catalog_entry, stream_version, db_column_types, filtered_vals,
                                            time_extracted)
        core.write_message(record_message, message_store=message_store, database_schema=catalog_entry.database)
        rows_saved = rows_saved + 1

    return rows_saved


def handle_update_rows_event(event, catalog_entry, state, columns, rows_saved, time_extracted, watch_columns=None,
                             ignore_columns=None, message_store: core.MessageStore = None):
    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    db_column_types = get_db_column_types(event)

    for row in event.rows:

        changed = False
        if ignore_columns is not None and ignore_columns != []:
            all_cols = list(row['after_values'].keys())

            for wc in all_cols:
                if wc in ignore_columns:
                    continue
                else:
                    before_value = row['before_values'].get(wc)
                    after_value = row['after_values'].get(wc)

                    if before_value != after_value:
                        changed = True
                        break

        elif watch_columns is not None and watch_columns != []:
            for wc in watch_columns:
                before_value = row['before_values'].get(wc)
                after_value = row['after_values'].get(wc)

                if before_value != after_value:
                    changed = True
                    break

        else:
            changed = True

        if changed is False:
            pass
        else:
            vals = row['after_values']

            vals[common.KBC_DELETED] = None
            vals[common.KBC_SYNCED] = common.SYNC_STARTED_AT
            vals[common.BINLOG_CHANGE_AT] = event.timestamp
            vals[common.BINLOG_READ_AT] = utils.now(format='ts_1e6')

            if columns == [] or columns is None:
                filtered_vals = vals
            else:
                filtered_vals = {k: v for k, v in vals.items() if k in columns}

            record_message = row_to_data_record(catalog_entry, stream_version, db_column_types, filtered_vals,
                                                time_extracted)

            core.write_message(record_message, message_store=message_store, database_schema=catalog_entry.database)

            rows_saved += 1

    return rows_saved


def handle_delete_rows_event(event, catalog_entry, state, columns, rows_saved, time_extracted,
                             message_store: core.MessageStore = None):
    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    db_column_types = get_db_column_types(event)

    for row in event.rows:
        event_ts = datetime.datetime.utcfromtimestamp(event.timestamp).replace(tzinfo=pytz.UTC)
        vals = row['values']

        vals[common.KBC_DELETED] = event_ts
        vals[common.KBC_SYNCED] = common.SYNC_STARTED_AT
        vals[common.BINLOG_CHANGE_AT] = event.timestamp
        vals[common.BINLOG_READ_AT] = utils.now(format='ts_1e6')

        if columns == [] or columns is None:
            filtered_vals = vals
        else:
            filtered_vals = {k: v for k, v in vals.items() if k in columns}

        record_message = row_to_data_record(catalog_entry, stream_version, db_column_types, filtered_vals,
                                            time_extracted)

        core.write_message(record_message, message_store=message_store, database_schema=catalog_entry.database)

        rows_saved = rows_saved + 1

    return rows_saved


def generate_streams_map(binlog_streams):
    stream_map = {}

    for catalog_entry in binlog_streams:
        columns = add_automatic_properties(catalog_entry,
                                           list(catalog_entry.schema.properties.keys()))

        stream_map[catalog_entry.tap_stream_id] = {
            'catalog_entry': catalog_entry,
            'desired_columns': columns
        }

    return stream_map


def handle_schema_change_event(binlog_event: QueryEventWithSchemaChanges, message_store):
    if binlog_event.event_type != QueryEventWithSchemaChanges.QueryType.ALTER_QUERY:
        return

    for change in binlog_event.schema_changes:
        row = {'schema': change.schema,
               'table': change.table_name,
               'change_type': change.type.value,
               'column_name': change.column_name,
               'query': change.query,
               'timestamp': str(binlog_event.timestamp)}
        message_store.write_schema_change_message(row)


def _run_binlog_sync(mysql_conn, reader: BinLogStreamReaderAlterTracking, binlog_streams_map, state, columns={},
                     message_store: core.MessageStore = None):
    time_extracted = utils.now()

    rows_saved = 0
    events_skipped = 0

    current_log_file, current_log_pos = fetch_current_log_file_and_pos(mysql_conn)
    parsing_log_file = ''
    parsing_log_pos = current_log_pos

    for binlog_event in reader:

        if parsing_log_file != reader.log_file:
            parsing_log_file = reader.log_file
            parsing_log_pos = reader.log_pos
            logging.info("Parsing first 50M records in binary logs file.")

        if reader.log_pos - parsing_log_pos > 50000000:
            parsing_log_pos = reader.log_pos
            logging.info(f"Parsing binary another 50M records in logs file {parsing_log_file}, "
                         f"starting position {parsing_log_pos}.")

        if isinstance(binlog_event, RotateEvent):
            state = update_bookmarks(state, binlog_streams_map, binlog_event.next_binlog, binlog_event.position)

        elif isinstance(binlog_event, QueryEventWithSchemaChanges):
            handle_schema_change_event(binlog_event, message_store)

        else:
            tap_stream_id = common.generate_tap_stream_id(binlog_event.schema, binlog_event.table)
            streams_map_entry = binlog_streams_map.get(tap_stream_id, {})
            catalog_entry = streams_map_entry.get('catalog_entry')

            desired_columns = columns[tap_stream_id].get('desired', [])
            ignored_columns = columns[tap_stream_id].get('ignore', [])
            watched_columns = columns[tap_stream_id].get('watch', [])

            if not catalog_entry:
                logging.debug('No catalog entry, skip events number: {}'.format(events_skipped))
                events_skipped = events_skipped + 1

                if events_skipped % UPDATE_BOOKMARK_PERIOD == 0:
                    logging.info("Skipped %s events so far as they were not for selected tables; %s rows extracted",
                                 events_skipped, rows_saved)

            elif catalog_entry:
                # ugly injection of current schema
                current_column_schema = reader.schema_cache.get_column_schema(binlog_event.schema, binlog_event.table)
                catalog_entry.current_column_cache = current_column_schema

                if isinstance(binlog_event, WriteRowsEvent):
                    rows_saved = handle_write_rows_event(binlog_event, catalog_entry, state, desired_columns,
                                                         rows_saved, time_extracted, message_store=message_store)

                elif isinstance(binlog_event, UpdateRowsEvent):
                    rows_saved = handle_update_rows_event(binlog_event, catalog_entry, state, desired_columns,
                                                          rows_saved, time_extracted,
                                                          watch_columns=watched_columns, ignore_columns=ignored_columns,
                                                          message_store=message_store)

                elif isinstance(binlog_event, DeleteRowsEvent):
                    rows_saved = handle_delete_rows_event(binlog_event, catalog_entry, state, desired_columns,
                                                          rows_saved, time_extracted, message_store=message_store)
                else:
                    logging.info("Skipping event for table %s.%s as it is not an INSERT, UPDATE, or DELETE",
                                 binlog_event.schema,
                                 binlog_event.table)

        state = update_bookmarks(state, binlog_streams_map, reader.log_file, reader.log_pos)
        # Store last schema cache
        state = bookmarks.update_schema_in_state(state, reader.schema_cache.table_schema_cache)

        # The iterator across python-mysql-replication's fetchone method should ultimately terminate
        # upon receiving an EOF packet. There seem to be some cases when a MySQL server will not send
        # one causing binlog replication to hang.

        # TODO: Consider moving log pos back slightly to avoid hanging process (maybe 200 or so)
        if current_log_file == reader.log_file and reader.log_pos >= current_log_pos:
            break

        if ((rows_saved and rows_saved % UPDATE_BOOKMARK_PERIOD == 0) or
                (events_skipped and events_skipped % UPDATE_BOOKMARK_PERIOD == 0)):
            core.write_message(core.StateMessage(value=copy.deepcopy(state)), message_store=message_store)


class ShowBinlogMethodFactory:
    """
    SHOW BINLOG may be slow, this supports other methods of binlog retrieval
    """

    def __init__(self, mysql_connection, method_configuration: dict):
        self._mysql_conn = mysql_connection
        self._configuration = method_configuration

    def get_show_binlog_method(self):
        method_config = self._configuration
        if method_config.get('method', 'direct') == 'direct':
            show_binlog_method = self._show_binlog_from_db
        elif method_config.get('method') == 'endpoint':
            show_binlog_method = self._show_binlog_from_endpoint
        else:
            raise ValueError(f'Provided show_binlog_method: {method_config} is invalid')
        return show_binlog_method

    def _show_binlog_from_db(self) -> dict:
        with connect_with_backoff(self._mysql_conn) as open_conn:
            with open_conn.cursor() as cur:
                logging.debug('Executing SHOW BINARY LOGS')
                cur.execute("SHOW BINARY LOGS")

                binary_logs = cur.fetchall()
                return binary_logs

    def _show_binlog_from_endpoint(self) -> List[Tuple]:
        """
        Expects endpoint returning SHOW BINLOGS array in
        {"logs":[{'log_name': 'mysql-bin-changelog.189135', 'file_size': '134221723'}]} response

        Returns:

        """

        def get_session(max_retries: int = 3, backoff_factor: float = 0.3,
                        status_forcelist: Tuple[int, ...] = (500, 502, 503, 504)) -> requests.Session:
            session = requests.Session()
            retry = Retry(
                total=max_retries,
                read=max_retries,
                connect=max_retries,
                backoff_factor=backoff_factor,
                status_forcelist=status_forcelist,
                allowed_methods='GET'
            )
            adapter = HTTPAdapter(max_retries=retry)
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            return session

        endpoint_url = self._configuration.get('endpoint_url')
        auth = None
        if self._configuration.get('authentication'):
            auth = (self._configuration['user'], self._configuration['#password'])

        if not endpoint_url:
            raise ValueError(f'Show binlog method from endpoint requires "endpoint_url" parameters defined! '
                             f'Provided configuration is invalid: {self._configuration}.')
        logging.info(f"Getting SHOW Binary logs from {endpoint_url} endpoint")
        response = get_session().get(endpoint_url, auth=auth, timeout=300)
        response.raise_for_status()
        log_array = response.json()['logs']
        binlogs = [(lg['log_name'], int(lg['file_size'])) for lg in log_array]
        return binlogs


def sync_binlog_stream(mysql_conn, config, binlog_streams, state, message_store: core.MessageStore = None,
                       schemas=[], tables=[], columns={}):
    last_table_schema_cache = state.get(bookmarks.KEY_LAST_TABLE_SCHEMAS, {})
    binlog_streams_map = generate_streams_map(binlog_streams)

    for tap_stream_id in binlog_streams_map.keys():
        common.whitelist_bookmark_keys(BOOKMARK_KEYS, tap_stream_id, state)

    # build show binary log method
    shbn_factory = ShowBinlogMethodFactory(mysql_conn, config.get('show_binary_log_config', {}))
    show_binlog_method = shbn_factory.get_show_binlog_method()

    log_file, log_pos, binary_logs = calculate_bookmark(show_binlog_method, binlog_streams_map, state)

    verify_log_file_exists(binary_logs, log_file, log_pos)

    if config.get('server_id'):
        server_id = int(config.get('server_id'))
        logging.info("Using provided server_id=%s", server_id)
    else:
        server_id = fetch_server_id(mysql_conn)
        logging.info("No server_id provided, will use global server_id=%s", server_id)

    connection_wrapper = make_connection_wrapper(config)

    slave_uuid = 'kbc-slave-{}-{}'.format(str(uuid.uuid4()), server_id)
    logging.info('Connecting with Stream Reader to Slave UUID {}'.format(slave_uuid))
    try:
        reader = BinLogStreamReaderAlterTracking(
            connection_settings={},
            server_id=server_id,
            slave_uuid=slave_uuid,
            log_file=log_file,
            log_pos=log_pos,
            resume_stream=True,
            only_events=[RotateEvent, WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, QueryEventWithSchemaChanges],
            freeze_schema=True,
            pymysql_wrapper=connection_wrapper,
            only_schemas=schemas,
            only_tables=tables,
            table_schema_cache=last_table_schema_cache
        )

        logging.info("Starting binlog replication with log_file=%s, log_pos=%s", log_file, log_pos)
        _run_binlog_sync(mysql_conn, reader, binlog_streams_map, state, columns, message_store=message_store)
    except Exception as e:
        logging.exception(e)
        raise e
    finally:
        # BinLogStreamReader doesn't implement the `with` methods
        # So, try/finally will close the chain from the top
        reader.close()

    core.write_message(core.StateMessage(value=copy.deepcopy(state)), message_store=message_store)
