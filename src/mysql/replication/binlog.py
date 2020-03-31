"""
Binary log row-based replication.
"""
import copy
import datetime
import json
import pytz
import socket

import pymysql.connections
import pymysql.err

from pymysqlreplication.constants import FIELD_TYPE
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import RotateEvent
from pymysqlreplication.row_event import (DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent)

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

LOGGER = core.get_logger()
socket.setdefaulttimeout(5.0)

BOOKMARK_KEYS = {'log_file', 'log_pos', 'version'}
UPDATE_BOOKMARK_PERIOD = 1000

mysql_timestamp_types = {FIELD_TYPE.TIMESTAMP, FIELD_TYPE.TIMESTAMP2}


def add_automatic_properties(catalog_entry, columns):
    catalog_entry.schema.properties[common.KBC_SYNCED] = Schema(type=["null", "string"], format="date-time")
    catalog_entry.schema.properties[common.KBC_DELETED] = Schema(type=["null", "string"], format="date-time")
    columns.append(common.KBC_SYNCED)
    columns.append(common.KBC_DELETED)

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


def verify_log_file_exists(mysql_conn, log_file, log_pos):
    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute("SHOW BINARY LOGS")
            result = cur.fetchall()

            existing_log_file = list(filter(lambda log: log[0] == log_file, result))

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


def row_to_singer_record(catalog_entry, version, db_column_map, row, time_extracted):
    row_to_persist = {}

    for column_name, val in row.items():
        property_type = catalog_entry.schema.properties[column_name].type
        db_column_type = db_column_map.get(column_name)

        if isinstance(val, (datetime.datetime, datetime.date, datetime.timedelta)):
            the_utc_date = common.to_utc_datetime_str(val)
            row_to_persist[column_name] = the_utc_date

        elif db_column_type == FIELD_TYPE.JSON:
            row_to_persist[column_name] = json.dumps(json_bytes_to_string(val))

        elif 'boolean' in property_type or property_type == 'boolean':
            if val is None:
                boolean_representation = None
            elif val == 0:
                boolean_representation = False
            elif db_column_type == FIELD_TYPE.BIT:
                boolean_representation = int(val) != 0
            else:
                boolean_representation = True
            row_to_persist[column_name] = boolean_representation

        else:
            row_to_persist[column_name] = val

    return core.RecordMessage(
        stream=catalog_entry.stream,
        record=row_to_persist,
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


def calculate_bookmark(mysql_conn, binlog_streams_map, state):
    min_log_pos_per_file = get_min_log_pos_per_log_file(binlog_streams_map, state)

    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute("SHOW BINARY LOGS")

            binary_logs = cur.fetchall()

            if binary_logs:
                server_logs_set = {log[0] for log in binary_logs}
                state_logs_set = set(min_log_pos_per_file.keys())
                expired_logs = state_logs_set.difference(server_logs_set)

                if expired_logs:
                    raise Exception("Unable to replicate binlog stream because the following binary log(s) no longer "
                                    "exist: {}".format(", ".join(expired_logs)))

                for log_file in sorted(server_logs_set):
                    if min_log_pos_per_file.get(log_file):
                        return log_file, min_log_pos_per_file[log_file]['log_pos']

            raise Exception("Unable to replicate binlog stream because no binary logs exist on the server.")


def update_bookmarks(state, binlog_streams_map, log_file, log_pos):
    for tap_stream_id in binlog_streams_map.keys():
        state = core.write_bookmark(state, tap_stream_id, 'log_file', log_file)
        state = core.write_bookmark(state, tap_stream_id, 'log_pos', log_pos)

    return state


def get_db_column_types(event):
    return {c.name: c.type for c in event.columns}


def handle_write_rows_event(event, catalog_entry, state, columns, rows_saved, time_extracted):
    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    db_column_types = get_db_column_types(event)

    for row in event.rows:
        event_ts = datetime.datetime.utcfromtimestamp(event.timestamp).replace(tzinfo=pytz.UTC)
        vals = row['values']
        vals[common.KBC_DELETED] = None
        vals[common.KBC_SYNCED] = event_ts
        filtered_vals = {k: v for k, v in vals.items() if k in columns}

        record_message = row_to_singer_record(catalog_entry, stream_version, db_column_types, filtered_vals,
                                              time_extracted)

        core.write_message(record_message)
        rows_saved = rows_saved + 1

    return rows_saved


def handle_update_rows_event(event, catalog_entry, state, columns, rows_saved, time_extracted):
    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    db_column_types = get_db_column_types(event)

    for row in event.rows:
        event_ts = datetime.datetime.utcfromtimestamp(event.timestamp).replace(tzinfo=pytz.UTC)
        vals = row['after_values']

        vals[common.KBC_DELETED] = None
        vals[common.KBC_SYNCED] = event_ts
        filtered_vals = {k: v for k, v in vals.items() if k in columns}

        record_message = row_to_singer_record(catalog_entry,
                                              stream_version,
                                              db_column_types,
                                              filtered_vals,
                                              time_extracted)

        core.write_message(record_message)

        rows_saved += 1

    return rows_saved


def handle_delete_rows_event(event, catalog_entry, state, columns, rows_saved, time_extracted):
    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    db_column_types = get_db_column_types(event)

    for row in event.rows:
        event_ts = datetime.datetime.utcfromtimestamp(event.timestamp).replace(tzinfo=pytz.UTC)
        vals = row['values']

        vals[common.KBC_DELETED] = event_ts
        vals[common.KBC_SYNCED] = event_ts

        filtered_vals = {k: v for k, v in vals.items() if k in columns}

        record_message = row_to_singer_record(catalog_entry, stream_version, db_column_types, filtered_vals,
                                              time_extracted)

        core.write_message(record_message)

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


def _run_binlog_sync(mysql_conn, reader, binlog_streams_map, state):
    time_extracted = utils.now()

    rows_saved = 0
    events_skipped = 0

    current_log_file, current_log_pos = fetch_current_log_file_and_pos(mysql_conn)

    binlogs_processed = 0
    for binlog_event in reader:
        binlogs_processed += 1

        if isinstance(binlog_event, RotateEvent):
            # print('Rotation event...')
            state = update_bookmarks(state, binlog_streams_map, binlog_event.next_binlog, binlog_event.position)
        else:
            tap_stream_id = common.generate_tap_stream_id(binlog_event.schema, binlog_event.table)
            streams_map_entry = binlog_streams_map.get(tap_stream_id, {})
            catalog_entry = streams_map_entry.get('catalog_entry')
            desired_columns = streams_map_entry.get('desired_columns')

            if not catalog_entry:
                LOGGER.debug('No catalog entry, skip events number: {}'.format(events_skipped))
                events_skipped = events_skipped + 1

                if events_skipped % UPDATE_BOOKMARK_PERIOD == 0:
                    LOGGER.info("Skipped %s events so far as they were not for selected tables; %s rows extracted",
                                events_skipped, rows_saved)

            elif catalog_entry:
                if isinstance(binlog_event, WriteRowsEvent):
                    rows_saved = handle_write_rows_event(binlog_event, catalog_entry, state, desired_columns,
                                                         rows_saved, time_extracted)

                elif isinstance(binlog_event, UpdateRowsEvent):
                    # LOGGER.info('Handling update row event')
                    rows_saved = handle_update_rows_event(binlog_event, catalog_entry, state, desired_columns,
                                                          rows_saved, time_extracted)

                elif isinstance(binlog_event, DeleteRowsEvent):
                    rows_saved = handle_delete_rows_event(binlog_event, catalog_entry, state, desired_columns,
                                                          rows_saved, time_extracted)
                else:
                    LOGGER.info("Skipping event for table %s.%s as it is not an INSERT, UPDATE, or DELETE",
                                binlog_event.schema,
                                binlog_event.table)

        state = update_bookmarks(state, binlog_streams_map, reader.log_file, reader.log_pos)

        # The iterator across python-mysql-replication's fetchone method should ultimately terminate
        # upon receiving an EOF packet. There seem to be some cases when a MySQL server will not send
        # one causing binlog replication to hang.

        # TODO: Consider moving log pos back slightly to avoid hanging process (maybe 200 or so)
        if current_log_file == reader.log_file and reader.log_pos >= current_log_pos:
            break

        if ((rows_saved and rows_saved % UPDATE_BOOKMARK_PERIOD == 0) or
                (events_skipped and events_skipped % UPDATE_BOOKMARK_PERIOD == 0)):
            core.write_message(core.StateMessage(value=copy.deepcopy(state)))


def sync_binlog_stream(mysql_conn, config, binlog_streams, state):
    binlog_streams_map = generate_streams_map(binlog_streams)

    for tap_stream_id in binlog_streams_map.keys():
        common.whitelist_bookmark_keys(BOOKMARK_KEYS, tap_stream_id, state)

    log_file, log_pos = calculate_bookmark(mysql_conn, binlog_streams_map, state)

    verify_log_file_exists(mysql_conn, log_file, log_pos)

    if config.get('server_id'):
        server_id = int(config.get('server_id'))
        LOGGER.info("Using provided server_id=%s", server_id)
    else:
        server_id = fetch_server_id(mysql_conn)
        LOGGER.info("No server_id provided, will use global server_id=%s", server_id)

    connection_wrapper = make_connection_wrapper(config)
    # LOGGER.info('Defining wrapper details: {}; {}; {}; {}'.format(connection_wrapper.host, connection_wrapper.user,
    #                                                           connection_wrapper.password,
    #                                                           connection_wrapper.bind_address))

    try:
        reader = BinLogStreamReader(
            connection_settings={},
            server_id=server_id,
            slave_uuid='kbc-slave-{}'.format(server_id),
            log_file=log_file,
            log_pos=log_pos,
            resume_stream=True,
            only_events=[RotateEvent, WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
            freeze_schema=True,
            pymysql_wrapper=connection_wrapper
        )
        LOGGER.info("Starting binlog replication with log_file=%s, log_pos=%s", log_file, log_pos)
        _run_binlog_sync(mysql_conn, reader, binlog_streams_map, state)
    finally:
        # BinLogStreamReader doesn't implement the `with` methods
        # So, try/finally will close the chain from the top
        reader.close()

    core.write_message(core.StateMessage(value=copy.deepcopy(state)))
