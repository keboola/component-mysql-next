"""
Common patterns for data replication.
"""
import copy
import csv
import datetime
import logging
import os
import time

import pymysql
import pytz


try:
    import core as core
    import core.metrics as metrics
    from core import metadata
    from core import utils
    from core.messages import handle_binary_data
except ImportError:
    import src.core as core
    import src.core.metrics as metrics
    from src.core import metadata
    from src.core import utils
    from src.core.messages import handle_binary_data

CURRENT_PATH = os.path.dirname(__file__)

# NB: Upgrading pymysql from 0.7.11 --> 0.9.3 had the undocumented change
# to how `0000-00-00 00:00:00` date/time types are returned. In 0.7.11,
# they are returned as NULL, and in 0.9.3, they are returned as the string
# `0000-00-00 00:00:00`. To maintain backwards-compatability, we are
# monkey patching the functions so they continue returning None
original_convert_datetime = pymysql.converters.convert_datetime
original_convert_date = pymysql.converters.convert_date

CSV_CHUNK_SIZE = 100000
SYNC_STARTED_AT = datetime.datetime.utcnow().strftime(utils.DATETIME_FMT_SAFE)
KBC_SYNCED = '_KBC_SYNCED_AT'
KBC_DELETED = '_KBC_DELETED_AT'
BINLOG_CHANGE_AT = '_BINLOG_CHANGE_AT'
BINLOG_READ_AT = '_BINLOG_READ_AT'
KBC_METADATA_COLS = (KBC_SYNCED, KBC_DELETED, BINLOG_CHANGE_AT, BINLOG_READ_AT)
KBC_METADATA = (SYNC_STARTED_AT, None, 0, 0)


def now():
    return time.time()


def patch_datetime(datetime_str):
    value = original_convert_datetime(datetime_str)
    if datetime_str == value:
        return None
    return value


def patch_date(date_str):
    value = original_convert_date(date_str)
    if date_str == value:
        return None
    return value


# Patch converters to properly handle date representations as strings (i.e. 0/0/0000)
pymysql.converters.convert_datetime = patch_datetime
pymysql.converters.convert_date = patch_date

pymysql.converters.conversions[pymysql.constants.FIELD_TYPE.DATETIME] = patch_datetime
pymysql.converters.conversions[pymysql.constants.FIELD_TYPE.DATE] = patch_date


def escape(string):
    if '`' in string:
        raise Exception("Can't escape identifier {} because it contains a backtick"
                        .format(string))
    return '`' + string + '`'


def generate_tap_stream_id(table_schema, table_name):
    return table_schema + '-' + table_name


def get_stream_version(tap_stream_id, state):
    stream_version = core.get_bookmark(state, tap_stream_id, 'version')

    if stream_version is None:
        stream_version = int(time.time() * 1000)

    return stream_version


def stream_is_selected(stream):
    md_map = metadata.to_map(stream.metadata)
    selected_md = metadata.get(md_map, (), 'selected')

    return selected_md


def property_is_selected(stream, property_name):
    md_map = metadata.to_map(stream.metadata)
    return core.should_sync_field(
        metadata.get(md_map, ('properties', property_name), 'inclusion'),
        metadata.get(md_map, ('properties', property_name), 'selected'),
        True)


def get_is_view(catalog_entry):
    md_map = metadata.to_map(catalog_entry.metadata)

    return md_map.get((), {}).get('is-view')


def get_database_name(catalog_entry):
    md_map = metadata.to_map(catalog_entry.metadata)

    return md_map.get((), {}).get('database-name')


def get_key_properties(catalog_entry):
    catalog_metadata = metadata.to_map(catalog_entry.metadata)
    stream_metadata = catalog_metadata.get((), {})

    is_view = get_is_view(catalog_entry)

    if is_view:
        key_properties = stream_metadata.get('view-key-properties', [])
    else:
        key_properties = stream_metadata.get('table-key-properties', [])

    return key_properties


def generate_select_sql(catalog_entry, columns):
    database_name = get_database_name(catalog_entry)
    escaped_db = escape(database_name)
    escaped_table = escape(catalog_entry.table)
    escaped_columns = [escape(c) for c in columns]

    select_sql = 'SELECT {} FROM {}.{}'.format(
        ','.join(escaped_columns),
        escaped_db,
        escaped_table)

    # escape percent signs
    select_sql = select_sql.replace('%', '%%')
    return select_sql


def to_utc_datetime_str(val):
    if isinstance(val, datetime.datetime):
        the_datetime = val
    elif isinstance(val, datetime.date):
        # the_datetime = datetime.datetime.combine(val, datetime.datetime.min.time())
        return val.strftime('%Y-%m-%d')

    elif isinstance(val, datetime.timedelta):
        epoch = datetime.datetime.utcfromtimestamp(0)
        the_datetime = epoch + val

    else:
        raise ValueError("{!r} is not a valid date or time type".format(val))

    if the_datetime.tzinfo is None:
        # The mysql-replication library creates naive date and datetime objects
        # which will use the local timezone thus we must set tzinfo accordingly
        # See: https://github.com/noplay/python-mysql-replication/blob/master/pymysqlreplication/row_event.py#L143-L145

        # NB> this code will only work correctly when the local time is set to UTC because of the method timestamp()
        the_datetime = datetime.datetime.fromtimestamp(the_datetime.timestamp(), pytz.timezone('UTC'))

    return utils.strftime(the_datetime.astimezone(tz=pytz.UTC))


def row_to_data_record(catalog_entry, version, row, columns, time_extracted):
    # Adding metadata for Keboola. Can presume not there since should only be called for full sync

    kbc_metadata = (SYNC_STARTED_AT, None, 0, 0)
    kbc_metadata_cols = (KBC_SYNCED, KBC_DELETED, BINLOG_CHANGE_AT, BINLOG_READ_AT)
    row_with_metadata = row + kbc_metadata
    columns.extend(kbc_metadata_cols)
    catalog_entry.schema.properties[KBC_SYNCED] = core.schema.Schema(type=["null", "string"], format="date-time")
    catalog_entry.schema.properties[KBC_DELETED] = core.schema.Schema(type=["null", "string"], format="date-time")
    catalog_entry.schema.properties[BINLOG_CHANGE_AT] = core.schema.Schema(type=["null", "integer"])
    catalog_entry.schema.properties[BINLOG_READ_AT] = core.schema.Schema(type=["null", "integer"])

    row_to_persist = ()
    for idx, elem in enumerate(row_with_metadata):
        property_type = catalog_entry.schema.properties[columns[idx]].type

        if isinstance(elem, (datetime.datetime, datetime.date, datetime.timedelta)):
            the_utc_date = to_utc_datetime_str(elem)
            row_to_persist += (the_utc_date,)

        elif isinstance(elem, bytes):
            # for BIT value, treat 0 as False and anything else as True
            boolean_representation = elem != b'\x00'
            row_to_persist += (boolean_representation,)

        elif 'boolean' in property_type or property_type == 'boolean':
            if elem is None:
                boolean_representation = None
            elif elem == 0:
                boolean_representation = False
            else:
                boolean_representation = True
            row_to_persist += (boolean_representation,)

        else:
            row_to_persist += (elem,)
    rec = dict(zip(columns, row_to_persist))

    # rec[KBC_DELETED] = None
    # rec[KBC_SYNCED] = utils.now()

    return core.RecordMessage(stream=catalog_entry.stream, record=rec, version=version, time_extracted=time_extracted)


def whitelist_bookmark_keys(bookmark_key_set, tap_stream_id, state):
    for bk in [non_whitelisted_bookmark_key
               for non_whitelisted_bookmark_key
               in state.get('bookmarks', {}).get(tap_stream_id, {}).keys()
               if non_whitelisted_bookmark_key not in bookmark_key_set]:
        core.clear_bookmark(state, tap_stream_id, bk)


def sync_query(cursor, catalog_entry, state, select_sql, columns, stream_version, params,
               message_store: core.MessageStore = None):
    replication_key = core.get_bookmark(state, catalog_entry.tap_stream_id, 'replication_key')

    query_string = cursor.mogrify(select_sql, params)
    time_extracted = utils.now()

    logging.info('Running %s', query_string)
    cursor.execute(select_sql, params)

    row = cursor.fetchone()
    rows_saved = 0

    database_name = get_database_name(catalog_entry)

    with metrics.record_counter(None) as counter:
        counter.tags['database'] = database_name
        counter.tags['table'] = catalog_entry.table

        while row:
            counter.increment()
            rows_saved += 1

            record_message = row_to_data_record(catalog_entry, stream_version, row, columns, time_extracted)
            core.write_message(record_message)

            md_map = metadata.to_map(catalog_entry.metadata)
            stream_metadata = md_map.get((), {})
            replication_method = stream_metadata.get('replication-method')

            if replication_method.upper() in {'FULL_TABLE', 'LOG_BASED'}:
                key_properties = get_key_properties(catalog_entry)

                max_pk_values = core.get_bookmark(state, catalog_entry.tap_stream_id, 'max_pk_values')

                if max_pk_values:
                    last_pk_fetched = {k: v for k, v in record_message.record.items()
                                       if k in key_properties}

                    state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'last_pk_fetched', last_pk_fetched)

            elif replication_method.upper() == 'INCREMENTAL':
                if replication_key is not None:
                    state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'replication_key', replication_key)

                    state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'replication_key_value',
                                                record_message.record[replication_key])

            if rows_saved % 1000 == 0:
                core.write_message(core.StateMessage(value=copy.deepcopy(state)), message_store=message_store)
                if rows_saved % 1000000 == 0:
                    logging.info('Ingested row count has hit {} for table {}'.format(rows_saved,
                                                                                     catalog_entry.tap_stream_id))

            row = cursor.fetchone()

    core.write_message(core.StateMessage(value=copy.deepcopy(state)), message_store=message_store)
    return rows_saved


def _add_kbc_metadata_to_rows(rows, catalog_entry):
    """Add metadata for Keboola. Can presume not there since should only be called for full sync."""
    # Add headers to schema definition (data is added in write step)
    catalog_entry.schema.properties[KBC_SYNCED] = core.schema.Schema(type=["null", "string"], format="date-time")
    catalog_entry.schema.properties[KBC_DELETED] = core.schema.Schema(type=["null", "string"], format="date-time")
    catalog_entry.schema.properties[BINLOG_CHANGE_AT] = core.schema.Schema(type=["null", "integer"])
    catalog_entry.schema.properties[BINLOG_READ_AT] = core.schema.Schema(type=["null", "integer"])

    return rows


def sync_query_bulk(conn, cursor: pymysql.cursors.Cursor, catalog_entry, state, select_sql, columns, stream_version,
                    params, tables_destination: str = None, message_store: core.MessageStore = None):
    replication_key = core.get_bookmark(state, catalog_entry.tap_stream_id, 'replication_key')  # noqa

    query_string = cursor.mogrify(select_sql, params)
    logging.info('Running query {}'.format(query_string))

    # Chunk Processing
    has_more_data = True
    current_chunk = 0

    logging.info('Starting chunk processing for stream {}'.format(catalog_entry.tap_stream_id))
    all_chunks_start_time = utils.now()

    try:
        cursor.execute(select_sql)
        while has_more_data:
            chunk_start = utils.now()
            current_chunk += 1

            query_output_rows = cursor.fetchmany(CSV_CHUNK_SIZE)

            if query_output_rows:
                number_of_rows = len(query_output_rows)

                # Add Keboola metadata columns
                rows = _add_kbc_metadata_to_rows(query_output_rows, catalog_entry)
                logging.debug('Fetched {} rows from query result'.format(number_of_rows))

                if current_chunk == 1:
                    table_and_headers = {}

                    # Fetch column names from first item in cursor description tuple
                    headers = list()
                    for i in cursor.description:
                        headers.append(i[0])
                    for column in KBC_METADATA_COLS:
                        headers.append(column)

                    table_and_headers[catalog_entry.table] = headers

                    # Write to CSV of specific structure: table, headers (no header is written to this CSV)
                    tables_headers_path = os.path.join(CURRENT_PATH, '..', '..', '')
                    with open(tables_headers_path + 'table_headers.csv', 'a+') as headers_csv:
                        writer = csv.writer(headers_csv, delimiter='\t')
                        writer.writerow([catalog_entry.table, headers])
                        logging.info('Setting table {} metadata for columns to {}, staged for manifest'.format(
                            catalog_entry.table, headers
                        ))

                destination_output_path = os.path.join(tables_destination, catalog_entry.table.upper() + '.csv', '')

                if not os.path.exists(destination_output_path):
                    os.mkdir(destination_output_path)

                csv_path = os.path.join(destination_output_path, catalog_entry.table.upper() + '-' +
                                        str(current_chunk) + '.csv')

                with open(csv_path, 'w', encoding='utf-8') as output_data_file:
                    writer = csv.DictWriter(output_data_file, fieldnames=headers, quoting=csv.QUOTE_MINIMAL)
                    for row in rows:
                        rows_with_metadata = row + KBC_METADATA
                        rows_dict = dict(zip(headers, rows_with_metadata))
                        rows_to_write = handle_binary_data(rows_dict, catalog_entry.binary_columns,
                                                           message_store.binary_data_handler, True)
                        writer.writerow(rows_to_write)

                chunk_end = utils.now()
                chunk_processing_duration = (chunk_end - chunk_start).total_seconds()
                logging.info(
                    'Chunk {} had processing time: {} seconds'.format(current_chunk, chunk_processing_duration))

            else:
                has_more_data = False

    except Exception:
        logging.error('Failed to execute query {}'.format(query_string))
        raise

    logging.info('Finished chunk processing for stream {}'.format(catalog_entry.tap_stream_id))

    all_chunks_end_time = utils.now()
    full_chunk_processing_duration = (all_chunks_end_time - all_chunks_start_time).total_seconds()
    logging.info('Total processing time: {} seconds'.format(full_chunk_processing_duration))

    core.write_message(core.StateMessage(value=copy.deepcopy(state)), message_store=message_store)
