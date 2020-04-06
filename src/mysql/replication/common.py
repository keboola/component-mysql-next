"""
Common patterns for data replication.
"""
import copy
import csv
import datetime
import logging
import os
import time

import pandas as pd
import pytz
import pymysql

try:
    import core as core
    import core.metrics as metrics
    from core import metadata
    from core import utils
except ImportError:
    import src.core as core
    import src.core.metrics as metrics
    from src.core import metadata
    from src.core import utils

LOGGER = logging.getLogger(__name__)
CURRENT_PATH = os.path.dirname(__file__)

# NB: Upgrading pymysql from 0.7.11 --> 0.9.3 had the undocumented change
# to how `0000-00-00 00:00:00` date/time types are returned. In 0.7.11,
# they are returned as NULL, and in 0.9.3, they are returned as the string
# `0000-00-00 00:00:00`. To maintain backwards-compatability, we are
# monkey patching the functions so they continue returning None
original_convert_datetime = pymysql.converters.convert_datetime
original_convert_date = pymysql.converters.convert_date

CSV_CHUNK_SIZE = 250000
SYNC_STARTED_AT = datetime.datetime.utcnow().isoformat()
KBC_SYNCED = '_KBC_SYNCED_AT'
KBC_DELETED = '_KBC_DELETED_AT'


def monkey_patch_datetime(datetime_str):
    value = original_convert_datetime(datetime_str)
    if datetime_str == value:
        return None
    return value


def monkey_patch_date(date_str):
    value = original_convert_date(date_str)
    if date_str == value:
        return None
    return value


pymysql.converters.convert_datetime = monkey_patch_datetime
pymysql.converters.convert_date = monkey_patch_date

pymysql.converters.conversions[pymysql.constants.FIELD_TYPE.DATETIME] = monkey_patch_datetime
pymysql.converters.conversions[pymysql.constants.FIELD_TYPE.DATE] = monkey_patch_date


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
        the_datetime = datetime.datetime.combine(val, datetime.datetime.min.time())

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

    kbc_metadata = (SYNC_STARTED_AT, None)
    kbc_metadata_cols = (KBC_SYNCED, KBC_DELETED)
    row_with_metadata = row + kbc_metadata
    columns.extend(kbc_metadata_cols)
    catalog_entry.schema.properties[KBC_SYNCED] = core.schema.Schema(type=["null", "string"], format="date-time")
    catalog_entry.schema.properties[KBC_DELETED] = core.schema.Schema(type=["null", "string"], format="date-time")

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


def sync_query(cursor, catalog_entry, state, select_sql, columns, stream_version, params):
    replication_key = core.get_bookmark(state, catalog_entry.tap_stream_id, 'replication_key')

    query_string = cursor.mogrify(select_sql, params)
    time_extracted = utils.now()

    LOGGER.info('Running %s', query_string)
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

            if replication_method in {'FULL_TABLE', 'LOG_BASED'}:
                key_properties = get_key_properties(catalog_entry)

                max_pk_values = core.get_bookmark(state, catalog_entry.tap_stream_id, 'max_pk_values')

                if max_pk_values:
                    last_pk_fetched = {k: v for k, v in record_message.record.items()
                                       if k in key_properties}

                    state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'last_pk_fetched', last_pk_fetched)

            elif replication_method == 'INCREMENTAL':
                if replication_key is not None:
                    state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'replication_key', replication_key)

                    state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'replication_key_value',
                                                record_message.record[replication_key])

            if rows_saved % 1000 == 0:
                core.write_message(core.StateMessage(value=copy.deepcopy(state)))
                if rows_saved % 1000000 == 0:
                    LOGGER.info('Ingested row count has hit {} for table {}'.format(rows_saved,
                                                                                    catalog_entry.tap_stream_id))

            row = cursor.fetchone()

    core.write_message(core.StateMessage(value=copy.deepcopy(state)))
    return rows_saved


def _add_kbc_metadata_to_df(table_df: pd.DataFrame, catalog_entry):
    """Add metadata for Keboola. Can presume not there since should only be called for full sync."""
    kbc_metadata_cols = (KBC_SYNCED, KBC_DELETED)
    kbc_metadata = (SYNC_STARTED_AT, None)

    catalog_entry.schema.properties[KBC_SYNCED] = core.schema.Schema(type=["null", "string"], format="date-time")
    catalog_entry.schema.properties[KBC_DELETED] = core.schema.Schema(type=["null", "string"], format="date-time")

    for column, value in zip(kbc_metadata_cols, kbc_metadata):
        table_df[column] = value


def sync_query_bulk(conn, cursor, catalog_entry, state, select_sql, columns, stream_version, params,
                    tables_destination: str = None):
    replication_key = core.get_bookmark(state, catalog_entry.tap_stream_id, 'replication_key')

    query_string = cursor.mogrify(select_sql, params)

    LOGGER.info('Running %s', query_string)
    # Chunk Changes Here
    current_chunk = 0

    LOGGER.info('Starting chunk processing for stream {}'.format(catalog_entry.tap_stream_id))
    start_time = utils.now()
    for chunk in pd.read_sql(select_sql, con=conn, chunksize=CSV_CHUNK_SIZE):
        chunk_start_time = utils.now()
        current_chunk += 1
        if current_chunk > 1:
            LOGGER.info('Finished writing {} rows to CSV for batch {}, total ingested so far: {}'.format(
                CSV_CHUNK_SIZE, current_chunk - 1, current_chunk * CSV_CHUNK_SIZE
            ))

        _add_kbc_metadata_to_df(chunk, catalog_entry)

        if current_chunk == 1:
            table_and_headers = {}
            headers = list(chunk.columns.values)
            table_and_headers[catalog_entry.table] = headers

            # Write to CSV of specific structure: table, headers (no header is written to this CSV)
            tables_headers_path = os.path.join(CURRENT_PATH, '..', '..', '')
            with open(tables_headers_path + 'table_headers.csv', 'a+') as headers_csv:
                writer = csv.writer(headers_csv, delimiter='\t')
                writer.writerow([catalog_entry.table, headers])
                LOGGER.info('Setting table {} metadata for columns to {}, staged for manifest'.format(
                    catalog_entry.table, headers
                ))

        destination_output_path = os.path.join(tables_destination, catalog_entry.table.upper() + '.csv', '')

        if not os.path.exists(destination_output_path):
            os.mkdir(destination_output_path)

        csv_path = os.path.join(destination_output_path, catalog_entry.table.upper() + '-' +
                                str(current_chunk) + '.csv')

        chunk.to_csv(csv_path, index=False, mode='a', header=False)
        LOGGER.info('Ingested {} rows to path {}'.format(chunk.shape[0], os.path.basename(csv_path)))
        chunk_end_time = utils.now()
        chunk_processing_duration = (chunk_end_time - chunk_start_time).total_seconds()
        LOGGER.info('Chunk {} processing time: {} seconds'.format(current_chunk, chunk_processing_duration))

    end_time = utils.now()
    LOGGER.info('Finished chunk processing for stream {}'.format(catalog_entry.tap_stream_id))
    full_chunking_processing_duration = (end_time - start_time).total_seconds()
    LOGGER.info('Total processing time: {} seconds'.format(full_chunking_processing_duration))

    # TODO: Determine if we need last PK fetched and these states, or if we can remove altogether
    # md_map = metadata.to_map(catalog_entry.metadata)
    # stream_metadata = md_map.get((), {})
    # replication_method = stream_metadata.get('replication-method')

    # if replication_method in {'FULL_TABLE', 'LOG_BASED'}:
    #     key_properties = get_key_properties(catalog_entry)

    # max_pk_values = core.get_bookmark(state, catalog_entry.tap_stream_id, 'max_pk_values')

    # if max_pk_values:
    #     # Get last row for max of PK fetched
    #     last_file_df = pd.read_csv(os.path.join(tables_destination, catalog_entry.table, str(current_chunk)))
    #     last_row_fetched = last_file_df.tail(1).values.to
    #     last_pk_fetched = {k: v for k, v in last_row_fetched
    #                        if k in key_properties}

    # state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'last_pk_fetched', last_pk_fetched)

    # TODO: Handle for key-based incremental here
    # elif replication_method == 'INCREMENTAL':
    #     if replication_key is not None:
    #         state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'replication_key', replication_key)
    #
    #         state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'replication_key_value',
    #                                     record_message.record[replication_key])

    core.write_message(core.StateMessage(value=copy.deepcopy(state)))

    # End Chunk Changes
