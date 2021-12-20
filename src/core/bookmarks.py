import logging

from mysql.replication import common

KEY_STORAGE_COLUMNS = 'storage_columns'
KEY_LAST_TABLE_SCHEMAS = 'last_table_schemas'


def ensure_bookmark_path(state, path):
    submap = state
    for path_component in path:
        if submap.get(path_component) is None:
            submap[path_component] = {}

        submap = submap[path_component]
    return state


def write_bookmark(state, tap_stream_id, key, val):
    state = ensure_bookmark_path(state, ['bookmarks', tap_stream_id])
    state['bookmarks'][tap_stream_id][key] = val
    return state


def clear_bookmark(state, tap_stream_id, key):
    state = ensure_bookmark_path(state, ['bookmarks', tap_stream_id])
    state['bookmarks'][tap_stream_id].pop(key, None)
    return state


def reset_stream(state, tap_stream_id):
    state = ensure_bookmark_path(state, ['bookmarks', tap_stream_id])
    state['bookmarks'][tap_stream_id] = {}
    return state


def get_bookmark(state, tap_stream_id, key, default=None):
    return state.get('bookmarks', {}).get(tap_stream_id, {}).get(key, default)


def set_offset(state, tap_stream_id, offset_key, offset_value):
    state = ensure_bookmark_path(state, ['bookmarks', tap_stream_id, "offset", offset_key])
    state['bookmarks'][tap_stream_id]["offset"][offset_key] = offset_value
    return state


def clear_offset(state, tap_stream_id):
    state = ensure_bookmark_path(state, ['bookmarks', tap_stream_id, "offset"])
    state['bookmarks'][tap_stream_id]["offset"] = {}
    return state


def get_offset(state, tap_stream_id, default=None):
    return state.get('bookmarks', {}).get(tap_stream_id, {}).get("offset", default)


def set_currently_syncing(state, tap_stream_id):
    state['currently_syncing'] = tap_stream_id
    return state


def get_currently_syncing(state, default=None):
    return state.get('currently_syncing', default)


# ################ state parameters

def update_schema_in_state(state: dict, table_schema_cache: dict):
    last_table_schemas = state.get(KEY_LAST_TABLE_SCHEMAS, {})  # SAPI converts empty dicts to lists
    if last_table_schemas == []:
        last_table_schemas = {}

    state[KEY_LAST_TABLE_SCHEMAS] = {**last_table_schemas, **table_schema_cache}
    # store columns
    if not state.get(KEY_STORAGE_COLUMNS):
        state[KEY_STORAGE_COLUMNS] = {}

    for table in table_schema_cache:

        last_storage_columns = state.get(KEY_STORAGE_COLUMNS, {})
        if last_storage_columns == []:
            last_storage_columns = {}

        columns = last_storage_columns.get(table, [])
        # append non-existing
        for schema in table_schema_cache[table]:
            if schema['COLUMN_NAME'].upper() not in columns:
                logging.debug(f"Adding new column {schema['COLUMN_NAME']} to the column state "
                              f"for table {schema}.{table}. "
                              f"Current state: {columns}. Current table_schema_cache: {table_schema_cache} ")
                columns.append(schema['COLUMN_NAME'].upper())

        # append system
        if common.KBC_SYNCED not in columns:
            columns.append(common.KBC_SYNCED)
        if common.KBC_DELETED not in columns:
            columns.append(common.KBC_DELETED)
        if common.BINLOG_CHANGE_AT not in columns:
            columns.append(common.BINLOG_CHANGE_AT)
        if common.BINLOG_READ_AT not in columns:
            columns.append(common.BINLOG_READ_AT)

        state[KEY_STORAGE_COLUMNS][table] = columns
    return state
