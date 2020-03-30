"""
Incremental replication via key-based replication.
"""
import pendulum

try:
    import core as core
    from core import metadata
    from mysql.client import connect_with_backoff
    import mysql.replication.common as common
except ImportError:
    import src.core as core
    from src.core import metadata
    from src.mysql.client import connect_with_backoff
    import src.mysql.replication.common as common

LOGGER = core.get_logger()

BOOKMARK_KEYS = {'replication_key', 'replication_key_value', 'version'}


def sync_table(mysql_conn, catalog_entry, state, columns, limit=None):
    common.whitelist_bookmark_keys(BOOKMARK_KEYS, catalog_entry.tap_stream_id, state)

    catalog_metadata = metadata.to_map(catalog_entry.metadata)
    stream_metadata = catalog_metadata.get((), {})

    iterate_limit = True
    while iterate_limit:
        replication_key_metadata = stream_metadata.get('replication-key')
        replication_key_state = core.get_bookmark(state, catalog_entry.tap_stream_id, 'replication_key')
        replication_key_value = None

        if replication_key_metadata == replication_key_state:
            replication_key_value = core.get_bookmark(state, catalog_entry.tap_stream_id, 'replication_key_value')
        else:
            state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'replication_key',
                                        replication_key_metadata)
            state = core.clear_bookmark(state, catalog_entry.tap_stream_id, 'replication_key_value')

        stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
        state = core.write_bookmark(state, catalog_entry.tap_stream_id, 'version', stream_version)

        activate_version_message = core.ActivateVersionMessage(
            stream=catalog_entry.stream,
            version=stream_version
        )

        core.write_message(activate_version_message)

        with connect_with_backoff(mysql_conn) as open_conn:
            with open_conn.cursor() as cursor:
                select_sql = common.generate_select_sql(catalog_entry, columns)
                params = {}

                if replication_key_value is not None:
                    if catalog_entry.schema.properties[replication_key_metadata].format == 'date-time':
                        replication_key_value = pendulum.parse(replication_key_value)

                    select_sql += ' WHERE `{}` >= %(replication_key_value)s ORDER BY `{}` ASC'.format(
                        replication_key_metadata,
                        replication_key_metadata)

                    params['replication_key_value'] = replication_key_value
                elif replication_key_metadata is not None:
                    select_sql += ' ORDER BY `{}` ASC'.format(replication_key_metadata)

                if limit:
                    select_sql += ' LIMIT {}'.format(limit)

                num_rows = common.sync_query(cursor, catalog_entry, state, select_sql, columns, stream_version, params)
                if limit is None or num_rows < limit:
                    iterate_limit = False
