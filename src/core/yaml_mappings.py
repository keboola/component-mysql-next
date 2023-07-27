# import yaml
import json


def make_yaml_mapping_file(json_mappings: dict):
    """Creates the input YAML mapping file for choosing desired tables and columns based on mappings JSON."""
    yaml_data = []
    stream_data = json_mappings['streams']
    databases_and_tables = {}

    for stream in stream_data:
        # Iterate through mappings per database
        table_name = stream['stream']
        table_metadata = stream['metadata'][0]['metadata']
        columns_metadata = stream['metadata'][1:]

        # Mandatory metadata: Get tables and table choices
        stream_database = table_metadata['database-name']
        is_selected = table_metadata.get('selected') or False
        replication_method_raw = table_metadata.get('replication-method') or 'log_based'
        replication_method = str(replication_method_raw).lower()
        replication_key = table_metadata.get('replication-key')

        # Mandatory metadata: Get columns and column choices
        column_meta_mappings = {}
        for column_metadata in columns_metadata:
            column_details = column_metadata['metadata']
            column = column_metadata['breadcrumb'][1]
            column_is_selected = column_details.get('selected') or column_details.get('selected-by-default')

            column_meta_mappings[column] = column_is_selected

        # Put the table and column mappings together
        table_meta_mappings = {"selected": is_selected, "replication-method": replication_method}
        if replication_key or replication_method.upper() == 'INCREMENTAL':
            table_meta_mappings["replication-key"] = replication_key
        table_meta_mappings["columns"] = column_meta_mappings

        table_mapping = {table_name: table_meta_mappings}
        if databases_and_tables.get(stream_database):
            databases_and_tables[stream_database]['tables'].append(table_mapping)
        else:
            databases_and_tables[stream_database] = {"tables": [table_mapping]}

    yaml_data.append(databases_and_tables)
    return yaml_data


# Convert YAML to JSON table mappings choices
def convert_yaml_to_json_mapping(yaml_mappings, raw_json_mapping):
    """Convert YAML table and column choices to JSON mapping.

    Args:
        yaml_mappings: The YAML mappings to be converted
        raw_json_mapping: The Raw JSON table mappings from the database
    """
    syncing_tables = []

    # First, determine which items to pull from YAML mappings
    for database_mapping in yaml_mappings:
        for database, tables_info in database_mapping.items():

            for table in tables_info['tables']:
                for table_name, table_metadata in table.items():
                    stream_id = database + '-' + table_name
                    table_columns = table_metadata.get('columns', {})
                    selected_table = table_metadata.get('selected', False)
                    replication_method = table_metadata.get('replication-method')
                    replication_key = table_metadata.get('replication-key')
                    output_table = table_metadata.get('output-table')

                    if selected_table:
                        syncing_table_data = {
                            "stream-id": stream_id,
                            "selected": True,
                            "replication-method": replication_method.upper(),
                            "columns": table_columns
                        }
                        if replication_key:
                            syncing_table_data['replication-key'] = replication_key
                        if output_table:
                            syncing_table_data['output-table'] = output_table

                        syncing_tables.append(syncing_table_data)

    synced_stream_ids = [stream['stream-id'] for stream in syncing_tables]
    json_mapping_streams = raw_json_mapping['streams']
    for json_mapping in json_mapping_streams:
        if json_mapping['tap_stream_id'] not in synced_stream_ids:
            continue

        matched_sync_table_index = -1
        for index, synced_table in enumerate(syncing_tables):
            if synced_table['stream-id'] == json_mapping['tap_stream_id']:
                matched_sync_table_index = index

        json_table_metadata = json_mapping['metadata'][0]['metadata']
        json_table_metadata['selected'] = syncing_tables[matched_sync_table_index]['selected']
        json_table_metadata['replication-method'] = syncing_tables[matched_sync_table_index]['replication-method']

        if syncing_tables[matched_sync_table_index].get('replication-key'):
            json_table_metadata['replication-key'] = syncing_tables[matched_sync_table_index]['replication-key']
        if syncing_tables[matched_sync_table_index].get('output-table'):
            json_table_metadata['output-table'] = syncing_tables[matched_sync_table_index]['output-table']

        json_columns_metadata = json_mapping['metadata'][1:]
        syncing_table_columns = syncing_tables[matched_sync_table_index]['columns']

        for json_column_metadata in json_columns_metadata:
            json_column_name = json_column_metadata['breadcrumb'][1]
            json_column_metadata_metadata_path = json_column_metadata['metadata']

            for syncing_column, column_selected in syncing_table_columns.items():
                if json_column_name == syncing_column:
                    if not column_selected:
                        json_column_metadata_metadata_path['selected'] = False

    output_stream_mappings = json.dumps({'streams': json_mapping_streams})
    return output_stream_mappings
