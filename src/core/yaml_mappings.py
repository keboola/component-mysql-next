import json
import yaml

yaml_file = '/Users/johnathanbrooks/PycharmProjects/keboola_ex_mysql_nextv2/data/in/files/mappings.yaml'
table_mappings_file = '/Users/johnathanbrooks/PycharmProjects/keboola_ex_mysql_nextv2/data/in/files/table_mappings.json'

new_mappings_file = '/Users/johnathanbrooks/PycharmProjects/keboola_ex_mysql_nextv2/data/in/files/table_mappings_raw.json'


def make_yaml_mapping_file(json_mappings: dict, yaml_output_file: str):
    """Creates the input YAML mapping file for choosing desired tables and columns based on mappings JSON."""
    yaml_data = []
    stream_data = json_mappings['streams']
    databases_and_tables = {}

    for stream in stream_data:
        # Iterate through mappings per database
        table_name = stream['stream']
        table_metadata = stream['metadata'][0]['metadata']
        columns_metadata = stream['metadata'][1:]

        # Get tables and table choices
        stream_database = table_metadata['database-name']
        is_selected = table_metadata.get('selected') or False
        replication_method = str(table_metadata.get('replication-method'))
        replication_key = table_metadata.get('replication-key')

        # Get columns and column choices
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
            databases_and_tables[stream_database].append(table_mapping)
        else:
            databases_and_tables[stream_database] = [table_mapping]

    yaml_data.append(databases_and_tables)

    # Add final output to resulting YAML file
    with open(yaml_output_file, 'w') as yaml_output_mapping:
        yaml.dump(yaml_data, yaml_output_mapping, default_flow_style=False, allow_unicode=True)


# Convert YAML to JSON table mappings choices
with open(yaml_file, 'r') as yaml_input_mapping:
    yaml_mappings = yaml.load(yaml_input_mapping)

json_data = dict()
json_data['streams'] = yaml_mappings

with open(new_mappings_file, 'w') as json_output_mapping:
    json.dump(json_data, json_output_mapping)


if __name__ == '__main__':
    with open(table_mappings_file, encoding='utf-8') as json_input_mapping:
        json_mappings = json.load(json_input_mapping)

    make_yaml_mapping_file(json_mappings, yaml_file)
