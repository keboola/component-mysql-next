import json
import logging
import yaml


def make_yaml_mapping_file(json_mappings: dict):
    """Creates the input YAML mapping file for choosing desired tables and columns based on mappings JSON."""
    yaml_data = []
    stream_data = json_mappings['streams']
    full_json_map = {}
    databases_and_tables = {}

    # print(stream_data)
    for stream in stream_data:
        # print(stream)
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
            # databases_and_tables[stream_database] = [table_mapping]

    yaml_data.append(databases_and_tables)
    return yaml_data


# Convert YAML to JSON table mappings choices
def convert_yaml_to_json_mapping(yaml_mappings, raw_json_mapping):
    """Convert YAML table and column choices to JSON mapping.

    Args:
        yaml_mappings: The YAML mappings to be converted
        raw_json_mapping: The Raw JSON table mappings from the database
    """
    # with open(yaml_file, 'r') as yaml_input_mapping:
    #     yaml_mappings = yaml.load(yaml_input_mapping)

    stream_data = raw_json_mapping['streams']
    print(stream_data)

    # table_metadata = stream['metadata'][0]['metadata']
    # columns_metadata = stream['metadata'][1:]

    json_data = dict()
    for schema_sets in yaml_mappings:
        print('schema sets:')
        print(schema_sets)
        for schema_name, table_mappings in schema_sets.items():
            # print(schema_name)
            # print(table_mappings)
            for table_sets in table_mappings['tables']:
                print(table_sets)
                for table_name, mappings in table_sets.items():
                    table_json_index = -1
                    for index, stream in enumerate(stream_data):
                        if stream.get('stream') == table_name:
                            table_json_index = index

                    if table_json_index >= 0:
                        table_metadata_location = stream_data[table_json_index]['metadata'][0]['metadata']
                        table_metadata_location['selected'] = mappings['selected']
                        table_metadata_location['replication-method'] = mappings['replication-method']
                    else:
                        logging.warning('Table {} specified in YAML config not found in database'.format(table_name))
                        continue

                    table_column_metadata = stream_data[table_json_index]['metadata']
                    for column_name, is_selected in mappings['columns'].items():
                        column_json_index = -1
                        for index, column_metadata in enumerate(table_column_metadata[1:]):
                            if column_metadata['breadcrumb'][1] == column_name:
                                column_json_index = index

                        if column_json_index >= 0:
                            table_column_metadata[column_json_index]['metadata']['selected'] = is_selected
                        else:
                            logging.warning('Column {} in table {} in YAML config not found in database'.format(
                                column_name, table_name
                            ))

                    # print(schema_name)
                    # print(table_name)
                    # print(mappings)
                    # table_excluded_columns = []
                    # for column_name, is_selected in mappings['columns'].items():
                    #     if not is_selected:
                    #         table_excluded_columns.append(column_name)
                    #     print(column_name)
                    #     print(is_selected)
                    # print(json_data)
                    # if json_data.get(schema_name):
                    #     print(json_data[schema_name])
                    #     print('append')
                    #     json_data[schema_name].append({table_name: mappings})
                    # else:
                    #     print('new')
                    #     json_data[schema_name] = [{table_name: mappings}]
                    #     print(json_data)
                    # json_data[schema_name] = table_name
                    # print(table_name)
                # print(mappings) !

    # print(yaml_mappings)
    # json_data['streams'] = yaml_mappings
    # json_data = yaml_mappings
    #
    # output_json = json.dumps(json_data)
    # print('stream data:')

    output_stream_mappings = json.dumps({'streams': stream_data})
    # print(output_stream_mappings)
    return output_stream_mappings

    # with open(new_mappings_file, 'w') as json_output_mapping:
    #     json.dump(json_data, json_output_mapping)


if __name__ == '__main__':
    yaml_file = '/Users/johnathanbrooks/PycharmProjects/keboola_ex_mysql_nextv2/data/in/files/mappings.yaml'
    table_mappings_file = '/Users/johnathanbrooks/PycharmProjects/keboola_ex_mysql_nextv2/data/in/files/table_mappings.json'

    new_mappings_file = '/Users/johnathanbrooks/PycharmProjects/keboola_ex_mysql_nextv2/data/in/files/table_mappings_raw.json'

    # with open(table_mappings_file, encoding='utf-8') as json_input_mapping:
    #     json_mappings = json.load(json_input_mapping)
    #
    # make_yaml_mapping_file(json_mappings, yaml_file)

    with open(yaml_file, encoding='utf-8') as yaml_input_mapping:
        yaml_mappings = yaml.load(yaml_input_mapping)

    with open(new_mappings_file, encoding='utf-8') as new_raw_mapping_file:
        new_raw_mappings = json.load(new_raw_mapping_file)
        json_mapping = convert_yaml_to_json_mapping(yaml_mappings, new_raw_mappings)
        print(json_mapping)
