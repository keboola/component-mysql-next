#!/usr/bin/env python
"""
Output to CSV.
"""
import argparse
import collections
import csv
import io
import json
import sys
import os
# from datetime import datetime
from jsonschema.validators import Draft4Validator

try:
    import core as core
except ImportError:
    import src.core as core

LOGGER = core.get_logger()
MAX_CSV_FILE_SIZE_BYTES = 100000000


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        LOGGER.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def flatten(d, parent_key='', sep='__'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)

# TODO: Consider writing pre-sliced rather than slicing afterwards
# def write_to_csv_split(destination_table_path: str, message, headers, max_size_bytes: int = 1000000):
#     with open(destination_table_path, 'w', encoding='utf-8') as output_file:
#         writer = csv.writer(output_file)
#         maxsize = max_size_bytes  # max file size in bytes
#         writer.writerow(row)
#             if f.tell() > maxsize:  # f.tell() gives byte offset, no need to worry about multiwide chars
#                 break


def persist_messages(delimiter, quotechar, messages, destination_path):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}

    for message in messages:
        try:
            o = core.parse_message(message).asdict()
        except json.decoder.JSONDecodeError:
            LOGGER.error("Unable to parse:\n{}".format(message))
            raise
        message_type = o['type']
        if message_type == 'RECORD':
            if o['stream'] not in schemas:
                raise Exception("A record for stream {}"
                                "was encountered before a corresponding schema".format(o['stream']))

            validators[o['stream']].validate(o['record'])

            filename = o['stream'] + '.csv'
            filename = os.path.expanduser(os.path.join(destination_path, filename))
            file_is_empty = (not os.path.isfile(filename)) or os.stat(filename).st_size == 0

            flattened_record = flatten(o['record'])
            if o['stream'] not in headers and not file_is_empty:
                with open(filename, 'r') as csv_file:
                    reader = csv.reader(csv_file, delimiter=delimiter, quotechar=quotechar)
                    first_line = next(reader)
                    headers[o['stream']] = first_line if first_line else flattened_record.keys()
            else:
                headers[o['stream']] = flattened_record.keys()

            with open(filename, 'a') as csv_file:
                writer = csv.DictWriter(csv_file, headers[o['stream']], extrasaction='ignore', delimiter=delimiter,
                                        quotechar=quotechar)
                if file_is_empty:
                    writer.writeheader()

                writer.writerow(flattened_record)

            state = None
        elif message_type == 'STATE':
            LOGGER.info('Setting state to {}'.format(o['value']))
            state = o['value']
        elif message_type == 'SCHEMA':
            stream = o['stream']
            schemas[stream] = o['schema']
            validators[stream] = Draft4Validator(o['schema'])
            key_properties[stream] = o['key_properties']
        else:
            LOGGER.warning("Unknown message type {} in message {}".format(o['type'], o))

    return state, headers


# def persist_messages_with_slicing(destination_path, messages, delimiter: str = ',', method: str = 'byte_size',
#                                   quotechar: str = '"', row_count_check: int = 1000000, byte_size: int = 1000000):
#     """Persist messages with algorithm to push to slices of output CSVs. Choose method 'row_count' or 'byte_size."""
#     state = None
#     schemas = {}
#     key_properties = {}
#     headers = {}
#     validators = {}
#     message_number = 0
#     lag_table_file_num = 1
#     table_file_num = 1
#
#     for message in messages:
#         message_number += 1
#         try:
#             o = core.parse_message(message).asdict()
#         except json.decoder.JSONDecodeError:
#             LOGGER.error("Unable to parse:\n{}".format(message))
#             raise
#
#         message_type = o['type']
#         if message_type == 'RECORD':
#             if o['stream'] not in schemas:
#                 raise Exception("A record for stream {}"
#                                 "was encountered before a corresponding schema".format(o['stream']))
#
#             validators[o['stream']].validate(o['record'])
#
#             filename = o['stream'] + '-' + str(lag_table_file_num) + '.csv'
#             file_path = os.path.expanduser(os.path.join(destination_path, o['stream']))
#             if not os.path.exists(file_path):
#                 os.makedirs(file_path)
#             full_path = os.path.join(file_path, filename)
#
#             file_is_empty = (not os.path.isfile(full_path)) or os.stat(full_path).st_size == 0
#
#             flattened_record = flatten(o['record'])
#             LOGGER.info('Flattened record: {}'.format(flattened_record))
#
#             if (o['stream'] not in headers and not file_is_empty) or lag_table_file_num < table_file_num:
#                 lag_table_file_num = table_file_num
#                 with open(full_path, 'r') as csv_file:
#                     reader = csv.reader(csv_file, delimiter=delimiter, quotechar=quotechar)
#                     first_line = next(reader)
#                     headers[o['stream']] = first_line if first_line else flattened_record.keys()
#             else:
#                 headers[o['stream']] = flattened_record.keys()
#
#             with open(full_path, 'a+') as csv_file:
#                 writer = csv.DictWriter(csv_file, headers[o['stream']], extrasaction='ignore', delimiter=delimiter,
#                                         quotechar=quotechar)
#                 # if file_is_empty:
#                 #     writer.writeheader()
#
#                 writer.writerow(flattened_record)
#
#             # Every million messages, see if we should split the file
#             if method.lower() == 'row_count':
#                 if message_number % size == 0:
#                     LOGGER.info('Iterating table {} to number {}'.format(o['stream'], table_file_num + 1))
#                     table_file_num += 1
#             elif method.lower() == 'bytez_size':
#                 if
#             else:
#                 LOGGER.error('Chosen method is not valid, must be either "row_count" or "byte_size"')
#                 exit(1)
#
#             state = None
#         elif message_type == 'STATE':
#             LOGGER.info('Setting state to {}'.format(o['value']))
#             state = o['value']
#         elif message_type == 'SCHEMA':
#             stream = o['stream']
#             schemas[stream] = o['schema']
#             validators[stream] = Draft4Validator(o['schema'])
#             key_properties[stream] = o['key_properties']
#         else:
#             LOGGER.warning("Unknown message type {} in message {}"
#                            .format(o['type'], o))
#
#     # Add Columns to Manifest files.
#     for table, header in headers.items():
#         table_slice_path = os.path.join(destination_path, table)
#         if os.path.exists(table_slice_path):
#             manifest_file = os.path.join(destination_path, table + '.csv.manifest')
#             if os.path.exists(manifest_file):
#                 with open(manifest_file) as manifest:
#                     metadata = json.load(manifest)
#                     metadata['columns'] = header
#             else:
#                 LOGGER.warning('Manifest file specified to write to did not exist when attempting columns write')
#
#     return state


def split_csv(file_handler, delimiter: str = ',', row_limit: int = 10000, output_name_template: str = 'output_%s.csv',
              output_path: str = '.', keep_headers: bool = True) -> None:
    """Splits a CSV file into multiple pieces.

    A quick bastardization of the Python CSV library.
    Arguments:
        `row_limit`: The number of rows you want in each output file. 10,000 by default.
        `output_name_template`: A %s-style template for the numbered output files.
        `output_path`: Where to stick the output files.
        `keep_headers`: Whether or not to print the headers in each output file.
    Example usage:
        >> from toolbox import csv_splitter;
        >> csv_splitter.split(open('/home/ben/input.csv', 'r'));
    """
    reader = csv.reader(file_handler, delimiter=delimiter)
    current_piece = 1
    current_out_path = os.path.join(output_path, output_name_template % current_piece)

    current_out_writer = csv.writer(open(current_out_path, 'w'), delimiter=delimiter)
    current_limit = row_limit

    if keep_headers:
        headers = reader.next()
        current_out_writer.writerow(headers)

    for i, row in enumerate(reader):
        if i + 1 > current_limit:
            current_piece += 1
            current_limit = row_limit * current_piece
            current_out_path = os.path.join(output_path, output_name_template % current_piece)

            current_out_writer = csv.writer(open(current_out_path, 'w'), delimiter=delimiter)
            if keep_headers:
                current_out_writer.writerow(headers)

        current_out_writer.writerow(row)


def write_state(state: dict, state_output_path: str):
    """Write final state to state JSON file."""
    with open(state_output_path, 'w') as state_out_file:
        json.dump(state, state_out_file)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input_json:
            config = json.load(input_json)
    else:
        config = {}

    destination_path = config.get('destination_path', '')
    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state, headers = persist_messages(config.get('delimiter', ','), config.get('quotechar', '"'),
                                      input_messages, destination_path)

    # Split large CSV files.
    for file in os.listdir(destination_path):
        if os.path.splitext(file)[1] != '.csv':
            continue

        LOGGER.info('File: {}; destination path: {}'.format(file, destination_path))
        file_name_ext = os.path.basename(file)
        file_name = os.path.splitext(file_name_ext)[0]
        file_byte_size = os.stat(os.path.expanduser(os.path.join(destination_path, file))).st_size
        # LOGGER.info('byte size: {}'.format(file_byte_size))
        if file_byte_size > MAX_CSV_FILE_SIZE_BYTES:
            new_table_destination = os.path.expanduser(os.path.join(destination_path, file_name, file))
            prior_table_destination = os.path.expanduser(os.path.join(destination_path, file))
            # LOGGER.info('Prior destination of file is {}'.format(prior_table_destination))
            # LOGGER.info('Splitting file, table destination is: {}'.format(new_table_destination))
            if not os.path.exists(os.path.dirname(new_table_destination)):
                os.makedirs(os.path.dirname(new_table_destination))
            with open(prior_table_destination, 'r') as output_data_file:
                split_csv(output_data_file, keep_headers=False, output_path=os.path.dirname(new_table_destination),
                          output_name_template=file_name + '_%s.csv')
            os.remove(prior_table_destination)

            # Write columns to manifest file.
            table_slice_path = os.path.join(destination_path, file)
            # if os.path.exists(table_slice_path):
            manifest_file = os.path.expanduser(os.path.join(destination_path, file_name + '.csv.manifest'))
            LOGGER.info('Manifest file: {}'.format(manifest_file))
            # if os.path.isfile(manifest_file):
            #     LOGGER.info('Manifest file does exist')
            with open(manifest_file, 'r') as manifest:
                metadata = json.load(manifest)
                metadata['columns'] = list(headers[file_name])
            with open(manifest_file, 'w') as manifest:
                manifest.write(json.dumps(metadata))
            # else:
            #     LOGGER.warning('Manifest file specified to write to did not exist when attempting columns write')

    # Write final state.
    state_output_full_path = os.path.join(config.get('output_state_path'), 'state.json')
    write_state(state, state_output_full_path)
    emit_state(state)
    LOGGER.info("Exiting normally")


if __name__ == '__main__':
    main()
