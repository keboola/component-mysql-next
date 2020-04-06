#!/usr/bin/env python
"""
Output to CSV.
"""
import argparse
import collections
import csv
import io
import json
import logging
import sys
import os
from jsonschema.validators import Draft4Validator

try:
    import core as core
except ImportError:
    import src.core as core

LOGGER = logging.getLogger(__name__)
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
            #     LOGGER.info('Came across a RECORD message, here it is {}'.format(message))
            if o['stream'] not in schemas:
                raise Exception("A record for stream {}"
                                "was encountered before a corresponding schema".format(o['stream']))

            validators[o['stream']].validate(o['record'])

            filename = o['stream'].upper() + '.csv'
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
            # LOGGER.info('Setting state to {}'.format(o['value']))
            state = o['value']
        elif message_type == 'SCHEMA':
            stream = o['stream']
            schemas[stream] = o['schema']
            validators[stream] = Draft4Validator(o['schema'])
            key_properties[stream] = o['key_properties']
        else:
            LOGGER.info(o)

    return state, headers


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

    # Check if manifest file there without corresponding table (file or folder for split)... if so delete manifest file
    output_object_names = [sys_object for sys_object in os.listdir(destination_path)]
    for file in os.listdir(destination_path):
        file_name, ext = os.path.splitext(file)
        if ext == '.manifest':
            if file_name not in output_object_names:
                LOGGER.info('Missing {}, removing its manifest {}'.format(file_name, file))
                os.remove(os.path.join(destination_path, file))

    # Write final state to both Keboola and file output mapping, for QA purposes.
    state_output_full_path = os.path.join(config.get('output_state_path'), 'state.json')
    state_file_output_path = os.path.join(config.get('output_state_path'), 'files', 'state.json')

    write_state(state, state_output_full_path)
    write_state(state, state_file_output_path)
    emit_state(state)

    LOGGER.info("Exiting normally")


if __name__ == '__main__':
    main()
