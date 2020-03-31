#!/usr/bin/env python
"""
Output to CSV.
"""
import argparse
import io
import os
import sys
import json
import csv
# from datetime import datetime
import collections
from jsonschema.validators import Draft4Validator

try:
    import core as core
except ImportError:
    import src.core as core

LOGGER = core.get_logger()
MAX_CSV_FILE_SIZE_BYTES = 1000000


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

    return state


def persist_messages_with_slicing(delimiter, quotechar, messages, destination_path, slice_row_count: int = 1000000):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}
    message_number = 0
    lag_table_file_num = 1
    table_file_num = 1

    for message in messages:
        message_number += 1
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

            filename = o['stream'] + '-' + str(lag_table_file_num) + '.csv'
            file_path = os.path.expanduser(os.path.join(destination_path, o['stream']))
            if not os.path.exists(file_path):
                os.makedirs(file_path)
            full_path = os.path.join(file_path, filename)

            file_is_empty = (not os.path.isfile(full_path)) or os.stat(full_path).st_size == 0

            flattened_record = flatten(o['record'])
            LOGGER.info('Flattened record: {}'.format(flattened_record))

            if (o['stream'] not in headers and not file_is_empty) or lag_table_file_num < table_file_num:
                lag_table_file_num = table_file_num
                with open(full_path, 'r') as csv_file:
                    reader = csv.reader(csv_file, delimiter=delimiter, quotechar=quotechar)
                    first_line = next(reader)
                    headers[o['stream']] = first_line if first_line else flattened_record.keys()
            else:
                headers[o['stream']] = flattened_record.keys()

            with open(full_path, 'a+') as csv_file:
                writer = csv.DictWriter(csv_file, headers[o['stream']], extrasaction='ignore', delimiter=delimiter,
                                        quotechar=quotechar)
                # if file_is_empty:
                #     writer.writeheader()

                writer.writerow(flattened_record)

            # Every million messages, see if we should split the file
            if message_number % slice_row_count == 0:
                LOGGER.info('Iterating table {} to number {}'.format(o['stream'], table_file_num + 1))
                table_file_num += 1

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
            LOGGER.warning("Unknown message type {} in message {}"
                           .format(o['type'], o))

    # Add Columns to Manifest files.
    for table, header in headers.items():
        table_slice_path = os.path.join(destination_path, table)
        if os.path.exists(table_slice_path):
            manifest_file = os.path.join(destination_path, table + '.csv.manifest')
            if os.path.exists(manifest_file):
                with open(manifest_file) as manifest:
                    metadata = json.load(manifest)
                    metadata['columns'] = header
            else:
                LOGGER.warning('Manifest file specified to write to did not exist when attempting columns write')

    return state


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

    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_messages(config.get('delimiter', ','), config.get('quotechar', '"'),
                             input_messages, config.get('destination_path', ''))

    state_output_full_path = os.path.join(config.get('output_state_path'), 'state.json')
    write_state(state, state_output_full_path)
    emit_state(state)
    LOGGER.info("Exiting normally")


if __name__ == '__main__':
    main()
