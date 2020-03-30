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
            LOGGER.info('Flattened record: {}'.format(flattened_record))
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
            LOGGER.warning("Unknown message type {} in message {}"
                           .format(o['type'], o))

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
    state = persist_messages(config.get('delimiter', ','), config.get('quotechar', '"'), input_messages,
                             config.get('destination_path', ''))

    state_output_full_path = os.path.join(config.get('output_state_path'), 'state.json')
    write_state(state, state_output_full_path)
    emit_state(state)
    LOGGER.info("Exiting normally")


if __name__ == '__main__':
    main()
