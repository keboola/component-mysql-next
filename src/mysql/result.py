"""
Write result to CSV.
"""
import argparse
import collections
import csv
import io
import json
import logging
import os
import sys
from datetime import datetime

from jsonschema.validators import Draft4Validator
import src.core as core

LOGGER = logging.getLogger(__name__)


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


def persist_messages(destination_path, messages, delimiter, quotechar):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}

    print('messages')
    print(type(messages))
    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    for message in messages:
        print('got to message: {}'.format(message))
        print(type(message))
        try:
            o = core.parse_message(message).asdict()
            print(o)
            print(type(o))
        except json.decoder.JSONDecodeError:
            LOGGER.error("Unable to parse:\n{}".format(message))
            raise
        message_type = o['type']
        print(message_type)
        if message_type == 'RECORD':
            if o['stream'] not in schemas:
                raise Exception("A record for stream {}"
                                "was encountered before a corresponding schema".format(o['stream']))

            validators[o['stream']].validate(o['record'])
            print('finished validator')
            filename = o['stream'] + '-' + now + '.csv'
            filename = os.path.expanduser(os.path.join(destination_path, filename))
            file_is_empty = (not os.path.isfile(filename)) or os.stat(filename).st_size == 0
            print('is file empty: {}'.format(file_is_empty))

            flattened_record = flatten(o['record'])
            print('finished flattening: {}'.format(flattened_record))

            if o['stream'] not in headers and not file_is_empty:
                with open(filename, 'r') as csvfile:
                    reader = csv.reader(csvfile, delimiter=delimiter, quotechar=quotechar)
                    first_line = next(reader)
                    headers[o['stream']] = first_line if first_line else flattened_record.keys()
                    print('got to header write')
                    print(first_line)
            else:
                headers[o['stream']] = flattened_record.keys()
                print('finished writing a record...')

            with open(filename, 'a') as csvfile:
                writer = csv.DictWriter(csvfile, headers[o['stream']], extrasaction='ignore', delimiter=delimiter,
                                        quotechar=quotechar)
                if file_is_empty:
                    writer.writeheader()
                    print('finished writing header for empty file')

                writer.writerow(flattened_record)
                print('writing row {}'.format(flattened_record))

            state = None
        elif message_type == 'STATE':
            print('handling stage message')
            LOGGER.debug('Setting state to {}'.format(o['value']))
            state = o['value']
        elif message_type == 'SCHEMA':
            print('handling schema message')
            stream = o['stream']
            schemas[stream] = o['schema']
            validators[stream] = Draft4Validator(o['schema'])
            key_properties[stream] = o['key_properties']
            print('finished schema handling')
        else:
            LOGGER.warning("Unknown message type {} in message {}".format(o['type'], o))

    return state


def write_stdin(destination_path: str, delimiter: str = ',', quotechar: str = '"'):
    """Write results to CSV output destination."""
    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    print(input_messages)
    state = persist_messages(destination_path, input_messages, delimiter=delimiter, quotechar=quotechar)
    emit_state(state)
    LOGGER.info('Finish write execution normally.')


def write(destination_path: str, result: str, delimiter: str = ',', quotechar: str = '"'):
    """Write results to CSV output destination."""
    # input_messages = io.TextIOWrapper(source, encoding='utf-8')

    input_messages = result
    print(input_messages)
    state = persist_messages(destination_path, input_messages, delimiter=delimiter, quotechar=quotechar)
    emit_state(state)
    LOGGER.info('Finish write execution normally.')


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
    state = persist_messages(config.get('delimiter', ','),
                             config.get('quotechar', '"'),
                             input_messages,
                             config.get('destination_path', ''))

    emit_state(state)
    LOGGER.debug("Exiting normally")


if __name__ == '__main__':
    main()
