import base64
import csv
import logging
import os
import tempfile
from typing import Dict, List

import ciso8601
import pytz
import simplejson as json
from kbc.csv_tools import CachedOrthogonalDictWriter

from core.bookmarks import KEY_STORAGE_COLUMNS
from mysql.replication import common

SCHEMA_CHANGE_COLS = ['schema', 'table', 'change_type', 'column_name', 'query', 'timestamp']

try:
    import core.utils as u
except ImportError:
    import src.core.utils as u


class Message:
    """Base class for messages."""

    def asdict(self):  # pylint: disable=no-self-use
        raise Exception('Not implemented')

    def __eq__(self, other):
        return isinstance(other, Message) and self.asdict() == other.asdict()

    def __repr__(self):
        pairs = ["{}={}".format(k, v) for k, v in self.asdict().items()]
        attrstr = ", ".join(pairs)
        return "{}({})".format(self.__class__.__name__, attrstr)

    def __str__(self):
        return str(self.asdict())


class RecordMessage(Message):
    """RECORD message.
    The RECORD message has these fields:
      * stream (string) - The name of the stream the record belongs to.
      * record (dict) - The raw data for the record
      * version (optional, int) - For versioned streams, the version
        number. Note that this feature is experimental and most Taps and
        Targets should not need to use versioned streams.
    msg = core.RecordMessage(
        stream='users',
        record={'id': 1, 'name': 'Mary'})
    """

    def __init__(self, stream, record, column_map: dict, version=None, time_extracted=None):
        self.stream = stream
        self.record = record
        self.version = version
        self.time_extracted = time_extracted
        self.column_map = column_map
        if time_extracted and not time_extracted.tzinfo:
            raise ValueError("'time_extracted' must be either None " +
                             "or an aware datetime (with a time zone)")

    def asdict(self):
        result = {
            'type': 'RECORD',
            'stream': self.stream,
            'record': self.record,
            'column_map': self.column_map
        }
        if self.version is not None:
            result['version'] = self.version
        if self.time_extracted:
            as_utc = self.time_extracted.astimezone(pytz.utc)
            result['time_extracted'] = u.strftime(as_utc)
        return result

    def __str__(self):
        return str(self.asdict())


class SchemaMessage(Message):
    """SCHEMA message.
    The SCHEMA message has these fields:
      * stream (string) - The name of the stream this schema describes.
      * schema (dict) - The JSON schema.
      * key_properties (list of strings) - List of primary key properties.
    msg = core.SchemaMessage(
        stream='users',
        schema={'type': 'object',
                'properties': {
                    'id': {'type': 'integer'},
                    'name': {'type': 'string'}
                }
               },
        key_properties=['id'])
    """

    def __init__(self, stream, schema, key_properties, bookmark_properties=None):
        self.stream = stream
        self.schema = schema
        self.key_properties = key_properties

        if isinstance(bookmark_properties, (str, bytes)):
            bookmark_properties = [bookmark_properties]
        if bookmark_properties and not isinstance(bookmark_properties, list):
            raise Exception("bookmark_properties must be a string or list of strings")

        self.bookmark_properties = bookmark_properties

    def asdict(self):
        result = {
            'type': 'SCHEMA',
            'stream': self.stream,
            'schema': self.schema,
            'key_properties': self.key_properties
        }
        if self.bookmark_properties:
            result['bookmark_properties'] = self.bookmark_properties
        return result


class StateMessage(Message):
    """STATE message.
    The STATE message has one field:
      * value (dict) - The value of the state.
    msg = core.StateMessage(
        value={'users': '2017-06-19T00:00:00'})
    """

    def __init__(self, value):
        self.value = value

    def asdict(self):
        return {
            'type': 'STATE',
            'value': self.value
        }


class ActivateVersionMessage(Message):
    """ACTIVATE_VERSION message (EXPERIMENTAL).
    The ACTIVATE_VERSION messages has these fields:
      * stream - The name of the stream.
      * version - The version number to activate.
    This is a signal to the Target that it should delete all previously
    seen data and replace it with all the RECORDs it has seen where the
    record's version matches this version number.
    Note that this feature is experimental. Most Taps and Targets should
    not need to use the "version" field of "RECORD" messages or the
    "ACTIVATE_VERSION" message at all.
    msg = core.ActivateVersionMessage(
        stream='users',
        version=2)
    """

    def __init__(self, stream, version):
        self.stream = stream
        self.version = version

    def asdict(self):
        return {
            'type': 'ACTIVATE_VERSION',
            'stream': self.stream,
            'version': self.version
        }


def _required_key(msg, k):
    if k not in msg:
        raise Exception("Message is missing required key '{}': {}".format(k, msg))

    return msg[k]


def get_message_type(input_message: dict):
    return _required_key(input_message, 'type')


def get_stream_name(input_message: dict):
    return _required_key(input_message, 'stream')


def parse_message(msg):
    """Parse a message string into a Message object."""
    # TODO: May use Decimal for parsing data here for perfect precision
    obj = json.loads(msg, use_decimal=True)
    msg_type = _required_key(obj, 'type')

    if msg_type == 'RECORD':
        time_extracted = obj.get('time_extracted')
        if time_extracted:
            try:
                time_extracted = ciso8601.parse_datetime(time_extracted)
            except Exception:
                logging.warning("unable to parse time_extracted with ciso8601 library")
                time_extracted = None

            # time_extracted = dateutil.parser.parse(time_extracted)
        return RecordMessage(stream=_required_key(obj, 'stream'), record=_required_key(obj, 'record'),
                             version=obj.get('version'), time_extracted=time_extracted)

    elif msg_type == 'SCHEMA':
        return SchemaMessage(stream=_required_key(obj, 'stream'), schema=_required_key(obj, 'schema'),
                             key_properties=_required_key(obj, 'key_properties'),
                             bookmark_properties=obj.get('bookmark_properties'))

    elif msg_type == 'STATE':
        return StateMessage(value=_required_key(obj, 'value'))

    elif msg_type == 'ACTIVATE_VERSION':
        return ActivateVersionMessage(stream=_required_key(obj, 'stream'), version=_required_key(obj, 'version'))
    else:
        return None


def handle_binary_data(row: dict, binary_columns: list, binary_data_handler, replace_nulls: bool = False):
    for column in binary_columns:
        if column not in row:
            pass

        else:

            value_to_convert = row[column]

            if replace_nulls is True:
                value_to_convert = value_to_convert.strip(b'\x00')

            if binary_data_handler == 'plain':
                row[column] = value_to_convert.decode()
            elif binary_data_handler == 'hex':
                row[column] = value_to_convert.hex().upper()
            elif binary_data_handler == 'base64':
                row[column] = base64.b64encode(value_to_convert).decode()
            else:
                logging.error(f"Unknown binary data handler format: {binary_data_handler}.")
                exit(1)

    return row


class MessageStore(dict):
    """Storage for log-based messages"""

    def __init__(self, state: dict = None, flush_row_threshold: int = 5000,
                 output_table_path: str = '/data/out/tables', binary_handler: str = 'plain', output_bucket=''):
        super().__init__()
        self.state = state
        self.flush_row_threshold = flush_row_threshold
        self.output_table_path = output_table_path
        self.binary_data_handler = binary_handler

        # cache of dict writers
        self._writer_cache: Dict[str, CachedOrthogonalDictWriter] = {}
        self._schema_change_writer = None

        self._data_store = {}
        self._found_schemas = []
        self._found_tables = []
        self._found_headers = {}
        self._processed_records = 0
        self._flush_count = 0
        self.output_bucket = output_bucket

        # self.io = {}
        # self.io_csv = {}

    def __str__(self):
        return str(self._data_store)
        # return json.dumps(self._data_store)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.flush_records()
        self._close_writer_cache()

    @property
    def found_schemas(self):
        return self._found_schemas

    @property
    def found_tables(self):
        return self._found_tables

    @property
    def found_headers(self):
        return self._found_headers

    @property
    def flush_count(self):
        return self._flush_count

    @property
    def total_records_flushed(self):
        return self._flush_count * self.flush_row_threshold

    def get_state(self):
        return self.state

    def add_schema(self, schema: str):
        logging.debug('Adding schema {} to message store'.format(schema))
        self._data_store[schema] = {}

    def add_table(self, schema: str, table: str):
        logging.debug('Adding table {} to message store in schema {}'.format(table, schema))
        store_schema = self._data_store.get(schema)
        if not store_schema:
            self.add_schema(schema)

        self._data_store[schema][table] = {'records': [], 'schemas': []}

    def _get_all_table_columns(self, schema: str, table: str):
        columns = list(self._data_store[schema][table]['column_schema'].keys())
        index = common.generate_tap_stream_id(schema, table)
        # merge with storage columns from state
        for c in self.state.get(KEY_STORAGE_COLUMNS, {}).get(index, []):
            if c not in columns:
                columns.append(c)
        return columns

    def write_schema_change_message(self, message: dict):
        if self._schema_change_writer is None:
            path = os.path.join(self.output_table_path, 'SCHEMA_CHANGES.csv')
            writer = csv.DictWriter(open(path, 'w+', newline=''), fieldnames=SCHEMA_CHANGE_COLS)
            self._schema_change_writer = writer
            self._schema_change_writer.writeheader()
            # create manifest
            _schema_changes_destination = f'{self.output_bucket}_metadata.SCHEMA_CHANGES'
            manifest = {'primary_key': ['column_name', 'query', 'timestamp'],
                        'incremental': True,
                        'destination': _schema_changes_destination}
            with open(path + '.manifest', 'w') as manifest_file:
                json.dump(manifest, manifest_file)

        self._schema_change_writer.writerow(message)

    def flush_records(self):
        logging.debug('Flushing records for each of found tables: {}'.format(self._found_tables))
        for schema in self._found_schemas:
            for table in self.found_tables:
                if self._data_store[schema][table].get('records'):
                    # logging.debug('got records for {} {}'.format(schema, table))

                    binary_columns = self._data_store[schema][table]['schemas'][0]['binary']
                    if binary_columns == []:
                        binary_columns = None
                    file_output = table.upper() + '.csv'

                    table_schema = self._get_all_table_columns(schema, table)

                    self.write_to_csv(self._data_store[schema][table].get('records'), file_output,
                                      table_schema, binary_columns)

                    self._clear_records(schema, table)

        self._processed_records = 0
        self._flush_count += 1

    def write_to_csv(self, data_records: List[dict], file_name: str, schema: list,
                     binary_columns: list = None):
        full_path = os.path.expanduser(os.path.join(self.output_table_path, file_name))
        if not data_records:
            logging.warning("No records to write!")
            return

        if not self._writer_cache.get(full_path):
            # init writer
            # get columns of all collected columns so far -> the rest will handle the writer
            columns = schema
            temp_directory = tempfile.TemporaryDirectory().name
            writer = CachedOrthogonalDictWriter(full_path, columns, quoting=csv.QUOTE_ALL,
                                                temp_directory=temp_directory)
            writer.writeheader()
            self._writer_cache[full_path] = writer
        else:
            writer = self._writer_cache.get(full_path)

        for record in data_records:
            if binary_columns is not None:
                record = handle_binary_data(record, binary_columns, self.binary_data_handler)
            writer.writerow(record)

    def add_message(self, schema: str, input_message: dict):
        msg_type = get_message_type(input_message)

        if schema and schema not in self._found_schemas:
            self.add_schema(schema)
            self._found_schemas.append(schema)

        if msg_type == 'RECORD':
            table_name = get_stream_name(input_message)
            if table_name not in self._found_tables:
                self.add_table(schema, table_name)
                self._found_tables.append(table_name)
            self._expand_column_schema(schema, table_name, input_message['column_map'])
            self._add_record_message(schema, table_name, _required_key(input_message, 'record'))

        elif msg_type == 'SCHEMA':
            table_name = get_stream_name(input_message)
            if table_name not in self._found_tables:
                self.add_table(schema, table_name)
                self._found_tables.append(table_name)

            self._add_schema_message(schema, table_name, _required_key(input_message, 'schema'))

        elif msg_type == 'STATE':
            self._set_state(_required_key(input_message, 'value'))

        else:
            logging.info('Message type not found: {}'.format(msg_type))

    def _clear_records(self, schema: str, table: str):
        self._data_store[schema][table]['records'] = []

    def _expand_column_schema(self, schema, table, column_schema):
        cache_record = self._data_store[schema][table]

        if cache_record.get('column_schema') is None:
            cache_record['column_schema'] = {}

        for col in column_schema:
            col_name = col['COLUMN_NAME']
            if col_name not in cache_record['column_schema']:
                cache_record['column_schema'][col_name] = col

            # originaly updated from initial schema message
            if 'binary' in col['COLUMN_TYPE'].lower() and col_name not in cache_record['schemas'][0]['binary']:
                cache_record['schemas'][0]['binary'].append(col_name)

    def _add_record_message(self, schema: str, table: str, record_message: dict):
        if self._processed_records > self.flush_row_threshold:
            self.flush_records()

        self._data_store[schema][table]['records'].append(record_message)
        self._processed_records += 1

    def _add_schema_message(self, schema: str, table: str, schema_message: dict):
        binary_columns = []

        for column_name, column_schema in schema_message['properties'].items():
            if 'binary' in column_schema['type']:
                binary_columns += [column_name]

        schema_message['binary'] = binary_columns
        self._data_store[schema][table]['schemas'].append(schema_message)
        if not self.found_headers.get(schema):
            self._found_headers[schema] = []
        self._found_headers[schema].append({table: list(schema_message.get('properties').keys())})

    def _set_state(self, state_message):
        self.state = state_message

    def _close_writer_cache(self):
        for table, wr in self._writer_cache.items():
            logging.info(f"Closing out stream for {table}")
            wr.close()


def format_message(message):
    try:
        return json.dumps(message.asdict(), use_decimal=True)
    except AttributeError:  # Message may be already dict if converted to handle for non-JSON-serializable columns
        return json.dumps(message, use_decimal=True)


def write_message(message, database_schema: str = None, message_store: MessageStore = None):
    if message_store is None:  # Specifically none, as default message store is empty dict
        logging.warning('NOTE: Write message declared without message store: {}'.format(message.asdict()))
    else:
        message_store.add_message(database_schema, message.asdict())
