import functools
import logging
import struct
from enum import Enum
from typing import List

import backoff
import pymysql
from pymysql.util import byte2int
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication import constants, row_event, event
from pymysqlreplication.column import Column
from pymysqlreplication.constants.BINLOG import TABLE_MAP_EVENT, ROTATE_EVENT, QUERY_EVENT
from pymysqlreplication.event import (QueryEvent, RotateEvent, BinLogEvent)
from pymysqlreplication.packet import BinLogPacketWrapper
from pymysqlreplication.table import Table

from mysql.replication import common
from mysql.replication.ddl_parser import AlterStatementParser, TableChangeType, TableSchemaChange

try:
    from pymysql.constants.COMMAND import COM_BINLOG_DUMP_GTID
except ImportError:
    # Handle old pymysql versions
    # See: https://github.com/PyMySQL/PyMySQL/pull/261
    COM_BINLOG_DUMP_GTID = 0x1e

# 2013 Connection Lost
# 2006 MySQL server has gone away
MYSQL_EXPECTED_ERROR_CODES = [2013, 2006]


class SchemaOffsyncError(Exception):
    pass


class TableColumnSchemaCache:
    """
    Container object for table Schema cache. Caches table column schema based on table name.

    The column schema object is a dictionary:

    ```
                   stream-id: {
                        'COLUMN_NAME': 'name',
                        'ORDINAL_POSITION': 1,
                        'COLLATION_NAME': None,
                        'CHARACTER_SET_NAME': None,
                        'COLUMN_COMMENT': None,
                        'COLUMN_TYPE': 'BLOB',
                        'COLUMN_KEY': ''
                    }
    ```

    """

    def __init__(self, table_schema_cache: dict, table_schema_current: dict = None):
        """

        Args:
            table_schema_cache: Column schemas cached from the last time. It needs to be updated with each ALTER event.
            table_schema_current:
                internal cache of column schema that is actual at the time of execution
                (possibly newer than table_schema_cache)
        """
        self.table_schema_cache = table_schema_cache or {}
        self.__table_indexes = {}

        # internal cache of column schema that is actual at the time of execution
        #         (possibly newer than table_schema_cache)
        self._table_schema_current = table_schema_current or {}

    def _get_db_default_schema(self):
        table_schema = {}
        if self._table_schema_current:
            key = next(iter(self._table_schema_current))
            current_schema = self._table_schema_current[key]
            if current_schema:
                table_schema = current_schema[0]

        if not table_schema.get('DEFAULT_CHARSET'):
            logging.warning('No default charset found, using utf8',
                            extra={"full_message": self._table_schema_current})
        return table_schema.get('DEFAULT_CHARSET', 'utf8')

    @staticmethod
    def build_column_schema(column_name: str, ordinal_position: int, column_type: str, is_primary_key: bool,
                            collation=None,
                            character_set_name=None, column_comment=None) -> dict:
        if is_primary_key:
            key = 'PRI'
        else:
            key = ''
        return {
            'COLUMN_NAME': column_name,
            'ORDINAL_POSITION': ordinal_position,
            'COLLATION_NAME': collation,
            'CHARACTER_SET_NAME': character_set_name,
            'COLUMN_COMMENT': column_comment,
            'COLUMN_TYPE': column_type,
            'COLUMN_KEY': key
        }

    def is_current_schema_cached(self, schema: str, table: str):
        if self._table_schema_current.get(self.get_table_cache_index(schema, table)):
            return True
        else:
            return False

    def update_current_schema_cache(self, schema: str, table: str, column_schema: []):
        """
        Update internal cache of column schema that is actual at the time of execution
        (possibly newer than table_schema_cache)
        Args:
            column_schema:

        Returns:

        """
        index = self.get_table_cache_index(schema, table)
        self._table_schema_current[index] = column_schema

    def get_column_schema(self, schema: str, table: str) -> List[dict]:
        index = self.get_table_cache_index(schema, table)
        return self.table_schema_cache.get(index)

    def update_table_ids_cache(self, schema: str, table: str, mysql_table_id: int):
        """
        Keeps MySQL internal table_ids cached

        Args:
            schema:
            table:
            mysql_table_id: Internal Mysql table id

        Returns:

        """
        index = self.get_table_cache_index(schema, table)
        if not self.__table_indexes.get(index):
            self.__table_indexes[index] = set()

        self.__table_indexes[index].add(mysql_table_id)

    def invalidate_table_ids_cache(self):
        self.__table_indexes = {}

    def get_table_ids(self, schema: str, table: str):
        """
        Returns internal table ID from cache.
        Args:
            schema:
            table:

        Returns:

        """
        index = self.get_table_cache_index(schema, table)
        return self.__table_indexes.get(index, [])

    def set_column_schema(self, schema: str, table: str, column_schema: List[dict]):
        index = self.get_table_cache_index(schema, table)
        self.table_schema_cache[index] = column_schema

    def get_table_cache_index(self, schema: str, table: str):
        # index as not case sensitive, use the same method as the tap_stream_id
        return common.generate_tap_stream_id(schema, table)

    def update_cache(self, table_change: TableSchemaChange):
        """
        Updates schema cache based on table changes.

        Args:
            table_changes:

        Returns:

        """

        if table_change.type == TableChangeType.DROP_COLUMN:
            self.update_cache_drop_column(table_change)
        elif table_change.type == TableChangeType.ADD_COLUMN:
            self.update_cache_add_column(table_change)

    def update_cache_drop_column(self, drop_change: TableSchemaChange):
        index = self.get_table_cache_index(drop_change.schema, drop_change.table_name)
        column_schema = self.table_schema_cache.get(index, [])

        # drop column if exists
        drop_at_position = None
        update_ordinal_position = False
        new_schema = []
        # 1 based
        for idx, col in enumerate(column_schema, start=1):
            if col['COLUMN_NAME'].upper() == drop_change.column_name.upper():
                # mark and skip
                update_ordinal_position = True
                continue

            if update_ordinal_position:
                # shift ordinal position
                col['ORDINAL_POSITION'] = idx - 1
            new_schema.append(col)

        if not update_ordinal_position:
            raise SchemaOffsyncError(f'Dropped column: "{drop_change.column_name}" '
                                     f'is already missing in the provided starting schema => may lead to value shift!')

        if not column_schema:
            # should not happen
            raise SchemaOffsyncError(f'Table {index} not found in the provided table schema cache!')

        self.table_schema_cache[index] = new_schema

    def __add_column_at_position(self, original_schema, added_column, after_col):
        new_schema = []
        # add column if not exists
        update_ordinal_position = False
        # 1 based
        for idx, col in enumerate(original_schema, start=1):

            # on first position
            if idx == 1 and after_col == '':
                added_column['ORDINAL_POSITION'] = 1
                update_ordinal_position = True
                new_schema.append(col)

            # after specific
            elif after_col and col['COLUMN_NAME'].upper() == after_col.upper():
                added_column['ORDINAL_POSITION'] = idx + 1
                # mark and add both
                new_schema.append(col)
                new_schema.append(added_column)
                update_ordinal_position = True

            elif update_ordinal_position:
                # shift ordinal position of others
                col['ORDINAL_POSITION'] = idx + 1
                new_schema.append(col)
            else:
                # otherwise append unchanged
                new_schema.append(col)

        if not update_ordinal_position:
            raise SchemaOffsyncError(f'Dropped column: "{added_column["COLUMN_NAME"]}" '
                                     f'is already missing in the provided starting schema => may lead to value shift!')
        return new_schema

    def update_cache_add_column(self, add_change: TableSchemaChange):
        index = self.get_table_cache_index(add_change.schema, add_change.table_name)
        column_schema = self.table_schema_cache.get(index, [])

        logging.debug(f'Current column schema cache: {self.table_schema_cache}, index: {index}')
        column_names = [c['COLUMN_NAME'].upper() for c in column_schema]
        if not column_names:
            raise RuntimeError(f'The schema cache for table {index} is not initialized!')

        if add_change.column_name.upper() in column_names:
            logging.warning(f'The added column "{add_change.column_name.upper()}" is already present '
                            f'in the schema "{index}", skipping.')
            return
        logging.debug(f"New schema ADD change received {add_change}")
        added_column = self._build_new_column_schema(add_change)
        logging.debug(f"New column schema built {added_column}")
        new_schema = []
        # get after_column
        if add_change.first_position:
            after_col = ''
        elif add_change.after_column:
            after_col = add_change.after_column
        else:
            # add after last
            added_column['ORDINAL_POSITION'] = len(column_schema) + 1
            column_schema.append(added_column)
            new_schema = column_schema

        # exit
        if not new_schema:
            new_schema = self.__add_column_at_position(column_schema, added_column, after_col)

        if not column_schema:
            # should not happen
            raise SchemaOffsyncError(f'Table {index} not found in the provided table schema cache!')

        self.table_schema_cache[index] = new_schema

    def _build_new_column_schema(self, table_change: TableSchemaChange) -> dict:
        index = self.get_table_cache_index(table_change.schema, table_change.table_name)

        current_schema = self._table_schema_current.get(index, [])
        if not current_schema:
            logging.warning(f'Table {table_change.table_name} not found in current schema cache.',
                            extra={'full_message': self._table_schema_current})
        existing_col = None

        # check if column exists in current schema
        # this allows to get all column metadata properly in case
        # we missed some ALTER COLUMN statement, e.g. for changing datatypes
        for c in current_schema:
            logging.debug(
                f"Added column '{table_change.column_name.upper()}' "
                f"exists in the current schema: {current_schema}")
            if c['COLUMN_NAME'].upper() == table_change.column_name.upper():
                # convert name to upper_case just in case
                # TODO: consider moving this to current_schema build-up
                c['COLUMN_NAME'] = c['COLUMN_NAME'].upper()
                existing_col = c

        if existing_col:
            new_column = existing_col
        else:
            # add metadata from the ALTER event
            is_pkey = table_change.column_key == 'PRI'

            charset_name = table_change.charset_name or self._get_db_default_schema()
            new_column = self.build_column_schema(column_name=table_change.column_name.upper(),
                                                  # to be updated later
                                                  ordinal_position=0,
                                                  column_type=table_change.data_type,
                                                  is_primary_key=is_pkey,
                                                  collation=table_change.collation,
                                                  character_set_name=charset_name
                                                  )

        return new_column


class BinLogStreamReaderAlterTracking(BinLogStreamReader):
    """Connect to replication stream and read event.
    Modification of default BinLogStreamReader, that is capable of handling schema changes (e.g. column drops)
    properly by processing ALTER query events.

    The client application needs to keep and pass the current schema of the replicated table to the Reader
    in order to track changes properly. The client application needs to collect the latest schema cache from the
    `BinLogStreamReaderAlterTracking.table_schema_cache` attribute after each sync.

    """
    report_slave = None

    def __init__(self, connection_settings, server_id,
                 ctl_connection_settings=None, resume_stream=False,
                 blocking=False, only_events=None, log_file=None,
                 log_pos=None, filter_non_implemented_events=True,
                 ignored_events=None, auto_position=None,
                 only_tables=None, ignored_tables=None,
                 only_schemas=None, ignored_schemas=None,
                 freeze_schema=False, skip_to_timestamp=None,
                 report_slave=None, slave_uuid=None,
                 pymysql_wrapper=None,
                 fail_on_table_metadata_unavailable=False,
                 slave_heartbeat=None, table_schema_cache: dict = None):
        """
        Attributes:
            ctl_connection_settings: Connection settings for cluster holding
                                     schema information
            resume_stream: Start for event from position or the latest event of
                           binlog or from older available event
            blocking: When master has finished reading/sending binlog it will
                      send EOF instead of blocking connection.
            only_events: Array of allowed events
            ignored_events: Array of ignored events
            log_file: Set replication start log file
            log_pos: Set replication start log pos (resume_stream should be
                     true)
            auto_position: Use master_auto_position gtid to set position
            only_tables: An array with the tables you want to watch (only works
                         in binlog_format ROW)
            ignored_tables: An array with the tables you want to skip
            only_schemas: An array with the schemas you want to watch
            ignored_schemas: An array with the schemas you want to skip
            freeze_schema: If true do not support ALTER TABLE. It's faster.
            skip_to_timestamp: Ignore all events until reaching specified
                               timestamp.
            report_slave: Report slave in SHOW SLAVE HOSTS.
            slave_uuid: Report slave_uuid in SHOW SLAVE HOSTS.
            fail_on_table_metadata_unavailable: Should raise exception if we
                                                can't get table information on
                                                row_events
            slave_heartbeat: (seconds) Should master actively send heartbeat on
                             connection. This also reduces traffic in GTID
                             replication on replication resumption (in case
                             many event to skip in binlog). See
                             MASTER_HEARTBEAT_PERIOD in mysql documentation
                             for semantics
            table_schema_cache: Latest schemas of the synced tables (from previous execution). Indexed by table name.

        """
        super().__init__(connection_settings, server_id,
                         ctl_connection_settings=ctl_connection_settings,
                         resume_stream=resume_stream,
                         blocking=blocking,
                         only_events=only_events,
                         log_file=log_file,
                         log_pos=log_pos,
                         filter_non_implemented_events=filter_non_implemented_events,
                         ignored_events=ignored_events,
                         auto_position=auto_position,
                         only_tables=only_tables,
                         ignored_tables=ignored_tables,
                         only_schemas=only_schemas,
                         ignored_schemas=ignored_schemas,
                         freeze_schema=freeze_schema,
                         skip_to_timestamp=skip_to_timestamp,
                         report_slave=report_slave,
                         slave_uuid=slave_uuid,
                         pymysql_wrapper=pymysql_wrapper,
                         fail_on_table_metadata_unavailable=fail_on_table_metadata_unavailable,
                         slave_heartbeat=slave_heartbeat)

        # We can't filter on packet level TABLE_MAP, Query and rotate event because
        # we need them for handling other operations. Add custom events
        self._BinLogStreamReader__allowed_events_in_packet = frozenset(
            [TableMapEventAlterTracking, RotateEvent, QueryEventWithSchemaChanges]).union(
            self._BinLogStreamReader__allowed_events)
        # expose
        self.__allowed_events = self._BinLogStreamReader__allowed_events

        # Store table meta information cached from the last time
        self.schema_cache = TableColumnSchemaCache(table_schema_cache)

        self.alter_parser = AlterStatementParser()

        # init just in case of redeploying new version
        self._init_column_schema_cache(only_schemas[0], only_tables)

    def _init_column_schema_cache(self, schema: str, tables: List[str]):
        """
        Helper method to initi schema cache in case the extraction started with empty state
        Returns:

        """
        for table in tables:
            # hacky way to call the parent secret method
            if self.schema_cache.get_column_schema(schema, table):
                continue
            logging.warning(f"Schema for table {schema}-{table} is not initialized, using current schema")
            current_column_schema = self._get_table_information_from_db(schema, table)
            # update cache with current schema
            self.schema_cache.set_column_schema(schema, table, current_column_schema)

            self.schema_cache.update_current_schema_cache(schema, table, current_column_schema)

    def fetchone(self):
        while True:
            if not self._BinLogStreamReader__connected_stream:
                self._BinLogStreamReader__connect_to_stream()

            if not self._BinLogStreamReader__connected_ctl:
                self._BinLogStreamReader__connect_to_ctl()

            try:
                if pymysql.__version__ < "0.6":
                    pkt = self._stream_connection.read_packet()
                else:
                    pkt = self._stream_connection._read_packet()
            except pymysql.OperationalError as error:
                code, message = error.args
                if code in MYSQL_EXPECTED_ERROR_CODES:
                    self._stream_connection.close()
                    self._BinLogStreamReader__connected_stream = False
                    continue
                raise

            if pkt.is_eof_packet():
                self.close()
                return None

            if not pkt.is_ok_packet():
                continue

            binlog_event = BinLogPacketWrapperModified(pkt, self.table_map,
                                                       self._ctl_connection,
                                                       self._BinLogStreamReader__use_checksum,
                                                       self._BinLogStreamReader__allowed_events_in_packet,
                                                       self._BinLogStreamReader__only_tables,
                                                       self._BinLogStreamReader__ignored_tables,
                                                       self._BinLogStreamReader__only_schemas,
                                                       self._BinLogStreamReader__ignored_schemas,
                                                       self._BinLogStreamReader__freeze_schema,
                                                       self._BinLogStreamReader__fail_on_table_metadata_unavailable)

            if binlog_event.event_type == ROTATE_EVENT:
                self.log_pos = binlog_event.event.position
                self.log_file = binlog_event.event.next_binlog
                # Table Id in binlog are NOT persistent in MySQL - they are in-memory identifiers
                # that means that when MySQL master restarts, it will reuse same table id for different tables
                # which will cause errors for us since our in-memory map will try to decode row data with
                # wrong table schema.
                # The fix is to rely on the fact that MySQL will also rotate to a new binlog file every time it
                # restarts. That means every rotation we see *could* be a sign of restart and so potentially
                # invalidates all our cached table id to schema mappings. This means we have to load them all
                # again for each logfile which is potentially wasted effort but we can't really do much better
                # without being broken in restart case
                self.table_map = {}
                self.schema_cache.invalidate_table_ids_cache()
            elif binlog_event.log_pos:
                self.log_pos = binlog_event.log_pos

            # This check must not occur before clearing the ``table_map`` as a
            # result of a RotateEvent.
            #
            # The first RotateEvent in a binlog file has a timestamp of
            # zero.  If the server has moved to a new log and not written a
            # timestamped RotateEvent at the end of the previous log, the
            # RotateEvent at the beginning of the new log will be ignored
            # if the caller provided a positive ``skip_to_timestamp``
            # value.  This will result in the ``table_map`` becoming
            # corrupt.
            #
            # https://dev.mysql.com/doc/internals/en/event-data-for-specific-event-types.html
            # From the MySQL Internals Manual:
            #
            #   ROTATE_EVENT is generated locally and written to the binary
            #   log on the master. It is written to the relay log on the
            #   slave when FLUSH LOGS occurs, and when receiving a
            #   ROTATE_EVENT from the master. In the latter case, there
            #   will be two rotate events in total originating on different
            #   servers.
            #
            #   There are conditions under which the terminating
            #   log-rotation event does not occur. For example, the server
            #   might crash.
            if self.skip_to_timestamp and binlog_event.timestamp < self.skip_to_timestamp:
                continue

            if binlog_event.event_type == TABLE_MAP_EVENT and \
                    binlog_event.event is not None:
                table_obj = binlog_event.event.get_table()
                self.table_map[binlog_event.event.table_id] = table_obj

                # store current schema
                self._update_current_schema(table_obj.schema, table_obj.table)

                # store internal table ids for convenience
                self.schema_cache.update_table_ids_cache(table_obj.schema, table_obj.table,
                                                         table_obj.table_id)

            # Process ALTER events and update schema cache so the mapping works properly
            if binlog_event.event_type == QUERY_EVENT and 'ALTER' in binlog_event.event.query.upper():
                table_changes = self._update_cache_and_map(binlog_event.event)
                binlog_event.event.schema_changes = table_changes

            # event is none if we have filter it on packet level
            # we filter also not allowed events
            if binlog_event.event is None or (binlog_event.event.__class__ not in self.__allowed_events):
                continue

            return binlog_event.event

    def _update_current_schema(self, schema, table):
        """
        Keeps current schema (at the point of execution) for later reference
        """
        if not self.schema_cache.is_current_schema_cached(schema, table):
            column_schema = self._get_table_information_from_db(schema, table)
            self.schema_cache.update_current_schema_cache(schema, table, column_schema)

    def _update_cache_and_map(self, binlog_event: QueryEvent):
        """
        Updates schema cache based on given ALTER events. Refreshes the table_map
        """
        table_changes = self.alter_parser.get_table_changes(binlog_event.query, binlog_event.schema.decode())
        monitored_changes = []
        for table_change in table_changes:
            # normalize table_name. We expect table names in lower case here.
            table_change.table_name = table_change.table_name.lower()
            table_change.schema = table_change.schema.lower()
            # only monitored tables
            logging.debug(
                f'Table change detected: {table_change}, monitored tables: {self._BinLogStreamReader__only_tables}, '
                f'monitored schemas: {self._BinLogStreamReader__only_schemas}')
            if table_change.table_name not in self._BinLogStreamReader__only_tables:
                continue
            # only monitored schemas
            if table_change.schema not in self._BinLogStreamReader__only_schemas:
                continue
            monitored_changes.append(table_change)
            self.schema_cache.update_cache(table_change)

            # invalidate table_map cache so it is rebuilt from schema cache next TABLE_MAP_EVENT
            self._invalidate_table_map(table_change.schema, table_change.table_name)
        if not monitored_changes:
            logging.debug(f"ALTER statements detected, but no table change recognised.")
        return monitored_changes

    def _invalidate_table_map(self, schema: str, table_name: str):
        indexes = self.schema_cache.get_table_ids(schema, table_name)
        for i in indexes:
            self.table_map.pop(i, None)

    def _BinLogStreamReader__get_table_information(self, schema, table):
        """ Uglily overridden BinLogStreamReader private method via the "name mangling" black magic.
            Get table information from Cache or fetch the current state.

            This is being used in the TableMapEvent to get the schema if not present in the cache.
            It allows to retrieve schema from the cache if present.

        """
        if self.schema_cache.get_column_schema(schema, table):
            return self._get_column_schema_from_cache(schema, table)
        else:
            # hacky way to call the parent secret method
            current_column_schema = self._get_table_information_from_db(schema, table)
            # update cache with current schema
            self.schema_cache.set_column_schema(schema, table, current_column_schema)

            # TODO: consider moving this to the binlog init so it's in sync and done only once
            self.schema_cache.update_current_schema_cache(schema, table, current_column_schema)
            return current_column_schema

    def _table_info_backoff(func):
        @functools.wraps(func)
        @backoff.on_exception(backoff.expo,
                              pymysql.OperationalError,
                              max_tries=3)
        def wrapper(self, *args, **kwargs):
            if not self._BinLogStreamReader__connected_ctl:
                logging.warning('Mysql unreachable, trying to reconnect')
                self._BinLogStreamReader__connect_to_ctl()
            return func(self, *args, **kwargs)

        return wrapper

    @_table_info_backoff
    def _get_table_information_from_db(self, schema, table):
        for i in range(1, 3):
            try:
                if not self._BinLogStreamReader__connected_ctl:
                    self._BinLogStreamReader__connect_to_ctl()

                cur = self._ctl_connection.cursor()
                query = cur.mogrify("""SELECT
                        COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME,
                        COLUMN_COMMENT, COLUMN_TYPE, COLUMN_KEY, ORDINAL_POSITION, defaults.DEFAULT_COLLATION_NAME,
                        defaults.DEFAULT_CHARSET
                    FROM
                        information_schema.columns col
                    JOIN (SELECT
                              default_character_set_name AS DEFAULT_CHARSET
                            , DEFAULT_COLLATION_NAME     AS DEFAULT_COLLATION_NAME
                        FROM information_schema.SCHEMATA
                        WHERE
                            SCHEMA_NAME = %s) as defaults ON 1=1
                    WHERE
                        table_schema = %s AND table_name = %s
                    ORDER BY ORDINAL_POSITION;
                    """, (schema, schema, table))
                logging.debug(query)
                cur.execute(query)
                return cur.fetchall()
            except pymysql.OperationalError as error:
                code, message = error.args
                if code in MYSQL_EXPECTED_ERROR_CODES:
                    self._BinLogStreamReader__connected_ctl = False
                    raise pymysql.OperationalError("Getting the initial schema failed, server unreachable!") from error
                else:
                    raise error

    def _get_column_schema_from_cache(self, schema: str, table: str):
        column_schema = self.schema_cache.get_column_schema(schema, table)
        # convert to upper case
        for c in column_schema:
            c['COLUMN_NAME'] = c['COLUMN_NAME'].upper()

        return column_schema


class TableMapEventAlterTracking(BinLogEvent):
    """This evenement describe the structure of a table.
    It's sent before a change happens on a table.
    An end user of the lib should have no usage of this.

    Modified to work with table schema cache, regularly updated from ALTER events.

    Converts column names to upper case.

    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super(TableMapEventAlterTracking, self).__init__(from_packet, event_size,
                                                         table_map, ctl_connection, **kwargs)
        self.__only_tables = kwargs["only_tables"]
        self.__ignored_tables = kwargs["ignored_tables"]
        self.__only_schemas = kwargs["only_schemas"]
        self.__ignored_schemas = kwargs["ignored_schemas"]

        # Post-Header
        self.table_id = self._read_table_id()

        self.flags = struct.unpack('<H', self.packet.read(2))[0]

        # Payload
        self.schema_length = byte2int(self.packet.read(1))
        self.schema = self.packet.read(self.schema_length).decode()
        self.packet.advance(1)
        self.table_length = byte2int(self.packet.read(1))
        self.table = self.packet.read(self.table_length).decode()

        if self.__only_tables is not None and self.table not in self.__only_tables:
            self._processed = False
            return
        elif self.__ignored_tables is not None and self.table in self.__ignored_tables:
            self._processed = False
            return

        if self.__only_schemas is not None and self.schema not in self.__only_schemas:
            self._processed = False
            return
        elif self.__ignored_schemas is not None and self.schema in self.__ignored_schemas:
            self._processed = False
            return

        self.packet.advance(1)
        self.column_count = self.packet.read_length_coded_binary()

        self.columns = []

        if self.table_id in table_map:
            self.column_schemas = table_map[self.table_id].column_schemas
        else:
            self.column_schemas = self._ctl_connection._get_table_information(self.schema, self.table)

        ordinal_pos_loc = 0

        if len(self.column_schemas) != 0:
            # Read columns meta data
            column_types = list(self.packet.read(self.column_count))
            self.packet.read_length_coded_binary()
            # TODO: fail on different schema size against cache
            for i in range(0, len(column_types)):
                column_type = column_types[i]
                try:
                    column_schema = self.column_schemas[ordinal_pos_loc]

                    # normalize header
                    column_schema['COLUMN_NAME'] = column_schema['COLUMN_NAME'].upper()

                    # only acknowledge the column definition if the iteration matches with ordinal position of
                    # the column. this helps in maintaining support for restricted columnar access
                    if i != (column_schema['ORDINAL_POSITION'] - 1):
                        # raise IndexError to follow the workflow of dropping columns which are not matching the
                        # underlying table schema
                        raise IndexError

                    ordinal_pos_loc += 1
                except IndexError:
                    # this should not happen as the Schema should be reconstructed from the Cache
                    raise SchemaOffsyncError(f"The schema of the table {self.schema}.{self.table} "
                                             f"is off-sync (e.g. column missing). "
                                             f"It is not possible to safely replicate the values. "
                                             f"Please run the full sync-first or restore the state "
                                             f"to include schema matching the last execution time and provide it "
                                             f"within the table_schema_cache parameter. ")

                col = Column(byte2int(column_type), column_schema, from_packet)
                self.columns.append(col)

        self.table_obj = Table(self.column_schemas, self.table_id, self.schema,
                               self.table, self.columns)

    def get_table(self):
        return self.table_obj

    def _dump(self):
        super(TableMapEventAlterTracking, self)._dump()
        print("Table id: %d" % (self.table_id))
        print("Schema: %s" % (self.schema))
        print("Table: %s" % (self.table))
        print("Columns: %s" % (self.column_count))


class QueryEventWithSchemaChanges(QueryEvent):
    """
    Query event wrapper that contains list of Alter Table Changes.


    """

    class QueryType(Enum):
        QUERY = 'query'
        ALTER_QUERY = 'alter_query'

    def __init__(self, from_packet, event_size, table_map,
                 ctl_connection, **kwargs):
        super().__init__(from_packet, event_size, table_map,
                         ctl_connection, **kwargs)

        self._schema_changes = []
        # default event type
        self.event_type = QueryEventWithSchemaChanges.QueryType.QUERY

    @property
    def schema_changes(self) -> List[TableSchemaChange]:
        return self._schema_changes

    @schema_changes.setter
    def schema_changes(self, schema_changes: List[TableSchemaChange]):
        self.event_type = QueryEventWithSchemaChanges.QueryType.ALTER_QUERY
        self._schema_changes = schema_changes


class BinLogPacketWrapperModified(BinLogPacketWrapper):
    """
    Modified version of BinLogPacketWrapper to include custom TABLE_MAP event class
    Bin Log Packet Wrapper. It uses an existing packet object, and wraps
    around it, exposing useful variables while still providing access
    to the original packet objects variables and methods.
    """

    _BinLogPacketWrapper__event_map = {
        # event
        constants.QUERY_EVENT: QueryEventWithSchemaChanges,
        constants.ROTATE_EVENT: event.RotateEvent,
        constants.FORMAT_DESCRIPTION_EVENT: event.FormatDescriptionEvent,
        constants.XID_EVENT: event.XidEvent,
        constants.INTVAR_EVENT: event.IntvarEvent,
        constants.GTID_LOG_EVENT: event.GtidEvent,
        constants.STOP_EVENT: event.StopEvent,
        constants.BEGIN_LOAD_QUERY_EVENT: event.BeginLoadQueryEvent,
        constants.EXECUTE_LOAD_QUERY_EVENT: event.ExecuteLoadQueryEvent,
        constants.HEARTBEAT_LOG_EVENT: event.HeartbeatLogEvent,
        # row_event
        constants.UPDATE_ROWS_EVENT_V1: row_event.UpdateRowsEvent,
        constants.WRITE_ROWS_EVENT_V1: row_event.WriteRowsEvent,
        constants.DELETE_ROWS_EVENT_V1: row_event.DeleteRowsEvent,
        constants.UPDATE_ROWS_EVENT_V2: row_event.UpdateRowsEvent,
        constants.WRITE_ROWS_EVENT_V2: row_event.WriteRowsEvent,
        constants.DELETE_ROWS_EVENT_V2: row_event.DeleteRowsEvent,

        # MODIFIED TABLE MAP
        constants.TABLE_MAP_EVENT: TableMapEventAlterTracking,
        # 5.6 GTID enabled replication events
        constants.ANONYMOUS_GTID_LOG_EVENT: event.NotImplementedEvent,
        constants.PREVIOUS_GTIDS_LOG_EVENT: event.NotImplementedEvent

    }
