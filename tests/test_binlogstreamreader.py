import unittest

from mock import patch

from mysql.replication.stream_reader import BinLogStreamReaderAlterTracking


class TestComponent(unittest.TestCase):

    def setUp(self) -> None:
        self.cached_table = 'test1'
        self.cached_schema = 'schema1'
        self.cached_table_index = f"{self.cached_schema}-{self.cached_table}"
        self.mock_stream_reader: BinLogStreamReaderAlterTracking = BinLogStreamReaderAlterTracking({}, '', only_tables=[
            self.cached_table],
                                                                                                   only_schemas=[
                                                                                                       self.cached_schema],
                                                                                                   table_schema_cache={
                                                                                                       f"{self.cached_schema}-{self.cached_table}": [
                                                                                                           {
                                                                                                               'COLUMN_NAME': 'SOMECOL',
                                                                                                               'ORDINAL_POSITION': 1,
                                                                                                               'COLLATION_NAME': None,
                                                                                                               'CHARACTER_SET_NAME': None,
                                                                                                               'COLUMN_COMMENT': None,
                                                                                                               'COLUMN_TYPE': 'int(11)',
                                                                                                               'COLUMN_KEY': 'PRI'
                                                                                                           }]})

    @patch("pymysqlreplication.event.QueryEvent")
    def test_update_cache_different_schema_passes(self, event):
        event.schema = 'someotherschema'.encode('utf-8')
        event.query = 'ALTER TABLE someotherschema.test DROP COLUMN SOMECOL'
        self.mock_stream_reader._update_cache_and_map(event)

    @patch("pymysqlreplication.event.QueryEvent")
    def test_update_cache_different_table_same_schema_passes(self, event):
        event.schema = 'someotherschema'.encode('utf-8')
        event.query = 'ALTER TABLE schema1.test2 DROP COLUMN SOMECOL'
        self.mock_stream_reader._update_cache_and_map(event)

    @patch("pymysqlreplication.event.QueryEvent")
    def test_update_cache_results_in_cache_updated(self, event):
        event.schema = self.cached_schema.encode('utf-8')
        event.query = f'ALTER TABLE {self.cached_schema}.{self.cached_table} DROP COLUMN SOMECOL'
        self.mock_stream_reader._update_cache_and_map(event)
        self.assertEqual(self.mock_stream_reader.schema_cache.table_schema_cache,
                         {f"{self.cached_schema}-{self.cached_table}": []})

    @patch("pymysqlreplication.event.QueryEvent")
    def test_update_cache_add_existing_column_in_schema_column_name_uppercase(self, event):
        event.schema = self.cached_schema.encode('utf-8')
        event.query = f'ALTER TABLE {self.cached_schema}.{self.cached_table} ADD COLUMN newcol TINYINT(1)'
        self.mock_stream_reader.schema_cache.update_current_schema_cache(self.cached_schema, self.cached_table, [{
            'COLUMN_NAME': 'newcol',
            'COLLATION_NAME': None,
            'CHARACTER_SET_NAME': None,
            'COLUMN_COMMENT': '',
            'COLUMN_TYPE': 'int(11)',
            'COLUMN_KEY': '',
            'ORDINAL_POSITION': 50,
            'DEFAULT_COLLATION_NAME': 'latin1_swedish_ci',
            'DEFAULT_CHARSET': 'latin1'
        }])

        expected_schema = [
            {'COLUMN_NAME': 'SOMECOL', 'ORDINAL_POSITION': 1, 'COLLATION_NAME': None, 'CHARACTER_SET_NAME': None,
             'COLUMN_COMMENT': None, 'COLUMN_TYPE': 'int(11)', 'COLUMN_KEY': 'PRI'},
            {'COLUMN_NAME': 'NEWCOL', 'COLLATION_NAME': None, 'CHARACTER_SET_NAME': None, 'COLUMN_COMMENT': '',
             'COLUMN_TYPE': 'int(11)', 'COLUMN_KEY': '', 'ORDINAL_POSITION': 2,
             'DEFAULT_COLLATION_NAME': 'latin1_swedish_ci', 'DEFAULT_CHARSET': 'latin1'}]

        self.mock_stream_reader._update_cache_and_map(event)
        self.assertEqual(self.mock_stream_reader.schema_cache.table_schema_cache[self.cached_table_index],
                         expected_schema)
