import unittest

from mock import patch

from mysql.replication.stream_reader import BinLogStreamReaderAlterTracking


class TestComponent(unittest.TestCase):

    def setUp(self) -> None:
        self.cached_table = 'test1'
        self.cached_schema = 'schema1'
        self.mock_stream_reader = BinLogStreamReaderAlterTracking({}, '', only_tables=[self.cached_table],
                                                                  only_schemas=[self.cached_schema],
                                                                  table_schema_cache={
                                                                      f"{self.cached_schema}-{self.cached_table}": [{
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
