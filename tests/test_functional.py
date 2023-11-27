import csv
import datetime
import glob
import os
import shutil
import tempfile
import unittest
from pathlib import Path

from datadirtest import DataDirTester, TestDataDir
from freezegun import freeze_time
from keboola.component import CommonInterface

from component import Component
from mysql.client import MySQLConnection
from mysql.replication import common
from tests.db_test_traits.db_test_traits import DbTestTable


class TestDatabaseEnvironment:

    def __init__(self, connection: MySQLConnection):
        self.connection = connection

    def perform_query(self, query: str):
        self.connection.query(query)

    def insert_rows(self, table: DbTestTable, row_data: list):
        # Create a cursor object
        cur = self.connection.cursor()
        values_str = ','.join(['%s' for c in table.columns])
        # Prepare the SQL query
        sql = f"INSERT INTO {table.table_name} VALUES ({values_str})"

        # Execute the query for each row
        cur.executemany(sql, row_data)

        # Commit the changes
        self.connection.commit()

        # Close the cursor and the connection
        cur.close()

    def prepare_initial_table(self, table_name: str, schema: str):
        self.connection.connect()
        self.perform_query(f'USE {schema}')
        table = DbTestTable.build_table(table_name)
        self.perform_query(f'DROP TABLE IF EXISTS `{table.table_name}`')
        self.perform_query(table.create_table_query)

        self.insert_rows(table, table.initial_rows)


class CustomDatadirTest(TestDataDir):
    def setUp(self):

        try:
            comp = Component(data_path_override=self.source_data_dir)
            comp.init_configuration()
            comp.init_connection_params()
            db_client = TestDatabaseEnvironment(MySQLConnection(comp.mysql_config_params))
        except Exception as e:
            raise e

        self.context_parameters['db_client'] = db_client
        super().setUp()

    def _create_temporary_copy(self):
        temp_dir = tempfile.mkdtemp(prefix=Path(self.orig_dir).name, dir='/tmp')
        dst_path = os.path.join(temp_dir, 'test_data')
        if os.path.exists(dst_path):
            shutil.rmtree(dst_path)
        if not os.path.exists(self.orig_dir):
            raise ValueError(f"{self.orig_dir} does not exist. ")
        shutil.copytree(self.orig_dir, dst_path)
        return dst_path

    @staticmethod
    def _remove_column_slice(table_path: str, column_slice: slice):
        tmp_path = f'{table_path}__tmp.csv'
        with open(table_path, 'r') as inp, open(tmp_path, 'w+') as outp:
            reader = csv.reader(inp)
            writer = csv.writer(outp, lineterminator='\n')
            for row in reader:
                writer.writerow(row[column_slice])

        os.remove(table_path)
        shutil.move(tmp_path, table_path)

    def _cleanup_result_data(self):
        """
        We cannot compare binlog read_at timestamp, so exclude these columns from the comparison.


        Returns:

        """
        # help with CI package
        ci = CommonInterface(self.source_data_dir)
        in_tables: list = glob.glob(f'{ci.tables_out_path}/*.csv')
        in_tables.extend(glob.glob(f'{ci.tables_out_path}/*.CSV'))

        # we now we need to remove last 2columns
        slice_to_keep = slice(0, -3)

        for in_table in in_tables:
            if not os.path.isdir(in_table):
                self._remove_column_slice(in_table, slice_to_keep)

    def run_component(self):
        super().run_component()
        self._cleanup_result_data()


class TestComponent(unittest.TestCase):
    @freeze_time("2023-11-03 14:50:42.833622")
    def test_functional(self):
        # freeze start date
        common.SYNC_STARTED_AT = datetime.datetime.fromisoformat('2023-11-03 14:50:42.833622')
        common.KBC_METADATA = (common.SYNC_STARTED_AT, None, 0, 0)
        functional_tests = DataDirTester(test_data_dir_class=CustomDatadirTest)
        functional_tests.run()


if __name__ == "__main__":
    unittest.main()
