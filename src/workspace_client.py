import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Protocol

from db_common.db_connection import ODBCConnection


@dataclass
class Credentials:
    account: str
    user: str
    password: str
    warehouse: str
    database: str = None
    schema: str = None
    role: str = None


class ExtractorUserException(Exception):
    pass


class WorkspaceStagingClient(Protocol):
    def connect(self):
        """Connect to staging"""

    def test_connection(self):
        self.connect()
        self.close_connection()

    def close_connection(self):
        pass

    def execute_query(self, query: str) -> list[dict]:
        """
        executes the query
        """

    def create_table(self, name, columns: [dict]):
        pass

    def copy_csv_into_table_from_file(self, table_name: str, table_columns: list[str], column_types: list[dict],
                                      csv_file_path: str,
                                      skip_header=True):
        """
        Import from file by default CSV format with skip header setup and ERROR_ON_COLUMN_COUNT_MISMATCH=false.

        Args:
            table_name:
            table_columns:
            column_types: types used for transformation
            csv_file_path:
            skip_header:
        """

    def dedupe_stage_table(self, table_name: str, id_columns: list[str]):
        """
        Dedupe staging table and keep only latest records.
        Based on the internal column kbc__event_order produced by CDC engine
        Args:
            table_name:
            id_columns:

        Returns:"""

    def wrap_columns_in_quotes(self, columns: list[str]) -> list[str]:
        pass

    def wrap_in_quote(self, s: str) -> str:
        pass


class DuckDBClient(WorkspaceStagingClient):

    def __init__(self):
        connection_string = self._build_connection_string()
        self._connection = ODBCConnection(connection_string)

    @staticmethod
    def _build_connection_string() -> str:
        return "Driver={DuckDB Driver}"

    @contextmanager
    def connect(self) -> 'DuckDBClient':
        logging.info("Connecting to database.")
        try:
            self._connection.connect()
            self.execute_query("SET temp_directory	='/tmp/dbtmp';")
            # self.execute_query("SET threads = 1;")
            self.execute_query("SET memory_limit='2GB';")
            yield self

        except Exception as e:
            raise ExtractorUserException(f"Login to database failed, please check your credentials. Detail: {e}") from e
        finally:
            self.close_connection()

    def test_connection(self):
        self.connect()
        self.close_connection()

    def close_connection(self):
        logging.debug("Closing the outer connection.")
        self._connection.connection.close()

    def execute_query(self, query):
        """
        executes the query
        """
        results = list(self._connection.perform_query(query))
        logging.debug(results)
        return results

    def create_table(self, name, columns: [dict]):
        query = f"""
        CREATE OR REPLACE TABLE {self.wrap_in_quote(name)} (
        """
        col_defs = [f"\"{col['name']}\" {col['type']}" for col in columns]
        query += ', '.join(col_defs)
        query += ");"
        self.execute_query(query)

    def create_temp_stage(self, name: str, file_format: dict = None):
        if not file_format:
            file_format = self.DEFAULT_FILE_FORMAT
        query = f"""
        CREATE OR REPLACE TEMP STAGE  "{name}"
        """

        query += " FILE_FORMAT = ("
        for key in file_format:
            query += f"{key}={file_format[key]} "
        query += ");"

        self.execute_query(query)

    def copy_csv_into_table_from_file(self, table_name: str, table_columns: list[str], column_types: list[dict],
                                      csv_file_path: str,
                                      file_format: dict = None):
        """
        Import from file by default CSV format with skip header setup and ERROR_ON_COLUMN_COUNT_MISMATCH=false.

        Args:
            table_name:
            table_columns:
            column_types: types used for transformation
            csv_file_path:
            file_format:
        """

        # create temp stage
        self.create_temp_stage(table_name, file_format)
        # copy to stage
        stage_sql = f"PUT file://{csv_file_path} @\"{table_name}\" AUTO_COMPRESS=TRUE;"
        self.execute_query(stage_sql)

        # insert data
        table_name = self.wrap_in_quote(table_name)
        columns = self.wrap_columns_in_quotes(table_columns)

        columns_converted = [self._convert_nulls(f'${idx + 1}', col.get('convert_nulls', False)) for idx, col in
                             enumerate(column_types)]
        query = f"COPY INTO {table_name} ({', '.join(columns)}) FROM " \
                f"(SELECT {','.join(columns_converted)} FROM @{table_name})"
        query += " FILE_FORMAT = ("
        for key in file_format:
            query += f"{key}={file_format[key]} "
        query += ");"
        self.execute_query(query)
        self.execute_query('commit')

    def _convert_nulls(self, col_name: str, convert: bool):
        col_def = col_name
        if convert:
            col_def = f"NULLIF({col_def},'')"
        return col_def

    def wrap_columns_in_quotes(self, columns):
        return [self.wrap_in_quote(col) for col in columns]

    def wrap_in_quote(self, s):
        return s if s.startswith('"') else '"' + s + '"'
