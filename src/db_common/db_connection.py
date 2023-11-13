import inspect
import logging
from typing import Protocol, Optional, Iterable, Callable

import pypyodbc


def _get_method_args(method_name: Callable, method_locals) -> dict:
    """
    Helper function to return arguments passed to method as a dict
    Args:
        method_name: method
        method_locals: locals() result sent from the top of the method

    Returns:

    """
    signature_values = inspect.signature(method_name).parameters.values()

    return {parameter.name: method_locals[parameter.name] for parameter in signature_values}


class DbConnection(Protocol):

    def test_connection(self) -> None:
        """Raises Connection error on connection failure"""

    def connect(self) -> None:
        """Connect to database"""

    def perform_query(self, query: str, bind_parameters: Optional[dict] = None) -> Iterable[dict]:
        """Performs query"""


class ConnectionUserException(Exception):
    pass


class ODBCConnection(DbConnection):

    def __init__(self, connection_string: str):
        """

        Args:
             connection_string: Full qualified ODBC connection string.
        """
        self._connection: pypyodbc.Connection
        self.connection_string = connection_string
        self.connected = False

    def test_connection(self) -> None:
        """Raises Connection error on connection failure"""
        self.connect()
        self._connection.close()

    def connect(self) -> None:
        """Connect to database"""
        self._connection = pypyodbc.connect(self.connection_string)
        self.connected = True

    @property
    def connection(self) -> pypyodbc.Connection:
        if not self.connected:
            raise ConnectionUserException("The connection is not initialized, please call connect() method first.")

        return self._connection

    def _result_to_dict(self, result: tuple, header: list[str]) -> dict:
        res_dict = dict()
        for i, h in enumerate(header):
            res_dict[h] = result[i]
        return res_dict

    # def _call_jdbc_metadata_system_function(self, function_name: str, *args) -> Iterable[tuple]:
    #     """
    #     Helper method to get results from default metadata JDBC functions, e.g. getTables
    #     Args:
    #         function_name:
    #         **kwargs:
    #
    #     Returns:
    #
    #     """
    #     results = getattr(self._connection.jconn.getMetaData(), function_name)(*args)
    #     table_reader_cursor = self._connection.cursor()
    #     table_reader_cursor._rs = results
    #     table_reader_cursor._meta = results.getMetaData()
    #     for row in table_reader_cursor.fetchall():
    #         yield row
    #
    # def get_tables(self, catalog: str = None,
    #                schema_pattern: str = None,
    #                table_name_pattern: str = None,
    #                types: list[str] = None) -> Iterable[dict]:
    #     """
    #     Calls JDBC system function getTables to retrieve table metadata.
    #      (https://www.tutorialspoint.com/java-databasemetadata-gettables-method-with-example)
    #
    #     Args:
    #         catalog:
    #         schema_pattern: name or pattern with wildcard % e.g. '%_address'
    #         table_name_pattern: name or pattern with wildcard % e.g. '%_address'
    #         types: Types e.g. 'TYPE', 'TABLE', 'INDEX', 'VIEW', 'SYSTEM TOAST INDEX', 'SYSTEM TABLE', 'SYSTEM INDEX',
    #                           'SYSTEM VIEW', 'SEQUENCE'
    #
    #     Returns:
    #
    #     """
    #     all_argument_values = _get_method_args(self.get_tables, locals())
    #     header = ['TABLE_CAT',
    #               'TABLE_SCHEM',
    #               'TABLE_NAME',
    #               'TABLE_TYPE',
    #               'REMARKS',
    #               'TYPE_SCHEM',
    #               'TYPE_NAME',
    #               'SELF_REFERENCING_COL_NAME',
    #               'REF_GENERATION']
    #     for row in self._call_jdbc_metadata_system_function('getTables', *all_argument_values.values()):
    #         yield self._result_to_dict(row, header)
    #
    # def get_columns(self, catalog: str = None,
    #                 schema_pattern: str = None,
    #                 table_name_pattern: str = None,
    #                 column_name_pattern: str = None) -> Iterable[dict]:
    #     """
    #     Calls JDBC system function getColumns to retrieve table metadata.
    #      (https://www.tutorialspoint.com/java-databasemetadata-getcolumns-method-with-example)
    #
    #     Args:
    #         catalog:
    #         schema_pattern: name or pattern with wildcard % e.g. '%_address'
    #         table_name_pattern: name or pattern with wildcard % e.g. '%_address'
    #         column_name_pattern: name or pattern with wildcard % e.g. '%_address'
    #
    #     Returns:
    #
    #     """
    #     all_argument_values = _get_method_args(self.get_columns, locals())
    #     header = ['TABLE_CAT',
    #               'TABLE_SCHEM',
    #               'TABLE_NAME',
    #               'COLUMN_NAME',
    #               'DATA_TYPE',
    #               'TYPE_NAME',
    #               'COLUMN_SIZE',
    #               'REMARKS',
    #               'COLUMN_DEF',
    #               'ORDINAL_POSITION',
    #               'IS_AUTOINCREMENT',
    #               'IS_GENERATEDCOLUMN']
    #
    #     for row in self._call_jdbc_metadata_system_function('getColumns', *all_argument_values.values()):
    #         yield self._result_to_dict(row, header)
    #
    # def get_primary_keys(self, catalog: str = None,
    #                      schema: str = None,
    #                      table: str = None) -> Iterable[dict]:
    #     """
    #     Calls JDBC system function getColumns to retrieve table metadata.
    #      (https://www.tutorialspoint.com/java-databasemetadata-getcolumns-method-with-example)
    #
    #     Args:
    #         catalog: str
    #         schema: str
    #         table: str
    #
    #     Returns:
    #
    #     """
    #     all_argument_values = _get_method_args(self.get_primary_keys, locals())
    #     header = ['TABLE_CAT',
    #               'TABLE_SCHEM',
    #               'TABLE_NAME',
    #               'COLUMN_NAME',
    #               'PK_NAME']
    #
    #     for row in self._call_jdbc_metadata_system_function('getPrimaryKeys', *all_argument_values.values()):
    #         yield self._result_to_dict(row, header)
    #
    # def get_catalogs(self) -> Iterable[dict]:
    #     """
    #     Calls JDBC system function getCatalogs to retrieve table metadata.
    #      (https://www.tutorialspoint.com/java-databasemetadata-getcatalogs-method-with-example)
    #
    #     Args:
    #     Returns:
    #
    #     """
    #     header = ['CATALOG']
    #
    #     for row in self._call_jdbc_metadata_system_function('getCatalogs'):
    #         yield self._result_to_dict(row, header)
    #
    # def get_schemas(self) -> Iterable[str]:
    #     """
    #     Calls JDBC system function getSchemas to retrieve table metadata.
    #      (https://www.tutorialspoint.com/java-databasemetadata-getcatalogs-method-with-example)
    #
    #     Args:
    #     Returns:
    #
    #     """
    #
    #     for row in self._call_jdbc_metadata_system_function('getSchemas'):
    #         yield row[0]

    def perform_query(self, query: str, bind_parameters: Optional[dict] = None) -> Iterable[dict]:
        """

        Args:
            query: Query string. Bind parameters are in query string prefixed with :. E.g. select * from t where ID=:id.
            bind_parameters: Dictionary of key value parameters to be bind to query. e.g. {"id":123}

        Returns:

            """
        cursor = self.connection.cursor()

        logging.debug(f'Running query: \n "{query}" \n '
                      f'Parameters: {bind_parameters}')
        try:
            cursor.execute(query, bind_parameters)
        except pypyodbc.DatabaseError as e:
            error, = e.args
            raise ConnectionUserException(f"Query failed with error: {error.message}",
                                          {"query": query, "parameters": bind_parameters}) from error

        try:
            try:
                for res in cursor.fetchall():
                    yield res
            except pypyodbc.ProgrammingError as e:
                # empty result
                if 'Invalid cursor state' in str(e):
                    yield []
                else:
                    raise
            except TypeError:
                yield []

        except Exception as e:
            raise e
