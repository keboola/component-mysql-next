import os

from datadirtest import TestDataDir

from tests.test_functional import TestDatabaseEnvironment


def run(context: TestDataDir):
    # get value from the context parameters injected via DataDirTester constructor
    sql_client: TestDatabaseEnvironment = context.context_parameters['db_client']
    schema = os.environ.get('MYSQL_DB_DATABASE', 'cdc')
    sql_client.prepare_initial_table('SalesTable', schema)
    print("Running before script")
