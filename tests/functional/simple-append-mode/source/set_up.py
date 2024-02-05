import os

from datadirtest import TestDataDir

from tests.test_functional import TestDatabaseEnvironment


def run(context: TestDataDir):
    # get value from the context parameters injected via DataDirTester constructor
    sql_client: TestDatabaseEnvironment = context.context_parameters['db_client']
    schema = os.environ.get('MYSQL_DB_DATABASE', 'cdc')

    sql_client.connection.connect()
    sql_client.perform_query("RESET MASTER")
    sql_client.prepare_initial_table('SalesTable', schema)
    sql_client.perform_query("UPDATE sales SET category = 'newcategory' WHERE sku = 'ZD111318'")
    sql_client.perform_query("UPDATE sales SET category = 'newcategory2' WHERE sku = 'ZD111318'")
    sql_client.perform_query("COMMIT")
    sql_client.connection.close()

    print("Running before script")
