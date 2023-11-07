import os

from datadirtest import TestDataDir

from tests.test_functional import TestDatabaseEnvironment


def get_transactions_queries():
    transactions_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'transactions.sql')
    return str(open(transactions_path, 'r').read()).split(';')


def run(context: TestDataDir):
    # get value from the context parameters injected via DataDirTester constructor
    sql_client: TestDatabaseEnvironment = context.context_parameters['db_client']
    sql_client.connection.connect()

    schema = os.environ.get('MYSQL_DB_DATABASE', 'cdc')
    sql_client.perform_query(f'USE `{schema}`')

    queries = get_transactions_queries()
    for q in queries:
        if q.strip():
            sql_client.perform_query(q)
    sql_client.perform_query('COMMIT')
    sql_client.connection.close()
    print("Running before script")
