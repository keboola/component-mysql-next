"""
Define MySQL connection, session parameters and backoff configuration.

TODO: Confirm correct behavior and security of SSL setup.
"""
import logging
import ssl

import backoff
import pymysql
from pymysql.constants import CLIENT
from core.exceptions import AppError

from typing import Union

MAX_CONNECT_RETRIES = 5
BACKOFF_FACTOR = 2
CONNECTION_TIMEOUT_SECONDS = 30
READ_TIMEOUT_SECONDS = 3000


@backoff.on_exception(backoff.expo, pymysql.err.OperationalError, max_tries=MAX_CONNECT_RETRIES, factor=BACKOFF_FACTOR)
def connect_with_backoff(connection):
    logging.debug('Connecting to MySQL server.')
    connection.connect()
    with connection.cursor() as cursor:
        set_session_parameters(cursor, net_read_timeout=READ_TIMEOUT_SECONDS,
                               max_execution_time=connection.connection_parameters["max_execution_time"])
    return connection


def get_db_version(cursor: pymysql.connections.Connection.cursor):
    cursor.execute('SELECT VERSION()')
    version = cursor.fetchone()[0]
    return version


def get_execution_time_parameter(cursor: pymysql.connections.Connection.cursor):
    version = get_db_version(cursor)
    exec_time_variable = 'max_statement_time' if 'MariaDB' in version else 'max_execution_time'
    return exec_time_variable


def set_session_parameters(cursor: pymysql.connections.Connection.cursor, max_execution_time: Union[int, bool],
                           wait_timeout: int = 300,
                           net_read_timeout: int = 60, innodb_lock_wait_timeout: int = 300, time_zone: str = '+0:00'
                           ):
    """Set MySQL session parameters to handle for data extraction appropriately.

    Args:
        cursor: MySQLDB connection cursor object for the active connection.
        wait_timeout: (Optional) Seconds server waits for activity on inactive session before closing, default 300.
        net_read_timeout: (Optional) Seconds to wait for more data from connection before aborting the read, default 60.
        innodb_lock_wait_timeout: (Optional) Seconds a transaction waits for a row lock before giving up, default 300.
        time_zone: (Optional) String representing the session time zone, default is UTC: '+0:00').
        max_execution_time: This is here as a workaround for server-caused timeouts
    Returns:
        None.
    """
    logged_warnings = []
    try:
        cursor.execute('SET @@session.wait_timeout={}'.format(wait_timeout))
        logging.info(f"Setting session parameter wait_timeout to {wait_timeout}")
    except pymysql.err.InternalError as internal_err:
        logged_warnings.append('Could not set session.wait_timeout. Error: ({}) {}'.format(*internal_err.args))
    try:
        cursor.execute("SET @@session.net_read_timeout={}".format(net_read_timeout))
        logging.info(f"Setting session parameter net_read_timeout to {net_read_timeout}")
    except pymysql.err.InternalError as internal_err:
        logged_warnings.append('Could not set session.net_read_timeout. Error: ({}) {}'.format(*internal_err.args))
    try:
        cursor.execute('SET @@session.time_zone="{}"'.format(time_zone))
        logging.info(f"Setting session parameter time_zone to {time_zone}")
    except pymysql.err.InternalError as internal_err:
        logged_warnings.append('Could not set session.time_zone. Error: ({}) {}'.format(*internal_err.args))
    try:
        cursor.execute('SET @@session.innodb_lock_wait_timeout={}'.format(innodb_lock_wait_timeout))
        logging.info(f"Setting session parameter innodb_lock_wait_timeout to {innodb_lock_wait_timeout}")
    except pymysql.err.InternalError as e:
        logged_warnings.append('Could not set session.innodb_lock_wait_timeout. Error: ({}) {}'.format(*e.args))

    if max_execution_time:
        exec_time_variable = get_execution_time_parameter(cursor)
        try:
            cursor.execute(f'SET @@session.{exec_time_variable}={max_execution_time}')
            logging.info(f"Setting session parameter max_execution_time to {max_execution_time}")
        except pymysql.err.InternalError as e:
            logged_warnings.append('Could not set session.max_execution_time. Error: ({}) {}'.format(*e.args))

    if logged_warnings:
        logging.info('Setting session parameters failed for at least one process, which may impact execution speed.')
        for warn_message in logged_warnings:
            logging.warning(warn_message)


class MySQLConnection(pymysql.connections.Connection):
    def __init__(self, config):
        args = {
            "user": config.get('user') or config.get('username'),
            "password": config.get('password') or config.get('#password'),
            "host": config['host'],
            "port": int(config['port']),
            "cursorclass": config.get('cursorclass') or pymysql.cursors.SSCursor,
            "connect_timeout": CONNECTION_TIMEOUT_SECONDS,
            "read_timeout": READ_TIMEOUT_SECONDS,
            "charset": 'utf8',
        }

        if config.get("database"):
            args["database"] = config["database"]

        ssl_arg = None
        use_ssl = config.get('ssl') == 'true'

        # Attempt self-signed SSL, if config vars are present
        use_self_signed_ssl = config.get("ssl_ca")
        self.connection_parameters = config
        super().__init__(defer_connect=True, ssl=ssl_arg, **args)

        # Configure SSL w/o custom CA -- Manually create context, override default behavior of CERT_NONE w/o CA supplied
        if use_ssl and not use_self_signed_ssl:
            logging.info("Attempting SSL connection")
            # For compatibility with previous version, verify mode is off by default
            verify_mode = config.get("verify_mode", "false") == 'true'
            if not verify_mode:
                logging.warning('Not verifying server certificate. The connection is encrypted, but the server '
                                'hasn''t been verified. Please provide a root CA certificate to enable verification.')
            self.ssl = True
            self.ctx = ssl.create_default_context()
            check_hostname = config.get("check_hostname", "false") == 'true'
            self.ctx.check_hostname = check_hostname
            self.ctx.verify_mode = ssl.CERT_REQUIRED if verify_mode else ssl.CERT_NONE
            self.client_flag |= CLIENT.SSL

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        del exc_info
        self.close()


def get_schemas(mysql_conn) -> list[str]:
    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name not in ('information_schema', 'sys', 'performance_schema');
            """)

            schemas = []
            for res in cur.fetchall():
                schemas.append(res[0])
            return schemas


def _get_replicas(cursor, sql_statement):
    try:
        cursor.execute(sql_statement)
        return cursor.fetchall()
    except Exception:
        pass


def get_replicas(mysql_conn) -> list[str]:
    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            # TODO: check returned value
            replicas = (
                _get_replicas(cur, 'SHOW REPLICAS;')
                or _get_replicas(cur, 'SHOW SLAVE HOSTS;'))

            if replicas is None:
                raise AppError("Couldn't fetch replicas.")

            replicas = [replica[0] for replica in replicas]
            return replicas


def get_tables(mysql_conn, schemas: list[str] = None) -> list[dict]:
    if schemas:
        filter_dbs_clause = ",".join(["'{}'".format(db) for db in schemas])
        table_schema_clause = "WHERE t.table_schema IN ({})".format(filter_dbs_clause)
    else:
        table_schema_clause = """
        WHERE t.table_schema NOT IN (
        'information_schema',
        'performance_schema',
        'mysql',
        'sys'
        )"""

    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            cur.execute(f"""
            SELECT t.table_schema,
                   t.table_name,
                   t.table_type,
                   t.table_rows
            FROM information_schema.tables t
            JOIN information_schema.TABLE_CONSTRAINTS k
                ON k.TABLE_NAME=t.TABLE_NAME and k.TABLE_SCHEMA=t.TABLE_SCHEMA
                {table_schema_clause}
            AND table_type != 'VIEW'
            AND table_type != 'VIEW'
            AND k.constraint_type='PRIMARY KEY';
            """)

            tables = []
            for res in cur.fetchall():
                tables.append({"schema": res[0], "name": res[1]})
            return tables


def make_connection_wrapper(config):
    class ConnectionWrapper(MySQLConnection):
        def __init__(self, *args, **kwargs):
            config["cursorclass"] = kwargs.get('cursorclass')
            super().__init__(config)
            connect_with_backoff(self)

    return ConnectionWrapper
