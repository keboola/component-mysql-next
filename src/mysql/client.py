"""
Define MySQL connection, session parameters and backoff configuration.

TODO: Integrate SSL here, if SSL is needed.
"""
import backoff
import logging
import pymysql

LOGGER = logging.getLogger(__name__)

MAX_CONNECT_RETRIES = 7
BACKOFF_FACTOR = 2
CONNECTION_TIMEOUT_SECONDS = 30
READ_TIMEOUT_SECONDS = 3000


@backoff.on_exception(backoff.expo, pymysql.err.OperationalError, max_tries=MAX_CONNECT_RETRIES, factor=BACKOFF_FACTOR)
def connect_with_backoff(connection):
    connection.connect()
    with connection.cursor() as cursor:
        set_session_parameters(cursor, net_read_timeout=READ_TIMEOUT_SECONDS)

    return connection


def set_session_parameters(cursor: pymysql.connections.Connection.cursor, wait_timeout: int = 300,
                           net_read_timeout: int = 60, innodb_lock_wait_timeout: int = 300, time_zone: str = '+0:00'):
    """Set MySQL session parameters to handle for data extraction appropriately.

    Args:
        cursor: MySQLDB connection cursor object for the active connection.
        wait_timeout: (Optional) Seconds server waits for activity on inactive session before closing, default 300.
        net_read_timeout: (Optional) Seconds to wait for more data from connection before aborting the read, default 60.
        innodb_lock_wait_timeout: (Optional) Seconds a transaction waits for a row lock before giving up, default 300.
        time_zone: (Optional) String representing the session time zone, default is UTC: '+0:00').
    Returns:
        None.
    """
    logged_warnings = []
    try:
        cursor.execute('SET @@session.wait_timeout={}'.format(wait_timeout))
    except pymysql.err.InternalError as internal_err:
        logged_warnings.append('Could not set session.wait_timeout. Error: ({}) {}'.format(*internal_err.args))
    try:
        cursor.execute("SET @@session.net_read_timeout={}".format(net_read_timeout))
    except pymysql.err.InternalError as internal_err:
        logged_warnings.append('Could not set session.net_read_timeout. Error: ({}) {}'.format(*internal_err.args))
    try:
        cursor.execute('SET @@session.time_zone="{}"'.format(time_zone))
    except pymysql.err.InternalError as internal_err:
        logged_warnings.append('Could not set session.time_zone. Error: ({}) {}'.format(*internal_err.args))

    try:
        cursor.execute('SET @@session.innodb_lock_wait_timeout={}'.format(innodb_lock_wait_timeout))
    except pymysql.err.InternalError as e:
        logged_warnings.append(
            'Could not set session.innodb_lock_wait_timeout. Error: ({}) {}'.format(*e.args)
        )

    if logged_warnings:
        LOGGER.info('Setting session parameters failed for at least one process, which may impact execution speed.')
        for warn_message in logged_warnings:
            LOGGER.warning(warn_message)


class MySQLConnection(pymysql.connections.Connection):
    def __init__(self, config):
        args = {
            "user": config["user"],
            "password": config["password"],
            "host": config["host"],
            "port": int(config["port"]),
            "cursorclass": config.get("cursorclass") or pymysql.cursors.SSCursor,
            "connect_timeout": CONNECTION_TIMEOUT_SECONDS,
            "read_timeout": READ_TIMEOUT_SECONDS,
            "charset": "utf8",
        }

        if config.get("database"):
            args["database"] = config["database"]

        super().__init__(defer_connect=True, **args)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        del exc_info
        self.close()


def make_connection_wrapper(config):
    class ConnectionWrapper(MySQLConnection):
        def __init__(self, *args, **kwargs):
            config["cursorclass"] = kwargs.get('cursorclass')
            super().__init__(config)

            connect_with_backoff(self)

    return ConnectionWrapper
