"""Component main class for data extraction.

Executes component endpoint executions based on dependent api_mappings.json file in same path. This file should have
a set structure, see example below:

TODO: Response for table mappings
TODO: Integrate SSL, if all else works and there is a need
"""
import json
import logging
import os
import sys

# from datetime import datetime
from kbc.env_handler import KBCEnvHandler
# from kbc.result import KBCTableDef
# from kbc.result import ResultWriter

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

try:
    from mysql.client import connect_with_backoff, MySQLConnection
except ImportError as import_err:
    LOGGER.warning('Running component locally, some features may not reflect their behavior in Keboola as we are not'
                   'running inside of a Docker container.')
    from src.mysql.client import connect_with_backoff, MySQLConnection

current_path = os.path.dirname(__file__)
module_path = os.path.dirname(current_path)

# Define mandatory parameter constants, matching Config Schema.
KEY_OBJECTS_ONLY = 'fetchObjectsOnly'
KEY_TABLE_MAPPINGS_JSON = 'tableMappingsJson'
KEY_MYSQL_HOST = 'host'
KEY_MYSQL_PORT = 'port'
KEY_MYSQL_USER = 'username'
KEY_MYSQL_PWD = '#password'
KEY_USE_SSH_TUNNEL = 'sshTunnel'

# Define optional parameters as constants for later use.
KEY_SSH_HOST = 'sshHost'
KEY_SSH_PORT = 'sshPort'
KEY_SSH_PUBLIC_KEY = 'sshPublicKey'

MAPPINGS_FILE = 'table_mappings.json'

# Keep for debugging
KEY_STDLOG = 'stdlogging'
KEY_DEBUG = 'debug'
MANDATORY_PARS = (KEY_OBJECTS_ONLY, KEY_MYSQL_HOST, KEY_MYSQL_PORT, KEY_MYSQL_USER, KEY_MYSQL_PWD, KEY_USE_SSH_TUNNEL)
MANDATORY_IMAGE_PARS = ()

APP_VERSION = '0.0.2'

# pymysql.converters.conversions[pendulum.Pendulum] = pymysql.converters.escape_datetime

# Bin database sub-types by type.
STRING_TYPES = {'char', 'enum', 'longtext', 'mediumtext', 'text', 'varchar'}
FLOAT_TYPES = {'double', 'float'}
DATETIME_TYPES = {'date', 'datetime', 'time', 'timestamp'}
BYTES_FOR_INTEGER_TYPE = {
    'tinyint': 1,
    'smallint': 2,
    'mediumint': 3,
    'int': 4,
    'bigint': 8
}


class Component(KBCEnvHandler):
    """Keboola extractor component."""
    def __init__(self, debug: bool = False, data_path: str = None):
        KBCEnvHandler.__init__(self, MANDATORY_PARS, data_path=data_path,
                               log_level=logging.DEBUG if debug else logging.INFO)
        # override debug from config
        if self.cfg_params.get(KEY_DEBUG):
            debug = True
        if debug:
            logging.getLogger().setLevel(logging.DEBUG)
        LOGGER.info('Running version %s', APP_VERSION)
        LOGGER.info('Loading configuration...')

        try:
            self.validate_config()
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as err:
            LOGGER.exception(err)
            exit(1)

        # TODO: Update to more clear environment variable; used must set local time to UTC.
        os.environ['TZ'] = 'UTC'

    def _check_file_inputs(self) -> str:
        """Return path name of file inputs if any."""
        file_input = os.path.join(self.data_path, 'in', 'files')
        has_file_inputs = any(os.path.isfile(os.path.join(file_input, file)) for file in os.listdir(file_input))

        if has_file_inputs:
            return file_input

    def run(self):
        """Execute main component extraction process."""
        params = self.cfg_params
        file_input_path = self._check_file_inputs()
        table_mappings = {}

        config_params = {
            "host": params[KEY_MYSQL_HOST],
            "port": params[KEY_MYSQL_PORT],
            "user": params[KEY_MYSQL_USER],
            "password": params[KEY_MYSQL_PWD]
        }
        print(config_params)

        mysql_client = MySQLConnection(config_params)
        # TODO: Consider logging server details here.

        if file_input_path:
            manual_table_mappings_file = os.path.join(file_input_path, MAPPINGS_FILE)
            logging.info('Fetching table mappings from file input mapping configuration: {}.'.format(
                manual_table_mappings_file))
            with open(manual_table_mappings_file, 'r') as mappings_file:
                table_mappings = json.load(mappings_file)
        elif KEY_TABLE_MAPPINGS_JSON:
            table_mappings = KEY_TABLE_MAPPINGS_JSON
        print(params[KEY_OBJECTS_ONLY])
        print(table_mappings)

        if params[KEY_OBJECTS_ONLY]:
            # Run only schema discovery process.
            LOGGER.info('Fetching only object and field names, not running full extraction.')
            # do_discover(mysql_client, params)
        elif table_mappings:
            # Run extractor data sync.
            prior_state = self.get_state_file() or {}
            if prior_state:
                LOGGER.info('Using prior state file to execute sync.')
            else:
                LOGGER.info('No prior state found, will need to execute full sync.')
            # do_sync(mysql_client, params, table_mappings, prior_state)
        else:
            LOGGER.info('You have either specified incorrect input parameters, or have not chosen to either specify a'
                        'table mappings file manually or via the File Input Mappings configuration.')


if __name__ == "__main__":
    if len(sys.argv) > 1:
        debug_arg = sys.argv[1]
    else:
        debug_arg = False
    try:
        # Note: If debugging, run docker-compose instead. Only use below two lines for early testing.
        debug_data_path = os.path.join(module_path, 'data')
        comp = Component(debug_arg, data_path=debug_data_path)
        comp.run()

        # TODO: Add standard call back in below once ready to move to Docker.
        # comp = Component(debug_arg)
        # comp.run()
    except Exception as generic_err:
        LOGGER.exception(generic_err)
        exit(1)
