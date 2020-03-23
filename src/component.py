"""Component main class for data extraction.

Executes component endpoint executions based on dependent api_mappings.json file in same path. This file should have
a set structure, see example below:

TODO: Response for table mappings
TODO: Integrate SSL, if all else works and there is a need
"""
import logging
import os
import sys

# from datetime import datetime
from kbc.env_handler import KBCEnvHandler
# from kbc.result import KBCTableDef
# from kbc.result import ResultWriter

# from mysql import client, result
# from mysql.client import HubspotClient
# from mysql.result import DealsWriter

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

current_path = os.path.dirname(__file__)
module_path = os.path.dirname(current_path)

# Define mandatory parameter constants, matching Config Schema.
KEY_OBJECTS_ONLY = 'fetchObjectsOnly'
KEY_MYSQL_HOST = 'host'
KEY_MYSQL_PORT = 'port'
KEY_MYSQL_USER = 'username',
KEY_MYSQL_PWD = '#password',
KEY_USE_SSH_TUNNEL = 'sshTunnel'

# Define optional parameters as constants for later use.
KEY_SSH_HOST = 'sshHost'
KEY_SSH_PORT = 'sshPort'
KEY_SSH_PUBLIC_KEY = 'sshPublicKey'

# Keep for debugging
KEY_STDLOG = 'stdlogging'
KEY_DEBUG = 'debug'
MANDATORY_PARS = (KEY_OBJECTS_ONLY, KEY_MYSQL_HOST, KEY_MYSQL_PORT, KEY_MYSQL_USER, KEY_MYSQL_PWD, KEY_USE_SSH_TUNNEL)
MANDATORY_IMAGE_PARS = ()

APP_VERSION = '0.0.1'


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

    def run(self):
        """Execute main component extraction process."""
        params = self.cfg_params
        print(params)


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
