"""Component main class for data extraction.

Executes component endpoint executions based on dependent api_mappings.json file in same path. This file should have
a set structure, see example below:

TODO: Response for table mappings
"""

import logging
import sys

# from datetime import datetime
from kbc.env_handler import KBCEnvHandler
# from kbc.result import KBCTableDef
# from kbc.result import ResultWriter

# from mysql import client, result
# from mysql.client import HubspotClient
# from mysql.result import DealsWriter

KEY_ENDPOINTS = ''
KEY_API_TOKEN = ''

# Keep for debugging
KEY_STDLOG = 'stdlogging'
KEY_DEBUG = 'debug'
MANDATORY_PARS = [KEY_ENDPOINTS, KEY_API_TOKEN]
MANDATORY_IMAGE_PARS = []

APP_VERSION = '0.0.1'


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        KBCEnvHandler.__init__(self, MANDATORY_PARS, log_level=logging.DEBUG if debug else logging.INFO)
        # override debug from config
        if self.cfg_params.get(KEY_DEBUG):
            debug = True
        if debug:
            logging.getLogger().setLevel(logging.DEBUG)
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            self.validate_config()
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as err:
            logging.exception(err)
            exit(1)

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
        comp = Component(debug_arg)
        comp.run()
    except Exception as e:
        logging.exception(e)
        exit(1)
