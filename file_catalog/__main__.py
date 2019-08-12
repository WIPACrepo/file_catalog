from __future__ import absolute_import, division, print_function

import argparse
import logging
import os

from file_catalog.server import Server
from file_catalog.config import Config
from pprint import pprint

def main():
    parser = argparse.ArgumentParser(description='File catalog')
    parser.add_argument('--show-config-spec', action='store_true',
            help='Print configuration specification, including defaults, and exit')
    args = parser.parse_args()

    if args.show_config_spec:
        pprint(Config.SPEC)
        parser.exit()
    
    config = Config()
    config.update_from_env()

    logging.basicConfig(level=('DEBUG' if config['DEBUG'] else 'INFO'))
    try:
        Server(config, port=config['FC_PORT'], debug=config['DEBUG'],
                db_host=config['MONGODB_HOST'], db_port=config['MONGODB_PORT']).run()
    except Exception:
        logging.fatal('Server error', exc_info=True)
        raise

if __name__ == '__main__':
    main()
