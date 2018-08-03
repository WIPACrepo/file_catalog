from __future__ import absolute_import, division, print_function

import argparse
import logging
import os

from file_catalog.server import Server
from file_catalog.config import Config

def main():
    parser = argparse.ArgumentParser(description='File catalog')
    parser.add_argument('-p', '--port', help='port to listen on')
    parser.add_argument('--db_host', help='MongoDB host')
    parser.add_argument('--debug', action='store_true', default=False, help='Debug flag')
    parser.add_argument('--config', required=True, help='Path to config file')
    args = parser.parse_args()
    kwargs = {k:v for k,v in vars(args).items() if v}

    # create config dict
    config = Config(args.config)

    # Use config file if not defined explicitly
    def add_config(kwargs, key):
        if key not in kwargs:
            kwargs[key] = config['server'][key]

    add_config(kwargs, 'port')
    add_config(kwargs, 'db_host')
    add_config(kwargs, 'debug')

    # add config
    kwargs['config'] = config

    logging.basicConfig(level=('DEBUG' if args.debug else 'INFO'))
    try:
        Server(**kwargs).run()
    except Exception:
        logging.fatal('Server error', exc_info=True)
        raise

if __name__ == '__main__':
    main()
