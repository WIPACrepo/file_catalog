# fmt:off

from __future__ import absolute_import, division, print_function

import argparse
import logging
from pprint import pprint

import coloredlogs  # type: ignore[import]

from file_catalog.config import Config
from file_catalog.server import Server


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

    coloredlogs.install(level=('DEBUG' if config['DEBUG'] else 'INFO'))

    try:
        Server(config,
               port=config['FC_PORT'],
               debug=config['DEBUG'],
               db_host=config.get('MONGODB_HOST', None),
               db_port=config.get('MONGODB_PORT', None),
               db_auth_source=config['MONGODB_AUTH_SOURCE_DB'],
               db_user=config.get('MONGODB_AUTH_USER', None),
               db_pass=config.get('MONGODB_AUTH_PASS', None),
               db_uri=config.get('MONGODB_URI', None)).run()
    except Exception:
        logging.fatal('Server error', exc_info=True)
        raise


if __name__ == '__main__':
    main()
