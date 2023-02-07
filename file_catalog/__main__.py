# fmt:off

from __future__ import absolute_import, division, print_function

import argparse
import logging
from pprint import pprint
from typing import cast, Optional

import coloredlogs  # type: ignore[import]

from file_catalog.config import Config
from file_catalog.server import Server


def main() -> None:
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
               port           = cast(int,           config['FC_PORT']),                      # noqa: E221, E241, E251
               debug          = cast(bool,          config['DEBUG']),                        # noqa: E221, E241, E251
               db_host        = cast(str,           config.get('MONGODB_HOST', None)),       # noqa: E221, E241, E251
               db_port        = cast(int,           config.get('MONGODB_PORT', None)),       # noqa: E221, E241, E251
               db_auth_source = cast(str,           config['MONGODB_AUTH_SOURCE_DB']),       # noqa: E221, E241, E251
               db_user        = cast(Optional[str], config.get('MONGODB_AUTH_USER', None)),  # noqa: E221, E241, E251
               db_pass        = cast(Optional[str], config.get('MONGODB_AUTH_PASS', None)),  # noqa: E221, E241, E251
               db_uri         = cast(Optional[str], config.get('MONGODB_URI', None))         # noqa: E221, E241, E251
               ).run()
    except Exception:
        logging.fatal('Server error', exc_info=True)
        raise


if __name__ == '__main__':
    main()
