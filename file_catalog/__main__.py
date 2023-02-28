# __main__.py
"""Run the File Catalog."""

# fmt:off

import argparse
import asyncio
import logging
from pprint import pprint
from typing import cast, Optional

import coloredlogs  # type: ignore[import]

from file_catalog.config import Config
from file_catalog.mongo import Mongo
from file_catalog.server import create

logger = logging.getLogger(__name__)


async def main(config: Config) -> None:
    """Create and run the File Catalog service."""
    mongo = Mongo(host       = cast(str,           config.get('MONGODB_HOST',      None)),  # noqa: E221, E241, E251
                  port       = cast(int,           config.get('MONGODB_PORT',      None)),  # noqa: E221, E241, E251
                  authSource = cast(str,           config['MONGODB_AUTH_SOURCE_DB']),       # noqa: E221, E241, E251
                  username   = cast(Optional[str], config.get('MONGODB_AUTH_USER', None)),  # noqa: E221, E241, E251
                  password   = cast(Optional[str], config.get('MONGODB_AUTH_PASS', None)),  # noqa: E221, E241, E251
                  uri        = cast(Optional[str], config.get('MONGODB_URI',       None)))  # noqa: E221, E241, E251

    await mongo.create_indexes()

    create(config = config,                         # noqa: E221, E241, E251
           port   = cast(int,  config['FC_PORT']),  # noqa: E221, E241, E251
           debug  = cast(bool, config['DEBUG']),    # noqa: E221, E241, E251
           mongo  = mongo)                          # noqa: E221, E241, E251

    while True:
        logger.info("Will sleep for 60 seconds")
        await asyncio.sleep(60)


def main_sync() -> None:
    """Do synchronous setup for the File Catalog service."""
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
        asyncio.run(main(config))
    except Exception:
        logging.fatal('Server error', exc_info=True)
        raise


if __name__ == '__main__':
    main_sync()
