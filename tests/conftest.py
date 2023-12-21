# conftest.py
# See: https://docs.pytest.org/en/7.1.x/reference/fixtures.html#conftest-py-sharing-fixtures-across-multiple-files
"""
The conftest.py file serves as a means of providing fixtures for an
entire directory. Fixtures defined in a conftest.py can be used by any
test in that package without needing to import them (pytest will
automatically discover them).
"""

import logging
import os
import socket
from typing import Any, AsyncGenerator, cast, Dict

from pymongo import MongoClient  # type: ignore[import]
from pymongo.errors import ServerSelectionTimeoutError  # type: ignore[import]
import pytest
from pytest import MonkeyPatch
import pytest_asyncio
from rest_tools.client import RestClient

from file_catalog.config import Config
from file_catalog.mongo import Mongo
from file_catalog.server import create

logger = logging.getLogger(__name__)


@pytest_asyncio.fixture
async def mongo() -> AsyncGenerator[Mongo, None]:
    """Provide an instance of the File Catalog's internal MongoDB client."""
    # setup_function
    mongo_host = os.environ["TEST_DATABASE_HOST"]
    mongo_port = int(os.environ["TEST_DATABASE_PORT"])

    # did the user override the default database 'file_catalog'?
    database_name = "file_catalog"
    if "TEST_DATABASE_NAME" in os.environ:
        database_name = os.environ["TEST_DATABASE_NAME"]

    # connect a client to the test MongoDB
    mongo_url = f"mongodb://{mongo_host}"
    client: MongoClient[Any] = MongoClient(mongo_url, port=mongo_port, connect=True, serverSelectionTimeoutMS=100)
    db = client[database_name]

    # dump any existing collections to refresh the database
    try:
        for collection in db.list_collection_names():
            if 'system' not in collection:
                db.drop_collection(collection)
    except ServerSelectionTimeoutError:
        raise Exception("Unable to connect to MongoDB; do you have a MongoDB at TEST_DATABASE_{HOST,PORT}?")

    # create an instance of the File Catalog's internal MongoDB client
    fc_mongo = Mongo(host=mongo_host,
                     port=mongo_port,
                     authSource="admin")
    await fc_mongo.create_indexes()

    # provide the client as the fixture
    yield fc_mongo

    # -------------------------------------------------------------------------
    # NOTE: *Your Unit Test Function Runs Here*
    # -------------------------------------------------------------------------

    # teardown_function
    fc_mongo.close_me.close()


@pytest.fixture
def port() -> int:
    """Get an ephemeral port number."""
    # https://unix.stackexchange.com/a/132524
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    addr = s.getsockname()
    ephemeral_port = addr[1]
    s.close()
    return cast(int, ephemeral_port)


@pytest_asyncio.fixture
async def rest(monkeypatch: MonkeyPatch, mongo: Mongo, port: int) -> AsyncGenerator[RestClient, None]:
    """Start a File Catalog instance and get a RestClient configured to talk to it."""
    # setup_function
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.setenv("WIPACTEL_EXPORT_STDOUT", "FALSE")

    config: Config = Config()
    config.update({
        "AUTH_AUDIENCE": "file-catalog-testing",
        "AUTH_OPENID_URL": "https://keycloak.icecube.wisc.edu/auth/realms/IceCube",
        "FC_HOST": "localhost",
        "FC_PORT": port,
        "FC_PUBLIC_URL": f"http://localhost:{port}",
        "FC_QUERY_FILE_LIST_LIMIT": 10000,
    })

    rest_server = create(config=config,
                         port=port,
                         debug=True,
                         mongo=mongo)

    client = RestClient(f"http://localhost:{port}", timeout=5, retries=0)

    # provide the client as the fixture
    yield client

    # -------------------------------------------------------------------------
    # NOTE: *Your Unit Test Function Runs Here*
    # -------------------------------------------------------------------------

    # teardown_function
    client.close()
    await rest_server.stop()  # type: ignore[no-untyped-call]
