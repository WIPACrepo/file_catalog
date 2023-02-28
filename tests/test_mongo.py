# test_mongo.py
"""Test the File Catalog's internal MongoDB client."""

import logging
import os
import pytest
import re
from typing import Any, List, Tuple
from uuid import uuid4

from file_catalog.mongo import AllKeys, Mongo
from motor import MotorCollection  # type: ignore[import]
from pymongo.errors import DuplicateKeyError

logger = logging.getLogger(__name__)


def test_00_always_succeed() -> None:
    """Succeed with flying colors."""
    assert True


def test_01_constructor_mongo_uri() -> None:
    """Test that Mongo can be constructed with a URI."""
    mongo_host = os.environ["TEST_DATABASE_HOST"]
    mongo_port = int(os.environ["TEST_DATABASE_PORT"])
    mongo = Mongo(uri=f"mongodb://{mongo_host}:{mongo_port}", authSource="admin")
    assert mongo


def test_02_pytest_mongo_fixture(mongo: Mongo) -> None:
    """Test that Mongo will be provided by a pytest fixture."""
    assert mongo


@pytest.mark.asyncio
async def test_03_create_indexes(mongo: Mongo) -> None:
    """Test that Mongo will create indexes."""
    async def assert_index(col: MotorCollection,
                           key: List[Tuple[Any, Any]]) -> None:
        """Assert that the provided index key exists in the provided collection."""
        found_it = False
        ii = await col.index_information()
        for v in ii.values():
            if v["key"] == key:
                found_it = True
                break
        assert found_it

    db = mongo.client
    await assert_index(db.files, [('uuid', 1)])
    await assert_index(db.files, [('logical_name', 'hashed')])
    await assert_index(db.files, [('locations', 1)])
    await assert_index(db.files, [('locations.path', -1), ('locations.site', -1)])
    await assert_index(db.files, [('create_date', 1)])
    await assert_index(db.files, [('content_status', 1)])
    await assert_index(db.files, [('processing_level', 1), ('offline_processing_metadata.season', 1), ('locations.archive', 1)])
    await assert_index(db.files, [('data_type', 1)])
    await assert_index(db.files, [('run.run_number', 1)])
    await assert_index(db.files, [('run.start_datetime', 1)])
    await assert_index(db.files, [('run.end_datetime', 1)])
    await assert_index(db.files, [('offline_processing_metadata.first_event', 1)])
    await assert_index(db.files, [('offline_processing_metadata.last_event', 1)])
    await assert_index(db.files, [('offline_processing_metadata.season', 1)])
    await assert_index(db.files, [('iceprod.dataset', 1)])
    await assert_index(db.collections, [('uuid', 1)])
    await assert_index(db.collections, [('collection_name', 1)])
    await assert_index(db.collections, [('owner', 1)])
    await assert_index(db.snapshots, [('uuid', 1)])
    await assert_index(db.snapshots, [('collection_id', 1)])
    await assert_index(db.snapshots, [('owner', 1)])


def test_04__get_projection() -> None:
    """Ensure that _get_projection provides projection dictionaries."""
    assert Mongo._get_projection() == {"_id": False}
    assert Mongo._get_projection(default={"city": True, "state": False, "zip": True}) == {"_id": False, "city": True, "state": False, "zip": True}
    assert Mongo._get_projection(AllKeys()) == {"_id": False}
    assert Mongo._get_projection(["uuid", "name", "ssn"]) == {"_id": False, "uuid": True, "name": True, "ssn": True}
    with pytest.raises(TypeError):
        Mongo._get_projection("uuid")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_05__limit_result_list(mongo: Mongo) -> None:
    """Use start and limit to trim query results."""
    # create some records so we have something to start and limit
    for file_size in range(100):
        uuid = str(uuid4())
        await mongo.create_file({"uuid": uuid, "file_size": file_size, "locations": [{"site": "WIPAC", "path": f"{uuid}.zip"}], "data_type": "RAW"})

    cursor = mongo.client.files.find({"data_type": "RAW"}, {"_id": False}, max_time_ms=10)
    res = await Mongo._limit_result_list(cursor, limit=10)
    assert len(res) == 10
    for doc in res:
        assert doc["file_size"] >= 0
        assert doc["file_size"] < 10

    cursor = mongo.client.files.find({"data_type": "RAW"}, {"_id": False}, max_time_ms=10)
    res = await Mongo._limit_result_list(cursor, start=90)
    assert len(res) == 10
    for doc in res:
        assert doc["file_size"] >= 90

    cursor = mongo.client.files.find({"data_type": "RAW"}, {"_id": False}, max_time_ms=10)
    res = await Mongo._limit_result_list(cursor, start=10, limit=10)
    assert len(res) == 10
    for doc in res:
        assert doc["file_size"] >= 10
        assert doc["file_size"] < 20


@pytest.mark.asyncio
async def test_06_find_files(mongo: Mongo) -> None:
    """Use find_files to obtain documents from the files collection."""
    # create some records so we have something to find
    for file_size in range(100):
        uuid = str(uuid4())
        await mongo.create_file({"uuid": uuid, "file_size": file_size, "locations": [{"site": "WIPAC", "path": f"{uuid}.zip"}], "data_type": "RAW"})

    res = await mongo.find_files({"data_type": "RAW"}, ["file_size"], limit=10, max_time_ms=10)
    assert len(res) == 10
    for doc in res:
        assert doc["file_size"] >= 0
        assert doc["file_size"] < 10

    res = await mongo.find_files({"data_type": "RAW"}, ["file_size"], start=90, max_time_ms=10)
    assert len(res) == 10
    for doc in res:
        assert doc["file_size"] >= 90

    res = await mongo.find_files({"data_type": "RAW"}, ["file_size"], start=10, limit=10, max_time_ms=10)
    assert len(res) == 10
    for doc in res:
        assert doc["file_size"] >= 10
        assert doc["file_size"] < 20


@pytest.mark.asyncio
async def test_07_count_files(mongo: Mongo) -> None:
    """Use find_files to obtain documents from the files collection."""
    # create some records so we have something to find
    for file_size in range(100):
        uuid = str(uuid4())
        await mongo.create_file({"uuid": uuid, "file_size": file_size, "locations": [{"site": "WIPAC", "path": f"{uuid}.zip"}], "data_type": "RAW"})

    assert await mongo.count_files({"data_type": "RAW"}, max_time_ms=10) == 100
    assert await mongo.count_files() == 100


@pytest.mark.asyncio
async def test_08_create_file(mongo: Mongo) -> None:
    """Use create_file to create documents in the files collection."""
    uuid1 = str(uuid4())

    await mongo.create_file({"uuid": f"{uuid1}"})
    res = await mongo.find_files({"uuid": f"{uuid1}"}, ["uuid"], max_time_ms=10)
    assert len(res) == 1
    assert res[0]["uuid"] == f"{uuid1}"

    with pytest.raises(DuplicateKeyError):
        await mongo.create_file({"uuid": f"{uuid1}"})


@pytest.mark.asyncio
async def test_09_get_file(mongo: Mongo) -> None:
    """Use get_file to find a document in the files collection."""
    uuid1 = str(uuid4())
    uuid2 = str(uuid4())

    await mongo.create_file({"uuid": f"{uuid1}", "file_size": 0, "locations": [{"site": "WIPAC", "path": f"{uuid1}.zip"}]})
    res = await mongo.get_file({"uuid": f"{uuid1}"})
    assert res
    assert "uuid" in res
    assert res["uuid"] == f"{uuid1}"

    res = await mongo.get_file({"uuid": f"{uuid2}"})
    assert not res


@pytest.mark.asyncio
async def test_10__find_file_and_update(mongo: Mongo) -> None:
    """Use _find_file_and_update to update a document in the files collection."""
    uuid1 = str(uuid4())
    uuid2 = str(uuid4())
    uuid3 = str(uuid4())

    await mongo.create_file({"uuid": f"{uuid1}", "file_size": 0, "locations": [{"site": "WIPAC", "path": f"{uuid1}.zip"}]})
    res = await mongo._find_file_and_update(f"{uuid1}", {"$set": {"file_size": 1}})
    assert "uuid" in res
    assert res["uuid"] == f"{uuid1}"
    assert "file_size" in res
    assert res["file_size"] == 1

    await mongo.create_file({"uuid": f"{uuid2}", "file_size": 0, "locations": [{"site": "WIPAC", "path": f"{uuid2}.zip"}]})
    with pytest.raises(ValueError, match=re.escape("update only works with $ operators")):
        await mongo._find_file_and_update(f"{uuid2}", {"file_size": 1})

    with pytest.raises(FileNotFoundError, match=re.escape(f"Record ({uuid3}) was not found, so it was not updated")):
        await mongo._find_file_and_update(f"{uuid3}", {"$set": {"file_size": 1}})


@pytest.mark.asyncio
async def test_11_update_file(mongo: Mongo) -> None:
    """Use update_file to update a document in the files collection."""
    uuid1 = str(uuid4())
    uuid2 = str(uuid4())

    await mongo.create_file({"uuid": f"{uuid1}", "file_size": 0, "locations": [{"site": "WIPAC", "path": f"{uuid1}.zip"}]})
    res = await mongo.update_file(f"{uuid1}", {"file_size": 1})
    assert "uuid" in res
    assert res["uuid"] == f"{uuid1}"
    assert "file_size" in res
    assert res["file_size"] == 1
    assert "locations" in res
    assert len(res["locations"]) == 1

    with pytest.raises(FileNotFoundError, match=re.escape(f"Record ({uuid2}) was not found, so it was not updated")):
        await mongo.update_file(f"{uuid2}", {"file_size": 1})


@pytest.mark.asyncio
async def test_12_replace_file(mongo: Mongo) -> None:
    """Use replace_file to update a document in the files collection."""
    uuid1 = str(uuid4())
    uuid2 = str(uuid4())

    await mongo.create_file({"uuid": f"{uuid1}", "file_size": 0, "locations": [{"site": "WIPAC", "path": f"{uuid1}.zip"}]})
    await mongo.replace_file({"uuid": f"{uuid1}", "file_size": 1, "locations": [{"site": "WIPAC", "path": f"{uuid1}.zip"}]})
    res = await mongo.get_file({"uuid": f"{uuid1}"})
    assert res
    assert "uuid" in res
    assert res["uuid"] == f"{uuid1}"
    assert "file_size" in res
    assert res["file_size"] == 1
    assert "locations" in res
    assert len(res["locations"]) == 1

    await mongo.create_file({"uuid": "d6912180-caa9-4700-a405-d891d74d6065", "file_size": 0})
    with pytest.raises(KeyError, match="uuid"):
        await mongo.replace_file({"file_size": 1})

    with pytest.raises(Exception, match=re.escape(f"updated 0 files with id {uuid2}")):
        await mongo.replace_file({"uuid": f"{uuid2}", "file_size": 4, "locations": [{"site": "WIPAC", "path": f"{uuid2}.zip"}]})


@pytest.mark.asyncio
async def test_13_delete_file(mongo: Mongo) -> None:
    """Use delete_file to delete a document from the files collection."""
    uuid1 = str(uuid4())
    uuid2 = str(uuid4())
    uuid3 = str(uuid4())
    uuid5 = str(uuid4())
    uuid6 = str(uuid4())

    await mongo.create_file({"uuid": f"{uuid1}", "file_size": 0, "locations": [{"site": "WIPAC", "path": f"{uuid1}.zip"}]})
    await mongo.delete_file({"uuid": f"{uuid1}"})
    res = await mongo.get_file({"uuid": f"{uuid1}"})
    assert not res

    await mongo.create_file({"uuid": f"{uuid2}", "file_size": 2, "locations": [{"site": "WIPAC", "path": f"{uuid2}.zip"}]})
    await mongo.create_file({"uuid": f"{uuid3}", "file_size": 2, "locations": [{"site": "WIPAC", "path": f"{uuid3}.zip"}]})
    await mongo.create_file({"uuid": f"{uuid5}", "file_size": 2, "locations": [{"site": "WIPAC", "path": f"{uuid5}.zip"}]})
    with pytest.raises(Exception, match=re.escape("filters {'file_size': 2} matches 3 documents; preventing ambiguous delete of files document")):
        await mongo.delete_file({"file_size": 2})

    with pytest.raises(Exception, match=re.escape(f"deleted 0 files with filters {{'uuid': '{uuid6}'}}")):
        await mongo.delete_file({"uuid": f"{uuid6}"})


@pytest.mark.asyncio
async def test_14_find_collections(mongo: Mongo) -> None:
    """Use find_collections to find documents in the collections collection."""
    await mongo.create_collection({"uuid": "7e99b2c3-815e-4158-9c41-bed32412b47b", "owner": "Alice"})
    await mongo.create_collection({"uuid": "4feb1d5b-0518-464b-b6b5-b7f5a6d44ddd", "owner": "Alice"})
    await mongo.create_collection({"uuid": "9b6d2577-b107-4065-aa73-b125db342287", "owner": "Alice"})
    res = await mongo.find_collections()
    assert len(res) == 3


@pytest.mark.asyncio
async def test_15_create_collection(mongo: Mongo) -> None:
    """Use create_collection to create documents in the collections collection."""
    await mongo.create_collection({"uuid": "1506ca1a-638d-4161-a261-8988fd674832", "owner": "Alice"})
    await mongo.create_collection({"uuid": "9414dccd-fcf8-47ba-8473-bdf755a7dbd3", "owner": "Alice"})
    res = await mongo.find_collections()
    assert len(res) == 2

    await mongo.create_collection({"uuid": "f637f88d-2020-469e-8133-8e2e3561837b", "owner": "Alice"})
    with pytest.raises(DuplicateKeyError):
        await mongo.create_collection({"uuid": "f637f88d-2020-469e-8133-8e2e3561837b", "owner": "Bob"})

    with pytest.raises(KeyError, match="uuid"):
        await mongo.create_collection({"owner": "Alice"})


@pytest.mark.asyncio
async def test_16_get_collection(mongo: Mongo) -> None:
    """Use get_collection to find a documents in the collections collection."""
    await mongo.create_collection({"uuid": "ba92c24c-bbdc-44e0-adfb-6ae256da29ad", "owner": "Alice"})
    res = await mongo.get_collection({"uuid": "ba92c24c-bbdc-44e0-adfb-6ae256da29ad"})
    assert "uuid" in res
    assert res["uuid"] == "ba92c24c-bbdc-44e0-adfb-6ae256da29ad"


@pytest.mark.asyncio
async def test_17_find_snapshots(mongo: Mongo) -> None:
    """Use find_snapshots to find documents in the snapshots collection."""
    await mongo.create_snapshot({"uuid": "7e99b2c3-815e-4158-9c41-bed32412b47b", "owner": "Alice"})
    await mongo.create_snapshot({"uuid": "4feb1d5b-0518-464b-b6b5-b7f5a6d44ddd", "owner": "Alice"})
    await mongo.create_snapshot({"uuid": "9b6d2577-b107-4065-aa73-b125db342287", "owner": "Alice"})
    res = await mongo.find_snapshots({"owner": "Alice"})
    assert len(res) == 3


@pytest.mark.asyncio
async def test_18_create_snapshot(mongo: Mongo) -> None:
    """Use create_snapshot to create documents in the snapshots collection."""
    await mongo.create_snapshot({"uuid": "1506ca1a-638d-4161-a261-8988fd674832", "owner": "Alice"})
    await mongo.create_snapshot({"uuid": "9414dccd-fcf8-47ba-8473-bdf755a7dbd3", "owner": "Alice"})
    res = await mongo.find_snapshots({"owner": "Alice"})
    assert len(res) == 2

    await mongo.create_snapshot({"uuid": "375c1b13-c575-475c-9dbb-fb8edb2478fc", "owner": "Alice"})
    with pytest.raises(DuplicateKeyError):
        await mongo.create_snapshot({"uuid": "375c1b13-c575-475c-9dbb-fb8edb2478fc", "owner": "Bob"})

    with pytest.raises(KeyError, match="uuid"):
        await mongo.create_snapshot({"owner": "Alice"})


@pytest.mark.asyncio
async def test_19_get_snapshot(mongo: Mongo) -> None:
    """Use get_snapshot to create documents in the snapshots collection."""
    await mongo.create_snapshot({"uuid": "ba92c24c-bbdc-44e0-adfb-6ae256da29ad", "owner": "Alice"})
    res = await mongo.get_snapshot({"uuid": "ba92c24c-bbdc-44e0-adfb-6ae256da29ad"})
    assert "uuid" in res
    assert res["uuid"] == "ba92c24c-bbdc-44e0-adfb-6ae256da29ad"


@pytest.mark.asyncio
async def test_20_append_distinct_elements_to_file(mongo: Mongo) -> None:
    """Use append_distinct_elements_to_file to update a document in the files collection."""
    await mongo.create_file({"uuid": "2776ba87-fc07-4922-b524-a33fe6c8c471", "a": [1], "b": [2]})  # type: ignore
    res = await mongo.find_files({"uuid": "2776ba87-fc07-4922-b524-a33fe6c8c471"}, AllKeys())
    assert len(res) == 1
    assert res[0]["uuid"] == "2776ba87-fc07-4922-b524-a33fe6c8c471"
    assert "meta_modify_date" not in res[0]
    assert "a" in res[0]
    assert res[0]["a"] == [1]
    assert "b" in res[0]
    assert res[0]["b"] == [2]

    APPEND_ME = {
        "a": 3,
        "b": [4, 5, 6],
    }
    res = await mongo.append_distinct_elements_to_file("2776ba87-fc07-4922-b524-a33fe6c8c471", APPEND_ME)  # type: ignore
    assert "meta_modify_date" in res  # type: ignore
    assert "a" in res  # type: ignore
    assert res["a"] == [1, 3]  # type: ignore
    assert "b" in res  # type: ignore
    assert res["b"] == [2, 4, 5, 6]  # type: ignore
