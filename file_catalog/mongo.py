# mongo.py
"""File Catalog MongoDB Interface."""

import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Union, cast

from motor.motor_tornado import MotorClient, MotorCursor  # type: ignore[import]
import pymongo  # type: ignore[import]
from pymongo.results import InsertOneResult  # type: ignore[import]
from wipac_telemetry import tracing_tools as wtt

from .schema.types import Metadata

logger = logging.getLogger(__name__)


DEFAULT_MAX_TIME_MS = 10 * 60 * 1000  # 10 minutes


class AllKeys:  # pylint: disable=R0903
    """Include all keys in MongoDB find*() methods."""


class Mongo:
    """A ThreadPoolExecutor-based MongoDB client."""

    def __init__(  # pylint: disable=R0913
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        authSource: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        uri: Optional[str] = None,
    ) -> None:
        """Initialize the File Catalog's internal MongoDB client."""
        if uri:
            logger.info(f"MongoClient args: uri={uri}")
            self.close_me = MotorClient(uri, authSource=authSource)
            self.client = self.close_me.file_catalog
        else:
            logger.info(
                "MongoClient args: host=%s, port=%s, username=%s", host, port, username
            )
            self.close_me = MotorClient(
                host=host,
                port=port,
                authSource=authSource,
                username=username,
                password=password,
            )
            self.client = self.close_me.file_catalog

        self.executor = ThreadPoolExecutor(max_workers=10)
        logger.info("done setting up Mongo")

    @wtt.spanned(all_args=True)
    async def create_indexes(self) -> None:
        """Create indexes for all file-catalog mongo collections."""
        # all files (a.k.a. required fields)
        await self.client.files.create_index('uuid', unique=True, background=True)
        await self.client.files.create_index([('logical_name', pymongo.HASHED)], background=True)
        await self.client.files.create_index('locations', unique=True, background=True)
        await self.client.files.create_index(
            [('locations.path', pymongo.DESCENDING), ('locations.site', pymongo.DESCENDING)],
            background=True
        )
        await self.client.files.create_index('create_date', background=True)

        # all .i3 files
        await self.client.files.create_index('content_status', sparse=True, background=True)
        await self.client.files.create_index([('processing_level', 1), ('offline_processing_metadata.season', 1), ('locations.archive', 1)], sparse=True, background=True)
        await self.client.files.create_index('data_type', sparse=True, background=True)

        # data_type=real files
        await self.client.files.create_index('run.run_number', sparse=True, background=True)
        await self.client.files.create_index('run.start_datetime', sparse=True, background=True)
        await self.client.files.create_index('run.end_datetime', sparse=True, background=True)
        await self.client.files.create_index('offline_processing_metadata.first_event', sparse=True, background=True)
        await self.client.files.create_index('offline_processing_metadata.last_event', sparse=True, background=True)
        await self.client.files.create_index('offline_processing_metadata.season', sparse=True, background=True)

        # data_type=simulation files
        await self.client.files.create_index('iceprod.dataset', sparse=True, background=True)

        # # Collections
        await self.client.collections.create_index('uuid', unique=True, background=True)
        await self.client.collections.create_index('collection_name', background=True)
        await self.client.collections.create_index('owner', background=True)

        # # Snapshots
        await self.client.snapshots.create_index('uuid', unique=True, background=True)
        await self.client.snapshots.create_index('collection_id', background=True)
        await self.client.snapshots.create_index('owner', background=True)

    @staticmethod
    def _get_projection(
        keys: Optional[Union[List[str], AllKeys]] = None,
        default: Optional[Dict[str, bool]] = None,
    ) -> Dict[str, bool]:
        projection = {"_id": False}

        if not keys:
            if default:  # use default keys if they're available
                projection.update(default)
        elif isinstance(keys, AllKeys):
            pass  # only use "_id" constraint in projection
        elif isinstance(keys, list):
            projection.update({k: True for k in keys})
        else:
            raise TypeError(
                f"`keys` argument ({keys}) is not NoneType, list, or AllKeys"
            )

        return projection

    @staticmethod
    async def _limit_result_list(
        cursor: MotorCursor,
        limit: Optional[int] = None,
        start: int = 0,
    ) -> List[Dict[str, Any]]:
        """Get sublist of results from `cursor` using `limit` and `start`."""
        if limit:
            results = await cursor.skip(start).limit(limit).to_list(None)
        else:
            results = await cursor.skip(start).to_list(None)
        return cast(List[Dict[str, Any]], results)

    @wtt.spanned(all_args=True)
    async def find_files(
        self,
        query: Optional[Dict[str, Any]] = None,
        keys: Optional[Union[List[str], AllKeys]] = None,
        limit: Optional[int] = None,
        start: int = 0,
        max_time_ms: Optional[int] = DEFAULT_MAX_TIME_MS,
    ) -> List[Dict[str, Any]]:
        """Find files.

        Optionally, apply keyword arguments. "_id" is always excluded.

        Decorators:
            run_on_executor

        Keyword Arguments:
            query -- MongoDB query
            keys -- fields to include in MongoDB projection
            limit -- max count of files returned
            start -- starting index
            max_time_ms -- the query timeout in milliseconds

        Returns:
            List of MongoDB files
        """
        projection = Mongo._get_projection(
            keys, default={"uuid": True, "logical_name": True}
        )
        cursor = self.client.files.find(query, projection, max_time_ms=max_time_ms)
        results = await Mongo._limit_result_list(cursor, limit, start)

        return results

    @wtt.spanned(all_args=True)
    async def count_files(  # pylint: disable=W0613
        self,
        query: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> int:
        """Get count of files matching query."""
        if not query:
            query = {"uuid": {"$exists": True}}

        ret = await self.client.files.count_documents(query)

        return cast(int, ret)

    @wtt.spanned(all_args=True)
    async def create_file(self, metadata: Metadata) -> InsertOneResult:
        """Insert file metadata.

        Return InsertOneResult.
        """
        return cast(InsertOneResult, await self.client.files.insert_one(metadata))

    @wtt.spanned(all_args=True)
    async def get_file(
        self, filters: Dict[str, Any], max_time_ms: Optional[int] = DEFAULT_MAX_TIME_MS
    ) -> Optional[Metadata]:
        """Get file matching filters."""
        file = await self.client.files.find_one(
            filters, {"_id": False}, max_time_ms=max_time_ms
        )
        if file:
            return cast(Metadata, file)
        return None

    async def _find_file_and_update(
        self, uuid: str, update_query: Dict[str, Any]
    ) -> Metadata:
        """Wrap `find_one_and_update()`."""
        doc: Optional[Metadata] = await self.client.files.find_one_and_update(
            {"uuid": uuid},
            update_query,
            projection={"_id": False},
            maxTimeMS=DEFAULT_MAX_TIME_MS,
            return_document=pymongo.ReturnDocument.AFTER,
        )

        if doc is None:
            msg = f"Record ({uuid}) was not found, so it was not updated"
            logger.warning(msg)
            raise FileNotFoundError(msg)
        else:
            return doc

    @wtt.spanned(all_args=True)
    async def update_file(self, uuid: str, update: Metadata) -> Metadata:
        """Update file using `update` subset.

        Return the updated file document.
        """
        return await self._find_file_and_update(uuid, {"$set": update})

    @wtt.spanned(all_args=True)
    async def replace_file(self, metadata: Metadata) -> None:
        """Replace file.

        Metadata must include 'uuid'.
        """
        uuid = metadata["uuid"]

        result = await self.client.files.replace_one({"uuid": uuid}, metadata)

        if result.modified_count != 1:
            msg = f"updated {result.modified_count} files with id {uuid}"
            logger.error(msg)
            raise Exception(msg)

    @wtt.spanned(all_args=True)
    async def delete_file(self, filters: Dict[str, Any]) -> None:
        """Delete file matching filters."""
        # note: result.deleted_count == 1, even when more than one document matches
        match_count = await self.count_files(filters)
        if match_count > 1:
            msg = f"filters {filters} matches {match_count} documents; preventing ambiguous delete of files document"
            logger.error(msg)
            raise Exception(msg)

        result = await self.client.files.delete_one(filters)

        if result.deleted_count != 1:
            msg = f"deleted {result.deleted_count} files with filters {filters}"
            logger.error(msg)
            raise Exception(msg)

    async def find_collections(
        self,
        keys: Optional[Union[List[str], AllKeys]] = None,
        limit: Optional[int] = None,
        start: int = 0,
    ) -> List[Dict[str, Any]]:
        """Find all collections.

        Optionally, apply keyword arguments. "_id" is always excluded.

        Keyword Arguments:
            keys -- fields to include in MongoDB projection
            limit -- max count of collections returned
            start -- starting index

        Returns:
            List of MongoDB collections
        """
        projection = Mongo._get_projection(keys)  # show all fields by default
        cursor = self.client.collections.find({"uuid": {"$exists": True}}, projection)
        results = await Mongo._limit_result_list(cursor, limit, start)

        return results

    async def create_collection(self, metadata: Dict[str, Any]) -> str:
        """Create collection, insert metadata.

        Return uuid.
        """
        result = await self.client.collections.insert_one(metadata)

        if not result.inserted_id:
            msg = "did not insert new collection"
            logger.warning(msg)
            raise Exception(msg)

        return cast(str, metadata["uuid"])

    async def get_collection(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Get collection matching filters."""
        collection = await self.client.collections.find_one(filters, {"_id": False})
        return cast(Dict[str, Any], collection)

    async def find_snapshots(
        self,
        query: Optional[Dict[str, Any]] = None,
        keys: Optional[Union[List[str], AllKeys]] = None,
        limit: Optional[int] = None,
        start: int = 0,
    ) -> List[Dict[str, Any]]:
        """Find snapshots.

        Optionally, apply keyword arguments. "_id" is always excluded.

        Keyword Arguments:
            query -- MongoDB query
            keys -- fields to include in MongoDB projection
            limit -- max count of snapshots returned
            start -- starting index

        Returns:
            List of MongoDB snapshots
        """
        projection = Mongo._get_projection(keys)  # show all fields by default
        cursor = self.client.snapshots.find(query, projection)
        results = await Mongo._limit_result_list(cursor, limit, start)

        return results

    async def create_snapshot(self, metadata: Dict[str, Any]) -> str:
        """Insert metadata into 'snapshots' collection.

        Return uuid.
        """
        result = await self.client.snapshots.insert_one(metadata)

        if (not result) or (not result.inserted_id):
            msg = "did not insert new snapshot"
            logger.warning(msg)
            raise Exception(msg)

        return cast(str, metadata["uuid"])

    async def get_snapshot(self, filters: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Find snapshot, optionally filtered."""
        snapshot = await self.client.snapshots.find_one(filters, {"_id": False})
        return cast(Dict[str, Any], snapshot)

    @wtt.spanned(all_args=True)
    async def append_distinct_elements_to_file(
        self, uuid: str, metadata: Dict[str, Any]
    ) -> Metadata:
        """Append distinct elements to arrays within a file document.

        Return the updated file document.
        """
        # build the query to update the file document
        update_query: Dict[str, Any] = {"$addToSet": {}}
        for key in metadata:
            if isinstance(metadata[key], list):
                update_query["$addToSet"][key] = {"$each": metadata[key]}
            else:
                update_query["$addToSet"][key] = metadata[key]

        # update the file document
        update_query["$set"] = {"meta_modify_date": str(datetime.datetime.utcnow())}
        return await self._find_file_and_update(uuid, update_query)
