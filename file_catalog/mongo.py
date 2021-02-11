"""File Catalog MongoDB Interface."""


from __future__ import absolute_import, division, print_function

import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, cast, Dict, List, Optional, Union

import pymongo  # type: ignore[import]
from pymongo import MongoClient
from tornado.concurrent import run_on_executor

logger = logging.getLogger("mongo")


class AllKeys:  # pylint: disable=R0903
    """Include all keys in MongoDB find*() methods."""


class Mongo(object):
    """A ThreadPoolExecutor-based MongoDB client."""

    # fmt:off
    def __init__(self, host=None, port=None, authSource=None, username=None, password=None, uri=None):

        if uri:
            logger.info(f"MongoClient args: uri={uri}")
            self.client = MongoClient(uri, authSource=authSource).file_catalog
        else:
            logger.info('MongoClient args: host=%s, port=%s, username=%s', host, port, username)
            self.client = MongoClient(host=host, port=port,
                                      authSource=authSource,
                                      username=username, password=password).file_catalog

        # all files
        self.client.files.create_index('uuid', unique=True, background=True)
        self.client.files.create_index('logical_name', unique=True, background=True)
        self.client.files.create_index([('logical_name',pymongo.HASHED)], background=True)
        self.client.files.create_index('locations', unique=True, background=True)
        self.client.files.create_index([('locations.site',pymongo.DESCENDING),('locations.path',pymongo.DESCENDING)], background=True)
        self.client.files.create_index('locations.archive', background=True)
        self.client.files.create_index('create_date', background=True)

        # all .i3 files
        self.client.files.create_index('content_status', sparse=True, background=True)
        self.client.files.create_index('processing_level', sparse=True, background=True)
        self.client.files.create_index('data_type', sparse=True, background=True)

        # data_type=real files
        self.client.files.create_index('run_number', sparse=True, background=True)
        self.client.files.create_index('start_datetime', sparse=True, background=True)
        self.client.files.create_index('end_datetime', sparse=True, background=True)
        self.client.files.create_index('offline_processing_metadata.first_event', sparse=True, background=True)
        self.client.files.create_index('offline_processing_metadata.last_event', sparse=True, background=True)
        self.client.files.create_index('offline_processing_metadata.season', sparse=True, background=True)

        # data_type=simulation files
        self.client.files.create_index('iceprod.dataset', sparse=True, background=True)

        self.client.collections.create_index('uuid', unique=True, background=True)
        self.client.collections.create_index('collection_name', background=True)
        self.client.collections.create_index('owner', background=True)

        self.client.snapshots.create_index('uuid', unique=True, background=True)
        self.client.snapshots.create_index('collection_id', background=True)
        self.client.snapshots.create_index('owner', background=True)

        self.executor = ThreadPoolExecutor(max_workers=10)
        logger.info('done setting up Mongo')
    # fmt:on

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
            pass  # only use "id_" constraint in projection
        elif isinstance(keys, list):
            projection.update({k: True for k in keys})
        else:
            raise TypeError(
                f"`keys` argument ({keys}) is not NoneType, list, or AllKeys"
            )
        return projection

    @staticmethod
    def _limit_result_list(
        result: List[Dict[str, Any]], limit: Optional[int] = None, start: int = 0,
    ) -> List[Dict[str, Any]]:
        """Get sublist of `results` using `limit` and `start`.

         `limit` and `skip` are ignored by __getitem__:
         http://api.mongodb.com/python/current/api/pymongo/cursor.html#pymongo.cursor.Cursor.__getitem__

        Therefore, implement it manually.
        """
        ret = []
        end = None

        if limit is not None:
            end = start + limit

        for row in result[start:end]:
            ret.append(row)
        return ret

    @run_on_executor
    def find_files(
        self,
        query: Optional[Dict[str, Any]] = None,
        keys: Optional[Union[List[str], AllKeys]] = None,
        limit: Optional[int] = None,
        start: int = 0,
    ) -> List[Dict[str, Any]]:
        """Find files.

        Optionally, apply keyword arguments. "id_" is always excluded.

        Decorators:
            run_on_executor

        Keyword Arguments:
            query -- MongoDB query
            keys -- fields to include in MongoDB projection
            limit -- max count of files returned
            start -- starting index

        Returns:
            List of MongoDB files
        """
        projection = Mongo._get_projection(
            keys, default={"uuid": True, "logical_name": True}
        )
        result = self.client.files.find(query, projection)
        ret = Mongo._limit_result_list(result, limit, start)

        return ret

    @run_on_executor
    def count_files(  # pylint: disable=W0613
        self, query: Optional[Dict[str, Any]] = None, **kwargs: Any,
    ) -> int:
        """Get count of files matching query."""
        if not query:
            query = {}
        ret = self.client.files.count_documents(query)
        return cast(int, ret)

    @run_on_executor
    def create_file(self, metadata: Dict[str, Any]) -> str:
        """Insert file metadata.

        Return uuid.
        """
        result = self.client.files.insert_one(metadata)
        if (not result) or (not result.inserted_id):
            logger.warning("did not insert file")
            raise Exception("did not insert new file")
        return cast(str, metadata["uuid"])

    @run_on_executor
    def get_file(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Get file matching filters."""
        file = self.client.files.find_one(filters, {"_id": False})
        return cast(Dict[str, Any], file)

    @run_on_executor
    def update_file(self, uuid: str, metadata: Dict[str, Any]) -> None:
        """Update file."""
        result = self.client.files.update_one({"uuid": uuid}, {"$set": metadata})

        if result.modified_count is None:
            logger.warning(
                "Cannot determine if document has been modified since `result.modified_count` has the value `None`. `result.matched_count` is %s",
                result.matched_count,
            )
        elif result.modified_count != 1:
            logger.warning("updated %s files with id %r", result.modified_count, uuid)
            raise Exception("did not update")

    @run_on_executor
    def replace_file(self, metadata: Dict[str, Any]) -> None:
        """Replace file.

        Metadata must include 'uuid'.
        """
        uuid = metadata["uuid"]

        result = self.client.files.replace_one({"uuid": uuid}, metadata)

        if result.modified_count is None:
            logger.warning(
                "Cannot determine if document has been modified since `result.modified_count` has the value `None`. `result.matched_count` is %s",
                result.matched_count,
            )
        elif result.modified_count != 1:
            logger.warning("updated %s files with id %r", result.modified_count, uuid)
            raise Exception("did not update")

    @run_on_executor
    def delete_file(self, filters: Dict[str, Any]) -> None:
        """Delete file matching filters."""
        result = self.client.files.delete_one(filters)

        if result.deleted_count != 1:
            logger.warning(
                "deleted %d files with filter %r", result.deleted_count, filters
            )
            raise Exception("did not delete")

    @run_on_executor
    def find_collections(
        self,
        keys: Optional[Union[List[str], AllKeys]] = None,
        limit: Optional[int] = None,
        start: int = 0,
    ) -> List[Dict[str, Any]]:
        """Find all collections.

        Optionally, apply keyword arguments. "id_" is always excluded.

        Decorators:
            run_on_executor

        Keyword Arguments:
            keys -- fields to include in MongoDB projection
            limit -- max count of collections returned
            start -- starting index

        Returns:
            List of MongoDB collections
        """
        projection = Mongo._get_projection(keys)  # show all fields by default
        result = self.client.collections.find({}, projection)
        ret = Mongo._limit_result_list(result, limit, start)

        return ret

    @run_on_executor
    def create_collection(self, metadata: Dict[str, Any]) -> str:
        """Create collection, insert metadata.

        Return uuid.
        """
        result = self.client.collections.insert_one(metadata)
        if (not result) or (not result.inserted_id):
            logger.warning("did not insert collection")
            raise Exception("did not insert new collection")
        return cast(str, metadata["uuid"])

    @run_on_executor
    def get_collection(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Get collection matching filters."""
        collection = self.client.collections.find_one(filters, {"_id": False})
        return cast(Dict[str, Any], collection)

    @run_on_executor
    def find_snapshots(
        self,
        query: Optional[Dict[str, Any]] = None,
        keys: Optional[Union[List[str], AllKeys]] = None,
        limit: Optional[int] = None,
        start: int = 0,
    ) -> List[Dict[str, Any]]:
        """Find snapshots.

        Optionally, apply keyword arguments. "id_" is always excluded.

        Decorators:
            run_on_executor

        Keyword Arguments:
            query -- MongoDB query
            keys -- fields to include in MongoDB projection
            limit -- max count of snapshots returned
            start -- starting index

        Returns:
            List of MongoDB snapshots
        """
        projection = Mongo._get_projection(keys)  # show all fields by default
        result = self.client.snapshots.find(query, projection)
        ret = Mongo._limit_result_list(result, limit, start)

        return ret

    @run_on_executor
    def create_snapshot(self, metadata: Dict[str, Any]) -> str:
        """Insert metadata into 'snapshots' collection."""
        result = self.client.snapshots.insert_one(metadata)
        if (not result) or (not result.inserted_id):
            logger.warning("did not insert snapshot")
            raise Exception("did not insert new snapshot")
        return cast(str, metadata["uuid"])

    @run_on_executor
    def get_snapshot(self, filters: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Find snapshot, optionally filtered."""
        snapshot = self.client.snapshots.find_one(filters, {"_id": False})
        return cast(Dict[str, Any], snapshot)

    @run_on_executor
    def append_distinct_elements_to_file(
        self, uuid: str, metadata: Dict[str, Any]
    ) -> None:
        """Append distinct elements to arrays within a file document."""
        # build the query to update the file document
        update_query: Dict[str, Any] = {"$addToSet": {}}
        for key in metadata:
            if isinstance(metadata[key], list):
                update_query["$addToSet"][key] = {"$each": metadata[key]}
            else:
                update_query["$addToSet"][key] = metadata[key]

        # update the file document
        update_query["$set"] = {"meta_modify_date": str(datetime.datetime.utcnow())}
        result = self.client.files.update_one({"uuid": uuid}, update_query)

        # log and/or throw if the update results are surprising
        if result.modified_count is None:
            logger.warning(
                "Cannot determine if document has been modified since `result.modified_count` has the value `None`. `result.matched_count` is %s",
                result.matched_count,
            )
        elif result.modified_count != 1:
            logger.warning("updated %s files with id %r", result.modified_count, uuid)
            raise Exception("did not update")
