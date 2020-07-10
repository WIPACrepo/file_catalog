from __future__ import absolute_import, division, print_function

import datetime
import logging
from concurrent.futures import ThreadPoolExecutor

import pymongo
from bson.objectid import ObjectId
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from tornado.concurrent import run_on_executor

try:
    from collections.abc import Iterable
except ImportError:
    from collections import Iterable



logger = logging.getLogger('mongo')

class Mongo(object):
    """A ThreadPoolExecutor-based MongoDB client"""
    def __init__(self, host=None, port=None, authSource=None, username=None, password=None, uri=None):

        if uri:
            logger.info(f"MongoClient args: uri={uri}")
            self.client = MongoClient(uri, authSource=authSource).file_catalog
        else:
            logger.info('MongoClient args: host=%s, port=%s, username=%s', host, port, username)
            self.client = MongoClient(host=host, port=port,
                                      authSource=authSource,
                                      username=username, password=password).file_catalog

        self.client.files.create_index('uuid', unique=True, background=True)
        self.client.files.create_index('logical_name', unique=True, background=True)
        self.client.files.create_index([('logical_name',pymongo.HASHED)], background=True)
        self.client.files.create_index('locations', unique=True, background=True)
        self.client.files.create_index([('locations.site',pymongo.DESCENDING),('locations.path',pymongo.DESCENDING)], background=True)
        self.client.files.create_index('locations.archive', background=True)
        self.client.files.create_index('create_date', background=True)
        self.client.files.create_index('content_status', background=True)
        self.client.files.create_index('processing_level', sparse=True, background=True)
        self.client.files.create_index('run_number', sparse=True, background=True)
        self.client.files.create_index('start_datetime', sparse=True, background=True)
        self.client.files.create_index('end_datetime', sparse=True, background=True)
        self.client.files.create_index('offline_processing_metadata.first_event', sparse=True, background=True)
        self.client.files.create_index('offline_processing_metadata.last_event', sparse=True, background=True)
        self.client.files.create_index('offline_processing_metadata.season', sparse=True, background=True)
        self.client.files.create_index('iceprod.dataset', sparse=True, background=True)

        self.client.collections.create_index('uuid', unique=True, background=True)
        self.client.collections.create_index('collection_name', background=True)
        self.client.collections.create_index('owner', background=True)

        self.client.snapshots.create_index('uuid', unique=True, background=True)
        self.client.snapshots.create_index('collection_id', background=True)
        self.client.snapshots.create_index('owner', background=True)

        self.executor = ThreadPoolExecutor(max_workers=10)
        logger.info('done setting up Mongo')

    @run_on_executor
    def find_files(self, query={}, keys=None, limit=None, start=0):
        if keys and isinstance(keys,Iterable) and not isinstance(keys,str):
            projection = {k:True for k in keys}
        else:
            projection = {'uuid':True, 'logical_name':True}
        projection['_id'] = False

        result = self.client.files.find(query, projection)
        ret = []

        # `limit` and `skip` are ignored by __getitem__:
        # http://api.mongodb.com/python/current/api/pymongo/cursor.html#pymongo.cursor.Cursor.__getitem__
        #
        # Therefore, implement it manually:
        end = None

        if limit is not None:
            end = start + limit

        for row in result[start:end]:
            ret.append(row)
        return ret

    @run_on_executor
    def count_files(self, query={}, **kwargs):
        ret = self.client.files.count_documents(query)
        return ret

    @run_on_executor
    def create_file(self, metadata):
        result = self.client.files.insert_one(metadata)
        if (not result) or (not result.inserted_id):
            logger.warn('did not insert file')
            raise Exception('did not insert new file')
        return metadata['uuid']

    @run_on_executor
    def get_file(self, filters):
        return self.client.files.find_one(filters, {'_id':False})

    @run_on_executor
    def update_file(self, uuid, metadata):
        result = self.client.files.update_one({'uuid': uuid},
                                              {'$set': metadata})

        if result.modified_count is None:
            logger.warn('Cannot determine if document has been modified since `result.modified_count` has the value `None`. `result.matched_count` is %s' % result.matched_count)
        elif result.modified_count != 1:
            logger.warn('updated %s files with id %r',
                        result.modified_count, uuid)
            raise Exception('did not update')

    @run_on_executor
    def replace_file(self, metadata):
        uuid = metadata['uuid']

        result = self.client.files.replace_one({'uuid': uuid},
                                               metadata)

        if result.modified_count is None:
            logger.warn('Cannot determine if document has been modified since `result.modified_count` has the value `None`. `result.matched_count` is %s' % result.matched_count)
        elif result.modified_count != 1:
            logger.warn('updated %s files with id %r',
                        result.modified_count, uuid)
            raise Exception('did not update')

    @run_on_executor
    def delete_file(self, filters):
        result = self.client.files.delete_one(filters)

        if result.deleted_count != 1:
            logger.warn('deleted %d files with filter %r',
                        result.deleted_count, filter)
            raise Exception('did not delete')

    @run_on_executor
    def find_collections(self, keys=None, limit=None, start=0):
        if keys and isinstance(keys,Iterable) and not isinstance(keys,str):
            projection = {k:True for k in keys}
        else:
            projection = {} # show all fields
        projection['_id'] = False

        result = self.client.collections.find({}, projection)
        ret = []

        # `limit` and `skip` are ignored by __getitem__:
        # http://api.mongodb.com/python/current/api/pymongo/cursor.html#pymongo.cursor.Cursor.__getitem__
        #
        # Therefore, implement it manually:
        end = None

        if limit is not None:
            end = start + limit

        for row in result[start:end]:
            ret.append(row)
        return ret

    @run_on_executor
    def create_collection(self, metadata):
        result = self.client.collections.insert_one(metadata)
        if (not result) or (not result.inserted_id):
            logger.warn('did not insert collection')
            raise Exception('did not insert new collection')
        return metadata['uuid']

    @run_on_executor
    def get_collection(self, filters):
        return self.client.collections.find_one(filters, {'_id':False})

    @run_on_executor
    def find_snapshots(self, query={}, keys=None, limit=None, start=0):
        if keys and isinstance(keys,Iterable) and not isinstance(keys,str):
            projection = {k:True for k in keys}
        else:
            projection = {} # show all fields
        projection['_id'] = False

        result = self.client.snapshots.find(query, projection)
        ret = []

        # `limit` and `skip` are ignored by __getitem__:
        # http://api.mongodb.com/python/current/api/pymongo/cursor.html#pymongo.cursor.Cursor.__getitem__
        #
        # Therefore, implement it manually:
        end = None

        if limit is not None:
            end = start + limit

        for row in result[start:end]:
            ret.append(row)
        return ret

    @run_on_executor
    def create_snapshot(self, metadata):
        result = self.client.snapshots.insert_one(metadata)
        if (not result) or (not result.inserted_id):
            logger.warn('did not insert snapshot')
            raise Exception('did not insert new snapshot')
        return metadata['uuid']

    @run_on_executor
    def get_snapshot(self, filters):
        return self.client.snapshots.find_one(filters, {'_id':False})

    @run_on_executor
    def append_distinct_elements_to_file(self, uuid, metadata):
        """Append distinct elements to arrays within a file document."""
        # build the query to update the file document
        update_query = {"$addToSet": {}}
        for key in metadata:
            if isinstance(metadata[key], list):
                update_query["$addToSet"][key] = {"$each": metadata[key]}
            else:
                update_query["$addToSet"][key] = metadata[key]

        # update the file document
        update_query["$set"] = {"meta_modify_date": str(datetime.datetime.utcnow())}
        result = self.client.files.update_one({'uuid': uuid}, update_query)

        # log and/or throw if the update results are surprising
        if result.modified_count is None:
            logger.warn('Cannot determine if document has been modified since `result.modified_count` has the value `None`. `result.matched_count` is %s' % result.matched_count)
        elif result.modified_count != 1:
            logger.warn('updated %s files with id %r',
                        result.modified_count, uuid)
            raise Exception('did not update')
