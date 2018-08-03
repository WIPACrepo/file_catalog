from __future__ import absolute_import, division, print_function

import logging
from collections import Iterable

import pymongo
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from bson.objectid import ObjectId

from concurrent.futures import ThreadPoolExecutor
from tornado.concurrent import run_on_executor

logger = logging.getLogger('mongo')

class Mongo(object):
    """A ThreadPoolExecutor-based MongoDB client"""
    def __init__(self, host=None):
        kwargs = {}
        if host:
            parts = host.split(':')
            if len(parts) == 2:
                kwargs['port'] = int(parts[1])
            kwargs['host'] = parts[0]
        logger.info('MongoClient args: %r', kwargs)

        self.client = MongoClient(**kwargs).file_catalog

        self.client.files.create_index('uuid', unique=True)
        self.client.files.create_index('logical_name')
        self.client.files.create_index('locations.archive')
        self.client.files.create_index('create_date')
        self.client.files.create_index('content_status')
        self.client.files.create_index('processing_level', sparse=True)
        self.client.files.create_index('run_number', sparse=True)
        self.client.files.create_index('start_datetime', sparse=True)
        self.client.files.create_index('end_datetime', sparse=True)
        self.client.files.create_index('offline_processing_metadata.first_event', sparse=True)
        self.client.files.create_index('offline_processing_metadata.last_event', sparse=True)
        self.client.files.create_index('offline.season', sparse=True)
        self.client.files.create_index('iceprod.dataset', sparse=True)

        self.client.collections.create_index('uuid', unique=True)
        self.client.collections.create_index('collection_name')
        self.client.collections.create_index('owner')
        
        self.client.snapshots.create_index('uuid', unique=True)
        self.client.snapshots.create_index('collection_id')
        self.client.snapshots.create_index('owner')

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
    def update_file(self, metadata):
        uuid = metadata['uuid']

        result = self.client.files.update_one({'uuid': uuid},
                                              {'$set': metadata})

        if result.modified_count is None:
            logger.warn('Cannot determine if document has been modified since `result.modified_count` has the value `None`. `result.matched_count` is %s' % result.matched_count)
        elif result.modified_count != 1:
            logger.warn('updated %s files with id %r',
                        result.modified_count, metadata_id)
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
                        result.modified_count, metadata_id)
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

