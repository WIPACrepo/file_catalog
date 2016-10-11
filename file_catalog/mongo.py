from __future__ import absolute_import, division, print_function

import logging

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
        self.client = MongoClient(**kwargs).file_catalog
        self.executor = ThreadPoolExecutor(max_workers=10)

    @run_on_executor
    def find_files(self, query={}, limit=100000000000, start=0):
        if '_id' in query and not isinstance(query['_id'], dict):
            query['_id'] = ObjectId(query['_id'])
        projection = ('_id', 'uid')
        result = self.client.files.find(query, projection, limit=limit+start)
        ret = []
        for row in result[start:]:
            row['_id'] = str(row['_id'])
            ret.append(row)
        return ret

    @run_on_executor
    def create_file(self, metadata):
        result = self.client.files.insert_one(metadata)
        if (not result) or (not result.inserted_id):
            logger.warn('did not insert file')
            raise Exception('did not insert new file')
        return str(result.inserted_id)

    @run_on_executor
    def get_file(self, filters):
        if '_id' in filters and not isinstance(filters['_id'], dict):
            filters['_id'] = ObjectId(filters['_id'])
        ret = self.client.files.find_one(filters)
        if ret and '_id' in ret:
            ret['_id'] = str(ret['_id'])
        return ret

    @run_on_executor
    def update_file(self, metadata):
        if '_id' in metadata and not isinstance(metadata['_id'], dict):
            metadata['_id'] = ObjectId(metadata['_id'])

        # _id cannot be updated. Remove it from dict and add it later again
        # in order to preserve correct behavior after executing this function
        metadata_id = metadata['_id']
        del metadata['_id']

        result = self.client.files.update_one({'_id': metadata_id},
                                              {'$set': metadata})

        metadata['_id'] = str(metadata_id)

        if result.modified_count is None:
            logger.warn('Cannot detrmine if document has been modified since `result.modified_count` has the value `None`. `result.matched_count` is %s' % result.matched_count)
        elif result.modified_count != 1:
            logger.warn('updated %s files with id %r',
                        result.modified_count, metadata_id)
            raise Exception('did not update')

    @run_on_executor
    def delete_file(self, filters):
        if '_id' in filters and not isinstance(filters['_id'], dict):
            filters['_id'] = ObjectId(filters['_id'])
        result = self.client.files.delete_one(filters)
        if result.deleted_count != 1:
            logger.warn('deleted %d files with filter %r',
                        result.deleted_count, filter)
            raise Exception('did not delete')
