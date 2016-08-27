from __future__ import absolute_import, division, print_function

import logging

from pymongo import MongoClient
from pymongo.errors import BulkWriteError

from concurrent.futures import ThreadPoolExecutor
from tornado.concurrent import run_on_executor

logger = logging.getLogger('mongo')

class Mongo(object):
    """A ThreadPoolExecutor-based MongoDB client"""
    def __init__(self, host='localhost'):
        self.client = MongoClient(host)
        self.executor = ThreadPoolExecutor(max_workers=10)

    @run_on_executor
    def find_files(self, query={}, limit=100000000000, start=0):
        projection = ('_id', 'file_name')
        result = self.client.find(query, projection, limit=limit+start)
        return result[start:]

    @run_on_executor
    def create_file(self, metadata):
        result = self.client.files.insert_one(metadata)
        if (not result) or (not result.inserted_id):
            logger.warn('did not insert file')
            raise Exception('did not insert new file')
        return result.inserted_id

    @run_on_executor
    def get_file(self, **kwargs):
        result = self.client.find_one(kwargs)
        if not result:
            logger.warn('did not find file with filter %r', kwargs)
            raise Exception('did not find file')
        return result

    @run_on_executor
    def update_file(self, metadata):
        result = self.client.files.update_one({'_id':metadata['_id']}, metadata)
        if result.modified_count != 1:
            logger.warn('updated %d files with id %r',
                        result.modified_count, metadata['_id'])
            raise Exception('did not update')

    @run_on_executor
    def delete_file(self, id):
        result = self.client.files.delete_one({'_id':id})
        if result.deleted_count != 1:
            logger.warn('deleted %d files with id %r',
                        result.deleted_count, id)
            raise Exception('did not delete')
