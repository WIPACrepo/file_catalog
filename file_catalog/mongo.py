import logging

from pymongo import MongoClient
from pymongo.errors import BulkWriteError

logger = logging.getLogger('mongo')

class Mongo(object):
    def __init__(self, host='localhost'):
        self.client = MongoClient(host)
