#!/usr/bin/env python
import os

import pymongo
from pymongo import MongoClient

env = {
    'TEST_DATABASE_HOST': 'localhost',
    'TEST_DATABASE_PORT': 27017,
}
for k in env:
    if k in os.environ:
        if isinstance(env[k], int):
            env[k] = int(os.environ[k])
        elif isinstance(env[k], float):
            env[k] = float(os.environ[k])
        else:
            env[k] = os.environ[k]

db = MongoClient(host=env['TEST_DATABASE_HOST'], port=env['TEST_DATABASE_PORT']).file_catalog
db.set_profiling_level(pymongo.ALL)
print('MongoDB profiling enabled')