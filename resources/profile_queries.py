#!/usr/bin/env python

# fmt:off
# flake8:noqa

import os

import pymongo  # type: ignore[import]
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
ret = db.profiling_level()
if ret != pymongo.ALL:
    raise Exception('profiling disabled')
db.set_profiling_level(pymongo.OFF)

unrealistic_queries = [
    {'locations.archive': True},
    {'locations.archive': False},
    {'locations.archive': None},
    {'locations.archive': None, 'run.first_event': {'$lte': 400}, 'run.last_event': {'$gte': 400}},
]

bad_queries = []
ret = db.system.profile.find({ 'op': { '$nin' : ['command', 'insert'] } })
for query in ret:
    try:
        if 'find' in query['command'] and query['command']['find'] == 'collections':
            continue
        # exclude unrealistic test queries
        if 'filter' in query['command'] and query['command']['filter'] in unrealistic_queries:
            continue
        if 'planSummary' not in query:
            print(query)
            continue
        if 'IXSCAN' not in query['planSummary']:
            bad_queries.append((query['command'],query['planSummary']))
    except Exception:
        print(query)
        raise

if bad_queries:
    for q,p in bad_queries:
        print(q)
        print(p)
        print('---')
    raise Exception('Non-indexed queries')

print('MongoDB profiling OK')
