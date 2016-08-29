from __future__ import print_function, division, absolute_import

import logging
import random
import time
from pprint import pprint
import threading
from multiprocessing.dummy import Pool
import os
import string
import hashlib

import pymongo
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

def get_db():
    return MongoClient('localhost').test_file_catalog

datasets = string.letters+string.digits
years = [str(y) for y in range(2000,2030)]
days = ['%02d%02d'%(m,d) for m in range(1,13) for d in range(1,32)]
indexes = [str(x) for x in range(1500)]
types = ['.i3.bz2','.root','_EHE.i3.bz2','_IceTop.i3.bz2','.dst']
def make_path():
    p = '{}_{}_{}_{}{}'.format(random.choice(years),
                               random.choice(days),
                               ''.join(random.sample(datasets,50)),
                               random.choice(indexes),
                               random.choice(types))
    while p in make_path.cache:
        p = '{}_{}_{}_{}{}'.format(random.choice(years),
                                   random.choice(days),
                                   ''.join(random.sample(datasets,50)),
                                   random.choice(indexes),
                                   random.choice(types))
    make_path.cache.add(p)
    return p
make_path.cache = set()

def get_path():
    return random.sample(make_path.cache,1)[0]

def make_uri(path):
    return 'gsiftp://gridftp.icecube.wisc.edu/data/exp/{}'.format(path.replace('_','/'))

def get_checksum(path):
    return hashlib.sha512(path).hexdigest()

numeric_datasets = set()
def make_file():
    path = make_path()
    d = random.randint(0,1000)
    numeric_datasets.add(d)
    return {
        'path': path,
        'dataset': d,
        'checksum': get_checksum(path),
        'replicas':[make_uri(path)],
    }

def get_dataset():
    return random.sample(numeric_datasets,1)[0]

def create_db():
    db = get_db()
    db.files.drop()
    db.files.create_index("path", unique=True)

def fill(n=1000):
    try:
        db = get_db()
        db.files.insert_many((make_file() for _ in range(n)),
                             ordered=False)
    except BulkWriteError as bwe:
        pprint(bwe.details)

def read():
    db = get_db()
    p = '_'.join(get_path().split('_')[:-2])
    if db.files.find({'path': {'$regex': '^'+p} }).count() < 1:
        raise Exception('could not find path like {}'.format(p))

def read_one():
    db = get_db()
    p = get_path()
    if not db.files.find_one({'path': p}):
        raise Exception('could not find path like {}'.format(p))

def read_dataset():
    db = get_db()
    d = get_dataset()
    if not list(db.files.find({'dataset': d})):
        raise Exception('could not find dataset like {}'.format(d))

def main():
    n = 1000000
    m = 1
    m2 = 10000
    m3 = 100
    
    create_db()

    pool = Pool(processes=5)
    start = time.time()
    fill(n)
    fill_time = time.time() - start
    print('{} inserts in {}s'.format(n,fill_time))

    start = time.time()
    results = []
    for _ in range(m):
        results.append(pool.apply_async(read, ()))
#        results.append(pool.apply_async(read_dataset, ()))
        for i in range(m2):
            results.append(pool.apply_async(read_one, ()))
            if i%m3 == 0:
                results.append(pool.apply_async(fill, (1,)))
    for r in results:
        r.get(timeout=1000000)
    read_time = time.time() - start
    pool.terminate()

    print('{}.{} reads in {}s'.format(m,m2,read_time))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
