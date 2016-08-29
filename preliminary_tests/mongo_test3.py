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
from collections import OrderedDict
import pickle

import pymongo
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

def get_db():
    return MongoClient('mongodb-test-2.icecube.wisc.edu').test_file_catalog

datasets = string.letters+string.digits
years = [str(y) for y in range(2000,2030)]
days = ['%02d%02d'%(m,d) for m in range(1,13) for d in range(1,32)]
indexes = [str(x) for x in range(1500)]
types = ['.i3.bz2','.root','_EHE.i3.bz2','_IceTop.i3.bz2','.dst']
def make_path():
    p = '{}/{}/{}_{}{}'.format(random.choice(years),
                               random.choice(days),
                               ''.join(random.sample(datasets,50)),
                               random.choice(indexes),
                               random.choice(types))
    while p in make_path.cache:
        p = '{}/{}/{}_{}{}'.format(random.choice(years),
                                   random.choice(days),
                                   ''.join(random.sample(datasets,50)),
                                   random.choice(indexes),
                                   random.choice(types))
    make_path.cache.add(p)
    return p.split('/')
make_path.cache = set()

def get_path():
    return random.sample(make_path.cache,1)[0].split('/')

def make_uri(path):
    return os.path.join(*(['gsiftp://gridftp.icecube.wisc.edu/data/exp/']+path))

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
        'checksum': get_checksum('_'.join(path)),
        'replicas':[make_uri(path)],
    }

def get_dataset():
    return random.sample(numeric_datasets,1)[0]

def create_db():
    db = get_db()
    db.files.drop()

def encode(d):
    ret = d.copy()
    if 'children' in d:
        ret['children'] = pickle.dumps(d['children'])
    return ret

def decode(d):
    ret = d.copy()
    if 'children' in d:
        ret['children'] = pickle.loads(d['children'])
    return ret

root = None
def fill(n=1000):
    global root
    try:
        db = get_db()
        if not root:
            id = db.files.insert_one(encode({'name':None,'type':'directory','children':{}})).inserted_id
            root = {'_id':id,'name':None,'type':'directory','children':{}}
        for _ in range(n):
            f = make_file()
            d = root
            while len(f['path']) > 1:
                n = f['path'].pop(0)
                if n in d['children']:
                    d = decode(db.files.find_one({'_id':d['children'][n]}))
                else:
                    id = db.files.insert_one(encode({'name':n,'type':'directory','children':{}})).inserted_id
                    d['children'][n] = id
                    db.files.find_one_and_replace({'_id':d['_id']},encode(d))
                    d = {'_id':id,'name':n,'type':'directory','children':{}}
            f['name'] = n = f.pop('path')[0]
            f['type'] = 'file'
            id = db.files.insert_one(f).inserted_id
            d['children'][n] = id
            db.files.find_one_and_replace({'_id':d['_id']},encode(d))
    except BulkWriteError as bwe:
        pprint(bwe.details)
    except:
        pprint(d)
        raise

def read():
    db = get_db()
    p = path = get_path()[:-2]
    d = root
    while p:
        n = p.pop(0)
        id = d['children'][n]
        d2 = db.files.find_one({'_id':id})
        if not d2:
            raise Exception('could not find directory',n)
        d = decode(d2)
    files = []
    d = d['children'].values()
    while d:
        new_d = []
        for f in db.files.find({'_id':{'$in':d}}):
            if f['type'] == 'directory':
                new_d.extend(decode(f)['children'].values())
            else:
                files.append(f['_id'])
        d = new_d
    if db.files.find({'_id':{'$in':files}}).count() < 1:
        raise Exception('could not find path like {}'.format('_'.join(path)))

def read_one():
    db = get_db()
    p = path = get_path()
    d = root
    while p:
        n = p.pop(0)
        id = d['children'][n]
        d2 = db.files.find_one({'_id':id})
        if not d2:
            raise Exception('could not find directory/file',n)
        d = decode(d2)
    if not d:
        raise Exception('could not find path like {}'.format(path))

def read_dataset():
    db = get_db()
    d = get_dataset()
    if not list(db.files.find({'dataset': d})):
        raise Exception('could not find dataset like {}'.format(d))

def main():
    n = 10000
    m = 10
    m2 = 100
    
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
            if i%10 == 0:
                results.append(pool.apply_async(fill, (1,)))
    for r in results:
        r.get(timeout=1000000)
    read_time = time.time() - start
    pool.terminate()

    print('{}.{} reads in {}s'.format(m,m2,read_time))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
