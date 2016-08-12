from __future__ import print_function, division, absolute_import

import logging
import random
import time

import pymongo
from pymongo import MongoClient


def get_db():
    return MongoClient('mongodb-test-2.icecube.wisc.edu').test_file_catalog

years = ['2010','2011','2012','2013','2014','2015','2016']
days = ['%02d%02d'%(m,d) for m in range(1,13) for d in range(1,32)]
indexes = [str(x) for x in range(500)]
types = ['.i3.bz2','.root','_EHE.i3.bz2','_IceTop.i3.bz2','.dst']
def make_path():
    return '{}_{}_{}{}'.format(random.choice(years),
                               random.choice(days),
                               random.choice(indexes),
                               random.choice(types))

def make_uri(path):
    return 'gsiftp://gridftp.icecube.wisc.edu/data/exp/{}'.format(path.replace('_','/'))

def make_file():
    path = make_path()
    return {
        'path': path,
        'dataset': random.randint(0,1000000),
        'replicas':[make_uri(path)],
    }

def create_db():
    db = get_db()
    db.files.drop()
    db.files.create_index("path", unique=True)

def fill(n=1000):
    db = get_db()
    for i in range(n//1000):
        n_files = 1000 if i*1000 < n else n%1000
        db.files.insert_many([make_file() for _ in range(n_files)])

def read():
    p = '_'.join(make_path().split('_')[:-2])
    if db.files.find({'path': {'$regex': '/^{}/'.format(p)} }).count() < 1:
        raise Exception('could not find path like {}'.format(p))

def main():
    n = 10000
    m = 100

    create_db()

    start = time.time()
    fill(n)
    fill_time = time.time() - start

    start = time.time()
    for _ in range(m):
        read()
    read_time = time.time() - start

    print('{} inserts in {}s'.format(n,fill_time))
    print('{} reads in {}s'.format(m,read_time))

if __name__ == '__main__':
    #logging.basicConfig(level=logging.INFO)
    main()
