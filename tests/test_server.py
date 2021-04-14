"""Test REST Server."""

# fmt:off
# pylint: skip-file

from __future__ import absolute_import, division, print_function

import hashlib
import os
import random
import shutil
import subprocess
import tempfile
import time
import unittest
from functools import partial
from threading import Thread

import requests
from file_catalog.urlargparse import encode as jquery_encode
from pymongo import MongoClient
from tornado.escape import json_decode, json_encode
from tornado.ioloop import IOLoop


class TestServerAPI(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(dir=os.getcwd())
        self.addCleanup(partial(shutil.rmtree, self.tmpdir))
        self.address = 'http://localhost:8888'
        self.mongo_port = random.randint(10000,50000)
        dbpath = os.path.join(self.tmpdir,'db')
        os.mkdir(dbpath)
        dblog = os.path.join(dbpath,'logfile')

        if 'TEST_DATABASE_HOST' not in os.environ:
            m = subprocess.Popen(['mongod', '--port', str(self.mongo_port),
                                  '--dbpath', dbpath, '--smallfiles',
                                  '--quiet', '--nounixsocket',
                                  '--logpath', dblog])
            self.addCleanup(partial(time.sleep, 0.3))
            self.addCleanup(m.terminate)

    def clean_db(self, host, port):
        db = MongoClient(host=host, port=port).file_catalog
        colls = db.list_collection_names()
        for c in colls:
            if 'system' not in c:
                db.drop_collection(c)

    def start_server(self, config_override={}) -> None:
        env = dict(os.environ)
        env.update(config_override)
        env['MONGODB_PORT'] = str(self.mongo_port)
        env['DEBUG'] = '1'

        if 'TEST_DATABASE_HOST' in os.environ:
            env['MONGODB_HOST'] = os.environ['TEST_DATABASE_HOST']
        if 'TEST_DATABASE_PORT' in os.environ:
            env['MONGODB_PORT'] = os.environ['TEST_DATABASE_PORT']
        self.clean_db(env['MONGODB_HOST'], int(env['MONGODB_PORT']))
        s = subprocess.Popen(['python', '-m', 'file_catalog'], env=env)
        self.addCleanup(s.terminate)
        time.sleep(2)

    def get_token(self) -> str:
        if 'TOKEN_URL' in os.environ:
            r = requests.get(os.environ['TOKEN_URL']+'/token?scope=file_catalog')
            r.raise_for_status()
            return r.json()['access']
        else:
            raise Exception('testing token service not defined')

    def test_01_HATEOAS(self):
        self.start_server()
        r = requests.get(self.address+'/api')
        r.raise_for_status()
        data = r.json()
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)

        for m in ('post','put','delete','patch'):
            r = getattr(requests, m)(self.address+'/api')
            with self.assertRaises(Exception):
                r.raise_for_status()

    def test_05_login(self):
        self.start_server()
        r = requests.get(self.address+'/login')
        r.raise_for_status()


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestStringMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
