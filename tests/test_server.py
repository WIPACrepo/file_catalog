from __future__ import absolute_import, division, print_function

import os
import time
import tempfile
import shutil
import random
import subprocess
from functools import partial
from threading import Thread
import unittest
import hashlib

from tornado.escape import json_encode,json_decode
from tornado.ioloop import IOLoop
import requests
import jwt
from pymongo import MongoClient
from rest_tools.server import Auth

from file_catalog.urlargparse import encode as jquery_encode
from file_catalog.server import Server
from file_catalog.config import SafeConfigParser, Config

class TestServerAPI(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(dir=os.getcwd())
        self.addCleanup(partial(shutil.rmtree, self.tmpdir))
        self.address = 'http://localhost:8888'
        self.mongo_port = random.randint(10000,50000)
        dbpath = os.path.join(self.tmpdir,'db')
        os.mkdir(dbpath)
        dblog = os.path.join(dbpath,'logfile')

        if 'TEST_DATABASE_URL' not in os.environ:
            m = subprocess.Popen(['mongod', '--port', str(self.mongo_port),
                                  '--dbpath', dbpath, '--smallfiles',
                                  '--quiet', '--nounixsocket',
                                  '--logpath', dblog])
            self.addCleanup(partial(time.sleep, 0.3))
            self.addCleanup(m.terminate)
        
        self.config = os.path.join(self.tmpdir,'server.cfg')
        shutil.copy('resources/server.cfg', self.config)

    def edit_config(self, new_cfg):
        old_cfg = Config(self.config)
        old_cfg.update(new_cfg)
        
        tmp = SafeConfigParser()
        for k in old_cfg:
            tmp.add_section(k)
            for k2,v2 in old_cfg[k].items():
                tmp.set(k,k2,str(v2))
        with open(self.config,'w') as f:
            tmp.write(f)

    def clean_db(self, addr):
        db = MongoClient(addr).file_catalog
        colls = db.list_collection_names()
        for c in colls:
            db.drop_collection(c)

    def start_server(self):
        cmd = ['python','-m','file_catalog','--config',
               self.config,'-p','8888','--debug',
               '--db_host','localhost:%d'%self.mongo_port]
        if 'TEST_DATABASE_URL' in os.environ:
            cmd[-1] = os.environ['TEST_DATABASE_URL']
            self.clean_db(os.environ['TEST_DATABASE_URL'])
        s = subprocess.Popen(cmd)
        self.addCleanup(s.terminate)
        time.sleep(2)

    def get_token(self):
        if 'TOKEN_SERVICE' in os.environ:
            r = requests.get(os.environ['TOKEN_SERVICE']+'/token?scope=file_catalg')
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
