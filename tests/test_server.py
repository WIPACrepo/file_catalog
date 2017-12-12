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

import jwt

from file_catalog.urlargparse import encode as jquery_encode
from file_catalog.server import Server
from file_catalog.config import Config
from file_catalog import auth

class TestServerAPI(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(dir=os.getcwd())
        self.addCleanup(partial(shutil.rmtree, self.tmpdir))
        self.port = random.randint(10000,50000)
        self.mongo_port = random.randint(10000,50000)
        dbpath = os.path.join(self.tmpdir,'db')
        os.mkdir(dbpath)
        dblog = os.path.join(dbpath,'logfile')

        m = subprocess.Popen(['mongod', '--port', str(self.mongo_port),
                              '--dbpath', dbpath, '--smallfiles',
                              '--quiet', '--nounixsocket',
                              '--logpath', dblog])
        self.addCleanup(partial(time.sleep, 0.1))
        self.addCleanup(m.terminate)

        self.config = Config('resources/server.cfg')
        self.server = Server(self.config, port=self.port,
                             db_host='localhost:%d'%self.mongo_port,
                             debug=True)
        t = Thread(target=self.server.run)
        t.start()
        self.addCleanup(IOLoop.current().stop)
        time.sleep(0.3)

    def curl(self, url, method='GET', args=None, prefix='/api', headers=None):
        cmd = ['curl', '-X', method, '-s', '-i']
        if args:
            if 'query' in args:
                args['query'] = json_encode(args['query'])
            if method == 'GET':
                cmd.extend(['-G', '--data-binary', jquery_encode(args)])
            else:
                cmd.extend(['--data-binary', json_encode(args)])
        if headers:
            for h in headers:
                cmd.extend(['-H',h+': '+headers[h]])
        output = os.path.join(self.tmpdir, 'curl_out')
        cmd.extend(['-o', output, ('http://localhost:%d'%self.port)+prefix+url])
        subprocess.check_call(cmd)
        status = 0
        headers = {}
        data = {}
        with open(output) as f:
            lines = f.read().split('\n')
            for i,line in enumerate(lines):
                if line.startswith('HTTP/'):
                    status = int(line.split()[1])
                elif not line.strip():
                    break
                else:
                    parts = line.split(':',1)
                    if len(parts) == 2:
                        headers[parts[0].lower()] = parts[1].strip()
                    else:
                        print('skipping header',line)
            try:
                print('\n'.join(lines[i:]))
                data = json_decode('\n'.join(lines[i:]))
            except:
                pass
        return {
            'status': status,
            'headers': headers,
            'data': data,
        }

    def test_01_HATEOAS(self):
        ret = self.curl('', 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('files', ret['data'])

        for m in ('POST','PUT','DELETE','PATCH'):
            ret = self.curl('', m)
            self.assertEquals(ret['status'], 405)

    def test_05_token(self):
        appkey = 'secret2'
        self.config['auth'] = {
            'secret': 'secret',
            'expiration': 82400,
        }
        
        payload = {
            'iss': auth.ISSUER,
            'sub': 'test',
            'type': 'appkey'
        }
        appkey = jwt.encode(payload, 'secret', algorithm='HS512')

        ret = self.curl('/token', 'GET', headers={'Authorization':'JWT '+appkey})
        print(ret)
        self.assertEquals(ret['status'], 200)

        ret = self.curl('/token', 'GET', headers={'Authorization':'JWT blah'})
        self.assertEquals(ret['status'], 403)

        ret = self.curl('/token', 'GET')
        self.assertEquals(ret['status'], 403)

    def test_10_files(self):
        metadata = {
            'logical_name': 'blah',
            'checksum': {'sha512':hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [{u'site':u'test',u'path':u'blah.dat'}]
        }
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']

        ret = self.curl('/files', 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('files', ret['data'])
        self.assertEqual(len(ret['data']['files']), 1)
        self.assertIn(url, ret['data']['files'])

        for m in ('PUT','DELETE','PATCH'):
            ret = self.curl('/files', m)
            self.assertEquals(ret['status'], 405)

    def test_15_files_auth(self):
        appkey = 'secret2'
        self.config['auth'] = {
            'secret': 'secret',
            'expiration': 82400,
        }
        
        payload = {
            'iss': auth.ISSUER,
            'sub': 'test',
            'type': 'appkey'
        }
        appkey = jwt.encode(payload, 'secret', algorithm='HS512')
        
        metadata = {
            'logical_name': 'blah',
            'checksum': {'sha512':hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [{u'site':u'test',u'path':u'blah.dat'}]
        }
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 403)

        ret = self.curl('/files', 'POST', metadata, headers={'Authorization':'JWT '+appkey})
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']

    def test_20_file(self):
        metadata = {
            u'logical_name': u'blah',
            u'checksum': {u'sha512':hashlib.sha512('foo bar').hexdigest()},
            u'file_size': 1,
            u'locations': [{u'site':u'test',u'path':u'blah.dat'}]
        }
        ret = self.curl('/files', 'POST', metadata)
        print(ret)

        url = ret['data']['file']

        ret = self.curl(url, 'GET', prefix='')
        print(ret)

        self.assertEquals(ret['status'], 200)
        ret['data'].pop('_links')
        ret['data'].pop('meta_modify_date')
        ret['data'].pop('uuid')
        self.assertDictEqual(metadata, ret['data'])
        self.assertIn('etag', ret['headers'])

        metadata['test'] = 100

        metadata_cpy = metadata.copy()
        metadata_cpy['uuid'] = 'something else'
        ret2 = self.curl(url, 'PUT', prefix='', args=metadata_cpy,
                        headers={'If-None-Match':ret['headers']['etag']})
        print(ret2)
        self.assertEquals(ret2['status'], 400)

        ret = self.curl(url, 'PUT', prefix='', args=metadata,
                        headers={'If-None-Match':ret['headers']['etag']})
        print(ret)
        self.assertEquals(ret['status'], 200)

        ret['data'].pop('_links')
        ret['data'].pop('meta_modify_date')
        ret['data'].pop('uuid')
        self.assertDictEqual(metadata, ret['data'])
        ret = self.curl(url, 'GET', prefix='')
        ret['data'].pop('_links')
        ret['data'].pop('meta_modify_date')
        ret['data'].pop('uuid')
        self.assertDictEqual(metadata, ret['data'])

        ret = self.curl(url, 'PATCH', prefix='', args={'test2':200},
                        headers={'If-None-Match':ret['headers']['etag']})
        metadata['test2'] = 200
        print(ret)
        self.assertEquals(ret['status'], 200)
        ret['data'].pop('_links')
        ret['data'].pop('meta_modify_date')
        ret['data'].pop('uuid')
        self.assertDictEqual(metadata, ret['data'])
        ret = self.curl(url, 'GET', prefix='')
        ret['data'].pop('_links')
        ret['data'].pop('meta_modify_date')
        ret['data'].pop('uuid')
        self.assertDictEqual(metadata, ret['data'])
        
        ret = self.curl(url, 'DELETE', prefix='')
        print(ret)
        self.assertEquals(ret['status'], 204)

        ret = self.curl(url, 'DELETE', prefix='')
        print(ret)
        self.assertEquals(ret['status'], 404)
        
        ret = self.curl(url, 'POST', prefix='')
        self.assertEquals(ret['status'], 405)

    def test_30_archive(self):
        metadata = {
            u'logical_name': u'blah',
            u'checksum': {u'sha512':hashlib.sha512('foo bar').hexdigest()},
            u'file_size': 1,
            u'locations': [{u'site':u'test',u'path':u'blah.dat'}]
        }
        metadata2 = {
            u'logical_name': u'blah2',
            u'checksum': {u'sha512':hashlib.sha512('foo bar baz').hexdigest()},
            u'file_size': 2,
            u'locations': [{u'site':u'test',u'path':u'blah.dat',u'archive':True}]
        }
        ret = self.curl('/files', 'POST', metadata)
        url = ret['data']['file']
        ret = self.curl('/files', 'POST', metadata2)
        url2 = ret['data']['file']

        ret = self.curl('/files', 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('files', ret['data'])
        self.assertEqual(len(ret['data']['files']), 1)
        self.assertIn(url, ret['data']['files'])
        self.assertNotIn(url2, ret['data']['files'])

        ret = self.curl('/files', 'GET', args={'query':{'locations.archive':True}})
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('files', ret['data'])
        self.assertEqual(len(ret['data']['files']), 1)
        self.assertNotIn(url, ret['data']['files'])
        self.assertIn(url2, ret['data']['files'])

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestStringMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
