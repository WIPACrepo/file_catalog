from __future__ import absolute_import, division, print_function

import os
import time
import tempfile
import shutil
import random
import subprocess
from functools import partial
import unittest

from tornado.escape import json_encode,json_decode

from file_catalog.urlargparse import encode as jquery_encode

class TestServerAPI(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
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

        s = subprocess.Popen(['python', '-m', 'file_catalog',
                              '-p', str(self.port),
                              '--db_host', 'localhost:%d'%self.mongo_port,
                              '--debug'])
        self.addCleanup(s.terminate)
        time.sleep(0.3)

    def curl(self, url, method='GET', args=None, prefix='/api', headers=None):
        cmd = ['curl', '-X', method, '-s', '-i']
        if args:
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

    def test_10_files(self):
        metadata = {'file_name': 'blah', 'checksum': 'checked'}
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

    def test_20_file(self):
        metadata = {'file_name': 'blah', 'checksum': 'checked'}
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        url = ret['data']['file']

        ret = self.curl(url, 'GET', prefix='')
        print(ret)
        self.assertEquals(ret['status'], 200)
        ret['data'].pop('_id')
        ret['data'].pop('_links')
        self.assertDictEqual(metadata, ret['data'])
        self.assertIn('etag', ret['headers'])

        metadata['test'] = 100
        ret = self.curl(url, 'PUT', prefix='', args=metadata,
                        headers={'If-None-Match':ret['headers']['etag']})
        print(ret)
        self.assertEquals(ret['status'], 200)
        ret['data'].pop('_id')
        ret['data'].pop('_links')
        self.assertDictEqual(metadata, ret['data'])
        ret = self.curl(url, 'GET', prefix='')
        ret['data'].pop('_id')
        ret['data'].pop('_links')
        self.assertDictEqual(metadata, ret['data'])

        ret = self.curl(url, 'PATCH', prefix='', args={'test2':200},
                        headers={'If-None-Match':ret['headers']['etag']})
        metadata['test2'] = 200
        print(ret)
        self.assertEquals(ret['status'], 200)
        ret['data'].pop('_id')
        ret['data'].pop('_links')
        self.assertDictEqual(metadata, ret['data'])
        ret = self.curl(url, 'GET', prefix='')
        ret['data'].pop('_id')
        ret['data'].pop('_links')
        self.assertDictEqual(metadata, ret['data'])
        
        ret = self.curl(url, 'DELETE', prefix='')
        print(ret)
        self.assertEquals(ret['status'], 204)

        ret = self.curl(url, 'DELETE', prefix='')
        print(ret)
        self.assertEquals(ret['status'], 404)
        
        ret = self.curl(url, 'POST', prefix='')
        self.assertEquals(ret['status'], 405)
        

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestStringMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
