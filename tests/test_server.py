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
        self.addCleanup(m.terminate)

        s = subprocess.Popen(['python', '-m', 'file_catalog',
                              '-p', str(self.port),
                              '--db_host', 'localhost:%d'%self.mongo_port,
                              '--debug'])
        self.addCleanup(s.terminate)
        time.sleep(0.5)

    def curl(self, url, method='GET', args=None):
        cmd = ['curl', '-X', method, '-s', '-w', '%{http_code}']
        if args:
            if method == 'GET':
                cmd.extend(['-G', '--data-binary', jquery_encode(args)])
            else:
                cmd.extend(['--data-binary', json_encode(args)])
        output = os.path.join(self.tmpdir, 'curl_out')
        cmd.extend(['-o', output, ('http://localhost:%d/api'%self.port)+url])
        out = subprocess.check_output(cmd)
        out = int(out)
        try:
            return out,json_decode(open(output).read())
        except:
            return out,None

    def test_01_HATEOAS(self):
        code,ret = self.curl('', 'GET')
        print(code,ret)
        self.assertEquals(code, 200)
        self.assertIn('_links', ret)
        self.assertIn('self', ret['_links'])
        self.assertIn('files', ret)

        for m in ('POST','PUT','DELETE','PATCH'):
            code,ret = self.curl('',m)
            self.assertEquals(code, 405)

    def test_10_create_file(self):
        metadata = {'file_name': 'blah', 'checksum': 'cbecked'}
        code,ret = self.curl('/files', 'POST', metadata)
        print(code,ret)
        self.assertEquals(code, 201)
        self.assertIn('_links', ret)
        self.assertIn('self', ret['_links'])
        self.assertIn('', ret)

        for m in ('POST','PUT','DELETE','PATCH'):
            code,ret = self.curl('',m)
            self.assertEquals(code, 405)


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestStringMethods)
    unittest.TextTestRunner(verbosity=3).run(suite)
