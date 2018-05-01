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

from file_catalog.config import Config
from file_catalog import auth

from .test_server import TestServerAPI

class TestFilesAPI(TestServerAPI):
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
        uid = url.split('/')[-1]

        ret = self.curl('/files', 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('files', ret['data'])
        self.assertEqual(len(ret['data']['files']), 1)
        self.assertTrue(any(uid == f['uuid'] for f in ret['data']['files']))

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
        uid = url.split('/')[-1]
        ret = self.curl('/files', 'POST', metadata2)
        url2 = ret['data']['file']
        uid2 = url2.split('/')[-1]

        ret = self.curl('/files', 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('files', ret['data'])
        self.assertEqual(len(ret['data']['files']), 1)
        self.assertTrue(any(uid == f['uuid'] for f in ret['data']['files']))
        self.assertFalse(any(uid2 == f['uuid'] for f in ret['data']['files']))

        ret = self.curl('/files', 'GET', args={'query':{'locations.archive':True}})
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('files', ret['data'])
        self.assertEqual(len(ret['data']['files']), 1)
        self.assertFalse(any(uid == f['uuid'] for f in ret['data']['files']))
        self.assertTrue(any(uid2 == f['uuid'] for f in ret['data']['files']))

    def test_40_simple_query(self):
        metadata = {
            u'logical_name': u'blah',
            u'checksum': {u'sha512':hashlib.sha512('foo bar').hexdigest()},
            u'file_size': 1,
            u'locations': [{u'site':u'test',u'path':u'blah.dat'}],
            u'processing_level':u'level2',
            u'run_number':12345,
            u'first_event':345,
            u'last_event':456,
            u'iceprod':{
                u'dataset':23453,
            },
            u'offline':{
                u'season':2017,
            },
        }
        metadata2 = {
            u'logical_name': u'blah2',
            u'checksum': {u'sha512':hashlib.sha512('foo bar baz').hexdigest()},
            u'file_size': 2,
            u'locations': [{u'site':u'test',u'path':u'blah.dat'}],
            u'processing_level':u'level2',
            r'run_number':12356,
            u'first_event':578,
            u'last_event':698,
            u'iceprod':{
                u'dataset':23454,
            },
            u'offline':{
                u'season':2017,
            },
        }
        ret = self.curl('/files', 'POST', metadata)
        url = ret['data']['file']
        uid = url.split('/')[-1]
        ret = self.curl('/files', 'POST', metadata2)
        url2 = ret['data']['file']
        uid2 = url2.split('/')[-1]

        ret = self.curl('/files', 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('files', ret['data'])
        self.assertEqual(len(ret['data']['files']), 2)
        self.assertTrue(any(uid == f['uuid'] for f in ret['data']['files']))
        self.assertTrue(any(uid2 == f['uuid'] for f in ret['data']['files']))

        ret = self.curl('/files', 'GET', args={'processing_level':'level2'})
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('files', ret['data'])
        self.assertEqual(len(ret['data']['files']), 2)
        self.assertTrue(any(uid == f['uuid'] for f in ret['data']['files']))
        self.assertTrue(any(uid2 == f['uuid'] for f in ret['data']['files']))

        ret = self.curl('/files', 'GET', args={'run_number':12345})
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('files', ret['data'])
        self.assertEqual(len(ret['data']['files']), 1)
        self.assertTrue(any(uid == f['uuid'] for f in ret['data']['files']))
        self.assertFalse(any(uid2 == f['uuid'] for f in ret['data']['files']))

        ret = self.curl('/files', 'GET', args={'dataset':23454})
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('files', ret['data'])
        self.assertEqual(len(ret['data']['files']), 1)
        self.assertFalse(any(uid == f['uuid'] for f in ret['data']['files']))
        self.assertTrue(any(uid2 == f['uuid'] for f in ret['data']['files']))

        ret = self.curl('/files', 'GET', args={'event_id':400})
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('files', ret['data'])
        self.assertEqual(len(ret['data']['files']), 1)
        self.assertTrue(any(uid == f['uuid'] for f in ret['data']['files']))
        self.assertFalse(any(uid2 == f['uuid'] for f in ret['data']['files']))

        ret = self.curl('/files', 'GET', args={'season':2017})
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('files', ret['data'])
        self.assertEqual(len(ret['data']['files']), 2)
        self.assertTrue(any(uid == f['uuid'] for f in ret['data']['files']))
        self.assertTrue(any(uid2 == f['uuid'] for f in ret['data']['files']))

        ret = self.curl('/files', 'GET', args={'event_id':400, 'keys':'|'.join(['checksum','file_size','uuid'])})
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('files', ret['data'])
        self.assertEqual(len(ret['data']['files']), 1)
        self.assertTrue(any(uid == f['uuid'] for f in ret['data']['files']))
        self.assertFalse(any(uid2 == f['uuid'] for f in ret['data']['files']))
        self.assertIn('checksum', ret['data']['files'][0])
        self.assertIn('file_size', ret['data']['files'][0])

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestStringMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
