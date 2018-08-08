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
        self.start_server()
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
        self.edit_config({
            'auth':{
                'secret': 'secret',
                'expiration': 82400,
            }
        })
        self.start_server()

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
        self.start_server()
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
        self.start_server()
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
        self.start_server()
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
            u'locations': [{u'site':u'test',u'path':u'blah2.dat'}],
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

    def test_50_post_files_unique_logical_name(self):
        """Test that logical_name is unique when creating a new file."""
        self.start_server()

        # define the file to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }

        # create the file the first time; should be OK
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

        # check that the file was created properly
        ret = self.curl('/files', 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('files', ret['data'])
        self.assertEqual(len(ret['data']['files']), 1)
        self.assertTrue(any(uid == f['uuid'] for f in ret['data']['files']))

        # create the file the second time; should NOT be OK
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        # Conflict (if the file already exists); includes link to existing file
        self.assertEquals(ret['status'], 409)
        self.assertIn('message', ret['data'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

        # check that the second file was not created
        ret = self.curl('/files', 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('files', ret['data'])
        self.assertEqual(len(ret['data']['files']), 1)
        self.assertTrue(any(uid == f['uuid'] for f in ret['data']['files']))

    def test_51_put_files_uuid_unique_logical_name(self):
        """Test that logical_name is unique when replacing a file."""
        self.start_server()

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah2.dat'}]
        }

        # create the first file; should be OK
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

        # get the record of the file for its etag header
        ret = self.curl('/files/' + uid, 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('etag', ret['headers'])
        etag = ret['headers']['etag']

        # create the second file; should be OK
        ret = self.curl('/files', 'POST', metadata2)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])

        # try to replace the first file with a copy of the second; should NOT be OK
        ret = self.curl('/files/' + uid, 'PUT', metadata2, '/api', {'If-None-Match': etag})
        print(ret)
        self.assertEquals(ret['status'], 409)
        self.assertIn('message', ret['data'])
        self.assertIn('file', ret['data'])

    def test_52_put_files_uuid_replace_logical_name(self):
        """Test that a file can replace with the same logical_name."""
        self.start_server()

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar2').hexdigest()},
            'file_size': 2,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }

        # create the first file; should be OK
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

        # get the record of the file for its etag header
        ret = self.curl('/files/' + uid, 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('etag', ret['headers'])
        etag = ret['headers']['etag']

        # try to replace the first file with the second; should be OK
        ret = self.curl('/files/' + uid, 'PUT', metadata2, '/api', {'If-None-Match': etag})
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('logical_name', ret['data'])

    def test_53_patch_files_uuid_unique_logical_name(self):
        """Test that logical_name is unique when updating a file."""
        self.start_server()

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah2.dat'}]
        }

        # this is a PATCH to metadata; steps on metadata2's logical_name
        patch1 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar2').hexdigest()},
            'file_size': 2,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah2.dat'}]
        }

        # create the first file; should be OK
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

        # get the record of the file for its etag header
        ret = self.curl('/files/' + uid, 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('etag', ret['headers'])
        etag = ret['headers']['etag']

        # create the second file; should be OK
        ret = self.curl('/files', 'POST', metadata2)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])

        # try to update the first file with a patch; should NOT be OK
        ret = self.curl('/files/' + uid, 'PATCH', patch1, '/api', {'If-None-Match': etag})
        print(ret)
        self.assertEquals(ret['status'], 409)
        self.assertIn('message', ret['data'])
        self.assertIn('file', ret['data'])

    def test_54_patch_files_uuid_replace_logical_name(self):
        """Test that a file can be updated with the same logical_name."""
        self.start_server()

        # define the file to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }

        # this is a PATCH to metadata; matches the old logical_name
        patch1 = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar2').hexdigest()},
            'file_size': 2,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }

        # create the file; should be OK
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

        # get the record of the file for its etag header
        ret = self.curl('/files/' + uid, 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('etag', ret['headers'])
        etag = ret['headers']['etag']

        # try to update the file with a patch; should be OK
        ret = self.curl('/files/' + uid, 'PATCH', patch1, '/api', {'If-None-Match': etag})
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('logical_name', ret['data'])

    def test_55_post_files_unique_locations(self):
        """Test that locations is unique when creating a new file."""
        self.start_server()

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar2').hexdigest()},
            'file_size': 2,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }

        # create the first file; should be OK
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

        # check that the file was created properly
        ret = self.curl('/files', 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('files', ret['data'])
        self.assertEqual(len(ret['data']['files']), 1)
        self.assertTrue(any(uid == f['uuid'] for f in ret['data']['files']))

        # create the second file; should NOT be OK
        ret = self.curl('/files', 'POST', metadata2)
        print(ret)
        # Conflict (if the file already exists); includes link to existing file
        self.assertEquals(ret['status'], 409)
        self.assertIn('message', ret['data'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

    def test_56_put_files_uuid_unique_locations(self):
        """Test that locations is unique when replacing a file."""
        self.start_server()

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar2').hexdigest()},
            'file_size': 2,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah2.dat'}]
        }
        replace1 = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar2').hexdigest()},
            'file_size': 2,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah2.dat'}]
        }

        # create the first file; should be OK
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

        # get the record of the file for its etag header
        ret = self.curl('/files/' + uid, 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('etag', ret['headers'])
        etag = ret['headers']['etag']

        # create the second file; should be OK
        ret = self.curl('/files', 'POST', metadata2)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])

        # try to replace the first file with a location collision with the second; should NOT be OK
        ret = self.curl('/files/' + uid, 'PUT', replace1, '/api', {'If-None-Match': etag})
        print(ret)
        self.assertEquals(ret['status'], 409)
        self.assertIn('message', ret['data'])
        self.assertIn('file', ret['data'])

    def test_57_put_files_uuid_replace_locations(self):
        """Test that a file can replace with the same location."""
        self.start_server()

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar2').hexdigest()},
            'file_size': 2,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }

        # create the first file; should be OK
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

        # get the record of the file for its etag header
        ret = self.curl('/files/' + uid, 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('etag', ret['headers'])
        etag = ret['headers']['etag']

        # try to replace the first file with the second; should be OK
        ret = self.curl('/files/' + uid, 'PUT', metadata2, '/api', {'If-None-Match': etag})
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('logical_name', ret['data'])

    def test_58_patch_files_uuid_unique_locations(self):
        """Test that locations is unique when updating a file."""
        self.start_server()

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah2.dat'}]
        }

        # this is a PATCH to metadata; steps on metadata2's location
        patch1 = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar2').hexdigest()},
            'file_size': 2,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah2.dat'}]
        }

        # create the first file; should be OK
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

        # get the record of the file for its etag header
        ret = self.curl('/files/' + uid, 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('etag', ret['headers'])
        etag = ret['headers']['etag']

        # create the second file; should be OK
        ret = self.curl('/files', 'POST', metadata2)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])

        # try to update the first file with a patch; should NOT be OK
        ret = self.curl('/files/' + uid, 'PATCH', patch1, '/api', {'If-None-Match': etag})
        print(ret)
        self.assertEquals(ret['status'], 409)
        self.assertIn('message', ret['data'])
        self.assertIn('file', ret['data'])

    def test_59_patch_files_uuid_replace_locations(self):
        """Test that a file can be updated with the same location."""
        self.start_server()

        # define the file to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }

        # this is a PATCH to metadata; matches the old location
        patch1 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar2').hexdigest()},
            'file_size': 2,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }

        # create the file; should be OK
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

        # get the record of the file for its etag header
        ret = self.curl('/files/' + uid, 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('etag', ret['headers'])
        etag = ret['headers']['etag']

        # try to update the file with a patch; should be OK
        ret = self.curl('/files/' + uid, 'PATCH', patch1, '/api', {'If-None-Match': etag})
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('logical_name', ret['data'])
        self.assertIn('locations', ret['data'])

    def test_60_post_files_locations_1xN(self):
        """Test locations uniqueness under 1xN multiplicity."""
        self.start_server()

        # define the locations to be tested
        loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
        locs3a = [loc1a, loc1b, loc1c]
        locs3b = [loc1b, loc1c, loc1d]
        locs3c = [loc1a, loc1b, loc1d]

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [loc1a]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar2').hexdigest()},
            'file_size': 2,
            u'locations': locs3a
        }

        # create the first file; should be OK
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

        # check that the file was created properly
        ret = self.curl('/files', 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('files', ret['data'])
        self.assertEqual(len(ret['data']['files']), 1)
        self.assertTrue(any(uid == f['uuid'] for f in ret['data']['files']))

        # create the second file; should NOT be OK
        ret = self.curl('/files', 'POST', metadata2)
        print(ret)
        # Conflict (if the file already exists); includes link to existing file
        self.assertEquals(ret['status'], 409)
        self.assertIn('message', ret['data'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

    def test_61_post_files_locations_Nx1(self):
        """Test locations uniqueness under Nx1 multiplicity."""
        self.start_server()

        # define the locations to be tested
        loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
        locs3a = [loc1a, loc1b, loc1c]
        locs3b = [loc1b, loc1c, loc1d]
        locs3c = [loc1a, loc1b, loc1d]

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': locs3b
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar2').hexdigest()},
            'file_size': 2,
            u'locations': [loc1c]
        }

        # create the first file; should be OK
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

        # check that the file was created properly
        ret = self.curl('/files', 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('files', ret['data'])
        self.assertEqual(len(ret['data']['files']), 1)
        self.assertTrue(any(uid == f['uuid'] for f in ret['data']['files']))

        # check that the file was created properly, part deux
        ret = self.curl('/files/' + uid, 'GET')
        self.assertEquals(ret['status'], 200)

        # create the second file; should NOT be OK
        ret = self.curl('/files', 'POST', metadata2)
        print(ret)
        # Conflict (if the file already exists); includes link to existing file
        self.assertEquals(ret['status'], 409)
        self.assertIn('message', ret['data'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

    def test_62_post_files_locations_NxN(self):
        """Test locations uniqueness under NxN multiplicity."""
        self.start_server()

        # define the locations to be tested
        loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
        locs3a = [loc1a, loc1b, loc1c]
        locs3b = [loc1b, loc1c, loc1d]
        locs3c = [loc1a, loc1b, loc1d]

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': locs3a
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar2').hexdigest()},
            'file_size': 2,
            u'locations': locs3c
        }

        # create the first file; should be OK
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

        # check that the file was created properly
        ret = self.curl('/files', 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('files', ret['data'])
        self.assertEqual(len(ret['data']['files']), 1)
        self.assertTrue(any(uid == f['uuid'] for f in ret['data']['files']))

        # check that the file was created properly, part deux
        ret = self.curl('/files/' + uid, 'GET')
        self.assertEquals(ret['status'], 200)

        # create the second file; should NOT be OK
        ret = self.curl('/files', 'POST', metadata2)
        print(ret)
        # Conflict (if the file already exists); includes link to existing file
        self.assertEquals(ret['status'], 409)
        self.assertIn('message', ret['data'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

    def test_63_put_files_uuid_locations_1xN(self):
        """Test locations uniqueness under 1xN multiplicity."""
        self.start_server()

        # define the locations to be tested
        loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
        locs3a = [loc1a, loc1b, loc1c]
        locs3b = [loc1b, loc1c, loc1d]
        locs3c = [loc1a, loc1b, loc1d]

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [loc1a]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar2').hexdigest()},
            'file_size': 2,
            u'locations': [loc1b]
        }
        replace1 = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar2').hexdigest()},
            'file_size': 2,
            u'locations': locs3c
        }

        # create the first file; should be OK
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

        # get the record of the file for its etag header
        ret = self.curl('/files/' + uid, 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('etag', ret['headers'])
        etag = ret['headers']['etag']

        # create the second file; should be OK
        ret = self.curl('/files', 'POST', metadata2)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])

        # try to replace the first file with a location collision with the second; should NOT be OK
        ret = self.curl('/files/' + uid, 'PUT', replace1, '/api', {'If-None-Match': etag})
        print(ret)
        self.assertEquals(ret['status'], 409)
        self.assertIn('message', ret['data'])
        self.assertIn('file', ret['data'])

    def test_64_put_files_uuid_locations_Nx1(self):
        """Test locations uniqueness under Nx1 multiplicity."""
        self.start_server()

        # define the locations to be tested
        loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
        locs3a = [loc1a, loc1b, loc1c]
        locs3b = [loc1b, loc1c, loc1d]
        locs3c = [loc1a, loc1b, loc1d]

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [loc1d]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar2').hexdigest()},
            'file_size': 2,
            u'locations': locs3a
        }
        replace1 = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar2').hexdigest()},
            'file_size': 2,
            u'locations': [loc1a]
        }

        # create the first file; should be OK
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

        # get the record of the file for its etag header
        ret = self.curl('/files/' + uid, 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('etag', ret['headers'])
        etag = ret['headers']['etag']

        # create the second file; should be OK
        ret = self.curl('/files', 'POST', metadata2)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])

        # try to replace the first file with a location collision with the second; should NOT be OK
        ret = self.curl('/files/' + uid, 'PUT', replace1, '/api', {'If-None-Match': etag})
        print(ret)
        self.assertEquals(ret['status'], 409)
        self.assertIn('message', ret['data'])
        self.assertIn('file', ret['data'])

    def test_65_put_files_uuid_locations_NxN(self):
        """Test locations uniqueness under NxN multiplicity."""
        self.start_server()

        # define the locations to be tested
        loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
        locs3a = [loc1a, loc1b, loc1c]
        locs3b = [loc1b, loc1c, loc1d]
        locs3c = [loc1a, loc1b, loc1d]

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [loc1d]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar2').hexdigest()},
            'file_size': 2,
            u'locations': locs3a
        }
        replace1 = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar2').hexdigest()},
            'file_size': 2,
            u'locations': locs3b
        }

        # create the first file; should be OK
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

        # get the record of the file for its etag header
        ret = self.curl('/files/' + uid, 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('etag', ret['headers'])
        etag = ret['headers']['etag']

        # create the second file; should be OK
        ret = self.curl('/files', 'POST', metadata2)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])

        # try to replace the first file with a location collision with the second; should NOT be OK
        ret = self.curl('/files/' + uid, 'PUT', replace1, '/api', {'If-None-Match': etag})
        print(ret)
        self.assertEquals(ret['status'], 409)
        self.assertIn('message', ret['data'])
        self.assertIn('file', ret['data'])

    def test_66_patch_files_uuid_locations_1xN(self):
        """Test locations uniqueness under 1xN multiplicity."""
        self.start_server()

        # define the locations to be tested
        loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
        locs3a = [loc1a, loc1b, loc1c]
        locs3b = [loc1b, loc1c, loc1d]
        locs3c = [loc1a, loc1b, loc1d]

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [loc1a]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [loc1b]
        }

        # this is a PATCH to metadata; steps on metadata2's location
        patch1 = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar2').hexdigest()},
            'file_size': 2,
            u'locations': locs3c
        }

        # create the first file; should be OK
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

        # get the record of the file for its etag header
        ret = self.curl('/files/' + uid, 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('etag', ret['headers'])
        etag = ret['headers']['etag']

        # create the second file; should be OK
        ret = self.curl('/files', 'POST', metadata2)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])

        # try to update the first file with a patch; should NOT be OK
        ret = self.curl('/files/' + uid, 'PATCH', patch1, '/api', {'If-None-Match': etag})
        print(ret)
        self.assertEquals(ret['status'], 409)
        self.assertIn('message', ret['data'])
        self.assertIn('file', ret['data'])

    def test_67_patch_files_uuid_locations_Nx1(self):
        """Test locations uniqueness under Nx1 multiplicity."""
        self.start_server()

        # define the locations to be tested
        loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
        locs3a = [loc1a, loc1b, loc1c]
        locs3b = [loc1b, loc1c, loc1d]
        locs3c = [loc1a, loc1b, loc1d]

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [loc1a]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': locs3b
        }

        # this is a PATCH to metadata; steps on metadata2's location
        patch1 = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar2').hexdigest()},
            'file_size': 2,
            u'locations': [loc1c]
        }

        # create the first file; should be OK
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

        # get the record of the file for its etag header
        ret = self.curl('/files/' + uid, 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('etag', ret['headers'])
        etag = ret['headers']['etag']

        # create the second file; should be OK
        ret = self.curl('/files', 'POST', metadata2)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])

        # try to update the first file with a patch; should NOT be OK
        ret = self.curl('/files/' + uid, 'PATCH', patch1, '/api', {'If-None-Match': etag})
        print(ret)
        self.assertEquals(ret['status'], 409)
        self.assertIn('message', ret['data'])
        self.assertIn('file', ret['data'])

    def test_68_patch_files_uuid_locations_NxN(self):
        """Test locations uniqueness under NxN multiplicity."""
        self.start_server()

        # define the locations to be tested
        loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
        locs3a = [loc1a, loc1b, loc1c]
        locs3b = [loc1b, loc1c, loc1d]
        locs3c = [loc1a, loc1b, loc1d]

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': [loc1a]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar').hexdigest()},
            'file_size': 1,
            u'locations': locs3b
        }

        # this is a PATCH to metadata; steps on metadata2's location
        patch1 = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hashlib.sha512('foo bar2').hexdigest()},
            'file_size': 2,
            u'locations': locs3c
        }

        # create the first file; should be OK
        ret = self.curl('/files', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])
        url = ret['data']['file']
        uid = url.split('/')[-1]

        # get the record of the file for its etag header
        ret = self.curl('/files/' + uid, 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('etag', ret['headers'])
        etag = ret['headers']['etag']

        # create the second file; should be OK
        ret = self.curl('/files', 'POST', metadata2)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('file', ret['data'])

        # try to update the first file with a patch; should NOT be OK
        ret = self.curl('/files/' + uid, 'PATCH', patch1, '/api', {'If-None-Match': etag})
        print(ret)
        self.assertEquals(ret['status'], 409)
        self.assertIn('message', ret['data'])
        self.assertIn('file', ret['data'])


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestStringMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
