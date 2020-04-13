from __future__ import absolute_import, division, print_function

import os
import unittest
import hashlib

from tornado.escape import json_encode,json_decode
from rest_tools.client import RestClient

from .test_server import TestServerAPI

def hex(data):
    if isinstance(data, str):
        data = data.encode('utf-8')
    return hashlib.sha512(data).hexdigest()

class TestFilesAPI(TestServerAPI):
    def test_10_files(self):
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        metadata = {
            'logical_name': 'blah',
            'checksum': {'sha512':hex('foo bar')},
            'file_size': 1,
            u'locations': [{u'site':u'test',u'path':u'blah.dat'}]
        }
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        data = r.request_seq('GET', '/api/files')
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertTrue(any(uid == f['uuid'] for f in data['files']))

        for m in ('PUT','DELETE','PATCH'):
            with self.assertRaises(Exception):
                r.request_seq(m, '/api/files')

    def test_11_files_count(self):
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        metadata = {
            'logical_name': 'blah',
            'checksum': {'sha512':hex('foo bar')},
            'file_size': 1,
            u'locations': [{u'site':u'test',u'path':u'blah.dat'}]
        }
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        data = r.request_seq('GET', '/api/files/count')
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(data['files'], 1)

    def test_15_files_auth(self):
        self.start_server(config_override={'SECRET':'secret'})
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        metadata = {
            'logical_name': 'blah',
            'checksum': {'sha512':hex('foo bar')},
            'file_size': 1,
            u'locations': [{u'site':u'test',u'path':u'blah.dat'}]
        }
        r2 = RestClient(self.address, 'blah', timeout=1, retries=1)
        with self.assertRaises(Exception):
            r2.request_seq('POST', '/api/files', metadata)

        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']

    def test_20_file(self):
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        metadata = {
            u'logical_name': u'blah',
            u'checksum': {u'sha512':hex('foo bar')},
            u'file_size': 1,
            u'locations': [{u'site':u'test',u'path':u'blah.dat'}]
        }
        data = r.request_seq('POST', '/api/files', metadata)

        url = data['file']

        data = r.request_seq('GET', url)
        data.pop('_links')
        data.pop('meta_modify_date')
        data.pop('uuid')
        self.assertDictEqual(metadata, data)

        metadata['test'] = 100

        metadata_cpy = metadata.copy()
        metadata_cpy['uuid'] = 'something else'
        with self.assertRaises(Exception):
            data = r.request_seq('PUT', url, metadata_cpy)

        data = r.request_seq('PUT', url, metadata)
        data.pop('_links')
        data.pop('meta_modify_date')
        data.pop('uuid')
        self.assertDictEqual(metadata, data)

        data = r.request_seq('GET', url)
        data.pop('_links')
        data.pop('meta_modify_date')
        data.pop('uuid')
        self.assertDictEqual(metadata, data)

        metadata['test2'] = 200
        data = r.request_seq('PATCH', url, {'test2':200})
        data.pop('_links')
        data.pop('meta_modify_date')
        data.pop('uuid')
        self.assertDictEqual(metadata, data)

        data = r.request_seq('GET', url)
        data.pop('_links')
        data.pop('meta_modify_date')
        data.pop('uuid')
        self.assertDictEqual(metadata, data)

        data = r.request_seq('DELETE', url)

        # second delete should raise error
        with self.assertRaises(Exception):
            data = r.request_seq('DELETE', url)

        with self.assertRaises(Exception):
            data = r.request_seq('POST', url)

    def test_30_archive(self):
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        metadata = {
            u'logical_name': u'blah',
            u'checksum': {u'sha512':hex('foo bar')},
            u'file_size': 1,
            u'locations': [{u'site':u'test',u'path':u'blah.dat'}]
        }
        metadata2 = {
            u'logical_name': u'blah2',
            u'checksum': {u'sha512':hex('foo bar baz')},
            u'file_size': 2,
            u'locations': [{u'site':u'test',u'path':u'blah.dat',u'archive':True}]
        }
        data = r.request_seq('POST', '/api/files', metadata)
        url = data['file']
        uid = url.split('/')[-1]
        data = r.request_seq('POST', '/api/files', metadata2)
        url2 = data['file']
        uid2 = url2.split('/')[-1]

        data = r.request_seq('GET', '/api/files')
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertTrue(any(uid == f['uuid'] for f in data['files']))
        self.assertFalse(any(uid2 == f['uuid'] for f in data['files']))

        data = r.request_seq('GET', '/api/files', {'query':json_encode({'locations.archive':True})})
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertFalse(any(uid == f['uuid'] for f in data['files']))
        self.assertTrue(any(uid2 == f['uuid'] for f in data['files']))

    def test_40_simple_query(self):
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        metadata = {
            u'logical_name': u'blah',
            u'checksum': {u'sha512':hex('foo bar')},
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
            u'checksum': {u'sha512':hex('foo bar baz')},
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
        data = r.request_seq('POST', '/api/files', metadata)
        url = data['file']
        uid = url.split('/')[-1]
        data = r.request_seq('POST', '/api/files', metadata2)
        url2 = data['file']
        uid2 = url2.split('/')[-1]

        data = r.request_seq('GET', '/api/files')
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 2)
        self.assertTrue(any(uid == f['uuid'] for f in data['files']))
        self.assertTrue(any(uid2 == f['uuid'] for f in data['files']))

        data = r.request_seq('GET', '/api/files', {'processing_level':'level2'})
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 2)
        self.assertTrue(any(uid == f['uuid'] for f in data['files']))
        self.assertTrue(any(uid2 == f['uuid'] for f in data['files']))

        data = r.request_seq('GET', '/api/files', {'run_number':12345})
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertTrue(any(uid == f['uuid'] for f in data['files']))
        self.assertFalse(any(uid2 == f['uuid'] for f in data['files']))

        data = r.request_seq('GET', '/api/files', {'dataset':23454})
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertFalse(any(uid == f['uuid'] for f in data['files']))
        self.assertTrue(any(uid2 == f['uuid'] for f in data['files']))

        data = r.request_seq('GET', '/api/files', {'event_id':400})
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertTrue(any(uid == f['uuid'] for f in data['files']))
        self.assertFalse(any(uid2 == f['uuid'] for f in data['files']))

        data = r.request_seq('GET', '/api/files', {'season':2017})
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 2)
        self.assertTrue(any(uid == f['uuid'] for f in data['files']))
        self.assertTrue(any(uid2 == f['uuid'] for f in data['files']))

        data = r.request_seq('GET', '/api/files', {'event_id':400, 'keys':'|'.join(['checksum','file_size','uuid'])})
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertTrue(any(uid == f['uuid'] for f in data['files']))
        self.assertFalse(any(uid2 == f['uuid'] for f in data['files']))
        self.assertIn('checksum', data['files'][0])
        self.assertIn('file_size', data['files'][0])

    def test_50_post_files_unique_logical_name(self):
        """Test that logical_name is unique when creating a new file."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        # define the file to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }

        # create the file the first time; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # check that the file was created properly
        data = r.request_seq('GET', '/api/files')
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertTrue(any(uid == f['uuid'] for f in data['files']))

        # create the file the second time; should NOT be OK
        with self.assertRaises(Exception):
            data = r.request_seq('POST', '/api/files', metadata)

        # check that the second file was not created
        data = r.request_seq('GET', '/api/files')
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertTrue(any(uid == f['uuid'] for f in data['files']))

    def test_51_put_files_uuid_unique_logical_name(self):
        """Test that logical_name is unique when replacing a file."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah2.dat'}]
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # create the second file; should be OK
        data = r.request_seq('POST', '/api/files', metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)

        # try to replace the first file with a copy of the second; should NOT be OK
        with self.assertRaises(Exception):
            r.request_seq('PUT', '/api/files/' + uid, metadata2)

    def test_52_put_files_uuid_replace_logical_name(self):
        """Test that a file can replace with the same logical_name."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar2')},
            'file_size': 2,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # try to replace the first file with the second; should be OK
        data = r.request_seq('PUT', '/api/files/' + uid, metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('logical_name', data)

    def test_53_patch_files_uuid_unique_logical_name(self):
        """Test that logical_name is unique when updating a file."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah2.dat'}]
        }

        # this is a PATCH to metadata; steps on metadata2's logical_name
        patch1 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hex('foo bar2')},
            'file_size': 2,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah2.dat'}]
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # create the second file; should be OK
        data = r.request_seq('POST', '/api/files', metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)

        # try to update the first file with a patch; should NOT be OK
        with self.assertRaises(Exception):
            r.request_seq('PATCH', '/api/files/'+uuid, patch1)

    def test_54_patch_files_uuid_replace_logical_name(self):
        """Test that a file can be updated with the same logical_name."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        # define the file to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }

        # this is a PATCH to metadata; matches the old logical_name
        patch1 = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar2')},
            'file_size': 2,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }

        # create the file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # try to update the file with a patch; should be OK
        data = r.request_seq('PATCH', '/api/files/' + uid, patch1)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('logical_name', data)

    def test_55_post_files_unique_locations(self):
        """Test that locations is unique when creating a new file."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hex('foo bar2')},
            'file_size': 2,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # check that the file was created properly
        data = r.request_seq('GET', '/api/files')
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertTrue(any(uid == f['uuid'] for f in data['files']))

        # create the second file; should NOT be OK
        with self.assertRaises(Exception):
            r.request_seq('POST', '/api/files', metadata2)

    def test_56_put_files_uuid_unique_locations(self):
        """Test that locations is unique when replacing a file."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hex('foo bar2')},
            'file_size': 2,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah2.dat'}]
        }
        replace1 = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar2')},
            'file_size': 2,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah2.dat'}]
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # create the second file; should be OK
        data = r.request_seq('POST', '/api/files', metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)

        # try to replace the first file with a location collision with the second; should NOT be OK
        with self.assertRaises(Exception):
            r.request_seq('PUT', '/api/files/' + uuid, replace1)

    def test_57_put_files_uuid_replace_locations(self):
        """Test that a file can replace with the same location."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar2')},
            'file_size': 2,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # try to replace the first file with the second; should be OK
        data = r.request_seq('PUT', '/api/files/'+uid, metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('logical_name', data)

    def test_58_patch_files_uuid_unique_locations(self):
        """Test that locations is unique when updating a file."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        # define the files to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah2.dat'}]
        }

        # this is a PATCH to metadata; steps on metadata2's location
        patch1 = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar2')},
            'file_size': 2,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah2.dat'}]
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # create the second file; should be OK
        data = r.request_seq('POST', '/api/files', metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)

        # try to update the first file with a patch; should NOT be OK
        with self.assertRaises(Exception):
            r.request_seq('PATCH', '/api/files/' + uuid, patch1)

    def test_59_patch_files_uuid_replace_locations(self):
        """Test that a file can be updated with the same location."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        # define the file to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }

        # this is a PATCH to metadata; matches the old location
        patch1 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hex('foo bar2')},
            'file_size': 2,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }

        # create the file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # try to update the file with a patch; should be OK
        data = r.request_seq('PATCH', '/api/files/' + uid, patch1)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('logical_name', data)
        self.assertIn('locations', data)

    def test_60_post_files_locations_1xN(self):
        """Test locations uniqueness under 1xN multiplicity."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

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
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': [loc1a]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hex('foo bar2')},
            'file_size': 2,
            u'locations': locs3a
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # check that the file was created properly
        data = r.request_seq('GET', '/api/files')
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertTrue(any(uid == f['uuid'] for f in data['files']))

        # create the second file; should NOT be OK
        with self.assertRaises(Exception):
            r.request_seq('POST', '/api/files', metadata2)

    def test_61_post_files_locations_Nx1(self):
        """Test locations uniqueness under Nx1 multiplicity."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

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
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': locs3b
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hex('foo bar2')},
            'file_size': 2,
            u'locations': [loc1c]
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # check that the file was created properly
        data = r.request_seq('GET', '/api/files')
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertTrue(any(uid == f['uuid'] for f in data['files']))

        # check that the file was created properly, part deux
        data = r.request_seq('GET', '/api/files/' + uid)

        # create the second file; should NOT be OK
        with self.assertRaises(Exception):
            r.request_seq('POST', '/api/files', metadata2)

    def test_62_post_files_locations_NxN(self):
        """Test locations uniqueness under NxN multiplicity."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

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
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': locs3a
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hex('foo bar2')},
            'file_size': 2,
            u'locations': locs3c
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # check that the file was created properly
        data = r.request_seq('GET', '/api/files')
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertTrue(any(uid == f['uuid'] for f in data['files']))

        # check that the file was created properly, part deux
        data = r.request_seq('GET', '/api/files/' + uid)

        # create the second file; should NOT be OK
        with self.assertRaises(Exception):
            r.request_seq('POST', '/api/files', metadata2)

    def test_63_put_files_uuid_locations_1xN(self):
        """Test locations uniqueness under 1xN multiplicity."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

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
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': [loc1a]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hex('foo bar2')},
            'file_size': 2,
            u'locations': [loc1b]
        }
        replace1 = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar2')},
            'file_size': 2,
            u'locations': locs3c
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # create the second file; should be OK
        data = r.request_seq('POST', '/api/files', metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)

        # try to replace the first file with a location collision with the second; should NOT be OK
        with self.assertRaises(Exception):
            r.request_seq('PUT', '/api/files/' + uid, replace1)

    def test_64_put_files_uuid_locations_Nx1(self):
        """Test locations uniqueness under Nx1 multiplicity."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

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
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': [loc1d]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hex('foo bar2')},
            'file_size': 2,
            u'locations': locs3a
        }
        replace1 = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar2')},
            'file_size': 2,
            u'locations': [loc1a]
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # create the second file; should be OK
        data = r.request_seq('POST', '/api/files', metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)

        # try to replace the first file with a location collision with the second; should NOT be OK
        with self.assertRaises(Exception):
            r.request_seq('PUT', '/api/files/' + uid, replace1)

    def test_65_put_files_uuid_locations_NxN(self):
        """Test locations uniqueness under NxN multiplicity."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

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
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': [loc1d]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hex('foo bar2')},
            'file_size': 2,
            u'locations': locs3a
        }
        replace1 = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar2')},
            'file_size': 2,
            u'locations': locs3b
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # create the second file; should be OK
        data = r.request_seq('POST', '/api/files', metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)

        # try to replace the first file with a location collision with the second; should NOT be OK
        with self.assertRaises(Exception):
            r.request_seq('PUT', '/api/files/' + uid, replace1)

    def test_66_patch_files_uuid_locations_1xN(self):
        """Test locations uniqueness under 1xN multiplicity."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

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
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': [loc1a]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': [loc1b]
        }

        # this is a PATCH to metadata; steps on metadata2's location
        patch1 = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar2')},
            'file_size': 2,
            u'locations': locs3c
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # create the second file; should be OK
        data = r.request_seq('POST', '/api/files', metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)

        # try to update the first file with a patch; should NOT be OK
        with self.assertRaises(Exception):
            r.request_seq('PATCH', '/api/files/' + uid, patch1)

    def test_67_patch_files_uuid_locations_Nx1(self):
        """Test locations uniqueness under Nx1 multiplicity."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

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
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': [loc1a]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': locs3b
        }

        # this is a PATCH to metadata; steps on metadata2's location
        patch1 = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar2')},
            'file_size': 2,
            u'locations': [loc1c]
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # create the second file; should be OK
        data = r.request_seq('POST', '/api/files', metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)

        # try to update the first file with a patch; should NOT be OK
        with self.assertRaises(Exception):
            r.request_seq('PATCH', '/api/files/' + uid, patch1)

    def test_68_patch_files_uuid_locations_NxN(self):
        """Test locations uniqueness under NxN multiplicity."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

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
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': [loc1a]
        }
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': locs3b
        }

        # this is a PATCH to metadata; steps on metadata2's location
        patch1 = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar2')},
            'file_size': 2,
            u'locations': locs3c
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # create the second file; should be OK
        data = r.request_seq('POST', '/api/files', metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)

        # try to update the first file with a patch; should NOT be OK
        with self.assertRaises(Exception):
            r.request_seq('PATCH', '/api/files/' + uid, patch1)

    def test_70_abuse_post_files_locations(self):
        """Abuse the POST /api/files/UUID/locations route to test error handling."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        # define some locations to be tested
        loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
        locations = [loc1a, loc1b, loc1c, loc1d]

        # try to POST to an invalid UUID
        valid_post_body = {"locations": locations}
        with self.assertRaises(Exception):
            r.request_seq('POST', '/api/files/bobsyeruncle/locations', valid_post_body)

        # try to POST to an non-existant UUID
        with self.assertRaises(Exception):
            r.request_seq('POST', '/api/files/6e4ec06d-8e22-4a2b-a392-f4492fb25eb1/locations', valid_post_body)

        # define a file to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': [loc1a]
        }

        # create the file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # try to POST to the file without a post body
        with self.assertRaises(Exception):
            r.request_seq('POST', '/api/files/' + uid + '/locations', {})

        # try to POST to the file with a non-array locations
        with self.assertRaises(Exception):
            r.request_seq('POST', '/api/files/' + uid + '/locations', {"locations": "bobsyeruncle"})

    def test_71_post_files_locations_duplicate(self):
        """Test that POST /api/files/UUID/locations is a no-op for non-distinct locations."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        # define some locations to be tested
        loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
        locations = [loc1a, loc1b, loc1c, loc1d]

        # define a file to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': locations
        }

        # create the file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # read the full record of the file; should be OK
        rec = r.request_seq('GET', '/api/files/' + uid)
        self.assertEqual(4, len(rec["locations"]))
        self.assertIn('meta_modify_date', rec)
        mmd = rec['meta_modify_date']

        # try to POST existant locations to the file; should be OK
        not_so_new_locations = {"locations": [loc1b, loc1d]}
        rec2 = r.request_seq('POST', '/api/files/' + uid + '/locations', not_so_new_locations)

        # ensure the record is the same (not updated)
        self.assertEqual(4, len(rec2["locations"]))
        self.assertListEqual(rec["locations"], rec2["locations"])
        self.assertEqual(mmd, rec2["meta_modify_date"])

    def test_72_post_files_locations_conflict(self):
        """Test that POST /api/files/UUID/locations returns an error on conflicting duplicate locations."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        # define some locations to be tested
        loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
        locations = [loc1a, loc1b]

        # define a file to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': locations
        }

        # create the file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # define a second file to be created
        locations2 = [loc1c, loc1d]
        metadata2 = {
            'logical_name': '/blah/data/exp/IceCube/blah2.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': locations2
        }

        # create the file; should be OK
        data = r.request_seq('POST', '/api/files', metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid2 = url.split('/')[-1]

        # try to POST a second file location to the first file
        with self.assertRaises(Exception):
            conflicting_locations = {"locations": [loc1d]}
            rec2 = r.request_seq('POST', '/api/files/' + uid + '/locations', conflicting_locations)

    def test_73_post_files_locations(self):
        """Test that POST /api/files/UUID/locations can add distinct non-conflicting locations."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        # define some locations to be tested
        loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
        locations = [loc1a, loc1b]

        # define a file to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': locations
        }

        # create the file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # read the full record of the file; should be OK
        rec = r.request_seq('GET', '/api/files/' + uid)
        self.assertEqual(2, len(rec["locations"]))
        self.assertIn('meta_modify_date', rec)
        mmd = rec['meta_modify_date']

        # try to POST existant locations to the file; should be OK
        new_locations = {"locations": [loc1c, loc1d]}
        rec2 = r.request_seq('POST', '/api/files/' + uid + '/locations', new_locations)

        # ensure the record has changed (is updated)
        self.assertEqual(4, len(rec2["locations"]))
        self.assertNotEqual(mmd, rec2["meta_modify_date"])
        self.assertIn(loc1a, rec2["locations"])
        self.assertIn(loc1b, rec2["locations"])
        self.assertIn(loc1c, rec2["locations"])
        self.assertIn(loc1d, rec2["locations"])

    def test_74_post_files_locations_just_one(self):
        """Test that POST /api/files/UUID/locations can add distinct non-conflicting locations."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        # define some locations to be tested
        loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
        loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
        locations = [loc1a]

        # define a file to be created
        metadata = {
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 1,
            u'locations': locations
        }

        # create the file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uid = url.split('/')[-1]

        # read the full record of the file; should be OK
        rec = r.request_seq('GET', '/api/files/' + uid)
        self.assertEqual(1, len(rec["locations"]))
        self.assertIn('meta_modify_date', rec)
        mmd = rec['meta_modify_date']

        # try to POST existant locations to the file; should be OK
        new_locations = {"locations": [loc1c]}
        rec2 = r.request_seq('POST', '/api/files/' + uid + '/locations', new_locations)

        # ensure the record has changed (is updated)
        self.assertEqual(2, len(rec2["locations"]))
        self.assertNotEqual(mmd, rec2["meta_modify_date"])
        self.assertIn(loc1a, rec2["locations"])
        self.assertNotIn(loc1b, rec2["locations"])
        self.assertIn(loc1c, rec2["locations"])
        self.assertNotIn(loc1d, rec2["locations"])


if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestStringMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
