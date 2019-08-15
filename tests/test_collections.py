from __future__ import absolute_import, division, print_function

import jwt
import os
import unittest

from rest_tools.client import RestClient

from .test_server import TestServerAPI
from .test_files import hex

class TestCollectionsAPI(TestServerAPI):
    def test_10_collections(self):
        token = self.get_token()
        alg = jwt.get_unverified_header(token)['alg']
        self.start_server(config_override={'TOKEN_AUTH_ALGORITHM':alg})
        r = RestClient(self.address, token, timeout=1, retries=1)

        metadata = {
            'collection_name': 'blah',
            'owner': 'foo',
        }
        data = r.request_seq('POST', '/api/collections', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('collection', data)
        url = data['collection']
        uid = url.split('/')[-1]

        data = r.request_seq('GET', '/api/collections')
        self.assertIn('collections', data)
        self.assertIn(uid,{row['uuid'] for row in data['collections']})

    def test_20_collection_by_id(self):
        token = self.get_token()
        alg = jwt.get_unverified_header(token)['alg']
        self.start_server(config_override={'TOKEN_AUTH_ALGORITHM':alg})
        r = RestClient(self.address, token, timeout=1, retries=1)

        metadata = {
            'collection_name': 'blah',
            'owner': 'foo',
        }
        data = r.request_seq('POST', '/api/collections', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('collection', data)
        url = data['collection']
        uid = url.split('/')[-1]

        data = r.request_seq('GET', '/api/collections/' + uid)
        for k in metadata:
            self.assertIn(k, data)
            self.assertEqual(metadata[k], data[k])

    def test_21_collection_by_name(self):
        token = self.get_token()
        alg = jwt.get_unverified_header(token)['alg']
        self.start_server(config_override={'TOKEN_AUTH_ALGORITHM':alg})
        r = RestClient(self.address, token, timeout=1, retries=1)

        metadata = {
            'collection_name': 'blah',
            'owner': 'foo',
        }
        data = r.request_seq('POST', '/api/collections', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('collection', data)
        url = data['collection']
        uid = url.split('/')[-1]

        data = r.request_seq('GET', '/api/collections/blah')
        for k in metadata:
            self.assertIn(k, data)
            self.assertEqual(metadata[k], data[k])

    def test_30_collection_files(self):
        token = self.get_token()
        alg = jwt.get_unverified_header(token)['alg']
        self.start_server(config_override={'TOKEN_AUTH_ALGORITHM':alg})
        r = RestClient(self.address, token, timeout=1, retries=1)

        metadata = {
            'collection_name': 'blah',
            'owner': 'foo',
        }
        data = r.request_seq('POST', '/api/collections', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('collection', data)
        url = data['collection']
        uid = url.split('/')[-1]

        data = r.request_seq('GET', '/api/collections/blah/files')
        self.assertEqual(data['files'], [])

        # add a file
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

        data = r.request_seq('GET', '/api/collections/blah/files',
                             {'keys':'uuid|logical_name|checksum|locations'})
        self.assertEqual(len(data['files']), 1)
        self.assertEqual(data['files'][0]['uuid'], uid)
        self.assertEqual(data['files'][0]['checksum'], metadata['checksum'])

    def test_70_snapshot_create(self):
        token = self.get_token()
        alg = jwt.get_unverified_header(token)['alg']
        self.start_server(config_override={'TOKEN_AUTH_ALGORITHM':alg})
        r = RestClient(self.address, token, timeout=1, retries=1)

        metadata = {
            'collection_name': 'blah',
            'owner': 'foo',
        }
        data = r.request_seq('POST', '/api/collections', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('collection', data)
        url = data['collection']
        uid = url.split('/')[-1]

        data = r.request_seq('GET', '/api/collections/' + uid)

        data = r.request_seq('POST', '/api/collections/{}/snapshots'.format(uid))
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('snapshot', data)
        url = data['snapshot']
        snap_uid = url.split('/')[-1]

        data = r.request_seq('GET', '/api/collections/{}/snapshots'.format(uid))
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('snapshots', data)
        self.assertEqual(len(data['snapshots']), 1)
        self.assertEqual(data['snapshots'][0]['uuid'], snap_uid)

    def test_71_snapshot_find(self):
        token = self.get_token()
        alg = jwt.get_unverified_header(token)['alg']
        self.start_server(config_override={'TOKEN_AUTH_ALGORITHM':alg})
        r = RestClient(self.address, token, timeout=1, retries=1)

        metadata = {
            'collection_name': 'blah',
            'owner': 'foo',
        }
        data = r.request_seq('POST', '/api/collections', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('collection', data)
        url = data['collection']
        uid = url.split('/')[-1]

        data = r.request_seq('GET', '/api/collections/' + uid)

        data = r.request_seq('POST', '/api/collections/{}/snapshots'.format(uid))
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('snapshot', data)
        url = data['snapshot']
        snap_uid = url.split('/')[-1]

        data = r.request_seq('GET', '/api/snapshots/' + snap_uid)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files',data)
        self.assertEqual(data['files'], [])

    def test_80_snapshot_files(self):
        token = self.get_token()
        alg = jwt.get_unverified_header(token)['alg']
        self.start_server(config_override={'TOKEN_AUTH_ALGORITHM':alg})
        r = RestClient(self.address, token, timeout=1, retries=1)

        metadata = {
            'collection_name': 'blah',
            'owner': 'foo',
        }
        data = r.request_seq('POST', '/api/collections', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('collection', data)
        url = data['collection']
        uid = url.split('/')[-1]

        data = r.request_seq('GET', '/api/collections/' + uid)

        data = r.request_seq('POST', '/api/collections/{}/snapshots'.format(uid))
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('snapshot', data)
        url = data['snapshot']
        snap_uid = url.split('/')[-1]

        data = r.request_seq('GET', '/api/snapshots/{}/files'.format(snap_uid))
        self.assertEqual(data['files'], [])

        # add a file
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
        file_uid = url.split('/')[-1]
        
        # old snapshot stays empty
        data = r.request_seq('GET', '/api/snapshots/{}/files'.format(snap_uid))
        self.assertEqual(data['files'], [])

        # new snapshot should have file
        data = r.request_seq('POST', '/api/collections/{}/snapshots'.format(uid))
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('snapshot', data)
        url = data['snapshot']
        snap_uid = url.split('/')[-1]

        data = r.request_seq('GET', '/api/snapshots/{}/files'.format(snap_uid),
                             {'keys':'uuid|logical_name|checksum|locations'})
        self.assertEqual(len(data['files']), 1)
        self.assertEqual(data['files'][0]['uuid'], file_uid)
        self.assertEqual(data['files'][0]['checksum'], metadata['checksum'])
        

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestStringMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
