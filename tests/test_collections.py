from __future__ import absolute_import, division, print_function

import os
import time
import random
import unittest
import hashlib

from file_catalog import auth

from .test_server import TestServerAPI

class TestCollectionsAPI(TestServerAPI):
    def test_10_collections(self):
        metadata = {
            'collection_name': 'blah',
            'owner': 'foo',
        }
        ret = self.curl('/collections', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('collection', ret['data'])
        url = ret['data']['collection']
        uid = url.split('/')[-1]

        ret = self.curl('/collections', 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('collections', ret['data'])
        self.assertIn(uid,{row['uuid'] for row in ret['data']['collections']})

    def test_20_collection_by_id(self):
        metadata = {
            'collection_name': 'blah',
            'owner': 'foo',
        }
        ret = self.curl('/collections', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('collection', ret['data'])
        url = ret['data']['collection']
        uid = url.split('/')[-1]

        ret = self.curl('/collections/%s'%(uid,), 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        for k in metadata:
            self.assertIn(k, ret['data'])
            self.assertEquals(metadata[k], ret['data'][k])

    def test_21_collection_by_name(self):
        metadata = {
            'collection_name': 'blah',
            'owner': 'foo',
        }
        ret = self.curl('/collections', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('collection', ret['data'])
        url = ret['data']['collection']
        uid = url.split('/')[-1]

        ret = self.curl('/collections/%s'%('blah',), 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        for k in metadata:
            self.assertIn(k, ret['data'])
            self.assertEquals(metadata[k], ret['data'][k])

    def test_30_collection_files(self):
        metadata = {
            'collection_name': 'blah',
            'owner': 'foo',
        }
        ret = self.curl('/collections', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('collection', ret['data'])
        url = ret['data']['collection']
        uid = url.split('/')[-1]

        ret = self.curl('/collections/%s/files'%('blah',), 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertEquals(ret['data']['files'], [])

        # add a file
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

        ret = self.curl('/collections/%s/files'%('blah',), 'GET',
                        args={'keys':'uuid|logical_name|checksum|locations'})
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertEquals(len(ret['data']['files']), 1)
        self.assertEquals(ret['data']['files'][0]['uuid'], uid)
        self.assertEquals(ret['data']['files'][0]['checksum'], metadata['checksum'])

    def test_70_snapshot_create(self):
        metadata = {
            'collection_name': 'blah',
            'owner': 'foo',
        }
        ret = self.curl('/collections', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('collection', ret['data'])
        url = ret['data']['collection']
        uid = url.split('/')[-1]

        ret = self.curl('/collections/%s'%(uid,), 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)

        ret = self.curl('/collections/%s/snapshots'%(uid,), 'POST', {})
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('snapshot', ret['data'])
        url = ret['data']['snapshot']
        snap_uid = url.split('/')[-1]

        ret = self.curl('/collections/%s/snapshots'%(uid,), 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('snapshots', ret['data'])
        self.assertEqual(len(ret['data']['snapshots']), 1)
        self.assertEqual(ret['data']['snapshots'][0]['uuid'], snap_uid)

    def test_71_snapshot_find(self):
        metadata = {
            'collection_name': 'blah',
            'owner': 'foo',
        }
        ret = self.curl('/collections', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('collection', ret['data'])
        url = ret['data']['collection']
        uid = url.split('/')[-1]

        ret = self.curl('/collections/%s'%(uid,), 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)

        ret = self.curl('/collections/%s/snapshots'%(uid,), 'POST', {})
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('snapshot', ret['data'])
        url = ret['data']['snapshot']
        snap_uid = url.split('/')[-1]

        ret = self.curl('/snapshots/%s'%(snap_uid,), 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('files',ret['data'])
        self.assertEqual(ret['data']['files'], [])

    def test_80_snapshot_files(self):
        metadata = {
            'collection_name': 'blah',
            'owner': 'foo',
        }
        ret = self.curl('/collections', 'POST', metadata)
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('collection', ret['data'])
        url = ret['data']['collection']
        uid = url.split('/')[-1]

        ret = self.curl('/collections/%s/snapshots'%(uid,), 'POST', {})
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('snapshot', ret['data'])
        url = ret['data']['snapshot']
        snap_uid = url.split('/')[-1]

        ret = self.curl('/snapshots/%s/files'%(snap_uid,), 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertEquals(ret['data']['files'], [])

        # add a file
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
        file_uid = url.split('/')[-1]
        
        # old snapshot stays empty
        ret = self.curl('/snapshots/%s/files'%(snap_uid,), 'GET')
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertEquals(ret['data']['files'], [])

        # new snapshot should have file
        ret = self.curl('/collections/%s/snapshots'%(uid,), 'POST', {})
        print(ret)
        self.assertEquals(ret['status'], 201)
        self.assertIn('_links', ret['data'])
        self.assertIn('self', ret['data']['_links'])
        self.assertIn('snapshot', ret['data'])
        url = ret['data']['snapshot']
        snap_uid = url.split('/')[-1]

        ret = self.curl('/snapshots/%s/files'%(snap_uid,), 'GET',
                        args={'keys':'uuid|logical_name|checksum|locations'})
        print(ret)
        self.assertEquals(ret['status'], 200)
        self.assertEquals(len(ret['data']['files']), 1)
        self.assertEquals(ret['data']['files'][0]['uuid'], file_uid)
        self.assertEquals(ret['data']['files'][0]['checksum'], metadata['checksum'])
        

if __name__ == '__main__':
    suite = unittest.TestLoader().loadTestsFromTestCase(TestStringMethods)
    unittest.TextTestRunner(verbosity=2).run(suite)
