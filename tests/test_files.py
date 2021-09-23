"""Test /api/files."""

# fmt:off
# pylint: skip-file

from __future__ import absolute_import, division, print_function

import copy
import hashlib
import itertools
import os
import unittest
from typing import Any, Dict, List, Optional

import requests
from file_catalog.schema import types
from rest_tools.client import RestClient  # type: ignore[import]
from tornado.escape import json_encode

from .test_server import TestServerAPI


def hex(data: Any) -> str:
    """Get sha512."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha512(data).hexdigest()


def _assert_httperror(exception: Exception, code: int, reason: str) -> None:
    """Assert that this is the expected HTTPError."""
    print(exception)
    assert isinstance(exception, requests.exceptions.HTTPError)
    assert exception.response.status_code == code
    assert exception.response.reason == reason


class TestFilesAPI(TestServerAPI):
    """Test /api/files/*."""

    def test_10_files(self) -> None:
        """Test POST then GET."""
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
        uuid = url.split('/')[-1]

        data = r.request_seq('GET', '/api/files')
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertTrue(any(uuid == f['uuid'] for f in data['files']))

        for m in ('PUT','DELETE','PATCH'):
            with self.assertRaises(Exception) as cm:
                r.request_seq(m, '/api/files')
            _assert_httperror(cm.exception, 405, "Method Not Allowed")

    def test_11_files_count(self) -> None:
        """Test /api/files/count."""
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
        uuid = url.split('/')[-1]

        data = r.request_seq('GET', '/api/files/count')
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(data['files'], 1)

    def test_12_files_keys(self) -> None:
        """Test the 'keys' and all-keys' arguments."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        metadata = {
            "logical_name": "blah",
            "checksum": {"sha512": hex("foo bar")},
            "file_size": 1,
            "locations": [{"site": "test", "path": "blah.dat"}],
            "extra": "foo",
            "supplemental": ["green", "eggs", "ham"],
        }
        data = r.request_seq("POST", "/api/files", metadata)
        self.assertIn("_links", data)
        self.assertIn("self", data["_links"])
        self.assertIn("file", data)
        assert "extra" not in data
        assert "supplemental" not in data
        url = data["file"]
        uuid = url.split("/")[-1]

        # w/o all-keys
        data = r.request_seq("GET", "/api/files")
        assert set(data["files"][0].keys()) == {"logical_name", "uuid"}

        # w/ all-keys
        args: Dict[str, Any] = {"all-keys": True}
        data = r.request_seq("GET", "/api/files", args)
        assert set(data["files"][0].keys()) == {
            "logical_name",
            "uuid",
            "checksum",
            "file_size",
            "locations",
            "extra",
            "supplemental",
            "meta_modify_date",
        }

        # w/ all-keys = False
        args = {"all-keys": False}
        data = r.request_seq("GET", "/api/files", args)
        assert set(data["files"][0].keys()) == {"logical_name", "uuid"}

        # w/ all-keys & keys
        args = {"all-keys": True, "keys": "checksum|file_size"}
        data = r.request_seq("GET", "/api/files", args)
        assert set(data["files"][0].keys()) == {
            "logical_name",
            "uuid",
            "checksum",
            "file_size",
            "locations",
            "extra",
            "supplemental",
            "meta_modify_date",
        }

        # w/ all-keys = False & keys
        args = {"all-keys": False, "keys": "checksum|file_size"}
        data = r.request_seq("GET", "/api/files", args)
        assert set(data["files"][0].keys()) == {"checksum", "file_size"}

        # w/ just keys
        args = {"keys": "checksum|file_size"}
        data = r.request_seq("GET", "/api/files", args)
        assert set(data["files"][0].keys()) == {"checksum", "file_size"}

    def test_13_files_path_like_args(self) -> None:
        """Test the path-like base/shortcut arguments.

        "logical_name", "directory", "filename", "path", & "logical-name-regex".
        """
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        metadata_objs = [
            {
                "logical_name": "/foo/bar/baz/bat.txt",
                "checksum": {"sha512": hex("1")},
                "file_size": 1,
                "locations": [{"site": "test", "path": "foo/bar/baz/bat.txt"}],
            },
            {
                "logical_name": "/foo/bar/ham.txt",
                "checksum": {"sha512": hex("2")},
                "file_size": 2,
                "locations": [{"site": "test", "path": "/foo/bar/ham.txt"}],
            },
            {
                "logical_name": "/green/eggs/and/ham.txt",
                "checksum": {"sha512": hex("3")},
                "file_size": 3,
                "locations": [{"site": "test", "path": "/green/eggs/and/ham.txt"}],
            },
            {
                "logical_name": "/john/paul/george/ringo/ham.txt",
                "checksum": {"sha512": hex("4")},
                "file_size": 4,
                "locations": [
                    {"site": "test", "path": "/john/paul/george/ringo/ham.txt"}
                ],
            },
        ]
        for meta in metadata_objs:
            r.request_seq("POST", "/api/files", meta)

        def get_logical_names(args: Optional[Dict[str, str]] = None) -> List[str]:
            if not args:
                args = {}
            ret = r.request_seq("GET", "/api/files", args)
            print(ret)
            return [f["logical_name"] for f in ret["files"]]

        assert len(get_logical_names()) == 4
        # logical_name
        assert len(get_logical_names({"logical_name": "/foo/bar/ham.txt"})) == 1
        # directory
        paths = get_logical_names({"directory": "/foo/bar"})
        assert set(paths) == {"/foo/bar/ham.txt", "/foo/bar/baz/bat.txt"}
        assert len(get_logical_names({"directory": "/fo"})) == 0
        # filename
        paths = get_logical_names({"filename": "ham.txt"})
        assert set(paths) == {
            "/foo/bar/ham.txt",
            "/green/eggs/and/ham.txt",
            "/john/paul/george/ringo/ham.txt",
        }
        assert len(get_logical_names({"filename": ".txt"})) == 0
        # directory & filename
        paths = get_logical_names({"directory": "/foo", "filename": "ham.txt"})
        assert paths == ["/foo/bar/ham.txt"]
        # logical-name-regex
        paths = get_logical_names({"logical-name-regex": r".*george/ringo.*"})
        assert paths == ["/john/paul/george/ringo/ham.txt"]
        assert len(get_logical_names({"logical-name-regex": r".*"})) == 4

    def test_15_files_auth(self) -> None:
        """Test auth/token; good and bad (403) cases.

        Override/set the `SECRET` environment variable.
        """
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
        with self.assertRaises(Exception) as cm:
            r2.request_seq('POST', '/api/files', metadata)
        _assert_httperror(cm.exception, 403, "Forbidden")

        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']

    def test_16_files_uri(self) -> None:
        """Test changing the MongoDB URI.

        Override/set the `MONGODB_URI` environment variable.
        """
        host = os.environ['TEST_DATABASE_HOST']
        port = os.environ['TEST_DATABASE_PORT']
        uri = f"mongodb://{host}:{port}"

        self.start_server(config_override={'MONGODB_URI': uri})
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        metadata = {
            'logical_name': 'blah',
            'checksum': {'sha512':hex('foo bar')},
            'file_size': 1,
            u'locations': [{u'site':u'test',u'path':u'blah.dat'}]
        }
        r2 = RestClient(self.address, 'blah', timeout=1, retries=1)
        with self.assertRaises(Exception) as cm:
            r2.request_seq('POST', '/api/files', metadata)
        _assert_httperror(cm.exception, 403, "Forbidden")

        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']

    def test_20_file(self) -> None:
        """Test POST, GET, PUT, PATCH, and DELETE."""
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

        metadata_cpy = copy.deepcopy(metadata)
        metadata_cpy['uuid'] = 'something else'
        with self.assertRaises(Exception) as cm:
            data = r.request_seq('PUT', url, metadata_cpy)
        _assert_httperror(cm.exception, 400, "Validation Error: forbidden attribute update `uuid`")

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
        data = r.request_seq('PATCH', url, {'test2': 200})
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
        with self.assertRaises(Exception) as cm:
            data = r.request_seq('DELETE', url)
        _assert_httperror(cm.exception, 404, "File uuid not found")

        with self.assertRaises(Exception) as cm:
            data = r.request_seq('POST', url)
        _assert_httperror(cm.exception, 405, "Method Not Allowed")

    def test_21_file_404(self) -> None:
        """Test 404 (File Not Found) cases for GET, PUT, PATCH, and DELETE."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        # Start by putting something in the FC
        metadata: types.Metadata = {
            u'logical_name': u'blah',
            u'checksum': {u'sha512': hex('foo bar')},
            u'file_size': 1,
            u'locations': [{u'site': u'test', u'path': u'blah.dat'}]
        }
        r.request_seq('POST', '/api/files', metadata)

        metadata_404 = copy.deepcopy(metadata)
        metadata_404['uuid'] = 'n0t-a-R3al-Uu1d'

        # GET
        with self.assertRaises(Exception) as cm:
            r.request_seq('GET', '/api/files/' + metadata_404['uuid'])
        _assert_httperror(cm.exception, 404, "File uuid not found")

        # PUT
        with self.assertRaises(Exception) as cm:
            r.request_seq('PUT', '/api/files/' + metadata_404['uuid'], metadata_404)
        _assert_httperror(cm.exception, 404, "File uuid not found")

        # PATCH
        with self.assertRaises(Exception) as cm:
            r.request_seq('PATCH', '/api/files/' + metadata_404['uuid'], metadata_404)
        _assert_httperror(cm.exception, 404, "File uuid not found")

        # DELETE
        with self.assertRaises(Exception) as cm:
            r.request_seq('DELETE', '/api/files/' + metadata_404['uuid'])
        _assert_httperror(cm.exception, 404, "File uuid not found")

    def test_30_archive(self) -> None:
        """Test GET w/ query arg: `locations.archive`."""
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
        uuid = url.split('/')[-1]
        data = r.request_seq('POST', '/api/files', metadata2)
        url2 = data['file']
        uuid2 = url2.split('/')[-1]

        data = r.request_seq('GET', '/api/files')
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertTrue(any(uuid == f['uuid'] for f in data['files']))
        self.assertFalse(any(uuid2 == f['uuid'] for f in data['files']))

        data = r.request_seq('GET', '/api/files', {'query':json_encode({'locations.archive':True})})
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertFalse(any(uuid == f['uuid'] for f in data['files']))
        self.assertTrue(any(uuid2 == f['uuid'] for f in data['files']))

        data = r.request_seq('GET', '/api/files', {'query':json_encode({'locations.archive':False})})
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 0)

        metadata3 = {
            u'logical_name': u'blah3',
            u'checksum': {u'sha512':hex('1234')},
            u'file_size': 3,
            u'locations': [{u'site':u'test',u'path':u'blah.dat',u'archive':False}]
        }
        data = r.request_seq('POST', '/api/files', metadata3)
        url3 = data['file']
        uid3 = url3.split('/')[-1]

        data = r.request_seq('GET', '/api/files', {'query':json_encode({'locations.archive':False})})
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertFalse(any(uuid == f['uuid'] for f in data['files']))
        self.assertFalse(any(uuid2 == f['uuid'] for f in data['files']))
        self.assertTrue(any(uid3 == f['uuid'] for f in data['files']))

    def test_40_simple_query(self):
        """Test GET w/ shortcut-metadata query args."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        metadata = {
            u'logical_name': u'blah',
            u'checksum': {u'sha512': hex('foo bar')},
            u'file_size': 1,
            u'locations': [{u'site': u'test', u'path': u'blah.dat'}],
            u'processing_level': u'level2',
            u'run': {
                u'run_number': 12345,
                u'first_event': 345,
                u'last_event': 456,
            },
            u'iceprod': {
                u'dataset': 23453,
            },
            u'offline_processing_metadata': {
                u'season': 2017,
            },
        }
        metadata2 = {
            u'logical_name': u'blah2',
            u'checksum': {u'sha512': hex('foo bar baz')},
            u'file_size': 2,
            u'locations': [{u'site': u'test', u'path': u'blah2.dat'}],
            u'processing_level': u'level2',
            u'run': {
                r'run_number': 12356,
                u'first_event': 578,
                u'last_event': 698,
            },
            u'iceprod': {
                u'dataset': 23454,
            },
            u'offline_processing_metadata': {
                u'season': 2017,
            },
        }
        data = r.request_seq('POST', '/api/files', metadata)
        url = data['file']
        uuid = url.split('/')[-1]
        data = r.request_seq('POST', '/api/files', metadata2)
        url2 = data['file']
        uuid2 = url2.split('/')[-1]

        data = r.request_seq('GET', '/api/files')
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 2)
        self.assertTrue(any(uuid == f['uuid'] for f in data['files']))
        self.assertTrue(any(uuid2 == f['uuid'] for f in data['files']))

        data = r.request_seq('GET', '/api/files', {'processing_level':'level2'})
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 2)
        self.assertTrue(any(uuid == f['uuid'] for f in data['files']))
        self.assertTrue(any(uuid2 == f['uuid'] for f in data['files']))

        data = r.request_seq('GET', '/api/files', {'run_number':12345})
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertTrue(any(uuid == f['uuid'] for f in data['files']))
        self.assertFalse(any(uuid2 == f['uuid'] for f in data['files']))

        data = r.request_seq('GET', '/api/files', {'dataset':23454})
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertFalse(any(uuid == f['uuid'] for f in data['files']))
        self.assertTrue(any(uuid2 == f['uuid'] for f in data['files']))

        data = r.request_seq('GET', '/api/files', {'event_id':400})
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertTrue(any(uuid == f['uuid'] for f in data['files']))
        self.assertFalse(any(uuid2 == f['uuid'] for f in data['files']))

        data = r.request_seq('GET', '/api/files', {'season':2017})
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 2)
        self.assertTrue(any(uuid == f['uuid'] for f in data['files']))
        self.assertTrue(any(uuid2 == f['uuid'] for f in data['files']))

        data = r.request_seq('GET', '/api/files', {'event_id':400, 'keys':'|'.join(['checksum','file_size','uuid'])})
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertTrue(any(uuid == f['uuid'] for f in data['files']))
        self.assertFalse(any(uuid2 == f['uuid'] for f in data['files']))
        self.assertIn('checksum', data['files'][0])
        self.assertIn('file_size', data['files'][0])

    def test_41_simple_query(self) -> None:
        """Test the limit and start parameters."""
        self.start_server()
        token = self.get_token()
        r = RestClient(self.address, token, timeout=1, retries=1)

        # Populate FC
        for i in range(100):
            metadata = {
                'logical_name': f'/foo/bar/{i}.dat',
                'checksum': {'sha512': hex(f'foo bar {i}')},
                'file_size': 3 * i,
                u'locations': [{u'site': u'WIPAC', u'path': f'/foo/bar/{i}.dat'}]
            }
            r.request_seq('POST', '/api/files', metadata)

        # Some Legal Corner Cases
        assert len(r.request_seq('GET', '/api/files')['files']) == 100
        assert len(r.request_seq('GET', '/api/files', {'limit': 300})['files']) == 100
        assert not r.request_seq('GET', '/api/files', {'start': 300})['files']
        assert len(r.request_seq('GET', '/api/files', {'start': 99})['files']) == 1
        assert len(r.request_seq('GET', '/api/files', {'start': None})['files']) == 100
        assert len(r.request_seq('GET', '/api/files', {'limit': None})['files']) == 100

        # Normal Usage
        limit = 3
        received = []
        for i in itertools.count():
            start = i * limit
            print(f"{i=} {start=} {limit=}")
            res = r.request_seq('GET', '/api/files', {'start': start, 'limit': limit})

            # normal query batch
            if i < (100 // limit):
                assert len(res['files']) == limit
            # penultimate query batch
            elif i == (100 // limit):
                assert len(res['files']) == (100 % limit)
            # final query batch, AKA nothing more
            else:
                assert not res['files']
                break

            assert not any(f in received for f in res['files'])
            received.extend(res['files'])
        assert len(received) == 100

        # Some Error Cases
        for err in [{'start': -7}, {'limit': 0}, {'limit': -12}]:
            with self.assertRaises(requests.exceptions.HTTPError) as cm:
                r.request_seq('GET', '/api/files', err)
            _assert_httperror(cm.exception, 400, 'Invalid query parameter(s)')

    def test_50_post_files_unique_logical_name(self) -> None:
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
        uuid = url.split('/')[-1]

        # check that the file was created properly
        data = r.request_seq('GET', '/api/files')
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertTrue(any(uuid == f['uuid'] for f in data['files']))

        # create the file the second time; should NOT be OK
        with self.assertRaises(Exception) as cm:
            data = r.request_seq('POST', '/api/files', metadata)
        _assert_httperror(
            cm.exception,
            409,
            f"Conflict with existing file (logical_name already exists `{metadata['logical_name']}`)"
        )

        # check that the second file was not created
        data = r.request_seq('GET', '/api/files')
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertTrue(any(uuid == f['uuid'] for f in data['files']))

    def test_51_put_files_uuid_unique_logical_name(self) -> None:
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
        uuid = url.split('/')[-1]

        # create the second file; should be OK
        data = r.request_seq('POST', '/api/files', metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)

        # try to replace the first file with a copy of the second; should NOT be OK
        with self.assertRaises(Exception) as cm:
            r.request_seq('PUT', '/api/files/' + uuid, metadata2)
        _assert_httperror(
            cm.exception,
            409,
            f"Conflict with existing file (logical_name already exists `{metadata2['logical_name']}`)"
        )

    def test_52_put_files_uuid_replace_logical_name(self) -> None:
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
        uuid = url.split('/')[-1]

        # try to replace the first file with the second; should be OK
        data = r.request_seq('PUT', '/api/files/' + uuid, metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('logical_name', data)

    def test_53_patch_files_uuid_unique_logical_name(self) -> None:
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
        uuid = url.split('/')[-1]

        # create the second file; should be OK
        data = r.request_seq('POST', '/api/files', metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)

        # try to update the first file with a patch; should NOT be OK
        with self.assertRaises(Exception) as cm:
            r.request_seq('PATCH', '/api/files/' + uuid, patch1)
        _assert_httperror(
            cm.exception,
            409,
            f"Conflict with existing file (logical_name already exists `{metadata2['logical_name']}`)"
        )

    def test_54_patch_files_uuid_replace_logical_name(self) -> None:
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
        uuid = url.split('/')[-1]

        # try to update the file with a patch; should be OK
        data = r.request_seq('PATCH', '/api/files/' + uuid, patch1)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('logical_name', data)

    def test_55_post_files_unique_locations(self) -> None:
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
        uuid = url.split('/')[-1]

        # check that the file was created properly
        data = r.request_seq('GET', '/api/files')
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertTrue(any(uuid == f['uuid'] for f in data['files']))

        # create the second file; should NOT be OK
        with self.assertRaises(Exception) as cm:
            r.request_seq('POST', '/api/files', metadata2)
        _assert_httperror(
            cm.exception,
            409,
            f"Conflict with existing file (location already exists `{metadata['logical_name']}`)"
        )

    def test_56_put_files_uuid_unique_locations(self) -> None:
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
        uuid = url.split('/')[-1]

        # create the second file; should be OK
        data = r.request_seq('POST', '/api/files', metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)

        # try to replace the first file with a location collision with the second; should NOT be OK
        with self.assertRaises(Exception) as cm:
            r.request_seq('PUT', '/api/files/' + uuid, replace1)
        _assert_httperror(
            cm.exception,
            409,
            f"Conflict with existing file (location already exists `{metadata2['logical_name']}`)"
        )

    def test_57_put_files_uuid_replace_locations(self) -> None:
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
        uuid = url.split('/')[-1]

        # try to replace the first file with the second; should be OK
        data = r.request_seq('PUT', '/api/files/'+uuid, metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('logical_name', data)

    def test_58_patch_files_uuid_unique_locations(self) -> None:
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
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 2,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah2.dat'}]
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uuid = url.split('/')[-1]

        # create the second file; should be OK
        data = r.request_seq('POST', '/api/files', metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)

        # try to update the first file with a patch; should NOT be OK
        with self.assertRaises(Exception) as cm:
            r.request_seq('PATCH', '/api/files/' + uuid, patch1)
        _assert_httperror(
            cm.exception,
            409,
            f"Conflict with existing file (location already exists `{metadata2['logical_name']}`)"
        )

    def test_59_patch_files_uuid_replace_locations(self) -> None:
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
            'logical_name': '/blah/data/exp/IceCube/blah.dat',
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 2,
            u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
        }

        # create the file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uuid = url.split('/')[-1]

        # try to update the file with a patch; should be OK
        data = r.request_seq('PATCH', '/api/files/' + uuid, patch1)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('logical_name', data)
        self.assertIn('locations', data)

    def test_60_post_files_locations_1xN(self) -> None:
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
        uuid = url.split('/')[-1]

        # check that the file was created properly
        data = r.request_seq('GET', '/api/files')
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertTrue(any(uuid == f['uuid'] for f in data['files']))

        # create the second file; should NOT be OK
        with self.assertRaises(Exception) as cm:
            r.request_seq('POST', '/api/files', metadata2)
        _assert_httperror(
            cm.exception,
            409,
            "Conflict with existing file (location already exists `/data/test/exp/IceCube/foo.dat`)"
        )

    def test_61_post_files_locations_Nx1(self) -> None:
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
        uuid = url.split('/')[-1]

        # check that the file was created properly
        data = r.request_seq('GET', '/api/files')
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertTrue(any(uuid == f['uuid'] for f in data['files']))

        # check that the file was created properly, part deux
        data = r.request_seq('GET', '/api/files/' + uuid)

        # create the second file; should NOT be OK
        with self.assertRaises(Exception) as cm:
            r.request_seq('POST', '/api/files', metadata2)
        _assert_httperror(
            cm.exception,
            409,
            "Conflict with existing file (location already exists `/data/test/exp/IceCube/foo.dat`)"
        )

    def test_62_post_files_locations_NxN(self) -> None:
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
        uuid = url.split('/')[-1]

        # check that the file was created properly
        data = r.request_seq('GET', '/api/files')
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('files', data)
        self.assertEqual(len(data['files']), 1)
        self.assertTrue(any(uuid == f['uuid'] for f in data['files']))

        # check that the file was created properly, part deux
        data = r.request_seq('GET', '/api/files/' + uuid)

        # create the second file; should NOT be OK
        with self.assertRaises(Exception) as cm:
            r.request_seq('POST', '/api/files', metadata2)
        _assert_httperror(
            cm.exception,
            409,
            "Conflict with existing file (location already exists `/data/test/exp/IceCube/foo.dat`)"
        )

    def test_63_put_files_uuid_locations_1xN(self) -> None:
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
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 2,
            u'locations': locs3c
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uuid = url.split('/')[-1]

        # create the second file; should be OK
        data = r.request_seq('POST', '/api/files', metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)

        # try to replace the first file with a location collision with the second; should NOT be OK
        with self.assertRaises(Exception) as cm:
            r.request_seq('PUT', '/api/files/' + uuid, replace1)
        _assert_httperror(
            cm.exception,
            409,
            "Conflict with existing file (location already exists `/data/test/exp/IceCube/foo.dat`)"
        )

    def test_64_put_files_uuid_locations_Nx1(self) -> None:
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
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 2,
            u'locations': [loc1a]
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uuid = url.split('/')[-1]

        # create the second file; should be OK
        data = r.request_seq('POST', '/api/files', metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)

        # try to replace the first file with a location collision with the second; should NOT be OK
        with self.assertRaises(Exception) as cm:
            r.request_seq('PUT', '/api/files/' + uuid, replace1)
        _assert_httperror(
            cm.exception,
            409,
            "Conflict with existing file (location already exists `/data/test/exp/IceCube/foo.dat`)"
        )

    def test_65_put_files_uuid_locations_NxN(self) -> None:
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
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 2,
            u'locations': locs3b
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uuid = url.split('/')[-1]

        # create the second file; should be OK
        data = r.request_seq('POST', '/api/files', metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)

        # try to replace the first file with a location collision with the second; should NOT be OK
        with self.assertRaises(Exception) as cm:
            r.request_seq('PUT', '/api/files/' + uuid, replace1)
        _assert_httperror(
            cm.exception,
            409,
            "Conflict with existing file (location already exists `/data/test/exp/IceCube/foo.dat`)"
        )

    def test_66_patch_files_uuid_locations_1xN(self) -> None:
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
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 2,
            u'locations': locs3c
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uuid = url.split('/')[-1]

        # create the second file; should be OK
        data = r.request_seq('POST', '/api/files', metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)

        # try to update the first file with a patch; should NOT be OK
        with self.assertRaises(Exception) as cm:
            r.request_seq('PATCH', '/api/files/' + uuid, patch1)
        _assert_httperror(
            cm.exception,
            409,
            "Conflict with existing file (location already exists `/data/test/exp/IceCube/foo.dat`)"
        )

    def test_67_patch_files_uuid_locations_Nx1(self) -> None:
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
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 2,
            u'locations': [loc1c]
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uuid = url.split('/')[-1]

        # create the second file; should be OK
        data = r.request_seq('POST', '/api/files', metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)

        # try to update the first file with a patch; should NOT be OK
        with self.assertRaises(Exception) as cm:
            r.request_seq('PATCH', '/api/files/' + uuid, patch1)
        _assert_httperror(
            cm.exception,
            409,
            "Conflict with existing file (location already exists `/data/test/exp/IceCube/foo.dat`)"
        )

    def test_68_patch_files_uuid_locations_NxN(self) -> None:
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
            'checksum': {'sha512': hex('foo bar')},
            'file_size': 2,
            u'locations': locs3c
        }

        # create the first file; should be OK
        data = r.request_seq('POST', '/api/files', metadata)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)
        url = data['file']
        uuid = url.split('/')[-1]

        # create the second file; should be OK
        data = r.request_seq('POST', '/api/files', metadata2)
        self.assertIn('_links', data)
        self.assertIn('self', data['_links'])
        self.assertIn('file', data)

        # try to update the first file with a patch; should NOT be OK
        with self.assertRaises(Exception) as cm:
            r.request_seq('PATCH', '/api/files/' + uuid, patch1)
        _assert_httperror(
            cm.exception,
            409,
            "Conflict with existing file (location already exists `/data/test/exp/IceCube/foo.dat`)"
        )

    def test_70_abuse_post_files_locations(self) -> None:
        """Abuse the POST /api/files/UUID/locations route to test error
        handling."""
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
        with self.assertRaises(Exception) as cm:
            r.request_seq('POST', '/api/files/bobsyeruncle/locations', valid_post_body)
        _assert_httperror(cm.exception, 404, "File uuid not found")

        # try to POST to an non-existant UUID
        with self.assertRaises(Exception) as cm:
            r.request_seq('POST', '/api/files/6e4ec06d-8e22-4a2b-a392-f4492fb25eb1/locations', valid_post_body)
        _assert_httperror(cm.exception, 404, "File uuid not found")

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
        uuid = url.split('/')[-1]

        # try to POST to the file without a post body
        with self.assertRaises(Exception) as cm:
            r.request_seq('POST', '/api/files/' + uuid + '/locations', {})
        _assert_httperror(cm.exception, 400, "POST body requires 'locations' field")

        # try to POST to the file with a non-array locations
        with self.assertRaises(Exception) as cm:
            r.request_seq('POST', '/api/files/' + uuid + '/locations', {"locations": "bobsyeruncle"})
        _assert_httperror(cm.exception, 400, "Validation Error: member `locations` must be a list with 1+ entries, each with keys: ['site', 'path']")

    def test_71_post_files_locations_duplicate(self) -> None:
        """Test that POST /api/files/UUID/locations is a no-op for non-distinct
        locations."""
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
        uuid = url.split('/')[-1]

        # read the full record of the file; should be OK
        rec = r.request_seq('GET', '/api/files/' + uuid)
        self.assertEqual(4, len(rec["locations"]))
        self.assertIn('meta_modify_date', rec)
        mmd = rec['meta_modify_date']

        # try to POST existant locations to the file; should be OK
        not_so_new_locations = {"locations": [loc1b, loc1d]}
        rec2 = r.request_seq('POST', '/api/files/' + uuid + '/locations', not_so_new_locations)

        # ensure the record is the same (not updated)
        self.assertEqual(4, len(rec2["locations"]))
        self.assertListEqual(rec["locations"], rec2["locations"])
        self.assertEqual(mmd, rec2["meta_modify_date"])

    def test_72_post_files_locations_conflict(self) -> None:
        """Test that POST /api/files/UUID/locations returns an error on
        conflicting duplicate locations."""
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
        uuid = url.split('/')[-1]

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
        uuid2 = url.split('/')[-1]

        # try to POST a second file location to the first file
        with self.assertRaises(Exception) as cm:
            conflicting_locations = {"locations": [loc1d]}
            rec2 = r.request_seq('POST', '/api/files/' + uuid + '/locations', conflicting_locations)
        _assert_httperror(
            cm.exception,
            409,
            "Conflict with existing file (location already exists `/data/test/exp/IceCube/foo.dat`)"
        )

    def test_73_post_files_locations(self) -> None:
        """Test that POST /api/files/UUID/locations can add distinct non-
        conflicting locations."""
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
        uuid = url.split('/')[-1]

        # read the full record of the file; should be OK
        rec = r.request_seq('GET', '/api/files/' + uuid)
        self.assertEqual(2, len(rec["locations"]))
        self.assertIn('meta_modify_date', rec)
        mmd = rec['meta_modify_date']

        # try to POST existant locations to the file; should be OK
        new_locations = {"locations": [loc1c, loc1d]}
        rec2 = r.request_seq('POST', '/api/files/' + uuid + '/locations', new_locations)

        # ensure the record has changed (is updated)
        self.assertEqual(4, len(rec2["locations"]))
        self.assertNotEqual(mmd, rec2["meta_modify_date"])
        self.assertIn(loc1a, rec2["locations"])
        self.assertIn(loc1b, rec2["locations"])
        self.assertIn(loc1c, rec2["locations"])
        self.assertIn(loc1d, rec2["locations"])

    def test_74_post_files_locations_just_one(self) -> None:
        """Test that POST /api/files/UUID/locations can add distinct non-
        conflicting locations."""
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
        uuid = url.split('/')[-1]

        # read the full record of the file; should be OK
        rec = r.request_seq('GET', '/api/files/' + uuid)
        self.assertEqual(1, len(rec["locations"]))
        self.assertIn('meta_modify_date', rec)
        mmd = rec['meta_modify_date']

        # try to POST existant locations to the file; should be OK
        new_locations = {"locations": [loc1c]}
        rec2 = r.request_seq('POST', '/api/files/' + uuid + '/locations', new_locations)

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
