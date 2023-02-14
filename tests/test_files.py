# test_files.py
"""Test /api/files."""

# fmt:off
# pylint: skip-file

import copy
import hashlib
import itertools
import logging
from typing import Any, cast, Dict, List, Optional, Tuple, Union

from file_catalog.schema import types
import pytest
import requests
from rest_tools.client import RestClient
from tornado.escape import json_encode

logger = logging.getLogger(__name__)

StrDict = Dict[str, Any]


def hex(data: Any) -> str:
    """Get sha512."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha512(data).hexdigest()


def copy_without_rest_response_keys(data: StrDict) -> StrDict:
    """Return copy of `data` without the keys inserted by REST in a response."""
    response_keys = ['_links', 'meta_modify_date', 'uuid']
    return {k: v for k, v in copy.deepcopy(data).items() if k not in response_keys}


# -----------------------------------------------------------------------------


def _assert_httperror(exception: Exception, code: int, reason: str) -> None:
    """Assert that this is the expected HTTPError."""
    print(exception)
    assert isinstance(exception, requests.exceptions.HTTPError)
    assert exception.response.status_code == code
    assert exception.response.reason == reason


async def _assert_in_fc(rest: RestClient,
                        uuids: Union[str, List[str]],
                        all_keys: bool = False) -> StrDict:
    """Also return data."""
    if isinstance(uuids, str):
        uuids = [uuids]

    if all_keys:
        data = await rest.request('GET', '/api/files', {'all-keys': True})
    else:
        data = await rest.request('GET', '/api/files')

    assert '_links' in data
    assert 'self' in data['_links']
    assert 'files' in data

    assert len(data['files']) == len(uuids)
    for f in data['files']:
        assert f['uuid'] in uuids

    return data  # type: ignore[no-any-return]


async def _patch_and_assert(rest: RestClient, patch: StrDict, uuid: str) -> StrDict:
    """Also return data."""
    data = await rest.request('PATCH', '/api/files/' + uuid, patch)
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'logical_name' in data
    return data  # type: ignore[no-any-return]


async def _post_and_assert(rest: RestClient, metadata: StrDict) -> Tuple[StrDict, str, str]:
    """Also return (data, url, uuid)."""
    data = await rest.request('POST', '/api/files', metadata)
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'file' in data
    url = data['file']
    uuid = url.split('/')[-1]
    return data, url, uuid


async def _put_and_assert(rest: RestClient, metadata: StrDict, uuid: str) -> StrDict:
    """Also return data."""
    data = await rest.request('PUT', '/api/files/' + uuid, metadata)
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'logical_name' in data
    return data  # type: ignore[no-any-return]


# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_10_files(rest: RestClient) -> None:
    """Test POST then GET."""
    metadata = {
        'logical_name': 'blah',
        'checksum': {'sha512': hex('foo bar')},
        'file_size': 1,
        u'locations': [{u'site': u'test', u'path': u'blah.dat'}]
    }
    data, url, uuid = await _post_and_assert(rest, metadata)

    data = await _assert_in_fc(rest, uuid)  # noqa: F841
    for m in ('PUT', 'DELETE', 'PATCH'):
        with pytest.raises(Exception) as cm:
            await rest.request(m, '/api/files')
        _assert_httperror(cm.value, 405, "Method Not Allowed")


@pytest.mark.asyncio
async def test_11_files_count(rest: RestClient) -> None:
    """Test /api/files/count."""
    metadata = {
        'logical_name': 'blah',
        'checksum': {'sha512': hex('foo bar')},
        'file_size': 1,
        u'locations': [{u'site': u'test', u'path': u'blah.dat'}]
    }
    data, url, uuid = await _post_and_assert(rest, metadata)

    data = await rest.request('GET', '/api/files/count')
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'files' in data
    assert data['files'] == 1


@pytest.mark.asyncio
async def test_12_files_keys(rest: RestClient) -> None:
    """Test the 'keys' and all-keys' arguments."""
    metadata = {
        "logical_name": "blah",
        "checksum": {"sha512": hex("foo bar")},
        "file_size": 1,
        "locations": [{"site": "test", "path": "blah.dat"}],
        "extra": "foo",
        "supplemental": ["green", "eggs", "ham"],
    }
    data = await rest.request("POST", "/api/files", metadata)
    assert "_links" in data
    assert "self" in data["_links"]
    assert "file" in data
    assert "extra" not in data
    assert "supplemental" not in data
    url = data["file"]
    uuid = url.split("/")[-1]  # noqa: F841

    # w/o all-keys
    data = await rest.request("GET", "/api/files")
    assert set(data["files"][0].keys()) == {"logical_name", "uuid"}

    # w/ all-keys
    args: Dict[str, Any] = {"all-keys": True}
    data = await rest.request("GET", "/api/files", args)
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
    data = await rest.request("GET", "/api/files", args)
    assert set(data["files"][0].keys()) == {"logical_name", "uuid"}

    # w/ all-keys & keys
    args = {"all-keys": True, "keys": "checksum|file_size"}
    data = await rest.request("GET", "/api/files", args)
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
    data = await rest.request("GET", "/api/files", args)
    assert set(data["files"][0].keys()) == {"checksum", "file_size"}

    # w/ just keys
    args = {"keys": "checksum|file_size"}
    data = await rest.request("GET", "/api/files", args)
    assert set(data["files"][0].keys()) == {"checksum", "file_size"}


@pytest.mark.asyncio
async def test_13_files__name_type_args(rest: RestClient) -> None:
    """Test the name-type base/shortcut arguments.

    "logical_name", "directory", "filename", & "logical-name-regex".
    """
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
        await rest.request("POST", "/api/files", meta)

    async def get_logical_names(args: Optional[Dict[str, str]] = None) -> List[str]:
        if not args:
            args = {}
        ret = await rest.request("GET", "/api/files", args)
        print(ret)
        return [f["logical_name"] for f in ret["files"]]

    assert len(await get_logical_names()) == 4
    # logical_name
    assert len(await get_logical_names({"logical_name": "/foo/bar/ham.txt"})) == 1
    # directory
    paths = await get_logical_names({"directory": "/foo/bar"})
    assert set(paths) == {"/foo/bar/ham.txt", "/foo/bar/baz/bat.txt"}
    assert len(await get_logical_names({"directory": "/fo"})) == 0
    # filename
    paths = await get_logical_names({"filename": "ham.txt"})
    assert set(paths) == {
        "/foo/bar/ham.txt",
        "/green/eggs/and/ham.txt",
        "/john/paul/george/ringo/ham.txt",
    }
    assert len(await get_logical_names({"filename": ".txt"})) == 0
    # directory & filename
    paths = await get_logical_names({"directory": "/foo", "filename": "ham.txt"})
    assert paths == ["/foo/bar/ham.txt"]
    # logical-name-regex
    paths = await get_logical_names({"logical-name-regex": r".*george/ringo.*"})
    assert paths == ["/john/paul/george/ringo/ham.txt"]
    assert len(await get_logical_names({"logical-name-regex": r".*"})) == 4


@pytest.mark.asyncio
async def test_20_files(rest: RestClient) -> None:
    """Test POST, GET, PUT, PATCH, and DELETE."""
    metadata = {
        u'logical_name': u'blah',
        u'checksum': {u'sha512': hex('foo bar')},
        u'file_size': 1,
        u'locations': [{u'site': u'test', u'path': u'blah.dat'}]
    }
    data = await rest.request('POST', '/api/files', metadata)

    logger.error(f"data: {data}")

    url = data['file']

    data = await rest.request('GET', url)
    data.pop('_links')
    data.pop('meta_modify_date')
    data.pop('uuid')
    assert metadata == data

    metadata['test'] = 100

    metadata_cpy = copy.deepcopy(metadata)
    metadata_cpy['uuid'] = 'something else'
    data = await rest.request('PUT', url, metadata_cpy)
    assert data['uuid'] != metadata_cpy['uuid']
    assert data['uuid'] == url.split('/')[-1]

    data = await rest.request('PUT', url, metadata)
    data.pop('_links')
    data.pop('meta_modify_date')
    data.pop('uuid')
    assert metadata == data

    data = await rest.request('GET', url)
    data.pop('_links')
    data.pop('meta_modify_date')
    data.pop('uuid')
    assert metadata == data

    metadata['test2'] = 200
    data = await rest.request('PATCH', url, {'test2': 200})
    data.pop('_links')
    data.pop('meta_modify_date')
    data.pop('uuid')
    assert metadata == data

    data = await rest.request('GET', url)
    data.pop('_links')
    data.pop('meta_modify_date')
    data.pop('uuid')
    assert metadata == data

    data = await rest.request('DELETE', url)

    # second delete should raise error
    with pytest.raises(Exception) as cm:
        data = await rest.request('DELETE', url)
    _assert_httperror(cm.value, 404, "File uuid not found")

    with pytest.raises(Exception) as cm:
        data = await rest.request('POST', url)
    _assert_httperror(cm.value, 405, "Method Not Allowed")


@pytest.mark.asyncio
async def test_21_files__404(rest: RestClient) -> None:
    """Test 404 (File Not Found) cases for GET, PUT, PATCH, and DELETE."""
    # Start by putting something in the FC
    metadata: types.Metadata = {
        u'logical_name': u'blah',
        u'checksum': {u'sha512': hex('foo bar')},
        u'file_size': 1,
        u'locations': [{u'site': u'test', u'path': u'blah.dat'}]
    }
    await rest.request('POST', '/api/files', cast(Dict[str, Any], metadata))

    metadata_404 = copy.deepcopy(metadata)
    metadata_404['uuid'] = 'n0t-a-R3al-Uu1d'

    # GET
    with pytest.raises(Exception) as cm:
        await rest.request('GET', '/api/files/' + metadata_404['uuid'])
    _assert_httperror(cm.value, 404, "File uuid not found")

    # PUT
    with pytest.raises(Exception) as cm:
        await rest.request('PUT', '/api/files/' + metadata_404['uuid'], cast(Dict[str, Any], metadata_404))
    _assert_httperror(cm.value, 404, "File uuid not found")

    # PATCH
    with pytest.raises(Exception) as cm:
        await rest.request('PATCH', '/api/files/' + metadata_404['uuid'], cast(Dict[str, Any], metadata_404))
    _assert_httperror(cm.value, 404, "File uuid not found")

    # DELETE
    with pytest.raises(Exception) as cm:
        await rest.request('DELETE', '/api/files/' + metadata_404['uuid'])
    _assert_httperror(cm.value, 404, "File uuid not found")


@pytest.mark.asyncio
async def test_30_files__archive(rest: RestClient) -> None:
    """Test GET w/ query arg: `locations.archive`."""
    metadata = {
        u'logical_name': u'blah',
        u'checksum': {u'sha512': hex('foo bar')},
        u'file_size': 1,
        u'locations': [{u'site': u'test', u'path': u'blah.dat'}]
    }
    metadata2 = {
        u'logical_name': u'blah2',
        u'checksum': {u'sha512': hex('foo bar baz')},
        u'file_size': 2,
        u'locations': [{u'site': u'test', u'path': u'blah.dat', u'archive': True}]
    }
    data = await rest.request('POST', '/api/files', metadata)
    url = data['file']
    uuid = url.split('/')[-1]
    data = await rest.request('POST', '/api/files', metadata2)
    url2 = data['file']
    uuid2 = url2.split('/')[-1]

    data = await _assert_in_fc(rest, uuid)
    assert not any(uuid2 == f['uuid'] for f in data['files'])

    data = await rest.request('GET', '/api/files', {'query': json_encode({'locations.archive': True})})
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'files' in data
    assert len(data['files']) == 1
    assert not any(uuid == f['uuid'] for f in data['files'])
    assert any(uuid2 == f['uuid'] for f in data['files'])

    data = await rest.request('GET', '/api/files', {'query': json_encode({'locations.archive': False})})
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'files' in data
    assert len(data['files']) == 0

    metadata3 = {
        u'logical_name': u'blah3',
        u'checksum': {u'sha512': hex('1234')},
        u'file_size': 3,
        u'locations': [{u'site': u'test', u'path': u'blah.dat', u'archive': False}]
    }
    data = await rest.request('POST', '/api/files', metadata3)
    url3 = data['file']
    uid3 = url3.split('/')[-1]

    data = await rest.request('GET', '/api/files', {'query': json_encode({'locations.archive': False})})
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'files' in data
    assert len(data['files']) == 1
    assert not any(uuid == f['uuid'] for f in data['files'])
    assert not any(uuid2 == f['uuid'] for f in data['files'])
    assert any(uid3 == f['uuid'] for f in data['files'])


@pytest.mark.asyncio
async def test_40_files__simple_query(rest: RestClient) -> None:
    """Test GET w/ shortcut-metadata query args."""
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
    data = await rest.request('POST', '/api/files', metadata)
    url = data['file']
    uuid = url.split('/')[-1]
    data = await rest.request('POST', '/api/files', metadata2)
    url2 = data['file']
    uuid2 = url2.split('/')[-1]

    data = await rest.request('GET', '/api/files')
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'files' in data
    assert len(data['files']) == 2
    assert any(uuid == f['uuid'] for f in data['files'])
    assert any(uuid2 == f['uuid'] for f in data['files'])

    data = await rest.request('GET', '/api/files', {'processing_level': 'level2'})
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'files' in data
    assert len(data['files']) == 2
    assert any(uuid == f['uuid'] for f in data['files'])
    assert any(uuid2 == f['uuid'] for f in data['files'])

    data = await rest.request('GET', '/api/files', {'run_number': 12345})
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'files' in data
    assert len(data['files']) == 1
    assert any(uuid == f['uuid'] for f in data['files'])
    assert not any(uuid2 == f['uuid'] for f in data['files'])

    data = await rest.request('GET', '/api/files', {'dataset': 23454})
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'files' in data
    assert len(data['files']) == 1
    assert not any(uuid == f['uuid'] for f in data['files'])
    assert any(uuid2 == f['uuid'] for f in data['files'])

    data = await rest.request('GET', '/api/files', {'event_id': 400})
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'files' in data
    assert len(data['files']) == 1
    assert any(uuid == f['uuid'] for f in data['files'])
    assert not any(uuid2 == f['uuid'] for f in data['files'])

    data = await rest.request('GET', '/api/files', {'season': 2017})
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'files' in data
    assert len(data['files']) == 2
    assert any(uuid == f['uuid'] for f in data['files'])
    assert any(uuid2 == f['uuid'] for f in data['files'])

    data = await rest.request('GET', '/api/files', {'event_id': 400, 'keys': '|'.join(['checksum', 'file_size', 'uuid'])})
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'files' in data
    assert len(data['files']) == 1
    assert any(uuid == f['uuid'] for f in data['files'])
    assert not any(uuid2 == f['uuid'] for f in data['files'])
    assert 'checksum' in data['files'][0]
    assert 'file_size' in data['files'][0]


@pytest.mark.asyncio
async def test_41_files__simple_query(rest: RestClient) -> None:
    """Test the limit and start parameters."""
    # Populate FC
    for i in range(100):
        metadata = {
            'logical_name': f'/foo/bar/{i}.dat',
            'checksum': {'sha512': hex(f'foo bar {i}')},
            'file_size': 3 * i,
            u'locations': [{u'site': u'WIPAC', u'path': f'/foo/bar/{i}.dat'}]
        }
        await rest.request('POST', '/api/files', metadata)

    # Some Legal Corner Cases
    assert len((await rest.request('GET', '/api/files'))['files']) == 100
    assert len((await rest.request('GET', '/api/files', {'limit': 300}))['files']) == 100
    assert not (await rest.request('GET', '/api/files', {'start': 300}))['files']
    assert len((await rest.request('GET', '/api/files', {'start': 99}))['files']) == 1
    assert len((await rest.request('GET', '/api/files', {'start': None}))['files']) == 100
    assert len((await rest.request('GET', '/api/files', {'limit': None}))['files']) == 100
    assert len((await rest.request('GET', '/api/files', {'limit': 1_000_000}))['files']) == 100

    # Normal Usage
    limit = 3
    received = []
    for i in itertools.count():
        start = i * limit
        # print(f"{i=} {start=} {limit=}")
        res = await rest.request('GET', '/api/files', {'start': start, 'limit': limit})

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
        with pytest.raises(requests.exceptions.HTTPError) as cm:
            await rest.request('GET', '/api/files', err)
        _assert_httperror(cm.value, 400, 'Invalid query parameter(s)')


@pytest.mark.asyncio
async def test_50a_post_files__conflicting_file_version__error(rest: RestClient) -> None:
    """Test that file-version (logical_name+checksum.sha512) is unique for creating a new file.

    If there's a conflict, there should be an error.
    """
    # define the file to be created
    metadata1 = {
        'logical_name': '/blah/data/exp/IceCube/blah.dat',
        'checksum': {'sha512': hex('foo bar')},
        'file_size': 1,
        u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
    }

    # create the file the first time; should be OK
    data, url, uuid = await _post_and_assert(rest, metadata1)

    # check that the file was created properly
    data = await _assert_in_fc(rest, uuid)

    # create the file the second time; should NOT be OK
    with pytest.raises(Exception) as cm:
        data = await rest.request('POST', '/api/files', metadata1)
    _assert_httperror(
        cm.value,
        409,
        f"Conflict with existing file-version ('logical_name' + 'checksum.sha512' already exists:"  # type: ignore[index]
        f"`{metadata1['logical_name']}` + `{metadata1['checksum']['sha512']}`)"
    )

    # check that the second file was not created
    data = await _assert_in_fc(rest, uuid)  # noqa: F841


@pytest.mark.asyncio
async def test_51a_post_files__unique_file_version__okay(rest: RestClient) -> None:
    """Test that file-version (logical_name+checksum.sha512) is unique when creating a new file.

    But a metadata with the same logical_name and different checksum
    (or visa-versa) is okay.
    """
    # define the file to be created
    logical_name = '/blah/data/exp/IceCube/blah.dat'
    checksum = {'sha512': hex('foo bar')}
    metadata1 = {
        'logical_name': logical_name,
        'checksum': checksum,
        'file_size': 1,
        u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
    }
    metadata_same_logical_name = {
        'logical_name': logical_name,
        'checksum': {'sha512': hex('foo bar baz boink')},
        'file_size': 1,
        u'locations': [{u'site': u'NORTH-POLE', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
    }
    metadata_same_checksum = {
        'logical_name': logical_name + '!!!',
        'checksum': checksum,
        'file_size': 1,
        u'locations': [{u'site': u'SOUTH-POLE', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
    }

    data, url, uuid1 = await _post_and_assert(rest, metadata1)
    data = await _assert_in_fc(rest, uuid1)

    data, url, uuid2 = await _post_and_assert(rest, metadata_same_logical_name)
    data = await _assert_in_fc(rest, [uuid1, uuid2])

    data, url, uuid3 = await _post_and_assert(rest, metadata_same_checksum)
    data = await _assert_in_fc(rest, [uuid1, uuid2, uuid3])  # noqa: F841


@pytest.mark.asyncio
async def test_52a_put_files_uuid__immutable_file_version__error(rest: RestClient) -> None:
    """Test that file-version (logical_name+checksum.sha512) cannot be changed."""
    metadata = {
        'logical_name': '/blah/data/exp/IceCube/blah.dat',
        'checksum': {'sha512': hex('foo bar')},
        'file_size': 1,
        u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
    }

    # create the first file; should be OK
    data, url, uuid = await _post_and_assert(rest, metadata)

    # try to change 'logical_name'
    metadata_diff_logical_name = copy.deepcopy(metadata)
    metadata_diff_logical_name['logical_name'] = '/this/shall/not/pass'
    with pytest.raises(Exception) as cm:
        await rest.request('PUT', '/api/files/' + uuid, metadata_diff_logical_name)
    _assert_httperror(
        cm.value,
        400,
        "Validation Error: forbidden field modification 'logical_name'"
    )

    # try to change 'checksum.sha512'
    metadata_diff_checksum = copy.deepcopy(metadata)
    metadata_diff_checksum['checksum'] = {'sha512': hex('baz baz baz')}
    with pytest.raises(Exception) as cm:
        await rest.request('PUT', '/api/files/' + uuid, metadata_diff_checksum)
    _assert_httperror(
        cm.value,
        400,
        "Validation Error: forbidden field modification 'checksum.sha512'"
    )

    # try to change 'checksum' to another non-sha512 checksum
    metadata_only_nonsha512 = copy.deepcopy(metadata)
    metadata_only_nonsha512['checksum'] = {'abc123': hex('yoink')}
    with pytest.raises(Exception) as cm:
        await rest.request('PUT', '/api/files/' + uuid, metadata_only_nonsha512)
    _assert_httperror(
        cm.value,
        400,
        "Validation Error: metadata missing mandatory field `checksum.sha512` "
        "(mandatory fields: uuid, logical_name, locations, file_size, checksum.sha512)"
    )


@pytest.mark.asyncio
async def test_52b_put_files_uuid__without_file_version__error(rest: RestClient) -> None:
    """Test that a file cannot be replaced if it does not have a file-version.

    In contrast, this scenario would be okay (normal) for PATCH.
    """
    metadata = {
        'logical_name': '/blah/data/exp/IceCube/blah.dat',
        'checksum': {'sha512': hex('foo bar')},
        'file_size': 1,
        u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
    }

    # create the first file; should be OK
    data, url, uuid = await _post_and_assert(rest, metadata)

    # try replace without a 'logical_name'
    metadata_no_logical_name = copy.deepcopy(metadata)
    metadata_no_logical_name.pop('logical_name')
    with pytest.raises(Exception) as cm:
        await rest.request('PUT', '/api/files/' + uuid, metadata_no_logical_name)
    _assert_httperror(
        cm.value,
        400,
        "Validation Error: metadata missing mandatory field `logical_name` (mandatory fields: uuid, logical_name, locations, file_size, checksum.sha512)"
    )

    # try replace without a 'checksum.sha512'
    metadata_no_checksum_sha512 = copy.deepcopy(metadata)
    metadata_no_checksum_sha512['checksum'].pop('sha512')  # type: ignore[attr-defined]
    with pytest.raises(Exception) as cm:
        await rest.request('PUT', '/api/files/' + uuid, metadata_no_checksum_sha512)
    _assert_httperror(
        cm.value,
        400,
        "Validation Error: metadata missing mandatory field `checksum.sha512` (mandatory fields: uuid, logical_name, locations, file_size, checksum.sha512)"
    )

    # try replace without a 'checksum.sha512' but with another checksum
    metadata_only_a_nonsha512_checksum = copy.deepcopy(metadata)
    metadata_only_a_nonsha512_checksum['checksum'].pop('sha512')  # type: ignore[attr-defined]
    metadata_only_a_nonsha512_checksum['checksum']['abc123'] = hex('scoop')  # type: ignore[index]
    with pytest.raises(Exception) as cm:
        await rest.request('PUT', '/api/files/' + uuid, metadata_only_a_nonsha512_checksum)
    _assert_httperror(
        cm.value,
        400,
        "Validation Error: metadata missing mandatory field `checksum.sha512` (mandatory fields: uuid, logical_name, locations, file_size, checksum.sha512)"
    )


@pytest.mark.asyncio
async def test_53a_put_files_uuid__with_file_version__okay(rest: RestClient) -> None:
    """Test that a file can replaced with the same file-version."""
    # define the files to be created
    metadata = {
        'logical_name': '/blah/data/exp/IceCube/blah.dat',
        'checksum': {'sha512': hex('foo bar')},
        'file_size': 1,
        u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
    }

    # create the first file; should be OK
    data, url, uuid = await _post_and_assert(rest, metadata)

    # try to replace the first file with the second; should be OK
    metadata2 = copy.deepcopy(metadata)
    metadata2['file_size'] = 200
    data = await _put_and_assert(rest, metadata2, uuid)  # noqa: F841


@pytest.mark.asyncio
async def test_53b_put_files_uuid__with_addl_checksum_algos__okay(rest: RestClient) -> None:
    """Check that PUT still work when there's also non-sha512 checksums."""
    # define the files to be created
    checksum_w_sha512 = {'sha512': hex('foo bar')}
    metadata = {
        'logical_name': '/blah/data/exp/IceCube/blah.dat',
        'checksum': checksum_w_sha512,
        'file_size': 1,
        u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
    }

    # create the first file; should be OK
    data, url, uuid = await _post_and_assert(rest, metadata)

    # try to replace the first file with the second; should be OK
    metadata_with_addl_nonsha512 = copy.deepcopy(metadata)
    metadata_with_addl_nonsha512['checksum'].update({'abc123': hex('scoop')})  # type: ignore[attr-defined]
    data = await _put_and_assert(rest, metadata_with_addl_nonsha512, uuid)
    data = await _assert_in_fc(rest, uuid)  # noqa: F841


@pytest.mark.asyncio
async def test_54a_patch_files_uuid__immutable_file_version__error(rest: RestClient) -> None:
    """Test that file-version (logical_name+checksum.sha512) cannot be changed."""
    checksum = {'sha512': hex('foo bar')}
    metadata = {
        'logical_name': '/blah/data/exp/IceCube/blah.dat',
        'checksum': checksum,
        'file_size': 1,
        u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
    }

    # create the first file; should be OK
    data, url, uuid = await _post_and_assert(rest, metadata)

    # try to change 'logical_name'
    patch_logical_name = {'logical_name': '/this/shall/not/pass'}
    with pytest.raises(Exception) as cm:
        await rest.request('PATCH', '/api/files/' + uuid, patch_logical_name)
    _assert_httperror(
        cm.value,
        400,
        "Validation Error: forbidden field modification 'logical_name'"
    )

    # try to change 'checksum.sha512'
    patch_checksums = [
        {"checksum": {"sha512": hex("baz baz baz")}},
        {"checksum": {"sha512": hex("baz baz baz"), "abc123": hex("yoink")}},
    ]
    for pc in patch_checksums:
        with pytest.raises(Exception) as cm:
            await rest.request('PATCH', '/api/files/' + uuid, pc)
        _assert_httperror(
            cm.value,
            400,
            "Validation Error: forbidden field modification 'checksum.sha512'"
        )

    # try to change 'checksum' to another non-sha512 checksum
    patch_checksum_only_nonsha512 = {'checksum': {'abc123': hex('yoink')}}
    with pytest.raises(Exception) as cm:
        await rest.request('PATCH', '/api/files/' + uuid, patch_checksum_only_nonsha512)
    _assert_httperror(
        cm.value,
        400,
        "Validation Error: metadata missing mandatory field `checksum.sha512` "
        "(mandatory fields: uuid, logical_name, locations, file_size, checksum.sha512)"
    )


@pytest.mark.asyncio
async def test_55a_patch_files_uuid__with_file_version__okay(rest: RestClient) -> None:
    """Test that a file can replaced with the same file-version."""
    # define the files to be created
    metadata = {
        'logical_name': '/blah/data/exp/IceCube/blah.dat',
        'checksum': {'sha512': hex('foo bar')},
        'file_size': 1,
        u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
    }

    # create the first file; should be OK
    data, url, uuid = await _post_and_assert(rest, metadata)

    # try to replace with full file-version; should be OK
    patch_w_file_version = copy.deepcopy(metadata)
    patch_w_file_version['file_size'] = 200
    patch_w_file_version.pop(u'locations')
    data = await _patch_and_assert(rest, patch_w_file_version, uuid)

    # try to replace with full file-version w/o checksum; should be OK
    patch_w_file_version_wo_checksum = copy.deepcopy(metadata)
    patch_w_file_version_wo_checksum['file_size'] = 20000
    patch_w_file_version_wo_checksum.pop(u'locations')
    patch_w_file_version_wo_checksum.pop(u'checksum')
    data = await _patch_and_assert(rest, patch_w_file_version_wo_checksum, uuid)

    # try to replace with full file-version w/o logical_name; should be OK
    patch_w_file_version_wo_logical_name = copy.deepcopy(metadata)
    patch_w_file_version_wo_logical_name['file_size'] = 20000000
    patch_w_file_version_wo_logical_name.pop(u'locations')
    patch_w_file_version_wo_logical_name.pop(u'logical_name')
    data = await _patch_and_assert(rest, patch_w_file_version_wo_logical_name, uuid)  # noqa: F841


@pytest.mark.asyncio
async def test_55b_patch_files_uuid__with_addl_checksum_algos__okay(rest: RestClient) -> None:
    """Check that PATCH still works when there's also non-sha512 checksums."""
    # define the files to be created
    checksum_w_sha512 = {'sha512': hex('foo bar')}
    metadata = {
        'logical_name': '/blah/data/exp/IceCube/blah.dat',
        'checksum': checksum_w_sha512,
        'file_size': 1,
        u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
    }

    # create the first file; should be OK
    data, url, uuid = await _post_and_assert(rest, metadata)

    # try to patch; should be OK
    patch_with_addl_nonsha512 = {'checksum': {'abc123': hex('scoop')}}
    patch_with_addl_nonsha512['checksum'].update(checksum_w_sha512)
    data = await _patch_and_assert(rest, patch_with_addl_nonsha512, uuid)
    data = await _assert_in_fc(rest, uuid)  # noqa: F841


@pytest.mark.asyncio
async def test_55c_patch_files_uuid__without_file_version__okay(rest: RestClient) -> None:
    """Test that a file can be updated if it has a file-version."""
    # define the files to be created
    metadata = {
        'logical_name': '/blah/data/exp/IceCube/blah.dat',
        'checksum': {'sha512': hex('foo bar')},
        'file_size': 1,
        u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
    }

    # create the first file; should be OK
    data, url, uuid = await _post_and_assert(rest, metadata)

    # try to replace the first file with the second; should be OK
    patch_file_size = {'file_size': 200}
    data = await _patch_and_assert(rest, patch_file_size, uuid)  # noqa: F841


@pytest.mark.asyncio
async def test_60a_put_files_uuid__replace_locations__okay(rest: RestClient) -> None:
    """Test that a file can replace with the same location."""
    # define the files to be created
    metadata = {
        'logical_name': '/blah/data/exp/IceCube/blah.dat',
        'checksum': {'sha512': hex('foo bar')},
        'file_size': 1,
        u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
    }
    metadata2 = {
        'logical_name': '/blah/data/exp/IceCube/blah.dat',
        'checksum': {'sha512': hex('foo bar')},
        'file_size': 2,
        u'locations': [{u'site': u'WIPAC', u'path': u'/blah/data/exp/IceCube/blah.dat'}]
    }

    # create the first file; should be OK
    data, url, uuid = await _post_and_assert(rest, metadata)

    # try to replace the first file with the second; should be OK
    data = await _put_and_assert(rest, metadata2, uuid)  # noqa: F841


@pytest.mark.asyncio
async def test_61a_patch_files_uuid__replace_locations__okay(rest: RestClient) -> None:
    """Test that a file can be updated with the same location."""
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
    data, url, uuid = await _post_and_assert(rest, metadata)

    # try to update the file with a patch; should be OK
    data = await _patch_and_assert(rest, patch1, uuid)
    assert 'locations' in data


@pytest.mark.asyncio
async def test_62a_post_files__locations_1xN__error(rest: RestClient) -> None:
    """Test locations uniqueness under 1xN multiplicity."""
    # define the locations to be tested
    loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
    locs3a = [loc1a, loc1b, loc1c]
    locs3b = [loc1b, loc1c, loc1d]  # noqa: F841
    locs3c = [loc1a, loc1b, loc1d]  # noqa: F841

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
    data, url, uuid = await _post_and_assert(rest, metadata)

    # check that the file was created properly
    data = await _assert_in_fc(rest, uuid)  # noqa: F841

    # create the second file; should NOT be OK
    with pytest.raises(Exception) as cm:
        await rest.request('POST', '/api/files', metadata2)
    _assert_httperror(
        cm.value,
        409,
        "Conflict with existing file (location already exists `/data/test/exp/IceCube/foo.dat`)"
    )


@pytest.mark.asyncio
async def test_62b_post_files__locations_Nx1__error(rest: RestClient) -> None:
    """Test locations uniqueness under Nx1 multiplicity."""
    # define the locations to be tested
    loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
    locs3a = [loc1a, loc1b, loc1c]  # noqa: F841
    locs3b = [loc1b, loc1c, loc1d]
    locs3c = [loc1a, loc1b, loc1d]  # noqa: F841

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
    data, url, uuid = await _post_and_assert(rest, metadata)

    # check that the file was created properly
    data = await _assert_in_fc(rest, uuid)

    # check that the file was created properly, part deux
    data = await rest.request('GET', '/api/files/' + uuid)  # noqa: F841

    # create the second file; should NOT be OK
    with pytest.raises(Exception) as cm:
        await rest.request('POST', '/api/files', metadata2)
    _assert_httperror(
        cm.value,
        409,
        "Conflict with existing file (location already exists `/data/test/exp/IceCube/foo.dat`)"
    )


@pytest.mark.asyncio
async def test_62c_post_files__locations_NxN__error(rest: RestClient) -> None:
    """Test locations uniqueness under NxN multiplicity."""
    # define the locations to be tested
    loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
    locs3a = [loc1a, loc1b, loc1c]
    locs3b = [loc1b, loc1c, loc1d]  # noqa: F841
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
    data, url, uuid = await _post_and_assert(rest, metadata)

    # check that the file was created properly
    data = await _assert_in_fc(rest, uuid)

    # check that the file was created properly, part deux
    data = await rest.request('GET', '/api/files/' + uuid)  # noqa: F841

    # create the second file; should NOT be OK
    with pytest.raises(Exception) as cm:
        await rest.request('POST', '/api/files', metadata2)
    _assert_httperror(
        cm.value,
        409,
        "Conflict with existing file (location already exists `/data/test/exp/IceCube/foo.dat`)"
    )


@pytest.mark.asyncio
async def test_63a_put_files_uuid__locations_1xN__error(rest: RestClient) -> None:
    """Test locations uniqueness under 1xN multiplicity."""
    # define the locations to be tested
    loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
    locs3a = [loc1a, loc1b, loc1c]  # noqa: F841
    locs3b = [loc1b, loc1c, loc1d]  # noqa: F841
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
    data, url, uuid = await _post_and_assert(rest, metadata)

    # create the second file; should be OK
    data, _, __ = await _post_and_assert(rest, metadata2)

    # try to replace the first file with a location collision with the second; should NOT be OK
    with pytest.raises(Exception) as cm:
        await rest.request('PUT', '/api/files/' + uuid, replace1)
    _assert_httperror(
        cm.value,
        409,
        "Conflict with existing file (location already exists `/data/test/exp/IceCube/foo.dat`)"
    )


@pytest.mark.asyncio
async def test_63b_put_files_uuid__locations_Nx1__error(rest: RestClient) -> None:
    """Test locations uniqueness under Nx1 multiplicity."""
    # define the locations to be tested
    loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
    locs3a = [loc1a, loc1b, loc1c]
    locs3b = [loc1b, loc1c, loc1d]  # noqa: F841
    locs3c = [loc1a, loc1b, loc1d]  # noqa: F841

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
    data, url, uuid = await _post_and_assert(rest, metadata)

    # create the second file; should be OK
    data, _, __ = await _post_and_assert(rest, metadata2)

    # try to replace the first file with a location collision with the second; should NOT be OK
    with pytest.raises(Exception) as cm:
        await rest.request('PUT', '/api/files/' + uuid, replace1)
    _assert_httperror(
        cm.value,
        409,
        "Conflict with existing file (location already exists `/data/test/exp/IceCube/foo.dat`)"
    )


@pytest.mark.asyncio
async def test_63c_put_files_uuid__locations_NxN__error(rest: RestClient) -> None:
    """Test locations uniqueness under NxN multiplicity."""
    # define the locations to be tested
    loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
    locs3a = [loc1a, loc1b, loc1c]
    locs3b = [loc1b, loc1c, loc1d]
    locs3c = [loc1a, loc1b, loc1d]  # noqa: F841

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
    data, url, uuid = await _post_and_assert(rest, metadata)

    # create the second file; should be OK
    data, _, __ = await _post_and_assert(rest, metadata2)

    # try to replace the first file with a location collision with the second; should NOT be OK
    with pytest.raises(Exception) as cm:
        await rest.request('PUT', '/api/files/' + uuid, replace1)
    _assert_httperror(
        cm.value,
        409,
        "Conflict with existing file (location already exists `/data/test/exp/IceCube/foo.dat`)"
    )


@pytest.mark.asyncio
async def test_64a_patch_files_uuid__locations_1xN__error(rest: RestClient) -> None:
    """Test locations uniqueness under 1xN multiplicity."""
    # define the locations to be tested
    loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
    locs3a = [loc1a, loc1b, loc1c]  # noqa: F841
    locs3b = [loc1b, loc1c, loc1d]  # noqa: F841
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
    data, url, uuid = await _post_and_assert(rest, metadata)

    # create the second file; should be OK
    data, _, __ = await _post_and_assert(rest, metadata2)

    # try to update the first file with a patch; should NOT be OK
    with pytest.raises(Exception) as cm:
        await rest.request('PATCH', '/api/files/' + uuid, patch1)
    _assert_httperror(
        cm.value,
        409,
        "Conflict with existing file (location already exists `/data/test/exp/IceCube/foo.dat`)"
    )


@pytest.mark.asyncio
async def test_64b_patch_files_uuid__locations_Nx1__error(rest: RestClient) -> None:
    """Test locations uniqueness under Nx1 multiplicity."""
    # define the locations to be tested
    loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
    locs3a = [loc1a, loc1b, loc1c]  # noqa: F841
    locs3b = [loc1b, loc1c, loc1d]
    locs3c = [loc1a, loc1b, loc1d]  # noqa: F841

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
    data, url, uuid = await _post_and_assert(rest, metadata)

    # create the second file; should be OK
    data, _, __ = await _post_and_assert(rest, metadata2)

    # try to update the first file with a patch; should NOT be OK
    with pytest.raises(Exception) as cm:
        await rest.request('PATCH', '/api/files/' + uuid, patch1)
    _assert_httperror(
        cm.value,
        409,
        "Conflict with existing file (location already exists `/data/test/exp/IceCube/foo.dat`)"
    )


@pytest.mark.asyncio
async def test_64c_patch_files_uuid__locations_NxN__error(rest: RestClient) -> None:
    """Test locations uniqueness under NxN multiplicity."""
    # define the locations to be tested
    loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
    locs3a = [loc1a, loc1b, loc1c]  # noqa: F841
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
    data, url, uuid = await _post_and_assert(rest, metadata)

    # create the second file; should be OK
    data, _, __ = await _post_and_assert(rest, metadata2)

    # try to update the first file with a patch; should NOT be OK
    with pytest.raises(Exception) as cm:
        await rest.request('PATCH', '/api/files/' + uuid, patch1)
    _assert_httperror(
        cm.value,
        409,
        "Conflict with existing file (location already exists `/data/test/exp/IceCube/foo.dat`)"
    )


@pytest.mark.asyncio
async def test_70_abuse_post_files__locations(rest: RestClient) -> None:
    """Abuse the POST /api/files/UUID/locations route to test error
    handling."""
    # define some locations to be tested
    loc1a = {'site': 'WIPAC', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1b = {'site': 'DESY', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1c = {'site': 'NERSC', 'path': '/data/test/exp/IceCube/foo.dat'}
    loc1d = {'site': 'OSG', 'path': '/data/test/exp/IceCube/foo.dat'}
    locations = [loc1a, loc1b, loc1c, loc1d]

    # try to POST to an invalid UUID
    valid_post_body = {"locations": locations}
    with pytest.raises(Exception) as cm:
        await rest.request('POST', '/api/files/bobsyeruncle/locations', valid_post_body)
    _assert_httperror(cm.value, 404, "File uuid not found")

    # try to POST to an non-existant UUID
    with pytest.raises(Exception) as cm:
        await rest.request('POST', '/api/files/6e4ec06d-8e22-4a2b-a392-f4492fb25eb1/locations', valid_post_body)
    _assert_httperror(cm.value, 404, "File uuid not found")

    # define a file to be created
    metadata = {
        'logical_name': '/blah/data/exp/IceCube/blah.dat',
        'checksum': {'sha512': hex('foo bar')},
        'file_size': 1,
        u'locations': [loc1a]
    }

    # create the file; should be OK
    data, url, uuid = await _post_and_assert(rest, metadata)

    # try to POST to the file without a post body
    with pytest.raises(Exception) as cm:
        await rest.request('POST', '/api/files/' + uuid + '/locations', {})
    _assert_httperror(cm.value, 400, "POST body requires 'locations' field")

    # try to POST to the file with a non-array locations
    with pytest.raises(Exception) as cm:
        await rest.request('POST', '/api/files/' + uuid + '/locations', {"locations": "bobsyeruncle"})
    _assert_httperror(cm.value, 400, "Validation Error: member `locations` must be a list with 1+ entries, each with keys: ['site', 'path']")


@pytest.mark.asyncio
async def test_71_post_files__locations_duplicate(rest: RestClient) -> None:
    """Test that POST /api/files/UUID/locations is a no-op for non-distinct
    locations."""
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
    data, url, uuid = await _post_and_assert(rest, metadata)

    # read the full record of the file; should be OK
    rec = await rest.request('GET', '/api/files/' + uuid)
    assert len(rec["locations"]) == 4
    assert 'meta_modify_date' in rec
    mmd = rec['meta_modify_date']

    # try to POST existant locations to the file; should be OK
    not_so_new_locations = {"locations": [loc1b, loc1d]}
    rec2 = await rest.request('POST', '/api/files/' + uuid + '/locations', not_so_new_locations)

    # ensure the record is the same (not updated)
    assert len(rec2["locations"]) == 4
    assert rec["locations"] == rec2["locations"]
    assert rec2["meta_modify_date"] == mmd


@pytest.mark.asyncio
async def test_72_post_files__locations_conflict(rest: RestClient) -> None:
    """Test that POST /api/files/UUID/locations returns an error on
    conflicting duplicate locations."""
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
    data, url, uuid = await _post_and_assert(rest, metadata)

    # define a second file to be created
    locations2 = [loc1c, loc1d]
    metadata2 = {
        'logical_name': '/blah/data/exp/IceCube/blah2.dat',
        'checksum': {'sha512': hex('foo bar')},
        'file_size': 1,
        u'locations': locations2
    }

    # create the file; should be OK
    data, url, uuid2 = await _post_and_assert(rest, metadata2)

    # try to POST a second file location to the first file
    with pytest.raises(Exception) as cm:
        conflicting_locations = {"locations": [loc1d]}
        rec2 = await rest.request('POST', '/api/files/' + uuid + '/locations', conflicting_locations)  # noqa: F841
    _assert_httperror(
        cm.value,
        409,
        "Conflict with existing file (location already exists `/data/test/exp/IceCube/foo.dat`)"
    )


@pytest.mark.asyncio
async def test_73_post_files__locations(rest: RestClient) -> None:
    """Test that POST /api/files/UUID/locations can add distinct non-
    conflicting locations."""
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
    data, url, uuid = await _post_and_assert(rest, metadata)

    # read the full record of the file; should be OK
    rec = await rest.request('GET', '/api/files/' + uuid)
    assert len(rec["locations"]) == 2
    assert 'meta_modify_date' in rec
    mmd = rec['meta_modify_date']

    # try to POST existant locations to the file; should be OK
    new_locations = {"locations": [loc1c, loc1d]}
    rec2 = await rest.request('POST', '/api/files/' + uuid + '/locations', new_locations)

    # ensure the record has changed (is updated)
    assert len(rec2["locations"]) == 4
    assert not rec2["meta_modify_date"] == mmd
    assert loc1a in rec2["locations"]
    assert loc1b in rec2["locations"]
    assert loc1c in rec2["locations"]
    assert loc1d in rec2["locations"]


@pytest.mark.asyncio
async def test_74_post_files__locations_just_one(rest: RestClient) -> None:
    """Test that POST /api/files/UUID/locations can add distinct non-
    conflicting locations."""
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
    data, url, uuid = await _post_and_assert(rest, metadata)

    # read the full record of the file; should be OK
    rec = await rest.request('GET', '/api/files/' + uuid)
    assert len(rec["locations"]) == 1
    assert 'meta_modify_date' in rec
    mmd = rec['meta_modify_date']

    # try to POST existant locations to the file; should be OK
    new_locations = {"locations": [loc1c]}
    rec2 = await rest.request('POST', '/api/files/' + uuid + '/locations', new_locations)

    # ensure the record has changed (is updated)
    assert len(rec2["locations"]) == 2
    assert rec2["meta_modify_date"] != mmd
    assert loc1a in rec2["locations"]
    assert loc1b not in rec2["locations"]
    assert loc1c in rec2["locations"]
    assert loc1d not in rec2["locations"]


@pytest.mark.asyncio
async def test_80a_files_uuid_actions_remove_location__keep_record__okay(rest: RestClient) -> None:
    """Test removing a location from a record with multiple locations."""
    # define the file to be created
    wipac_path = u'/blah/data/exp/IceCube/blah.dat'
    nersc_path = u'/blah/data/exp/IceCube/blah.dat'
    metadata1 = {
        'logical_name': '/blah/data/exp/IceCube/blah.dat',
        'checksum': {'sha512': hex('foo bar')},
        'file_size': 1,
        u'locations': [
            {u'site': u'WIPAC', u'path': wipac_path},
            {u'site': u'NERSC', u'path': nersc_path}
        ]
    }

    # create the file the first time; should be OK
    data, url, uuid = await _post_and_assert(rest, metadata1)
    data = await _assert_in_fc(rest, uuid)

    # remove WIPAC location
    data = await rest.request(
        'POST',
        f'/api/files/{uuid}/actions/remove_location',
        {'site': 'WIPAC', 'path': wipac_path}
    )
    metadata_without_wipac = copy.deepcopy(metadata1)
    metadata_without_wipac['locations'] = [{u'site': u'NERSC', u'path': nersc_path}]
    assert copy_without_rest_response_keys(data) == metadata_without_wipac

    # double-check FC
    data = await _assert_in_fc(rest, uuid, all_keys=True)
    assert copy_without_rest_response_keys(data['files'][0]) == metadata_without_wipac


@pytest.mark.asyncio
async def test_80b_files_uuid_actions_remove_location__keep_record__okay(rest: RestClient) -> None:
    """Test removing a location from a record with multiple locations.

    This time check that only the mandatory fields are needed to match.
    """
    # define the file to be created
    wipac_path = u'/blah/data/exp/IceCube/blah.dat'
    nersc_path = u'/blah/data/exp/IceCube/blah.dat'
    metadata1 = {
        'logical_name': '/blah/data/exp/IceCube/blah.dat',
        'checksum': {'sha512': hex('foo bar')},
        'file_size': 1,
        u'locations': [
            {u'site': u'WIPAC', u'path': wipac_path},
            {u'site': u'NERSC', u'path': nersc_path, 'archive': True}
        ]
    }

    # create the file the first time; should be OK
    data, url, uuid = await _post_and_assert(rest, metadata1)
    data = await _assert_in_fc(rest, uuid)

    # remove NERSC location -- BUT don't include "archive"
    data = await rest.request(
        'POST',
        f'/api/files/{uuid}/actions/remove_location',
        {'site': 'NERSC', 'path': nersc_path}
    )
    metadata_without_nersc = copy.deepcopy(metadata1)
    metadata_without_nersc['locations'] = [{u'site': u'WIPAC', u'path': wipac_path}]
    assert copy_without_rest_response_keys(data) == metadata_without_nersc

    # double-check FC
    data = await _assert_in_fc(rest, uuid, all_keys=True)
    assert copy_without_rest_response_keys(data['files'][0]) == metadata_without_nersc


@pytest.mark.asyncio
async def test_81_files_uuid_actions_remove_location__delete_record__okay(rest: RestClient) -> None:
    """Test removing a location from a record with only one location.

    Will also delete the record.
    """
    # define the file to be created
    wipac_path = u'/blah/data/exp/IceCube/blah.dat'
    metadata1 = {
        'logical_name': '/blah/data/exp/IceCube/blah.dat',
        'checksum': {'sha512': hex('foo bar')},
        'file_size': 1,
        u'locations': [{u'site': u'WIPAC', u'path': wipac_path}]
    }

    # create the file the first time; should be OK
    data, url, uuid = await _post_and_assert(rest, metadata1)
    data = await _assert_in_fc(rest, uuid)

    # remove sole location
    data = await rest.request(
        'POST',
        f'/api/files/{uuid}/actions/remove_location',
        {'site': 'WIPAC', 'path': wipac_path}
    )
    assert data == {}

    # double-check FC is empty
    data = await _assert_in_fc(rest, [])


@pytest.mark.asyncio
async def test_82_files_uuid_actions_remove_location__bad_args__error(rest: RestClient) -> None:
    """Test that there's an error when the given invalid args."""
    # define the file to be created
    wipac_path = u'/blah/data/exp/IceCube/blah.dat'
    metadata1 = {
        'logical_name': '/blah/data/exp/IceCube/blah.dat',
        'checksum': {'sha512': hex('foo bar')},
        'file_size': 1,
        u'locations': [{u'site': u'WIPAC', u'path': wipac_path}]
    }

    # create the file the first time; should be OK
    data, url, uuid = await _post_and_assert(rest, metadata1)
    data = await _assert_in_fc(rest, uuid)

    # missing args(s)
    with pytest.raises(Exception) as cm:
        await rest.request(
            'POST',
            f'/api/files/{uuid}/actions/remove_location',
            {'site': 'WIPAC'}
        )
    _assert_httperror(
        cm.value,
        400,
        "POST body requires 'site' & 'path' fields"
    )
    # check that nothing has changed
    data = await _assert_in_fc(rest, uuid, all_keys=True)
    assert copy_without_rest_response_keys(data['files'][0]) == metadata1

    # non-mandatory location-field args(s), like "archive"
    with pytest.raises(Exception) as cm:
        await rest.request(
            'POST',
            f'/api/files/{uuid}/actions/remove_location',
            {'site': 'WIPAC', 'path': wipac_path, 'archive': False}
        )
    _assert_httperror(
        cm.value,
        400,
        f"Extra POST body fields detected: {['archive']} "
        f"('site' & 'path' are required)"
    )
    # check that nothing has changed
    data = await _assert_in_fc(rest, uuid, all_keys=True)
    assert copy_without_rest_response_keys(data['files'][0]) == metadata1

    # other bogus args(s)
    pass


@pytest.mark.asyncio
async def test_83_files_uuid_actions_remove_location__location_404__error(rest: RestClient) -> None:
    """Test that there's an error when the given location is not in the record."""
    # define the file to be created
    wipac_path = u'/blah/data/exp/IceCube/blah.dat'
    metadata1 = {
        'logical_name': '/blah/data/exp/IceCube/blah.dat',
        'checksum': {'sha512': hex('foo bar')},
        'file_size': 1,
        u'locations': [{u'site': u'WIPAC', u'path': wipac_path}]
    }

    # create the file the first time; should be OK
    data, url, uuid = await _post_and_assert(rest, metadata1)
    data = await _assert_in_fc(rest, uuid)

    # bogus path
    with pytest.raises(Exception) as cm:
        await rest.request(
            'POST',
            f'/api/files/{uuid}/actions/remove_location',
            {'site': 'WIPAC', 'path': wipac_path + '!'}
        )
    _assert_httperror(
        cm.value,
        404,
        f"Location entry not found for site='WIPAC' & path='{wipac_path + '!'}'"
    )
    # check that nothing has changed
    data = await _assert_in_fc(rest, uuid, all_keys=True)
    assert copy_without_rest_response_keys(data['files'][0]) == metadata1

    # bogus site
    with pytest.raises(Exception) as cm:
        await rest.request(
            'POST',
            f'/api/files/{uuid}/actions/remove_location',
            {'site': 'WIPAC!', 'path': wipac_path}
        )
    _assert_httperror(
        cm.value,
        404,
        f"Location entry not found for site='WIPAC!' & path='{wipac_path}'"
    )
    # check that nothing has changed
    data = await _assert_in_fc(rest, uuid, all_keys=True)
    assert copy_without_rest_response_keys(data['files'][0]) == metadata1
