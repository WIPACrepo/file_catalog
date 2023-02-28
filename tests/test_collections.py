# test_collections.py
"""Test /api/collections."""

# fmt:off
# pylint: skip-file

from typing import Any, Dict

import pytest
from rest_tools.client import RestClient

from .test_files import hex


@pytest.mark.asyncio
async def test_10_collections(rest: RestClient) -> None:
    """Test GET /api/collections."""
    metadata = {
        'collection_name': 'blah',
        'owner': 'foo',
    }
    data = await rest.request('POST', '/api/collections', metadata)
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'collection' in data
    url = data['collection']
    uid = url.split('/')[-1]

    data = await rest.request('GET', '/api/collections')
    assert 'collections' in data
    assert uid in {row['uuid'] for row in data['collections']}


@pytest.mark.asyncio
async def test_20_collection_by_id(rest: RestClient) -> None:
    """Test GET /api/collections/{uuid}."""
    metadata = {
        'collection_name': 'blah',
        'owner': 'foo',
    }
    data = await rest.request('POST', '/api/collections', metadata)
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'collection' in data
    url = data['collection']
    uid = url.split('/')[-1]

    data = await rest.request('GET', '/api/collections/' + uid)
    for k in metadata:
        assert k in data
        assert metadata[k] == data[k]


@pytest.mark.asyncio
async def test_21_collection_by_name(rest: RestClient) -> None:
    """Test GET /api/collections/{name}."""
    metadata = {
        'collection_name': 'blah',
        'owner': 'foo',
    }
    data = await rest.request('POST', '/api/collections', metadata)
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'collection' in data
    url = data['collection']
    uid = url.split('/')[-1]  # noqa: F841

    data = await rest.request('GET', f'/api/collections/{uid}')
    for k in metadata:
        assert k in data
        assert metadata[k] == data[k]

    data = await rest.request('GET', '/api/collections/blah')
    for k in metadata:
        assert k in data
        assert metadata[k] == data[k]


@pytest.mark.asyncio
async def test_30_collection_files(rest: RestClient) -> None:
    """Test GET /api/collections/{name}/files."""
    metadata: Dict[str, Any] = {
        'collection_name': 'blah',
        'owner': 'foo',
    }
    data = await rest.request('POST', '/api/collections', metadata)
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'collection' in data
    url = data['collection']
    uid = url.split('/')[-1]

    data = await rest.request('GET', f'/api/collections/{uid}/files')
    assert data['files'] == []

    data = await rest.request('GET', '/api/collections/blah/files')
    assert data['files'] == []

    # add a file
    metadata = {
        'logical_name': 'blah',
        'checksum': {'sha512': hex('foo bar')},
        'file_size': 1,
        u'locations': [{u'site': u'test', u'path': u'blah.dat'}]
    }
    data = await rest.request('POST', '/api/files', metadata)
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'file' in data
    url = data['file']
    uid = url.split('/')[-1]

    data = await rest.request('GET', '/api/collections/blah/files',
                              {'keys': 'uuid|logical_name|checksum|locations'})
    assert len(data['files']) == 1
    assert data['files'][0]['uuid'] == uid
    assert data['files'][0]['checksum'] == metadata['checksum']


@pytest.mark.asyncio
async def test_70_snapshot_create(rest: RestClient) -> None:
    """Test POST /api/collections/{uuid}/snapshots."""
    metadata: Dict[str, Any] = {
        'collection_name': 'blah',
        'owner': 'foo',
    }
    data = await rest.request('POST', '/api/collections', metadata)
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'collection' in data
    url = data['collection']
    uid = url.split('/')[-1]

    data = await rest.request('GET', '/api/collections/' + uid)
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'collection_name' in data

    data = await rest.request('POST', '/api/collections/{}/snapshots'.format(uid))
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'snapshot' in data
    url = data['snapshot']
    snap_uid = url.split('/')[-1]

    data = await rest.request('GET', '/api/collections/{}/snapshots'.format(uid))
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'snapshots' in data
    assert len(data['snapshots']) == 1
    assert data['snapshots'][0]['uuid'] == snap_uid


@pytest.mark.asyncio
async def test_71_snapshot_find(rest: RestClient) -> None:
    """Test POST /api/collections/{uuid}/snapshots immutability."""
    metadata: Dict[str, Any] = {
        'collection_name': 'blah',
        'owner': 'foo',
    }
    data = await rest.request('POST', '/api/collections', metadata)
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'collection' in data
    url = data['collection']
    uid = url.split('/')[-1]

    data = await rest.request('GET', '/api/collections/' + uid)
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'collection_name' in data

    data = await rest.request('POST', '/api/collections/{}/snapshots'.format(uid))
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'snapshot' in data
    url = data['snapshot']
    snap_uid = url.split('/')[-1]

    data = await rest.request('GET', '/api/snapshots/{}/files'.format(snap_uid))
    assert data['files'] == []

    # add a file
    metadata = {
        'logical_name': 'blah',
        'checksum': {'sha512': hex('foo bar')},
        'file_size': 1,
        u'locations': [{u'site': u'test', u'path': u'blah.dat'}]
    }
    data = await rest.request('POST', '/api/files', metadata)
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'file' in data
    url = data['file']
    file_uid = url.split('/')[-1]

    # old snapshot stays empty
    data = await rest.request('GET', '/api/snapshots/{}/files'.format(snap_uid))
    assert data['files'] == []

    # new snapshot should have file
    data = await rest.request('POST', '/api/collections/{}/snapshots'.format(uid))
    assert '_links' in data
    assert 'self' in data['_links']
    assert 'snapshot' in data
    url = data['snapshot']
    snap_uid = url.split('/')[-1]

    data = await rest.request('GET', '/api/snapshots/{}/files'.format(snap_uid),
                              {'keys': 'uuid|logical_name|checksum|locations'})
    assert len(data['files']) == 1
    assert data['files'][0]['uuid'] == file_uid
    assert data['files'][0]['checksum'] == metadata['checksum']
