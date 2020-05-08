"""Test indexer filename parsing."""

import pytest
from indexer import ACCEPTED_ROOTS, check_path, path_in_blacklist


def test_accepted_roots():
    """Test contents of ACCEPTED_ROOTS."""
    assert '/data' in ACCEPTED_ROOTS


def test_check_path():
    """Test filepath white-listing."""
    check_path('/data/foo')
    check_path('/data/foo/bar')
    check_path('/data/')
    check_path('/data')

    with pytest.raises(Exception):
        check_path('foo')
    with pytest.raises(Exception):
        check_path('/data2')
    with pytest.raises(Exception):
        check_path('~/data')
    with pytest.raises(Exception):
        check_path('data/')


def test_blacklist():
    """Test filepath black-listing."""
    blacklist = ['/foo/bar', '/foo/baz']

    assert path_in_blacklist('/foo/bar', blacklist)
    assert path_in_blacklist('/foo/baz', blacklist)
    assert path_in_blacklist('/foo/baz/foobar', blacklist)

    assert not path_in_blacklist('/foo/baz2', blacklist)
    assert not path_in_blacklist('/foo/baz2/foobar', blacklist)
    assert not path_in_blacklist('/foo', blacklist)
