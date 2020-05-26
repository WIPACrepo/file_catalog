"""Test indexer filename parsing."""

import pathlib

import pytest
from indexer import ACCEPTED_ROOTS, check_path, path_in_blacklist, sorted_unique_filepaths


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


def test_sorted_unique_filepaths():
    """Test sorting, removing duplicate, and checking for illegal characters in filepaths."""
    filepaths = ['foo/bar/baz.txt', 'foo/bar/baz.txt', 'baz/FOO.txt']

    this_dir = pathlib.Path(__file__).parent.absolute()
    result = sorted_unique_filepaths(file_of_filepaths=f'{this_dir}/illegal_filepaths',
                                     list_of_filepaths=filepaths)
    expected = ['/bar/baz.py', 'baz/FOO.txt', 'foo/bar/baz.txt']
    assert result == expected
