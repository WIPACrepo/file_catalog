"""Test argbuilder.py functions."""

# pylint: disable=W0212

from file_catalog import argbuilder


def test_00_path_args() -> None:
    """Test _handle_path_args."""
    assert argbuilder._handle_path_args({}) == {}
    assert argbuilder._handle_path_args({"foo": 5}) == {"foo": 5}
