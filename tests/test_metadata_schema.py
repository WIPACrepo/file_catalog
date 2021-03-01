"""Test metadata_schema."""


from inspect import getmembers

# local imports
from file_catalog import metadata_schema


def test_00_types() -> None:
    """Simply check imports."""
    assert "Metadata" in getmembers(metadata_schema.types)
