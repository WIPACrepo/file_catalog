"""Test schema."""


# local imports
from file_catalog import schema


def test_00_types() -> None:
    """Simply check imports."""
    type_dicts = [
        "Checksum",
        "LocationEntry",
        "SoftwareEntry",
        "EventsData",
        "Run",
        "GapEntry",
        "Event",
        "OfflineProcessingMetadata",
        "IceProdMetadata",
        "SimulationMetadata",
        "Metadata",
    ]
    for type_dict_class in type_dicts:
        assert type_dict_class in dir(schema.types)
