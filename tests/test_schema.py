"""Test schema."""

# pylint: disable=W0212

from pprint import pprint
from typing import List, Optional, TypedDict

# local imports
from file_catalog.schema import types
from file_catalog.schema.validation import Validation


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
        assert type_dict_class in dir(types)


def test_10_find_missing_mandatory_field() -> None:
    """Test _find_missing_mandatory_field()."""

    class _TestCases(TypedDict):
        metadata: types.Metadata
        mandatory_fields: List[str]
        missing_field: Optional[str]

    test_cases: List[_TestCases] = [
        {  # null-null case
            "metadata": {},
            "mandatory_fields": [],
            "missing_field": None,
        },
        {  # metadata null case
            "metadata": {},
            "mandatory_fields": ["bart", "lisa"],
            "missing_field": "bart",
        },
        {  # mandatory_fields null case (no mandatory fields)
            "metadata": {"homer": 33, "marge": 88},  # type: ignore[typeddict-item]
            "mandatory_fields": [],
            "missing_field": None,
        },
        {  # nothing missing, nothing extra
            "metadata": {"foo": 1, "bar": 2, "aye": {"bee": {"sea": 333}}},  # type: ignore[typeddict-item]
            "mandatory_fields": ["aye.bee.sea", "foo", "bar"],
            "missing_field": None,
        },
        {  # nothing missing, with extra
            "metadata": {"foo": 1, "bar": 2, "extra": "baz"},  # type: ignore[typeddict-item]
            "mandatory_fields": ["foo", "bar"],
            "missing_field": None,
        },
        {  # missing whole regular field
            "metadata": {"foo": 1, "far": 7},  # type: ignore[typeddict-item]
            "mandatory_fields": ["foo", "bar"],
            "missing_field": "bar",
        },
        {  # missing whole compound field
            "metadata": {"foo": 1, "bar": 2},  # type: ignore[typeddict-item]
            "mandatory_fields": ["aye.bee.sea", "foo", "bar"],
            "missing_field": "aye.bee.sea",
        },
        {  # missing nested compound sub-field
            "metadata": {"foo": 1, "bar": 2, "aye": {"bee": {"dee": 5}}},  # type: ignore[typeddict-item]
            "mandatory_fields": ["aye.bee.sea", "foo", "bar"],
            "missing_field": "aye.bee.sea",
        },
    ]

    for case in test_cases:
        pprint(case)
        assert case["missing_field"] == Validation._find_missing_mandatory_field(
            case["metadata"], case["mandatory_fields"]
        )
