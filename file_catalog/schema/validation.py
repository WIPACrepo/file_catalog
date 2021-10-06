"""Utilities for metadata validation."""


import re
from typing import Any, Dict, List, Optional, cast

from .. import utils
from . import types


def _get_val_in_metadata_dotted(field: str, metadata: types.Metadata) -> Any:
    return utils.get_val_in_dict_dotted(field, cast(Dict[str, Any], metadata))


class Validation:
    """Validating field-specific metadata."""

    # keys/fields
    FORBIDDEN_FIELDS_CREATION = ["mongo_id", "_id", "meta_modify_date"]
    FORBIDDEN_FIELDS_MODIFICATION = [
        "mongo_id",
        "_id",
        "meta_modify_date",
        "uuid",
        "logical_name",
        "checksum.sha512",
    ]
    MANDATORY_FIELDS = [
        "uuid",
        "logical_name",
        "locations",
        "file_size",
        "checksum.sha512",
    ]
    MANDATORY_LOCATION_KEYS = ["site", "path"]

    # error messages
    INVALID_LOCATIONS_LIST_MESSAGE = (
        f"Validation Error: member `locations` must be a list with "
        f"1+ entries, each with keys: {MANDATORY_LOCATION_KEYS}"
    )

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config

    @staticmethod
    def is_valid_sha512(hash_str: str) -> bool:
        """Check if `hash_str` is a valid SHA512 hash."""
        return re.match(r"[0-9a-f]{128}", str(hash_str), re.IGNORECASE) is not None

    @staticmethod
    def is_valid_location_list(locations: List[types.LocationEntry]) -> bool:
        """Check if `locations` is a valid list of location-entries."""
        if not isinstance(locations, list):
            return False
        if not locations:
            return False

        for loc in locations:
            if not Validation.is_valid_location(loc):
                return False

        return True

    @staticmethod
    def is_valid_location(location: types.LocationEntry) -> bool:
        """Check if `location` is a valid location-entry."""
        if not location:
            return False
        if not isinstance(location, dict):
            return False

        if not all(key in location for key in Validation.MANDATORY_LOCATION_KEYS):
            return False

        return True

    @staticmethod
    def _find_all_field_vals(
        metadata: types.Metadata, fields: List[str]
    ) -> Dict[str, Any]:
        """Return all of fields' values in metadata."""
        field_vals = {}
        for field in fields:
            try:
                field_vals[field] = _get_val_in_metadata_dotted(field, metadata)
            except utils.DottedKeyError:
                continue
        return field_vals

    @staticmethod
    def _field_vals_are_different(
        field: str, val: Any, old_metadata: types.Metadata
    ) -> bool:
        """Values aren't the same OR no value for that key in old metadata."""
        try:
            old_val = _get_val_in_metadata_dotted(field, old_metadata)
            return bool(val != old_val)
        except utils.DottedKeyError:
            return True  # value was not found in old_metadata

    @staticmethod
    def _has_forbidden_fields(
        apihandler: Any,
        metadata: types.Metadata,
        old_metadata: types.Metadata,
        forbidden_fields: List[str],
        http_error_message: str,
    ) -> bool:
        forbidden_matches = Validation._find_all_field_vals(metadata, forbidden_fields)

        for field, val in forbidden_matches.items():
            if Validation._field_vals_are_different(field, val, old_metadata):
                apihandler.send_error(
                    400,
                    reason=f"Validation Error: {http_error_message} '{field}'",
                    file=apihandler.files_url,
                )
                return True
        return False

    def has_forbidden_fields_creation(
        self, apihandler: Any, metadata: types.Metadata
    ) -> bool:
        """Check if `metadata` has forbidden fields."""
        return self._has_forbidden_fields(
            apihandler,
            metadata,
            {},
            self.FORBIDDEN_FIELDS_CREATION,
            "forbidden field creation",
        )

    def has_forbidden_fields_modification(
        self, apihandler: Any, metadata: types.Metadata, old_metadata: types.Metadata
    ) -> bool:
        """Check if `metadata` has forbidden field modifications."""
        return self._has_forbidden_fields(
            apihandler,
            metadata,
            old_metadata,
            self.FORBIDDEN_FIELDS_MODIFICATION,
            "forbidden field modification",
        )

    @staticmethod
    def _find_missing_mandatory_field(
        metadata: types.Metadata, fields: List[str]
    ) -> Optional[str]:
        """Return the first field found to be missing, or `None`."""
        for field in fields:
            try:
                _get_val_in_metadata_dotted(field, metadata)
            except utils.DottedKeyError:
                return field
        return None

    def validate_metadata_schema_typing(
        self, apihandler: Any, metadata: types.Metadata
    ) -> bool:
        """Check that `metadata` is okay to insert into the database.

        Utilizes `send_error` and returns `False` if validation failed.
        If validation was successful, `True` is returned.
        """
        # fmt: off
        # MANDATORY FIELDS
        missing = self._find_missing_mandatory_field(metadata, self.MANDATORY_FIELDS)
        if missing:
            apihandler.send_error(
                400,
                reason=f"Validation Error: metadata missing mandatory field `{missing}` "
                       f"(mandatory fields: {', '.join(self.MANDATORY_FIELDS)})",
                file=apihandler.files_url
            )
            return False

        # CHECKSSUM.SHA512
        if not self.is_valid_sha512(metadata['checksum']['sha512']):
            # force to use SHA512
            apihandler.send_error(
                400,
                reason='Validation Error: `checksum[sha512]` needs to be a SHA512 hash',
                file=apihandler.files_url
            )
            return False

        # LOCATIONS LIST & ITS ENTRIES
        if not self.is_valid_location_list(metadata['locations']):
            apihandler.send_error(
                400,
                reason=self.INVALID_LOCATIONS_LIST_MESSAGE,
                file=apihandler.files_url
            )
            return False

        return True
        # fmt: on
