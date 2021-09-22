"""Utilities for metadata validation."""

# fmt:off

import re
from typing import Any, Dict, List, Optional, cast

from .. import utils
from . import types


def _get_val_in_metadata_dotted(field: str, metadata: types.Metadata) -> Any:
    """Wraps `field_in_dict_dot_recurse()` with an incoming type cast.

    Raises:
        KeyError - if field not found
        TypeError - if parent-field is not `dict`-like
    """
    return utils.get_val_in_dict_dotted(field, cast(Dict[str, Any], metadata))


class Validation:
    """Validating field-specific metadata."""

    # keys/fields
    FORBIDDEN_FIELDS_CREATION = ['mongo_id', '_id', 'meta_modify_date']
    FORBIDDEN_FIELDS_UPDATE = [
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
    MANDATORY_LOCATION_KEYS = ['site', 'path']

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
    def _find_all_field_vals(metadata: types.Metadata, fields: List[str]) -> Dict[str, Any]:
        """Return all of fields' values in metadata."""
        field_vals = {}
        for field in fields:
            try:
                field_vals[field] = _get_val_in_metadata_dotted(field, metadata)
            except (KeyError, TypeError):
                continue
        return field_vals

    def has_forbidden_attributes_creation(self, apihandler: Any, metadata: types.Metadata, old_metadata: types.Metadata) -> bool:
        """Check if `metadata` has forbidden attributes and they have changed.

        Returns `True` if it has forbidden attributes.
        """
        forbidden_matches = self._find_all_field_vals(metadata, self.FORBIDDEN_FIELDS_CREATION)

        for field, val in forbidden_matches.items():
            if field not in old_metadata or val != _get_val_in_metadata_dotted(field, old_metadata):
                # forbidden fields
                apihandler.send_error(
                    400,
                    reason=f"Validation Error: forbidden attribute creation '{field}'",
                    file=apihandler.files_url,
                )
                return True
        return False

    def has_forbidden_attributes_modification(self, apihandler: Any, metadata: types.Metadata, old_metadata: types.Metadata) -> bool:
        """Check if `metadata` has forbidden attribute updates."""
        forbidden_matches = self._find_all_field_vals(metadata, self.FORBIDDEN_FIELDS_UPDATE)

        for field, val in forbidden_matches.items():
            if field not in old_metadata or val != _get_val_in_metadata_dotted(field, old_metadata):
                # forbidden fields
                apihandler.send_error(
                    400,
                    reason=f"Validation Error: forbidden attribute update '{field}'",
                    file=apihandler.files_url,
                )
                return True
        return False

    def validate_metadata_creation(self, apihandler: Any, metadata: types.Metadata) -> bool:
        """Validate metadata for creation.

        Utilizes `send_error` and returns `False` if validation failed.
        If validation was successful, `True` is returned.
        """
        if self.has_forbidden_attributes_creation(apihandler, metadata, {}):
            return False
        return self.validate_metadata_modification(apihandler, metadata)

    @staticmethod
    def _find_missing_mandatory_field(metadata: types.Metadata, fields: List[str]) -> Optional[str]:
        """Return the first field found to be missing, or `None`."""
        for field in fields:
            try:
                _get_val_in_metadata_dotted(field, metadata)
            except (KeyError, TypeError):
                return field
        return None

    def validate_metadata_modification(self, apihandler: Any, metadata: types.Metadata) -> bool:
        """Validate metadata for modification.

        Utilizes `send_error` and returns `False` if validation failed.
        If validation was successful, `True` is returned.
        """
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

        # CHECKSUM
        if ((not isinstance(metadata['checksum'], dict)) or 'sha512' not in metadata['checksum']):
            # checksum needs to be a dict with an sha512
            apihandler.send_error(
                400,
                reason='Validation Error: member `checksum` must be a dict with a sha512 hash',
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
