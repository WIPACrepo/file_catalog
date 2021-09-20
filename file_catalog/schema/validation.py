"""Utilities for metadata validation."""

# fmt:off

import re
from typing import Any, Dict, List, Optional

from . import types


def _find_missing_mandatory_field(metadata: types.Metadata, fields: List[str]) -> Optional[str]:
    """Return the first field found to be missing, or `None`."""
    for field in fields:
        try:
            if "." in field:  # compound field; ex: "checksum.sha512"
                parent, child = field.split(".", maxsplit=1)  # ex: "checksum" & "sha512"
                if _find_missing_mandatory_field(metadata[parent], [child]):  # type: ignore[misc]
                    return field
            else:
                # just try to access the field
                _ = metadata[field]  # type: ignore[misc]
        except KeyError:
            return field

    return None


class Validation:
    """Validating field-specific metadata."""

    MANDATORY_LOCATION_KEYS = ['site', 'path']
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

    def has_forbidden_attributes_creation(self, apihandler: Any, metadata: types.Metadata, old_metadata: types.Metadata) -> bool:
        """Check if `metadata` has forbidden attributes and they have changed.

        Returns `True` if it has forbidden attributes.
        """
        for key in set(self.config['META_FORBIDDEN_FIELDS_CREATION']).intersection(metadata):
            if key not in old_metadata or metadata[key] != old_metadata[key]:  # type: ignore[misc]
                # forbidden fields
                apihandler.send_error(400, reason=f'Validation Error: forbidden attribute creation `{key}`',
                                      file=apihandler.files_url)
                return True
        return False

    def has_forbidden_attributes_modification(self, apihandler: Any, metadata: types.Metadata, old_metadata: types.Metadata) -> bool:
        """Check if `metadata` has forbidden attribute updates."""
        for key in set(self.config['META_FORBIDDEN_FIELDS_UPDATE']).intersection(metadata):
            if key not in old_metadata or metadata[key] != old_metadata[key]:  # type: ignore[misc]
                # forbidden fields
                apihandler.send_error(400, reason=f'Validation Error: forbidden attribute update `{key}`',
                                      file=apihandler.files_url)
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

    def validate_metadata_modification(self, apihandler: Any, metadata: types.Metadata) -> bool:
        """Validate metadata for modification.

        Utilizes `send_error` and returns `False` if validation failed.
        If validation was successful, `True` is returned.
        """
        # MANDATORY FIELDS
        missing = _find_missing_mandatory_field(metadata, self.config['META_MANDATORY_FIELDS'])
        if missing:
            apihandler.send_error(
                400,
                reason=f"Validation Error: metadata missing mandatory field `{missing}` "
                       f"(mandatory fields: {', '.join(self.config['META_MANDATORY_FIELDS'])})",
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
