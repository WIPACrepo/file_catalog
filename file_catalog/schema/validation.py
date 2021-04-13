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

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config

    @staticmethod
    def is_valid_sha512(hash_str: str) -> bool:
        """Check if `hash_str` is a valid SHA512 hash."""
        return re.match(r"[0-9a-f]{128}", str(hash_str), re.IGNORECASE) is not None

    def has_forbidden_attributes_creation(self, apihandler: Any, metadata: types.Metadata, old_metadata: types.Metadata) -> bool:
        """Check if `metadata` has forbidden attributes and they have changed.

        Returns `True` if it has forbidden attributes.
        """
        for key in set(self.config['META_FORBIDDEN_FIELDS_CREATION']).intersection(metadata):
            if key not in old_metadata or metadata[key] != old_metadata[key]:  # type: ignore[misc]
                # forbidden fields
                apihandler.send_error(400, reason='forbidden attributes',
                                      file=apihandler.files_url)
                return True
        return False

    def has_forbidden_attributes_modification(self, apihandler: Any, metadata: types.Metadata, old_metadata: types.Metadata) -> bool:
        """Check if `metadata` has forbidden attribute updates."""
        for key in set(self.config['META_FORBIDDEN_FIELDS_UPDATE']).intersection(metadata):
            if key not in old_metadata or metadata[key] != old_metadata[key]:  # type: ignore[misc]
                # forbidden fields
                apihandler.send_error(400, reason='forbidden attributes',
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
        missing = _find_missing_mandatory_field(metadata, self.config['META_MANDATORY_FIELDS'])
        if missing:
            apihandler.send_error(
                400,
                reason=f"mandatory metadata missing `{missing}` "
                       f"(mandatory fields: {', '.join(self.config['META_MANDATORY_FIELDS'])}",
                file=apihandler.files_url
            )
            return False

        if ((not isinstance(metadata['checksum'], dict)) or 'sha512' not in metadata['checksum']):
            # checksum needs to be a dict with an sha512
            apihandler.send_error(
                400,
                reason='member `checksum` must be a dict with a sha512 hash',
                file=apihandler.files_url
            )
            return False

        elif not self.is_valid_sha512(metadata['checksum']['sha512']):
            # force to use SHA512
            apihandler.send_error(
                400,
                reason='`checksum[sha512]` needs to be a SHA512 hash',
                file=apihandler.files_url
            )
            return False

        elif ((not isinstance(metadata['locations'], list))
              or (not metadata['locations'])
              or not all(loc for loc in metadata['locations'])):
            # locations needs to be a non-empty list
            apihandler.send_error(
                400,
                reason='member `locations` must be a list with at least one entry',
                file=apihandler.files_url
            )
            return False

        return True
