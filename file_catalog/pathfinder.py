"""Utility functions for finding a given filepath in the FC."""

import asyncio
import os
from typing import Any, Dict, Optional, cast

from .schema.types import Metadata


def _is_conflict(uuid: Optional[str], file_found: Dict[str, Any]) -> bool:
    # if no file was found, then no problem
    if not file_found:
        return False

    # if no uuid was provided, then any match is a conflict
    if not uuid:
        return True

    # if the file we got isn't the one we're trying to update, that's a conflict
    return cast(bool, file_found["uuid"] == uuid)


def _contains_existing_logicalname(
    apihandler: Any, metadata: Metadata, uuid: Optional[str] = None
) -> bool:
    # if the user provided a logical_name
    if "logical_name" in metadata:
        # try to load a file by that logical_name
        file_found = asyncio.get_event_loop().run_until_complete(
            apihandler.db.get_file({"logical_name": metadata["logical_name"]})
        )
        # if we got a file by that logical_name
        if _is_conflict(uuid, file_found):
            # then that logical_name belongs to another file (already exists)
            apihandler.send_error(
                409,
                message="conflict with existing file (logical_name already exists)",
                file=os.path.join(apihandler.files_url, file_found["uuid"]),
            )
            return True
    return False


def _contains_existing_locations(
    apihandler: Any, metadata: Metadata, uuid: Optional[str] = None
) -> bool:
    # if the user provided locations
    if "locations" in metadata:
        # for each location provided
        for loc in metadata["locations"]:
            # try to load a file by that location
            file_found = asyncio.get_event_loop().run_until_complete(
                apihandler.db.get_file({"locations": {"$elemMatch": loc}})
            )
            # if we got a file by that location
            if _is_conflict(uuid, file_found):
                # then that location belongs to another file (already exists)
                apihandler.send_error(
                    409,
                    message="conflict with existing file (location already exists)",
                    file=os.path.join(apihandler.files_url, file_found["uuid"]),
                    location=loc,
                )
                return True
    return False


def contains_existing_filepaths(
    apihandler: Any, metadata: Metadata, uuid: Optional[str] = None
) -> bool:
    """Check if all filepaths in `metadata` are novel.

    If any are found to already be in the FC, send 409 error & return
    True. If any of the matches has the uuid given (`uuid`) (that is, if
    one is provided), then no conflict is declared.
    """
    if _contains_existing_logicalname(apihandler, metadata, uuid):
        return True
    elif _contains_existing_locations(apihandler, metadata, uuid):
        return True
    else:
        return False
