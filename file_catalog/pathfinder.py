"""Utility functions for finding a given filepath in the FC."""

import asyncio
import os
from typing import Any

from .schema.types import Metadata


def _has_conflicting_logicalname(
    apihandler: Any, uuid: str, metadata: Metadata
) -> bool:
    # if the user provided a logical_name
    if "logical_name" in metadata:
        # try to load a file by that logical_name
        check = asyncio.get_event_loop().run_until_complete(
            apihandler.db.get_file({"logical_name": metadata["logical_name"]})
        )
        # if we got a file by that logical_name
        if check:
            # if the file we got isn't the one we're trying to update
            if check["uuid"] != uuid:
                # then that logical_name belongs to another file (already exists)
                apihandler.send_error(
                    409,
                    message="conflict with existing file (logical_name already exists)",
                    file=os.path.join(apihandler.files_url, check["uuid"]),
                )
                return True
    return False


def _has_conflicting_locations(apihandler: Any, uuid: str, metadata: Metadata) -> bool:
    # if the user provided locations
    if "locations" in metadata:
        # for each location provided
        for loc in metadata["locations"]:
            # try to load a file by that location
            check = asyncio.get_event_loop().run_until_complete(
                apihandler.db.get_file({"locations": {"$elemMatch": loc}})
            )
            # if we got a file by that location
            if check:
                # if the file we got isn't the one we're trying to update
                if check["uuid"] != uuid:
                    # then that location belongs to another file (already exists)
                    apihandler.send_error(
                        409,
                        message="conflict with existing file (location already exists)",
                        file=os.path.join(apihandler.files_url, check["uuid"]),
                        location=loc,
                    )
                    return True
    return False


def has_conflicting_filepaths(apihandler: Any, uuid: str, metadata: Metadata) -> bool:
    """Check if all filepaths in `metadata` are novel.

    If any are found to already be in the FC, send 409 error & return
    True.
    """
    if _has_conflicting_logicalname(apihandler, uuid, metadata):
        return True
    elif _has_conflicting_locations(apihandler, uuid, metadata):
        return True
    else:
        return False
