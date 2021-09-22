"""Utility functions for finding a given filepath in the FC."""

import os
from typing import Any, List, Optional

from .schema import types


def _is_conflict(uuid: Optional[str], file_found: types.Metadata) -> bool:
    # if no file was found, then no problem
    if not file_found:
        return False

    # if no uuid was provided, then any match is a conflict
    if not uuid:
        return True

    # if the file we got isn't the one we're trying to update, that's a conflict
    return file_found["uuid"] != uuid


async def file_version_already_in_db(
    apihandler: Any,
    logical_name: Optional[str],
    checksum: Optional[types.Checksum],
    ignore_uuid: Optional[str] = None,
) -> bool:
    """Return whether the file-version is already in the database.

    A "file-version" is defined as the unique combination of a
    `logical_name` and a `checksum`.

    Pass in `ignore_uuid` to disregard matches (records) with this uuid.

    If it is found to already be in the DB, send 409 error.
    """
    if not logical_name:
        return False
    if not checksum:
        return False

    # try to load a file by that file-version
    file_found = await apihandler.db.get_file(
        {"logical_name": logical_name, "checksum": checksum}
    )
    # if we got a file by that file-version
    if _is_conflict(ignore_uuid, file_found):
        # then that file-version belongs to another file (already exists)
        apihandler.send_error(
            409,
            reason=(
                f"Conflict with existing file-version"
                f" ('logical_name' + 'checksum' already exists:"
                f"`{logical_name}` + `{checksum}`)"
            ),
            file=os.path.join(apihandler.files_url, file_found["uuid"]),
        )
        return True

    return False


async def any_location_already_in_db(
    apihandler: Any,
    locations: Optional[List[types.LocationEntry]],
    ignore_uuid: Optional[str] = None,
) -> bool:
    """Return whether any of the given locations are already in the database.

    Pass in `ignore_uuid` to disregard matches (records) with this uuid.

    If any are found to already be in the DB, send 409 error.
    """
    if not locations:
        return False

    # for each location provided
    for loc in locations:
        # try to load a file by that location
        file_found = await apihandler.db.get_file({"locations": {"$elemMatch": loc}})
        # if we got a file by that location
        if _is_conflict(ignore_uuid, file_found):
            # then that location belongs to another file (already exists)
            apihandler.send_error(
                409,
                reason=f"Conflict with existing file (location already exists `{loc['path']}`)",
                file=os.path.join(apihandler.files_url, file_found["uuid"]),
                location=loc,
            )
            return True

    return False
