"""Utility functions for avoiding conflicts in the FC."""

import os
from typing import Any, AsyncGenerator, List, Optional, Tuple

from wipac_telemetry import tracing_tools as wtt

from .mongo import Mongo
from .schema import types


def _is_conflict(uuid: Optional[str], file_found: Optional[types.Metadata]) -> bool:
    # if no file was found, then no problem
    if not file_found:
        return False

    # if no uuid was provided, then any match is a conflict
    if not uuid:
        return True

    # if the file we got isn't the one we're trying to update, that's a conflict
    return file_found["uuid"] != uuid


class IndeterminateFileVersionError(Exception):
    """Raised when the file-version cannot be determined from the given parameters."""


class FileVersion:
    """Encapsulate the file-version representation for a metadata entry."""

    def __init__(self, metadata: types.Metadata):
        try:
            self.logical_name = metadata["logical_name"]
            self.checksum_sha512 = metadata["checksum"]["sha512"]
        except KeyError as e:
            raise IndeterminateFileVersionError() from e

    @wtt.spanned(all_args=True)
    async def is_in_db(self, apihandler: Any, skip: Optional[str] = None) -> bool:
        """Return whether the file-version is already in the database.

        A "file-version" is defined as the unique combination of a
        `logical_name` and a `checksum.sha512`.

        Pass in `skip` to disregard matches (records) with this uuid.

        If it is found to already be in the DB, send 409 error.
        """
        # try to load a file by that file-version
        from_db = await apihandler.db.get_file(
            {"logical_name": self.logical_name, "checksum.sha512": self.checksum_sha512}
        )
        # if we got a file by that file-version
        if _is_conflict(skip, from_db):
            # then that file-version belongs to another file (already exists)
            apihandler.send_error(
                409,
                reason=(
                    f"Conflict with existing file-version"
                    f" ('logical_name' + 'checksum.sha512' already exists:"
                    f"`{self.logical_name}` + `{self.checksum_sha512}`)"
                ),
                file=os.path.join(apihandler.files_url, from_db["uuid"]),
            )
            return True

        return False


@wtt.spanned(all_args=True)
async def find_each_location_in_db(
    db: Mongo,
    locations: List[types.LocationEntry],
) -> AsyncGenerator[Tuple[types.LocationEntry, Optional[types.Metadata]], None]:
    """Yield each location entry with its database metadata file (or `None`)."""
    for loc in locations:
        # try to load a file by that location
        from_db = await db.get_file({"locations": {"$elemMatch": loc}})
        yield loc, from_db


def send_location_conflict_error(
    apihandler: Any, loc: types.LocationEntry, uuid: str
) -> None:
    """Send standard error message for a location conflict."""
    apihandler.send_error(
        409,
        reason=f"Conflict with existing file (location already exists `{loc['path']}`)",
        file=os.path.join(apihandler.files_url, uuid),
        location=loc,
    )


@wtt.spanned(all_args=True)
async def any_location_in_db(
    apihandler: Any,
    locations: Optional[List[types.LocationEntry]],
    skip: Optional[str] = None,
) -> bool:
    """Return whether any of the given locations are already in the database.

    Pass in `skip` to disregard matches (records) with this uuid.

    If any are found to already be in the DB, send 409 error.
    """
    if not locations:
        return False

    # for each location provided
    async for loc, from_db in find_each_location_in_db(apihandler.db, locations):
        # if we got a file by that location
        if from_db and _is_conflict(skip, from_db):
            # then that location belongs to another file (already exists)
            send_location_conflict_error(apihandler, loc, from_db["uuid"])
            return True

    return False
