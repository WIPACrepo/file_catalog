"""Cast offline_processing_metadata.season FC-record values to integers."""


import argparse
import logging
from itertools import count
from typing import Any, cast, Dict, Generator, List

import coloredlogs  # type: ignore[import]
from rest_tools.client import RestClient  # type: ignore[import]

PAGE_SIZE = 10000

FCMetadata = Dict[str, Any]


def bad_fc_metadata(rc: RestClient) -> Generator[FCMetadata, None, None]:
    """Yield each FC entry that has a str-typed season value.

    Search will be halted either by a REST error, manually by the user,
    or when the FC has been exhausted.
    """

    def has_bad_season(fcm: FCMetadata) -> bool:
        try:
            return isinstance(fcm["offline_processing_metadata"]["season"], str)
        except KeyError:
            return False

    for page in count(0):
        logging.warning(
            f"Looking for more bad season values (page={page}, limit={PAGE_SIZE})..."
        )

        # Query
        body = {"start": page * PAGE_SIZE, "limit": PAGE_SIZE, "all-keys": True}
        # TODO -- think about query & whether I want to deal w/ FC-resp universe changing size
        resp = rc.request_seq("GET", "/api/files", body)
        fc_metas = cast(List[FCMetadata], resp["files"])

        # pre-check
        if not fc_metas:
            logging.error("No more files.")
            return
        if len(fc_metas) != PAGE_SIZE:
            logging.error(f"Asked for {PAGE_SIZE} files, received {len(fc_metas)}")

        # get bads
        bad_fc_metas = [fcm for fcm in fc_metas if has_bad_season(fcm)]
        if not bad_fc_metas:
            logging.error("No bad metadata found in page.")
            continue

        for fcm in bad_fc_metas:
            logging.warning(f"PAGE-{page}")
            yield fcm


def patch_catalog_entries(rc: RestClient, dryrun: bool = False) -> int:
    """Patch each FC entry that has a str-typed season value."""
    i = 0
    for i, bad_fcm in enumerate(bad_fc_metadata(rc), start=1):
        patched_fcm = bad_fcm  # TODO -- patch file - can I just send the one field?

        # patch!
        if dryrun:
            logging.error(
                f"Dry-Run Enabled: Not PATCHING'ing File Catalog entry! i={i}  -- {bad_fcm['uuid']}"
            )
        else:
            rc.request_seq("PATCH", f"/api/files/{bad_fcm['uuid']}", patched_fcm)
            logging.warning(f"PATCHED #{i} -- {bad_fcm['uuid']}")

    return i


def main() -> None:
    """Do Main."""
    # Args
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--token", required=True, help="file catalog token")
    parser.add_argument(
        "--dryrun",
        default=False,
        action="store_true",
        help="do everything except patching File Catalog entries",
    )
    parser.add_argument("-l", "--log", default="WARNING", help="output logging level")
    args = parser.parse_args()

    coloredlogs.install(level=args.log)
    rc = RestClient("https://file-catalog.icecube.wisc.edu/", token=args.token)

    # Go
    total_patched = patch_catalog_entries(rc, args.dryrun)
    if not total_patched:
        raise Exception("No FC entries found/patched")
    else:
        logging.warning(f"Total Patched: {total_patched}")
