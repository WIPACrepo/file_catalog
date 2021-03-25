"""Cast offline_processing_metadata.season FC-record values to integers."""


import argparse
import json
import logging
from itertools import count
from typing import Any, Dict, Generator, List, Set, Tuple, cast

import coloredlogs  # type: ignore[import]
from rest_tools.client import RestClient  # type: ignore[import]

PAGE_SIZE = 10000

FCMetadata = Dict[str, Any]
OfflineProcessingMetadata = Dict[str, Any]


def get_offline_processing_metadata_w_str_season(
    rc: RestClient, str_season: str
) -> Generator[Tuple[str, OfflineProcessingMetadata], None, None]:
    """Yield each FC entry that has a str-typed season value.

    Search will be halted either by a REST error, manually by the user,
    or when the FC has been exhausted.
    """
    # type check str_season
    int(str_season)
    if not isinstance(str_season, str):
        raise TypeError("`str_season` must be a str")

    previous_uuids: Set[Dict[str, Any]] = set()

    def check_seasons(fc_metas: List[FCMetadata]) -> None:
        for fcm in fc_metas:
            if fcm["offline_processing_metadata"]["season"] != str_season:
                raise RuntimeError(f"Wrong season! (not {str_season}) {fcm}")

    # infinite querying (break when no more files)
    for num in count(1):
        logging.info(
            f'Looking for more "{str_season}" string-season entries (Query #{num}, limit={PAGE_SIZE})...'
        )

        # Query
        body = {
            "start": 0,  # always start at the first page b/c will delete from front of queue
            "limit": PAGE_SIZE,
            "keys": "uuid|offline_processing_metadata",
            "query": json.dumps({"offline_processing_metadata.season": str_season}),
        }
        resp = rc.request_seq("GET", "/api/files", body)
        fc_metas = cast(List[FCMetadata], resp["files"])

        # pre-check
        if not fc_metas:
            logging.warning("No more files.")
            return
        if len(fc_metas) != PAGE_SIZE:
            logging.warning(f"Asked for {PAGE_SIZE} files, received {len(fc_metas)}")
        check_seasons(fc_metas)
        if set(f["uuid"] for f in fc_metas) == previous_uuids:
            msg = "This page is the same as the previous page."
            logging.critical(msg)
            raise RuntimeError(msg)
        previous_uuids = set(f["uuid"] for f in fc_metas)

        # yield
        for fcm in fc_metas:
            logging.info(f'Season "{str_season}", Query #{num}')
            yield fcm["uuid"], fcm["offline_processing_metadata"]


def patch_fc_entries_seasons(
    rc: RestClient, str_season: str, dryrun: bool = False
) -> int:
    """Patch each FC entry that has a str-typed season value."""
    total_patched = 0
    logging.info(f'Looking at offline_processing_metadata.season="{str_season}"')

    for i, (uuid, op_meta) in enumerate(
        get_offline_processing_metadata_w_str_season(rc, str_season), start=1
    ):
        # fix
        op_meta["season"] = int(op_meta["season"])

        # patch!
        if dryrun:
            logging.error(
                f"Dry-Run Enabled: Not PATCHING'ing File Catalog entry! "
                f"i={i} -- {op_meta['season']} | {uuid} | {op_meta}"
            )
        else:
            rc.request_seq(
                "PATCH", f"/api/files/{uuid}", {"offline_processing_metadata": op_meta},
            )
            total_patched += 1
            logging.info(
                f"PATCHED #{i} (total patched: {total_patched}) -- {op_meta['season']} | {uuid}"
            )

    return total_patched


def main() -> None:
    """Do Main."""
    # Args
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--token", required=True, help="file catalog token")
    parser.add_argument("--timeout", type=int, default=3600, help="REST-client timeout")
    parser.add_argument("--retries", type=int, default=24, help="REST-client retries")
    parser.add_argument(
        "--dryrun",
        default=False,
        action="store_true",
        help="do everything except patching File Catalog entries",
    )
    parser.add_argument("-l", "--log", default="INFO", help="output logging level")
    args = parser.parse_args()

    coloredlogs.install(level=args.log)
    rc = RestClient(
        "https://file-catalog.icecube.wisc.edu/",
        token=args.token,
        timeout=args.timeout,
        retries=args.retries,
    )

    # Find & Patch by Season
    patch_totals: Dict[str, int] = {}
    for int_season in range(2000, 2025):
        total_patched = patch_fc_entries_seasons(rc, str(int_season), args.dryrun)
        logging.warning(f'Total Patched (Season="{int_season}"): {total_patched}')
        patch_totals[str(int_season)] = total_patched

    logging.warning(f"Seasons Patched: {patch_totals}")
    logging.warning(f"Grand Total Patched: {sum(tot for tot in patch_totals.values())}")


if __name__ == "__main__":
    main()
