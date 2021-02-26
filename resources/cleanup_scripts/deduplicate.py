"""Cleanup near duplicate File Catalog records...

where the duplicate is indexed under /mnt/lfs*/.
"""


import argparse
import json
import logging
from itertools import count
from typing import Any, cast, Dict, Generator, List, Set

import coloredlogs  # type: ignore[import]
import dateutil.parser
from rest_tools.client import RestClient  # type: ignore[import]

PAGE_SIZE = 10000

FCMetadata = Dict[str, Any]


def _find_fc_metadata(rc: RestClient, logical_name: str) -> FCMetadata:
    try:
        body = {"query": json.dumps({"logical_name": logical_name}), "all-keys": True}
        resp = rc.request_seq("GET", "/api/files", body)
        fc_metas = cast(List[FCMetadata], resp["files"])
    except KeyError:
        raise FileNotFoundError
    if not fc_metas:
        raise FileNotFoundError
    if len(fc_metas) > 1:
        raise Exception(f"Multiple FC matches for {logical_name}")

    return fc_metas[0]


def remove_prefix(string: str, prefix: str) -> str:
    """Return string without the given prefix."""
    return string[len(prefix) :]


def _get_good_path(fpath: str) -> str:

    replacement_roots = [
        ("/mnt/lfs6/exp/", "/data/exp/"),
        ("/mnt/lfs6/sim/", "/data/sim/"),
    ]

    for bad_root, good_root in replacement_roots:
        if fpath.startswith(bad_root):
            return good_root + remove_prefix(fpath, bad_root)

    raise Exception(f"Unaccounted for prefix: {fpath}")


def _compatible_locations_values(
    evil_twin_locations: List[Dict[str, str]], good_twin_locations: List[Dict[str, str]]
) -> bool:
    # are these the same?
    if evil_twin_locations == good_twin_locations:
        return True

    # does the evil twin have any locations that the good twin does not?
    # the good twin can have more locations--AKA it's been moved to NERSC
    for evil_locus in evil_twin_locations:
        if evil_locus not in good_twin_locations:
            return False

    return True


def _compare_twins(
    evil_twin: FCMetadata, good_twin: FCMetadata, ignored_fields: List[str]
) -> bool:
    keys = set(list(evil_twin.keys()) + list(good_twin.keys()))

    for key in keys:
        if key in ignored_fields:
            continue
        if evil_twin[key] != good_twin[key]:
            logging.error(
                f"Field Mismatch: {key} (evil_twin={evil_twin[key]}, good_twin={good_twin[key]})"
            )
            return False

    return True


def _evil_twin_was_updated_later(evil_twin: FCMetadata, good_twin: FCMetadata) -> bool:
    evil_twin_time = dateutil.parser.isoparse(evil_twin["meta_modify_date"])
    good_twin_time = dateutil.parser.isoparse(good_twin["meta_modify_date"])

    return evil_twin_time > good_twin_time


def _resolve_deprecated_fields(fc_meta: FCMetadata) -> FCMetadata:
    deprecated_fields = [
        ("end_datetime", "end_datetime"),
        ("first_event", "first_event"),
        ("last_event", "last_event"),
        ("run_number", "run_number"),
        ("start_datetime", "start_datetime"),
        ("subrun_number", "subrun_number"),
        ("events", "event_count"),
    ]
    for old_field, run_field in deprecated_fields:
        if old_field not in fc_meta:
            continue
        if run_field not in fc_meta["run"]:
            raise Exception(
                f"Deprecated field: {run_field} is not also in 'run' object"
            )
        elif fc_meta[old_field] != fc_meta["run"][run_field]:
            raise Exception(
                f"Deprecated field: {old_field} has differing value than 'run' object "
                f"({fc_meta[old_field]} vs {fc_meta['run'][run_field]})"
            )
        del fc_meta[old_field]

    return fc_meta


def _resolve_gcd_filepath(fc_meta: FCMetadata) -> FCMetadata:
    if "offline_processing_metadata" in fc_meta:
        path = _get_good_path(fc_meta["offline_processing_metadata"]["L2_gcd_file"])
        fc_meta["offline_processing_metadata"]["L2_gcd_file"] = path
    return fc_meta


def _resolve_wipac_location_filepath(fc_meta: FCMetadata) -> FCMetadata:
    # replace WIPAC-site path (these will differ b/c they are the logical_name)
    for i in range(len(fc_meta["locations"])):  # pylint: disable=C0200
        # allow in-line changes
        if fc_meta["locations"][i]["site"] == "WIPAC":
            path = _get_good_path(fc_meta["locations"][i]["path"])
            fc_meta["locations"][i]["path"] = path
    return fc_meta


def _resolve_season_value(fc_meta: FCMetadata) -> FCMetadata:
    if "offline_processing_metadata" in fc_meta:
        season = fc_meta["offline_processing_metadata"]["season"]
        fc_meta["offline_processing_metadata"]["season"] = int(season)
    return fc_meta


def has_good_twin(rc: RestClient, evil_twin: FCMetadata) -> bool:
    """Return whether the `evil_twin` has a good twin.

    If the two sets of metadata are not compatible, raise an Exception.
    """
    try:
        good_twin = _find_fc_metadata(rc, _get_good_path(evil_twin["logical_name"]))
    except FileNotFoundError:
        return False

    # resolve special fields
    evil_twin = _resolve_deprecated_fields(evil_twin)
    evil_twin = _resolve_gcd_filepath(evil_twin)
    evil_twin = _resolve_wipac_location_filepath(evil_twin)
    #
    evil_twin = _resolve_season_value(evil_twin)
    good_twin = _resolve_season_value(good_twin)

    # compare metadata
    try:
        # compare "locations"-fields
        if not _compatible_locations_values(
            evil_twin["locations"], good_twin["locations"]
        ):
            raise Exception("Locations metadata not compatible")

        # compare "meta_modify_date"-fields
        if _evil_twin_was_updated_later(evil_twin, good_twin):
            raise Exception("Evil twin was updated after the good twin")

        # compare basic fields
        ignored_fields = [
            "_links",
            "logical_name",
            "uuid",
            "locations",
            "meta_modify_date",
        ]
        if not _compare_twins(evil_twin, good_twin, ignored_fields):
            raise Exception(f"Fields don't match (disregarding: {ignored_fields})")

    except Exception:
        logging.critical(f"evil_twin={evil_twin}")
        logging.critical(f"good_twin={good_twin}")
        raise

    return True


def bad_fc_metadata(rc: RestClient) -> Generator[FCMetadata, None, None]:
    """Yield each FC entry (w/ full metadata) rooted at /mnt/lfs*/.

    Search will be halted either by a REST error, manually by the user,
    or when the FC has been exhausted.
    """
    previous_uuids: Set[Dict[str, Any]] = set()

    def check_paths(fc_metas: List[FCMetadata]) -> None:
        for fcm in fc_metas:
            if not fcm["logical_name"].startswith("/mnt/lfs"):
                raise RuntimeError(f"Wrong path! (doesn't start with /mnt/lfs) {fcm}")

    # infinite querying (break when no more files)
    page_seek = 0  # start at the first page b/c will delete from front of queue
    for num in count(1):
        logging.info(
            f"Looking for more bad-rooted paths "
            f"(Query #{num}, limit={PAGE_SIZE}, page_seek={page_seek})..."
        )

        # Query
        body = {
            "start": page_seek * PAGE_SIZE,
            "limit": PAGE_SIZE,
            "all-keys": True,
            "query": json.dumps({"logical_name": {"$regex": r"^\/mnt\/lfs.*"}}),
        }
        resp = rc.request_seq("GET", "/api/files", body)
        fc_metas = cast(List[FCMetadata], resp["files"])

        # pre-check
        if not fc_metas:
            logging.warning("No more files.")
            return
        if len(fc_metas) != PAGE_SIZE:
            logging.warning(f"Asked for {PAGE_SIZE} files, received {len(fc_metas)}")
        check_paths(fc_metas)
        if set(f["uuid"] for f in fc_metas) == previous_uuids:
            logging.warning(
                "This page is the same as the previous page "
                "(that portion of the queue was not cleared)."
            )
            page_seek += 1
            logging.warning(f"Now seeking to page #{page_seek}...")
            continue
        previous_uuids = set(f["uuid"] for f in fc_metas)

        # yield
        for fcm in fc_metas:
            logging.info(f"Query #{num} (Page-Seek {page_seek})")
            yield fcm


def delete_evil_twin_catalog_entries(rc: RestClient, dryrun: bool = False) -> int:
    """Delete each bad-rooted path FC entry (if each has a good twin)."""
    i = 0
    with open("unmatched.paths", "a+") as unmatched_f:
        for i, bad_fcm in enumerate(bad_fc_metadata(rc), start=1):
            uuid = bad_fcm["uuid"]
            logging.info(f"Bad path #{i}: {bad_fcm['logical_name']}")

            if not has_good_twin(rc, bad_fcm):
                logging.error(
                    f"No good twin found -- appending logical name to {unmatched_f.name}"
                )
                print(bad_fcm["logical_name"], file=unmatched_f)
                continue
            # sanity check -- this is the point of no return
            if uuid != bad_fcm["uuid"]:
                raise Exception(f"uuid was changed ({uuid}) vs ({bad_fcm['uuid']}).")

            # delete!
            if dryrun:
                logging.error(
                    f"Dry-Run Enabled: Not DELETE'ing File Catalog entry! i={i}  -- {uuid}"
                )
            else:
                rc.request_seq("DELETE", f"/api/files/{uuid}")
                logging.info(f"DELETED #{i} -- {uuid}")

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
        help="do everything except deleting File Catalog entries. "
        "NOTE: since the FC will remain the same size, "
        '"GET" @ "/api/files" will continue to return the same entries.',
    )
    parser.add_argument("-l", "--log", default="INFO", help="output logging level")
    args = parser.parse_args()

    coloredlogs.install(level=args.log)
    rc = RestClient("https://file-catalog.icecube.wisc.edu/", token=args.token)

    # Go
    total_deleted = delete_evil_twin_catalog_entries(rc, args.dryrun)
    if not total_deleted:
        raise Exception("No FC entries found/deleted")
    else:
        logging.info(f"Total Deleted: {total_deleted}")


if __name__ == "__main__":
    main()
