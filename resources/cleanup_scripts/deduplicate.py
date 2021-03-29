"""Cleanup near duplicate File Catalog records...

where the duplicate is indexed under /mnt/lfs*/.
"""


import argparse
import json
import logging
import os
from itertools import count
from typing import Any, Dict, Generator, List, Set, cast

import coloredlogs  # type: ignore[import]
from dateutil.parser import isoparse
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
        ("/mnt/lfs7/exp/", "/data/exp/"),
        ("/mnt/lfs7/sim/", "/data/sim/"),
    ]

    for bad_root, good_root in replacement_roots:
        if fpath.startswith(bad_root):
            return good_root + remove_prefix(fpath, bad_root)

    raise Exception(f'Unaccounted for prefix: "{fpath}"')


def _is_subset(minor: List[Any], master: List[Any]) -> bool:
    if minor == master:
        return True

    # does minor have any extra things that master does not?
    for thing in minor:
        if thing not in master:
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
            logging.error(f"Field Mismatch: {key}")
            logging.error(f"evil_twin.{key}:{evil_twin[key]}")
            logging.error(f"good_twin.{key}:{good_twin[key]}")
            return False

    return True


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
        gcd = fc_meta["offline_processing_metadata"]["L2_gcd_file"]
        if gcd:  # L2_gcd_file could be ""
            fc_meta["offline_processing_metadata"]["L2_gcd_file"] = _get_good_path(gcd)
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
    """Cast `"season"` value to an `int` if it's not `None`."""
    if "offline_processing_metadata" in fc_meta:
        season = fc_meta["offline_processing_metadata"]["season"]
        if fc_meta["offline_processing_metadata"]["season"] is not None:
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

    # compare `create_date` fields
    if "create_date" not in good_twin:
        raise Exception(
            f'Good twin doesn\'t have "create_date" field ({good_twin["logical_name"]})'
        )
    if "create_date" in evil_twin:
        if isoparse(good_twin["create_date"]) == isoparse(evil_twin["create_date"]):
            pass
        # if the good_twin was updated/fixed a bunch of fields won't match anyways
        elif isoparse(good_twin["create_date"]) > isoparse(evil_twin["create_date"]):
            return True
        else:
            raise Exception("Evil twin was created after good twin")

    # resolve special fields
    evil_twin = _resolve_deprecated_fields(evil_twin)
    evil_twin = _resolve_gcd_filepath(evil_twin)
    evil_twin = _resolve_wipac_location_filepath(evil_twin)
    #
    evil_twin = _resolve_season_value(evil_twin)
    good_twin = _resolve_season_value(good_twin)

    # compare metadata
    try:
        # compare "locations"-lists
        # the good twin can have more locations--AKA it's been moved to NERSC
        if not _is_subset(evil_twin["locations"], good_twin["locations"]):
            raise Exception("Locations lists not compatible")

        # compare "software"-lists
        if not _is_subset(evil_twin.get("software", []), good_twin.get("software", [])):
            raise Exception("Software lists not compatible")

        # compare "meta_modify_date"-fields
        if isoparse(evil_twin["meta_modify_date"]) > isoparse(
            good_twin["meta_modify_date"]
        ):
            raise Exception("Evil twin was updated after the good twin")

        # compare basic fields
        ignored_fields = [
            "_links",
            "logical_name",
            "uuid",
            "locations",
            "software",
            "meta_modify_date",
            "create_date",
        ]
        if not _compare_twins(evil_twin, good_twin, ignored_fields):
            raise Exception(f"Fields don't match (disregarding: {ignored_fields})")

    except Exception:
        logging.error(f"evil_twin={evil_twin}")
        logging.error(f"good_twin={good_twin}")
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


def path_still_exists(fc_meta: FCMetadata) -> bool:
    """Return whether the path still exists."""
    return os.path.exists(fc_meta["logical_name"])


DEDUP = "dedup-errors.paths"
UNMATCHED = "unmatched-missing.paths"


def delete_evil_twin_catalog_entries(rc: RestClient, dryrun: bool = False) -> int:
    """Delete each bad-rooted path FC entry (if each has a good twin)."""
    try:
        errors: List[str] = [ln.strip() for ln in open(DEDUP)]
    except FileNotFoundError:
        errors = []
    try:
        ignored: List[str] = [ln.strip() for ln in open("./mnt-nersc.paths")]
    except FileNotFoundError:
        ignored = []
    try:
        unmatched: List[str] = [ln.strip() for ln in open(UNMATCHED)]
    except FileNotFoundError:
        unmatched = []

    total_deleted = 0
    with open(UNMATCHED, "a+") as unmatched_f, open(DEDUP, "a+") as errors_f:
        for i, bad_fcm in enumerate(bad_fc_metadata(rc), start=1):
            uuid = bad_fcm["uuid"]
            logging.info(f"Bad path #{i}: {bad_fcm['logical_name']}")

            # have we already seen it?
            if bad_fcm["logical_name"] in ignored:
                logging.info("file is in `ignored` list, skipping")
                continue
            if bad_fcm["logical_name"] in unmatched:
                logging.info(f"already in {UNMATCHED}")
                continue
            if bad_fcm["logical_name"] in errors:
                logging.info(f"already in {DEDUP}")
                continue

            # guard rails
            try:
                if not has_good_twin(rc, bad_fcm):
                    unmatched.append(bad_fcm["logical_name"])
                    # is the path still active?
                    if path_still_exists(bad_fcm):  # pylint: disable=R1724
                        # write to file & skip
                        logging.error(
                            f"No good twin found (path still exists) "
                            f"-- appending logical name to {UNMATCHED}"
                        )
                        print(bad_fcm["logical_name"], file=unmatched_f)
                        continue
                    # okay, we can delete it
                    else:
                        logging.warning(
                            "No good twin found, but path no longer exists "
                            "-- so deleting anyways"
                        )
            except Exception as e:  # pylint: disable=W0703
                errors.append(bad_fcm["logical_name"])
                # write to file & skip
                logging.error(
                    f"`{type(e).__name__}:{e}` -- appending logical name to {DEDUP} ({bad_fcm['logical_name']})"
                )
                print(bad_fcm["logical_name"], file=errors_f)
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
                total_deleted += 1
                logging.info(f"DELETED #{i} (total deleted: {total_deleted}) -- {uuid}")

    return total_deleted


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
        help="do everything except deleting File Catalog entries. "
        "NOTE: since the FC will remain the same size, "
        '"GET" @ "/api/files" will continue to return the same entries.',
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

    # Go
    total_deleted = delete_evil_twin_catalog_entries(rc, args.dryrun)
    if not total_deleted:
        raise Exception("No FC entries found/deleted")
    else:
        logging.info(f"Total Deleted: {total_deleted}")

    logging.info("Done.")


if __name__ == "__main__":
    main()
