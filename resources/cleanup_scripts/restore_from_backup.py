"""Restore FC entries via their JSON backups."""


import argparse
import asyncio
import json
import logging
from typing import Any, Dict, List, cast

import coloredlogs  # type: ignore[import]
import requests
from rest_tools.client import RestClient  # type: ignore[import]

FCMetadata = Dict[str, Any]


POP_KEYS = ["meta_modify_date", "_id"]


async def already_in_fc(rc: RestClient, uuid: str) -> bool:
    """Return whether the uuid is already in the FC."""
    try:
        await rc.request("GET", "/api/files/" + uuid)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            return False
        else:
            raise
    return True


async def restore(rc: RestClient, fc_entries: List[FCMetadata], dryrun: bool) -> None:
    """Send the fc_entries one-by-one to the FC.

    PUT if FC entry already exists. Otherwise, POST with uuid.
    """
    for i, fcm in enumerate(fc_entries):
        print(f"{i}/{len(fc_entries)}")
        logging.debug(fcm)
        if dryrun:
            continue
        if await already_in_fc(rc, fcm["uuid"]):
            logging.info(
                f"Entry is already in the FC ({fcm['uuid']}); Replacing (PUT)..."
            )
            await rc.request("PUT", f'/api/files/{fcm["uuid"]}', fcm)
        else:
            logging.info(
                f"Entry is not already in the FC ({fcm['uuid']}); Posting (POST)..."
            )
            await rc.request("POST", "/api/files", fcm)


def get_fc_entries(file: str) -> List[FCMetadata]:
    """Parse out the FC metadata/entries from the JSON file."""

    def parse(line: str) -> FCMetadata:
        fc_meta = cast(FCMetadata, json.loads(line.strip()))
        for key in POP_KEYS:
            fc_meta.pop(key, None)
        if "uuid" not in fc_meta:
            raise Exception(f"FC entry is missing a `uuid` field: {fc_meta}")
        logging.debug(fc_meta)
        return fc_meta

    fc_entries = [parse(ln) for ln in open(file)]
    logging.info(f"Parsed {len(fc_entries)} FC entries from {file}")
    return fc_entries


def main() -> None:
    """Do Main."""
    # Args
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=f"Restores FC entries from a JSON file (new-line delimited). "
        f"The contents of each entry is not altered. "
        f"However, these keys are removed prior to sending to the File Catalog: {POP_KEYS}. "
        f"Each entry must include a `uuid` field.",
    )
    parser.add_argument(
        "--json", required=True, help="JSON backup file (new-line delimited)"
    )
    parser.add_argument("--token", required=True, help="file catalog token")
    parser.add_argument("--timeout", type=int, default=3600, help="REST-client timeout")
    parser.add_argument("--retries", type=int, default=24, help="REST-client retries")
    parser.add_argument("--dryrun", default=False, action="store_true")
    parser.add_argument("-l", "--log", default="INFO", help="output logging level")
    args = parser.parse_args()

    coloredlogs.install(level=args.log)
    for arg, val in vars(args).items():
        logging.warning(f"{arg}: {val}")

    rc = RestClient(
        "https://file-catalog.icecube.wisc.edu/",
        token=args.token,
        timeout=args.timeout,
        retries=args.retries,
    )

    fc_entries = get_fc_entries(args.json)
    asyncio.get_event_loop().run_until_complete(restore(rc, fc_entries, args.dryrun))

    logging.info("Done.")


if __name__ == "__main__":
    main()
