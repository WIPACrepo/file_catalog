"""Builder utility functions for arg/kwargs dicts."""

from typing import Any, Dict

from tornado.escape import json_decode

# local imports
from file_catalog.mongo import AllKeys


def build_files_query(kwargs: Dict[str, Any]) -> None:
    """Build `"query"` dict with formatted/fully-named arguments.

    Pop corresponding shortcut-keys from `kwargs`.
    """
    if "query" in kwargs:
        # keep whatever was already in here, then add to it
        if isinstance(kwargs["query"], (str, bytes)):
            query = json_decode(kwargs.pop("query"))
        else:
            query = kwargs.pop("query")
    else:
        query = {}

    if "locations.archive" not in query:
        query["locations.archive"] = None

    # shortcut query params
    if "logical_name" in kwargs:
        query["logical_name"] = kwargs.pop("logical_name")
    if "run_number" in kwargs:
        query["run.run_number"] = kwargs.pop("run_number")
    if "dataset" in kwargs:
        query["iceprod.dataset"] = kwargs.pop("dataset")
    if "event_id" in kwargs:
        e = kwargs.pop("event_id")
        query["run.first_event"] = {"$lte": e}
        query["run.last_event"] = {"$gte": e}
    if "processing_level" in kwargs:
        query["processing_level"] = kwargs.pop("processing_level")
    if "season" in kwargs:
        query["offline_processing_metadata.season"] = kwargs.pop("season")

    kwargs["query"] = query


def build_keys(kwargs: Dict[str, Any]) -> None:
    """Build `"keys"` list, potentially using `"all-keys"` keyword."""
    if "keys" not in kwargs:
        return

    if kwargs.get("all-keys", None):
        kwargs["keys"] = AllKeys()
    else:
        kwargs["keys"] = kwargs["keys"].split("|")
