"""Builder utility functions for arg/kwargs dicts."""


from typing import Any, Dict, Optional, Union

from tornado.escape import json_decode

from file_catalog.mongo import AllKeys


def build_limit(kwargs: Dict[str, Any], config: Dict[str, Any]) -> None:
    """Build the `"limit"` argument."""
    if "limit" in kwargs:
        kwargs["limit"] = int(kwargs["limit"])
        if kwargs["limit"] < 1:
            raise Exception("limit is not positive")

        # check with config
        if kwargs["limit"] > config["FC_QUERY_FILE_LIST_LIMIT"]:
            kwargs["limit"] = config["FC_QUERY_FILE_LIST_LIMIT"]
    else:
        # if no limit has been defined, set max limit
        kwargs["limit"] = config["FC_QUERY_FILE_LIST_LIMIT"]


def build_start(kwargs: Dict[str, Any]) -> None:
    """Build the `"start"` argument."""
    if "start" in kwargs:
        kwargs["start"] = int(kwargs["start"])
        if kwargs["start"] < 0:
            raise Exception("start is negative")


def _resolve_name_args(kwargs: Dict[str, Any]) -> Optional[Union[Dict[str, Any], str]]:
    """Resolve the name-type shortcut arguments by precedence.

    Pop each key from `kwargs`, even if it's not used.
    """
    arg: Optional[Union[Dict[str, Any], str]] = None

    # regex
    if "logical-name-regex" in kwargs:
        arg = {"$regex": kwargs.pop("logical-name-regex")}

    # normal path
    if "logical_name" in kwargs:
        arg = kwargs.pop("logical_name")

    # directory & filename
    if "directory" in kwargs or "filename" in kwargs:
        if not (directory := kwargs.pop("directory", "").rstrip("/")):
            directory = r".*"
        if not (fname := kwargs.pop("filename", "").lstrip("/")):
            fname = r".*"
        arg = {"$regex": rf"^{directory}/(.*/)?{fname}$"}

    return arg


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
    if path := _resolve_name_args(kwargs):
        query["logical_name"] = path
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
    """Build `"keys"` list, potentially using `"all-keys"`.

    Pop `"all-keys"`.
    """
    use_all_keys = kwargs.pop("all-keys", None) in ["True", "true", 1]

    if use_all_keys:
        kwargs["keys"] = AllKeys()
    elif "keys" in kwargs:
        kwargs["keys"] = kwargs["keys"].split("|")
