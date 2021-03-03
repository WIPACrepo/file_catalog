"""Test argbuilder.py functions."""

# pylint: disable=W0212

import pprint
from typing import Any, Dict, List, Optional, TypedDict, Union

from file_catalog import argbuilder


def test_00_path_args() -> None:
    """Test _handle_path_args."""

    class KwargsTests(TypedDict):  # pylint: disable=C0115
        kwargs_in: Dict[str, Any]
        kwargs_after: Dict[str, Any]
        ret: Optional[Union[Dict[str, Any], str]]

    kwargs_test_dicts: List[KwargsTests] = [
        # null case
        {"kwargs_in": {}, "kwargs_after": {}, "ret": None},
        # no path-args
        {
            "kwargs_in": {"an-extra-argument": [12, 34, 56]},
            "kwargs_after": {"an-extra-argument": [12, 34, 56]},
            "ret": None,
        },
        # only "path-regex"
        {
            "kwargs_in": {"path-regex": r"/reg-ex/this.*/(file/)?path"},
            "kwargs_after": {},
            "ret": {"$regex": r"/reg-ex/this.*/(file/)?path"},
        },
        # only "path"
        {
            "kwargs_in": {"an-extra-argument": [12, 34, 56], "path": "PATH"},
            "kwargs_after": {"an-extra-argument": [12, 34, 56]},
            "ret": "PATH",
        },
        # only "logical_name"
        {
            "kwargs_in": {"logical_name": "LOGICAL_NAME"},
            "kwargs_after": {},
            "ret": "LOGICAL_NAME",
        },
        # only "directory"
        {
            "kwargs_in": {"directory": "/path/to/dir/"},
            "kwargs_after": {},
            "ret": {"$regex": r"^/path/to/dir((/)|(/.*/)).*$"},
        },
        # only "directory" w/o trailing '/'
        {
            "kwargs_in": {"directory": "/path/to/dir"},
            "kwargs_after": {},
            "ret": {"$regex": r"^/path/to/dir((/)|(/.*/)).*$"},
        },
        # only "filename"
        {
            "kwargs_in": {"filename": "my-file"},
            "kwargs_after": {},
            "ret": {"$regex": r"^.*((/)|(/.*/))my-file$"},
        },
        # only "filename" w/ a sub-directory
        {
            "kwargs_in": {"filename": "/sub-dir/my-file"},
            "kwargs_after": {},
            "ret": {"$regex": r"^.*((/)|(/.*/))sub-dir/my-file$"},
        },
        # "directory" & "filename"
        {
            "kwargs_in": {"directory": "/path/to/dir/", "filename": "my-file"},
            "kwargs_after": {},
            "ret": {"$regex": r"^/path/to/dir((/)|(/.*/))my-file$"},
        },
    ]

    for ktd in kwargs_test_dicts:
        pprint.pprint(ktd)
        print()
        assert argbuilder._resolve_path_args(ktd["kwargs_in"]) == ktd["ret"]
        assert ktd["kwargs_in"] == ktd["kwargs_after"]

    # test multiple path-args (each loop pops the arg of the highest precedence)
    args = [  # list in decreasing order of precedence
        ("directory", "/path/to/dir/", {"$regex": r"^/path/to/dir((/)|(/.*/)).*$"}),
        # not testing "filename" b/c that is equal to "directory" in precedence
        ("logical_name", "LOGICAL_NAME", "LOGICAL_NAME"),
        ("path", "PATH", "PATH"),
        ("path-regex", r"this.*is?a.path", {"$regex": r"this.*is?a.path"}),
    ]
    while args:
        kwargs = {k: v for (k, v, _) in args}
        pprint.pprint(kwargs)
        assert argbuilder._resolve_path_args(kwargs) == args[0][2]
        assert not kwargs  # everything was popped
        args.pop(0)
