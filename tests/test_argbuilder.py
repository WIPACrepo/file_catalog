"""Test argbuilder.py functions."""

# pylint: disable=W0212

import pprint
from typing import Any, Dict, List, TypedDict

from file_catalog import argbuilder


def test_00_path_args() -> None:
    """Test _handle_path_args."""

    class KwargsTests(TypedDict):  # pylint: disable=C0115
        kwargs_in: Dict[str, Any]
        kwargs_after: Dict[str, Any]

    kwargs_test_dicts: List[KwargsTests] = [
        # null case
        {"kwargs_in": {}, "kwargs_after": {}},
        # no path-args
        {
            "kwargs_in": {"an-extra-argument": [12, 34, 56]},
            "kwargs_after": {"an-extra-argument": [12, 34, 56]},
        },
        # only "path-regex"
        {
            "kwargs_in": {"path-regex": r"/reg-ex/this.*/(file/)?path"},
            "kwargs_after": {
                "logical_name": {"$regex": r"/reg-ex/this.*/(file/)?path"}
            },
        },
        # only "path"
        {
            "kwargs_in": {"an-extra-argument": [12, 34, 56], "path": "PATH"},
            "kwargs_after": {"an-extra-argument": [12, 34, 56], "logical_name": "PATH"},
        },
        # only "logical_name"
        {
            "kwargs_in": {"logical_name": "LOGICAL_NAME"},
            "kwargs_after": {"logical_name": "LOGICAL_NAME"},
        },
        # only "directory"
        {
            "kwargs_in": {"directory": "/path/to/dir/"},
            "kwargs_after": {
                "logical_name": {"$regex": r"^/path/to/dir((/)|(/.*/)).*$"},
            },
        },
        # only "directory" w/o trailing '/'
        {
            "kwargs_in": {"directory": "/path/to/dir"},
            "kwargs_after": {
                "logical_name": {"$regex": r"^/path/to/dir((/)|(/.*/)).*$"},
            },
        },
        # only "filename"
        {
            "kwargs_in": {"filename": "my-file"},
            "kwargs_after": {"logical_name": {"$regex": r"^.*((/)|(/.*/))my-file$"}},
        },
        # only "filename" w/ a sub-directory
        {
            "kwargs_in": {"filename": "/sub-dir/my-file"},
            "kwargs_after": {
                "logical_name": {"$regex": r"^.*((/)|(/.*/))sub-dir/my-file$"}
            },
        },
        # "directory" & "filename"
        {
            "kwargs_in": {"directory": "/path/to/dir/", "filename": "my-file"},
            "kwargs_after": {
                "logical_name": {"$regex": r"^/path/to/dir((/)|(/.*/))my-file$"}
            },
        },
    ]

    for ktd in kwargs_test_dicts:
        pprint.pprint(ktd)
        print()
        argbuilder._resolve_path_args(ktd["kwargs_in"])
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
        argbuilder._resolve_path_args(kwargs)
        assert kwargs == {"logical_name": args[0][2]}
        args.pop(0)
