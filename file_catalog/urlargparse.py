# urlargparse.py
# See: https://api.jquery.com/jQuery.param/
"""Encode and parse URL args in jQuery.param format."""

from copy import deepcopy
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

from tornado.escape import url_escape, url_unescape


class SubscriptType(Enum):
    END = 1    # =
    INDEX = 2  # [2]
    KEY = 3    # [b]


Args = Dict[str, Any]
Subscript = Tuple[SubscriptType, str, SubscriptType]  # this-type, this-key, next-type


def decode_value(value: str) -> Optional[Union[float, int, str]]:
    """Convert a value the way jQuery does."""
    if value == "":
        return None
    try:
        if float(value) == int(value):
            return int(value)
    except Exception:
        try:
            return float(value)
        except Exception:
            try:
                return int(value)
            except Exception:
                pass
    return str(value)


def encode(args: Args) -> str:
    """Create a serialized representation of a dictionary, suitable for use in a URL query string."""
    if not isinstance(args, dict):
        raise TypeError("encode() expected argument dictionary")
    ret = encode_args(args)
    return '&'.join(ret)


def encode_args(obj: Args) -> List[str]:
    """Serialize the provided argument dictionary."""
    ret = []
    for key in obj:
        if isinstance(obj[key], dict):
            ret.extend(encode_dict(obj[key], key))
        elif isinstance(obj[key], list):
            ret.extend(encode_list(obj[key], key))
        else:
            if obj[key] is None:
                ret.append(f"{url_escape(key)}=")
            else:
                ret.append(f"{url_escape(key)}={url_escape(encode_value(obj[key]))}")
    return ret


def encode_dict(obj: Args, prefix: str) -> List[str]:
    """Serialize elements of the provided dictionary."""
    ret = []
    for key in obj:
        if isinstance(obj[key], dict):
            ret.extend(encode_dict(obj[key], f"{prefix}[{key}]"))
        elif isinstance(obj[key], list):
            ret.extend(encode_list(obj[key], f"{prefix}[{key}]"))
        else:
            if obj[key] is None:
                ret.append(f"{url_escape(prefix + '[' + key + ']')}=")
            else:
                ret.append(f"{url_escape(prefix + '[' + key + ']')}={url_escape(encode_value(obj[key]))}")
    return ret


def encode_list(obj: List[Any], prefix: str) -> List[str]:
    """Serialize elements of the provided list."""
    ret = []
    for key, value in enumerate(obj):
        if isinstance(value, dict):
            ret.extend(encode_dict(value, f"{prefix}[{key}]"))
        elif isinstance(value, list):
            ret.extend(encode_list(value, f"{prefix}[{key}]"))
        else:
            if value is None:
                ret.append(f"{url_escape(prefix + '[]')}=")
            else:
                ret.append(f"{url_escape(prefix + '[]')}={url_escape(encode_value(value))}")
    return ret


def encode_value(obj: Any) -> str:
    """Convert a value the way jQuery does."""
    try:
        if float(obj) == int(obj):
            return str(int(obj))
        else:
            return str(float(obj))
    except Exception:
        try:
            return str(float(obj))
        except Exception:
            try:
                return str(int(obj))
            except Exception:
                pass
    return str(obj)


def parse(data: str) -> Args:
    """Parse query arguments encoded in jQuery.param() format."""
    ret: Args = {}
    for part in data.split("&"):
        if part:
            ret = parse_arg(ret, part)
    return ret


def parse_arg(orig_args: Args, data: str) -> Args:
    """Parse a query argument encoded in jQuery.param() format."""
    key, value = url_unescape(data).split("=", 1)
    work_args = deepcopy(orig_args)
    # DEBUG: print(f"key:'{key}' value:'{value}'")
    obj: Any = work_args
    key_path = parse_key(key)
    for path_elem in key_path:
        # DEBUG: print(f"\tpath_elem:'{path_elem}'")
        # if we're in a dictionary
        if isinstance(obj, dict):
            # update the dictionary and return the next path object
            obj = update_dict(obj, path_elem, value)
        elif isinstance(obj, list):
            # update the list and return the next path object
            obj = update_list(obj, path_elem, value)
        else:
            raise Exception("path object is neither Dict or List")
        # DEBUG: print(f"\t\twork_args: {work_args}")
    # return the updated arguments dictionary to the caller
    return work_args


def parse_key(key: str) -> List[Subscript]:
    """Parse a query argument key encoded in jQuery.param() format."""
    # look for the opening of the first subscript
    open_index = key.find("[")
    # if this key contains no subscripts at all, we're done
    if open_index < 0:
        return [(SubscriptType.KEY, key, SubscriptType.END)]
    # otherwise, we'll parse until we're done
    ret = []
    ret.append((SubscriptType.KEY, key[0:open_index], SubscriptType.END))
    finished = False
    while not finished:
        close_index = key.find("]", open_index)
        subscript = key[open_index + 1:close_index]
        type = parse_subscript_type(subscript)
        ret.append((type, subscript, SubscriptType.END))
        open_index = key.find("[", close_index)
        # check if we're done`
        if (open_index < 0) or (close_index < 0):
            finished = True
    # now we'll clean up the next list
    for i in range(0, len(ret) - 1):
        ret[i] = (ret[i][0], ret[i][1], ret[i + 1][0])
    # return the list of key subscripts to the caller
    return ret


def parse_subscript_type(subscript: str) -> SubscriptType:
    """Convert a subscript string into a SubscriptType."""
    # if this is an empty subscript '[]', it's an array append
    if subscript == "":
        return SubscriptType.INDEX
    # otherwise, it could be an array index
    try:
        _ = int(subscript)
        return SubscriptType.INDEX
    except Exception:
        # nope, it's a dictionary key
        return SubscriptType.KEY


def update_dict(obj: Args, path_elem: Subscript, value: str) -> Any:
    """Update the path object when it's a dictionary."""
    # determine how we'll update the provided object
    this_type: SubscriptType = path_elem[0]
    this_key: str = path_elem[1]
    next_type: SubscriptType = path_elem[2]

    # if this type is INDEX, we're barking up the wrong Dict, pal!
    if this_type == SubscriptType.INDEX:
        raise Exception("INDEX subscript on Dict")

    # if this type is KEY, set the key according to the next type
    if this_type == SubscriptType.KEY:
        if next_type == SubscriptType.END:
            obj[this_key] = decode_value(value)
        elif next_type == SubscriptType.INDEX:
            if this_key not in obj:
                obj[this_key] = []
            obj = obj[this_key]
        elif next_type == SubscriptType.KEY:
            if this_key not in obj:
                obj[this_key] = {}
            obj = obj[this_key]
        else:
            raise Exception(f"update_dict: Unknown next_type: {next_type}")

    # return the next path object to the caller
    return obj


def update_list(obj: List[Any], path_elem: Subscript, value: str) -> Any:
    """Update the path object when it's a list."""
    # determine how we'll update the provided object
    this_type: SubscriptType = path_elem[0]
    this_key: str = path_elem[1]
    next_type: SubscriptType = path_elem[2]

    # determine if we were provided an index or not
    try:
        this_index = int(this_key)
    except Exception:
        this_index = len(obj)

    # if this type is KEY, we're barking up the wrong List, pal!
    if this_type == SubscriptType.KEY:
        raise Exception("KEY subscript on List")

    # if this type is INDEX, set the index according to the next type
    if this_type == SubscriptType.INDEX:
        if next_type == SubscriptType.END:
            if this_index >= len(obj):
                obj.append(decode_value(value))
            else:
                # reassignment of existing value; 悪因悪果...
                obj[this_index] = decode_value(value)
        elif next_type == SubscriptType.INDEX:
            if this_index >= len(obj):
                obj.append([])
            obj = obj[this_index]
        elif next_type == SubscriptType.KEY:
            if this_index >= len(obj):
                obj.append({})
            obj = obj[this_index]
        else:
            raise Exception(f"update_list: Unknown next_type: {next_type}")

    # return the next path object to the caller
    return obj
