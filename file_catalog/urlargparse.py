"""Utilities for parsing url args."""

# fmt: off

from typing import Any, Dict

from tornado.escape import url_escape, url_unescape


def get_type(val):
    try:
        return int(val)
    except:  # noqa: E722
        try:
            return float(val)
        except:  # noqa: E722
            return val


def parse_one(key, value, ret, sym='['):
    print('key',key,'value',value)
    if key == '[]':
        ret.append(value)
    else:
        if key[0] == '[':
            key = key[1:]
        start = key.find(sym)
        if start < 0:
            ret[get_type(key)] = value
        else:
            val = get_type(key[:start])
            if isinstance(ret,dict) and val not in ret:
                ret[val] = [] if key[start+1:start+3] == '[]' else {}
            elif isinstance(ret,list) and len(ret) <= val:
                ret.append([] if key[start+1:start+3] == '[]' else {})
            if not key[start+1:]:
                ret[val] = value
            else:
                parse_one(key[start+1:],value,ret[val],sym=']')


def parse(data: str) -> Dict[str, Any]:
    """Parse url-encoded data from jQuery.param()"""
    ret: Dict[str, Any] = {}
    for part in data.split('&'):
        if part:
            key, value = url_unescape(part).split('=',1)
            value = get_type(value)
            parse_one(key, value, ret)
    return ret


def encode(args):
    """Encode data using the jQuery.param() syntax."""
    ret = []

    def recurse(obj,prefix=''):
        if isinstance(obj,dict):
            for k in obj:
                recurse(obj[k],prefix+'['+k+']')
        elif isinstance(obj,list):
            for i,v in enumerate(obj):
                if isinstance(v,(dict,list)):
                    recurse(v,prefix+'[%d]'%i)
                else:
                    recurse(v,prefix+'[]')
        else:
            ret.append(url_escape(prefix)+'='+url_escape(str(obj)))

    for k in args:
        recurse(args[k],k)
    return '&'.join(ret)
