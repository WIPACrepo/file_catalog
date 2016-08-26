from tornado.escape import url_unescape
def get_type(val):
    try:
        return int(val)
    except:
        try:
            return float(val)
        except:
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
def parse(data):
    """Parse url-encoded data from jQuery.param()"""
    ret = {}
    for part in data.split('&'):
        key, value = url_unescape(part).split('=',1)
        value = get_type(value)
        parse_one(key, value, ret)
    return ret
