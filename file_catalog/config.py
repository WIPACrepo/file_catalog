import os

from collections import namedtuple

class ConfigValidationError(Exception):
    pass

cps = namedtuple('ConfigParamSpec', 'default env_adapter description')

class Config(dict):
    SPEC = {
        'DEBUG': cps(False, bool,
                'debug mode (set to "" or unset to disable)'),
        'FC_COOKIE_SECRET': cps(None, str,
                'Value of cookie_secret argument for tornado.web.Application'),
        'FC_PORT': cps(8888, int,
                'Port for File Catalog server to listen on'),
        'FC_PUBLIC_URL': cps('http://localhost:8888', str,
                'Public URL for accessing File Catalog'),
        'FC_QUERY_FILE_LIST_LIMIT': cps(10000, int,
                'Maximal number of files that are returned in the file list by the server'),
        'MONGODB_AUTH_PASS': cps(None, str,
                'MongoDB authentication password'),
        'MONGODB_AUTH_SOURCE_DB': cps('admin', str,
                'The database to authenticate on. Defaults to "admin"'),
        'MONGODB_AUTH_USER': cps(None, str,
                'MongoDB authentication username'),
        'MONGODB_HOST': cps('localhost', str,
                'MongoDB host'),
        'MONGODB_PORT': cps(27017, int,
                'MongoDB port'),
        'META_FORBIDDEN_FIELDS_CREATION': cps(
                ['mongo_id', '_id', 'meta_modify_date'], str.split,
                'List of fields not allowed in the metadata for creation'),
        'META_FORBIDDEN_FIELDS_UPDATE': cps(
                ['mongo_id', '_id', 'meta_modify_date', 'uuid'], str.split,
                'List of fields not allowed in the metadata for update/replace'),
        'META_MANDATORY_FIELDS': cps(
                ['uuid', 'logical_name', 'locations', 'file_size', 'checksum.sha512'], str.split,
                'List of mandatory metadata fields'),
        'TOKEN_ALGORITHM': cps('RS512', str,
                'Token signature algorithm'),
        'TOKEN_KEY': cps(None, str,
                'Token signature verification key, e.g. public or symmetric key of the token service'),
        'TOKEN_URL': cps(None, str,
                'Token service URL, e.g. https://tokens.icecube.wisc.edu'),
    }

    def __init__(self):
        super().__init__([(name, spec.default) for name,spec in self.SPEC.items()])

    def update_from_env(self, env=None):
        if env is None:
            env = os.environ
        for name,spec in self.SPEC.items():
            if name in env:
                self[name] = spec.env_adapter(env[name])

    def __setitem__(self, key, val):
        if key not in self.SPEC:
            raise ConfigValidationError('%s is not a valid configuration parameter')
        else:
            super().__setitem__(key, val)
