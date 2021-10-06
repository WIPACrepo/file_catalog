"""Config."""

# fmt:off

import os
from collections import namedtuple


class ConfigValidationError(Exception):
    pass


ConfigParamSpec = namedtuple('ConfigParamSpec', 'default env_adapter description')


class Config(dict):
    SPEC = {
        'DEBUG': ConfigParamSpec(
            False, bool, 'debug mode (set to "" or unset to disable)'
        ),
        'FC_COOKIE_SECRET': ConfigParamSpec(
            None, str, 'Value of cookie_secret argument for tornado.web.Application'
        ),
        'FC_PORT': ConfigParamSpec(
            8888, int, 'Port for File Catalog server to listen on'
        ),
        'FC_PUBLIC_URL': ConfigParamSpec(
            'http://localhost:8888', str, 'Public URL for accessing File Catalog'
        ),
        'FC_QUERY_FILE_LIST_LIMIT': ConfigParamSpec(
            10000,
            int,
            'Maximal number of files that are returned in the file list by the server',
        ),
        'MONGODB_AUTH_PASS': ConfigParamSpec(
            None, str, 'MongoDB authentication password'
        ),
        'MONGODB_AUTH_SOURCE_DB': ConfigParamSpec(
            'admin', str, 'The database to authenticate on. Defaults to "admin"'
        ),
        'MONGODB_AUTH_USER': ConfigParamSpec(
            None, str, 'MongoDB authentication username'
        ),
        'MONGODB_HOST': ConfigParamSpec('localhost', str, 'MongoDB host'),
        'MONGODB_PORT': ConfigParamSpec(27017, int, 'MongoDB port'),
        'MONGODB_URI': ConfigParamSpec(None, str, 'MongoDB URI'),
        'TOKEN_ALGORITHM': ConfigParamSpec('RS512', str, 'Token signature algorithm'),
        'TOKEN_KEY': ConfigParamSpec(
            None,
            str,
            'Token signature verification key, e.g. public or symmetric key of the token service',
        ),
        'TOKEN_URL': ConfigParamSpec(
            None, str, 'Token service URL, e.g. https://tokens.icecube.wisc.edu'
        ),
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
