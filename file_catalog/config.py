
from ConfigParser import SafeConfigParser

import os

class Config:
    _config = None
    _cache = {}

    @classmethod
    def get_config(cls):
        if cls._config is None:
            cls._config = SafeConfigParser()
            cls._config.read(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'server.cfg')))

        return cls._config

    @classmethod
    def get_list(cls, section, name):
        if section in cls._cache:
            if name in cls._cache[section]:
                return cls._cache[section][name]
        else:
            cls._cache[section] = {}

        value = cls.get_config().get(section, name)
        cls._cache[section][name] = [e.strip() for e in value.split(',') if e.strip()]

        return cls._cache[section][name]

