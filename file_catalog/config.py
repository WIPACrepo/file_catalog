try:
    from ConfigParser import SafeConfigParser
except ModuleNotFoundError:
    try:
        from configparser import SafeConfigParser
    except ImportError:
        from configparser import ConfigParser as SafeConfigParser

import os
import ast

class Config(dict):
    def __init__(self, path):
        self.path = path

        # read file
        tmp = SafeConfigParser()
        tmp.read(path)
        self._config_options_dict(tmp)

    def _config_options_dict(self, config):
        """
        Parsing config file
        Args:
            config: Python config parser object
        """
        # Method copied from https://github.com/WIPACrepo/pyglidein/blob/master/client_util.py#L96

        for section in config.sections():
            self[section] = {}
            for option in config.options(section):
                val = config.get(section, option)
                try:
                    val = ast.literal_eval(val)
                except Exception:
                    pass
                self[section][option] = val

    def get_list(self, section, name):
        """
        Parses the value of a given `section`/`name` pair and returns a list.
        It is expected that the value is a comma separated list, e.g. `a, b, c,d,e ,f , g`.
        Note that all white spaces are removed before and after a comma.
        """
        value = self[section][name]
        return [e.strip() for e in value.split(',') if e.strip()]

