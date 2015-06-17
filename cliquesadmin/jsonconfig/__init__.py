import os
import json
from cliquesadmin import CONFIG_PATH


class ConfigurationError(BaseException):

    def __init__(self, message):
        self.message = message


class JsonConfigParser(object):
    """
    Simple JSON config parser which acts like built-in ConfigParser but for
    Node-style JSON config files

    Specify default values in: config_path/default.json

    Specify any environment-specific values in: config_path/[ENV].json

    Use ENV environment variable to set environment
    """
    def __init__(self, config_path=None):
        if config_path is None:
            config_path = CONFIG_PATH
        self.config_path = config_path
        self.env = os.environ.get('ENV', None)
        self._get_config()

    def _get_config(self):
        """
        Sets self.default_config and self.env_config objects
        """
        try:
            default_file = file(os.path.join(self.config_path, 'default.json'), 'rb')
            self.default_config = json.load(default_file)
            default_file.close()
        except IOError:
            raise IOError('No default.json file found in config_path %s' % self.config_path)

        self.env_config = {}
        if self.env is not None:
            try:
                env_file = file(os.path.join(self.config_path, '%s.json' % self.env), 'rb')
                self.env_config = json.load(env_file)
                env_file.close()
            except IOError:
                # Fail silently if matching config JSON file not found
                # for specified environment. This is how Node parser
                # behaves (I think)
                pass

    def get(self, *args):
        """
        Gets config value.

        Behaves kind of like ConfigParser.get method, i.e. takes list of
        config values to lookup, but can take an arbitrarily long list
        of nested config values, as they are stored in JSON object rather
        than .cnf file (which doesn't support nesting).

        Ex: in default.json:

        {"app":
            {"mysql":
                {"hostname": "localhost"}
             }
         }

         >>> parser = JsonConfigParser()
         >>> parser.get('app', 'mysql', 'hostname')
        'localhost'

        Will return None if it can't resolve a config value

        :param args:
        :return:
        """
        this_default_conf = self.default_config
        this_env_conf = self.env_config
        env_val = None
        for arg in args:
            default_val = this_default_conf.get(arg, None)
            env_val = this_env_conf.get(arg, default_val)
            this_default_conf = default_val
            this_env_conf = env_val
        # currently just return None if no config value found,
        # might want to pitch more of a fit in the future
        return env_val

# if __name__ == '__main__':
#     os.environ['ENV'] = 'production'
#     parser = JsonConfigParser()
#     print parser.get('Exchange', 'mongodb', 'exchange', 'primary', 'host')