import os
import json
from optparse import OptionParser


class ConfigParser(object):

    @staticmethod
    def options_from_config(filename=None):
        """ Options from file config.json """

        if not filename:
            filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'settings', 'develop.json')

        with open(filename) as f:
            options = json.load(f)

        return options

    def __init__(self,
                 options=None):

        if not options:
            self.config = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'settings',
                                       'develop.json')
        else:
            self.config = options
        self.parse()

    def parse(self):

        usage = '%prog [options] '
        parser = OptionParser(usage=usage)

        parser.add_option('-c', '--config', action='store', dest='config', type="string", help='config')

        (options, args) = parser.parse_args()

        if 'APP_CONFIG' in os.environ:
            self.config = json.loads(os.environ['APP_CONFIG'])
        else:
            if not options.config:
                # if there are no config try to read config.json from current folder
                config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'settings', 'develop.json')
                if not os.path.isfile(config_path):
                    raise Exception("Please select path to config or env APP_CONFIG. "
                                    "Ex. /settings/develop.json "
                                    "Full Ex.:"
                                    "python app.py "                                    
                                    "--config ./settings/develop.json"
                                    )
            else:
                config_path = options.config

            self.config = self.options_from_config(config_path)

        if 'APP_MONGO_URI' in os.environ:
            mongo_uri = os.environ['APP_MONGO_URI']
            self.config['mongo']['uri'] = mongo_uri

        if 'APP_MONGO_DB' in os.environ:
            mongo_db = os.environ['APP_MONGO_DB']
            self.config['mongo']['db'] = mongo_db
