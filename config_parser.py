import os
import json
from optparse import OptionParser


class ConfigParser(object):

    @staticmethod
    def options_from_config(filename=None):
        """ Options from file config.json """

        if not filename:
            filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'settings', 'config.json')

        with open(filename) as f:
            options = json.load(f)

        return options

    def __init__(self,
                 connection_network='rskTesnetPublic',
                 config_network='mocTestnetAlpha',
                 options=None):

        self.connection_network = connection_network
        self.config_network = config_network
        if not options:
            self.config = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                       'config.json')
        else:
            self.config = options
        self.parse()

    def parse(self):

        usage = '%prog [options] '
        parser = OptionParser(usage=usage)

        parser.add_option('-n', '--connection_network', action='store', dest='connection_network', type="string",
                          help='network to connect')

        parser.add_option('-e', '--config_network', action='store', dest='config_network', type="string",
                          help='enviroment to connect')

        parser.add_option('-c', '--config', action='store', dest='config', type="string", help='config')

        (options, args) = parser.parse_args()

        if 'APP_CONFIG' in os.environ:
            self.config = json.loads(os.environ['APP_CONFIG'])
        else:
            if not options.config:
                # if there are no config try to read config.json from current folder
                config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config.json')
                if not os.path.isfile(config_path):
                    raise Exception("Please select path to config or env APP_CONFIG. "
                                    "Ex. /settings/settings-moc-alpha-testnet.json "
                                    "Full Ex.:"
                                    "python app_run_moc_indexer.py "
                                    "--connection_network=rskTestnetPublic "
                                    "--config_network=mocTestnetAlpha "
                                    "--config ./settings/settings-moc-alpha-testnet.json"
                                    )
            else:
                config_path = options.config

            self.config = self.options_from_config(config_path)

        if 'APP_CONNECTION_NETWORK' in os.environ:
            self.connection_network = os.environ['APP_CONNECTION_NETWORK']
        else:
            if not options.connection_network:
                raise Exception("Please select connection network or env APP_CONNECTION_NETWORK. "
                                "Ex.: rskTesnetPublic. "
                                "Full Ex.:"
                                "python app_run_moc_indexer.py "
                                "--connection_network=rskTestnetPublic "
                                "--config_network=mocTestnetAlpha "
                                "--config ./settings/settings-moc-alpha-testnet.json"
                                )
            else:
                self.connection_network = options.connection_network

        if 'APP_CONFIG_NETWORK' in os.environ:
            self.config_network = os.environ['APP_CONFIG_NETWORK']
        else:
            if not options.config_network:
                raise Exception("Please select enviroment of your config or env APP_CONFIG_NETWORK. "
                                "Ex.: rdocTestnetAlpha"
                                "Full Ex.:"
                                "python app_run_moc_indexer.py "
                                "--connection_network=rskTestnetPublic "
                                "--config_network=mocTestnetAlpha "
                                "--config ./settings/settings-moc-alpha-testnet.json"
                                )
            else:
                self.config_network = options.config_network

        if 'APP_MONGO_URI' in os.environ:
            mongo_uri = os.environ['APP_MONGO_URI']
            self.config['mongo']['uri'] = mongo_uri

        if 'APP_MONGO_DB' in os.environ:
            mongo_db = os.environ['APP_MONGO_DB']
            self.config['mongo']['db'] = mongo_db
