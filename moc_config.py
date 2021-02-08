import json
import logging
import os
from optparse import OptionParser


def options_from_config(filename='config.json'):
    """ Options from file config.json """
    with open(filename) as f:
        config_options = json.load(f)
    return config_options


class MoCCfg:
    default_config_path = os.path.join(
                            os.path.dirname(os.path.realpath(__file__)),
                            'settings', 'settings-moc-alpha-testnet.json')
    default_connection_network = 'rskTesnetPublic'
    default_config_network = 'mocTestnetAlpha'

    def __init__(self, customize_parser_cb=None, prog=None):
        logger = logging.getLogger('default')
        self.parser = OptionParser(usage='%prog [options]', prog=prog)
        self.parser.add_option('-n', '--connection_network', action='store', dest='connection_network',
                               type="string", help='connection_network')
        self.parser.add_option('-e', '--config_network', action='store', dest='config_network',
                               type="string", help='config_network')
        self.parser.add_option('-c', '--config', action='store', dest='config',
                               type="string", help='config')
        if not(customize_parser_cb is None):
            customize_parser_cb(self.parser)
            
        (options, args) = self.parser.parse_args()
    
        if 'APP_CONFIG' in os.environ:
            logger.info("Config: from APP_CONFIG(env)")
            config = json.loads(os.environ['APP_CONFIG'])
        else:
            if not options.config:
                config_path = self.default_config_path
            else:
                config_path = options.config
            logger.info("Config: from: %s" % config_path)
            config = options_from_config(config_path)

        if 'APP_CONNECTION_NETWORK' in os.environ:
            logger.info("Connection Network: from: APP_CONNECTION_NETWORK(env)")
            connection_network = os.environ['APP_CONNECTION_NETWORK']
        else:
            if not options.connection_network:
                connection_network = self.default_connection_network
            else:
                connection_network = options.connection_network
        logger.info("Connection Network=%s" % connection_network)
    
        if 'APP_CONFIG_NETWORK' in os.environ:
            logger.info("Connection Network: from: APP_CONFIG_NETWORK(env)")
            config_network = os.environ['APP_CONFIG_NETWORK']
        else:
            if not options.config_network:
                config_network = self.default_config_network
            else:
                config_network = options.config_network
        logger.info("Config Network=%s" % config_network)

        if 'APP_MONGO_URI' in os.environ:
            mongo_uri = os.environ['APP_MONGO_URI']
            config['mongo']['uri'] = mongo_uri
    
        if 'APP_MONGO_DB' in os.environ:
            mongo_db = os.environ['APP_MONGO_DB']
            config['mongo']['db'] = mongo_db

        self.config = config
        self.config_network = config_network
        self.connection_network = connection_network
        self.options = options

