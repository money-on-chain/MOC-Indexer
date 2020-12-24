import json
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
                            'settings', 'settings-rdoc-mainnet-historic.json')
    default_network = 'rdocMainnet'

    def __init__(self, customize_parser_cb=None, prog=None):
        logger = logging.getLogger('default')
        self.parser = OptionParser(usage='%prog [options]', prog=prog)
        self.parser.add_option('-n', '--network', action='store', dest='network',
                          type="string", help='network')
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
            logger.info("Config: from: %s"%config_path)
            config = options_from_config(config_path)
    
        if 'APP_NETWORK' in os.environ:
            logger.info("Network: from: APP_NETWORK(env)")
            network = os.environ['APP_NETWORK']
        else:
            if not options.network:
                network = self.default_network
            else:
                network = options.network
        logger.info("Network=%s"%network)

        if 'APP_MONGO_URI' in os.environ:
            mongo_uri = os.environ['APP_MONGO_URI']
            config['mongo']['uri'] = mongo_uri
    
        if 'APP_MONGO_DB' in os.environ:
            mongo_db = os.environ['APP_MONGO_DB']
            config['mongo']['db'] = mongo_db

        self.config = config
        self.network = network
        self.options = options

