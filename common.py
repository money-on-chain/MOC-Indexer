import json
import os
import sys
from optparse import OptionParser


def options_from_config(filename='config.json'):
    """ Options from file config.json """
    with open(filename) as f:
        config_options = json.load(f)
    return config_options


def getConfig(config_opt, defaultConfig=None):
    if 'APP_CONFIG' in os.environ:
        config = json.loads(os.environ['APP_CONFIG'])
    else:
        if not config_opt:
            if defaultConfig is None:
                defaultConfig = os.path.join(
                    os.path.dirname(os.path.realpath(__file__)), '..', 'config',
                    'config-testnet.json')
            config_path = defaultConfig
        else:
            config_path = config_opt
        config = options_from_config(config_path)
    return config


def getConnectionNetwork(options, default):
    if 'APP_CONNECTION_NETWORK' in os.environ:
        connection_network = os.environ['APP_CONNECTION_NETWORK']
    else:
        if not options.connection_network:
            connection_network = default
        else:
            connection_network = options.connection_network
    return connection_network


def getConfigNetwork(options, default):
    if 'APP_CONFIG_NETWORK' in os.environ:
        config_network = os.environ['APP_CONFIG_NETWORK']
    else:
        if not options.config_network:
            config_network = default
        else:
            config_network = options.config_network
    return config_network


# def getMocCfg(parser, indexerClass=None, defaultNet='rdocTestnet',
#               defaultConfig=None):
#     (options, args) = parser.parse_args()
#     _config = getConfig(options.config, defaultConfig)
#     _network = getNetwork(options, defaultNet)
#     class MocCfg:
#         config = _config
#         network = _network
#         if indexerClass is not None:
#             def get_indexer(self):
#                 return indexerClass(self.config, self.network)
#     return MocCfg


def getParser(progname=None):
    if progname is None:
        progname = progname
    else:
        progname = os.path.split(progname)[1]
    usage = '%s [options] '%progname
    parser = OptionParser(usage=usage)
    parser.add_option('-n', '--connection_network', action='store', dest='connection_network',
                      type="string", help='connection_network')
    parser.add_option('-e', '--config_network', action='store', dest='config_network',
                      type="string", help='config_network')
    parser.add_option('-c', '--config', action='store', dest='config',
                      type="string", help='config')
    return parser

