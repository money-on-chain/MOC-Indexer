import os
from optparse import OptionParser
import json
from job_indexer import JobsIndexer


def options_from_config(filename='config.json'):
    """ Options from file config.json """

    with open(filename) as f:
        config_options = json.load(f)

    return config_options


if __name__ == '__main__':

    usage = '%prog [options] '
    parser = OptionParser(usage=usage)

    parser.add_option('-n', '--network', action='store', dest='network', type="string", help='network')

    parser.add_option('-c', '--config', action='store', dest='config', type="string", help='config')

    (options, args) = parser.parse_args()

    if 'APP_CONFIG' in os.environ:
        config = json.loads(os.environ['APP_CONFIG'])
    else:
        if not options.config:
            config_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                       'settings',
                                       'settings-moc-alpha-testnet.json')
        else:
            config_path = options.config

        config = options_from_config(config_path)

    if 'APP_NETWORK' in os.environ:
        network = os.environ['APP_NETWORK']
    else:
        if not options.network:
            network = 'mocTestnetAlpha'
        else:
            network = options.network

    job_index = JobsIndexer(config, network)
    job_index.time_loop_start()