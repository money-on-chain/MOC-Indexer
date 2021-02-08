from config_parser import ConfigParser
import json
from job_indexer import JobsIndexer


def options_from_config(filename='config.json'):
    """ Options from file config.json """

    with open(filename) as f:
        config_options = json.load(f)

    return config_options


if __name__ == '__main__':

    config_parser = ConfigParser()

    job_index = JobsIndexer(
        config_parser.config,
        config_parser.config_network,
        config_parser.connection_network)
    job_index.time_loop_start()
