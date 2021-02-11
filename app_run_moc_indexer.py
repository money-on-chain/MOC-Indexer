from config_parser import ConfigParser
from indexer.jobs import JobsIndexer


if __name__ == '__main__':

    config_parser = ConfigParser()

    job_index = JobsIndexer(
        config_parser.config,
        config_parser.config_network,
        config_parser.connection_network)
    job_index.time_loop_start()
