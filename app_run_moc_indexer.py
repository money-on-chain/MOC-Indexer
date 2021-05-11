from config_parser import ConfigParser
from indexer.jobs import JobsIndexer
from indexer.jobs_vendors import JobsIndexer as VendorsJobsIndexer


if __name__ == '__main__':

    config_parser = ConfigParser()

    if config_parser.config['index_mode'] == 'vendors':
        job_index = VendorsJobsIndexer(
            config_parser.config,
            config_parser.config_network,
            config_parser.connection_network)
    else:
        job_index = JobsIndexer(
            config_parser.config,
            config_parser.config_network,
            config_parser.connection_network)

    job_index.time_loop_start()
