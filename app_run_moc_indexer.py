from config_parser import ConfigParser
from indexer.tasks import MoCIndexerTasks


if __name__ == '__main__':

    config_parser = ConfigParser()

    indexer_tasks = MoCIndexerTasks(
        config_parser.config,
        config_parser.config_network,
        config_parser.connection_network)

    indexer_tasks.start_loop()
