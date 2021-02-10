from indexer.moc import ScanBlocks
from config_parser import ConfigParser


if __name__ == '__main__':

    config_parser = ConfigParser()

    moc_inc = ScanBlocks(
        config_parser.config,
        config_parser.config_network,
        config_parser.connection_network)
    moc_inc.scan_moc_blocks()
