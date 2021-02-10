from config_parser import ConfigParser
from indexer.moc import ScanPrices


if __name__ == '__main__':

    config_parser = ConfigParser()

    moc_inc = ScanPrices(
        config_parser.config,
        config_parser.config_network,
        config_parser.connection_network)
    moc_inc.scan_moc_prices()
