from config_parser import ConfigParser
from report.historical import ReportHistorical


if __name__ == '__main__':

    config_parser = ConfigParser()

    report = ReportHistorical(
        config_parser.config,
        config_parser.config_network,
        config_parser.connection_network)

    report.report_to_console()
