from moc_indexer import MoCIndexer
from taskrunner import Job


class MocIndexerJob(Job):
    def __init__(self, *args, **kw):
        super(MocIndexerJob, self).__init__(*args, **kw)
        try:
            self.interval = self.options['tasks'][self.name]['interval']
        except KeyError:
            #traceback.print_exc()
            self.interval = 20
        self.moc_indexer = MoCIndexer(self.runner.options,
                                      self.runner.config_network,
                                      self.runner.connection_network)


class scan_moc_blocks(MocIndexerJob):
    def run_job(self):
        self.moc_indexer.scan_moc_blocks()


class scan_moc_prices(MocIndexerJob):
    def run_job(self):
        self.moc_indexer.scan_moc_prices()


class scan_moc_state(MocIndexerJob):
    def run_job(self):
        self.moc_indexer.scan_moc_state()


class scan_moc_status(MocIndexerJob):
    def run_job(self):
        self.moc_indexer.scan_transaction_status()


class scan_moc_state_status(MocIndexerJob):
    def run_job(self):
        self.moc_indexer.scan_moc_state_status()


class scan_user_state_update(MocIndexerJob):
    def run_job(self):
        self.moc_indexer.scan_user_state_update()


class scan_moc_blocks_not_processed(MocIndexerJob):
    def run_job(self):
        self.moc_indexer.scan_moc_blocks_not_processed()


