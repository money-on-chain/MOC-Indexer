from jobs import MocIndexerJob


class scan_moc_blocks_history(MocIndexerJob):
    def plugin_init(self):
        super().plugin_init()
        force_start = self.runner.moccfg.config['scan_moc_history']['force_start']
        if force_start:
            self.moc_indexer.force_start_history()


    def run_job(self):
        self.moc_indexer.scan_moc_blocks_history()


# class scan_moc_prices_history(MocIndexerJob):
#     def run_job(self):
#         self.moc_indexer.scan_moc_prices_history()
#
#
# class scan_moc_state_history(MocIndexerJob):
#     def run_job(self):
#         self.moc_indexer.scan_moc_state_history()
#
#
# class scan_moc_state_status_history(MocIndexerJob):
#     def run_job(self):
#         self.moc_indexer.scan_moc_state_status_history()
