import json
import os
import traceback
from optparse import OptionParser

from common import getMocCfg, getParser
from moc_indexer import MoCIndexer
from taskrunner import Job, JobsRunner


class MocIndexerJob(Job):
    def __init__(self, *args, **kw):
        super(MocIndexerJob, self).__init__(*args, **kw)
        try:
            self.interval = self.options['tasks'][self.name]['interval']
        except KeyError:
            traceback.print_exc()
            self.interval = 20
        self.moc_indexer = MoCIndexer(self.runner.options, self.runner.network)


class scan_moc_blocks(MocIndexerJob):
    def run_job(self):
        self.moc_indexer.scan_moc_blocks()


class scan_moc_blocks_history(MocIndexerJob):
    def run_job(self):
        self.moc_indexer.scan_moc_blocks_history()


class scan_moc_prices(MocIndexerJob):
    def run_job(self):
        self.moc_indexer.scan_moc_prices()


class scan_moc_prices_history(MocIndexerJob):
    def run_job(self):
        self.moc_indexer.scan_moc_prices_history()


class scan_moc_state(MocIndexerJob):
    def run_job(self):
        self.moc_indexer.scan_moc_state()


class scan_moc_state_history(MocIndexerJob):
    def run_job(self):
        self.moc_indexer.scan_moc_state_history()


class scan_moc_status(MocIndexerJob):
    def run_job(self):
        self.moc_indexer.scan_transaction_status()


class scan_moc_state_status(MocIndexerJob):
    def run_job(self):
        self.moc_indexer.scan_moc_state_status()


class scan_moc_state_status_history(MocIndexerJob):
    def run_job(self):
        self.moc_indexer.scan_moc_state_status_history()


class scan_user_state_update(MocIndexerJob):
    def run_job(self):
        self.moc_indexer.scan_user_state_update()


class scan_moc_blocks_not_processed(MocIndexerJob):
    def run_job(self):
        self.moc_indexer.scan_moc_blocks_not_processed()


def add_jobs():
    jobs = [
        "jobs:scan_moc_blocks",
        "jobs:scan_moc_prices",
        "jobs:scan_moc_state",
        "jobs:scan_moc_status",
        "jobs:scan_moc_state_status",
        "jobs:scan_user_state_update",
        "jobs:scan_moc_blocks_not_processed",
    ]
    return jobs

def add_jobs_history():
    jobs = [
        "jobs:scan_moc_blocks_history",
        "jobs:scan_moc_prices_history",
        "jobs:scan_moc_state_history",
        "jobs:scan_moc_state_status_history",
    ]
    return jobs

def add_jobs_tx_history():
    jobs = [
        "jobs:scan_moc_blocks_history"
    ]
    return jobs

#
# def main(moccfg):
#     runner = JobsRunner(moccfg=moccfg)
#     force_start = False
#
#     if moccfg.config['index_mode']=='normal':
#         f = add_jobs
#
#     elif moccfg.config['index_mode']=='history':
#         force_start = moccfg.config['scan_moc_history']['force_start']
#         f = add_jobs_tx_history
#     else:
#         raise Exception("Index mode not recognize")
#
#     if force_start:
#         moccfg.get_indexer().force_start_history()
#     for jobdesc in f():
#         runner.add_jobdesc(jobdesc)
#     runner.time_loop_start()
#
#
# if __name__ == '__main__':
#     parser = getParser(__file__)
#     main(moccfg=getMocCfg(parser, MoCIndexer))
