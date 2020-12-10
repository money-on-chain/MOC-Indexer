from common import getMocCfg, getParser
from moc_indexer import MoCIndexer
from taskrunner import JobsRunner


if __name__ == '__main__':
    parser = getParser(__file__)
    moccfg = getMocCfg(parser, MoCIndexer)
    runner = JobsRunner(moccfg=moccfg)
    runner.add_jobdesc("jobs:scan_moc_prices")
    runner.time_loop_start()
