import os
from common import getParser, getMocCfg
from jobs import add_jobs, add_jobs_tx_history
from moc_indexer import MoCIndexer
from taskrunner import JobsRunner


def main(moccfg):
    runner = JobsRunner(moccfg=moccfg)
    force_start = False

    if moccfg.config['index_mode']=='normal':
        f = add_jobs

    elif moccfg.config['index_mode']=='history':
        force_start = moccfg.config['scan_moc_history']['force_start']
        f = add_jobs_tx_history
    else:
        raise Exception("Index mode not recognize")

    if force_start:
        moccfg.get_indexer().force_start_history()
    for jobdesc in f():
        runner.add_jobdesc(jobdesc)
    runner.time_loop_start()


if __name__ == '__main__':
    defConfig = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                       'settings',
                                       'settings-rdoc-mainnet-historic.json')
    parser = getParser(__file__)
    main(moccfg=getMocCfg(parser, MoCIndexer, defaultNet='rdocMainnet',
                          defaultConfig=defConfig))


