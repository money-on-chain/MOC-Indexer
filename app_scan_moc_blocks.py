from moc_config import MoCCfg
from taskrunner import JobsRunner


if __name__ == '__main__':
    moccfg = MoCCfg(prog='app_scan_moc_blocks.py')
    runner = JobsRunner(moccfg=moccfg)
    runner.add_jobdesc("jobs:scan_moc_blocks")
    runner.time_loop_start()
