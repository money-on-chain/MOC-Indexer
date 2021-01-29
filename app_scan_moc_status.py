from moc_config import MoCCfg
from taskrunner import JobsRunner


if __name__ == '__main__':
    moccfg = MoCCfg(prog='app_scan_moc_status.py')
    runner = JobsRunner(moccfg=moccfg)
    runner.add_jobdesc("jobs:scan_moc_status")
    runner.time_loop_start()
