from moc_config import MoCCfg
from taskrunner import JobsRunner


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


def main(moccfg):
    runner = JobsRunner(moccfg=moccfg)

    if moccfg.config['index_mode'] == 'normal':
        f = add_jobs
    elif moccfg.config['index_mode'] == 'history':
        f = add_jobs_tx_history
    else:
        raise Exception("Index mode not recognize")

    for job_desc in f():
        runner.add_jobdesc(job_desc)
    runner.time_loop_start()


if __name__ == '__main__':
    moccfg = MoCCfg(prog='app_run_indexer.py')
    main(moccfg)


