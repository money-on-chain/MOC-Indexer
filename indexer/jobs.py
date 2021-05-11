import os
import datetime


from timeloop import Timeloop
import boto3
import time


from indexer.moc import ScanBlocks, \
    ScanPrices, \
    ScanState, \
    ScanStatus, \
    ScanUser

from .logger import log


class JobsIndexer(ScanBlocks, ScanPrices, ScanState, ScanStatus, ScanUser):

    def __init__(self, *tx_args, **tx_vars):

        super().__init__(*tx_args, **tx_vars)

        self.tl = Timeloop()

    @staticmethod
    def aws_put_metric_heart_beat(value):

        if 'AWS_ACCESS_KEY_ID' not in os.environ:
            return

        # Create CloudWatch client
        cloudwatch = boto3.client('cloudwatch')

        # Put custom metrics
        cloudwatch.put_metric_data(
            MetricData=[
                {
                    'MetricName': os.environ['MOC_INDEXER_NAME'],
                    'Dimensions': [
                        {
                            'Name': 'INDEXER',
                            'Value': 'Error'
                        },
                    ],
                    'Unit': 'None',
                    'Value': value
                },
            ],
            Namespace='MOC/EXCEPTIONS'
        )

    def task_scan_moc_blocks(self):

        try:
            self.scan_moc_blocks()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def task_scan_moc_blocks_history(self):

        try:
            self.scan_moc_blocks_history()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def task_scan_moc_prices(self):

        try:
            self.scan_moc_prices()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def task_scan_moc_prices_history(self):

        try:
            self.scan_moc_prices_history()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def task_scan_moc_state(self):

        try:
            self.scan_moc_state()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def task_scan_moc_state_history(self):

        try:
            self.scan_moc_state_history()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def task_scan_moc_status(self):

        try:
            self.scan_transaction_status()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def task_scan_moc_state_status(self):

        try:
            self.scan_moc_state_status()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def task_scan_moc_state_status_history(self):

        try:
            self.scan_moc_state_status_history()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def task_scan_user_state_update(self):

        try:
            self.scan_user_state_update()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def task_scan_moc_blocks_not_processed(self):

        try:
            self.scan_moc_blocks_not_processed()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def add_jobs(self):

        log.info("Starting adding jobs...")

        # creating the alarm
        self.aws_put_metric_heart_beat(0)

        # scan_moc_blocks
        log.info("Jobs add scan_moc_blocks")
        interval = self.options['tasks']['scan_moc_blocks']['interval']
        self.tl._add_job(self.task_scan_moc_blocks, datetime.timedelta(seconds=interval))

        # scan_moc_prices
        log.info("Jobs add scan_moc_prices")
        interval = self.options['tasks']['scan_moc_prices']['interval']
        self.tl._add_job(self.task_scan_moc_prices, datetime.timedelta(seconds=interval))

        # scan_moc_state
        log.info("Jobs add scan_moc_state")
        interval = self.options['tasks']['scan_moc_state']['interval']
        self.tl._add_job(self.task_scan_moc_state, datetime.timedelta(seconds=interval))

        # scan_moc_status
        log.info("Jobs add scan_moc_status")
        interval = self.options['tasks']['scan_moc_status']['interval']
        self.tl._add_job(self.task_scan_moc_status, datetime.timedelta(seconds=interval))

        # scan_moc_state_status
        log.info("Jobs add scan_moc_state_status")
        interval = self.options['tasks']['scan_moc_state_status']['interval']
        self.tl._add_job(self.task_scan_moc_state_status, datetime.timedelta(seconds=interval))

        # scan_moc_state_status
        log.info("Jobs add scan_user_state_update")
        interval = self.options['tasks']['scan_user_state_update']['interval']
        self.tl._add_job(self.task_scan_user_state_update, datetime.timedelta(seconds=interval))

        # scan_moc_blocks_not_processed
        log.info("Jobs add scan_moc_blocks_not_processed")
        interval = self.options['tasks']['scan_moc_blocks_not_processed']['interval']
        self.tl._add_job(self.task_scan_moc_blocks_not_processed, datetime.timedelta(seconds=interval))

    def add_jobs_history(self):

        log.info("Starting adding history jobs...")

        force_start = self.options['scan_moc_history']['force_start']
        if force_start:
            self.force_start_history()

        # creating the alarm
        self.aws_put_metric_heart_beat(0)

        # task_scan_moc_blocks_history
        log.info("Jobs add task_scan_moc_blocks_history")
        interval = self.options['tasks']['scan_moc_blocks']['interval']
        self.tl._add_job(self.task_scan_moc_blocks_history, datetime.timedelta(seconds=interval))

        # task_scan_moc_prices_history
        log.info("Jobs add task_scan_moc_prices_history")
        interval = self.options['tasks']['scan_moc_prices']['interval']
        self.tl._add_job(self.task_scan_moc_prices_history, datetime.timedelta(seconds=interval))

        # task_scan_moc_state_history
        log.info("Jobs add task_scan_moc_state_history")
        interval = self.options['tasks']['scan_moc_state']['interval']
        self.tl._add_job(self.task_scan_moc_state_history, datetime.timedelta(seconds=interval))

        # task_scan_moc_state_status_history
        log.info("Jobs add task_scan_moc_state_status_history")
        interval = self.options['tasks']['scan_moc_state_status']['interval']
        self.tl._add_job(self.task_scan_moc_state_status_history, datetime.timedelta(seconds=interval))

    def add_jobs_tx_history(self):

        log.info("Starting adding tx history jobs...")

        force_start = self.options['scan_moc_history']['force_start']
        if force_start:
            self.force_start_history()

        # creating the alarm
        self.aws_put_metric_heart_beat(0)

        # task_scan_moc_blocks_history
        log.info("Jobs add task_scan_moc_blocks_history")
        interval = self.options['tasks']['scan_moc_blocks']['interval']
        self.tl._add_job(self.task_scan_moc_blocks_history, datetime.timedelta(seconds=interval))

    def time_loop_start(self):

        if self.options['index_mode'] in ['normal', 'vendors']:
            self.add_jobs()
        elif self.options['index_mode'] in ['history', 'vendors_history']:
            self.add_jobs_tx_history()
        else:
            raise Exception("Index mode not recognize")

        #self.tl.start(block=True)
        self.tl.start()
        while True:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                self.tl.stop()
                log.info("Shutting DOWN! TASKS")
                break
