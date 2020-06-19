import os
import datetime


from timeloop import Timeloop
import boto3
import time

from moc_indexer import MoCIndexer

import logging
import logging.config


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger('default')


class JobsIndexer:

    def __init__(self, config_app, network_app):

        self.options = config_app
        self.network = network_app
        self.moc_indexer = MoCIndexer(self.options, self.network)

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
            self.moc_indexer.scan_moc_blocks()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def task_scan_moc_prices(self):

        try:
            self.moc_indexer.scan_moc_prices()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def task_scan_moc_state(self):

        try:
            self.moc_indexer.scan_moc_state()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def task_scan_moc_status(self):

        try:
            self.moc_indexer.scan_transaction_status()
        except Exception as e:
            log.error(e, exc_info=True)
            self.aws_put_metric_heart_beat(1)

    def task_scan_moc_state_status(self):

        try:
            self.moc_indexer.scan_moc_state_status()
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

    def time_loop_start(self):

        self.add_jobs()
        #self.tl.start(block=True)
        self.tl.start()
        while True:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                self.tl.stop()
                log.info("Shutting DOWN! TASKS")
                break
