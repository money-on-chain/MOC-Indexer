import os
import boto3

from moneyonchain.networks import chain

from web3 import Web3
from web3.exceptions import TransactionNotFound

from brownie.network.transaction import Status

from .logger import log


def transactions_receipt(transactions):
    l_tx_receipt = list()
    for tx in transactions:
        try:
            tx_receipt = chain.get_transaction(Web3.toHex(tx['hash']))
        except TransactionNotFound:
            log.error("No transaction receipt for hash: [{0}]".format(
                Web3.toHex(tx['hash'])))
            tx_receipt = None
        if tx_receipt:
            if tx_receipt.status == Status.Confirmed:
                l_tx_receipt.append(tx_receipt)

    return l_tx_receipt


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
