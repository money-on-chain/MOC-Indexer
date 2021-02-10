from moneyonchain.networks import chain

from web3 import Web3
from web3.exceptions import TransactionNotFound

from brownie.network.transaction import Status


import logging
import logging.config


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger('default')


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
