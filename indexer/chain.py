
from web3 import Web3
from web3.types import BlockData
from web3.exceptions import TransactionNotFound

from hexbytes import HexBytes
from typing import Union
from brownie.network.transaction import Status

from moneyonchain.networks import web3
from moneyonchain.transaction import TransactionReceipt

from indexer.logger import log


def filter_transactions(transactions, filter_addresses):
    l_transactions = list()
    d_index_transactions = dict()
    for transaction in transactions:
        tx_to = None
        tx_from = None
        if 'to' in transaction:
            if transaction['to']:
                tx_to = str.lower(transaction['to'])

        if 'from' in transaction:
            if transaction['from']:
                tx_from = str.lower(transaction['from'])

        if tx_to in filter_addresses or tx_from in filter_addresses:
            l_transactions.append(transaction)
            d_index_transactions[
                Web3.toHex(transaction['hash'])] = transaction

    return l_transactions, d_index_transactions


def get_transaction(txid: Union[str, bytes], required_confs=1) -> TransactionReceipt:
    """
    Return a TransactionReceipt object for the given transaction hash.
    """
    if not isinstance(txid, str):
        txid = HexBytes(txid).hex()
    return TransactionReceipt(txid, silent=True, required_confs=required_confs)


def transactions_receipt(transactions):
    l_tx_receipt = list()
    for tx in transactions:
        try:
            tx_receipt = get_transaction(Web3.toHex(tx['hash']))
        except TransactionNotFound:
            log.error("No transaction receipt for hash: [{0}]".format(
                Web3.toHex(tx['hash'])))
            tx_receipt = None
        if tx_receipt:
            if tx_receipt.status == Status.Confirmed:
                l_tx_receipt.append(tx_receipt)

    return l_tx_receipt


class ChainBase:

    def __init__(self, filter_tx):
        self.filter_tx = filter_tx

    def block_moc_transactions(self, block_number: int, full_transactions=True) -> BlockData:

        # get block and full transactions
        f_block = web3.eth.get_block(block_number, full_transactions=full_transactions)

        # Filter to only tx From MOC Contracts and tokens
        moc_transactions, d_moc_transactions = filter_transactions(f_block['transactions'], self.filter_tx)

        # get transactions receipts
        moc_transactions_receipts = transactions_receipt(moc_transactions)