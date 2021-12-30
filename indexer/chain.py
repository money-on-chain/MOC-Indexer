import datetime
from web3 import Web3
from web3.exceptions import TransactionNotFound

from hexbytes import HexBytes
from typing import Union
from brownie.network.transaction import Status

from moneyonchain.networks import web3
from moneyonchain.transaction import TransactionReceipt

from indexer.logger import log


LOCAL_TIMEZONE = datetime.datetime.now().astimezone().tzinfo


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


def transactions_receipt(transactions, index_status=Status.Confirmed, index_min_confirmation=1):
    """ Get transaction receipt by default only confirmed and 1 block confirmation"""

    l_tx_receipt = list()
    for tx in transactions:
        try:
            tx_receipt = get_transaction(Web3.toHex(tx['hash']), required_confs=index_min_confirmation)
        except TransactionNotFound:
            log.error("No transaction receipt for hash: [{0}]".format(
                Web3.toHex(tx['hash'])))
            tx_receipt = None
        if tx_receipt:
            if tx_receipt.status == index_status and tx_receipt.confirmations >= index_min_confirmation:
                l_tx_receipt.append(tx_receipt)

    return l_tx_receipt


def block_filtered_transactions(block_number: int, full_transactions=True, filter_tx=None, index_min_confirmation=1):
    """ Get only interested transactions"""

    # get block and full transactions
    f_block = web3.eth.get_block(block_number, full_transactions=full_transactions)

    # Filter to only tx
    fil_transactions, d_fil_transactions = filter_transactions(f_block['transactions'], filter_tx)

    # get transactions receipts
    fil_transactions_receipts = transactions_receipt(fil_transactions, index_min_confirmation=index_min_confirmation)

    txs = dict()
    txs['txs'] = fil_transactions
    txs['d_txs'] = d_fil_transactions
    txs['receipts'] = fil_transactions_receipts
    txs['block_number'] = f_block['number']
    txs['block_ts'] = datetime.datetime.fromtimestamp(f_block['timestamp'], LOCAL_TIMEZONE)

    return txs


class ChainBlock:
    def __init__(self,
                 block_number,
                 last_block_number):
        self.block_number = block_number
        self.last_block_number = last_block_number
