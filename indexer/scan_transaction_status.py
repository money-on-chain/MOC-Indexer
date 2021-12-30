import datetime
import time

from brownie.network.transaction import Status
from moneyonchain.networks import network_manager, chain

from web3.exceptions import TransactionNotFound
from indexer.mongo_manager import mongo_manager
from indexer.logger import log


class ScanTransactionStatus:

    def __init__(self, options, app_mode, contract_loaded, contract_addresses):
        self.options = options
        self.app_mode = app_mode
        self.contract_loaded = contract_loaded
        self.contract_addresses = contract_addresses
        self.debug_mode = self.options['debug']

        # update block info
        self.last_block = network_manager.block_number
        self.block_ts = network_manager.block_timestamp(self.last_block)

    def update_info_last_block(self, m_client):

        collection_moc_indexer = mongo_manager.collection_moc_indexer(m_client)
        moc_index = collection_moc_indexer.find_one(sort=[("updatedAt", -1)])
        if moc_index:
            if 'last_block_number' in moc_index:
                self.last_block = moc_index['last_block_number']
                self.block_ts = moc_index['last_block_ts']

    def is_confirmed_block(self, block_height, block_height_last, block_height_last_ts):

        confirm_blocks = self.options['scan_moc_blocks']['confirm_blocks']
        if block_height_last - block_height > confirm_blocks:
            status = 'confirmed'
            confirmation_time = block_height_last_ts
            confirming_percent = 100
        else:
            status = 'confirming'
            confirmation_time = None
            confirming_percent = (block_height_last - block_height) * 10

        return status, confirmation_time, confirming_percent

    def scan_transaction_status_block(self, m_client, block_height, block_height_ts):

        collection_tx = mongo_manager.collection_transaction(m_client)

        seconds_not_in_chain_error = self.options['scan_moc_blocks']['seconds_not_in_chain_error']

        # Get pendings tx and check for confirming, confirmed or failed
        tx_pendings = collection_tx.find({'status': 'pending'})
        for tx_pending in tx_pendings:

            try:
                tx_receipt = chain.get_transaction(tx_pending['transactionHash'])
                #tx_receipt = network_manager.web3.eth.getTransactionReceipt(tx_pending['transactionHash'])
            except TransactionNotFound:
                tx_receipt = None

            if tx_receipt:
                d_tx_up = dict()
                if tx_receipt.status == Status.Confirmed:
                    d_tx_up['status'], d_tx_up['confirmationTime'], d_tx_up['confirmingPercent'] = \
                        self.is_confirmed_block(
                            tx_receipt.block_number,
                            block_height,
                            block_height_ts)
                elif tx_receipt.status == Status.Reverted:
                    d_tx_up['status'] = 'failed'
                    d_tx_up['confirmationTime'] = block_height_ts
                else:
                    continue

                collection_tx.find_one_and_update(
                    {"_id": tx_pending["_id"]},
                    {"$set": d_tx_up})

                log.info("[4. Scan Moc Status] Setting TX STATUS: {0} hash: {1}".format(
                    d_tx_up['status'],
                    tx_pending['transactionHash']))

        # Get confirming tx and check for confirming, confirmed or failed
        tx_pendings = collection_tx.find({'status': 'confirming'})
        for tx_pending in tx_pendings:

            try:
                tx_receipt = chain.get_transaction(tx_pending['transactionHash'])
                #tx_receipt = self.connection_manager.web3.eth.getTransactionReceipt(tx_pending['transactionHash'])
            except TransactionNotFound:
                tx_receipt = None

            if tx_receipt:
                d_tx_up = dict()
                if tx_receipt.status == Status.Confirmed:
                    d_tx_up['status'], d_tx_up['confirmationTime'], d_tx_up['confirmingPercent'] = \
                        self.is_confirmed_block(
                            tx_receipt.block_number,
                            block_height,
                            block_height_ts)
                    # if d_tx_up['status'] == 'confirming':
                    #    # is already on confirming status
                    #    # not write to db
                    #    continue
                elif tx_receipt.status == Status.Reverted:
                    d_tx_up['status'] = 'failed'
                    d_tx_up['confirmationTime'] = block_height_ts
                else:
                    continue

                collection_tx.find_one_and_update(
                    {"_id": tx_pending["_id"]},
                    {"$set": d_tx_up})

                log.info("[4. Scan Moc Status] Setting TX STATUS: {0} hash: {1}".format(
                    d_tx_up['status'],
                    tx_pending['transactionHash']))
            else:
                # no receipt from tx
                # here problem with eternal confirming
                created_at = tx_pending['createdAt']
                if created_at:
                    dte = created_at + datetime.timedelta(seconds=seconds_not_in_chain_error)
                    if dte < block_height_ts:
                        d_tx_up = dict()
                        d_tx_up['status'] = 'failed'
                        d_tx_up['errorCode'] = 'staleTransaction'
                        d_tx_up['confirmationTime'] = block_height_ts

                        collection_tx.find_one_and_update(
                            {"_id": tx_pending["_id"]},
                            {"$set": d_tx_up})

                        log.info("[4. Scan Moc Status] Setting TX STATUS: {0} hash: {1}".format(
                            d_tx_up['status'],
                            tx_pending['transactionHash']))

    def scan_transaction_status(self, task=None):

        # conect to mongo db
        m_client = mongo_manager.connect()

        # update block information
        self.update_info_last_block(m_client)

        # get last block from node
        last_block = self.last_block #network_manager.block_number

        # get block time from node
        last_block_ts = self.block_ts #network_manager.block_timestamp(last_block)

        collection_moc_indexer = mongo_manager.collection_moc_indexer(m_client)
        moc_index = collection_moc_indexer.find_one(sort=[("updatedAt", -1)])
        last_moc_status_block = 0
        if moc_index:
            if 'last_moc_status_block' in moc_index:
                last_moc_status_block = int(moc_index['last_moc_status_block'])

        if last_block <= last_moc_status_block:
            if self.debug_mode:
                log.info("[4. Scan Moc Status] Its not time to run Scan Transactions status")
            return

        if self.debug_mode:
            log.info("[4. Scan Moc Status] Starting to Scan Transactions status last block: {0} ".format(last_block))

        start_time = time.time()

        collection_moc_indexer.update_one({},
                                          {'$set': {'last_moc_status_block': last_block,
                                                    'updatedAt': datetime.datetime.now()}},
                                          upsert=True)

        self.scan_transaction_status_block(m_client, last_block, last_block_ts)

        duration = time.time() - start_time
        log.info("[4. Scan Moc Status] Done!  [{0}] [{1} seconds.]".format(last_block, duration))

    def on_task(self, task=None):
        self.scan_transaction_status(task=task)

