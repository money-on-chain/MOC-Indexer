import datetime
import time

from moneyonchain.networks import network_manager, chain

from web3.exceptions import TransactionNotFound
from indexer.mongo_manager import mongo_manager
from indexer.logger import log

from .status import State


class ScanStatus(State):

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
                if tx_receipt['status'] == 1:
                    d_tx_up['status'], d_tx_up['confirmationTime'], d_tx_up['confirmingPercent'] = \
                        self.is_confirmed_block(
                            tx_receipt['blockNumber'],
                            block_height,
                            block_height_ts)
                elif tx_receipt['status'] == 0:
                    d_tx_up['status'] = 'failed'
                    d_tx_up['confirmationTime'] = block_height_ts
                else:
                    continue

                collection_tx.find_one_and_update(
                    {"_id": tx_pending["_id"]},
                    {"$set": d_tx_up})

                log.info("[SCAN TX STATUS] Setting TX STATUS: {0} hash: {1}".format(
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
                if tx_receipt['status'] == 1:
                    d_tx_up['status'], d_tx_up['confirmationTime'], d_tx_up['confirmingPercent'] = \
                        self.is_confirmed_block(
                            tx_receipt['blockNumber'],
                            block_height,
                            block_height_ts)
                    # if d_tx_up['status'] == 'confirming':
                    #    # is already on confirming status
                    #    # not write to db
                    #    continue
                elif tx_receipt['status'] == 0:
                    d_tx_up['status'] = 'failed'
                    d_tx_up['confirmationTime'] = block_height_ts
                else:
                    continue

                collection_tx.find_one_and_update(
                    {"_id": tx_pending["_id"]},
                    {"$set": d_tx_up})

                log.info("[SCAN TX STATUS] Setting TX STATUS: {0} hash: {1}".format(
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

                        log.info("[SCAN TX STATUS] Setting TX STATUS: {0} hash: {1}".format(
                            d_tx_up['status'],
                            tx_pending['transactionHash']))

    def scan_transaction_status(self):

        # conect to mongo db
        m_client = mongo_manager.connect()

        # get last block from node
        last_block = network_manager.block_number

        # get block time from node
        last_block_ts = network_manager.block_timestamp(last_block)

        collection_moc_indexer = mongo_manager.collection_moc_indexer(m_client)
        moc_index = collection_moc_indexer.find_one(sort=[("updatedAt", -1)])
        last_moc_status_block = 0
        if moc_index:
            if 'last_moc_status_block' in moc_index:
                last_moc_status_block = int(moc_index['last_moc_status_block'])

        if last_block <= last_moc_status_block:
            if self.debug_mode:
                log.info("[SCAN TX STATUS] Its not time to run Scan Transactions status")
            return

        if self.debug_mode:
            log.info("[SCAN TX STATUS] Starting to Scan Transactions status last block: {0} ".format(last_block))

        start_time = time.time()

        collection_moc_indexer.update_one({},
                                          {'$set': {'last_moc_status_block': last_block,
                                                    'updatedAt': datetime.datetime.now()}},
                                          upsert=True)

        self.scan_transaction_status_block(m_client, last_block, last_block_ts)

        duration = time.time() - start_time
        log.info("[SCAN TX STATUS] BLOCK HEIGHT: [{0}] Done in {1} seconds.".format(last_block, duration))

    def scan_moc_state_status_block(self, collection_moc_state_status, current_block):

        # get block time from node
        block_ts = network_manager.block_timestamp(current_block)

        # get all functions from smart contract
        d_status = self.state_status_from_sc(block_identifier=current_block)
        d_status["blockHeight"] = current_block
        d_status["createdAt"] = block_ts

        collection_moc_state_status.find_one_and_update(
            {"blockHeight": current_block},
            {"$set": d_status},
            upsert=True)

        if self.debug_mode:
            log.info("Done scan state status block height: [{0}]".format(current_block))

    def scan_moc_state_status(self):

        # conect to mongo db
        m_client = mongo_manager.connect()

        config_blocks_look_behind = self.options['scan_moc_state_status']['blocks_look_behind']

        # get last block from node
        last_block = network_manager.block_number

        collection_moc_indexer = mongo_manager.collection_moc_indexer(m_client)
        moc_index = collection_moc_indexer.find_one(sort=[("updatedAt", -1)])
        last_block_indexed = 0
        if moc_index:
            if 'last_moc_state_status_block' in moc_index:
                last_block_indexed = moc_index['last_moc_state_status_block']

        from_block = last_block - config_blocks_look_behind
        if last_block_indexed > 0:
            from_block = last_block_indexed + 1

        if from_block >= last_block:
            if self.debug_mode:
                log.info("[SCAN STATE STATUS] Its not the time to run indexer no new blocks avalaible!")
            return

        to_block = last_block
        if from_block > to_block:
            log.error("[SCAN STATE STATUS] To block > from block!!??")
            return

        current_block = from_block

        # get collection price from mongo
        collection_moc_state_status = mongo_manager.collection_moc_state_status(m_client)

        if self.debug_mode:
            log.info("[SCAN STATE STATUS] Starting to Scan Moc State Status: {0} To Block: {1} ...".format(
                from_block, to_block))

        start_time = time.time()
        while current_block <= to_block:

            if self.debug_mode:
                log.info("[SCAN STATE STATUS] Starting to scan Moc State Status block height: [{0}]".format(
                    current_block))

            self.scan_moc_state_status_block(collection_moc_state_status, current_block)

            collection_moc_indexer.update_one({},
                                              {'$set': {'last_moc_state_status_block': current_block,
                                                        'updatedAt': datetime.datetime.now()}},
                                              upsert=True)
            # Go to next block
            current_block += 1

        duration = time.time() - start_time
        log.info("[SCAN STATE STATUS] LAST BLOCK HEIGHT: [{0}] Done in {1} seconds.".format(current_block, duration))

    def scan_moc_state_status_history(self):

        # conect to mongo db
        m_client = mongo_manager.connect()

        from_block = self.options['scan_moc_history']['from_block']
        to_block = self.options['scan_moc_history']['to_block']

        collection_moc_indexer_history = mongo_manager.collection_moc_indexer_history(m_client)
        moc_index = collection_moc_indexer_history.find_one(sort=[("updatedAt", -1)])
        last_block_indexed = 0
        if moc_index:
            if 'last_moc_state_status_block' in moc_index:
                last_block_indexed = moc_index['last_moc_state_status_block']

        if last_block_indexed > 0:
            from_block = last_block_indexed + 1

        if from_block >= to_block:
            if self.debug_mode:
                log.info("[SCAN STATE STATUS HISTORY] Its not the time to run indexer no new blocks avalaible!")
            return

        current_block = from_block

        # get collection price from mongo
        collection_moc_state_status = mongo_manager.collection_moc_state_status(m_client)

        if self.debug_mode:
            log.info("[SCAN STATE STATUS HISTORY] Starting to Scan Moc State Status: {0} To Block: {1} ...".format(
                from_block, to_block))

        start_time = time.time()
        while current_block <= to_block:

            if self.debug_mode:
                log.info("[SCAN STATE STATUS HISTORY] Starting to scan Moc State Status block height: [{0}]".format(
                    current_block))

            self.scan_moc_state_status_block(collection_moc_state_status, current_block)

            collection_moc_indexer_history.update_one({},
                                              {'$set': {'last_moc_state_status_block': current_block,
                                                        'updatedAt': datetime.datetime.now()}},
                                              upsert=True)
            # Go to next block
            current_block += 1

        duration = time.time() - start_time
        log.info("[SCAN STATE STATUS HISTORY] LAST BLOCK HEIGHT: [{0}] Done in {1} seconds.".format(
            current_block, duration))
