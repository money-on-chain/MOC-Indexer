import datetime
import time

from moneyonchain.networks import network_manager, chain
from moneyonchain.utils import filter_transactions

from web3.exceptions import TransactionNotFound
from indexer.mongo_manager import mongo_manager

from .events_mocexchange import IndexRiskProMint, \
    IndexRiskProRedeem, \
    IndexRiskProxMint, \
    IndexRiskProxRedeem, \
    IndexStableTokenMint, \
    IndexStableTokenRedeem, \
    IndexFreeStableTokenRedeem
from .utils import transactions_receipt
from .balances import Balances

import logging
import logging.config


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger('default')


class ScanBlocks(Balances):

    def scan_moc_block(self, current_block, block_reference, m_client, scan_transfer=True):

        if self.debug_mode:
            log.info("[SCAN TX] Starting to scan MOC transactions block height: [{0}] last block height: [{1}]".format(
                current_block, block_reference))

        # get block time from node
        block_ts = network_manager.block_timestamp(current_block)

        # get moc contracts adressess
        moc_addresses = self.moc_contract_addresses()

        # get block and full transactions
        f_block = chain.get_block(current_block)
        all_transactions = f_block['transactions']

        # From MOC Contract transactions
        moc_transactions, d_moc_transactions = filter_transactions(all_transactions, moc_addresses)

        # get transactions receipts
        moc_transactions_receipts = transactions_receipt(moc_transactions)

        index_info = dict(
            m_client=m_client,
            parent=self,
            confirm_blocks=self.options['scan_moc_blocks']['confirm_blocks'],
            block_height=current_block,
            block_height_current=block_reference,
            transactions=d_moc_transactions,
            block_ts=block_ts
        )

        # 1. Exchange events index
        index_riskpro_mint = IndexRiskProMint(**index_info)
        index_riskpro_redeem = IndexRiskProRedeem(**index_info)
        index_riskprox_mint = IndexRiskProxMint(**index_info)
        index_riskprox_redeem = IndexRiskProxRedeem(**index_info)
        index_stabletoken_mint = IndexStableTokenMint(**index_info)
        index_stabletoken_redeem = IndexStableTokenRedeem(**index_info)
        index_freestabletoken_redeem = IndexFreeStableTokenRedeem(**index_info)

        # process only MoC contract transactions
        for tx_receipt in moc_transactions_receipts:

            # 1. MoC Exchange

            # IndexRiskProMint
            index_riskpro_mint.index_from_receipt(tx_receipt)

            # IndexRiskProRedeem
            index_riskpro_redeem.index_from_receipt(tx_receipt)

            # IndexRiskProxMint
            index_riskprox_mint.index_from_receipt(tx_receipt)

            # IndexRiskProxRedeem
            index_riskprox_redeem.index_from_receipt(tx_receipt)

            # IndexStableTokenMint
            index_stabletoken_mint.index_from_receipt(tx_receipt)

            # IndexStableTokenRedeem
            index_stabletoken_redeem.index_from_receipt(tx_receipt)

            # IndexFreeStableTokenRedeem
            index_freestabletoken_redeem.index_from_receipt(tx_receipt)

            # self.logs_process_moc_settlement(tx_receipt,
            #                                  m_client,
            #                                  current_block,
            #                                  block_reference,
            #                                  d_moc_transactions,
            #                                  block_ts)
            # self.logs_process_moc_inrate(tx_receipt, m_client, block_ts)
            # self.logs_process_moc(tx_receipt,
            #                       m_client,
            #                       current_block,
            #                       block_reference,
            #                       d_moc_transactions, block_ts)
            # self.logs_process_moc_state(tx_receipt, m_client)
            # if self.app_mode == "RRC20":
            #     self.logs_process_reserve_approval(tx_receipt, m_client)
            #     self.logs_process_transfer_from_reserve(tx_receipt,
            #                                             m_client,
            #                                             current_block,
            #                                             block_reference, block_ts)
            #
            # # Process transfer for MOC 2020-06-23
            # self.process_transfer_from_moc(tx_receipt,
            #                                d_moc_transactions,
            #                                m_client,
            #                                current_block,
            #                                block_reference, block_ts)

        # process all transactions looking for transfers
        if scan_transfer:
            if self.debug_mode:
                log.info("[SCAN TX] Starting to scan Transfer transactions block height: [{0}] last block height: [{1}]".format(
                    current_block, block_reference))

            all_transactions_receipts = transactions_receipt(all_transactions)
            for tx_receipt in all_transactions_receipts:
                pass
                #self.logs_process_transfer(tx_receipt, m_client, current_block, block_reference, block_ts)

    def scan_moc_blocks(self,
                        scan_transfer=True):

        start_time = time.time()

        # conect to mongo db
        m_client = mongo_manager.connect()

        # get the block recesion is a margin of problems to not get the inmediat new instead
        # 2 older blocks from new.
        config_blocks_recession = self.options['scan_moc_blocks']['blocks_recession']

        # get last block from node compare 2 blocks older than new
        last_block = network_manager.block_number - config_blocks_recession

        collection_moc_indexer = mongo_manager.collection_moc_indexer(m_client)
        moc_index = collection_moc_indexer.find_one(sort=[("updatedAt", -1)])
        last_block_indexed = 0
        if moc_index:
            if 'last_moc_block' in moc_index:
                last_block_indexed = moc_index['last_moc_block']

        config_blocks_look_behind = self.options['scan_moc_blocks']['blocks_look_behind']
        from_block = last_block - config_blocks_look_behind
        if last_block_indexed > 0:
            from_block = last_block_indexed + 1

        if from_block >= last_block:
            if self.debug_mode:
                log.info("[SCAN TX] Its not the time to run indexer no new blocks avalaible!")
            return

        to_block = last_block

        if from_block > to_block:
            log.error("[SCAN TX] To block > from block!!??")
            return

        # block reference is the last block, is to compare to... except you specified in the settings
        block_reference = last_block

        # start with from block
        current_block = from_block

        if self.debug_mode:
            log.info("[SCAN TX] Starting to Scan Transactions: {0} To Block: {1} ...".format(from_block, to_block))

        while current_block <= to_block:

            self.scan_moc_block(current_block, block_reference, m_client, scan_transfer=scan_transfer)

            log.info("[SCAN TX] DONE BLOCK HEIGHT: [{0}] / [{1}]".format(current_block, to_block))
            collection_moc_indexer.update_one({},
                                              {'$set': {'last_moc_block': current_block,
                                                        'updatedAt': datetime.datetime.now()}},
                                              upsert=True)
            # Go to next block
            current_block += 1

        duration = time.time() - start_time
        log.info("[SCAN TX] LAST BLOCK HEIGHT: [{0}] Done in {1} seconds".format(current_block, duration))

    def scan_moc_blocks_history(self,
                                scan_transfer=True):

        start_time = time.time()

        # get the block recesion is a margin of problems to not get the inmediat new instead
        # 2 older blocks from new.
        config_blocks_recession = self.options['scan_moc_blocks']['blocks_recession']

        # get last block from node compare 2 blocks older than new
        last_block = network_manager.block_number - config_blocks_recession

        # conect to mongo db
        m_client = mongo_manager.connect()

        from_block = self.options['scan_moc_history']['from_block']
        to_block = self.options['scan_moc_history']['to_block']

        collection_moc_indexer_history = mongo_manager.collection_moc_indexer_history(m_client)
        moc_index = collection_moc_indexer_history.find_one(sort=[("updatedAt", -1)])
        last_block_indexed = 0
        if moc_index:
            if 'last_moc_block' in moc_index:
                if moc_index['last_moc_block'] > 0:
                    last_block_indexed = moc_index['last_moc_block']

        if last_block_indexed > 0:
            from_block = last_block_indexed + 1

        if from_block >= to_block:
            if self.debug_mode:
                log.info("[SCAN TX HISTORY] Its not the time to run indexer no new blocks avalaible!")
            return

        # start with from block
        current_block = from_block

        if self.debug_mode:
            log.info("[SCAN TX HISTORY] Starting to Scan Transactions: {0} To Block: {1} ...".format(from_block,
                                                                                                     to_block))

        while current_block <= to_block:
            self.scan_moc_block(current_block, last_block, m_client, scan_transfer=scan_transfer)

            log.info("[SCAN TX HISTORY] DONE BLOCK HEIGHT: [{0}] / [{1}]".format(current_block, to_block))
            collection_moc_indexer_history.update_one({},
                                              {'$set': {'last_moc_block': current_block,
                                                        'updatedAt': datetime.datetime.now()}},
                                              upsert=True)

            # Go to next block
            current_block += 1

        duration = time.time() - start_time
        log.info("[SCAN TX HISTORY] LAST BLOCK HEIGHT: [{0}] Done in {1} seconds".format(current_block, duration))

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

    def force_start_history(self):

        log.info("[FORCE START HISTORY] Removing collection...")

        # conect to mongo db
        m_client = mongo_manager.connect()

        collection_moc_indexer_history = mongo_manager.collection_moc_indexer_history(m_client)

        collection_moc_indexer_history.update_one({},
                                                  {'$set': {'last_moc_block': 0,
                                                            'updatedAt': datetime.datetime.now()}},
                                                  upsert=True)

        #collection_moc_indexer_history.drop()

        log.info("[FORCE START HISTORY] DONE! Collection remove it!.")

    def scan_moc_blocks_not_processed(self):

        if self.debug_mode:
            log.info("[SCAN BLOCK NOT PROCESSED] Starting to scan blocks Not processed ")

        start_time = time.time()

        # get last block from node
        last_block = network_manager.block_number

        # conect to mongo db
        m_client = mongo_manager.connect()

        collection_tx = mongo_manager.collection_transaction(m_client)

        # we need to query tx with processLogs=None and in the last 60 minutes
        only_new_ones = datetime.datetime.now() - datetime.timedelta(minutes=10)
        moc_txs = collection_tx.find({"processLogs": None,
                                      "status": "confirmed",
                                      "createdAt": {"$gte": only_new_ones}},
                                     sort=[("createdAt", -1)])

        if moc_txs:
            for moc_tx in moc_txs:
                log.info("[SCAN BLOCK NOT PROCESSED] PROCESSING HASH: [{0}]".format(moc_tx['transactionHash']))
                try:
                    tx_receipt = chain.get_transaction(moc_tx['transactionHash'])
                    #tx_receipt = self.connection_manager.web3.eth.getTransactionReceipt(moc_tx['transactionHash'])
                except TransactionNotFound:
                    log.error("[SCAN BLOCK NOT PROCESSED] TX NOT FOUND: [{0}]".format(moc_tx['transactionHash']))
                    continue

                log.info("[SCAN BLOCK NOT PROCESSED] PROCESSING HASH: [{0}]".format(moc_tx['transactionHash']))

                self.scan_moc_block(tx_receipt['blockNumber'], last_block, m_client)

        duration = time.time() - start_time

        log.info("[SCAN BLOCK NOT PROCESSED] Done in {0} seconds.".format(duration))
