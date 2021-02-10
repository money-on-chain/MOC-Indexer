import datetime
import time

from moneyonchain.networks import network_manager

from indexer.mongo_manager import mongo_manager
from .prices import Prices

import logging
import logging.config


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger('default')


class ScanPrices(Prices):

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

    def scan_moc_prices_block(self, collection_price, current_block):

        # get block time from node
        block_ts = network_manager.block_timestamp(current_block)

        # get all functions from smart contract
        d_prices = self.prices_from_sc(block_identifier=current_block)
        if d_prices:
            # only write if there are prices
            d_prices["blockHeight"] = current_block
            d_prices["createdAt"] = block_ts

            collection_price.find_one_and_update(
                {"blockHeight": current_block},
                {"$set": d_prices},
                upsert=True)

            if self.debug_mode:
                log.info("[SCAN PRICES] Done scan prices block height: [{0}]".format(current_block))

    def scan_moc_prices(self):

        # conect to mongo db
        m_client = mongo_manager.connect()

        config_blocks_look_behind = self.options['scan_moc_blocks']['blocks_look_behind']

        # get last block from node
        last_block = network_manager.block_number

        collection_moc_indexer = mongo_manager.collection_moc_indexer(m_client)
        moc_index = collection_moc_indexer.find_one(sort=[("updatedAt", -1)])
        last_block_indexed = 0
        if moc_index:
            if 'last_moc_prices_block' in moc_index:
                last_block_indexed = moc_index['last_moc_prices_block']

        from_block = last_block - config_blocks_look_behind
        if last_block_indexed > 0:
            from_block = last_block_indexed + 1

        if from_block >= last_block:
            if self.debug_mode:
                log.info("[SCAN PRICES] Its not the time to run indexer no new blocks avalaible!")
            return

        to_block = last_block
        if from_block > to_block:
            log.error("[SCAN PRICES] To block > from block!!??")
            return

        current_block = from_block

        # get collection price from mongo
        collection_price = mongo_manager.collection_price(m_client)

        if self.debug_mode:
            log.info("[SCAN PRICES] Starting to Scan prices: {0} To Block: {1} ...".format(
                from_block, to_block))

        start_time = time.time()
        while current_block <= to_block:

            if self.debug_mode:
                log.info("[SCAN PRICES] Starting to scan MOC prices block height: [{0}]".format(
                    current_block))

            self.scan_moc_prices_block(collection_price, current_block)

            collection_moc_indexer.update_one({},
                                              {'$set': {'last_moc_prices_block': current_block,
                                                        'updatedAt': datetime.datetime.now()}},
                                              upsert=True)
            # Go to next block
            current_block += 1

        duration = time.time() - start_time
        log.info("[SCAN PRICES] LAST BLOCK HEIGHT: [{0}] Done in {1} seconds.".format(current_block, duration))

    def scan_moc_prices_history(self):

        # conect to mongo db
        m_client = mongo_manager.connect()

        from_block = self.options['scan_moc_history']['from_block']
        to_block = self.options['scan_moc_history']['to_block']

        collection_moc_indexer_history = mongo_manager.collection_moc_indexer_history(m_client)
        moc_index = collection_moc_indexer_history.find_one(sort=[("updatedAt", -1)])
        last_block_indexed = 0
        if moc_index:
            if 'last_moc_prices_block' in moc_index:
                last_block_indexed = moc_index['last_moc_prices_block']

        if last_block_indexed > 0:
            from_block = last_block_indexed + 1

        if from_block >= to_block:
            if self.debug_mode:
                log.info("[SCAN PRICES HISTORY] Its not the time to run indexer no new blocks avalaible!")
            return

        current_block = from_block

        # get collection price from mongo
        collection_price = mongo_manager.collection_price(m_client)

        if self.debug_mode:
            log.info("[SCAN PRICES HISTORY] Starting to Scan prices: {0} To Block: {1} ...".format(from_block, to_block))

        start_time = time.time()
        while current_block <= to_block:

            if self.debug_mode:
                log.info("[SCAN PRICES HISTORY] Starting to scan MOC prices block height: [{0}]".format(
                    current_block))

            self.scan_moc_prices_block(collection_price, current_block)

            collection_moc_indexer_history.update_one({},
                                              {'$set': {'last_moc_prices_block': current_block,
                                                        'updatedAt': datetime.datetime.now()}},
                                              upsert=True)
            # Go to next block
            current_block += 1

        duration = time.time() - start_time
        log.info("[SCAN PRICES HISTORY] LAST BLOCK HEIGHT: [{0}] Done in {1} seconds.".format(current_block, duration))
