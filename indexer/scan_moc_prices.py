import datetime
import time

from moneyonchain.networks import network_manager

from indexer.mongo_manager import mongo_manager
from indexer.logger import log
from .moc_prices import prices_from_sc


class ScanMoCPrices:

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

    def scan_moc_prices_block(self, collection_price, current_block):

        # get all functions from smart contract
        d_prices = prices_from_sc(
            self.contract_loaded,
            self.contract_addresses,
            block_identifier=current_block,
            block_ts=self.block_ts,
            app_mode=self.app_mode)
        if d_prices:
            # only write if there are prices
            collection_price.find_one_and_update(
                {"blockHeight": d_prices["blockHeight"]},
                {"$set": d_prices},
                upsert=True)

            if self.debug_mode:
                log.info("[3. Scan Prices] Done scan prices block height: [{0}]".format(current_block))

    def scan_moc_prices(self, task=None):

        # conect to mongo db
        m_client = mongo_manager.connect()

        config_blocks_look_behind = self.options['scan_moc_prices']['blocks_look_behind']

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
                log.info("[3. Scan Prices] Its not the time to run indexer no new blocks avalaible!")
            return

        to_block = last_block
        if from_block > to_block:
            log.error("[3. Scan Prices] To block > from block!!??")
            return

        current_block = from_block

        # get collection price from mongo
        collection_price = mongo_manager.collection_price(m_client)

        if self.debug_mode:
            log.info("[3. Scan Prices] Starting to Scan prices: [{0} / {1}]".format(
                from_block, to_block))

        start_time = time.time()
        while current_block <= to_block:

            # update block information
            self.update_info_last_block(m_client)

            log.info("[3. Scan Prices] Starting to scan MOC prices [{0} / {1}]".format(
                current_block, to_block))

            self.scan_moc_prices_block(collection_price, current_block)

            collection_moc_indexer.update_one({},
                                              {'$set': {'last_moc_prices_block': current_block,
                                                        'updatedAt': datetime.datetime.now()}},
                                              upsert=True)
            # Go to next block
            current_block += 1

        duration = time.time() - start_time
        log.info("[3. Scan Prices] Done! [{0}] [{1} seconds.]".format(current_block, duration))

    def on_task(self, task=None):
        self.scan_moc_prices(task=task)
