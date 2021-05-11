import datetime
import time

from moneyonchain.networks import network_manager

from indexer.mongo_manager import mongo_manager
from indexer.logger import log
from .status import State


class ScanState(State):

    def scan_moc_state(self):

        config_block_height = self.options['scan_moc_state']['block_height']

        # conect to mongo db
        m_client = mongo_manager.connect()

        # get last block from node
        last_block = network_manager.block_number

        block_height = config_block_height
        if block_height <= 0:
            block_height = last_block

        if self.debug_mode:
            log.info("[SCAN MOC STATE]  Starting to index MoC State on block height: {0}".format(block_height))

        # get collection moc_state from mongo
        collection_moc_state = mongo_manager.collection_moc_state(m_client)

        exist_moc_state = collection_moc_state.find_one({"lastUpdateHeight": block_height})
        if exist_moc_state:
            if self.debug_mode:
                log.info("[SCAN MOC STATE]  Not time to run moc state, already exist")
            return

        start_time = time.time()

        # get all functions from smart contract
        d_moc_state = self.moc_state_from_sc(block_identifier=block_height)
        if not d_moc_state:
            return

        # price variation
        old_block_height = last_block - d_moc_state['dayBlockSpan']

        # get last price written in mongo
        collection_price = mongo_manager.collection_price(m_client)
        daily_last_price = collection_price.find_one(filter={"blockHeight": {"$lt": old_block_height}},
                                                     sort=[("blockHeight", -1)])

        # price variation on settlement day
        d_moc_state["isDailyVariation"] = True
        if d_moc_state["blockSpan"] - d_moc_state['blocksToSettlement'] <= d_moc_state['dayBlockSpan']:
            # Price Variation is built in-app and not retrieved from blockchain.
            # For leveraged coin, variation must be against the BTC price
            # stated at the last settlement period.

            collection_settlement = mongo_manager.collection_settlement_state(m_client)

            last_settlement = collection_settlement.find_one(
                {},
                sort=[("startBlockNumber", -1)]
            )
            if last_settlement:
                daily_last_price['bprox2PriceInUsd'] = last_settlement['btcPrice']
                d_moc_state["isDailyVariation"] = False

        d_moc_state["lastUpdateHeight"] = block_height
        d_price_variation = dict()
        d_price_variation['daily'] = daily_last_price
        d_moc_state["priceVariation"] = d_price_variation

        # update or insert the new info on mocstate
        collection_moc_state.find_one_and_update(
            {},
            {"$set": d_moc_state, "$unset": {"commissionRate": ""}},
            upsert=True)

        # history
        collection_moc_state_history = mongo_manager.collection_moc_state_history(m_client)
        collection_moc_state_history.find_one_and_update(
            {"blockHeight": block_height},
            {"$set": d_moc_state},
            upsert=True)

        duration = time.time() - start_time
        log.info("[SCAN MOC STATE] BLOCKHEIGHT: [{0}] Done in {1} seconds.".format(block_height, duration))

    def scan_moc_state_history(self):

        # conect to mongo db
        m_client = mongo_manager.connect()

        from_block = self.options['scan_moc_history']['from_block']
        to_block = self.options['scan_moc_history']['to_block']

        collection_moc_indexer_history = mongo_manager.collection_moc_indexer_history(m_client)
        moc_index = collection_moc_indexer_history.find_one(sort=[("updatedAt", -1)])
        last_block_indexed = 0
        if moc_index:
            if 'last_moc_state_block' in moc_index:
                last_block_indexed = moc_index['last_moc_state_block']

        if last_block_indexed > 0:
            from_block = last_block_indexed + 1

        if from_block >= to_block:
            if self.debug_mode:
                log.info("[SCAN MOC STATE HISTORY] Its not the time to run indexer no new blocks avalaible!")
            return

        # start with from block
        current_block = from_block

        if self.debug_mode:
            log.info("[SCAN MOC STATE HISTORY] Starting to index MoC State: {0} To Block: {1} ...".format(
                from_block, to_block))

        start_time = time.time()

        while current_block <= to_block:

            # get all functions from smart contract
            d_moc_state = self.moc_state_from_sc(block_identifier=current_block)
            if not d_moc_state:
                current_block += 1
                continue

            d_moc_state["lastUpdateHeight"] = current_block
            d_moc_state["priceVariation"] = None

            # history
            collection_moc_state_history = mongo_manager.collection_moc_state_history(m_client)
            collection_moc_state_history.find_one_and_update(
                {"blockHeight": current_block},
                {"$set": d_moc_state},
                upsert=True)

            collection_moc_indexer_history.update_one({},
                                                      {'$set': {'last_moc_state_block': current_block,
                                                                'updatedAt': datetime.datetime.now()}},
                                                      upsert=True)

            if self.debug_mode:
                log.info("[SCAN MOC STATE HISTORY] [{0}]".format(current_block))

            # Go to next block
            current_block += 1

        duration = time.time() - start_time
        log.info("[SCAN MOC STATE HISTORY] Done in {0} seconds".format(duration))
