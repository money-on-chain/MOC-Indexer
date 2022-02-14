import time

from moneyonchain.networks import network_manager

from indexer.mongo_manager import mongo_manager
from indexer.logger import log
from .moc_state import moc_state_from_sc


class ScanMoCState:

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

    def scan_moc_state(self, task=None):

        config_block_height = self.options['scan_moc_state']['block_height']

        # conect to mongo db
        m_client = mongo_manager.connect()

        # update block information
        self.update_info_last_block(m_client)

        # get last block from node
        last_block = self.last_block #network_manager.block_number

        block_height = config_block_height
        if block_height <= 0:
            block_height = last_block

        if self.debug_mode:
            log.info("[4. Scan Moc State]  Starting to index MoC State on block height: {0}".format(block_height))

        # get collection moc_state from mongo
        collection_moc_state = mongo_manager.collection_moc_state(m_client)

        exist_moc_state = collection_moc_state.find_one({"lastUpdateHeight": block_height})
        if exist_moc_state:
            if self.debug_mode:
                log.info("[4. Scan Moc State]  Not time to run moc state, already exist")
            return

        start_time = time.time()

        # get all functions from smart contract
        d_moc_state = moc_state_from_sc(
            self.contract_loaded,
            self.contract_addresses,
            block_identifier=block_height,
            block_ts=self.block_ts,
            app_mode=self.app_mode)

        if not d_moc_state:
            log.error("[4. Scan Moc State] NOT VALID PRICE OR MOC STATE. blockheight: [0]".format(block_height))
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
            {"$set": d_moc_state, "$unset": {"commissionRate": "", "bproAvailableToMint": ""}},
            upsert=True)

        # history
        collection_moc_state_history = mongo_manager.collection_moc_state_history(m_client)
        collection_moc_state_history.find_one_and_update(
            {"blockHeight": block_height},
            {"$set": d_moc_state},
            upsert=True)

        duration = time.time() - start_time
        log.info("[4. Scan Moc State] Done! [{0}] [{1} seconds.]".format(block_height, duration))

    def on_task(self, task=None):
        self.scan_moc_state(task=task)
