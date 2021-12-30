import time

from moneyonchain.networks import network_manager

from indexer.mongo_manager import mongo_manager
from indexer.logger import log

from .moc_balances import update_balance_address


class ScanUser:

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

    def scan_user_state_update(self, task=None):

        # conect to mongo db
        m_client = mongo_manager.connect()

        # get last block from node
        last_block = self.last_block#network_manager.block_number

        collection_user_state_update = mongo_manager.collection_user_state_update(m_client)
        users_pending_update = collection_user_state_update.find({})

        if self.debug_mode:
            log.info("[6. Scan User State Update] Starting to update user balance on block: {0} ".format(last_block))

        start_time = time.time()

        # get list of users to update balance
        for user_update in users_pending_update:

            # update block information
            self.update_info_last_block(m_client)

            block_height = self.last_block #network_manager.block_number

            # udpate balance of address of the account on the last block height
            update_balance_address(m_client,
                                   self.contract_loaded,
                                   self.contract_addresses,
                                   user_update['account'],
                                   block_height,
                                   app_mode=self.app_mode,
                                   block_ts=self.block_ts)

            collection_user_state_update.delete_many({'account': user_update['account']})

            if self.debug_mode:
                log.info("[6. Scan User State Update] UPDATING ACCOUNT BALANCE: {0} BLOCKHEIGHT: {1}".format(
                    user_update['account'],
                    block_height))

        duration = time.time() - start_time
        log.info("[6. Scan User State Update] Done! [{0}] [{1} seconds.]".format(last_block, duration))

    def on_task(self, task=None):
        self.scan_user_state_update(task=task)
