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
        self.last_block = network_manager.block_number
        self.block_ts = network_manager.block_timestamp(self.last_block)
        self.debug_mode = self.options['debug']

    def scan_user_state_update(self, task=None):

        # conect to mongo db
        m_client = mongo_manager.connect()

        # get last block from node
        last_block = network_manager.block_number

        collection_user_state_update = mongo_manager.collection_user_state_update(m_client)
        users_pending_update = collection_user_state_update.find({})

        if self.debug_mode:
            log.info("[6. Scan User State Update] Starting to update user balance on block: {0} ".format(last_block))

        start_time = time.time()

        # get list of users to update balance
        for user_update in users_pending_update:

            block_height = network_manager.block_number

            # udpate balance of address of the account on the last block height
            update_balance_address(m_client,
                                   self.contract_loaded,
                                   self.contract_addresses,
                                   user_update['account'],
                                   block_height,
                                   app_mode=self.app_mode)

            collection_user_state_update.delete_many({'account': user_update['account']})

            if self.debug_mode:
                log.info("[6. Scan User State Update] UPDATING ACCOUNT BALANCE: {0} BLOCKHEIGHT: {1}".format(
                    user_update['account'],
                    block_height))

        duration = time.time() - start_time
        log.info("[6. Scan User State Update] Done! [{0}] [{1} seconds.]".format(last_block, duration))

    def on_task(self, task=None):
        self.scan_user_state_update(task=task)
