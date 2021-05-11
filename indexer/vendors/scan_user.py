import time

from moneyonchain.networks import network_manager

from indexer.mongo_manager import mongo_manager
from indexer.logger import log

from .balances import Balances


class ScanUser(Balances):

    def scan_user_state_update(self):

        # conect to mongo db
        m_client = mongo_manager.connect()

        # get last block from node
        last_block = network_manager.block_number

        collection_user_state_update = mongo_manager.collection_user_state_update(m_client)
        users_pending_update = collection_user_state_update.find({})

        if self.debug_mode:
            log.info("[SCAN USER STATE UPDATE] Starting to update user balance on block: {0} ".format(last_block))

        start_time = time.time()

        # get list of users to update balance
        for user_update in users_pending_update:

            block_height = network_manager.block_number

            # udpate balance of address of the account on the last block height
            self.update_balance_address(m_client, user_update['account'], block_height)

            collection_user_state_update.remove({'account': user_update['account']})

            if self.debug_mode:
                log.info("[SCAN USER STATE UPDATE] UPDATING ACCOUNT BALANCE: {0} BLOCKHEIGHT: {1}".format(
                    user_update['account'],
                    block_height))

        duration = time.time() - start_time
        log.info("[SCAN USER STATE UPDATE] BLOCK HEIGHT: [{0}] Done in {1} seconds.".format(last_block, duration))
