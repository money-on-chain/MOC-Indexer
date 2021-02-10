import datetime
from collections import OrderedDict
from web3 import Web3

from moneyonchain.moc import MoCBucketLiquidation

from indexer.mongo_manager import mongo_manager
from .events import BaseIndexEvent

import logging
import logging.config


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger('default')


class IndexBucketLiquidation(BaseIndexEvent):

    name = 'BucketLiquidation'

    def index_event(self, tx_event, log_index=None):

        # status of tx
        status, confirmation_time = self.status_tx()

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(self.m_client)

        tx_hash = self.tx_receipt.txid
        moc_tx = self.transactions[tx_hash]

        # get all address who has bprox , at the time all users because
        # we dont know who hast bprox in certain block
        collection_users = mongo_manager.collection_user_state(self.m_client)
        users = collection_users.find()
        l_users_riskprox = list()
        for user in users:
            l_users_riskprox.append(user)
            # if float(user['bprox2Balance']) > 0.0:
            #    l_users_riskprox.append(user)

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = self.tx_receipt.block_number
        d_tx["event"] = 'BucketLiquidation'
        d_tx["tokenInvolved"] = 'RISKPROX'
        d_tx["bucket"] = 'X2'
        d_tx["status"] = status
        d_tx["confirmationTime"] = confirmation_time
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = self.tx_receipt.gas_used * Web3.fromWei(self.tx_receipt.gas_price, 'ether')
        # d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["processLogs"] = True
        d_tx["createdAt"] = self.block_ts

        prior_block_to_liquidation = self.tx_receipt.block_number - 1
        l_transactions = list()
        for user_riskprox in l_users_riskprox:
            try:
                d_user_balances = self.riskprox_balances_from_address(user_riskprox["address"],
                                                                      prior_block_to_liquidation)
            except:
                continue

            if float(d_user_balances["bprox2Balance"]) > 0.0:
                d_tx["address"] = user_riskprox["address"]
                d_tx["amount"] = str(d_user_balances["bprox2Balance"])

                post_id = collection_tx.find_one_and_update(
                    {"transactionHash": tx_hash,
                     "address": d_tx["address"],
                     "event": d_tx["event"]},
                    {"$set": d_tx},
                    upsert=True)

                log.info("Tx {0} From: [{1}] Amount: {2} Tx Hash: {3}".format(
                    d_tx["event"],
                    d_tx["address"],
                    d_tx["amount"],
                    tx_hash))

                # update user balances
                self.parent.update_balance_address(self.m_client, d_tx["address"], self.block_height)

                l_transactions.append(d_tx)

    def notifications(self, tx_event, log_index=None):
        """Event: """

        collection_tx = mongo_manager.collection_notification(self.m_client)
        tx_hash = self.tx_receipt.txid
        event_name = 'BucketLiquidation'

        d_tx = OrderedDict()
        d_tx["event"] = event_name
        d_tx["transactionHash"] = tx_hash
        d_tx["logIndex"] = log_index
        d_tx["bucket"] = 'X2'
        d_tx["timestamp"] = datetime.datetime.fromtimestamp(self.tx_receipt.timestamp)
        d_tx["processLogs"] = True

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash, "event": event_name, "logIndex": log_index},
            {"$set": d_tx},
            upsert=True)

        d_tx['post_id'] = post_id

        return d_tx

    def on_event(self, tx_event, log_index=None):
        """ Event """

        d_event = MoCBucketLiquidation(tx_event, tx_receipt=self.tx_receipt)
        self.index_event(d_event.event, log_index=log_index)
        self.notifications(d_event.event, log_index=log_index)
