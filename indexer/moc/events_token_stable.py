import datetime
from collections import OrderedDict
from web3 import Web3

from moneyonchain.moc import ERC20Transfer

from indexer.mongo_manager import mongo_manager
from indexer.logger import log
from .events import BaseIndexEvent


class IndexSTABLETransfer(BaseIndexEvent):

    name = 'Transfer'

    def index_event(self, tx_event, log_index=None):

        token_involved = 'STABLE'

        # status of tx
        status, confirmation_time = self.status_tx()

        address_from_contract = '0x0000000000000000000000000000000000000000'

        address_not_allowed = [str.lower(address_from_contract), str.lower(self.moc_address)]
        if str.lower(tx_event["from"]) in address_not_allowed or \
                str.lower(tx_event["to"]) in address_not_allowed:
            # Transfer from our Contract we dont add because already done
            # with ...Mint
            # if self.debug_mode:
            #    log.info("Token transfer not processed! From: [{0}] To [{1}]".format(
            #        tx_event.e_from, tx_event.e_to))
            return

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(self.m_client)

        tx_hash = self.tx_receipt.txid

        collection_users = mongo_manager.collection_users(self.m_client)

        exist_user = collection_users.find_one(
            {"username": tx_event["from"]}
        )

        if exist_user:
            # FROM
            d_tx = OrderedDict()
            d_tx["address"] = tx_event["from"]
            d_tx["blockNumber"] = self.tx_receipt.block_number
            d_tx["event"] = 'Transfer'
            d_tx["transactionHash"] = tx_hash
            d_tx["amount"] = str(tx_event["value"])
            d_tx["confirmationTime"] = confirmation_time
            d_tx["isPositive"] = False
            d_tx["lastUpdatedAt"] = datetime.datetime.now()
            d_tx["otherAddress"] = tx_event["to"]
            d_tx["status"] = status
            d_tx["tokenInvolved"] = token_involved
            d_tx["processLogs"] = True
            d_tx["createdAt"] = self.block_ts

            post_id = collection_tx.find_one_and_update(
                {"transactionHash": tx_hash,
                 "address": d_tx["address"],
                 "event": d_tx["event"]},
                {"$set": d_tx},
                upsert=True)

            self.parent.update_balance_address(self.m_client, d_tx["address"], self.block_height)

            log.info("Tx Transfer {0} From: [{1}] To: [{2}] Amount: {3}".format(
                token_involved,
                tx_event["from"],
                tx_event["to"],
                tx_event["value"]))

        exist_user = collection_users.find_one(
            {"username": tx_event["to"]}
        )

        if exist_user:
            # TO
            d_tx = OrderedDict()
            d_tx["address"] = tx_event["to"]
            d_tx["blockNumber"] = self.tx_receipt.block_number
            d_tx["event"] = 'Transfer'
            d_tx["transactionHash"] = tx_hash
            d_tx["amount"] = str(tx_event["value"])
            d_tx["confirmationTime"] = confirmation_time
            d_tx["isPositive"] = True
            d_tx["lastUpdatedAt"] = datetime.datetime.now()
            d_tx["otherAddress"] = tx_event["from"]
            d_tx["status"] = status
            d_tx["tokenInvolved"] = token_involved
            d_tx["processLogs"] = True
            d_tx["createdAt"] = self.block_ts

            post_id = collection_tx.find_one_and_update(
                {"transactionHash": tx_hash,
                 "address": d_tx["address"],
                 "event": d_tx["event"]},
                {"$set": d_tx},
                upsert=True)

            self.parent.update_balance_address(self.m_client, d_tx["address"], self.block_height)

            log.info("Tx Transfer {0} From: [{1}] To: [{2}] Amount: {3}".format(
                token_involved,
                tx_event["from"],
                tx_event["to"],
                tx_event["value"]))

    def on_event(self, tx_event, log_index=None):
        """ Event """

        d_event = ERC20Transfer(tx_event, tx_receipt=self.tx_receipt)
        self.index_event(d_event.event, log_index=log_index)

