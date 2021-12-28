import datetime
from collections import OrderedDict
from web3 import Web3

from moneyonchain.moc import ERC20Transfer

from indexer.mongo_manager import mongo_manager
from indexer.logger import log
from indexer.moc_balances import insert_update_balance_address
from .events import BaseIndexEvent


class IndexSTABLETransfer(BaseIndexEvent):

    name = 'Transfer'

    def __init__(self, options, app_mode, moc_contract):

        self.options = options
        self.app_mode = app_mode
        self.moc_contract = moc_contract

        super().__init__(options, app_mode)

    def index_event(self, m_client, parse_receipt, tx_event):

        token_involved = 'STABLE'

        # status of tx
        status, confirmation_time = self.status_tx(parse_receipt)

        address_from_contract = '0x0000000000000000000000000000000000000000'

        address_not_allowed = [str.lower(address_from_contract), str.lower(self.moc_contract)]
        if str.lower(tx_event["from"]) in address_not_allowed or \
                str.lower(tx_event["to"]) in address_not_allowed:
            # Transfer from our Contract we dont add because already done
            # with ...Mint
            # if self.debug_mode:
            #    log.info("Token transfer not processed! From: [{0}] To [{1}]".format(
            #        tx_event.e_from, tx_event.e_to))
            return

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(m_client)

        tx_hash = parse_receipt["transactionHash"]

        collection_users = mongo_manager.collection_users(m_client)

        exist_user = collection_users.find_one(
            {"username": tx_event["from"]}
        )

        if exist_user:
            # FROM
            d_tx = OrderedDict()
            d_tx["address"] = tx_event["from"]
            d_tx["blockNumber"] = parse_receipt["blockNumber"]
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
            d_tx["createdAt"] = parse_receipt['chain']['block_ts']

            post_id = collection_tx.find_one_and_update(
                {"transactionHash": tx_hash,
                 "address": d_tx["address"],
                 "event": d_tx["event"]},
                {"$set": d_tx},
                upsert=True)

            # Insert as pending to update user balances
            insert_update_balance_address(m_client, d_tx["address"])

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
            d_tx["blockNumber"] = parse_receipt["blockNumber"]
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
            d_tx["createdAt"] = parse_receipt['chain']['block_ts']

            post_id = collection_tx.find_one_and_update(
                {"transactionHash": tx_hash,
                 "address": d_tx["address"],
                 "event": d_tx["event"]},
                {"$set": d_tx},
                upsert=True)

            # Insert as pending to update user balances
            insert_update_balance_address(m_client, d_tx["address"])

            log.info("Tx Transfer {0} From: [{1}] To: [{2}] Amount: {3}".format(
                token_involved,
                tx_event["from"],
                tx_event["to"],
                tx_event["value"]))

    def on_event(self, m_client, parse_receipt):
        """ Event """

        cl_tx_event = ERC20Transfer(parse_receipt)
        self.index_event(m_client, parse_receipt, cl_tx_event.event[self.name])
