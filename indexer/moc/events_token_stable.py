import datetime
from collections import OrderedDict
from web3 import Web3

from moneyonchain.moc import ERC20Transfer

from .mongo_manager import mongo_manager
from .base import BaseIndexEvent

import logging
import logging.config


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger('default')


BUCKET_X2 = '0x5832000000000000000000000000000000000000000000000000000000000000'
BUCKET_C0 = '0x4330000000000000000000000000000000000000000000000000000000000000'

d_states = {
    0: "Liquidated",
    1: "BProDiscount",
    2: "BelowCobj",
    3: "AboveCobj"
}


class IndexSTABLETransfer(BaseIndexEvent):

    name = 'Transfer'

    def index_event(self, tx_event, token_involved='STABLE'):

        if self.block_height_current - self.block_height > self.confirm_blocks:
            status = 'confirmed'
            confirmation_time = self.block_ts
        else:
            status = 'confirming'
            confirmation_time = None

        address_from_contract = '0x0000000000000000000000000000000000000000'
        addresses_moc = self.contract_MoC.address()

        address_not_allowed = [str.lower(address_from_contract), str.lower(addresses_moc)]
        if str.lower(tx_event["e_from"]) in address_not_allowed or \
                str.lower(tx_event["e_to"]) in address_not_allowed:
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
            {"username": tx_event["e_from"]}
        )

        if exist_user:
            # FROM
            d_tx = OrderedDict()
            d_tx["address"] = tx_event["e_from"]
            d_tx["blockNumber"] = tx_event["blockNumber"]
            d_tx["event"] = 'Transfer'
            d_tx["transactionHash"] = tx_hash
            d_tx["amount"] = str(tx_event["value"])
            d_tx["confirmationTime"] = confirmation_time
            d_tx["isPositive"] = False
            d_tx["lastUpdatedAt"] = datetime.datetime.now()
            d_tx["otherAddress"] = tx_event["e_to"]
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

            self.update_balance_address(self.m_client, d_tx["address"], self.block_height)

            log.info("Tx Transfer {0} From: [{1}] To: [{2}] Amount: {3}".format(
                token_involved,
                tx_event["e_from"],
                tx_event["e_to"],
                tx_event["value"]))

        exist_user = collection_users.find_one(
            {"username": tx_event["e_to"]}
        )

        if exist_user:
            # TO
            d_tx = OrderedDict()
            d_tx["address"] = tx_event["e_to"]
            d_tx["blockNumber"] = tx_event["blockNumber"]
            d_tx["event"] = 'Transfer'
            d_tx["transactionHash"] = tx_hash
            d_tx["amount"] = str(tx_event["value"])
            d_tx["confirmationTime"] = confirmation_time
            d_tx["isPositive"] = True
            d_tx["lastUpdatedAt"] = datetime.datetime.now()
            d_tx["otherAddress"] = tx_event["e_from"]
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

            self.update_balance_address(self.m_client, d_tx["address"], self.block_height)

            log.info("Tx Transfer {0} From: [{1}] To: [{2}] Amount: {3}".format(
                token_involved,
                tx_event["e_from"],
                tx_event["e_to"],
                tx_event["value"]))

    def index_events(self):
        """ Index  """

        if not self.tx_receipt.events:
            # return if there are no logs events decoded
            return

        if not self.tx_receipt.logs:
            # return if there are no logs events in raw mode
            return

        filter_address = self.contract_MoC.sc_moc_doc_token.address()

        tx_index = 0
        raw_logs = self.tx_receipt.logs

        # Transfer
        for tx_event in self.tx_receipt.events:

            if str(raw_logs[tx_index]['address']).lower() != str(filter_address).lower():
                continue

            if self.name in tx_event:
                d_event = ERC20Transfer(tx_event[self.name],
                                                 tx_receipt=self.tx_receipt)
                self.index_event(d_event.event, token_involved='STABLE')

            tx_index += 1
