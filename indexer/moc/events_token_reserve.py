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


class IndexRESERVETransfer(BaseIndexEvent):

    name = 'Transfer'

    def index_event(self, tx_event):

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

        collection_users = mongo_manager.collection_users(self.m_client)

        exist_user = collection_users.find_one(
            {"username": tx_event["e_from"]}
        )

        if exist_user:
            self.update_balance_address(self.m_client, tx_event["e_from"], self.block_height)

            log.info("Tx Transfer {0} From: [{1}] To: [{2}] Amount: {3}".format(
                'RESERVE',
                tx_event["e_from"],
                tx_event["e_to"],
                tx_event["value"]))

        exist_user = collection_users.find_one(
            {"username": tx_event["e_to"]}
        )

        if exist_user:
            self.update_balance_address(self.m_client, tx_event["e_to"], self.block_height)

            log.info("Tx Transfer {0} From: [{1}] To: [{2}] Amount: {3}".format(
                'RESERVE',
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

        filter_address = self.contract_ReserveToken.address()

        tx_index = 0
        raw_logs = self.tx_receipt.logs

        # Reserve Transfer
        if self.app_mode == 'RRC20':
            # Transfer
            for tx_event in self.tx_receipt.events:

                if str(raw_logs[tx_index]['address']).lower() != str(filter_address).lower():
                    continue

                if self.name in tx_event:
                    d_event = ERC20Transfer(tx_event[self.name],
                                                     tx_receipt=self.tx_receipt)
                    self.index_event(d_event.event)

                tx_index += 1
