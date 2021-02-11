from moneyonchain.moc import ERC20Transfer

from indexer.mongo_manager import mongo_manager
from indexer.logger import log
from .events import BaseIndexEvent


class IndexRESERVETransfer(BaseIndexEvent):

    name = 'Transfer'

    def index_event(self, tx_event, log_index=None):

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

        collection_users = mongo_manager.collection_users(self.m_client)

        exist_user = collection_users.find_one(
            {"username": tx_event["from"]}
        )

        if exist_user:
            self.parent.update_balance_address(self.m_client, tx_event["from"], self.block_height)

            log.info("Tx Transfer {0} From: [{1}] To: [{2}] Amount: {3}".format(
                'RESERVE',
                tx_event["from"],
                tx_event["to"],
                tx_event["value"]))

        exist_user = collection_users.find_one(
            {"username": tx_event["to"]}
        )

        if exist_user:
            self.parent.update_balance_address(self.m_client, tx_event["to"], self.block_height)

            log.info("Tx Transfer {0} From: [{1}] To: [{2}] Amount: {3}".format(
                'RESERVE',
                tx_event["from"],
                tx_event["to"],
                tx_event["value"]))

    def on_event(self, tx_event, log_index=None):
        """ Event """

        d_event = ERC20Transfer(tx_event, tx_receipt=self.tx_receipt)
        self.index_event(d_event.event, log_index=log_index)
