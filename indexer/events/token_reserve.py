from moneyonchain.moc import ERC20Transfer
from moneyonchain.moc import ERC20Approval

from indexer.mongo_manager import mongo_manager
from indexer.logger import log
from .events import BaseIndexEvent


class IndexRESERVETransfer(BaseIndexEvent):

    name = 'Transfer'

    def index_event(self, m_client, parse_receipt, tx_event):

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

        collection_users = mongo_manager.collection_users(m_client)

        exist_user = collection_users.find_one(
            {"username": tx_event["from"]}
        )

        if exist_user:
            self.parent.update_balance_address(m_client, tx_event["from"], parse_receipt['chain']['last_block'])

            log.info("Tx Transfer {0} From: [{1}] To: [{2}] Amount: {3}".format(
                'RESERVE',
                tx_event["from"],
                tx_event["to"],
                tx_event["value"]))

        exist_user = collection_users.find_one(
            {"username": tx_event["to"]}
        )

        if exist_user:
            self.parent.update_balance_address(m_client, tx_event["to"], parse_receipt['chain']['last_block'])

            log.info("Tx Transfer {0} From: [{1}] To: [{2}] Amount: {3}".format(
                'RESERVE',
                tx_event["from"],
                tx_event["to"],
                tx_event["value"]))

    def on_event(self, m_client, parse_receipt):
        """ Event """

        cl_tx_event = ERC20Transfer(parse_receipt)
        self.index_event(m_client, parse_receipt, cl_tx_event.event[self.name])


class IndexApproval(BaseIndexEvent):

    name = 'Approval'

    def index_event(self, m_client, parse_receipt, tx_event):

        user_address = tx_event["owner"]
        spender_address = tx_event["spender"]
        block_identifier = parse_receipt["blockNumber"]

        if str.lower(spender_address) not in [str.lower(self.moc_address)]:
            # Approval is not from our contract
            return

        self.parent.update_user_state_reserve(
            user_address,
            m_client,
            block_identifier=block_identifier)

    def on_event(self, m_client, parse_receipt):
        """ Event """

        cl_tx_event = ERC20Approval(parse_receipt)
        self.index_event(m_client, parse_receipt, cl_tx_event.event[self.name])
