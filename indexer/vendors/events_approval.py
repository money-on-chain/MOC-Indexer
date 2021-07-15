from moneyonchain.moc import ERC20Approval

from indexer.logger import log
from indexer.moc.events import BaseIndexEvent


class IndexApprovalMoCToken(BaseIndexEvent):

    name = 'Approval'

    def index_event(self, tx_event, log_index=None):

        user_address = tx_event["owner"]
        spender_address = tx_event["spender"]
        block_identifier = self.tx_receipt.block_number

        if str.lower(spender_address) not in [str.lower(self.moc_address)]:
            # Approval is not from our contract
            return

        self.parent.update_user_state_moc_allowance(
            user_address,
            self.m_client,
            block_identifier=block_identifier)

    def on_event(self, tx_event, log_index=None):
        """ Event """

        d_event = ERC20Approval(tx_event, tx_receipt=self.tx_receipt)
        self.index_event(d_event.event, log_index=log_index)
