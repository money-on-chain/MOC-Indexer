from moneyonchain.moc import ERC20Approval

from indexer.logger import log
from .events import BaseIndexEvent


class IndexApprovalMoCToken(BaseIndexEvent):

    name = 'Approval'

    def index_event(self, m_client, parse_receipt, tx_event):

        user_address = tx_event["owner"]
        spender_address = tx_event["spender"]
        block_identifier = parse_receipt["blockNumber"]

        if str.lower(spender_address) not in [str.lower(self.moc_address)]:
            # Approval is not from our contract
            return

        self.parent.update_user_state_moc_allowance(
            user_address,
            m_client,
            block_identifier=block_identifier)

    def on_event(self, m_client, parse_receipt):
        """ Event """

        cl_tx_event = ERC20Approval(parse_receipt)
        self.index_event(m_client, parse_receipt, cl_tx_event.event[self.name])
