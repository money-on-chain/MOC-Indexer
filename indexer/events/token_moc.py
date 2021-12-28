from moneyonchain.moc import ERC20Approval

from indexer.logger import log
from indexer.moc_balances import insert_update_balance_address
from .events import BaseIndexEvent


class IndexApprovalMoCToken(BaseIndexEvent):

    name = 'Approval'

    def __init__(self, options, app_mode, moc_contract):

        self.options = options
        self.app_mode = app_mode
        self.moc_contract = moc_contract

        super().__init__(options, app_mode)

    def index_event(self, m_client, parse_receipt, tx_event):

        user_address = tx_event["owner"]
        spender_address = tx_event["spender"]
        block_identifier = parse_receipt["blockNumber"]

        if str.lower(spender_address) not in [str.lower(self.moc_contract)]:
            # Approval is not from our contract
            return

        insert_update_balance_address(m_client, user_address)

    def on_event(self, m_client, parse_receipt):
        """ Event """

        cl_tx_event = ERC20Approval(parse_receipt)
        self.index_event(m_client, parse_receipt, cl_tx_event.event[self.name])
