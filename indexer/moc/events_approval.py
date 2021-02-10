from moneyonchain.moc import ERC20Approval

from .base import BaseIndexEvent

import logging
import logging.config


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger('default')


class IndexApproval(BaseIndexEvent):

    name = 'Approval'

    def index_event(self, tx_event):

        user_address = tx_event["owner"]
        contract_address = tx_event["spender"]
        block_identifier = tx_event["blockNumber"]

        if str.lower(contract_address) not in [str.lower(self.contract_MoC.address())]:
            # Approval is not from our contract
            return

        self.update_user_state_reserve(
            user_address,
            self.m_client,
            block_identifier=block_identifier)

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

        # Approval
        for tx_event in self.tx_receipt.events:

            if str(raw_logs[tx_index]['address']).lower() != str(filter_address).lower():
                continue

            if self.name in tx_event:
                d_event = ERC20Approval(tx_event[self.name],
                                        tx_receipt=self.tx_receipt)
                self.index_event(d_event.event)

            tx_index += 1
