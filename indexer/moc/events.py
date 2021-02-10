from .balances import Balances


class BlockInfo(object):
    def __init__(self,
                 confirm_blocks=None,
                 block_height=None,
                 block_height_current=None,
                 transactions=None,
                 block_ts=None
                 ):
        """ Block info"""

        self.confirm_blocks = confirm_blocks
        self.block_height = block_height
        self.block_height_current = block_height_current
        self.transactions = transactions
        self.block_ts = block_ts


class BaseIndexEvent(BlockInfo, Balances):

    name = 'Name'
    precision = 10 ** 18
    app_mode = 'MoC'

    def __init__(self,
                 m_client=None,
                 parent=None,
                 contract_address=None,
                 tx_receipt=None,
                 **tx_vars):

        super().__init__(**tx_vars)
        self.m_client = m_client
        self.parent = parent
        self.contract_address = contract_address
        self.tx_receipt = tx_receipt

    def status_tx(self):

        if self.block_height_current - self.block_height > self.confirm_blocks:
            status = 'confirmed'
            confirmation_time = self.block_ts
        else:
            status = 'confirming'
            confirmation_time = None

        return status, confirmation_time

    def index_event(self, tx_event):
        """ This is the event """

    def on_event(self, tx_event):
        """ On event"""

    def on_events(self):
        """ Iterate on events  """

        if not self.tx_receipt.events:
            # return if there are no logs events decoded
            return

        if self.name in self.tx_receipt.events:
            for tx_event in self.tx_receipt.events[self.name]:
                self.on_event(tx_event)

    def index_from_receipt(self, tx_receipt, block_info=None):
        """ Index from receipt """

        self.tx_receipt = tx_receipt

        if block_info:
            self.m_client = block_info.m_client
            self.confirm_blocks = block_info.confirm_blocks
            self.block_height = block_info.block_height
            self.block_height_current = block_info.block_height_current
            self.transactions = block_info.transactions
            self.block_ts = block_info.block_ts

        self.on_events()
