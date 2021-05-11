from brownie.network.event import _decode_logs
from indexer.logger import log
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
                 moc_address=None,
                 reserve_address=None,
                 tx_receipt=None,
                 **tx_vars):

        super().__init__(**tx_vars)
        self.m_client = m_client
        self.parent = parent
        self.contract_address = contract_address
        self.moc_address = moc_address
        self.reserve_address = reserve_address
        self.tx_receipt = tx_receipt

    def update_info(self, **tx_args):

        if 'm_client' in tx_args:
            self.m_client = tx_args['m_client']
        if 'parent' in tx_args:
            self.parent = tx_args['parent']
        if 'block_height' in tx_args:
            self.block_height = tx_args['block_height']
        if 'block_height_current' in tx_args:
            self.block_height_current = tx_args['block_height_current']
        if 'transactions' in tx_args:
            self.transactions = tx_args['transactions']
        if 'block_ts' in tx_args:
            self.block_ts = tx_args['block_ts']

    def status_tx(self):

        if self.block_height_current - self.block_height > self.confirm_blocks:
            status = 'confirmed'
            confirmation_time = self.block_ts
        else:
            status = 'confirming'
            confirmation_time = None

        return status, confirmation_time

    def index_event(self, tx_event, log_index=None):
        """ This is the event """

    def on_event(self, tx_event, log_index=None):
        """ On event"""

    def on_events(self):
        """ Iterate on events  """

        if not self.tx_receipt.logs:
            # no events so no logs
            return

        if not self.tx_receipt.events:
            # return if there are no logs events decoded
            return

        if self.name not in self.tx_receipt.events:
            # no events
            return

        if not self.contract_address:
            log.warning("No contract address set for the event: {0}".format(self.name))

        for raw_log in self.tx_receipt.logs:

            if self.contract_address and str.lower(raw_log['address']) != str.lower(self.contract_address):
                log.warning("[WARN] Event with the same name but different address: {0}".format(raw_log['address']))
                continue

            tx_event = _decode_logs([raw_log])

            if self.name not in tx_event:
                log.warning("[WARN] Event not correspond with this class: {0} skipping! This ocurrs when multiple events on the same contract!".format(self.name))
                continue

            self.on_event(tx_event[self.name], log_index=raw_log['logIndex'])

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
