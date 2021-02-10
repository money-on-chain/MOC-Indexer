import datetime
from collections import OrderedDict
from web3 import Web3

from moneyonchain.moc import MoCStateStateTransition

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


class IndexStateTransition(BaseIndexEvent):

    name = 'StateTransition'

    def index_event(self, tx_event):

        return

    def notifications(self, tx_event):
        """Event: """

        collection_tx = mongo_manager.collection_notification(self.m_client)
        tx_hash = self.tx_receipt.txid
        event_name = 'StateTransition'
        log_index = self.tx_log['logIndex']

        d_tx = OrderedDict()
        d_tx["event"] = event_name
        d_tx["transactionHash"] = tx_hash
        d_tx["logIndex"] = log_index
        d_tx["newState"] = d_states[tx_event["newState"]]
        d_tx["timestamp"] = tx_event["timestamp"]
        d_tx["processLogs"] = True

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash, "event": event_name, "logIndex": log_index},
            {"$set": d_tx},
            upsert=True)

        d_tx['post_id'] = post_id

        return d_tx

    def index_events(self):
        """ Index  """

        if not self.tx_receipt.events:
            # return if there are no logs events decoded
            return

        if not self.tx_receipt.logs:
            # return if there are no logs events in raw mode
            return

        filter_address = self.contract_MoC.contract_MoCInrate.address()

        tx_index = 0
        raw_logs = self.tx_receipt.logs

        # SettlementStarted
        for tx_event in self.tx_receipt.events:

            if str(raw_logs[tx_index]['address']).lower() != str(filter_address).lower():
                continue

            if self.name in tx_event:
                d_event = MoCStateStateTransition(tx_event[self.name],
                                                 tx_receipt=self.tx_receipt)
                self.index_event(d_event.event)
                self.notifications(d_event.event)

            tx_index += 1

