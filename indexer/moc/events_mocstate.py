import datetime
from collections import OrderedDict

from moneyonchain.moc import MoCStateStateTransition

from indexer.mongo_manager import mongo_manager
from indexer.logger import log
from .events import BaseIndexEvent


d_states = {
    0: "Liquidated",
    1: "BProDiscount",
    2: "BelowCobj",
    3: "AboveCobj"
}


class IndexStateTransition(BaseIndexEvent):

    name = 'StateTransition'

    def index_event(self, tx_event, log_index=None):

        return

    def notifications(self, tx_event, log_index=None):
        """Event: """

        collection_tx = mongo_manager.collection_notification(self.m_client)
        tx_hash = self.tx_receipt.txid
        event_name = 'StateTransition'

        d_tx = OrderedDict()
        d_tx["event"] = event_name
        d_tx["transactionHash"] = tx_hash
        d_tx["logIndex"] = log_index
        d_tx["newState"] = d_states[tx_event["newState"]]
        d_tx["timestamp"] = datetime.datetime.fromtimestamp(self.tx_receipt.timestamp)
        d_tx["processLogs"] = True

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash, "event": event_name, "logIndex": log_index},
            {"$set": d_tx},
            upsert=True)

        d_tx['post_id'] = post_id

        return d_tx

    def on_event(self, tx_event, log_index=None):
        """ Event """

        d_event = MoCStateStateTransition(tx_event, tx_receipt=self.tx_receipt)
        self.index_event(d_event.event)
        self.notifications(d_event.event, log_index=log_index)

