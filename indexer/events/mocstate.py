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

    def index_event(self, m_client, parse_receipt, tx_event):
        return

    def notifications(self, m_client, parse_receipt, tx_event):
        """Event: """

        collection_tx = mongo_manager.collection_notification(m_client)
        tx_hash = parse_receipt["transactionHash"]
        event_name = 'StateTransition'

        d_tx = OrderedDict()
        d_tx["event"] = event_name
        d_tx["transactionHash"] = tx_hash
        d_tx["logIndex"] = parse_receipt["log_index"]
        d_tx["newState"] = d_states[tx_event["newState"]]
        d_tx["timestamp"] = parse_receipt["timestamp"]
        d_tx["processLogs"] = True

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash, "event": event_name, "logIndex": parse_receipt["log_index"]},
            {"$set": d_tx},
            upsert=True)

        d_tx['post_id'] = post_id

        return d_tx

    def on_event(self, m_client, parse_receipt):
        """ Event """

        cl_tx_event = MoCStateStateTransition(parse_receipt)
        self.index_event(m_client, parse_receipt, cl_tx_event.event[self.name])
        self.notifications(m_client, parse_receipt, cl_tx_event.event[self.name])

