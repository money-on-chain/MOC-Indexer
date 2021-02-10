from collections import OrderedDict

from moneyonchain.moc import MoCInrateDailyPay, \
    MoCInrateRiskProHoldersInterestPay

from indexer.mongo_manager import mongo_manager
from .events import BaseIndexEvent

import logging
import logging.config


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger('default')


class IndexInrateDailyPay(BaseIndexEvent):

    name = 'InrateDailyPay'

    def index_event(self, tx_event, log_index=None):

        collection_tx = mongo_manager.collection_notification(self.m_client)
        tx_hash = self.tx_receipt.txid
        event_name = 'InrateDailyPay'

        d_tx = dict()
        d_tx["event"] = event_name
        d_tx["transactionHash"] = tx_hash
        d_tx["logIndex"] = log_index
        d_tx["amount"] = str(tx_event["amount"])
        d_tx["daysToSettlement"] = str(tx_event["daysToSettlement"])
        d_tx["timestamp"] = tx_event["timestamp"]
        d_tx["processLogs"] = True

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash, "event": event_name, "logIndex": log_index},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        return d_tx

    def notifications(self, tx_event, log_index=None):
        """Event: SettlementStarted"""

        collection_tx = mongo_manager.collection_notification(self.m_client)
        tx_hash = self.tx_receipt.txid
        event_name = 'InrateDailyPay'

        d_tx = dict()
        d_tx["event"] = event_name
        d_tx["transactionHash"] = tx_hash
        d_tx["logIndex"] = log_index
        d_tx["amount"] = str(tx_event["amount"])
        d_tx["daysToSettlement"] = str(tx_event["daysToSettlement"])
        d_tx["timestamp"] = tx_event["timestamp"]
        d_tx["processLogs"] = True

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash, "event": event_name, "logIndex": log_index},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        return d_tx

    def on_event(self, tx_event, log_index=None):
        """ Event """

        d_event = MoCInrateDailyPay(tx_event, tx_receipt=self.tx_receipt)
        self.index_event(d_event.event, log_index=log_index)
        self.notifications(d_event.event, log_index=log_index)


class IndexRiskProHoldersInterestPay(BaseIndexEvent):

    name = 'RiskProHoldersInterestPay'

    def index_event(self, tx_event, log_index=None):

        collection_inrate = mongo_manager.collection_bitpro_holders_interest(self.m_client)

        exist_tx = collection_inrate.find_one(
            {"blockHeight": self.tx_receipt.block_number}
        )
        if exist_tx:
            log.warning(
                "Event [RiskPro Holders Interest Pay] already exist for blockNumber: [{0}] Not Writting...".format(
                    self.tx_receipt.block_number))
            return

        d_tx = OrderedDict()
        d_tx["blockHeight"] = self.tx_receipt.block_number
        d_tx["amount"] = str(tx_event["amount"])
        d_tx["nBtcBucketC0BeforePay"] = str(tx_event["nReserveBucketC0BeforePay"])
        d_tx["timestamp"] = tx_event["timestamp"]
        d_tx["processLogs"] = True
        d_tx["createdAt"] = self.block_ts

        post_id = collection_inrate.find_one_and_update(
            {"blockHeight": self.tx_receipt.block_number},
            {"$set": d_tx},
            upsert=True)

        log.info("Event RiskPro Holders Interest Pay - Blockheight: [{0}] amount: {1}".format(
            d_tx["blockHeight"],
            d_tx["amount"],
        ))

        d_tx['post_id'] = post_id

        return d_tx

    def notifications(self, tx_event, log_index=None):
        """Event: """

        collection_tx = mongo_manager.collection_notification(self.m_client)
        tx_hash = self.tx_receipt.txid
        event_name = 'RiskProHoldersInterestPay'

        d_tx = OrderedDict()
        d_tx["event"] = event_name
        d_tx["transactionHash"] = tx_hash
        d_tx["logIndex"] = log_index
        d_tx["amount"] = str(tx_event["amount"])
        d_tx["nBtcBucketC0BeforePay"] = str(tx_event["nReserveBucketC0BeforePay"])
        d_tx["timestamp"] = tx_event["timestamp"]
        d_tx["processLogs"] = True

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash, "event": event_name, "logIndex": log_index},
            {"$set": d_tx},
            upsert=True)

        d_tx['post_id'] = post_id

        return d_tx

    def on_event(self, tx_event, log_index=None):
        """ Event """

        d_event = MoCInrateRiskProHoldersInterestPay(tx_event, tx_receipt=self.tx_receipt)
        self.index_event(d_event.event, log_index=log_index)
        self.notifications(d_event.event, log_index=log_index)

