from collections import OrderedDict
import datetime

from moneyonchain.moc import MoCInrateDailyPay, \
    MoCInrateRiskProHoldersInterestPay

from indexer.mongo_manager import mongo_manager
from indexer.logger import log
from .events import BaseIndexEvent


class IndexInrateDailyPay(BaseIndexEvent):

    name = 'InrateDailyPay'

    def index_event(self, m_client, parse_receipt, tx_event):

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(m_client)

        tx_hash = parse_receipt["transactionHash"]

        event_name = 'InrateDailyPay'

        d_tx = dict()
        d_tx["event"] = event_name
        d_tx["transactionHash"] = tx_hash
        d_tx["logIndex"] = parse_receipt["log_index"]
        d_tx["amount"] = str(tx_event["amount"])
        d_tx["daysToSettlement"] = str(tx_event["daysToSettlement"])
        d_tx["timestamp"] = parse_receipt["timestamp"]
        d_tx["processLogs"] = True

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash, "event": event_name, "logIndex": parse_receipt["log_index"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        return d_tx

    def notifications(self, m_client, parse_receipt, tx_event):
        """Event: SettlementStarted"""

        collection_tx = mongo_manager.collection_notification(m_client)
        tx_hash = parse_receipt["transactionHash"]
        event_name = 'InrateDailyPay'

        d_tx = dict()
        d_tx["event"] = event_name
        d_tx["transactionHash"] = tx_hash
        d_tx["logIndex"] = parse_receipt["log_index"]
        d_tx["amount"] = str(tx_event["amount"])
        d_tx["daysToSettlement"] = str(tx_event["daysToSettlement"])
        d_tx["timestamp"] = parse_receipt["timestamp"]
        d_tx["processLogs"] = True

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash, "event": event_name, "logIndex": parse_receipt["log_index"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        return d_tx

    def on_event(self, m_client, parse_receipt, log_index=None):
        """ Event """

        cl_tx_event = MoCInrateDailyPay(parse_receipt)
        self.index_event(m_client, parse_receipt, cl_tx_event.event[self.name])
        self.notifications(m_client, parse_receipt, cl_tx_event.event[self.name])


class IndexRiskProHoldersInterestPay(BaseIndexEvent):

    name = 'RiskProHoldersInterestPay'

    def index_event(self, m_client, parse_receipt, tx_event):

        collection_inrate = mongo_manager.collection_bitpro_holders_interest(m_client)

        exist_tx = collection_inrate.find_one(
            {"blockHeight": parse_receipt["blockNumber"]}
        )
        if exist_tx:
            log.warning(
                "Event [RiskPro Holders Interest Pay] already exist for blockNumber: [{0}] Not Writting...".format(
                    parse_receipt["blockNumber"]))
            return

        d_tx = OrderedDict()
        d_tx["blockHeight"] = parse_receipt["blockNumber"]
        d_tx["amount"] = str(tx_event["amount"])
        d_tx["nBtcBucketC0BeforePay"] = str(tx_event["nReserveBucketC0BeforePay"])
        d_tx["timestamp"] = parse_receipt["timestamp"]
        d_tx["processLogs"] = True
        d_tx["createdAt"] = parse_receipt['chain']['block_ts']

        post_id = collection_inrate.find_one_and_update(
            {"blockHeight": parse_receipt["blockNumber"]},
            {"$set": d_tx},
            upsert=True)

        log.info("Event RiskPro Holders Interest Pay - Blockheight: [{0}] amount: {1}".format(
            d_tx["blockHeight"],
            d_tx["amount"],
        ))

        d_tx['post_id'] = post_id

        return d_tx

    def notifications(self,  m_client, parse_receipt, tx_event):
        """Event: """

        collection_tx = mongo_manager.collection_notification(m_client)
        tx_hash = parse_receipt["transactionHash"]
        event_name = 'RiskProHoldersInterestPay'

        d_tx = OrderedDict()
        d_tx["event"] = event_name
        d_tx["transactionHash"] = tx_hash
        d_tx["logIndex"] = parse_receipt["log_index"]
        d_tx["amount"] = str(tx_event["amount"])
        d_tx["nBtcBucketC0BeforePay"] = str(tx_event["nReserveBucketC0BeforePay"])
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

        cl_tx_event = MoCInrateRiskProHoldersInterestPay(parse_receipt)
        self.index_event(m_client, parse_receipt, cl_tx_event.event[self.name])
        self.notifications(m_client, parse_receipt, cl_tx_event.event[self.name])

