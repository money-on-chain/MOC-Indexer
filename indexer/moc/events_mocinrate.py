import datetime
from collections import OrderedDict
from web3 import Web3

from moneyonchain.moc import MoCInrateDailyPay, \
    MoCInrateRiskProHoldersInterestPay

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


class IndexInrateDailyPay(BaseIndexEvent):

    name = 'InrateDailyPay'

    def index_event(self, tx_event):

        collection_tx = mongo_manager.collection_notification(self.m_client)
        tx_hash = self.tx_receipt.txid
        event_name = 'InrateDailyPay'
        log_index = self.tx_log['logIndex']

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

    def notifications(self, tx_event):
        """Event: SettlementStarted"""

        collection_tx = mongo_manager.collection_notification(self.m_client)
        tx_hash = self.tx_receipt.txid
        event_name = 'InrateDailyPay'
        log_index = self.tx_log['logIndex']

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

    def index_events(self):
        """ Index  """

        if not self.tx_receipt.events:
            # return if there are no logs events decoded
            return

        if not self.tx_receipt.logs:
            # return if there are no logs events in raw mode
            return

        settlement_address = self.contract_MoC.sc_moc_settlement.address()

        tx_index = 0
        raw_logs = self.tx_receipt.logs

        # SettlementStarted
        for tx_event in self.tx_receipt.events:

            if str(raw_logs[tx_index]['address']).lower() != str(settlement_address).lower():
                continue

            if self.name in tx_event:
                d_event = MoCInrateDailyPay(tx_event[self.name],
                                                 tx_receipt=self.tx_receipt)
                self.index_event(d_event.event)
                self.notifications(d_event.event)

            tx_index += 1


class IndexRiskProHoldersInterestPay(BaseIndexEvent):

    name = 'RiskProHoldersInterestPay'

    def index_event(self, tx_event):

        collection_inrate = mongo_manager.collection_bitpro_holders_interest(self.m_client)

        exist_tx = collection_inrate.find_one(
            {"blockHeight": tx_event["blockNumber"]}
        )
        if exist_tx:
            log.warning(
                "Event [RiskPro Holders Interest Pay] already exist for blockNumber: [{0}] Not Writting...".format(
                    tx_event["blockNumber"]))
            return

        d_tx = OrderedDict()
        d_tx["blockHeight"] = tx_event["blockNumber"]
        d_tx["amount"] = str(tx_event["amount"])
        d_tx["nBtcBucketC0BeforePay"] = str(tx_event["nReserveBucketC0BeforePay"])
        d_tx["timestamp"] = tx_event["timestamp"]
        d_tx["processLogs"] = True
        d_tx["createdAt"] = self.block_ts

        post_id = collection_inrate.find_one_and_update(
            {"blockHeight": tx_event["blockNumber"]},
            {"$set": d_tx},
            upsert=True)

        log.info("Event RiskPro Holders Interest Pay - Blockheight: [{0}] amount: {1}".format(
            d_tx["blockHeight"],
            d_tx["amount"],
        ))

        d_tx['post_id'] = post_id

        return d_tx

    def notifications(self, tx_event):
        """Event: """

        collection_tx = mongo_manager.collection_notification(self.m_client)
        tx_hash = self.tx_receipt.txid
        event_name = 'RiskProHoldersInterestPay'
        log_index = self.tx_log['logIndex']

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

    def index_events(self):
        """ Index  """

        if not self.tx_receipt.events:
            # return if there are no logs events decoded
            return

        if not self.tx_receipt.logs:
            # return if there are no logs events in raw mode
            return

        filter_address = self.contract_MoC.sc_moc_inrate.address()

        tx_index = 0
        raw_logs = self.tx_receipt.logs

        # RiskProHoldersInterestPay
        for tx_event in self.tx_receipt.events:

            if str(raw_logs[tx_index]['address']).lower() != str(filter_address).lower():
                continue

            if self.name in tx_event:
                d_event = MoCInrateRiskProHoldersInterestPay(tx_event[self.name],
                                                 tx_receipt=self.tx_receipt)
                self.index_event(d_event.event)
                self.notifications(d_event.event)

            tx_index += 1
