from moneyonchain.fastbtc import NewBitcoinTransfer, \
    BitcoinTransferStatusUpdated

from indexer.mongo_manager import mongo_manager
from indexer.logger import log
from .events import BaseIndexEvent


class IndexNewBitcoinTransfer(BaseIndexEvent):

    name = 'NewBitcoinTransfer'

    def index_event(self, m_client, parse_receipt, tx_event):

        # get collection fast btc bridge
        collection_bridge = mongo_manager.collection_fast_btc_bridge(m_client)

        tx_hash = parse_receipt["transactionHash"]

        d_tx = dict()
        d_tx["transactionHash"] = tx_hash
        d_tx["transactionHashLastUpdated"] = tx_hash
        d_tx["blockNumber"] = parse_receipt["blockNumber"]
        d_tx["type"] = 'PEG_OUT'
        d_tx["transferId"] = str(tx_event["transferId"])
        d_tx["btcAddress"] = tx_event["btcAddress"]
        d_tx["nonce"] = tx_event["nonce"]
        d_tx["amountSatoshi"] = str(tx_event["amountSatoshi"])
        d_tx["feeSatoshi"] = str(tx_event["feeSatoshi"])
        d_tx["rskAddress"] = tx_event["rskAddress"]
        d_tx["status"] = 0
        d_tx["timestamp"] = parse_receipt["timestamp"]
        d_tx["updated"] = parse_receipt["timestamp"]
        d_tx["processLogs"] = True

        post_id = collection_bridge.find_one_and_update(
            {"transferId": d_tx["transferId"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("EVENT::NewBitcoinTransfer::{0}".format(d_tx["transferId"]))
        log.info(d_tx)

        return d_tx

    def on_event(self, m_client, parse_receipt, log_index=None):
        """ Event """

        cl_tx_event = NewBitcoinTransfer(parse_receipt)
        self.index_event(m_client, parse_receipt, cl_tx_event.event[self.name])


class IndexBitcoinTransferStatusUpdated(BaseIndexEvent):

    name = 'BitcoinTransferStatusUpdated'

    def index_event(self, m_client, parse_receipt, tx_event):

        # get collection fast btc bridge
        collection_bridge = mongo_manager.collection_fast_btc_bridge(m_client)

        d_tx = dict()
        d_tx["transactionHashLastUpdated"] = parse_receipt["transactionHash"]
        d_tx["status"] = tx_event["newStatus"]
        d_tx["updated"] = parse_receipt["timestamp"]

        post_id = collection_bridge.find_one_and_update(
            {"transferId": d_tx["transferId"]},
            {"$set": d_tx},
            upsert=False)
        d_tx['post_id'] = post_id

        log.info("EVENT::BitcoinTransferStatusUpdated::{0}".format(d_tx["transferId"]))
        log.info(d_tx)

        return d_tx

    def on_event(self, m_client, parse_receipt, log_index=None):
        """ Event """

        cl_tx_event = BitcoinTransferStatusUpdated(parse_receipt)
        self.index_event(m_client, parse_receipt, cl_tx_event.event[self.name])
