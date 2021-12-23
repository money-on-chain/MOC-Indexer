import datetime
from collections import OrderedDict
from web3 import Web3

from moneyonchain.moc import MoCSettlementRedeemRequestAlter, \
    MoCSettlementSettlementRedeemStableToken, \
    MoCSettlementSettlementDeleveraging, \
    MoCSettlementSettlementStarted, \
    MoCSettlementRedeemRequestProcessed, \
    MoCSettlementSettlementCompleted

from indexer.mongo_manager import mongo_manager
from indexer.logger import log
from .events import BaseIndexEvent


class IndexSettlementStarted(BaseIndexEvent):

    name = 'SettlementStarted'

    def index_event(self, tx_event, log_index=None):

        return

    def update_settlement_state(self, tx_event):
        """Event: SettlementStarted"""

        # SettlementState
        collection_tx = mongo_manager.collection_settlement_state(self.m_client)

        exist_tx = collection_tx.find_one(
            {"startBlockNumber": self.tx_receipt.block_number}
        )

        d_tx = dict()
        d_tx["inProcess"] = True
        d_tx["startBlockNumber"] = self.tx_receipt.block_number
        d_tx["docRedeemCount"] = tx_event["stableTokenRedeemCount"]
        d_tx["deleveragingCount"] = tx_event["deleveragingCount"]
        #adjust_price = Web3.fromWei(tx_event.riskProxPrice, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        #d_tx["btcxPrice"] = str(int(adjust_price * self.precision))
        d_tx["btcxPrice"] = str(tx_event["riskProxPrice"])
        d_tx["btcPrice"] = str(tx_event["reservePrice"])
        d_tx["createdAt"] = self.block_ts
        d_tx["processLogs"] = True

        if not exist_tx:
            post_id = collection_tx.insert_one(d_tx).inserted_id
            d_tx['post_id'] = post_id
        else:
            log.warning("SettlementState already exist!")
            d_tx['post_id'] = None

        log.info("Tx {0} startBlockNumber: [{1}] docRedeemCount: {2} deleveragingCount: {3}".format(
            'SettlementStarted',
            d_tx["startBlockNumber"],
            d_tx["docRedeemCount"],
            d_tx["deleveragingCount"]))

        return d_tx

    def on_event(self, tx_event, log_index=None):
        """ Event """

        d_event = MoCSettlementSettlementStarted(tx_event, tx_receipt=self.tx_receipt)
        self.index_event(d_event.event, log_index=log_index)
        self.update_settlement_state(d_event.event)


class IndexRedeemRequestAlter(BaseIndexEvent):
    name = 'RedeemRequestAlter'

    def index_event(self, tx_event, log_index=None):

        # status of tx
        status, confirmation_time = self.status_tx()

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(self.m_client)

        tx_hash = self.tx_receipt.txid
        moc_tx = self.transactions[tx_hash]

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = self.tx_receipt.block_number
        d_tx["address"] = tx_event["redeemer"]
        d_tx["status"] = status
        d_tx["event"] = 'RedeemRequestAlter'
        d_tx["tokenInvolved"] = 'STABLE'
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["amount"] = str(tx_event["delta"])
        d_tx["confirmationTime"] = confirmation_time
        gas_fee = self.tx_receipt.gas_used * Web3.fromWei(self.tx_receipt.gas_price, 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["processLogs"] = True

        is_addition = tx_event["isAddition"]
        if isinstance(is_addition, str):
            if is_addition == 'True':
                is_addition = True
            else:
                is_addition = False

        d_tx["isPositive"] = is_addition
        d_tx["createdAt"] = self.block_ts

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "address": d_tx["address"],
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} From: [{1}] Amount: {2} Tx Hash: {3}".format(
            d_tx["event"],
            d_tx["address"],
            d_tx["amount"],
            tx_hash))

        # update user balances
        info_balance = self.parent.update_balance_address(self.m_client, d_tx["address"],
                                                          self.block_height)

        # QUEUE DOC
        # Is the operation of sending or cancel doc to queue is
        # always the absolute value
        # first we need to delete previous queue doc
        # collection_tx.remove({'address': tx_event.redeemer, 'event': 'QueueDOC'})
        #
        # d_tx = OrderedDict()
        # d_tx["transactionHash"] = tx_hash
        # d_tx["blockNumber"] = tx_event.blockNumber
        # d_tx["address"] = tx_event.redeemer
        # d_tx["status"] = status
        # d_tx["event"] = 'QueueDOC'
        # d_tx["tokenInvolved"] = 'STABLE'
        # d_tx["lastUpdatedAt"] = datetime.datetime.now()
        # d_tx["amount"] = str(info_balance['docToRedeem'])
        # d_tx["confirmationTime"] = confirmation_time
        # d_tx["isPositive"] = False
        # gas_fee = tx_receipt['gasUsed'] * Web3.fromWei(moc_tx['gasPrice'], 'ether')
        # d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        # d_tx["processLogs"] = True
        # d_tx["createdAt"] = datetime.datetime.now()
        #
        # post_id = collection_tx.find_one_and_update(
        #     {"transactionHash": tx_hash,
        #      "address": d_tx["address"],
        #      "event": d_tx["event"]},
        #     {"$set": d_tx},
        #     upsert=True)
        # d_tx['post_id'] = post_id
        #
        # log.info("Tx {0} From: [{1}] Amount: {2} Tx Hash: {3}".format(
        #     d_tx["event"],
        #     d_tx["address"],
        #     d_tx["amount"],
        #     tx_hash))

        return d_tx

    def on_event(self, tx_event, log_index=None):
        """ Event """

        d_event = MoCSettlementRedeemRequestAlter(tx_event, tx_receipt=self.tx_receipt)
        self.index_event(d_event.event, log_index=log_index)


class IndexRedeemRequestProcessed(BaseIndexEvent):
    name = 'RedeemRequestProcessed'

    def index_event(self, tx_event, log_index=None):
        # status of tx
        status, confirmation_time = self.status_tx()

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(self.m_client)

        tx_hash = self.tx_receipt.txid
        moc_tx = self.transactions[tx_hash]

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = self.tx_receipt.block_number
        d_tx["address"] = tx_event["redeemer"]
        d_tx["status"] = status
        d_tx["event"] = 'RedeemRequestProcessed'
        d_tx["tokenInvolved"] = 'STABLE'
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["amount"] = str(tx_event["amount"])
        d_tx["confirmationTime"] = confirmation_time
        d_tx["isPositive"] = False
        gas_fee = self.tx_receipt.gas_used * Web3.fromWei(self.tx_receipt.gas_price, 'ether')
        # d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["processLogs"] = True
        d_tx["createdAt"] = self.block_ts

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "address": d_tx["address"],
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} From: [{1}] Amount: {2} Tx Hash: {3}".format(
            d_tx["event"],
            d_tx["address"],
            d_tx["amount"],
            tx_hash))

        # update user balances
        self.parent.update_balance_address(self.m_client, d_tx["address"], self.block_height)

        return d_tx

    def on_event(self, tx_event, log_index=None):
        """ Event """

        d_event = MoCSettlementRedeemRequestProcessed(tx_event, tx_receipt=self.tx_receipt)
        self.index_event(d_event.event, log_index=log_index)


class IndexSettlementRedeemStableToken(BaseIndexEvent):
    name = 'SettlementRedeemStableToken'

    def index_event(self, tx_event, log_index=None):

        return

    def notifications(self, tx_event, log_index=None):

        # Notifications
        collection_tx = mongo_manager.collection_notification(self.m_client)
        tx_hash = self.tx_receipt.txid
        event_name = 'SettlementRedeemStableToken'

        d_tx = dict()
        d_tx["event"] = event_name
        d_tx["transactionHash"] = tx_hash
        d_tx["logIndex"] = log_index
        d_tx["queueSize"] = str(tx_event["queueSize"])
        d_tx["accumCommissions"] = str(tx_event["accumCommissions"])
        d_tx["reservePrice"] = str(tx_event["reservePrice"])
        d_tx["timestamp"] = datetime.datetime.fromtimestamp(self.tx_receipt.timestamp)
        d_tx["processLogs"] = True

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash, "event": event_name, "logIndex": log_index},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} queueSize: [{1}] accumCommissions: {2} reservePrice: {3} Tx Hash: {4}".format(
            d_tx["event"],
            d_tx["queueSize"],
            d_tx["accumCommissions"],
            d_tx["reservePrice"],
            tx_hash))

        return d_tx

    def on_event(self, tx_event, log_index=None):
        """ Event """

        d_event = MoCSettlementSettlementRedeemStableToken(tx_event, tx_receipt=self.tx_receipt)
        self.index_event(d_event.event, log_index=log_index)
        self.notifications(d_event.event, log_index=log_index)


class IndexSettlementDeleveraging(BaseIndexEvent):
    name = 'SettlementDeleveraging'

    def index_event(self, tx_event, log_index=None):

        # status of tx
        status, confirmation_time = self.status_tx()

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(self.m_client)

        tx_hash = self.tx_receipt.txid
        moc_tx = self.transactions[tx_hash]

        # get all address who has bprox , at the time all users because
        # we dont know who hast bprox in certain block
        collection_users = mongo_manager.collection_user_state(self.m_client)
        users = collection_users.find()
        l_users_riskprox = list()
        for user in users:
            l_users_riskprox.append(user)
            # if float(user['bprox2Balance']) > 0.0:
            #    l_users_riskprox.append(user)

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = self.tx_receipt.block_number
        d_tx["event"] = 'SettlementDeleveraging'
        d_tx["tokenInvolved"] = 'RISKPROX'
        d_tx["status"] = status
        d_tx["settlement_status"] = 0
        d_tx["confirmationTime"] = confirmation_time
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = self.tx_receipt.gas_used * Web3.fromWei(self.tx_receipt.gas_price, 'ether')
        # d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        # if self.app_mode != "RRC20":
        #    d_tx["gasFeeUSD"] = str(int(gas_fee * Web3.fromWei(tx_event.reservePrice, 'ether') * self.precision))
        d_tx["processLogs"] = True
        d_tx["createdAt"] = self.block_ts

        riskprox_price = Web3.fromWei(tx_event["riskProxPrice"], 'ether')
        reserve_price = Web3.fromWei(tx_event["reservePrice"], 'ether')

        start_block_number = tx_event["startBlockNumber"]
        prior_block_to_deleveraging = start_block_number - 1
        l_transactions = list()

        for user_riskprox in l_users_riskprox:
            try:
                d_user_balances = self.parent.riskprox_balances_from_address(
                    user_riskprox["address"],
                    prior_block_to_deleveraging)
            except:
                continue

            if float(d_user_balances["bprox2Balance"]) > 0.0:
                d_tx["address"] = user_riskprox["address"]
                d_tx["amount"] = str(d_user_balances["bprox2Balance"])
                d_tx["USDAmount"] = str(riskprox_price * reserve_price * int(
                    d_user_balances["bprox2Balance"]))
                rbtc_amount = riskprox_price * int(
                    d_user_balances["bprox2Balance"])
                d_tx["RBTCAmount"] = str(rbtc_amount)
                d_tx["reservePrice_deleveraging"] = str(reserve_price)
                rbtc_total = rbtc_amount - int(gas_fee * self.precision)
                d_tx["RBTCTotal"] = str(rbtc_total)
                rbtc_total_ether = Web3.fromWei(abs(rbtc_total), 'ether')
                if rbtc_total < 0:
                    rbtc_total_ether = -rbtc_total_ether
                usd_total = rbtc_total_ether * reserve_price
                d_tx["USDTotal"] = str(int(usd_total * self.precision))

                post_id = collection_tx.find_one_and_update(
                    {"transactionHash": tx_hash,
                     "address": d_tx["address"],
                     "event": d_tx["event"]},
                    {"$set": d_tx},
                    upsert=True)

                log.info("Tx {0} From: [{1}] Amount: {2} Tx Hash: {3}".format(
                    d_tx["event"],
                    d_tx["address"],
                    d_tx["amount"],
                    tx_hash))

                # update user balances
                self.parent.update_balance_address(self.m_client, d_tx["address"],
                                                   self.block_height)

                l_transactions.append(d_tx)

        return l_transactions

    def set_settlement_state(self, tx_event):
        """Event: SettlementDeleveraging"""

        # SettlementState
        collection_tx = mongo_manager.collection_settlement_state(self.m_client)

        d_tx = dict()
        d_tx["inProcess"] = False
        d_tx["startBlockNumber"] = self.tx_receipt.block_number
        d_tx["processLogs"] = True

        d_tx_insert = OrderedDict()
        d_tx_insert["docRedeemCount"] = 0
        d_tx_insert["deleveragingCount"] = 0
        d_tx_insert["btcxPrice"] = str(tx_event["riskProxPrice"])
        d_tx_insert["btcPrice"] = str(tx_event["reservePrice"])
        d_tx_insert["createdAt"] = self.block_ts

        post_id = collection_tx.find_one_and_update(
            {"startBlockNumber": self.tx_receipt.block_number},
            {"$set": d_tx,
             "$setOnInsert": d_tx_insert},
            upsert=True)

        d_tx['post_id'] = post_id

        log.info("Tx {0} blockNumber: [{1}] Tx Hash:".format(
            'SettlementDeleveraging',
            d_tx["startBlockNumber"]))

        return d_tx

    def on_event(self, tx_event, log_index=None):
        """ Event """

        d_event = MoCSettlementSettlementDeleveraging(tx_event, tx_receipt=self.tx_receipt)
        self.index_event(d_event.event, log_index=log_index)
        self.set_settlement_state(d_event.event)


class IndexSettlementCompleted(BaseIndexEvent):
    name = 'SettlementCompleted'

    def index_event(self, tx_event, log_index=None):

        return

    def moc_settlement_completed(self, tx_event):
        """Event: SettlementDeleveraging"""

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(self.m_client)

        # remove all RedeemRequestAlter
        collection_tx.delete_many({"event": "RedeemRequestAlter",
                              "blockHeight": {"$lte": self.block_height}})

        # also delete with created at < 31 days
        old_records = self.block_ts - datetime.timedelta(days=31)
        collection_tx.delete_many({"event": "RedeemRequestAlter",
                              "createdAt": {"$lte": old_records}})

    def on_event(self, tx_event, log_index=None):
        """ Event """

        d_event = MoCSettlementSettlementCompleted(tx_event, tx_receipt=self.tx_receipt)
        self.index_event(d_event.event, log_index=log_index)
        self.moc_settlement_completed(d_event.event)
