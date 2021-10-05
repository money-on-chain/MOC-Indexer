import datetime
from collections import OrderedDict
from web3 import Web3

from moneyonchain.moc import MoCExchangeRiskProMint, \
    MoCExchangeStableTokenMint, \
    MoCExchangeRiskProxMint, \
    MoCExchangeRiskProRedeem, \
    MoCExchangeFreeStableTokenRedeem, \
    MoCExchangeRiskProxRedeem, \
    MoCExchangeStableTokenRedeem

from moneyonchain.rdoc import MoCExchangeRiskProMint, \
    MoCExchangeStableTokenMint, \
    MoCExchangeRiskProxMint, \
    MoCExchangeRiskProRedeem, \
    MoCExchangeFreeStableTokenRedeem, \
    MoCExchangeRiskProxRedeem, \
    MoCExchangeStableTokenRedeem


from indexer.mongo_manager import mongo_manager
from indexer.logger import log


from .events import BaseIndexEvent


class IndexRiskProMint(BaseIndexEvent):

    name = 'RiskProMint'

    def index_event(self, tx_event, log_index=None):

        # status of tx
        status, confirmation_time = self.status_tx()

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(self.m_client)

        tx_hash = self.tx_receipt.txid

        d_tx = OrderedDict()
        d_tx["address"] = tx_event["account"]
        d_tx["blockNumber"] = self.tx_receipt.block_number
        d_tx["event"] = 'RiskProMint'
        d_tx["transactionHash"] = tx_hash
        d_tx["RBTCAmount"] = str(tx_event["reserveTotal"])
        usd_amount = Web3.fromWei(tx_event["reserveTotal"],
                                  'ether') * Web3.fromWei(tx_event["reservePrice"],
                                                          'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))
        d_tx["amount"] = str(tx_event["amount"])
        d_tx["confirmationTime"] = confirmation_time
        d_tx["isPositive"] = True
        d_tx["lastUpdatedAt"] = datetime.datetime.now()

        if self.app_mode != "RRC20":
            rbtc_commission = tx_event["commission"] + tx_event["btcMarkup"]
        else:
            rbtc_commission = tx_event["commission"] + tx_event["reserveTokenMarkup"]

        moc_commission = tx_event["mocCommissionValue"] + tx_event["mocMarkup"]
        if rbtc_commission > 0:
            usd_commission = Web3.fromWei(rbtc_commission, 'ether') * Web3.fromWei(tx_event["reservePrice"], 'ether')
        else:
            usd_commission = Web3.fromWei(moc_commission, 'ether') * Web3.fromWei(tx_event["mocPrice"], 'ether')
        d_tx["rbtcCommission"] = str(rbtc_commission)

        d_tx["USDCommission"] = str(int(usd_commission * self.precision))
        d_tx["status"] = status
        d_tx["tokenInvolved"] = 'RISKPRO'
        d_tx["reservePrice"] = str(tx_event["reservePrice"])

        d_tx["mocCommissionValue"] = str(moc_commission)
        d_tx["mocPrice"] = str(tx_event["mocPrice"])

        gas_fee = self.tx_receipt.gas_used * Web3.fromWei(self.tx_receipt.gas_price, 'ether')
        #gas_fee = self.tx_receipt.gas_used * Web3.fromWei(moc_tx['gasPrice'],
        #                                               'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        if self.app_mode != "RRC20":
            d_tx["gasFeeUSD"] = str(int(
                gas_fee * Web3.fromWei(tx_event["reservePrice"],
                                       'ether') * self.precision))
        rbtc_total = tx_event["reserveTotal"] + tx_event["commission"] + int(
            gas_fee * self.precision)
        d_tx["RBTCTotal"] = str(rbtc_total)
        usd_total = Web3.fromWei(rbtc_total, 'ether') * Web3.fromWei(
            tx_event["reservePrice"], 'ether')
        d_tx["USDTotal"] = str(int(usd_total * self.precision))
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

        d_event = MoCExchangeRiskProMint(tx_event, tx_receipt=self.tx_receipt)
        self.index_event(d_event.event, log_index=log_index)


class IndexRiskProRedeem(BaseIndexEvent):

    name = 'RiskProRedeem'

    def index_event(self, tx_event, log_index=None):

        # status of tx
        status, confirmation_time = self.status_tx()

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(self.m_client)

        tx_hash = self.tx_receipt.txid

        d_tx = OrderedDict()
        d_tx["event"] = 'RiskProRedeem'
        d_tx["blockNumber"] = self.tx_receipt.block_number
        d_tx["transactionHash"] = tx_hash
        d_tx["address"] = tx_event["account"]
        d_tx["tokenInvolved"] = 'RISKPRO'
        d_tx["userAmount"] = str(Web3.fromWei(tx_event["amount"], 'ether'))
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["status"] = status
        d_tx["RBTCAmount"] = str(tx_event["reserveTotal"])
        usd_amount = Web3.fromWei(tx_event["reserveTotal"],
                                  'ether') * Web3.fromWei(tx_event["reservePrice"],
                                                          'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))
        d_tx["amount"] = str(tx_event["amount"])
        d_tx["confirmationTime"] = confirmation_time
        if self.app_mode != "RRC20":
            rbtc_commission = tx_event["commission"] + tx_event["btcMarkup"]
        else:
            rbtc_commission = tx_event["commission"] + tx_event["reserveTokenMarkup"]
        moc_commission = tx_event["mocCommissionValue"] + tx_event["mocMarkup"]
        if rbtc_commission > 0:
            usd_commission = Web3.fromWei(rbtc_commission, 'ether') * Web3.fromWei(tx_event["reservePrice"], 'ether')
        else:
            usd_commission = Web3.fromWei(moc_commission, 'ether') * Web3.fromWei(tx_event["mocPrice"], 'ether')
        d_tx["rbtcCommission"] = str(rbtc_commission)
        d_tx["USDCommission"] = str(int(usd_commission * self.precision))
        d_tx["isPositive"] = False
        d_tx["reservePrice"] = str(tx_event["reservePrice"])
        d_tx["mocCommissionValue"] = str(moc_commission)
        d_tx["mocPrice"] = str(tx_event["mocPrice"])
        gas_fee = self.tx_receipt.gas_used * Web3.fromWei(self.tx_receipt.gas_price, 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        if self.app_mode != "RRC20":
            d_tx["gasFeeUSD"] = str(int(
                gas_fee * Web3.fromWei(tx_event["reservePrice"],
                                       'ether') * self.precision))
        rbtc_total = tx_event["reserveTotal"] - int(gas_fee * self.precision)
        d_tx["RBTCTotal"] = str(rbtc_total)
        rbtc_total_ether = Web3.fromWei(abs(rbtc_total), 'ether')
        if rbtc_total < 0:
            rbtc_total_ether = -rbtc_total_ether
        usd_total = rbtc_total_ether * Web3.fromWei(tx_event["reservePrice"],
                                                    'ether')
        d_tx["USDTotal"] = str(int(usd_total * self.precision))
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

        d_event = MoCExchangeRiskProRedeem(tx_event, tx_receipt=self.tx_receipt)
        self.index_event(d_event.event, log_index=log_index)


class IndexRiskProxMint(BaseIndexEvent):

    name = 'RiskProxMint'

    def index_event(self, tx_event, log_index=None):

        # status of tx
        status, confirmation_time = self.status_tx()

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(self.m_client)

        tx_hash = self.tx_receipt.txid

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = self.tx_receipt.block_number
        d_tx["address"] = tx_event["account"]
        d_tx["status"] = status
        d_tx["event"] = 'RiskProxMint'
        d_tx["tokenInvolved"] = 'RISKPROX'
        d_tx["userAmount"] = str(Web3.fromWei(tx_event["amount"], 'ether'))
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["RBTCAmount"] = str(tx_event["reserveTotal"])
        usd_amount = Web3.fromWei(tx_event["reserveTotal"],
                                  'ether') * Web3.fromWei(tx_event["reservePrice"],
                                                          'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))
        d_tx["amount"] = str(tx_event["amount"])
        d_tx["confirmationTime"] = confirmation_time
        d_tx["isPositive"] = True
        d_tx["leverage"] = str(tx_event["leverage"])
        if self.app_mode != "RRC20":
            rbtc_commission = tx_event["commission"] + tx_event["btcMarkup"]
        else:
            rbtc_commission = tx_event["commission"] + tx_event["reserveTokenMarkup"]
        moc_commission = tx_event["mocCommissionValue"] + tx_event["mocMarkup"]
        if rbtc_commission > 0:
            usd_commission = Web3.fromWei(rbtc_commission, 'ether') * Web3.fromWei(tx_event["reservePrice"], 'ether')
        else:
            usd_commission = Web3.fromWei(moc_commission, 'ether') * Web3.fromWei(tx_event["mocPrice"], 'ether')
        d_tx["rbtcCommission"] = str(rbtc_commission)
        d_tx["USDCommission"] = str(int(usd_commission * self.precision))
        d_tx["rbtcInterests"] = str(tx_event["interests"])
        usd_interest = Web3.fromWei(tx_event["interests"], 'ether') * Web3.fromWei(
            tx_event["reservePrice"], 'ether')
        d_tx["USDInterests"] = str(int(usd_interest * self.precision))
        d_tx["reservePrice"] = str(tx_event["reservePrice"])
        d_tx["mocCommissionValue"] = str(moc_commission)
        d_tx["mocPrice"] = str(tx_event["mocPrice"])
        gas_fee = self.tx_receipt.gas_used * Web3.fromWei(self.tx_receipt.gas_price, 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        if self.app_mode != "RRC20":
            d_tx["gasFeeUSD"] = str(int(
                gas_fee * Web3.fromWei(tx_event["reservePrice"],
                                       'ether') * self.precision))
        rbtc_total = tx_event["reserveTotal"] + tx_event["commission"] + tx_event["interests"] + int(
            gas_fee * self.precision)
        d_tx["RBTCTotal"] = str(rbtc_total)
        usd_total = Web3.fromWei(rbtc_total, 'ether') * Web3.fromWei(
            tx_event["reservePrice"], 'ether')
        d_tx["USDTotal"] = str(int(usd_total * self.precision))
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

        d_event = MoCExchangeRiskProxMint(tx_event, tx_receipt=self.tx_receipt)
        self.index_event(d_event.event, log_index=log_index)


class IndexRiskProxRedeem(BaseIndexEvent):

    name = 'RiskProxRedeem'

    def index_event(self, tx_event, log_index=None):

        # status of tx
        status, confirmation_time = self.status_tx()

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(self.m_client)

        tx_hash = self.tx_receipt.txid

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = self.tx_receipt.block_number
        d_tx["address"] = tx_event["account"]
        d_tx["status"] = status
        d_tx["event"] = 'RiskProxRedeem'
        d_tx["tokenInvolved"] = 'RISKPROX'
        d_tx["userAmount"] = str(Web3.fromWei(tx_event["amount"], 'ether'))
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["RBTCAmount"] = str(tx_event["reserveTotal"])
        usd_amount = Web3.fromWei(tx_event["reserveTotal"],
                                  'ether') * Web3.fromWei(tx_event["reservePrice"],
                                                          'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))
        d_tx["amount"] = str(tx_event["amount"])
        d_tx["confirmationTime"] = confirmation_time
        d_tx["leverage"] = str(tx_event["leverage"])
        if self.app_mode != "RRC20":
            rbtc_commission = tx_event["commission"] + tx_event["btcMarkup"]
        else:
            rbtc_commission = tx_event["commission"] + tx_event["reserveTokenMarkup"]
        moc_commission = tx_event["mocCommissionValue"] + tx_event["mocMarkup"]
        if rbtc_commission > 0:
            usd_commission = Web3.fromWei(rbtc_commission, 'ether') * Web3.fromWei(tx_event["reservePrice"], 'ether')
        else:
            usd_commission = Web3.fromWei(moc_commission, 'ether') * Web3.fromWei(tx_event["mocPrice"], 'ether')
        d_tx["rbtcCommission"] = str(rbtc_commission)
        d_tx["USDCommission"] = str(int(usd_commission * self.precision))
        d_tx["rbtcInterests"] = str(tx_event["interests"])
        usd_interest = Web3.fromWei(tx_event["interests"], 'ether') * Web3.fromWei(
            tx_event["reservePrice"], 'ether')
        d_tx["USDInterests"] = str(int(usd_interest * self.precision))
        d_tx["isPositive"] = False
        d_tx["reservePrice"] = str(tx_event["reservePrice"])
        d_tx["mocCommissionValue"] = str(moc_commission)
        d_tx["mocPrice"] = str(tx_event["mocPrice"])
        gas_fee = self.tx_receipt.gas_used * Web3.fromWei(self.tx_receipt.gas_price, 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        if self.app_mode != "RRC20":
            d_tx["gasFeeUSD"] = str(int(
                gas_fee * Web3.fromWei(tx_event["reservePrice"],
                                       'ether') * self.precision))
        rbtc_total = tx_event["reserveTotal"] + tx_event["interests"] - int(
            gas_fee * self.precision)
        d_tx["RBTCTotal"] = str(rbtc_total)
        rbtc_total_ether = Web3.fromWei(abs(rbtc_total), 'ether')
        if rbtc_total < 0:
            rbtc_total_ether = -rbtc_total_ether
        usd_total = rbtc_total_ether * Web3.fromWei(tx_event["reservePrice"],
                                                    'ether')
        d_tx["USDTotal"] = str(int(usd_total * self.precision))
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

        d_event = MoCExchangeRiskProxRedeem(tx_event, tx_receipt=self.tx_receipt)
        self.index_event(d_event.event, log_index=log_index)


class IndexStableTokenMint(BaseIndexEvent):
    name = 'StableTokenMint'

    def index_event(self, tx_event, log_index=None):

        # status of tx
        status, confirmation_time = self.status_tx()

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(self.m_client)

        tx_hash = self.tx_receipt.txid

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = self.tx_receipt.block_number
        d_tx["address"] = tx_event["account"]
        d_tx["status"] = status
        d_tx["event"] = 'StableTokenMint'
        d_tx["tokenInvolved"] = 'STABLE'
        # WARNING something to investigate, commented think is correct
        # d_tx["userAmount"] = str(Web3.fromWei(tx_event.amount, 'ether'))
        d_tx["userAmount"] = str(Web3.fromWei(tx_event["reserveTotal"], 'ether'))
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["RBTCAmount"] = str(tx_event["reserveTotal"])
        usd_amount = Web3.fromWei(tx_event["reserveTotal"],
                                  'ether') * Web3.fromWei(tx_event["reservePrice"],
                                                          'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))
        if self.app_mode != "RRC20":
            rbtc_commission = tx_event["commission"] + tx_event["btcMarkup"]
        else:
            rbtc_commission = tx_event["commission"] + tx_event["reserveTokenMarkup"]
        moc_commission = tx_event["mocCommissionValue"] + tx_event["mocMarkup"]
        if rbtc_commission > 0:
            usd_commission = Web3.fromWei(rbtc_commission, 'ether') * Web3.fromWei(tx_event["reservePrice"], 'ether')
        else:
            usd_commission = Web3.fromWei(moc_commission, 'ether') * Web3.fromWei(tx_event["mocPrice"], 'ether')
        d_tx["USDCommission"] = str(int(usd_commission * self.precision))
        d_tx["amount"] = str(tx_event["amount"])
        d_tx["confirmationTime"] = confirmation_time
        d_tx["isPositive"] = True
        d_tx["rbtcCommission"] = str(rbtc_commission)
        d_tx["reservePrice"] = str(tx_event["reservePrice"])
        d_tx["mocCommissionValue"] = str(moc_commission)
        d_tx["mocPrice"] = str(tx_event["mocPrice"])
        gas_fee = self.tx_receipt.gas_used * Web3.fromWei(self.tx_receipt.gas_price, 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        if self.app_mode != "RRC20":
            d_tx["gasFeeUSD"] = str(int(
                gas_fee * Web3.fromWei(tx_event["reservePrice"],
                                       'ether') * self.precision))
        rbtc_total = tx_event["reserveTotal"] + tx_event["commission"] + int(
            gas_fee * self.precision)
        d_tx["RBTCTotal"] = str(rbtc_total)
        usd_total = Web3.fromWei(rbtc_total, 'ether') * Web3.fromWei(
            tx_event["reservePrice"], 'ether')
        d_tx["USDTotal"] = str(int(usd_total * self.precision))
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

        d_event = MoCExchangeStableTokenMint(tx_event, tx_receipt=self.tx_receipt)
        self.index_event(d_event.event, log_index=log_index)


class IndexStableTokenRedeem(BaseIndexEvent):
    name = 'StableTokenRedeem'

    def index_event(self, tx_event, log_index=None):

        # status of tx
        status, confirmation_time = self.status_tx()

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(self.m_client)

        tx_hash = self.tx_receipt.txid

        d_tx = OrderedDict()
        d_tx["address"] = tx_event["account"]
        d_tx["blockNumber"] = self.tx_receipt.block_number
        d_tx["event"] = 'StableTokenRedeem'
        d_tx["transactionHash"] = tx_hash
        d_tx["RBTCAmount"] = str(tx_event["reserveTotal"])
        usd_amount = Web3.fromWei(tx_event["reserveTotal"],
                                  'ether') * Web3.fromWei(tx_event["reservePrice"],
                                                          'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))
        d_tx["amount"] = str(tx_event["amount"])
        d_tx["confirmationTime"] = confirmation_time
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["status"] = status
        d_tx["tokenInvolved"] = 'STABLE'
        if self.app_mode != "RRC20":
            rbtc_commission = tx_event["commission"] + tx_event["btcMarkup"]
        else:
            rbtc_commission = tx_event["commission"] + tx_event["reserveTokenMarkup"]
        moc_commission = tx_event["mocCommissionValue"] + tx_event["mocMarkup"]
        if rbtc_commission > 0:
            usd_commission = Web3.fromWei(rbtc_commission, 'ether') * Web3.fromWei(tx_event["reservePrice"], 'ether')
        else:
            usd_commission = Web3.fromWei(moc_commission, 'ether') * Web3.fromWei(tx_event["mocPrice"], 'ether')
        d_tx["rbtcCommission"] = str(rbtc_commission)
        d_tx["USDCommission"] = str(int(usd_commission * self.precision))
        d_tx["isPositive"] = False
        d_tx["reservePrice"] = str(tx_event["reservePrice"])
        d_tx["mocCommissionValue"] = str(moc_commission)
        d_tx["mocPrice"] = str(tx_event["mocPrice"])
        gas_fee = self.tx_receipt.gas_used * Web3.fromWei(self.tx_receipt.gas_price, 'ether')
        # d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        # if self.app_mode != "RRC20":
        #    d_tx["gasFeeUSD"] = str(int(gas_fee * Web3.fromWei(tx_event.reservePrice, 'ether') * self.precision))
        rbtc_total = tx_event["reserveTotal"] - int(gas_fee * self.precision)
        d_tx["RBTCTotal"] = str(rbtc_total)
        rbtc_total_ether = Web3.fromWei(abs(rbtc_total), 'ether')
        if rbtc_total < 0:
            rbtc_total_ether = -rbtc_total_ether
        usd_total = rbtc_total_ether * Web3.fromWei(tx_event["reservePrice"],
                                                    'ether')
        d_tx["USDTotal"] = str(int(usd_total * self.precision))
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

        # Update the queue operation to delete
        collection_tx.remove({'address': d_tx["address"], 'event': 'QueueDOC'})

        return d_tx

    def on_event(self, tx_event, log_index=None):
        """ Event """

        d_event = MoCExchangeStableTokenRedeem(tx_event, tx_receipt=self.tx_receipt)
        self.index_event(d_event.event, log_index=log_index)


class IndexFreeStableTokenRedeem(BaseIndexEvent):
    name = 'FreeStableTokenRedeem'

    def index_event(self, tx_event, log_index=None):

        # status of tx
        status, confirmation_time = self.status_tx()

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(self.m_client)

        tx_hash = self.tx_receipt.txid

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = self.tx_receipt.block_number
        d_tx["address"] = tx_event["account"]
        d_tx["status"] = status
        d_tx["event"] = 'FreeStableTokenRedeem'
        d_tx["tokenInvolved"] = 'STABLE'
        d_tx["userAmount"] = str(Web3.fromWei(tx_event["amount"], 'ether'))
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["RBTCAmount"] = str(tx_event["reserveTotal"])
        usd_amount = Web3.fromWei(tx_event["reserveTotal"],
                                  'ether') * Web3.fromWei(tx_event["reservePrice"],
                                                          'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))
        d_tx["amount"] = str(tx_event["amount"])
        d_tx["confirmationTime"] = confirmation_time
        if self.app_mode != "RRC20":
            rbtc_commission = tx_event["commission"] + tx_event["btcMarkup"]
        else:
            rbtc_commission = tx_event["commission"] + tx_event["reserveTokenMarkup"]
        moc_commission = tx_event["mocCommissionValue"] + tx_event["mocMarkup"]
        if rbtc_commission > 0:
            usd_commission = Web3.fromWei(rbtc_commission, 'ether') * Web3.fromWei(tx_event["reservePrice"], 'ether')
        else:
            usd_commission = Web3.fromWei(moc_commission, 'ether') * Web3.fromWei(tx_event["mocPrice"], 'ether')
        d_tx["rbtcCommission"] = str(rbtc_commission)
        d_tx["USDCommission"] = str(int(usd_commission * self.precision))
        d_tx["rbtcInterests"] = str(tx_event["interests"])
        usd_interest = Web3.fromWei(tx_event["interests"], 'ether') * Web3.fromWei(
            tx_event["reservePrice"], 'ether')
        d_tx["USDInterests"] = str(int(usd_interest * self.precision))
        d_tx["isPositive"] = False
        d_tx["reservePrice"] = str(tx_event["reservePrice"])
        d_tx["mocCommissionValue"] = str(moc_commission)
        d_tx["mocPrice"] = str(tx_event["mocPrice"])
        gas_fee = self.tx_receipt.gas_used * Web3.fromWei(self.tx_receipt.gas_price, 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        if self.app_mode != "RRC20":
            d_tx["gasFeeUSD"] = str(int(
                gas_fee * Web3.fromWei(tx_event["reservePrice"],
                                       'ether') * self.precision))
        rbtc_total = tx_event["reserveTotal"] - tx_event["commission"] - int(
            gas_fee * self.precision)
        d_tx["RBTCTotal"] = str(rbtc_total)
        rbtc_total_ether = Web3.fromWei(abs(rbtc_total), 'ether')
        if rbtc_total < 0:
            rbtc_total_ether = -rbtc_total_ether
        usd_total = rbtc_total_ether * Web3.fromWei(tx_event["reservePrice"],
                                                    'ether')
        d_tx["USDTotal"] = str(int(usd_total * self.precision))
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

        d_event = MoCExchangeFreeStableTokenRedeem(tx_event, tx_receipt=self.tx_receipt)
        self.index_event(d_event.event, log_index=log_index)
