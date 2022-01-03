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

from moneyonchain.rdoc import MoCExchangeRiskProMint as RDOCMoCExchangeRiskProMint, \
    MoCExchangeStableTokenMint as RDOCMoCExchangeStableTokenMint, \
    MoCExchangeRiskProxMint as RDOCMoCExchangeRiskProxMint, \
    MoCExchangeRiskProRedeem as RDOCMoCExchangeRiskProRedeem, \
    MoCExchangeFreeStableTokenRedeem as RDOCMoCExchangeFreeStableTokenRedeem, \
    MoCExchangeRiskProxRedeem as RDOCMoCExchangeRiskProxRedeem, \
    MoCExchangeStableTokenRedeem as RDOCMoCExchangeStableTokenRedeem

from indexer.mongo_manager import mongo_manager
from indexer.logger import log
from indexer.moc_balances import insert_update_balance_address
from .events import BaseIndexEvent


class IndexRiskProMint(BaseIndexEvent):

    name = 'RiskProMint'

    def index_event(self, m_client, parse_receipt, tx_event):

        # status of tx
        status, confirmation_time = self.status_tx(parse_receipt)

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(m_client)

        tx_hash = parse_receipt["transactionHash"]

        d_tx = OrderedDict()
        d_tx["address"] = tx_event["account"]
        d_tx["blockNumber"] = parse_receipt["blockNumber"]
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

        if "reserveTokenMarkup" in tx_event:
            rbtc_commission = tx_event["commission"] + tx_event["reserveTokenMarkup"]
        else:
            rbtc_commission = tx_event["commission"] + tx_event["btcMarkup"]

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

        gas_fee = parse_receipt['gas_used'] * Web3.fromWei(parse_receipt["gas_price"], 'ether')
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
        d_tx["createdAt"] = parse_receipt['createdAt']

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

        # Insert as pending to update user balances
        insert_update_balance_address(m_client, d_tx["address"])

        return d_tx

    def on_event(self, m_client, parse_receipt):
        """ Event """

        if self.app_mode != "RRC20":
            cl_tx_event = MoCExchangeRiskProMint(parse_receipt)
        else:
            cl_tx_event = RDOCMoCExchangeRiskProMint(parse_receipt)

        self.index_event(m_client, parse_receipt, cl_tx_event.event[self.name])


class IndexRiskProRedeem(BaseIndexEvent):

    name = 'RiskProRedeem'

    def index_event(self, m_client, parse_receipt, tx_event):

        # status of tx
        status, confirmation_time = self.status_tx(parse_receipt)

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(m_client)

        tx_hash = parse_receipt["transactionHash"]

        d_tx = OrderedDict()
        d_tx["event"] = 'RiskProRedeem'
        d_tx["blockNumber"] = parse_receipt["blockNumber"]
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
        if "reserveTokenMarkup" in tx_event:
            rbtc_commission = tx_event["commission"] + tx_event["reserveTokenMarkup"]
        else:
            rbtc_commission = tx_event["commission"] + tx_event["btcMarkup"]
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
        gas_fee = parse_receipt["gas_used"] * Web3.fromWei(parse_receipt["gas_price"], 'ether')
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
        d_tx["createdAt"] = parse_receipt['createdAt']

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

        # Insert as pending to update user balances
        insert_update_balance_address(m_client, d_tx["address"])

        return d_tx

    def on_event(self, m_client, parse_receipt):
        """ Event """

        if self.app_mode != "RRC20":
            cl_tx_event = MoCExchangeRiskProRedeem(parse_receipt)
        else:
            cl_tx_event = RDOCMoCExchangeRiskProRedeem(parse_receipt)

        self.index_event(m_client, parse_receipt, cl_tx_event.event[self.name])


class IndexRiskProxMint(BaseIndexEvent):

    name = 'RiskProxMint'

    def index_event(self, m_client, parse_receipt, tx_event):

        # status of tx
        status, confirmation_time = self.status_tx(parse_receipt)

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(m_client)

        tx_hash = parse_receipt["transactionHash"]

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = parse_receipt["blockNumber"]
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
        if "reserveTokenMarkup" in tx_event:
            rbtc_commission = tx_event["commission"] + tx_event["reserveTokenMarkup"]
        else:
            rbtc_commission = tx_event["commission"] + tx_event["btcMarkup"]
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
        gas_fee = parse_receipt["gas_used"] * Web3.fromWei(parse_receipt["gas_price"], 'ether')
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
        d_tx["createdAt"] = parse_receipt['createdAt']

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

        # Insert as pending to update user balances
        insert_update_balance_address(m_client, d_tx["address"])

        return d_tx

    def on_event(self, m_client, parse_receipt):
        """ Event """

        if self.app_mode != "RRC20":
            cl_tx_event = MoCExchangeRiskProxMint(parse_receipt)
        else:
            cl_tx_event = RDOCMoCExchangeRiskProxMint(parse_receipt)

        self.index_event(m_client, parse_receipt, cl_tx_event.event[self.name])


class IndexRiskProxRedeem(BaseIndexEvent):

    name = 'RiskProxRedeem'

    def index_event(self, m_client, parse_receipt, tx_event):

        # status of tx
        status, confirmation_time = self.status_tx(parse_receipt)

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(m_client)

        tx_hash = parse_receipt["transactionHash"]

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = parse_receipt["blockNumber"]
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
        if "reserveTokenMarkup" in tx_event:
            rbtc_commission = tx_event["commission"] + tx_event["reserveTokenMarkup"]
        else:
            rbtc_commission = tx_event["commission"] + tx_event["btcMarkup"]
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
        gas_fee = parse_receipt["gas_used"] * Web3.fromWei(parse_receipt["gas_price"], 'ether')
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
        d_tx["createdAt"] = parse_receipt['createdAt']

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

        # Insert as pending to update user balances
        insert_update_balance_address(m_client, d_tx["address"])

        return d_tx

    def on_event(self, m_client, parse_receipt):
        """ Event """

        if self.app_mode != "RRC20":
            cl_tx_event = MoCExchangeRiskProxRedeem(parse_receipt)
        else:
            cl_tx_event = RDOCMoCExchangeRiskProxRedeem(parse_receipt)

        self.index_event(m_client, parse_receipt, cl_tx_event.event[self.name])


class IndexStableTokenMint(BaseIndexEvent):
    name = 'StableTokenMint'

    def index_event(self, m_client, parse_receipt, tx_event):

        # status of tx
        status, confirmation_time = self.status_tx(parse_receipt)

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(m_client)

        tx_hash = parse_receipt["transactionHash"]

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = parse_receipt["blockNumber"]
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
        if "reserveTokenMarkup" in tx_event:
            rbtc_commission = tx_event["commission"] + tx_event["reserveTokenMarkup"]
        else:
            rbtc_commission = tx_event["commission"] + tx_event["btcMarkup"]
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
        gas_fee = parse_receipt["gas_used"] * Web3.fromWei(parse_receipt["gas_price"], 'ether')
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
        d_tx["createdAt"] = parse_receipt['createdAt']

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

        # Insert as pending to update user balances
        insert_update_balance_address(m_client, d_tx["address"])

        return d_tx

    def on_event(self, m_client, parse_receipt):
        """ Event """

        if self.app_mode != "RRC20":
            cl_tx_event = MoCExchangeStableTokenMint(parse_receipt)
        else:
            cl_tx_event = RDOCMoCExchangeStableTokenMint(parse_receipt)

        self.index_event(m_client, parse_receipt, cl_tx_event.event[self.name])


class IndexStableTokenRedeem(BaseIndexEvent):
    name = 'StableTokenRedeem'

    def index_event(self, m_client, parse_receipt, tx_event):

        # status of tx
        status, confirmation_time = self.status_tx(parse_receipt)

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(m_client)

        tx_hash = parse_receipt["transactionHash"]

        d_tx = OrderedDict()
        d_tx["address"] = tx_event["account"]
        d_tx["blockNumber"] = parse_receipt["blockNumber"]
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
        if "reserveTokenMarkup" in tx_event:
            rbtc_commission = tx_event["commission"] + tx_event["reserveTokenMarkup"]
        else:
            rbtc_commission = tx_event["commission"] + tx_event["btcMarkup"]
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
        gas_fee = parse_receipt["gas_used"] * Web3.fromWei(parse_receipt["gas_price"], 'ether')
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
        d_tx["createdAt"] = parse_receipt['createdAt']

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

        # Insert as pending to update user balances
        insert_update_balance_address(m_client, d_tx["address"])

        # Update the queue operation to delete
        collection_tx.delete_many({'address': d_tx["address"], 'event': 'QueueDOC'})

        return d_tx

    def on_event(self, m_client, parse_receipt):
        """ Event """

        if self.app_mode != "RRC20":
            cl_tx_event = MoCExchangeStableTokenRedeem(parse_receipt)
        else:
            cl_tx_event = RDOCMoCExchangeStableTokenRedeem(parse_receipt)

        self.index_event(m_client, parse_receipt, cl_tx_event.event[self.name])


class IndexFreeStableTokenRedeem(BaseIndexEvent):
    name = 'FreeStableTokenRedeem'

    def index_event(self, m_client, parse_receipt, tx_event):

        # status of tx
        status, confirmation_time = self.status_tx(parse_receipt)

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(m_client)

        tx_hash = parse_receipt["transactionHash"]

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = parse_receipt["blockNumber"]
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
        if "reserveTokenMarkup" in tx_event:
            rbtc_commission = tx_event["commission"] + tx_event["reserveTokenMarkup"]
        else:
            rbtc_commission = tx_event["commission"] + tx_event["btcMarkup"]
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
        gas_fee = parse_receipt["gas_used"] * Web3.fromWei(parse_receipt["gas_price"], 'ether')
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
        d_tx["createdAt"] = parse_receipt['createdAt']

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

        # Insert as pending to update user balances
        insert_update_balance_address(m_client, d_tx["address"])

        return d_tx

    def on_event(self, m_client, parse_receipt):
        """ Event """

        if self.app_mode != "RRC20":
            cl_tx_event = MoCExchangeFreeStableTokenRedeem(parse_receipt)
        else:
            cl_tx_event = RDOCMoCExchangeFreeStableTokenRedeem(parse_receipt)

        self.index_event(m_client, parse_receipt, cl_tx_event.event[self.name])
