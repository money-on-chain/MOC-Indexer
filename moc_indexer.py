import os

from web3 import Web3
import boto3
import pymongo
import datetime
import time
import random
from bson.decimal128 import Decimal128
from collections import OrderedDict
from web3.types import BlockIdentifier
from web3.logs import DISCARD

from moneyonchain.manager import ConnectionManager
from moneyonchain.moc import MoC, MoCState, MoCInrate
from moneyonchain.rdoc import RDOCMoC, RDOCMoCState, RDOCMoCInrate
from moneyonchain.events import MoCExchangeRiskProMint, \
    MoCExchangeStableTokenMint, \
    MoCExchangeRiskProxMint, \
    MoCExchangeRiskProRedeem, \
    MoCExchangeFreeStableTokenRedeem, \
    MoCExchangeRiskProxRedeem, \
    MoCExchangeStableTokenRedeem, \
    MoCSettlementRedeemRequestAlter, \
    MoCSettlementSettlementRedeemStableToken, \
    MoCSettlementSettlementDeleveraging, \
    MoCInrateDailyPay, \
    MoCInrateRiskProHoldersInterestPay,\
    MoCBucketLiquidation, \
    MoCStateStateTransition

import logging
import logging.config


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger('default')


d_states = {
    0: "Liquidated",
    1: "BProDiscount",
    2: "BelowCobj",
    3: "AboveCobj"
}


class MongoManager:

    def __init__(self, config_options):

        self.options = config_options

    def connect(self):

        uri = self.options['mongo']['uri']
        client = pymongo.MongoClient(uri)

        return client

    def collection_moc_state(self, client, start_index=True):

        mongo_db = self.options['mongo']['db']
        db = client[mongo_db]
        collection = db['MocState']

        # index creation
        #if start_index:
        #    collection.create_index([('block_number', pymongo.DESCENDING)], unique=True)

        return collection

    def collection_price(self, client, start_index=True):

        mongo_db = self.options['mongo']['db']
        db = client[mongo_db]
        collection = db['Price']

        # index creation
        if start_index:
            collection.create_index([('blockHeight', pymongo.ASCENDING)], unique=True)

        return collection

    def collection_user_state(self, client, start_index=True):

        mongo_db = self.options['mongo']['db']
        db = client[mongo_db]
        collection = db['UserState']

        # index creation
        #if start_index:
        #    collection.create_index([('block_number', pymongo.DESCENDING)], unique=True)

        return collection

    def collection_bitpro_holders_interest(self, client, start_index=True):

        mongo_db = self.options['mongo']['db']
        db = client[mongo_db]
        collection = db['BitProHoldersInterest']

        # index creation
        if start_index:
            collection.create_index([('blockHeight', pymongo.ASCENDING)], unique=True)

        return collection

    def collection_settlement_state(self, client, start_index=True):

        mongo_db = self.options['mongo']['db']
        db = client[mongo_db]
        collection = db['SettlementState']

        # index creation
        if start_index:
            collection.create_index([('startBlockNumber', pymongo.ASCENDING)], unique=True)

        return collection

    def collection_inrate_income(self, client, start_index=True):

        mongo_db = self.options['mongo']['db']
        db = client[mongo_db]
        collection = db['InRateIncome']

        # index creation
        if start_index:
            collection.create_index([('blockHeight', pymongo.ASCENDING)], unique=True)

        return collection

    def collection_transaction(self, client, start_index=True):

        mongo_db = self.options['mongo']['db']
        db = client[mongo_db]
        collection = db['Transaction']

        # index creation
        if start_index:
            collection.create_index([('transactionHash', pymongo.ASCENDING),
                                     ('address', pymongo.ASCENDING),
                                     ('event', pymongo.ASCENDING)], unique=True)

        return collection

    def collection_notification(self, client, start_index=True):

        mongo_db = self.options['mongo']['db']
        db = client[mongo_db]
        collection = db['Notification']

        # index creation
        if start_index:
            collection.create_index([('transactionHash', pymongo.ASCENDING),
                                     ('logIndex', pymongo.ASCENDING),
                                     ('event', pymongo.ASCENDING)], unique=True)

        return collection


class MoCIndexer:

    precision = 10 ** 18

    def __init__(self, monitor_config, network_nm):

        self.options = monitor_config
        self.network = network_nm

        self.connection_manager = ConnectionManager(options=self.options, network=self.network)
        self.app_mode = self.options['networks'][self.network]['app_mode']
        self.debug_mode = self.options['debug']

        if self.app_mode == "RRC20":
            self.contract_MoC = RDOCMoC(self.connection_manager)
            self.contract_MoCState = RDOCMoCState(self.connection_manager)
            self.contract_MoCInrate = RDOCMoCInrate(self.connection_manager)
        else:
            self.contract_MoC = MoC(self.connection_manager)
            self.contract_MoCState = MoCState(self.connection_manager)
            self.contract_MoCInrate = MoCInrate(self.connection_manager)

        # initialize mongo db
        self.mm = MongoManager(self.options)

        # Create CloudWatch client
        self.cloudwatch = boto3.client('cloudwatch')

    def balances_from_address(self, address, block_identifier: BlockIdentifier = 'latest'):

        d_user_balance = OrderedDict()
        d_user_balance["mocBalance"] = str(0)
        d_user_balance["bProHoldIncentive"] = str(0)
        d_user_balance["docBalance"] = str(self.contract_MoC.doc_balance_of(
            address,
            formatted=False,
            block_identifier=block_identifier))
        d_user_balance["bproBalance"] = str(self.contract_MoC.bpro_balance_of(
            address,
            formatted=False,
            block_identifier=block_identifier))
        d_user_balance["bprox2Balance"] = str(self.contract_MoC.bprox_balance_of(
            address,
            formatted=False,
            block_identifier=block_identifier))
        d_user_balance["rbtcBalance"] = str(self.contract_MoC.rbtc_balance_of(
            address,
            formatted=False,
            block_identifier=block_identifier))
        d_user_balance["docToRedeem"] = str(self.contract_MoC.doc_amount_to_redeem(
            address,
            formatted=False,
            block_identifier=block_identifier))
        d_user_balance["reserveAllowance"] = str(self.contract_MoC.reserve_allowance(
            address,
            formatted=False,
            block_identifier=block_identifier))
        d_user_balance["spendableBalance"] = str(self.contract_MoC.spendable_balance(
            address,
            formatted=False,
            block_identifier=block_identifier))
        d_user_balance["potentialBprox2MaxInterest"] = str(
            self.contract_MoCInrate.calc_mint_interest_value(
                int(d_user_balance["rbtcBalance"]),
                formatted=False,
                precision=False
            )
        )
        d_user_balance["estimateGasMintBpro"] = str(self.contract_MoC.mint_bpro_gas_estimated(
            int(d_user_balance["rbtcBalance"]))
        )
        d_user_balance["estimateGasMintDoc"] = str(self.contract_MoC.mint_doc_gas_estimated(
            int(d_user_balance["rbtcBalance"]))
        )
        d_user_balance["estimateGasMintBprox2"] = str(self.contract_MoC.mint_bprox_gas_estimated(
            int(d_user_balance["rbtcBalance"]))
        )

        return d_user_balance

    def moc_state_from_sc(self, block_identifier: BlockIdentifier = 'latest'):

        bucket_x2 = str.encode('X2')
        bucket_c0 = str.encode('C0')

        d_moc_state = OrderedDict()
        d_moc_state["bitcoinPrice"] = str(self.contract_MoCState.bitcoin_price(
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["bproAvailableToMint"] = str(self.contract_MoCState.max_mint_bpro_available(
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["bproAvailableToRedeem"] = str(self.contract_MoCState.absolute_max_bpro(
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["bprox2AvailableToMint"] = str(self.contract_MoCState.max_bprox(
            bucket_x2,
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["docAvailableToMint"] = str(self.contract_MoCState.absolute_max_doc(
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["docAvailableToRedeem"] = str(self.contract_MoCState.free_doc(
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["b0Leverage"] = str(self.contract_MoCState.leverage(
            bucket_c0,
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["b0TargetCoverage"] = str(self.contract_MoCState.cobj(
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["x2Leverage"] = str(self.contract_MoCState.leverage(
            bucket_x2,
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["totalBTCAmount"] = str(self.contract_MoCState.rbtc_in_system(
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["bitcoinMovingAverage"] = str(self.contract_MoCState.bitcoin_moving_average(
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["b0BTCInrateBag"] = str(self.contract_MoCState.get_inrate_bag(
            bucket_c0,
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["b0BTCAmount"] = str(self.contract_MoCState.bucket_nbtc(
            bucket_c0,
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["b0DocAmount"] = str(self.contract_MoCState.bucket_ndoc(
            bucket_c0,
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["b0BproAmount"] = str(self.contract_MoCState.bucket_nbpro(
            bucket_c0,
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["x2BTCAmount"] = str(self.contract_MoCState.bucket_nbtc(
            bucket_x2,
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["x2DocAmount"] = str(self.contract_MoCState.bucket_ndoc(
            bucket_x2,
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["x2BproAmount"] = str(self.contract_MoCState.bucket_nbpro(
            bucket_x2,
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["globalCoverage"] = str(self.contract_MoCState.global_coverage(
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["reservePrecision"] = str(self.contract_MoC.reserve_precision(
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["mocPrecision"] = str(self.contract_MoC.sc_precision(
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["x2Coverage"] = str(self.contract_MoCState.coverage(
            bucket_x2,
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["bproPriceInRbtc"] = str(self.contract_MoCState.bpro_tec_price(
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["bproPriceInUsd"] = str(self.contract_MoCState.bpro_price(
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["bproDiscountRate"] = str(self.contract_MoCState.bpro_discount_rate(
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["maxBproWithDiscount"] = str(self.contract_MoCState.max_bpro_with_discount(
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["bproDiscountPrice"] = str(self.contract_MoCState.bpro_discount_price(
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["bprox2PriceInRbtc"] = str(self.contract_MoCState.btc2x_tec_price(
            bucket_x2,
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["bprox2PriceInBpro"] = str(self.contract_MoCState.bprox_price(
            bucket_x2,
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["spotInrate"] = str(self.contract_MoCInrate.spot_inrate(
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["commissionRate"] = str(self.contract_MoCInrate.commission_rate(
            formatted=False,
            block_identifier=block_identifier))
        d_moc_state["bprox2PriceInUsd"] = str(
            int(d_moc_state["bprox2PriceInRbtc"]) * int(d_moc_state["bitcoinPrice"]) / int(
                d_moc_state["reservePrecision"]))
        #d_moc_state["lastUpdateHeight"] = lastUpdateHeight
        d_moc_state["createdAt"] = datetime.datetime.now()
        d_moc_state["dayBlockSpan"] = self.contract_MoCState.day_block_span(
            block_identifier=block_identifier)
        d_moc_state["blocksToSettlement"] = self.contract_MoCState.blocks_to_settlement(
            block_identifier=block_identifier)
        d_moc_state["state"] = self.contract_MoCState.state(
            block_identifier=block_identifier)
        d_moc_state["lastPriceUpdateHeight"] = 0
        #d_moc_state["priceVariation"] = dailyPriceRef
        d_moc_state["paused"] = self.contract_MoC.paused(
            block_identifier=block_identifier)

        return d_moc_state

    def prices_from_sc(self, block_identifier: BlockIdentifier = 'latest'):

        bucket_x2 = str.encode('X2')

        d_price = OrderedDict()
        d_price["bitcoinPrice"] = str(self.contract_MoCState.bitcoin_price(
            formatted=False,
            block_identifier=block_identifier))
        d_price["bproPriceInRbtc"] = str(self.contract_MoCState.bpro_tec_price(
            formatted=False,
            block_identifier=block_identifier))
        d_price["bproPriceInUsd"] = str(self.contract_MoCState.bpro_price(
            formatted=False,
            block_identifier=block_identifier))
        d_price["bproDiscountPrice"] = str(self.contract_MoCState.bpro_discount_price(
            formatted=False,
            block_identifier=block_identifier))
        d_price["bprox2PriceInRbtc"] = str(self.contract_MoCState.btc2x_tec_price(
            bucket_x2,
            formatted=False,
            block_identifier=block_identifier))
        d_price["bprox2PriceInBpro"] = str(self.contract_MoCState.bprox_price(
            bucket_x2,
            formatted=False,
            block_identifier=block_identifier))
        d_price["reservePrecision"] = str(self.contract_MoC.reserve_precision(
            formatted=False,
            block_identifier=block_identifier))
        d_price["bprox2PriceInUsd"] = str(
            int(d_price["bprox2PriceInRbtc"]) * int(d_price["bitcoinPrice"]) / int(
                d_price["reservePrecision"]))
        d_price["createdAt"] = datetime.datetime.now()

        return d_price

    def update_moc_state(self, block_identifier: BlockIdentifier = 'latest'):

        # conect to mongo db
        m_client = self.mm.connect()

        # get last block from node
        last_block = self.connection_manager.block_number

        if block_identifier == 'latest':
            block_height = last_block
        else:
            block_height = block_identifier

        # get all functions from smart contract
        d_moc_state = self.moc_state_from_sc(block_identifier=block_height)

        old_block_height = last_block - d_moc_state['dayBlockSpan']

        # get last price written in mongo
        collection_price = self.mm.collection_price(m_client)
        last_price = collection_price.find_one(filter={"blockHeight": {"$lt": old_block_height}},
                                               sort=[("blockHeight", -1)])

        d_moc_state["lastUpdateHeight"] = block_height
        d_moc_state["priceVariation"] = last_price

        # get collection moc_state from mongo
        collection_moc_state = self.mm.collection_moc_state(m_client)

        # update or insert the new info on mocstate
        post_id = collection_moc_state.find_one_and_update(
            {},
            {"$set": d_moc_state},
            upsert=True)
        d_moc_state['post_id'] = post_id

        return d_moc_state

    def update_balance_from_account(self, account_address, block_identifier: BlockIdentifier = 'latest'):

        # conect to mongo db
        m_client = self.mm.connect()

        # get last block from node
        last_block = self.connection_manager.block_number

        if block_identifier == 'latest':
            block_height = last_block
        else:
            block_height = block_identifier

        # get all functions from smart contract
        d_user_balance = self.balances_from_address(account_address, block_identifier=block_height)

        # get collection user state from mongo
        collection_user_state = self.mm.collection_user_state(m_client)

        user_state = collection_user_state.find_one(
            {"address": account_address}
        )

        if not user_state:
            # if the user not exist in the database created but default info
            d_user_balance["prefLanguage"] = 'en'
            d_user_balance["createdAt"] = datetime.datetime.now()
            d_user_balance["lastNotificationCheckAt"] = datetime.datetime.now()
            d_user_balance["showTermsAndConditions"] = True
            d_user_balance["showTutorialNoMore"] = False

        # update or insert
        post_id = collection_user_state.find_one_and_update(
            {"address": account_address},
            {"$set": d_user_balance},
            upsert=True)

        d_user_balance['post_id'] = post_id

        return d_user_balance

    def update_prices(self, block_identifier: BlockIdentifier = 'latest'):

        # conect to mongo db
        m_client = self.mm.connect()

        # get last block from node
        last_block = self.connection_manager.block_number

        if block_identifier == 'latest':
            block_height = last_block
        else:
            block_height = block_identifier

        # get collection price from mongo
        collection_price = self.mm.collection_price(m_client)

        exist_price = collection_price.find_one(
            {"blockHeight": block_height}
        )

        if exist_price:
            log.warning("Not updating prices! Already exist for that block")
            return -1

        # get all functions from smart contract
        d_prices = self.prices_from_sc(block_identifier=block_height)
        d_prices["blockHeight"] = block_height
        d_prices["createdAt"] = datetime.datetime.now()
        d_prices["isDailyVariation"] = False

        post_id = collection_price.insert_one(d_prices).inserted_id
        d_prices['post_id'] = post_id

        return d_prices

    def moc_contract_addresses(self):

        network = self.connection_manager.network

        moc_addresses = list()
        moc_addresses.append(
            str.lower(self.connection_manager.options['networks'][network]['addresses']['MoC']))
        moc_addresses.append(
            str.lower(self.connection_manager.options['networks'][network]['addresses']['MoCSettlement']))
        moc_addresses.append(
            str.lower(self.connection_manager.options['networks'][network]['addresses']['MoCExchange']))
        moc_addresses.append(
            str.lower(self.connection_manager.options['networks'][network]['addresses']['BProToken']))
        moc_addresses.append(
            str.lower(self.connection_manager.options['networks'][network]['addresses']['DoCToken']))
        moc_addresses.append(
            str.lower(self.connection_manager.options['networks'][network]['addresses']['MoCState']))
        moc_addresses.append(
            str.lower(self.connection_manager.options['networks'][network]['addresses']['MoCInrate']))
        moc_addresses.append(
            str.lower(self.connection_manager.options['networks'][network]['addresses']['oracle']))

        return moc_addresses

    @staticmethod
    def filter_transactions(transactions, filter_addresses):

        l_transactions = list()
        for transaction in transactions:
            tx_to = None
            tx_from = None
            if 'to' in transaction:
                if transaction['to']:
                    tx_to = str.lower(transaction['to'])

            if 'from' in transaction:
                if transaction['from']:
                    tx_from = str.lower(transaction['from'])

            if tx_to in filter_addresses or tx_from in filter_addresses:
                l_transactions.append(transaction)

        return l_transactions

    def search_moc_transaction(self, block):

        moc_addresses = self.moc_contract_addresses()

        f_block = self.connection_manager.get_block(block, full_transactions=True)
        l_transactions = self.filter_transactions(f_block['transactions'], moc_addresses)

        return l_transactions

    def transactions_receipt(self, transactions):

        l_tx_receipt = list()
        for tx in transactions:
            tx_receipt = self.connection_manager.web3.eth.getTransactionReceipt(tx['hash'])
            l_tx_receipt.append(tx_receipt)
        return l_tx_receipt

    def moc_exchange_risk_pro_mint(self, tx_receipt, tx_event, m_client):

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        tx_hash = Web3.toHex(tx_receipt['transactionHash'])

        d_tx = OrderedDict()
        d_tx["address"] = tx_event.account
        d_tx["event"] = 'RiskProMint'
        d_tx["transactionHash"] = tx_hash
        d_tx["RBTCAmount"] = str(tx_event.reserveTotal)
        usd_amount = Web3.fromWei(tx_event.reserveTotal, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))
        d_tx["amount"] = str(tx_event.amount)
        d_tx["confirmationTime"] = None
        d_tx["isPositive"] = True
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["rbtcCommission"] = str(tx_event.commission)
        d_tx["status"] = 'confirming'
        d_tx["tokenInvolved"] = 'RISKPRO'
        d_tx["createdAt"] = datetime.datetime.now()

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        return d_tx

    def moc_exchange_risk_pro_redeem(self, tx_receipt, tx_event, m_client):

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        tx_hash = Web3.toHex(tx_receipt['transactionHash'])

        d_tx = OrderedDict()
        d_tx["event"] = 'RiskProRedeem'
        d_tx["transactionHash"] = tx_hash
        d_tx["address"] = tx_event.account
        d_tx["tokenInvolved"] = 'RISKPRO'
        d_tx["userAmount"] = str(Web3.fromWei(tx_event.amount, 'ether'))
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["createdAt"] = datetime.datetime.now()
        d_tx["status"] = 'confirming'
        d_tx["RBTCAmount"] = str(tx_event.reserveTotal)
        usd_amount = Web3.fromWei(tx_event.reserveTotal, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))
        d_tx["amount"] = str(tx_event.amount)
        d_tx["confirmationTime"] = None
        d_tx["rbtcCommission"] = str(tx_event.commission)

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash},
            {"$set": d_tx},
            upsert=True)

        d_tx['post_id'] = post_id

        return d_tx

    def moc_exchange_risk_prox_mint(self, tx_receipt, tx_event, m_client):

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        tx_hash = Web3.toHex(tx_receipt['transactionHash'])

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["address"] = tx_event.account
        d_tx["status"] = 'confirming'
        d_tx["event"] = 'RiskProxMint'
        d_tx["tokenInvolved"] = 'RISKPROX'
        d_tx["userAmount"] = str(Web3.fromWei(tx_event.amount, 'ether'))
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["createdAt"] = datetime.datetime.now()
        d_tx["RBTCAmount"] = str(tx_event.reserveTotal)
        usd_amount = Web3.fromWei(tx_event.reserveTotal, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))
        d_tx["amount"] = str(tx_event.amount)
        d_tx["confirmationTime"] = None
        d_tx["isPositive"] = True
        d_tx["leverage"] = str(tx_event.leverage)
        d_tx["rbtcCommission"] = str(tx_event.commission)
        d_tx["rbtcInterests"] = str(tx_event.interests)

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash},
            {"$set": d_tx},
            upsert=True)

        d_tx['post_id'] = post_id

        return d_tx

    def moc_exchange_risk_prox_redeem(self, tx_receipt, tx_event, m_client):

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        tx_hash = Web3.toHex(tx_receipt['transactionHash'])

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["address"] = tx_event.account
        d_tx["status"] = 'confirming'
        d_tx["event"] = 'RiskProxRedeem'
        d_tx["tokenInvolved"] = 'RISKPROX'
        d_tx["userAmount"] = str(Web3.fromWei(tx_event.amount, 'ether'))
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["createdAt"] = datetime.datetime.now()
        d_tx["RBTCAmount"] = str(tx_event.reserveTotal)
        usd_amount = Web3.fromWei(tx_event.reserveTotal, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))
        d_tx["amount"] = str(tx_event.amount)
        d_tx["confirmationTime"] = None
        d_tx["leverage"] = str(tx_event.leverage)
        d_tx["rbtcCommission"] = str(tx_event.commission)
        d_tx["rbtcInterests"] = str(tx_event.interests)

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash},
            {"$set": d_tx},
            upsert=True)

        d_tx['post_id'] = post_id

        return d_tx

    def moc_exchange_stable_token_mint(self, tx_receipt, tx_event, m_client):

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        tx_hash = Web3.toHex(tx_receipt['transactionHash'])

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["address"] = tx_event.account
        d_tx["status"] = 'confirming'
        d_tx["event"] = 'StableTokenMint'
        d_tx["tokenInvolved"] = 'STABLE'
        # WARNING something to investigate, commented think is correct
        #d_tx["userAmount"] = str(Web3.fromWei(tx_event.amount, 'ether'))
        d_tx["userAmount"] = str(Web3.fromWei(tx_event.reserveTotal, 'ether'))
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["createdAt"] = datetime.datetime.now()
        d_tx["RBTCAmount"] = str(tx_event.reserveTotal)
        usd_amount = Web3.fromWei(tx_event.reserveTotal, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))
        d_tx["amount"] = str(tx_event.amount)
        d_tx["confirmationTime"] = None
        d_tx["isPositive"] = True
        d_tx["rbtcCommission"] = str(tx_event.commission)

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash},
            {"$set": d_tx},
            upsert=True)

        d_tx['post_id'] = post_id

        return d_tx

    def moc_exchange_stable_token_redeem(self, tx_receipt, tx_event, m_client):

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        tx_hash = Web3.toHex(tx_receipt['transactionHash'])

        d_tx = OrderedDict()
        d_tx["address"] = tx_event.account
        d_tx["event"] = 'StableTokenRedeem'
        d_tx["transactionHash"] = tx_hash
        d_tx["RBTCAmount"] = str(tx_event.reserveTotal)
        usd_amount = Web3.fromWei(tx_event.reserveTotal, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))
        d_tx["amount"] = str(tx_event.amount)
        d_tx["confirmationTime"] = None
        d_tx["createdAt"] = datetime.datetime.now()
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["status"] = 'confirming'
        d_tx["tokenInvolved"] = 'STABLE'
        d_tx["rbtcCommission"] = str(tx_event.commission)

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash},
            {"$set": d_tx},
            upsert=True)

        d_tx['post_id'] = post_id

        return d_tx

    def moc_exchange_free_stable_token_redeem(self, tx_receipt, tx_event, m_client):

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        tx_hash = Web3.toHex(tx_receipt['transactionHash'])

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["address"] = tx_event.account
        d_tx["status"] = 'confirming'
        d_tx["event"] = 'FreeStableTokenRedeem'
        d_tx["tokenInvolved"] = 'STABLE'
        d_tx["userAmount"] = str(Web3.fromWei(tx_event.amount, 'ether'))
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["createdAt"] = datetime.datetime.now()
        d_tx["RBTCAmount"] = str(tx_event.reserveTotal)
        usd_amount = Web3.fromWei(tx_event.reserveTotal, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))
        d_tx["amount"] = str(tx_event.amount)
        d_tx["confirmationTime"] = None
        d_tx["rbtcCommission"] = str(tx_event.commission)
        d_tx["rbtcInterests"] = str(tx_event.interests)

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash},
            {"$set": d_tx},
            upsert=True)

        d_tx['post_id'] = post_id

        return d_tx

    def logs_process_moc_exchange(self, tx_receipt, m_client):

        events = self.contract_MoC.sc_moc_exchange.events

        # RiskProMint
        tx_logs = events.RiskProMint().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            tx_event = MoCExchangeRiskProMint(self.connection_manager, tx_log)
            self.moc_exchange_risk_pro_mint(tx_receipt, tx_event, m_client)

        # RiskProRedeem
        tx_logs = events.RiskProRedeem().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            tx_event = MoCExchangeRiskProRedeem(self.connection_manager, tx_log)
            self.moc_exchange_risk_pro_redeem(tx_receipt, tx_event, m_client)

        # RiskProxMint
        tx_logs = events.RiskProxMint().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            tx_event = MoCExchangeRiskProxMint(self.connection_manager, tx_log)
            self.moc_exchange_risk_prox_mint(tx_receipt, tx_event, m_client)

        # RiskProxRedeem
        tx_logs = events.RiskProxRedeem().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            tx_event = MoCExchangeRiskProxRedeem(self.connection_manager, tx_log)
            self.moc_exchange_risk_prox_redeem(tx_receipt, tx_event, m_client)

        # StableTokenMint
        tx_logs = events.StableTokenMint().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            tx_event = MoCExchangeStableTokenMint(self.connection_manager, tx_log)
            self.moc_exchange_stable_token_mint(tx_receipt, tx_event, m_client)

        # StableTokenRedeem
        tx_logs = events.StableTokenRedeem().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            tx_event = MoCExchangeStableTokenRedeem(self.connection_manager, tx_log)
            self.moc_exchange_stable_token_redeem(tx_receipt, tx_event, m_client)

        # FreeStableTokenRedeem
        tx_logs = events.FreeStableTokenRedeem().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            tx_event = MoCExchangeFreeStableTokenRedeem(self.connection_manager, tx_log)
            self.moc_exchange_free_stable_token_redeem(tx_receipt, tx_event, m_client)

    def moc_settlement_redeem_request_alter(self, tx_receipt, tx_event, m_client):

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        tx_hash = Web3.toHex(tx_receipt['transactionHash'])

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["address"] = tx_event.redeemer
        d_tx["status"] = 'pending'
        d_tx["event"] = 'RedeemRequestAlter'
        d_tx["tokenInvolved"] = 'STABLE'
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["createdAt"] = datetime.datetime.now()
        d_tx["amount"] = str(tx_event.delta)
        d_tx["confirmationTime"] = None
        d_tx["isPositive"] = tx_event.isAddition

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash},
            {"$set": d_tx},
            upsert=True)

        d_tx['post_id'] = post_id

        return d_tx

    def moc_settlement_redeem_stable_token(self, tx_receipt, tx_event, m_client):
        pass

    def moc_settlement_redeem_stable_token_notification(self, tx_receipt, tx_event, tx_log, m_client):

        # Notifications
        collection_tx = self.mm.collection_notification(m_client)
        tx_hash = Web3.toHex(tx_receipt['transactionHash'])
        event_name = 'SettlementRedeemStableToken'
        log_index = tx_log['logIndex']

        d_tx = dict()
        d_tx["event"] = event_name
        d_tx["transactionHash"] = tx_hash
        d_tx["logIndex"] = log_index
        d_tx["queueSize"] = str(tx_event.queueSize)
        d_tx["accumCommissions"] = str(tx_event.accumCommissions)
        d_tx["reservePrice"] = str(tx_event.reservePrice)
        d_tx["timestamp"] = tx_event.timestamp

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash, "event": event_name, "logIndex": log_index},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        return d_tx

    def moc_settlement_deleveraging(self, tx_receipt, tx_event, m_client):
        pass

    def logs_process_moc_settlement(self, tx_receipt, m_client):

        events = self.contract_MoC.sc_moc_settlement.events

        # RedeemRequestAlter
        tx_logs = events.RedeemRequestAlter().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            tx_event = MoCSettlementRedeemRequestAlter(self.connection_manager, tx_log)
            self.moc_settlement_redeem_request_alter(tx_receipt, tx_event, m_client)

        # SettlementRedeemStableToken
        tx_logs = events.SettlementRedeemStableToken().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            tx_event = MoCSettlementSettlementRedeemStableToken(self.connection_manager, tx_log)
            self.moc_settlement_redeem_stable_token(tx_receipt, tx_event, m_client)
            self.moc_settlement_redeem_stable_token_notification(tx_receipt, tx_event, tx_log, m_client)

        # SettlementDeleveraging
        tx_logs = events.SettlementDeleveraging().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            tx_event = MoCSettlementSettlementDeleveraging(self.connection_manager, tx_log)
            self.moc_settlement_deleveraging(tx_receipt, tx_event, m_client)

    def moc_inrate_daily_pay(self, tx_receipt, tx_event, m_client):

        collection_inrate = self.mm.collection_inrate_income(m_client)

        d_tx = OrderedDict()
        d_tx["blockHeight"] = tx_event.blockNumber
        d_tx["ratePayAmount"] = str(tx_event.amount)
        d_tx["nBTCBitProOfBucketZero"] = str(tx_event.nReserveBucketC0)
        d_tx["timestamp"] = tx_event.timestamp
        d_tx["createdAt"] = datetime.datetime.now()

        post_id = collection_inrate.find_one_and_update(
            {"blockHeight": tx_event.blockNumber},
            {"$set": d_tx},
            upsert=True)

        d_tx['post_id'] = post_id

        return d_tx

    def moc_inrate_daily_pay_notification(self, tx_receipt, tx_event, tx_log, m_client):

        collection_tx = self.mm.collection_notification(m_client)
        tx_hash = Web3.toHex(tx_receipt['transactionHash'])
        event_name = 'InrateDailyPay'
        log_index = tx_log['logIndex']

        d_tx = dict()
        d_tx["event"] = event_name
        d_tx["transactionHash"] = tx_hash
        d_tx["logIndex"] = log_index
        d_tx["amount"] = str(tx_event.amount)
        d_tx["daysToSettlement"] = str(tx_event.daysToSettlement)
        d_tx["timestamp"] = tx_event.timestamp

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash, "event": event_name, "logIndex": log_index},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        return d_tx

    def moc_inrate_risk_pro_holders_interest_pay(self, tx_receipt, tx_event, m_client):

        collection_inrate = self.mm.collection_bitpro_holders_interest(m_client)

        d_tx = OrderedDict()
        d_tx["blockHeight"] = tx_event.blockNumber
        d_tx["amount"] = str(tx_event.amount)
        d_tx["nBtcBucketC0BeforePay"] = str(tx_event.nReserveBucketC0BeforePay)
        d_tx["timestamp"] = tx_event.timestamp
        d_tx["createdAt"] = datetime.datetime.now()

        post_id = collection_inrate.find_one_and_update(
            {"blockHeight": tx_event.blockNumber},
            {"$set": d_tx},
            upsert=True)

        d_tx['post_id'] = post_id

        return d_tx

    def moc_inrate_risk_pro_holders_interest_pay_notification(self, tx_receipt, tx_log, tx_event, m_client):

        collection_tx = self.mm.collection_notification(m_client)
        tx_hash = Web3.toHex(tx_receipt['transactionHash'])
        event_name = 'RiskProHoldersInterestPay'
        log_index = tx_log['logIndex']

        d_tx = OrderedDict()
        d_tx["event"] = event_name
        d_tx["transactionHash"] = tx_hash
        d_tx["logIndex"] = log_index
        d_tx["amount"] = str(tx_event.amount)
        d_tx["nBtcBucketC0BeforePay"] = str(tx_event.nReserveBucketC0BeforePay)
        d_tx["timestamp"] = tx_event.timestamp

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash, "event": event_name, "logIndex": log_index},
            {"$set": d_tx},
            upsert=True)

        d_tx['post_id'] = post_id

        return d_tx

    def logs_process_moc_inrate(self, tx_receipt, m_client):

        events = self.contract_MoC.sc_moc_inrate.events

        # InrateDailyPay
        tx_logs = events.InrateDailyPay().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            tx_event = MoCInrateDailyPay(self.connection_manager, tx_log)
            self.moc_inrate_daily_pay(tx_receipt, tx_event, m_client)
            self.moc_inrate_daily_pay_notification(tx_receipt, tx_event, tx_log, m_client)

        # RiskProHoldersInterestPay
        tx_logs = events.RiskProHoldersInterestPay().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            tx_event = MoCInrateRiskProHoldersInterestPay(self.connection_manager, tx_log)
            self.moc_inrate_risk_pro_holders_interest_pay(tx_receipt, tx_event, m_client)
            self.moc_inrate_risk_pro_holders_interest_pay_notification(tx_receipt, tx_log, tx_event, m_client)

    def moc_bucket_liquidation(self, tx_receipt, tx_event, m_client):
        pass

    def moc_bucket_liquidation_notification(self, tx_receipt, tx_event, tx_log, m_client):

        collection_tx = self.mm.collection_notification(m_client)
        tx_hash = Web3.toHex(tx_receipt['transactionHash'])
        event_name = 'BucketLiquidation'
        log_index = tx_log['logIndex']

        d_tx = OrderedDict()
        d_tx["event"] = event_name
        d_tx["transactionHash"] = tx_hash
        d_tx["logIndex"] = log_index
        d_tx["bucket"] = tx_event.bucket
        d_tx["timestamp"] = tx_event.timestamp

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash, "event": event_name, "logIndex": log_index},
            {"$set": d_tx},
            upsert=True)

        d_tx['post_id'] = post_id

        return d_tx

    def logs_process_moc(self, tx_receipt, m_client):

        events = self.contract_MoC.events

        # BucketLiquidation
        tx_logs = events.BucketLiquidation().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            tx_event = MoCBucketLiquidation(self.connection_manager, tx_log)
            self.moc_bucket_liquidation(tx_receipt, tx_event, m_client)
            self.moc_bucket_liquidation_notification(tx_receipt, tx_event, tx_log, m_client)

    def moc_state_transition(self, tx_receipt, tx_event, m_client):
        pass

    def moc_state_transition_notification(self, tx_receipt, tx_event, tx_log, m_client):

        collection_tx = self.mm.collection_notification(m_client)
        tx_hash = Web3.toHex(tx_receipt['transactionHash'])
        event_name = 'StateTransition'
        log_index = tx_log['logIndex']

        d_tx = OrderedDict()
        d_tx["event"] = event_name
        d_tx["transactionHash"] = tx_hash
        d_tx["logIndex"] = log_index
        d_tx["newState"] = d_states[tx_event.newState]
        d_tx["timestamp"] = tx_event.timestamp

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash, "event": event_name, "logIndex": log_index},
            {"$set": d_tx},
            upsert=True)

        d_tx['post_id'] = post_id

        return d_tx

    def logs_process_moc_state(self, tx_receipt, m_client):

        events = self.contract_MoC.sc_moc_state.events

        # StateTransition
        tx_logs = events.StateTransition().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            tx_event = MoCStateStateTransition(self.connection_manager, tx_log)
            self.moc_state_transition(tx_receipt, tx_event, m_client)
            self.moc_state_transition_notification(tx_receipt, tx_event, tx_log, m_client)

    def logs_transactions_receipts(self, tx_receipts, m_client):

        network = self.connection_manager.network
        moc_addresses = self.connection_manager.options['networks'][network]['addresses']

        for tx_receipt in tx_receipts:
            if not tx_receipt['logs']:
                continue
            for tx_log in tx_receipt['logs']:
                tx_logs_address = str.lower(tx_log['address'])
                if tx_logs_address in [str.lower(moc_addresses['MoCExchange']),
                                       str.lower(moc_addresses['MoCSettlement']),
                                       str.lower(moc_addresses['MoCInrate']),
                                       str.lower(moc_addresses['MoC']),
                                       str.lower(moc_addresses['MoCState'])]:

                    self.logs_process_moc_exchange(tx_receipt, m_client)
                    self.logs_process_moc_settlement(tx_receipt, m_client)
                    self.logs_process_moc_inrate(tx_receipt, m_client)
                    self.logs_process_moc(tx_receipt, m_client)
                    self.logs_process_moc_state(tx_receipt, m_client)

    def update_moc_events(self, block_identifier: BlockIdentifier = 'latest'):

        # conect to mongo db
        m_client = self.mm.connect()

        # get last block from node
        last_block = self.connection_manager.block_number

        if block_identifier == 'latest':
            block_height = last_block
        else:
            block_height = block_identifier

        transactions = self.search_moc_transaction(block_height)
        transactions_receipts = self.transactions_receipt(transactions)
        self.logs_transactions_receipts(transactions_receipts, m_client)
