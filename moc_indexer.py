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
from web3.exceptions import TransactionNotFound
from requests.exceptions import HTTPError

from moneyonchain.manager import ConnectionManager
from moneyonchain.moc import MoC, MoCState, MoCInrate, MoCSettlement, MoCMedianizer
from moneyonchain.rdoc import RDOCMoC, RDOCMoCState, RDOCMoCInrate, RDOCMoCSettlement, RDOCMoCMedianizer
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
    MoCStateStateTransition, \
    MoCSettlementSettlementStarted, \
    ERC20Approval, \
    ERC20Transfer, \
    MoCSettlementRedeemRequestProcessed, \
    MoCSettlementSettlementCompleted
from moneyonchain.token import RIF, RIFDoC, RIFPro, DoCToken, BProToken

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

    def collection_moc_state_history(self, client, start_index=True):

        mongo_db = self.options['mongo']['db']
        db = client[mongo_db]
        collection = db['MocState_history']

        # index creation
        if start_index:
            collection.create_index([('blockHeight', pymongo.DESCENDING)], unique=True)

        return collection

    def collection_moc_state_status(self, client, start_index=True):

        mongo_db = self.options['mongo']['db']
        db = client[mongo_db]
        collection = db['MocState_status']

        # index creation
        if start_index:
            collection.create_index([('blockHeight', pymongo.DESCENDING)], unique=True)

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

    def collection_user_state_update(self, client, start_index=True):

        mongo_db = self.options['mongo']['db']
        db = client[mongo_db]
        collection = db['UserState_update']

        # index creation
        #if start_index:
        #    collection.create_index([('block_number', pymongo.DESCENDING)], unique=True)

        return collection

    def collection_users(self, client, start_index=True):

        mongo_db = self.options['mongo']['db']
        db = client[mongo_db]
        collection = db['users']

        # index creation
        #if start_index:
        #    collection.create_index([('block_number', pymongo.DESCENDING)], unique=True)

        return collection

    def collection_bitpro_holders_interest(self, client, start_index=True):

        mongo_db = self.options['mongo']['db']
        db = client[mongo_db]
        collection = db['BitProHoldersInterest']

        # index creation
        #if start_index:
        #    collection.create_index([('blockHeight', pymongo.ASCENDING)], unique=True)

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
        #if start_index:
        #    collection.create_index([('blockHeight', pymongo.ASCENDING)], unique=True)

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

    def collection_moc_indexer(self, client, start_index=True):

        mongo_db = self.options['mongo']['db']
        db = client[mongo_db]
        collection = db['moc_indexer']

        return collection


class MoCIndexer:

    precision = 10 ** 18

    def __init__(self, config_app, network_app):

        self.options = config_app
        self.network = network_app

        self.connection_manager = ConnectionManager(options=self.options, network=self.network)
        self.app_mode = self.options['networks'][self.network]['app_mode']
        self.debug_mode = self.options['debug']

        if self.app_mode == "RRC20":
            self.contract_MoC = RDOCMoC(self.connection_manager)
            self.contract_MoCState = RDOCMoCState(self.connection_manager)
            self.contract_MoCInrate = RDOCMoCInrate(self.connection_manager)
            self.contract_MoCSettlement = RDOCMoCSettlement(self.connection_manager)
            self.contract_ReserveToken = RIF(self.connection_manager)
            self.contract_StableToken = RIFDoC(self.connection_manager)
            self.contract_RiskProToken = RIFPro(self.connection_manager)
            self.contract_MoCMedianizer = RDOCMoCMedianizer(self.connection_manager)
        else:
            self.contract_MoC = MoC(self.connection_manager)
            self.contract_MoCState = MoCState(self.connection_manager)
            self.contract_MoCInrate = MoCInrate(self.connection_manager)
            self.contract_MoCSettlement = MoCSettlement(self.connection_manager)
            self.contract_StableToken = DoCToken(self.connection_manager)
            self.contract_RiskProToken = BProToken(self.connection_manager)
            self.contract_MoCMedianizer = MoCMedianizer(self.connection_manager)

        # initialize mongo db
        self.mm = MongoManager(self.options)

        # Create CloudWatch client
        self.cloudwatch = boto3.client('cloudwatch')

    def balances_from_address(self, address, block_height):

        d_user_balance = OrderedDict()
        d_user_balance["mocBalance"] = str(0)
        d_user_balance["bProHoldIncentive"] = str(0)
        d_user_balance["docBalance"] = str(self.contract_MoC.doc_balance_of(
            address,
            formatted=False,
            block_identifier=block_height))
        d_user_balance["bproBalance"] = str(self.contract_MoC.bpro_balance_of(
            address,
            formatted=False,
            block_identifier=block_height))
        d_user_balance["bprox2Balance"] = str(self.contract_MoC.bprox_balance_of(
            address,
            formatted=False,
            block_identifier=block_height))
        d_user_balance["rbtcBalance"] = str(self.contract_MoC.rbtc_balance_of(
            address,
            formatted=False,
            block_identifier=block_height))
        d_user_balance["docToRedeem"] = str(self.contract_MoC.doc_amount_to_redeem(
            address,
            formatted=False,
            block_identifier=block_height))
        d_user_balance["reserveAllowance"] = str(self.contract_MoC.reserve_allowance(
            address,
            formatted=False,
            block_identifier=block_height))
        d_user_balance["spendableBalance"] = str(self.contract_MoC.spendable_balance(
            address,
            formatted=False,
            block_identifier=block_height))
        try:
            d_user_balance["potentialBprox2MaxInterest"] = str(
                self.contract_MoCInrate.calc_mint_interest_value(
                    int(d_user_balance["rbtcBalance"]),
                    formatted=False,
                    precision=False
                )
            )
        except HTTPError:
            log.error("[WARNING] potentialBprox2MaxInterest Exception!")
            d_user_balance["potentialBprox2MaxInterest"] = '0'

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

    def riskprox_balances_from_address(self, address, block_identifier: BlockIdentifier = 'latest'):

        d_user_balance = OrderedDict()

        d_user_balance["bprox2Balance"] = str(self.contract_MoC.bprox_balance_of(
            address,
            formatted=False,
            block_identifier=block_identifier))

        return d_user_balance

    def moc_state_from_sc(self, block_identifier: BlockIdentifier = 'latest'):

        bucket_x2 = str.encode('X2')
        bucket_c0 = str.encode('C0')

        d_moc_state = OrderedDict()

        #peek = self.contract_MoCMedianizer.peek(formatted=False,
        #                                        block_identifier=block_identifier)
        #
        #d_moc_state["bitcoinPrice"] = str(peek[0])
        #d_moc_state["isPriceValid"] = str(peek[1])

        try:
            d_moc_state["bitcoinPrice"] = str(self.contract_MoCState.bitcoin_price(
                formatted=False,
                block_identifier=block_identifier))
        except HTTPError:
            log.error("No price valid in BLOCKHEIGHT: [{0}] skipping!".format(block_identifier))
            return

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
        try:
            d_moc_state["spotInrate"] = str(self.contract_MoCInrate.spot_inrate(
                formatted=False,
                block_identifier=block_identifier))
        except HTTPError:
            log.error("[WARNING] spotInrate Exception")
            d_moc_state["spotInrate"] = '0'

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
        d_moc_state["blockSpan"] = self.contract_MoCSettlement.block_span(
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

    def state_status_from_sc(self, block_identifier: BlockIdentifier = 'latest'):

        d_status = OrderedDict()

        try:
            str(self.contract_MoCState.bitcoin_price(
                formatted=False,
                block_identifier=block_identifier))
        except HTTPError:
            price_active = False
        else:
            price_active = True

        d_status['price_active'] = price_active
        d_status["paused"] = self.contract_MoC.paused(
            block_identifier=block_identifier)
        d_status["state"] = self.contract_MoCState.state(
            block_identifier=block_identifier)

        return d_status

    def prices_from_sc(self, block_identifier: BlockIdentifier = 'latest'):

        bucket_x2 = str.encode('X2')

        d_price = OrderedDict()

        #peek = self.contract_MoCMedianizer.peek(formatted=False,
        #                                        block_identifier=block_identifier)
        #
        #d_price["bitcoinPrice"] = str(peek[0])
        #d_price["isPriceValid"] = str(peek[1])

        try:
            d_price["bitcoinPrice"] = str(self.contract_MoCState.bitcoin_price(
                formatted=False,
                block_identifier=block_identifier))
        except HTTPError:
            log.error("No price valid in BLOCKHEIGHT: [{0}] skipping!".format(block_identifier))
            return

        d_price["bproPriceInRbtc"] = str(self.contract_MoCState.bpro_tec_price(
            formatted=False,
            block_identifier=block_identifier))
        d_price["bproPriceInUsd"] = str(self.contract_MoCState.bpro_price(
            formatted=False,
            block_identifier=block_identifier))

        try:
            d_price["bproDiscountPrice"] = str(self.contract_MoCState.bpro_discount_price(
                formatted=False,
                block_identifier=block_identifier))
        except HTTPError:
            log.error("No bproDiscountPrice valid in BLOCKHEIGHT: [{0}] skipping!".format(block_identifier))
            return

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

        if self.app_mode == 'RRC20':
            moc_addresses.append(
                str.lower(self.connection_manager.options['networks'][network]['addresses']['ReserveToken']))

        return moc_addresses

    @staticmethod
    def filter_transactions(transactions, filter_addresses):

        l_transactions = list()
        d_index_transactions = dict()
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
                d_index_transactions[Web3.toHex(transaction['hash'])] = transaction

        return l_transactions, d_index_transactions

    def search_moc_transaction(self, block):

        moc_addresses = self.moc_contract_addresses()

        f_block = self.connection_manager.get_block(block, full_transactions=True)
        l_transactions, d_index_transactions = self.filter_transactions(f_block['transactions'], moc_addresses)

        return l_transactions

    def transactions_receipt(self, transactions):

        l_tx_receipt = list()
        for tx in transactions:
            try:
                tx_receipt = self.connection_manager.web3.eth.getTransactionReceipt(tx['hash'])
            except TransactionNotFound:
                log.error("No transaction receipt for hash: [{0}]".format(Web3.toHex(tx['hash'])))
                tx_receipt = None
            if tx_receipt:
                l_tx_receipt.append(tx_receipt)

        return l_tx_receipt

    def moc_exchange_risk_pro_mint(self,
                                   tx_receipt,
                                   tx_event,
                                   m_client,
                                   block_height,
                                   block_height_current,
                                   d_moc_transactions):

        confirm_blocks = self.options['scan_moc_blocks']['confirm_blocks']
        if block_height_current - block_height > confirm_blocks:
            status = 'confirmed'
            confirmation_time = datetime.datetime.now()
        else:
            status = 'confirming'
            confirmation_time = None

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        tx_hash = Web3.toHex(tx_receipt['transactionHash'])
        moc_tx = d_moc_transactions[tx_hash]

        d_tx = OrderedDict()
        d_tx["address"] = tx_event.account
        d_tx["blockNumber"] = tx_event.blockNumber
        d_tx["event"] = 'RiskProMint'
        d_tx["transactionHash"] = tx_hash
        d_tx["RBTCAmount"] = str(tx_event.reserveTotal)
        usd_amount = Web3.fromWei(tx_event.reserveTotal, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))
        d_tx["amount"] = str(tx_event.amount)
        d_tx["confirmationTime"] = confirmation_time
        d_tx["isPositive"] = True
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["rbtcCommission"] = str(tx_event.commission)
        usd_commission = Web3.fromWei(tx_event.commission, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDCommission"] = str(int(usd_commission * self.precision))
        d_tx["status"] = status
        d_tx["tokenInvolved"] = 'RISKPRO'
        d_tx["reservePrice"] = str(tx_event.reservePrice)
        gas_fee = tx_receipt['gasUsed'] * Web3.fromWei(moc_tx['gasPrice'], 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["gasFeeUSD"] = str(int(gas_fee * Web3.fromWei(tx_event.reservePrice, 'ether') * self.precision))
        rbtc_total = tx_event.reserveTotal + tx_event.commission + int(gas_fee * self.precision)
        d_tx["RBTCTotal"] = str(rbtc_total)
        usd_total = Web3.fromWei(rbtc_total, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDTotal"] = str(int(usd_total * self.precision))
        d_tx["processLogs"] = True

        d_tx_insert = OrderedDict()
        d_tx_insert["createdAt"] = datetime.datetime.now()

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "address": d_tx["address"],
             "event": d_tx["event"]},
            {"$set": d_tx,
             "$setOnInsert": d_tx_insert},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} From: [{1}] Amount: {2} Tx Hash: {3}".format(
            d_tx["event"],
            d_tx["address"],
            d_tx["amount"],
            tx_hash))

        # update user balances
        self.update_balance_address(m_client, d_tx["address"], block_height)

        return d_tx

    def moc_exchange_risk_pro_redeem(self,
                                     tx_receipt,
                                     tx_event,
                                     m_client,
                                     block_height,
                                     block_height_current,
                                     d_moc_transactions):

        confirm_blocks = self.options['scan_moc_blocks']['confirm_blocks']
        if block_height_current - block_height > confirm_blocks:
            status = 'confirmed'
            confirmation_time = datetime.datetime.now()
        else:
            status = 'confirming'
            confirmation_time = None

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        tx_hash = Web3.toHex(tx_receipt['transactionHash'])
        moc_tx = d_moc_transactions[tx_hash]

        d_tx = OrderedDict()
        d_tx["event"] = 'RiskProRedeem'
        d_tx["blockNumber"] = tx_event.blockNumber
        d_tx["transactionHash"] = tx_hash
        d_tx["address"] = tx_event.account
        d_tx["tokenInvolved"] = 'RISKPRO'
        d_tx["userAmount"] = str(Web3.fromWei(tx_event.amount, 'ether'))
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["status"] = status
        d_tx["RBTCAmount"] = str(tx_event.reserveTotal)
        usd_amount = Web3.fromWei(tx_event.reserveTotal, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))
        d_tx["amount"] = str(tx_event.amount)
        d_tx["confirmationTime"] = confirmation_time
        d_tx["rbtcCommission"] = str(tx_event.commission)
        usd_commission = Web3.fromWei(tx_event.commission, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDCommission"] = str(int(usd_commission * self.precision))
        d_tx["isPositive"] = False
        d_tx["reservePrice"] = str(tx_event.reservePrice)
        gas_fee = tx_receipt['gasUsed'] * Web3.fromWei(moc_tx['gasPrice'], 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["gasFeeUSD"] = str(int(gas_fee * Web3.fromWei(tx_event.reservePrice, 'ether') * self.precision))
        rbtc_total = tx_event.reserveTotal - int(gas_fee * self.precision)
        d_tx["RBTCTotal"] = str(rbtc_total)
        usd_total = Web3.fromWei(rbtc_total, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDTotal"] = str(int(usd_total * self.precision))
        d_tx["processLogs"] = True

        d_tx_insert = OrderedDict()
        d_tx_insert["createdAt"] = datetime.datetime.now()

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "address": d_tx["address"],
             "event": d_tx["event"]},
            {"$set": d_tx,
             "$setOnInsert": d_tx_insert},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} From: [{1}] Amount: {2} Tx Hash: {3}".format(
            d_tx["event"],
            d_tx["address"],
            d_tx["amount"],
            tx_hash))

        # update user balances
        self.update_balance_address(m_client, d_tx["address"], block_height)

        return d_tx

    def moc_exchange_risk_prox_mint(self,
                                    tx_receipt,
                                    tx_event,
                                    m_client,
                                    block_height,
                                    block_height_current,
                                    d_moc_transactions):

        confirm_blocks = self.options['scan_moc_blocks']['confirm_blocks']
        if block_height_current - block_height > confirm_blocks:
            status = 'confirmed'
            confirmation_time = datetime.datetime.now()
        else:
            status = 'confirming'
            confirmation_time = None

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        tx_hash = Web3.toHex(tx_receipt['transactionHash'])
        moc_tx = d_moc_transactions[tx_hash]

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = tx_event.blockNumber
        d_tx["address"] = tx_event.account
        d_tx["status"] = status
        d_tx["event"] = 'RiskProxMint'
        d_tx["tokenInvolved"] = 'RISKPROX'
        d_tx["userAmount"] = str(Web3.fromWei(tx_event.amount, 'ether'))
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["RBTCAmount"] = str(tx_event.reserveTotal)
        usd_amount = Web3.fromWei(tx_event.reserveTotal, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))
        d_tx["amount"] = str(tx_event.amount)
        d_tx["confirmationTime"] = confirmation_time
        d_tx["isPositive"] = True
        d_tx["leverage"] = str(tx_event.leverage)
        d_tx["rbtcCommission"] = str(tx_event.commission)
        usd_commission = Web3.fromWei(tx_event.commission, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDCommission"] = str(int(usd_commission * self.precision))
        d_tx["rbtcInterests"] = str(tx_event.interests)
        usd_interest = Web3.fromWei(tx_event.interests, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDInterests"] = str(int(usd_interest * self.precision))
        d_tx["reservePrice"] = str(tx_event.reservePrice)
        gas_fee = tx_receipt['gasUsed'] * Web3.fromWei(moc_tx['gasPrice'], 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["gasFeeUSD"] = str(int(gas_fee * Web3.fromWei(tx_event.reservePrice, 'ether') * self.precision))
        rbtc_total = tx_event.reserveTotal + tx_event.commission + tx_event.interests + int(gas_fee * self.precision)
        d_tx["RBTCTotal"] = str(rbtc_total)
        usd_total = Web3.fromWei(rbtc_total, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDTotal"] = str(int(usd_total * self.precision))
        d_tx["processLogs"] = True

        d_tx_insert = OrderedDict()
        d_tx_insert["createdAt"] = datetime.datetime.now()

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "address": d_tx["address"],
             "event": d_tx["event"]},
            {"$set": d_tx,
             "$setOnInsert": d_tx_insert},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} From: [{1}] Amount: {2} Tx Hash: {3}".format(
            d_tx["event"],
            d_tx["address"],
            d_tx["amount"],
            tx_hash))

        # update user balances
        self.update_balance_address(m_client, d_tx["address"], block_height)

        return d_tx

    def moc_exchange_risk_prox_redeem(self,
                                      tx_receipt,
                                      tx_event,
                                      m_client,
                                      block_height,
                                      block_height_current,
                                      d_moc_transactions):

        confirm_blocks = self.options['scan_moc_blocks']['confirm_blocks']
        if block_height_current - block_height > confirm_blocks:
            status = 'confirmed'
            confirmation_time = datetime.datetime.now()
        else:
            status = 'confirming'
            confirmation_time = None

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        tx_hash = Web3.toHex(tx_receipt['transactionHash'])
        moc_tx = d_moc_transactions[tx_hash]

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = tx_event.blockNumber
        d_tx["address"] = tx_event.account
        d_tx["status"] = status
        d_tx["event"] = 'RiskProxRedeem'
        d_tx["tokenInvolved"] = 'RISKPROX'
        d_tx["userAmount"] = str(Web3.fromWei(tx_event.amount, 'ether'))
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["RBTCAmount"] = str(tx_event.reserveTotal)
        usd_amount = Web3.fromWei(tx_event.reserveTotal, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))
        d_tx["amount"] = str(tx_event.amount)
        d_tx["confirmationTime"] = confirmation_time
        d_tx["leverage"] = str(tx_event.leverage)
        d_tx["rbtcCommission"] = str(tx_event.commission)
        usd_commission = Web3.fromWei(tx_event.commission, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDCommission"] = str(int(usd_commission * self.precision))
        d_tx["rbtcInterests"] = str(tx_event.interests)
        usd_interest = Web3.fromWei(tx_event.interests, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDInterests"] = str(int(usd_interest * self.precision))
        d_tx["isPositive"] = False
        d_tx["reservePrice"] = str(tx_event.reservePrice)
        gas_fee = tx_receipt['gasUsed'] * Web3.fromWei(moc_tx['gasPrice'], 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["gasFeeUSD"] = str(int(gas_fee * Web3.fromWei(tx_event.reservePrice, 'ether') * self.precision))
        rbtc_total = tx_event.reserveTotal + tx_event.interests - int(gas_fee * self.precision)
        d_tx["RBTCTotal"] = str(rbtc_total)
        usd_total = Web3.fromWei(rbtc_total, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDTotal"] = str(int(usd_total * self.precision))
        d_tx["processLogs"] = True

        d_tx_insert = OrderedDict()
        d_tx_insert["createdAt"] = datetime.datetime.now()

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "address": d_tx["address"],
             "event": d_tx["event"]},
            {"$set": d_tx,
             "$setOnInsert": d_tx_insert},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} From: [{1}] Amount: {2} Tx Hash: {3}".format(
            d_tx["event"],
            d_tx["address"],
            d_tx["amount"],
            tx_hash))

        # update user balances
        self.update_balance_address(m_client, d_tx["address"], block_height)

        return d_tx

    def moc_exchange_stable_token_mint(self,
                                       tx_receipt,
                                       tx_event,
                                       m_client,
                                       block_height,
                                       block_height_current,
                                       d_moc_transactions):

        confirm_blocks = self.options['scan_moc_blocks']['confirm_blocks']
        if block_height_current - block_height > confirm_blocks:
            status = 'confirmed'
            confirmation_time = datetime.datetime.now()
        else:
            status = 'confirming'
            confirmation_time = None

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        tx_hash = Web3.toHex(tx_receipt['transactionHash'])
        moc_tx = d_moc_transactions[tx_hash]

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = tx_event.blockNumber
        d_tx["address"] = tx_event.account
        d_tx["status"] = status
        d_tx["event"] = 'StableTokenMint'
        d_tx["tokenInvolved"] = 'STABLE'
        # WARNING something to investigate, commented think is correct
        #d_tx["userAmount"] = str(Web3.fromWei(tx_event.amount, 'ether'))
        d_tx["userAmount"] = str(Web3.fromWei(tx_event.reserveTotal, 'ether'))
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["RBTCAmount"] = str(tx_event.reserveTotal)
        usd_amount = Web3.fromWei(tx_event.reserveTotal, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))
        usd_commission = Web3.fromWei(tx_event.commission, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDCommission"] = str(int(usd_commission * self.precision))
        d_tx["amount"] = str(tx_event.amount)
        d_tx["confirmationTime"] = confirmation_time
        d_tx["isPositive"] = True
        d_tx["rbtcCommission"] = str(tx_event.commission)
        d_tx["reservePrice"] = str(tx_event.reservePrice)
        gas_fee = tx_receipt['gasUsed'] * Web3.fromWei(moc_tx['gasPrice'], 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["gasFeeUSD"] = str(int(gas_fee * Web3.fromWei(tx_event.reservePrice, 'ether') * self.precision))
        rbtc_total = tx_event.reserveTotal + tx_event.commission + int(gas_fee * self.precision)
        d_tx["RBTCTotal"] = str(rbtc_total)
        usd_total = Web3.fromWei(rbtc_total, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDTotal"] = str(int(usd_total * self.precision))
        d_tx["processLogs"] = True

        d_tx_insert = OrderedDict()
        d_tx_insert["createdAt"] = datetime.datetime.now()

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "address": d_tx["address"],
             "event": d_tx["event"]},
            {"$set": d_tx,
             "$setOnInsert": d_tx_insert},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} From: [{1}] Amount: {2} Tx Hash: {3}".format(
            d_tx["event"],
            d_tx["address"],
            d_tx["amount"],
            tx_hash))

        # update user balances
        self.update_balance_address(m_client, d_tx["address"], block_height)

        return d_tx

    def moc_exchange_stable_token_redeem(self,
                                         tx_receipt,
                                         tx_event,
                                         m_client,
                                         block_height,
                                         block_height_current,
                                         d_moc_transactions):

        confirm_blocks = self.options['scan_moc_blocks']['confirm_blocks']
        if block_height_current - block_height > confirm_blocks:
            status = 'confirmed'
            confirmation_time = datetime.datetime.now()
        else:
            status = 'confirming'
            confirmation_time = None

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        tx_hash = Web3.toHex(tx_receipt['transactionHash'])
        moc_tx = d_moc_transactions[tx_hash]

        d_tx = OrderedDict()
        d_tx["address"] = tx_event.account
        d_tx["blockNumber"] = tx_event.blockNumber
        d_tx["event"] = 'StableTokenRedeem'
        d_tx["transactionHash"] = tx_hash
        d_tx["RBTCAmount"] = str(tx_event.reserveTotal)
        usd_amount = Web3.fromWei(tx_event.reserveTotal, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))
        d_tx["amount"] = str(tx_event.amount)
        d_tx["confirmationTime"] = confirmation_time
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["status"] = status
        d_tx["tokenInvolved"] = 'STABLE'
        d_tx["rbtcCommission"] = str(tx_event.commission)
        usd_commission = Web3.fromWei(tx_event.commission, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDCommission"] = str(int(usd_commission * self.precision))
        d_tx["isPositive"] = False
        d_tx["reservePrice"] = str(tx_event.reservePrice)
        gas_fee = tx_receipt['gasUsed'] * Web3.fromWei(moc_tx['gasPrice'], 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["gasFeeUSD"] = str(int(gas_fee * Web3.fromWei(tx_event.reservePrice, 'ether') * self.precision))
        rbtc_total = tx_event.reserveTotal - int(gas_fee * self.precision)
        d_tx["RBTCTotal"] = str(rbtc_total)
        usd_total = Web3.fromWei(rbtc_total, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDTotal"] = str(int(usd_total * self.precision))
        d_tx["processLogs"] = True

        d_tx_insert = OrderedDict()
        d_tx_insert["createdAt"] = datetime.datetime.now()

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "address": d_tx["address"],
             "event": d_tx["event"]},
            {"$set": d_tx,
             "$setOnInsert": d_tx_insert},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} From: [{1}] Amount: {2} Tx Hash: {3}".format(
            d_tx["event"],
            d_tx["address"],
            d_tx["amount"],
            tx_hash))

        # update user balances
        self.update_balance_address(m_client, d_tx["address"], block_height)

        # Update the queue operation to delete
        collection_tx.remove({'address': d_tx["address"], 'event': 'QueueDOC'})

        return d_tx

    def moc_exchange_free_stable_token_redeem(self,
                                              tx_receipt,
                                              tx_event,
                                              m_client,
                                              block_height,
                                              block_height_current,
                                              d_moc_transactions):

        confirm_blocks = self.options['scan_moc_blocks']['confirm_blocks']
        if block_height_current - block_height > confirm_blocks:
            status = 'confirmed'
            confirmation_time = datetime.datetime.now()
        else:
            status = 'confirming'
            confirmation_time = None

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        tx_hash = Web3.toHex(tx_receipt['transactionHash'])
        moc_tx = d_moc_transactions[tx_hash]

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = tx_event.blockNumber
        d_tx["address"] = tx_event.account
        d_tx["status"] = status
        d_tx["event"] = 'FreeStableTokenRedeem'
        d_tx["tokenInvolved"] = 'STABLE'
        d_tx["userAmount"] = str(Web3.fromWei(tx_event.amount, 'ether'))
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["RBTCAmount"] = str(tx_event.reserveTotal)
        usd_amount = Web3.fromWei(tx_event.reserveTotal, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))
        d_tx["amount"] = str(tx_event.amount)
        d_tx["confirmationTime"] = confirmation_time
        d_tx["rbtcCommission"] = str(tx_event.commission)
        usd_commission = Web3.fromWei(tx_event.commission, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDCommission"] = str(int(usd_commission * self.precision))
        d_tx["rbtcInterests"] = str(tx_event.interests)
        usd_interest = Web3.fromWei(tx_event.interests, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDInterests"] = str(int(usd_interest * self.precision))
        d_tx["isPositive"] = False
        d_tx["reservePrice"] = str(tx_event.reservePrice)
        gas_fee = tx_receipt['gasUsed'] * Web3.fromWei(moc_tx['gasPrice'], 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["gasFeeUSD"] = str(int(gas_fee * Web3.fromWei(tx_event.reservePrice, 'ether') * self.precision))
        rbtc_total = tx_event.reserveTotal - tx_event.commission - int(gas_fee * self.precision)
        d_tx["RBTCTotal"] = str(rbtc_total)
        usd_total = Web3.fromWei(rbtc_total, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        d_tx["USDTotal"] = str(int(usd_total * self.precision))
        d_tx["processLogs"] = True

        d_tx_insert = OrderedDict()
        d_tx_insert["createdAt"] = datetime.datetime.now()

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "address": d_tx["address"],
             "event": d_tx["event"]},
            {"$set": d_tx,
             "$setOnInsert": d_tx_insert},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} From: [{1}] Amount: {2} Tx Hash: {3}".format(
            d_tx["event"],
            d_tx["address"],
            d_tx["amount"],
            tx_hash))

        # update user balances
        self.update_balance_address(m_client, d_tx["address"], block_height)

        return d_tx

    def logs_process_moc_exchange(self, tx_receipt, m_client, block_height, block_height_current, d_moc_transactions):

        if not tx_receipt['logs']:
            # return if there are no logs
            return

        network = self.connection_manager.network
        moc_addresses = self.connection_manager.options['networks'][network]['addresses']

        events = self.contract_MoC.sc_moc_exchange.events

        # RiskProMint
        tx_logs = events.RiskProMint().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            if str(tx_log['address']).lower() == str(moc_addresses['MoCExchange']).lower():
                tx_event = MoCExchangeRiskProMint(self.connection_manager, tx_log)
                self.moc_exchange_risk_pro_mint(tx_receipt,
                                                tx_event,
                                                m_client,
                                                block_height,
                                                block_height_current,
                                                d_moc_transactions)

        # RiskProRedeem
        tx_logs = events.RiskProRedeem().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            if str(tx_log['address']).lower() == str(moc_addresses['MoCExchange']).lower():
                tx_event = MoCExchangeRiskProRedeem(self.connection_manager, tx_log)
                self.moc_exchange_risk_pro_redeem(tx_receipt,
                                                  tx_event,
                                                  m_client,
                                                  block_height,
                                                  block_height_current,
                                                  d_moc_transactions)

        # RiskProxMint
        tx_logs = events.RiskProxMint().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            if str(tx_log['address']).lower() == str(moc_addresses['MoCExchange']).lower():
                tx_event = MoCExchangeRiskProxMint(self.connection_manager, tx_log)
                self.moc_exchange_risk_prox_mint(tx_receipt,
                                                 tx_event,
                                                 m_client,
                                                 block_height,
                                                 block_height_current,
                                                 d_moc_transactions)

        # RiskProxRedeem
        tx_logs = events.RiskProxRedeem().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            if str(tx_log['address']).lower() == str(moc_addresses['MoCExchange']).lower():
                tx_event = MoCExchangeRiskProxRedeem(self.connection_manager, tx_log)
                self.moc_exchange_risk_prox_redeem(tx_receipt,
                                                   tx_event,
                                                   m_client,
                                                   block_height,
                                                   block_height_current,
                                                   d_moc_transactions)

        # StableTokenMint
        tx_logs = events.StableTokenMint().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            if str(tx_log['address']).lower() == str(moc_addresses['MoCExchange']).lower():
                tx_event = MoCExchangeStableTokenMint(self.connection_manager, tx_log)
                self.moc_exchange_stable_token_mint(tx_receipt,
                                                    tx_event,
                                                    m_client,
                                                    block_height,
                                                    block_height_current,
                                                    d_moc_transactions)

        # StableTokenRedeem
        tx_logs = events.StableTokenRedeem().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            if str(tx_log['address']).lower() == str(moc_addresses['MoCExchange']).lower():
                tx_event = MoCExchangeStableTokenRedeem(self.connection_manager, tx_log)
                self.moc_exchange_stable_token_redeem(tx_receipt,
                                                      tx_event,
                                                      m_client,
                                                      block_height,
                                                      block_height_current,
                                                      d_moc_transactions)

        # FreeStableTokenRedeem
        tx_logs = events.FreeStableTokenRedeem().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            if str(tx_log['address']).lower() == str(moc_addresses['MoCExchange']).lower():
                tx_event = MoCExchangeFreeStableTokenRedeem(self.connection_manager, tx_log)
                self.moc_exchange_free_stable_token_redeem(tx_receipt,
                                                           tx_event,
                                                           m_client,
                                                           block_height,
                                                           block_height_current,
                                                           d_moc_transactions)

    def moc_settlement_redeem_request_alter(self,
                                            tx_receipt,
                                            tx_event,
                                            m_client,
                                            block_height,
                                            block_height_current,
                                            d_moc_transactions):

        confirm_blocks = self.options['scan_moc_blocks']['confirm_blocks']
        if block_height_current - block_height > confirm_blocks:
            status = 'confirmed'
            confirmation_time = datetime.datetime.now()
        else:
            status = 'confirming'
            confirmation_time = None

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        tx_hash = Web3.toHex(tx_receipt['transactionHash'])
        moc_tx = d_moc_transactions[tx_hash]

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = tx_event.blockNumber
        d_tx["address"] = tx_event.redeemer
        d_tx["status"] = status
        d_tx["event"] = 'RedeemRequestAlter'
        d_tx["tokenInvolved"] = 'STABLE'
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["amount"] = str(tx_event.delta)
        d_tx["confirmationTime"] = confirmation_time
        gas_fee = tx_receipt['gasUsed'] * Web3.fromWei(moc_tx['gasPrice'], 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["processLogs"] = True

        is_addition = tx_event.isAddition
        if isinstance(is_addition, str):
            if is_addition == 'True':
                is_addition = True
            else:
                is_addition = False

        d_tx["isPositive"] = is_addition

        d_tx_insert = OrderedDict()
        d_tx_insert["createdAt"] = datetime.datetime.now()

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "address": d_tx["address"],
             "event": d_tx["event"]},
            {"$set": d_tx,
             "$setOnInsert": d_tx_insert},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} From: [{1}] Amount: {2} Tx Hash: {3}".format(
            d_tx["event"],
            d_tx["address"],
            d_tx["amount"],
            tx_hash))

        # update user balances
        info_balance = self.update_balance_address(m_client, d_tx["address"], block_height)

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

    def moc_settlement_redeem_stable_token(self,
                                           tx_receipt,
                                           tx_event,
                                           m_client,
                                           block_height,
                                           block_height_current,
                                           d_moc_transactions):
        # update user balances
        # self.update_balance_address(m_client, d_tx["address"], block_height)
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

    def set_settlement_state(self, tx_event, m_client):
        """Event: SettlementDeleveraging"""

        # SettlementState
        collection_tx = self.mm.collection_settlement_state(m_client)

        d_tx = dict()
        d_tx["inProcess"] = False
        d_tx["startBlockNumber"] = tx_event.blockNumber
        d_tx["processLogs"] = True

        d_tx_insert = OrderedDict()
        d_tx_insert["docRedeemCount"] = 0
        d_tx_insert["deleveragingCount"] = 0
        d_tx_insert["btcxPrice"] = str(tx_event.riskProxPrice)
        d_tx_insert["btcPrice"] = str(tx_event.reservePrice)
        d_tx_insert["createdAt"] = datetime.datetime.now()

        post_id = collection_tx.find_one_and_update(
            {"startBlockNumber": tx_event.blockNumber},
            {"$set": d_tx,
             "$setOnInsert": d_tx_insert},
            upsert=True)

        d_tx['post_id'] = post_id

        log.info("Tx {0} blockNumber: [{1}] Tx Hash:".format(
            'SettlementDeleveraging',
            d_tx["startBlockNumber"]))

        return d_tx

    def update_settlement_state(self, tx_event, m_client):
        """Event: SettlementStarted"""

        # SettlementState
        collection_tx = self.mm.collection_settlement_state(m_client)

        exist_tx = collection_tx.find_one(
            {"startBlockNumber": tx_event.blockNumber}
        )

        d_tx = dict()
        d_tx["inProcess"] = True
        d_tx["startBlockNumber"] = tx_event.blockNumber
        d_tx["docRedeemCount"] = tx_event.stableTokenRedeemCount
        d_tx["deleveragingCount"] = tx_event.deleveragingCount
        #adjust_price = Web3.fromWei(tx_event.riskProxPrice, 'ether') * Web3.fromWei(tx_event.reservePrice, 'ether')
        #d_tx["btcxPrice"] = str(int(adjust_price * self.precision))
        d_tx["btcxPrice"] = str(tx_event.riskProxPrice)
        d_tx["btcPrice"] = str(tx_event.reservePrice)
        d_tx["createdAt"] = datetime.datetime.now()
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

    def moc_settlement_deleveraging(self,
                                    tx_receipt,
                                    tx_event,
                                    m_client,
                                    block_height,
                                    block_height_current,
                                    d_moc_transactions):

        confirm_blocks = self.options['scan_moc_blocks']['confirm_blocks']
        if block_height_current - block_height > confirm_blocks:
            status = 'confirmed'
            confirmation_time = datetime.datetime.now()
        else:
            status = 'confirming'
            confirmation_time = None

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        tx_hash = Web3.toHex(tx_receipt['transactionHash'])
        moc_tx = d_moc_transactions[tx_hash]

        # get all address who has bprox , at the time all users because
        # we dont know who hast bprox in certain block
        collection_users = self.mm.collection_user_state(m_client)
        users = collection_users.find()
        l_users_riskprox = list()
        for user in users:
            l_users_riskprox.append(user)
            #if float(user['bprox2Balance']) > 0.0:
            #    l_users_riskprox.append(user)

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = tx_event.blockNumber
        d_tx["event"] = 'SettlementDeleveraging'
        d_tx["tokenInvolved"] = 'RISKPROX'
        d_tx["status"] = status
        d_tx["settlement_status"] = 0
        d_tx["confirmationTime"] = confirmation_time
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = tx_receipt['gasUsed'] * Web3.fromWei(moc_tx['gasPrice'], 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["gasFeeUSD"] = str(int(gas_fee * Web3.fromWei(tx_event.reservePrice, 'ether') * self.precision))
        d_tx["processLogs"] = True

        d_tx_insert = OrderedDict()
        d_tx_insert["createdAt"] = datetime.datetime.now()

        riskprox_price = Web3.fromWei(tx_event.riskProxPrice, 'ether')
        reserve_price = Web3.fromWei(tx_event.reservePrice, 'ether')

        start_block_number = tx_event.startBlockNumber
        prior_block_to_deleveraging = start_block_number - 1
        l_transactions = list()
        for user_riskprox in l_users_riskprox:
            d_user_balances = self.riskprox_balances_from_address(user_riskprox["address"],
                                                                  prior_block_to_deleveraging)
            if float(d_user_balances["bprox2Balance"]) > 0.0:
                d_tx["address"] = user_riskprox["address"]
                d_tx["amount"] = str(d_user_balances["bprox2Balance"])
                d_tx["USDAmount"] = str(riskprox_price * reserve_price * int(d_user_balances["bprox2Balance"]))
                rbtc_amount = riskprox_price * int(d_user_balances["bprox2Balance"])
                d_tx["RBTCAmount"] = str(rbtc_amount)
                d_tx["reservePrice_deleveraging"] = str(reserve_price)
                rbtc_total = rbtc_amount - int(gas_fee * self.precision)
                d_tx["RBTCTotal"] = str(rbtc_total)
                usd_total = Web3.fromWei(rbtc_total, 'ether') * reserve_price
                d_tx["USDTotal"] = str(int(usd_total * self.precision))

                post_id = collection_tx.find_one_and_update(
                    {"transactionHash": tx_hash,
                     "address": d_tx["address"],
                     "event": d_tx["event"]},
                    {"$set": d_tx,
                     "$setOnInsert": d_tx_insert},
                    upsert=True)

                log.info("Tx {0} From: [{1}] Amount: {2} Tx Hash: {3}".format(
                    d_tx["event"],
                    d_tx["address"],
                    d_tx["amount"],
                    tx_hash))

                # update user balances
                self.update_balance_address(m_client, d_tx["address"], block_height)

                l_transactions.append(d_tx)

        return l_transactions

    def moc_settlement_redeem_request_processed(self,
                                                tx_receipt,
                                                tx_event,
                                                m_client,
                                                block_height,
                                                block_height_current,
                                                d_moc_transactions):

        confirm_blocks = self.options['scan_moc_blocks']['confirm_blocks']
        if block_height_current - block_height > confirm_blocks:
            status = 'confirmed'
            confirmation_time = datetime.datetime.now()
        else:
            status = 'confirming'
            confirmation_time = None

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        tx_hash = Web3.toHex(tx_receipt['transactionHash'])
        moc_tx = d_moc_transactions[tx_hash]

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = tx_event.blockNumber
        d_tx["address"] = tx_event.redeemer
        d_tx["status"] = status
        d_tx["event"] = 'RedeemRequestProcessed'
        d_tx["tokenInvolved"] = 'STABLE'
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["amount"] = str(tx_event.amount)
        d_tx["confirmationTime"] = confirmation_time
        d_tx["isPositive"] = False
        gas_fee = tx_receipt['gasUsed'] * Web3.fromWei(moc_tx['gasPrice'], 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["processLogs"] = True

        d_tx_insert = OrderedDict()
        d_tx_insert["createdAt"] = datetime.datetime.now()

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "address": d_tx["address"],
             "event": d_tx["event"]},
            {"$set": d_tx,
             "$setOnInsert": d_tx_insert},
            upsert=True)
        d_tx['post_id'] = post_id

        log.info("Tx {0} From: [{1}] Amount: {2} Tx Hash: {3}".format(
            d_tx["event"],
            d_tx["address"],
            d_tx["amount"],
            tx_hash))

        # update user balances
        self.update_balance_address(m_client, d_tx["address"], block_height)

        return d_tx

    def moc_settlement_started(self, tx_receipt, tx_event, m_client):
        pass

    def moc_settlement_completed(self, tx_receipt, tx_event, m_client, block_height):
        # on settlement completed, remove alter queue cause
        # redeem of doc on settlement already ocurrs

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        # remove all RedeemRequestAlter
        collection_tx.remove({"event": "RedeemRequestAlter", "blockHeight": {"$lte": block_height}})

        # also delete with created at < 31 days
        old_records = datetime.datetime.now() - datetime.timedelta(days=31)
        collection_tx.remove({"event": "RedeemRequestAlter", "createdAt": {"$lte": old_records}})

    def logs_process_moc_settlement(self, tx_receipt, m_client, block_height, block_height_current, d_moc_transactions):

        if not tx_receipt['logs']:
            # return if there are no logs
            return

        network = self.connection_manager.network
        moc_addresses = self.connection_manager.options['networks'][network]['addresses']

        events = self.contract_MoC.sc_moc_settlement.events

        # SettlementStarted
        tx_logs = events.SettlementStarted().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            if str(tx_log['address']).lower() == str(moc_addresses['MoCSettlement']).lower():
                tx_event = MoCSettlementSettlementStarted(self.connection_manager, tx_log)
                self.moc_settlement_started(tx_receipt, tx_event, m_client)
                self.update_settlement_state(tx_event, m_client)

        # RedeemRequestAlter
        tx_logs = events.RedeemRequestAlter().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            if str(tx_log['address']).lower() == str(moc_addresses['MoCSettlement']).lower():
                tx_event = MoCSettlementRedeemRequestAlter(self.connection_manager, tx_log)
                self.moc_settlement_redeem_request_alter(tx_receipt,
                                                         tx_event,
                                                         m_client,
                                                         block_height,
                                                         block_height_current,
                                                         d_moc_transactions)

        # RedeemRequestProcessed
        tx_logs = events.RedeemRequestProcessed().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            if str(tx_log['address']).lower() == str(moc_addresses['MoCSettlement']).lower():
                tx_event = MoCSettlementRedeemRequestProcessed(self.connection_manager, tx_log)
                self.moc_settlement_redeem_request_processed(tx_receipt,
                                                             tx_event,
                                                             m_client,
                                                             block_height,
                                                             block_height_current,
                                                             d_moc_transactions)

        # SettlementRedeemStableToken
        tx_logs = events.SettlementRedeemStableToken().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            if str(tx_log['address']).lower() == str(moc_addresses['MoCSettlement']).lower():
                tx_event = MoCSettlementSettlementRedeemStableToken(self.connection_manager, tx_log)
                self.moc_settlement_redeem_stable_token(tx_receipt,
                                                        tx_event,
                                                        m_client,
                                                        block_height,
                                                        block_height_current,
                                                        d_moc_transactions)
                self.moc_settlement_redeem_stable_token_notification(tx_receipt, tx_event, tx_log, m_client)

        # SettlementDeleveraging
        tx_logs = events.SettlementDeleveraging().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            if str(tx_log['address']).lower() == str(moc_addresses['MoCSettlement']).lower():
                tx_event = MoCSettlementSettlementDeleveraging(self.connection_manager, tx_log)
                self.moc_settlement_deleveraging(tx_receipt,
                                                 tx_event,
                                                 m_client,
                                                 block_height,
                                                 block_height_current,
                                                 d_moc_transactions)
                self.set_settlement_state(tx_event, m_client)

        # SettlementCompleted
        tx_logs = events.SettlementCompleted().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            if str(tx_log['address']).lower() == str(moc_addresses['MoCSettlement']).lower():
                tx_event = MoCSettlementSettlementCompleted(self.connection_manager, tx_log)
                self.moc_settlement_completed(tx_receipt,
                                              tx_event,
                                              m_client,
                                              block_height)

    def moc_inrate_daily_pay(self, tx_receipt, tx_event, m_client):

        collection_inrate = self.mm.collection_inrate_income(m_client)

        exist_tx = collection_inrate.find_one(
            {"blockHeight": tx_event.blockNumber}
        )
        if exist_tx:
            log.warning("Event [Inrate Daily Pay] already exist for blockNumber: [{0}] Not Writting...".format(
                tx_event.blockNumber))
            return

        d_tx = OrderedDict()
        d_tx["blockHeight"] = tx_event.blockNumber
        d_tx["ratePayAmount"] = str(tx_event.amount)
        d_tx["nBTCBitProOfBucketZero"] = str(tx_event.nReserveBucketC0)
        d_tx["timestamp"] = tx_event.timestamp
        d_tx["processLogs"] = True

        d_tx_insert = OrderedDict()
        d_tx_insert["createdAt"] = datetime.datetime.now()

        post_id = collection_inrate.find_one_and_update(
            {"blockHeight": tx_event.blockNumber},
            {"$set": d_tx,
             "$setOnInsert": d_tx_insert},
            upsert=True)

        log.info("Event Inrate Daily Pay - Blockheight: [{0}] ratePayAmount: {1}".format(
            d_tx["blockHeight"],
            d_tx["ratePayAmount"],
            ))

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
        d_tx["processLogs"] = True

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash, "event": event_name, "logIndex": log_index},
            {"$set": d_tx},
            upsert=True)
        d_tx['post_id'] = post_id

        return d_tx

    def moc_inrate_risk_pro_holders_interest_pay(self, tx_receipt, tx_event, m_client):

        collection_inrate = self.mm.collection_bitpro_holders_interest(m_client)

        exist_tx = collection_inrate.find_one(
            {"blockHeight": tx_event.blockNumber}
        )
        if exist_tx:
            log.warning("Event [RiskPro Holders Interest Pay] already exist for blockNumber: [{0}] Not Writting...".format(
                tx_event.blockNumber))
            return

        d_tx = OrderedDict()
        d_tx["blockHeight"] = tx_event.blockNumber
        d_tx["amount"] = str(tx_event.amount)
        d_tx["nBtcBucketC0BeforePay"] = str(tx_event.nReserveBucketC0BeforePay)
        d_tx["timestamp"] = tx_event.timestamp
        d_tx["processLogs"] = True

        d_tx_insert = OrderedDict()
        d_tx_insert["createdAt"] = datetime.datetime.now()

        post_id = collection_inrate.find_one_and_update(
            {"blockHeight": tx_event.blockNumber},
            {"$set": d_tx,
             "$setOnInsert": d_tx_insert},
            upsert=True)

        log.info("Event RiskPro Holders Interest Pay - Blockheight: [{0}] amount: {1}".format(
            d_tx["blockHeight"],
            d_tx["amount"],
            ))

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
        d_tx["processLogs"] = True

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash, "event": event_name, "logIndex": log_index},
            {"$set": d_tx},
            upsert=True)

        d_tx['post_id'] = post_id

        return d_tx

    def logs_process_moc_inrate(self, tx_receipt, m_client):

        if not tx_receipt['logs']:
            # return if there are no logs
            return

        network = self.connection_manager.network
        moc_addresses = self.connection_manager.options['networks'][network]['addresses']

        events = self.contract_MoC.sc_moc_inrate.events

        # InrateDailyPay
        tx_logs = events.InrateDailyPay().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            if str(tx_log['address']).lower() == str(moc_addresses['MoCInrate']).lower():
                tx_event = MoCInrateDailyPay(self.connection_manager, tx_log)
                self.moc_inrate_daily_pay(tx_receipt, tx_event, m_client)
                self.moc_inrate_daily_pay_notification(tx_receipt, tx_event, tx_log, m_client)

        # RiskProHoldersInterestPay
        tx_logs = events.RiskProHoldersInterestPay().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            if str(tx_log['address']).lower() == str(moc_addresses['MoCInrate']).lower():
                tx_event = MoCInrateRiskProHoldersInterestPay(self.connection_manager, tx_log)
                self.moc_inrate_risk_pro_holders_interest_pay(tx_receipt, tx_event, m_client)
                self.moc_inrate_risk_pro_holders_interest_pay_notification(tx_receipt, tx_log, tx_event, m_client)

    def moc_bucket_liquidation(self,
                               tx_receipt,
                               tx_event,
                               m_client,
                               block_height,
                               block_height_current,
                               d_moc_transactions):

        confirm_blocks = self.options['scan_moc_blocks']['confirm_blocks']
        if block_height_current - block_height > confirm_blocks:
            status = 'confirmed'
            confirmation_time = datetime.datetime.now()
        else:
            status = 'confirming'
            confirmation_time = None

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        tx_hash = Web3.toHex(tx_receipt['transactionHash'])
        moc_tx = d_moc_transactions[tx_hash]

        # get all address who has bprox , at the time all users because
        # we dont know who hast bprox in certain block
        collection_users = self.mm.collection_user_state(m_client)
        users = collection_users.find()
        l_users_riskprox = list()
        for user in users:
            l_users_riskprox.append(user)
            # if float(user['bprox2Balance']) > 0.0:
            #    l_users_riskprox.append(user)

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = tx_event.blockNumber
        d_tx["event"] = 'BucketLiquidation'
        d_tx["tokenInvolved"] = 'RISKPROX'
        d_tx["bucket"] = 'X2'
        d_tx["status"] = status
        d_tx["confirmationTime"] = confirmation_time
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = tx_receipt['gasUsed'] * Web3.fromWei(moc_tx['gasPrice'], 'ether')
        d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["processLogs"] = True

        d_tx_insert = OrderedDict()
        d_tx_insert["createdAt"] = datetime.datetime.now()

        prior_block_to_liquidation = tx_event.blockNumber - 1
        l_transactions = list()
        for user_riskprox in l_users_riskprox:
            d_user_balances = self.riskprox_balances_from_address(user_riskprox["address"],
                                                                  prior_block_to_liquidation)
            if float(d_user_balances["bprox2Balance"]) > 0.0:
                d_tx["address"] = user_riskprox["address"]
                d_tx["amount"] = str(d_user_balances["bprox2Balance"])

                post_id = collection_tx.find_one_and_update(
                    {"transactionHash": tx_hash,
                     "address": d_tx["address"],
                     "event": d_tx["event"]},
                    {"$set": d_tx,
                     "$setOnInsert": d_tx_insert},
                    upsert=True)

                log.info("Tx {0} From: [{1}] Amount: {2} Tx Hash: {3}".format(
                    d_tx["event"],
                    d_tx["address"],
                    d_tx["amount"],
                    tx_hash))

                # update user balances
                self.update_balance_address(m_client, d_tx["address"], block_height)

                l_transactions.append(d_tx)

    def moc_bucket_liquidation_notification(self, tx_receipt, tx_event, tx_log, m_client):

        collection_tx = self.mm.collection_notification(m_client)
        tx_hash = Web3.toHex(tx_receipt['transactionHash'])
        event_name = 'BucketLiquidation'
        log_index = tx_log['logIndex']

        d_tx = OrderedDict()
        d_tx["event"] = event_name
        d_tx["transactionHash"] = tx_hash
        d_tx["logIndex"] = log_index
        d_tx["bucket"] = 'X2'
        d_tx["timestamp"] = tx_event.timestamp
        d_tx["processLogs"] = True

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash, "event": event_name, "logIndex": log_index},
            {"$set": d_tx},
            upsert=True)

        d_tx['post_id'] = post_id

        return d_tx

    def logs_process_moc(self,
                         tx_receipt,
                         m_client,
                         block_height,
                         block_height_current,
                         d_moc_transactions):

        if not tx_receipt['logs']:
            # return if there are no logs
            return

        network = self.connection_manager.network
        moc_addresses = self.connection_manager.options['networks'][network]['addresses']

        events = self.contract_MoC.events

        # BucketLiquidation
        tx_logs = events.BucketLiquidation().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            if str(tx_log['address']).lower() == str(moc_addresses['MoC']).lower():
                tx_event = MoCBucketLiquidation(self.connection_manager, tx_log)
                self.moc_bucket_liquidation(tx_receipt,
                                            tx_event,
                                            m_client,
                                            block_height,
                                            block_height_current,
                                            d_moc_transactions)
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
        d_tx["processLogs"] = True

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash, "event": event_name, "logIndex": log_index},
            {"$set": d_tx},
            upsert=True)

        d_tx['post_id'] = post_id

        return d_tx

    def logs_process_moc_state(self, tx_receipt, m_client):

        if not tx_receipt['logs']:
            # return if there are no logs
            return

        network = self.connection_manager.network
        moc_addresses = self.connection_manager.options['networks'][network]['addresses']

        events = self.contract_MoC.sc_moc_state.events

        # StateTransition
        tx_logs = events.StateTransition().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            if str(tx_log['address']).lower() == str(moc_addresses['MoCState']).lower():
                tx_event = MoCStateStateTransition(self.connection_manager, tx_log)
                self.moc_state_transition(tx_receipt, tx_event, m_client)
                self.moc_state_transition_notification(tx_receipt, tx_event, tx_log, m_client)

    def tx_token_transfer(self,
                          tx_receipt,
                          tx_event,
                          m_client,
                          block_height,
                          block_height_current,
                          token_involved='RISKPRO'):

        confirm_blocks = self.options['scan_moc_blocks']['confirm_blocks']
        if block_height_current - block_height > confirm_blocks:
            status = 'confirmed'
            confirmation_time = datetime.datetime.now()
        else:
            status = 'confirming'
            confirmation_time = None

        network = self.connection_manager.network
        address_from_contract = '0x0000000000000000000000000000000000000000'
        addresses_moc = self.connection_manager.options['networks'][network]['addresses']['MoC']
        address_not_allowed = [str.lower(address_from_contract), str.lower(addresses_moc)]
        if str.lower(tx_event.e_from) in address_not_allowed or \
                str.lower(tx_event.e_to) in address_not_allowed:

            # Transfer from our Contract we dont add because already done
            # with ...Mint
            #if self.debug_mode:
            #    log.info("Token transfer not processed! From: [{0}] To [{1}]".format(
            #        tx_event.e_from, tx_event.e_to))
            return

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        tx_hash = Web3.toHex(tx_receipt['transactionHash'])

        collection_users = self.mm.collection_users(m_client)

        exist_user = collection_users.find_one(
            {"username": tx_event.e_from}
        )

        if exist_user:

            # FROM
            d_tx = OrderedDict()
            d_tx["address"] = tx_event.e_from
            d_tx["blockNumber"] = tx_event.blockNumber
            d_tx["event"] = 'Transfer'
            d_tx["transactionHash"] = tx_hash
            d_tx["amount"] = str(tx_event.value)
            d_tx["confirmationTime"] = confirmation_time
            d_tx["isPositive"] = False
            d_tx["lastUpdatedAt"] = datetime.datetime.now()
            d_tx["otherAddress"] = tx_event.e_to
            d_tx["status"] = status
            d_tx["tokenInvolved"] = token_involved
            d_tx["processLogs"] = True

            d_tx_insert = OrderedDict()
            d_tx_insert["createdAt"] = datetime.datetime.now()

            post_id = collection_tx.find_one_and_update(
                {"transactionHash": tx_hash,
                 "address": d_tx["address"],
                 "event": d_tx["event"]},
                {"$set": d_tx,
                 "$setOnInsert": d_tx_insert},
                upsert=True)

            self.update_balance_address(m_client, d_tx["address"], block_height)

            log.info("Tx Transfer {0} From: [{1}] To: [{2}] Amount: {3}".format(
                token_involved,
                tx_event.e_from,
                tx_event.e_to,
                tx_event.value))

        exist_user = collection_users.find_one(
            {"username": tx_event.e_to}
        )

        if exist_user:
            # TO
            d_tx = OrderedDict()
            d_tx["address"] = tx_event.e_to
            d_tx["blockNumber"] = tx_event.blockNumber
            d_tx["event"] = 'Transfer'
            d_tx["transactionHash"] = tx_hash
            d_tx["amount"] = str(tx_event.value)
            d_tx["confirmationTime"] = confirmation_time
            d_tx["isPositive"] = True
            d_tx["lastUpdatedAt"] = datetime.datetime.now()
            d_tx["otherAddress"] = tx_event.e_from
            d_tx["status"] = status
            d_tx["tokenInvolved"] = token_involved
            d_tx["processLogs"] = True

            d_tx_insert = OrderedDict()
            d_tx_insert["createdAt"] = datetime.datetime.now()

            post_id = collection_tx.find_one_and_update(
                {"transactionHash": tx_hash,
                 "address": d_tx["address"],
                 "event": d_tx["event"]},
                {"$set": d_tx,
                 "$setOnInsert": d_tx_insert},
                upsert=True)

            self.update_balance_address(m_client, d_tx["address"], block_height)

            log.info("Tx Transfer {0} From: [{1}] To: [{2}] Amount: {3}".format(
                token_involved,
                tx_event.e_from,
                tx_event.e_to,
                tx_event.value))

    def tx_token_transfer_reserve(self,
                                  tx_receipt,
                                  tx_event,
                                  m_client,
                                  block_height,
                                  block_height_current):

        """ Only update balance on transfer reserve"""

        network = self.connection_manager.network
        address_from_contract = '0x0000000000000000000000000000000000000000'
        addresses_moc = self.connection_manager.options['networks'][network]['addresses']['MoC']
        address_not_allowed = [str.lower(address_from_contract), str.lower(addresses_moc)]
        if str.lower(tx_event.e_from) in address_not_allowed or \
                str.lower(tx_event.e_to) in address_not_allowed:
            # Transfer from our Contract we dont add because already done
            # with ...Mint
            # if self.debug_mode:
            #    log.info("Token transfer not processed! From: [{0}] To [{1}]".format(
            #        tx_event.e_from, tx_event.e_to))
            return

        collection_users = self.mm.collection_users(m_client)

        exist_user = collection_users.find_one(
            {"username": tx_event.e_from}
        )

        if exist_user:
            self.update_balance_address(m_client, tx_event.e_from, block_height)

            log.info("Tx Transfer {0} From: [{1}] To: [{2}] Amount: {3}".format(
                'RESERVE',
                tx_event.e_from,
                tx_event.e_to,
                tx_event.value))

        exist_user = collection_users.find_one(
            {"username": tx_event.e_to}
        )

        if exist_user:
            self.update_balance_address(m_client, tx_event.e_to, block_height)

            log.info("Tx Transfer {0} From: [{1}] To: [{2}] Amount: {3}".format(
                'RESERVE',
                tx_event.e_from,
                tx_event.e_to,
                tx_event.value))

    def logs_process_transfer(self, tx_receipt, m_client, block_height, block_height_current):
        """ Process events transfers only from our tokens"""

        if not tx_receipt['logs']:
            # return if there are no logs
            return

        network = self.connection_manager.network
        moc_addresses = self.connection_manager.options['networks'][network]['addresses']
        token_riskpro = moc_addresses['BProToken']
        token_stable = moc_addresses['DoCToken']

        # RiskProToken Transfer
        tx_logs = self.contract_RiskProToken.events.Transfer().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            if str(tx_log['address']).lower() == str(token_riskpro).lower():
                tx_event = ERC20Transfer(self.connection_manager, tx_log)
                self.tx_token_transfer(tx_receipt,
                                       tx_event,
                                       m_client,
                                       block_height,
                                       block_height_current,
                                       token_involved='RISKPRO')

        # StableToken Transfer
        tx_logs = self.contract_StableToken.events.Transfer().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            if str(tx_log['address']).lower() == str(token_stable).lower():
                tx_event = ERC20Transfer(self.connection_manager, tx_log)
                self.tx_token_transfer(tx_receipt,
                                       tx_event,
                                       m_client,
                                       block_height,
                                       block_height_current,
                                       token_involved='STABLE')

        # Reserve
        if self.app_mode == 'RRC20':
            # To update balance
            token_reserve = moc_addresses['ReserveToken']
            tx_logs = self.contract_ReserveToken.events.Transfer().processReceipt(tx_receipt, errors=DISCARD)
            for tx_log in tx_logs:
                if str(tx_log['address']).lower() == str(token_reserve).lower():
                    tx_event = ERC20Transfer(self.connection_manager, tx_log)
                    self.tx_token_transfer_reserve(tx_receipt,
                                                   tx_event,
                                                   m_client,
                                                   block_height,
                                                   block_height_current)

    # def logs_moc_transactions_receipts(self, tx_receipts, m_client, block_height, block_height_current):
    #     """ To speed it up we only accept from moc contract addressess"""
    #     network = self.connection_manager.network
    #     moc_addresses = self.connection_manager.options['networks'][network]['addresses']
    #
    #     for tx_receipt in tx_receipts:
    #         if not tx_receipt['logs']:
    #             continue
    #         for tx_log in tx_receipt['logs']:
    #             tx_logs_address = str.lower(tx_log['address'])
    #             if tx_logs_address in [str.lower(moc_addresses['MoCExchange']),
    #                                    str.lower(moc_addresses['MoCSettlement']),
    #                                    str.lower(moc_addresses['MoCInrate']),
    #                                    str.lower(moc_addresses['MoC']),
    #                                    str.lower(moc_addresses['MoCState'])]:
    #
    #                 self.logs_process_moc_exchange(tx_receipt, m_client, block_height, block_height_current)
    #                 self.logs_process_moc_settlement(tx_receipt, m_client, block_height, block_height_current)
    #                 self.logs_process_moc_inrate(tx_receipt, m_client)
    #                 self.logs_process_moc(tx_receipt, m_client)
    #                 self.logs_process_moc_state(tx_receipt, m_client)

    def update_user_state_reserve(self, user_address, m_client, block_identifier: BlockIdentifier = 'latest'):

        user_state = self.mm.collection_user_state(m_client)
        exist_user = user_state.find_one(
            {"address": user_address}
        )
        if exist_user:

            d_user_balance = OrderedDict()
            d_user_balance["reserveAllowance"] = str(self.contract_MoC.reserve_allowance(
                user_address,
                formatted=False,
                block_identifier=block_identifier))
            d_user_balance["spendableBalance"] = str(self.contract_MoC.spendable_balance(
                user_address,
                formatted=False,
                block_identifier=block_identifier))

            post_id = user_state.find_one_and_update(
                {"address": user_address},
                {"$set": d_user_balance}
            )
            if self.debug_mode:
                log.info("Update user approval: [{0}] -> {1} -> Mongo _id: {2}".format(
                    user_address,
                    d_user_balance,
                    post_id))

    def update_user_state_approval(self, tx_event, m_client):

        network = self.connection_manager.network
        moc_addresses = self.connection_manager.options['networks'][network]['addresses']

        user_address = tx_event.owner
        contract_address = tx_event.spender
        block_identifier = tx_event.blockNumber

        if str.lower(contract_address) not in [str.lower(moc_addresses['MoC'])]:
            # Approval is not from our contract
            return

        self.update_user_state_reserve(user_address, m_client, block_identifier=block_identifier)

    def logs_process_reserve_approval(self, tx_receipt, m_client):

        if not tx_receipt['logs']:
            # return if there are no logs
            return

        network = self.connection_manager.network
        moc_addresses = self.connection_manager.options['networks'][network]['addresses']

        events = self.contract_ReserveToken.events

        # Approval
        tx_logs = events.Approval().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            if str(tx_log['address']).lower() == str(moc_addresses['ReserveToken']).lower():
                tx_event = ERC20Approval(self.connection_manager, tx_log)
                self.update_user_state_approval(tx_event, m_client)

    def logs_process_transfer_from_reserve(self, tx_receipt, m_client, block_height, block_height_current):

        if not tx_receipt['logs']:
            # return if there are no logs
            return

        network = self.connection_manager.network
        moc_addresses = self.connection_manager.options['networks'][network]['addresses']

        # To update balance
        token_reserve = moc_addresses['ReserveToken']
        tx_logs = self.contract_ReserveToken.events.Transfer().processReceipt(tx_receipt, errors=DISCARD)
        for tx_log in tx_logs:
            if str(tx_log['address']).lower() == str(token_reserve).lower():
                tx_event = ERC20Transfer(self.connection_manager, tx_log)
                self.process_transfer_from_moc_reserve(tx_receipt,
                                                       tx_event,
                                                       m_client,
                                                       block_height,
                                                       block_height_current)

    def update_balance_address(self, m_client, account_address, block_height):

        # get collection user state from mongo
        collection_user_state = self.mm.collection_user_state(m_client)

        user_state = collection_user_state.find_one(
            {"address": account_address}
        )
        if user_state:
            if 'block_height' in user_state:
                if user_state['block_height'] >= block_height:
                    # not process if already have updated in this block
                    return

        # get all functions state from smart contract
        d_user_balance = self.balances_from_address(account_address, block_height)
        d_user_balance['block_height'] = block_height

        if not user_state:
            # if the user not exist in the database created but default info
            d_user_balance["prefLanguage"] = 'en'
            d_user_balance["createdAt"] = datetime.datetime.now()
            d_user_balance["lastNotificationCheckAt"] = datetime.datetime.now()
            d_user_balance["showTermsAndConditions"] = True
            d_user_balance["showTutorialNoMore"] = False
            d_user_balance["createdBlockHeight"] = block_height

        # update or insert
        post_id = collection_user_state.find_one_and_update(
            {"address": account_address},
            {"$set": d_user_balance},
            upsert=True)
        d_user_balance['post_id'] = post_id

        log.info("[UPDATE USERSTATE]: [{0}] BLOCKHEIGHT: [{1}] ".format(
            account_address,
            block_height))

        return d_user_balance

    def scan_moc_state(self):

        config_block_height = self.options['scan_moc_state']['block_height']

        # conect to mongo db
        m_client = self.mm.connect()

        # get last block from node
        last_block = self.connection_manager.block_number

        block_height = config_block_height
        if block_height <= 0:
            block_height = last_block

        if self.debug_mode:
            log.info("Starting to index MoC State on block height: {0}".format(block_height))

        # get collection moc_state from mongo
        collection_moc_state = self.mm.collection_moc_state(m_client)

        exist_moc_state = collection_moc_state.find_one({"lastUpdateHeight": block_height})
        if exist_moc_state:
            if self.debug_mode:
                log.info("Not time to run moc state, already exist")
            return

        start_time = time.time()

        # get all functions from smart contract
        d_moc_state = self.moc_state_from_sc(block_identifier=block_height)
        if not d_moc_state:
            return

        # price variation
        old_block_height = last_block - d_moc_state['dayBlockSpan']

        # get last price written in mongo
        collection_price = self.mm.collection_price(m_client)
        daily_last_price = collection_price.find_one(filter={"blockHeight": {"$lt": old_block_height}},
                                                     sort=[("blockHeight", -1)])

        # price variation on settlement day
        d_moc_state["isDailyVariation"] = True
        if d_moc_state["blockSpan"] - d_moc_state['blocksToSettlement'] <= d_moc_state['dayBlockSpan']:
            # Price Variation is built in-app and not retrieved from blockchain.
            # For leveraged coin, variation must be against the BTC price
            # stated at the last settlement period.

            collection_settlement = self.mm.collection_settlement_state(m_client)

            last_settlement = collection_settlement.find_one(
                {},
                sort=[("startBlockNumber", -1)]
            )
            if last_settlement:
                daily_last_price['bprox2PriceInUsd'] = last_settlement['btcPrice']
                d_moc_state["isDailyVariation"] = False

        d_moc_state["lastUpdateHeight"] = block_height
        d_price_variation = dict()
        d_price_variation['daily'] = daily_last_price
        d_moc_state["priceVariation"] = d_price_variation

        # update or insert the new info on mocstate
        collection_moc_state.find_one_and_update(
            {},
            {"$set": d_moc_state},
            upsert=True)

        # history
        collection_moc_state_history = self.mm.collection_moc_state_history(m_client)
        collection_moc_state_history.find_one_and_update(
            {"blockHeight": block_height},
            {"$set": d_moc_state},
            upsert=True)

        duration = time.time() - start_time
        log.info("[SCAN MOC STATE] BLOCKHEIGHT: [{0}] Done in {1} seconds.".format(block_height, duration))

    def process_transfer_from_moc_reserve(self,
                                          tx_receipt,
                                          tx_event,
                                          m_client,
                                          block_height,
                                          block_height_current):

        """ TX Tranfer from MOC Reserve """

        confirm_blocks = self.options['scan_moc_blocks']['confirm_blocks']
        if block_height_current - block_height > confirm_blocks:
            status = 'confirmed'
            confirmation_time = datetime.datetime.now()
        else:
            status = 'confirming'
            confirmation_time = None

        network = self.connection_manager.network
        moc_addresses = self.connection_manager.options['networks'][network]['addresses']

        if str.lower(tx_event.e_from) not in [str.lower(moc_addresses['MoC'])]:
            # If is not from our contract return
            return

        tx_hash = Web3.toHex(tx_receipt['transactionHash'])

        if tx_event.value <= 0:
            return

        if self.contract_MoC.project == 'RDoC':
            reserve_symbol = 'RIF'
        elif self.contract_MoC.project == 'MoC':
            reserve_symbol = 'RBTC'
        else:
            reserve_symbol = 'RBTC'

        # get last price written in mongo
        collection_price = self.mm.collection_price(m_client)
        last_price = collection_price.find_one(filter={"blockHeight": {"$lt": tx_event.blockNumber}},
                                               sort=[("blockHeight", -1)])

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        # FROM
        d_tx = OrderedDict()
        d_tx["address"] = tx_event.e_to
        d_tx["blockNumber"] = tx_event.blockNumber
        d_tx["event"] = 'TransferFromMoC'
        d_tx["transactionHash"] = tx_hash
        d_tx["amount"] = str(tx_event.value)
        d_tx["confirmationTime"] = confirmation_time
        d_tx["isPositive"] = False
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["status"] = status
        d_tx["reserveSymbol"] = reserve_symbol
        d_tx["processLogs"] = True

        usd_amount = Web3.fromWei(tx_event.value, 'ether') * Web3.fromWei(last_price['bitcoinPrice'], 'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))

        d_tx_insert = OrderedDict()
        d_tx_insert["createdAt"] = datetime.datetime.now()

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "address": d_tx["address"],
             "event": d_tx["event"]},
            {"$set": d_tx,
             "$setOnInsert": d_tx_insert},
            upsert=True)

        # update user balances
        self.update_balance_address(m_client, d_tx["address"], block_height)

    def process_transfer_from_moc(self, tx_receipt, d_moc_transactions, m_client, block_height, block_height_current):
        """ Process transfer from moc """

        confirm_blocks = self.options['scan_moc_blocks']['confirm_blocks']
        if block_height_current - block_height > confirm_blocks:
            status = 'confirmed'
            confirmation_time = datetime.datetime.now()
        else:
            status = 'confirming'
            confirmation_time = None

        network = self.connection_manager.network
        moc_addresses = self.connection_manager.options['networks'][network]['addresses']

        if str.lower(tx_receipt['from']) not in [str.lower(moc_addresses['MoC'])]:
            # If is not from our contract return
            return

        tx_hash = Web3.toHex(tx_receipt['transactionHash'])
        moc_tx = d_moc_transactions[tx_hash]

        if moc_tx['value'] <= 0:
            return

        if self.contract_MoC.project == 'RDoC':
            reserve_symbol = 'RIF'
        elif self.contract_MoC.project == 'MoC':
            reserve_symbol = 'RBTC'
        else:
            reserve_symbol = 'RBTC'

        # get last price written in mongo
        collection_price = self.mm.collection_price(m_client)
        last_price = collection_price.find_one(filter={"blockHeight": {"$lt": moc_tx['blockNumber']}},
                                               sort=[("blockHeight", -1)])

        # get collection transaction
        collection_tx = self.mm.collection_transaction(m_client)

        # FROM
        d_tx = OrderedDict()
        d_tx["address"] = moc_tx['to']
        d_tx["blockNumber"] = moc_tx['blockNumber']
        d_tx["event"] = 'TransferFromMoC'
        d_tx["transactionHash"] = tx_hash
        d_tx["amount"] = str(moc_tx['value'])
        d_tx["confirmationTime"] = confirmation_time
        d_tx["isPositive"] = False
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["status"] = status
        d_tx["reserveSymbol"] = reserve_symbol
        d_tx["processLogs"] = True

        usd_amount = Web3.fromWei(moc_tx['value'], 'ether') * Web3.fromWei(last_price['bitcoinPrice'], 'ether')
        d_tx["USDAmount"] = str(int(usd_amount * self.precision))

        d_tx_insert = OrderedDict()
        d_tx_insert["createdAt"] = datetime.datetime.now()

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "address": d_tx["address"],
             "event": d_tx["event"]},
            {"$set": d_tx,
             "$setOnInsert": d_tx_insert},
            upsert=True)

        # update user balances
        self.update_balance_address(m_client, d_tx["address"], block_height)

    def scan_moc_block(self, current_block, block_reference, m_client, scan_transfer=True):

        if self.debug_mode:
            log.info("Starting to scan MOC transactions block height: [{0}] last block height: [{1}]".format(
                current_block, block_reference))

        # get moc contracts adressess
        moc_addresses = self.moc_contract_addresses()

        # get block and full transactions
        f_block = self.connection_manager.get_block(current_block, full_transactions=True)
        all_transactions = f_block['transactions']

        # From MOC Contract transactions
        moc_transactions, d_moc_transactions = self.filter_transactions(all_transactions, moc_addresses)

        # get transactions receipts
        moc_transactions_receipts = self.transactions_receipt(moc_transactions)

        # process only MoC contract transactions
        for tx_receipt in moc_transactions_receipts:

            # MOC Events process
            self.logs_process_moc_exchange(tx_receipt,
                                           m_client,
                                           current_block,
                                           block_reference,
                                           d_moc_transactions)
            self.logs_process_moc_settlement(tx_receipt,
                                             m_client,
                                             current_block,
                                             block_reference,
                                             d_moc_transactions)
            self.logs_process_moc_inrate(tx_receipt, m_client)
            self.logs_process_moc(tx_receipt,
                                  m_client,
                                  current_block,
                                  block_reference,
                                  d_moc_transactions)
            self.logs_process_moc_state(tx_receipt, m_client)
            if self.app_mode == "RRC20":
                self.logs_process_reserve_approval(tx_receipt, m_client)
                self.logs_process_transfer_from_reserve(tx_receipt,
                                                        m_client,
                                                        current_block,
                                                        block_reference)

            # Process transfer for MOC 2020-06-23
            self.process_transfer_from_moc(tx_receipt,
                                           d_moc_transactions,
                                           m_client,
                                           current_block,
                                           block_reference)

        # process all transactions looking for transfers
        if scan_transfer:
            if self.debug_mode:
                log.info("Starting to scan Transfer transactions block height: [{0}] last block height: [{1}]".format(
                    current_block, block_reference))

            all_transactions_receipts = self.transactions_receipt(all_transactions)
            for tx_receipt in all_transactions_receipts:
                self.logs_process_transfer(tx_receipt, m_client, current_block, block_reference)

    def scan_moc_blocks(self,
                        scan_transfer=True):

        start_time = time.time()

        # conect to mongo db
        m_client = self.mm.connect()

        config_from_block = self.options['scan_moc_blocks']['block_start']
        config_to_block = self.options['scan_moc_blocks']['block_end']
        config_block_reference = self.options['scan_moc_blocks']['block_reference']
        config_blocks_look_behind = self.options['scan_moc_blocks']['blocks_look_behind']

        # get last block from node
        last_block = self.connection_manager.block_number

        collection_moc_indexer = self.mm.collection_moc_indexer(m_client)
        moc_index = collection_moc_indexer.find_one(sort=[("updatedAt", -1)])
        last_block_indexed = 0
        if moc_index:
            if 'last_moc_block' in moc_index:
                last_block_indexed = moc_index['last_moc_block']

        from_block = last_block - config_blocks_look_behind
        if last_block_indexed > 0:
            from_block = last_block_indexed + 1
        else:
            if config_from_block > 0:
                from_block = config_from_block

        if from_block >= last_block:
            if self.debug_mode:
                log.info("Its not the time to run indexer no new blocks avalaible!")
            return

        to_block = config_to_block
        if to_block <= 0:
            to_block = last_block

        if from_block > to_block:
            log.error("To block > from block!!??")
            return

        # block reference is the last block, is to compare to... except you specified in the settings
        block_reference = config_block_reference
        if block_reference <= 0:
            block_reference = last_block

        current_block = from_block

        if self.debug_mode:
            log.info("Starting to Scan Transactions: {0} To Block: {1} ...".format(from_block, to_block))

        while current_block <= to_block:

            self.scan_moc_block(current_block, block_reference, m_client, scan_transfer=scan_transfer)

            log.info("[RUNNING SCAN TX] DONE BLOCK HEIGHT: [{0}] / [{1}]".format(current_block, to_block))
            collection_moc_indexer.update_one({},
                                              {'$set': {'last_moc_block': current_block,
                                                        'updatedAt': datetime.datetime.now()}},
                                              upsert=True)
            # Go to next block
            current_block += 1

        duration = time.time() - start_time
        log.info("[SCAN TX] LAST BLOCK HEIGHT: [{0}] Done in {1} seconds".format(current_block, duration))

    def is_confirmed_block(self, block_height, block_height_last):

        confirm_blocks = self.options['scan_moc_blocks']['confirm_blocks']
        if block_height_last - block_height > confirm_blocks:
            status = 'confirmed'
            confirmation_time = datetime.datetime.now()
            confirming_percent = 100
        else:
            status = 'confirming'
            confirmation_time = None
            confirming_percent = (block_height_last - block_height) * 10

        return status, confirmation_time, confirming_percent

    def scan_transaction_status(self):

        seconds_not_in_chain_error = self.options['scan_moc_blocks']['seconds_not_in_chain_error']

        # conect to mongo db
        m_client = self.mm.connect()

        # get last block from node
        last_block = self.connection_manager.block_number

        collection_moc_indexer = self.mm.collection_moc_indexer(m_client)
        moc_index = collection_moc_indexer.find_one(sort=[("updatedAt", -1)])
        last_moc_status_block = 0
        if moc_index:
            if 'last_moc_status_block' in moc_index:
                last_moc_status_block = int(moc_index['last_moc_status_block'])

        if last_block <= last_moc_status_block:
            if self.debug_mode:
                log.info("Its not time to run Scan Transactions status")
            return

        if self.debug_mode:
            log.info("Starting to Scan Transactions status last block: {0} ".format(last_block))

        start_time = time.time()

        collection_tx = self.mm.collection_transaction(m_client)

        # Get pendings tx and check for confirming, confirmed or failed
        tx_pendings = collection_tx.find({'status': 'pending'})
        for tx_pending in tx_pendings:

            try:
                tx_receipt = self.connection_manager.web3.eth.getTransactionReceipt(tx_pending['transactionHash'])
            except TransactionNotFound:
                tx_receipt = None

            if tx_receipt:
                d_tx_up = dict()
                if tx_receipt['status'] == 1:
                    d_tx_up['status'], d_tx_up['confirmationTime'], d_tx_up['confirmingPercent'] = \
                        self.is_confirmed_block(
                        tx_receipt['blockNumber'],
                        last_block)
                elif tx_receipt['status'] == 0:
                    d_tx_up['status'] = 'failed'
                    d_tx_up['confirmationTime'] = datetime.datetime.now()
                else:
                    continue

                collection_tx.find_one_and_update(
                    {"_id": tx_pending["_id"]},
                    {"$set": d_tx_up})

                log.info("Setting TX STATUS: {0} hash: {1}".format(d_tx_up['status'],
                                                                   tx_pending['transactionHash']))

        # Get confirming tx and check for confirming, confirmed or failed
        tx_pendings = collection_tx.find({'status': 'confirming'})
        for tx_pending in tx_pendings:

            try:
                tx_receipt = self.connection_manager.web3.eth.getTransactionReceipt(tx_pending['transactionHash'])
            except TransactionNotFound:
                tx_receipt = None

            if tx_receipt:
                d_tx_up = dict()
                if tx_receipt['status'] == 1:
                    d_tx_up['status'], d_tx_up['confirmationTime'], d_tx_up['confirmingPercent'] = \
                        self.is_confirmed_block(
                        tx_receipt['blockNumber'],
                        last_block)
                    #if d_tx_up['status'] == 'confirming':
                    #    # is already on confirming status
                    #    # not write to db
                    #    continue
                elif tx_receipt['status'] == 0:
                    d_tx_up['status'] = 'failed'
                    d_tx_up['confirmationTime'] = datetime.datetime.now()
                else:
                    continue

                collection_tx.find_one_and_update(
                    {"_id": tx_pending["_id"]},
                    {"$set": d_tx_up})

                log.info("Setting TX STATUS: {0} hash: {1}".format(d_tx_up['status'],
                                                                   tx_pending['transactionHash']))
            else:
                # no receipt from tx
                # here problem with eternal confirming
                created_at = tx_pending['createdAt']
                if created_at:
                    dte = created_at + datetime.timedelta(seconds=seconds_not_in_chain_error)
                    if dte < datetime.datetime.now():
                        d_tx_up = dict()
                        d_tx_up['status'] = 'failed'
                        d_tx_up['errorCode'] = 'staleTransaction'
                        d_tx_up['confirmationTime'] = datetime.datetime.now()

                        collection_tx.find_one_and_update(
                            {"_id": tx_pending["_id"]},
                            {"$set": d_tx_up})

                        log.info("Setting TX STATUS: {0} hash: {1}".format(d_tx_up['status'],
                                                                           tx_pending['transactionHash']))

        collection_moc_indexer.update_one({},
                                          {'$set': {'last_moc_status_block': last_block,
                                                    'updatedAt': datetime.datetime.now()}},
                                          upsert=True)

        duration = time.time() - start_time
        log.info("[SCAN TX STATUS] BLOCK HEIGHT: [{0}] Done in {1} seconds.".format(last_block, duration))

    def scan_moc_prices(self):

        # conect to mongo db
        m_client = self.mm.connect()

        config_from_block = self.options['scan_moc_prices']['block_start']
        config_to_block = self.options['scan_moc_prices']['block_end']
        config_blocks_look_behind = self.options['scan_moc_blocks']['blocks_look_behind']

        # get last block from node
        last_block = self.connection_manager.block_number

        collection_moc_indexer = self.mm.collection_moc_indexer(m_client)
        moc_index = collection_moc_indexer.find_one(sort=[("updatedAt", -1)])
        last_block_indexed = 0
        if moc_index:
            if 'last_moc_prices_block' in moc_index:
                last_block_indexed = moc_index['last_moc_prices_block']

        from_block = last_block - config_blocks_look_behind
        if last_block_indexed > 0:
            from_block = last_block_indexed + 1
        else:
            if config_from_block > 0:
                from_block = config_from_block

        if from_block >= last_block:
            if self.debug_mode:
                log.info("Its not the time to run indexer no new blocks avalaible!")
            return

        to_block = config_to_block
        if to_block <= 0:
            to_block = last_block

        if from_block > to_block:
            log.error("To block > from block!!??")
            return

        current_block = from_block

        # get collection price from mongo
        collection_price = self.mm.collection_price(m_client)

        if self.debug_mode:
            log.info("Starting to Scan prices: {0} To Block: {1} ...".format(from_block, to_block))

        start_time = time.time()
        while current_block <= to_block:

            if self.debug_mode:
                log.info("Starting to scan MOC prices block height: [{0}]".format(
                    current_block))

            last_price_height = collection_price.find_one(
                filter={"blockHeight": {"$gte": current_block}},
                sort=[("blockHeight", -1)]
            )

            # disabling to update blocks already done
            #if last_price_height:
            #    if self.debug_mode:
            #        log.warning("Not updating prices! Already exist for that block")
            #    continue

            # get all functions from smart contract
            d_prices = self.prices_from_sc(block_identifier=current_block)
            if d_prices:
                # only write if there are prices
                d_prices["blockHeight"] = current_block
                d_prices["createdAt"] = datetime.datetime.now()

                collection_price.find_one_and_update(
                    {"blockHeight": current_block},
                    {"$set": d_prices},
                    upsert=True)

                if self.debug_mode:
                    log.info("Done scan prices block height: [{0}]".format(current_block))

            collection_moc_indexer.update_one({},
                                              {'$set': {'last_moc_prices_block': current_block,
                                                        'updatedAt': datetime.datetime.now()}},
                                              upsert=True)
            # Go to next block
            current_block += 1

        duration = time.time() - start_time
        log.info("[SCAN PRICES] LAST BLOCK HEIGHT: [{0}] Done in {1} seconds.".format(current_block, duration))

    def scan_moc_state_status(self):

        # conect to mongo db
        m_client = self.mm.connect()

        config_from_block = self.options['scan_moc_state_status']['block_start']
        config_to_block = self.options['scan_moc_state_status']['block_end']
        config_blocks_look_behind = self.options['scan_moc_state_status']['blocks_look_behind']

        # get last block from node
        last_block = self.connection_manager.block_number

        collection_moc_indexer = self.mm.collection_moc_indexer(m_client)
        moc_index = collection_moc_indexer.find_one(sort=[("updatedAt", -1)])
        last_block_indexed = 0
        if moc_index:
            if 'last_moc_state_status_block' in moc_index:
                last_block_indexed = moc_index['last_moc_state_status_block']

        from_block = last_block - config_blocks_look_behind
        if last_block_indexed > 0:
            from_block = last_block_indexed + 1
        else:
            if config_from_block > 0:
                from_block = config_from_block

        if from_block >= last_block:
            if self.debug_mode:
                log.info("Its not the time to run indexer no new blocks avalaible!")
            return

        to_block = config_to_block
        if to_block <= 0:
            to_block = last_block

        if from_block > to_block:
            log.error("To block > from block!!??")
            return

        current_block = from_block

        # get collection price from mongo
        collection_moc_state_status = self.mm.collection_moc_state_status(m_client)

        if self.debug_mode:
            log.info("Starting to Scan Moc State Status: {0} To Block: {1} ...".format(from_block, to_block))

        start_time = time.time()
        while current_block <= to_block:

            if self.debug_mode:
                log.info("Starting to scan Moc State Status block height: [{0}]".format(
                    current_block))

            last_moc_state_status_height = collection_moc_state_status.find_one(
                filter={"blockHeight": {"$gte": current_block}},
                sort=[("blockHeight", -1)]
            )

            # disabling to update blocks already done
            #if last_moc_state_status_height:
            #    if self.debug_mode:
            #        log.warning("Not updating moc state status! Already exist for that block")
            #    continue

            # get all functions from smart contract
            d_status = self.state_status_from_sc(block_identifier=current_block)
            d_status["blockHeight"] = current_block
            d_status["createdAt"] = datetime.datetime.now()

            collection_moc_state_status.find_one_and_update(
                {"blockHeight": current_block},
                {"$set": d_status},
                upsert=True)

            if self.debug_mode:
                log.info("Done scan state status block height: [{0}]".format(current_block))

            collection_moc_indexer.update_one({},
                                              {'$set': {'last_moc_state_status_block': current_block,
                                                        'updatedAt': datetime.datetime.now()}},
                                              upsert=True)
            # Go to next block
            current_block += 1

        duration = time.time() - start_time
        log.info("[SCAN STATE STATUS] LAST BLOCK HEIGHT: [{0}] Done in {1} seconds.".format(current_block, duration))

    def scan_user_state_update(self):

        # conect to mongo db
        m_client = self.mm.connect()

        # get last block from node
        last_block = self.connection_manager.block_number

        collection_user_state_update = self.mm.collection_user_state_update(m_client)
        users_pending_update = collection_user_state_update.find({})

        if self.debug_mode:
            log.info("Starting to update user balance on block: {0} ".format(last_block))

        start_time = time.time()

        # get list of users to update balance
        for user_update in users_pending_update:

            block_height = self.connection_manager.block_number

            # udpate balance of address of the account on the last block height
            self.update_balance_address(m_client, user_update['account'], block_height)

            collection_user_state_update.remove({'account': user_update['account']})

            if self.debug_mode:
                log.info("UPDATING ACCOUNT BALANCE: {0} BLOCKHEIGHT: {1}".format(
                    user_update['account'],
                    block_height))

        duration = time.time() - start_time
        log.info("[SCAN USER STATE UPDATE] BLOCK HEIGHT: [{0}] Done in {1} seconds.".format(last_block, duration))

    def scan_moc_blocks_not_processed(self):

        if self.debug_mode:
            log.info("Starting to scan blocks Not processed ")

        start_time = time.time()

        # get last block from node
        last_block = self.connection_manager.block_number

        # conect to mongo db
        m_client = self.mm.connect()

        collection_tx = self.mm.collection_transaction(m_client)

        # we need to query tx with processLogs=None and in the last 60 minutes
        only_new_ones = datetime.datetime.now() - datetime.timedelta(minutes=300)
        moc_txs = collection_tx.find({"processLogs": None,
                                      "status": "confirmed",
                                      "createdAt": {"$gte": only_new_ones}},
                                     sort=[("createdAt", -1)])

        if moc_txs:
            for moc_tx in moc_txs:
                log.info("[SCAN BLOCK NOT PROCESSED] PROCESSING HASH: [{0}]".format(moc_tx['transactionHash']))
                try:
                    tx_receipt = self.connection_manager.web3.eth.getTransactionReceipt(moc_tx['transactionHash'])
                except TransactionNotFound:
                    log.error("[SCAN BLOCK NOT PROCESSED] TX NOT FOUND: [{0}]".format(moc_tx['transactionHash']))
                    continue

                log.info("[SCAN BLOCK NOT PROCESSED] PROCESSING HASH: [{0}]".format(moc_tx['transactionHash']))

                self.scan_moc_block(tx_receipt['blockNumber'], last_block, m_client)

        duration = time.time() - start_time

        log.info("[SCAN BLOCK NOT PROCESSED] Done in {0} seconds.".format(duration))

