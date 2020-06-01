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

from moneyonchain.manager import ConnectionManager
from moneyonchain.moc import MoC, MoCState, MoCInrate
from moneyonchain.rdoc import RDOCMoC, RDOCMoCState, RDOCMoCInrate

import logging
import logging.config


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger('default')


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
        #    collection.create_index([('startBlockNumber', pymongo.ASCENDING)], unique=True)

        return collection


class MoCIndexer:

    def __init__(self, monitor_config, network_nm):

        self.options = monitor_config
        self.network = network_nm

        self.connection_manager = ConnectionManager(options=self.options, network=self.network)
        self.app_mode = self.options['networks'][self.network]['app_mode']

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

    def update_last_moc_state(self):

        # conect to mongo db
        m_client = self.mm.connect()

        # get last block from node
        last_block = self.connection_manager.block_number

        # get all functions from smart contract
        d_moc_state = self.moc_state_from_sc()

        block_height = last_block - d_moc_state['dayBlockSpan']

        # get last price written in mongo
        collection_price = self.mm.collection_price(m_client)
        last_price = collection_price.find_one(filter={"blockHeight": {"$lt": block_height}},
                                               sort=[("blockHeight", -1)])

        d_moc_state["lastUpdateHeight"] = last_block
        d_moc_state["priceVariation"] = last_price

        # get collection moc_state from mongo
        collection_moc_state = self.mm.collection_moc_state(m_client)

        # update or insert the new info on mocstate
        post_id = collection_moc_state.find_one_and_update(
            {},
            {"$set": d_moc_state},
            upsert=True)

        return post_id

    def update_from_address(self, account_address):

        # conect to mongo db
        m_client = self.mm.connect()

        # get all functions from smart contract
        d_user_balance = self.balances_from_address(account_address)

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
            d_user_balance["showTermsAndConditions"] = False
            d_user_balance["showTutorialNoMore"] = False

        # update or insert
        post_id = collection_user_state.find_one_and_update(
            {"address": account_address},
            {"$set": d_user_balance},
            upsert=True)

        return post_id
