import datetime
import time
from collections import OrderedDict

from moneyonchain.networks import network_manager, chain
from moneyonchain.utils import filter_transactions

from web3.exceptions import TransactionNotFound
from indexer.mongo_manager import mongo_manager
from indexer.utils import transactions_receipt
from indexer.logger import log

from indexer.moc.events_mocsettlement import IndexSettlementStarted, \
    IndexRedeemRequestAlter, \
    IndexRedeemRequestProcessed, \
    IndexSettlementRedeemStableToken, \
    IndexSettlementDeleveraging, \
    IndexSettlementCompleted

from indexer.moc.events_mocinrate import IndexInrateDailyPay, \
    IndexRiskProHoldersInterestPay

from indexer.moc.events_mocstate import IndexStateTransition

from indexer.moc.events_approval import IndexApproval

from indexer.moc.events_token_reserve import IndexRESERVETransfer
from indexer.moc.events_token_riskpro import IndexRISKPROTransfer
from indexer.moc.events_token_stable import IndexSTABLETransfer

from .events_moc import IndexBucketLiquidation, IndexContractLiquidated
from .events_approval import IndexApprovalMoCToken

from .events_mocexchange import IndexRiskProMint, \
    IndexRiskProRedeem, \
    IndexRiskProxMint, \
    IndexRiskProxRedeem, \
    IndexStableTokenMint, \
    IndexStableTokenRedeem, \
    IndexFreeStableTokenRedeem


from .balances import Balances


class ScanBlocks(Balances):

    def __init__(self,
                 *tx_args,
                 **tx_vars):

        super().__init__(*tx_args, **tx_vars)
        self.index_riskpro_mint = None
        self.index_riskpro_redeem = None
        self.index_riskprox_mint = None
        self.index_riskprox_redeem = None
        self.index_stabletoken_mint = None
        self.index_stabletoken_redeem = None
        self.index_freestabletoken_redeem = None

        self.index_settlement_started = None
        self.index_redeem_request_alter = None
        self.index_redeem_request_processed = None
        self.index_settlement_redeem_stabletoken = None
        self.index_settlement_deleveraging = None
        self.index_settlement_completed = None

        self.index_inrate_dailypay = None
        self.index_riskproholders_interestpay = None

        self.index_bucket_liquidation = None

        self.index_state_transition = None

        self.index_approval = None

        self.index_reserve_transfer = None

        self.index_riskpro_transfer = None

        self.index_stable_transfer = None

        self.index_contract_liquidated = None

        self.index_approval_moc_token = None

        self.init_indexer()

    def init_indexer(self):

        index_info = dict(
            parent=self,
            confirm_blocks=self.options['scan_moc_blocks']['confirm_blocks']
        )

        address_exchange = self.contract_MoC.sc_moc_exchange.address()

        # 1. Exchange events index
        self.index_riskpro_mint = IndexRiskProMint(contract_address=address_exchange, **index_info)
        self.index_riskpro_redeem = IndexRiskProRedeem(contract_address=address_exchange, **index_info)
        self.index_riskprox_mint = IndexRiskProxMint(contract_address=address_exchange, **index_info)
        self.index_riskprox_redeem = IndexRiskProxRedeem(contract_address=address_exchange, **index_info)
        self.index_stabletoken_mint = IndexStableTokenMint(contract_address=address_exchange, **index_info)
        self.index_stabletoken_redeem = IndexStableTokenRedeem(contract_address=address_exchange, **index_info)
        self.index_freestabletoken_redeem = IndexFreeStableTokenRedeem(contract_address=address_exchange, **index_info)

        address_settlement = self.contract_MoC.sc_moc_settlement.address()

        # 2. Settlement events index
        self.index_settlement_started = IndexSettlementStarted(contract_address=address_settlement, **index_info)
        self.index_redeem_request_alter = IndexRedeemRequestAlter(contract_address=address_settlement, **index_info)
        self.index_redeem_request_processed = IndexRedeemRequestProcessed(contract_address=address_settlement, **index_info)
        self.index_settlement_redeem_stabletoken = IndexSettlementRedeemStableToken(contract_address=address_settlement, **index_info)
        self.index_settlement_deleveraging = IndexSettlementDeleveraging(contract_address=address_settlement, **index_info)
        self.index_settlement_completed = IndexSettlementCompleted(contract_address=address_settlement, **index_info)

        address_inrate = self.contract_MoC.sc_moc_inrate.address()

        # 3. MoC Inrate
        self.index_inrate_dailypay = IndexInrateDailyPay(contract_address=address_inrate, **index_info)
        self.index_riskproholders_interestpay = IndexRiskProHoldersInterestPay(contract_address=address_inrate, **index_info)

        # 4. MoC
        address_moc = self.contract_MoC.address()
        self.index_bucket_liquidation = IndexBucketLiquidation(contract_address=address_moc, **index_info)

        # 5. MoCState
        address_mocstate = self.contract_MoC.sc_moc_state.address()
        self.index_state_transition = IndexStateTransition(contract_address=address_mocstate, **index_info)

        # 6. Approval
        if self.app_mode == 'RRC20':
            address_reserve = self.contract_ReserveToken.address()
            self.index_approval = IndexApproval(contract_address=address_reserve,
                                                moc_address=address_moc,
                                                **index_info)

            self.index_reserve_transfer = IndexRESERVETransfer(contract_address=address_reserve,
                                                               moc_address=address_moc,
                                                               **index_info)
        else:
            # 8. Contract lequidated
            self.index_contract_liquidated = IndexContractLiquidated(
                contract_address=address_moc,
                **index_info)

        # 7. Tokens
        address_riskpro = self.contract_MoC.sc_moc_bpro_token.address()
        self.index_riskpro_transfer = IndexRISKPROTransfer(contract_address=address_riskpro,
                                                           moc_address=address_moc,
                                                           **index_info)

        address_stable = self.contract_MoC.sc_moc_doc_token.address()
        self.index_stable_transfer = IndexSTABLETransfer(contract_address=address_stable,
                                                         moc_address=address_moc,
                                                         **index_info)
        # 9. MoC Token Aproval
        address_moc_token = self.contract_MoC.sc_moc_moc_token.address()
        self.index_approval_moc_token = IndexApprovalMoCToken(
            contract_address=address_moc_token,
            moc_address=address_moc,
            **index_info)

    def scan_moc_block(self, current_block, block_reference, m_client, scan_transfer=True):

        if self.debug_mode:
            log.info("[SCAN TX] Starting to scan MOC transactions block height: [{0}] last block height: [{1}]".format(
                current_block, block_reference))

        # get block time from node
        block_ts = network_manager.block_timestamp(current_block)

        # get moc contracts adressess
        moc_addresses = self.moc_contract_addresses()

        # get block and full transactions
        f_block = chain.get_block(current_block)
        all_transactions = f_block['transactions']

        # From MOC Contract transactions
        moc_transactions, d_moc_transactions = filter_transactions(all_transactions, moc_addresses)

        # get transactions receipts
        moc_transactions_receipts = transactions_receipt(moc_transactions)

        index_info = dict(
            m_client=m_client,
            block_height=current_block,
            block_height_current=block_reference,
            transactions=d_moc_transactions,
            block_ts=block_ts
        )

        self.index_riskpro_mint.update_info(**index_info)
        self.index_riskpro_redeem.update_info(**index_info)
        self.index_riskprox_mint.update_info(**index_info)
        self.index_riskprox_redeem.update_info(**index_info)
        self.index_stabletoken_mint.update_info(**index_info)
        self.index_stabletoken_redeem.update_info(**index_info)
        self.index_freestabletoken_redeem.update_info(**index_info)

        self.index_settlement_started.update_info(**index_info)
        self.index_redeem_request_alter.update_info(**index_info)
        self.index_redeem_request_processed.update_info(**index_info)
        self.index_settlement_redeem_stabletoken.update_info(**index_info)
        self.index_settlement_deleveraging.update_info(**index_info)
        self.index_settlement_completed.update_info(**index_info)

        self.index_inrate_dailypay.update_info(**index_info)
        self.index_riskproholders_interestpay.update_info(**index_info)

        self.index_bucket_liquidation.update_info(**index_info)

        self.index_state_transition.update_info(**index_info)

        if self.app_mode == 'RRC20':
            self.index_approval.update_info(**index_info)
            self.index_reserve_transfer.update_info(**index_info)
        else:
            self.index_contract_liquidated.update_info(**index_info)

        self.index_riskpro_transfer.update_info(**index_info)

        self.index_stable_transfer.update_info(**index_info)

        self.index_approval_moc_token.update_info(**index_info)

        # process only MoC contract transactions
        for tx_receipt in moc_transactions_receipts:

            # 1. MoC Exchange

            # IndexRiskProMint
            self.index_riskpro_mint.index_from_receipt(tx_receipt)

            # IndexRiskProRedeem
            self.index_riskpro_redeem.index_from_receipt(tx_receipt)

            # IndexRiskProxMint
            self.index_riskprox_mint.index_from_receipt(tx_receipt)

            # IndexRiskProxRedeem
            self.index_riskprox_redeem.index_from_receipt(tx_receipt)

            # IndexStableTokenMint
            self.index_stabletoken_mint.index_from_receipt(tx_receipt)

            # IndexStableTokenRedeem
            self.index_stabletoken_redeem.index_from_receipt(tx_receipt)

            # IndexFreeStableTokenRedeem
            self.index_freestabletoken_redeem.index_from_receipt(tx_receipt)

            # 2. MoC Settlement

            # IndexSettlementStarted
            self.index_settlement_started.index_from_receipt(tx_receipt)

            # IndexRedeemRequestAlter
            self.index_redeem_request_alter.index_from_receipt(tx_receipt)

            # IndexRedeemRequestProcessed
            self.index_redeem_request_processed.index_from_receipt(tx_receipt)

            # IndexSettlementRedeemStableToken
            self.index_settlement_redeem_stabletoken.index_from_receipt(tx_receipt)

            # IndexSettlementDeleveraging
            self.index_settlement_deleveraging.index_from_receipt(tx_receipt)

            # IndexSettlementCompleted
            self.index_settlement_completed.index_from_receipt(tx_receipt)

            # 3. MoC Inrate

            # IndexInrateDailyPay
            self.index_inrate_dailypay.index_from_receipt(tx_receipt)

            # IndexRiskProHoldersInterestPay
            self.index_riskproholders_interestpay.index_from_receipt(tx_receipt)

            # 4. MoC

            # IndexBucketLiquidation
            self.index_bucket_liquidation.index_from_receipt(tx_receipt)

            # 5. MoC State

            # IndexStateTransition
            self.index_state_transition.index_from_receipt(tx_receipt)

            # 6. Approval
            if self.app_mode == "RRC20":

                # IndexApproval
                self.index_approval.index_from_receipt(tx_receipt)

                # IndexRESERVETransfer
                self.index_reserve_transfer.index_from_receipt(tx_receipt)
            else:
                self.index_contract_liquidated.index_from_receipt(tx_receipt)

            # 6b. Approval MoC Token
            self.index_approval_moc_token.index_from_receipt(tx_receipt)

            # 7. Transfer from MOC
            # Process transfer for MOC 2020-06-23
            self.process_transfer_from_moc(tx_receipt,
                                           d_moc_transactions,
                                           m_client,
                                           current_block,
                                           block_reference,
                                           block_ts)

        # process all transactions looking for transfers
        if scan_transfer:
            if self.debug_mode:
                log.info("[SCAN TX] Starting to scan Transfer transactions block height: [{0}] last block height: [{1}]".format(
                    current_block, block_reference))

            all_transactions_receipts = transactions_receipt(all_transactions)
            for tx_receipt in all_transactions_receipts:
                self.index_riskpro_transfer.index_from_receipt(tx_receipt)
                self.index_stable_transfer.index_from_receipt(tx_receipt)
                if self.app_mode == 'RRC20':
                    self.index_reserve_transfer.index_from_receipt(tx_receipt)

    def process_transfer_from_moc(self,
                                  tx_receipt,
                                  d_moc_transactions,
                                  m_client,
                                  block_height,
                                  block_height_current,
                                  block_ts):
        """ Process transfer from moc """

        confirm_blocks = self.options['scan_moc_blocks']['confirm_blocks']
        if block_height_current - block_height > confirm_blocks:
            status = 'confirmed'
            confirmation_time = block_ts
        else:
            status = 'confirming'
            confirmation_time = None

        if str.lower(tx_receipt.sender) not in [str.lower(self.contract_MoC.address())]:
            # If is not from our contract return
            return

        tx_hash = tx_receipt.txid
        moc_tx = d_moc_transactions[tx_hash]

        if tx_receipt.value <= 0:
            return

        if self.contract_MoC.project == 'RDoC':
            reserve_symbol = 'RIF'
        elif self.contract_MoC.project == 'MoC':
            reserve_symbol = 'RBTC'
        else:
            reserve_symbol = 'RBTC'

        # get last price written in mongo
        collection_price = mongo_manager.collection_price(m_client)
        last_price = collection_price.find_one(filter={"blockHeight": {"$lt": moc_tx['blockNumber']}},
                                               sort=[("blockHeight", -1)])

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(m_client)

        # FROM
        d_tx = OrderedDict()
        d_tx["address"] = tx_receipt.receiver
        d_tx["blockNumber"] = tx_receipt.block_number
        d_tx["event"] = 'TransferFromMoC'
        d_tx["transactionHash"] = tx_hash
        d_tx["amount"] = str(tx_receipt.value)
        d_tx["confirmationTime"] = confirmation_time
        d_tx["isPositive"] = False
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        d_tx["status"] = status
        d_tx["reserveSymbol"] = reserve_symbol
        d_tx["processLogs"] = True
        #usd_amount = Web3.fromWei(moc_tx['value'], 'ether') * Web3.fromWei(last_price['bitcoinPrice'], 'ether')
        #d_tx["USDAmount"] = str(int(usd_amount * self.precision))
        d_tx["createdAt"] = block_ts

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash,
             "address": d_tx["address"],
             "event": d_tx["event"]},
            {"$set": d_tx},
            upsert=True)

        # update user balances
        self.update_balance_address(m_client, d_tx["address"], block_height)

    def scan_moc_blocks(self,
                        scan_transfer=True):

        start_time = time.time()

        # conect to mongo db
        m_client = mongo_manager.connect()

        # get the block recesion is a margin of problems to not get the inmediat new instead
        # 2 older blocks from new.
        config_blocks_recession = self.options['scan_moc_blocks']['blocks_recession']

        # get last block from node compare 2 blocks older than new
        last_block = network_manager.block_number - config_blocks_recession

        collection_moc_indexer = mongo_manager.collection_moc_indexer(m_client)
        moc_index = collection_moc_indexer.find_one(sort=[("updatedAt", -1)])
        last_block_indexed = 0
        if moc_index:
            if 'last_moc_block' in moc_index:
                last_block_indexed = moc_index['last_moc_block']

        config_blocks_look_behind = self.options['scan_moc_blocks']['blocks_look_behind']
        from_block = last_block - config_blocks_look_behind
        if last_block_indexed > 0:
            from_block = last_block_indexed + 1

        if from_block >= last_block:
            if self.debug_mode:
                log.info("[SCAN TX] Its not the time to run indexer no new blocks avalaible!")
            return

        to_block = last_block

        if from_block > to_block:
            log.error("[SCAN TX] To block > from block!!??")
            return

        # block reference is the last block, is to compare to... except you specified in the settings
        block_reference = last_block

        # start with from block
        current_block = from_block

        if self.debug_mode:
            log.info("[SCAN TX] Starting to Scan Transactions: {0} To Block: {1} ...".format(from_block, to_block))

        while current_block <= to_block:

            self.scan_moc_block(current_block, block_reference, m_client, scan_transfer=scan_transfer)

            log.info("[SCAN TX] DONE BLOCK HEIGHT: [{0}] / [{1}]".format(current_block, to_block))
            collection_moc_indexer.update_one({},
                                              {'$set': {'last_moc_block': current_block,
                                                        'updatedAt': datetime.datetime.now()}},
                                              upsert=True)
            # Go to next block
            current_block += 1

        duration = time.time() - start_time
        log.info("[SCAN TX] LAST BLOCK HEIGHT: [{0}] Done in {1} seconds".format(current_block, duration))

    def scan_moc_blocks_history(self,
                                scan_transfer=True):

        start_time = time.time()

        # get the block recesion is a margin of problems to not get the inmediat new instead
        # 2 older blocks from new.
        config_blocks_recession = self.options['scan_moc_blocks']['blocks_recession']

        # get last block from node compare 2 blocks older than new
        last_block = network_manager.block_number - config_blocks_recession

        # conect to mongo db
        m_client = mongo_manager.connect()

        from_block = self.options['scan_moc_history']['from_block']
        to_block = self.options['scan_moc_history']['to_block']

        collection_moc_indexer_history = mongo_manager.collection_moc_indexer_history(m_client)
        moc_index = collection_moc_indexer_history.find_one(sort=[("updatedAt", -1)])
        last_block_indexed = 0
        if moc_index:
            if 'last_moc_block' in moc_index:
                if moc_index['last_moc_block'] > 0:
                    last_block_indexed = moc_index['last_moc_block']

        if last_block_indexed > 0:
            from_block = last_block_indexed + 1

        if from_block >= to_block:
            if self.debug_mode:
                log.info("[SCAN TX HISTORY] Its not the time to run indexer no new blocks avalaible!")
            return

        # start with from block
        current_block = from_block

        if self.debug_mode:
            log.info("[SCAN TX HISTORY] Starting to Scan Transactions: {0} To Block: {1} ...".format(from_block,
                                                                                                     to_block))

        while current_block <= to_block:
            self.scan_moc_block(current_block, last_block, m_client, scan_transfer=scan_transfer)

            log.info("[SCAN TX HISTORY] DONE BLOCK HEIGHT: [{0}] / [{1}]".format(current_block, to_block))
            collection_moc_indexer_history.update_one({},
                                              {'$set': {'last_moc_block': current_block,
                                                        'updatedAt': datetime.datetime.now()}},
                                              upsert=True)

            # Go to next block
            current_block += 1

        duration = time.time() - start_time
        log.info("[SCAN TX HISTORY] LAST BLOCK HEIGHT: [{0}] Done in {1} seconds".format(current_block, duration))

    def is_confirmed_block(self, block_height, block_height_last, block_height_last_ts):

        confirm_blocks = self.options['scan_moc_blocks']['confirm_blocks']
        if block_height_last - block_height > confirm_blocks:
            status = 'confirmed'
            confirmation_time = block_height_last_ts
            confirming_percent = 100
        else:
            status = 'confirming'
            confirmation_time = None
            confirming_percent = (block_height_last - block_height) * 10

        return status, confirmation_time, confirming_percent

    def force_start_history(self):

        log.info("[FORCE START HISTORY] Removing collection...")

        # conect to mongo db
        m_client = mongo_manager.connect()

        collection_moc_indexer_history = mongo_manager.collection_moc_indexer_history(m_client)

        collection_moc_indexer_history.update_one({},
                                                  {'$set': {'last_moc_block': 0,
                                                            'updatedAt': datetime.datetime.now()}},
                                                  upsert=True)

        #collection_moc_indexer_history.drop()

        log.info("[FORCE START HISTORY] DONE! Collection remove it!.")

    def scan_moc_blocks_not_processed(self):

        if self.debug_mode:
            log.info("[SCAN BLOCK NOT PROCESSED] Starting to scan blocks Not processed ")

        start_time = time.time()

        # get last block from node
        last_block = network_manager.block_number

        # conect to mongo db
        m_client = mongo_manager.connect()

        collection_tx = mongo_manager.collection_transaction(m_client)

        # we need to query tx with processLogs=None and in the last 60 minutes
        only_new_ones = datetime.datetime.now() - datetime.timedelta(minutes=10)
        moc_txs = collection_tx.find({"processLogs": None,
                                      "status": "confirmed",
                                      "createdAt": {"$gte": only_new_ones}},
                                     sort=[("createdAt", -1)])

        if moc_txs:
            for moc_tx in moc_txs:
                log.info("[SCAN BLOCK NOT PROCESSED] PROCESSING HASH: [{0}]".format(moc_tx['transactionHash']))
                try:
                    tx_receipt = chain.get_transaction(moc_tx['transactionHash'])
                    #tx_receipt = self.connection_manager.web3.eth.getTransactionReceipt(moc_tx['transactionHash'])
                except TransactionNotFound:
                    log.error("[SCAN BLOCK NOT PROCESSED] TX NOT FOUND: [{0}]".format(moc_tx['transactionHash']))
                    continue

                log.info("[SCAN BLOCK NOT PROCESSED] PROCESSING HASH: [{0}]".format(moc_tx['transactionHash']))

                self.scan_moc_block(tx_receipt['blockNumber'], last_block, m_client)

        duration = time.time() - start_time

        log.info("[SCAN BLOCK NOT PROCESSED] Done in {0} seconds.".format(duration))
