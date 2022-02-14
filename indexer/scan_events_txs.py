import time
import datetime

from brownie.network.event import _decode_logs

from moneyonchain.networks import network_manager
from indexer.mongo_manager import mongo_manager
from indexer.logger import log

from indexer.events import IndexRiskProMint, IndexRiskProRedeem, IndexRiskProxMint, \
    IndexRiskProxRedeem, IndexStableTokenMint, IndexStableTokenRedeem, \
    IndexFreeStableTokenRedeem, \
    IndexBucketLiquidation, IndexContractLiquidated, \
    IndexInrateDailyPay, IndexRiskProHoldersInterestPay, \
    IndexSettlementStarted, IndexRedeemRequestAlter, \
    IndexRedeemRequestProcessed, IndexSettlementRedeemStableToken, \
    IndexSettlementDeleveraging, IndexSettlementCompleted, \
    IndexStateTransition, \
    IndexApproval, IndexApprovalMoCToken, \
    IndexRESERVETransfer, \
    IndexRISKPROTransfer, \
    IndexSTABLETransfer


class ScanEventsTxs:

    def __init__(self, options, app_mode, map_contract_addresses, contracts_loaded):
        self.options = options
        self.app_mode = app_mode
        self.map_contract_addresses = map_contract_addresses
        self.contracts_loaded = contracts_loaded
        self.confirm_blocks = self.options['scan_moc_blocks']['confirm_blocks']
        self.map_events_contracts = self.map_events()

        # update block info
        self.last_block = network_manager.block_number
        self.block_ts = network_manager.block_timestamp(self.last_block)

    def update_info_last_block(self, m_client):

        collection_moc_indexer = mongo_manager.collection_moc_indexer(m_client)
        moc_index = collection_moc_indexer.find_one(sort=[("updatedAt", -1)])
        if moc_index:
            if 'last_block_number' in moc_index:
                self.last_block = moc_index['last_block_number']
                self.block_ts = moc_index['last_block_ts']

    def map_events(self):

        d_event = dict()
        d_event[self.map_contract_addresses["MoC"]] = {
            "BucketLiquidation": IndexBucketLiquidation(self.options, self.app_mode, self.contracts_loaded),
            "ContractLiquidated": IndexContractLiquidated(self.options, self.app_mode, self.contracts_loaded)
        }
        d_event[self.map_contract_addresses["MoCSettlement"]] = {
            "SettlementStarted": IndexSettlementStarted(self.options, self.app_mode),
            "RedeemRequestAlter": IndexRedeemRequestAlter(self.options, self.app_mode),
            "RedeemRequestProcessed": IndexRedeemRequestProcessed(self.options, self.app_mode),
            "SettlementRedeemStableToken": IndexSettlementRedeemStableToken(self.options, self.app_mode),
            "SettlementDeleveraging": IndexSettlementDeleveraging(self.options, self.app_mode, self.contracts_loaded),
            "SettlementCompleted": IndexSettlementCompleted(self.options, self.app_mode)
        }
        d_event[self.map_contract_addresses["MoCExchange"]] = {
            "RiskProMint": IndexRiskProMint(self.options, self.app_mode),
            "RiskProRedeem": IndexRiskProRedeem(self.options, self.app_mode),
            "RiskProxMint": IndexRiskProxMint(self.options, self.app_mode),
            "RiskProxRedeem": IndexRiskProxRedeem(self.options, self.app_mode),
            "StableTokenMint": IndexStableTokenMint(self.options, self.app_mode),
            "StableTokenRedeem": IndexStableTokenRedeem(self.options, self.app_mode),
            "FreeStableTokenRedeem": IndexFreeStableTokenRedeem(self.options, self.app_mode)
        }
        d_event[self.map_contract_addresses["BProToken"]] = {
            "Transfer": IndexRISKPROTransfer(self.options, self.app_mode, self.map_contract_addresses["MoC"])
        }
        d_event[self.map_contract_addresses["DoCToken"]] = {
            "Transfer": IndexSTABLETransfer(self.options, self.app_mode, self.map_contract_addresses["MoC"])
        }
        d_event[self.map_contract_addresses["MoCState"]] = {
            "StateTransition": IndexStateTransition(self.options, self.app_mode)
        }
        d_event[self.map_contract_addresses["MoCInrate"]] = {
            "InrateDailyPay": IndexInrateDailyPay(self.options, self.app_mode),
            "RiskProHoldersInterestPay": IndexRiskProHoldersInterestPay(self.options, self.app_mode)
        }
        d_event[self.map_contract_addresses["MoCVendors"]] = {
            "VendorReceivedMarkup": ""
        }
        d_event[self.map_contract_addresses["MoCBProxManager"]] = {
            "BucketMovement": ""
        }
        d_event[self.map_contract_addresses["MoCToken"]] = {
            "Transfer": "",
            "Approval": IndexApprovalMoCToken(self.options, self.app_mode, self.map_contract_addresses["MoC"])
        }
        if self.app_mode == 'RRC20':
            d_event[self.map_contract_addresses["ReserveToken"]] = {
                "Transfer": IndexRESERVETransfer(self.options, self.app_mode, self.map_contract_addresses["MoC"]),
                "Approval": IndexApproval(self.options, self.app_mode, self.map_contract_addresses["MoC"])
            }
            d_event[self.map_contract_addresses["MoC_BProToken"]] = {
                "Transfer": ""
            }

        return d_event

    def on_init(self):
        pass

    def parse_tx_receipt(self, raw_tx, tx_event, event_name, log_index=1):

        tx_receipt = dict()
        tx_receipt['blockNumber'] = raw_tx['blockNumber']
        tx_receipt['transactionHash'] = raw_tx['hash']
        tx_receipt['gas_used'] = raw_tx['gas_used']
        tx_receipt['gas_price'] = int(raw_tx['gasPrice'])
        tx_receipt['timestamp'] = raw_tx['timestamp']
        tx_receipt['createdAt'] = raw_tx['createdAt']
        tx_receipt['log_index'] = log_index
        tx_receipt['event'] = dict()
        tx_receipt['event'][event_name] = tx_event
        tx_receipt['chain'] = dict()
        tx_receipt['chain']['last_block'] = self.last_block
        tx_receipt['chain']['block_ts'] = self.block_ts
        tx_receipt['chain']['confirm_blocks'] = self.confirm_blocks

        return tx_receipt

    def process_logs(self, m_client, raw_tx):

        l_contract_addresses = self.map_contract_addresses.values()
        inverted_map_contract_addresses = dict((v, k) for k, v in self.map_contract_addresses.items())

        if raw_tx["logs"]:
            for tx_log in raw_tx["logs"]:
                log_address = str.lower(tx_log['address'])
                log_index = tx_log['logIndex']
                if log_address in l_contract_addresses:
                    tx_event = _decode_logs([tx_log])
                    map_events_contract = inverted_map_contract_addresses[log_address]
                    for tx_event_name, tx_event_info in tx_event.items():
                        if log_address not in self.map_events_contracts:
                            log.warn("Address not found in our filtered contract: [{0}]. skipping!".format(log_address))
                            continue

                        if tx_event_name in self.map_events_contracts[log_address]:
                            # go map the event
                            event_class = self.map_events_contracts[log_address][tx_event_name]
                            if event_class:
                                parse_receipt = self.parse_tx_receipt(raw_tx, tx_event, tx_event_name,
                                                                      log_index=log_index)
                                event_class.on_event(m_client, parse_receipt)

    def scan_events_txs(self, task=None):

        start_time = time.time()

        # connect to mongo db
        m_client = mongo_manager.connect()

        # update block information
        self.update_info_last_block(m_client)

        collection_raw_transactions = mongo_manager.collection_raw_transactions(m_client)

        raw_txs = collection_raw_transactions.find({"processed": False}, sort=[("blockNumber", 1)])

        count = 0
        if raw_txs:
            for raw_tx in raw_txs:

                # update block information
                self.update_info_last_block(m_client)

                count += 1
                self.process_logs(m_client, raw_tx)

                collection_raw_transactions.find_one_and_update(
                    {"hash": raw_tx["hash"], "blockNumber": raw_tx["blockNumber"]},
                    {"$set": {"processed": True}},
                    upsert=False)

        duration = time.time() - start_time
        log.info("[2. Scan Events Txs] Processed: [{0}] Done! [{1} seconds]".format(count, duration))

    def scan_events_not_processed_txs(self, task=None):
        """ Trying to reindex when there is a problem with events"""

        start_time = time.time()

        # connect to mongo db
        m_client = mongo_manager.connect()

        collection_transactions = mongo_manager.collection_transaction(m_client)

        collection_raw_transactions = mongo_manager.collection_raw_transactions(m_client)

        collection_moc_indexer = mongo_manager.collection_moc_indexer(m_client)
        moc_index = collection_moc_indexer.find_one(sort=[("updatedAt", -1)])

        # we need to query tx with processLogs=None and in the last 24hs
        only_last_tx = datetime.datetime.now() - datetime.timedelta(minutes=1440)
        txs = collection_transactions.find({
            "processLogs": None,
            "createdAt": {"$gte": only_last_tx}}, sort=[("createdAt", 1)])

        count = 0
        if txs:
            for tx in txs:
                # only status confirmed and confirming
                if tx["status"] not in ["confirmed", "confirming"]:
                    continue

                raw_tx = collection_raw_transactions.find_one({"hash": tx["transactionHash"]})

                if not raw_tx:
                    log.info("[8. Scan Blocks not processed] Not exist RAW Tx for hash: {0}".format(tx["transactionHash"]))
                    continue

                dt_older_than = moc_index["last_block_ts"] - datetime.timedelta(minutes=5)
                if tx["createdAt"] >= dt_older_than:
                    continue

                log.info("[8. Scan Blocks not processed] Reindexing with hash: {0}".format(tx["transactionHash"]))

                # update block information
                self.update_info_last_block(m_client)

                count += 1
                self.process_logs(m_client, raw_tx)

        duration = time.time() - start_time
        log.info("[8. Scan Blocks not processed] Done! Processed: [{0}] [{1} seconds]".format(count, duration))

    def on_task(self, task=None):
        self.scan_events_txs(task=task)

    def on_task_not_processed(self, task=None):
        self.scan_events_not_processed_txs(task=task)
