import os
from tabulate import tabulate

from moneyonchain.networks import network_manager
from indexer.mongo_manager import mongo_manager
from moneyonchain.tokens import RIFDoC, RIF
from indexer.logger import log

CONTRACT_PRECISION = 10 ** 18
HISTORIC_BLOCK_HEIGHT_AMOUNT = 2880 * 15

OPERATIONS_TRANSLATE = {
    "RiskProRedeem": "RIFP Redeem",
    "RiskProMint": "RIFP Mint",
    "StableTokenMint": "RDOC Mint",
    "StableTokenRedeem": "RDOC Redeem",
    "FreeStableTokenRedeem": "RDOC Redeem",
    "RiskProxRedeem": "RIFX Redeem",
    "RiskProxMint": "RIFX Mint",
    "Transfer": "Transfer"
}


class ReportHistorical:

    def __init__(self, app_config, config_net, connection_net):

        self.options = app_config
        self.config_network = config_net
        self.connection_network = connection_net

        # install custom network if needit
        if self.connection_network.startswith("https") or self.connection_network.startswith("http"):
            a_connection = self.connection_network.split(',')
            host = a_connection[0]
            chain_id = a_connection[1]

            network_manager.add_network(
                network_name='rskCustomNetwork',
                network_host=host,
                network_chainid=chain_id,
                network_explorer='https://blockscout.com/rsk/mainnet/api',
                force=False
            )

            self.connection_network = 'rskCustomNetwork'

            log.info("Using custom network... id: {}".format(self.connection_network))

        # connect and init contracts
        self.connect()

        # initialize mongo db
        mongo_manager.set_connection(uri=self.options['mongo']['uri'], db=self.options['mongo']['db'])

    def connect(self):
        """ Init connection"""

        # connection network is the brownie connection network
        # config network is our enviroment we want to connect
        network_manager.connect(connection_network=self.connection_network,
                                config_network=self.config_network)

    def coins_and_tokens(self):

        app_project = self.options["networks"][self.config_network]["project"]
        coins = dict()
        coins["tokens"] = dict()
        if app_project in ["RDoC"]:
            coins["collateral"] = "RIF"
            coins["project_name"] = "ROC"
            coins["tokens"]["RISKPRO"] = "RIFP"
            coins["tokens"]["STABLE"] = "RDOC"
            coins["tokens"]["RISKPROX"] = "RIFX"
        elif app_project in ["MoC"]:
            coins["collateral"] = "RBTC"
            coins["project_name"] = "MOC"
            coins["tokens"]["RISKPRO"] = "BPRO"
            coins["tokens"]["STABLE"] = "DOC"
            coins["tokens"]["RISKPROX"] = "BTCX"
        else:
            raise Exception("Not recognize Config project")

        return coins

    def report_to_console(self):

        # conect to mongo db
        m_client = mongo_manager.connect()

        # get collection moc_state from mongo
        collection_moc_state = mongo_manager.collection_moc_state(m_client)
        collection_moc_state_history = mongo_manager.collection_moc_state_history(m_client)
        collection_transaction = mongo_manager.collection_transaction(m_client)

        info_tokens = self.coins_and_tokens()

        coll_moc_state = collection_moc_state.find_one({}, sort=[("blockHeight", -1)])
        if not coll_moc_state:
            log.error("MoC State collection not exist. Please run indexer first before running report")
            return

        # Current status
        log.info("Stable Project: {0}".format(info_tokens['project_name']))
        log.info("Collateral: {0}".format(info_tokens["collateral"]))
        log.info("")
        log.info("Current block height: {0}".format(coll_moc_state['blockHeight']))
        log.info("Global Coverage: {0}".format(int(coll_moc_state['globalCoverage']) / CONTRACT_PRECISION))
        price_collateral_usd = int(coll_moc_state['bitcoinPrice']) / CONTRACT_PRECISION
        log.info("1 {0} = {1} USD ".format(info_tokens["collateral"], price_collateral_usd))

        log.info("")
        log.info("TVL")
        total_collateral = int(coll_moc_state['b0BTCAmount']) / CONTRACT_PRECISION
        log.info("Total {0} in protocol: {1} ({2} USD)".format(
            info_tokens["collateral"], total_collateral, total_collateral * price_collateral_usd))
        log.info("Total {0} in protocol: {1}".format(
            info_tokens["tokens"]["RISKPRO"], int(coll_moc_state['b0BproAmount']) / CONTRACT_PRECISION))
        log.info("Total {0} in protocol: {1}".format(
            info_tokens["tokens"]["STABLE"], int(coll_moc_state['b0DocAmount']) / CONTRACT_PRECISION))
        log.info("Total {0} in protocol: {1}".format(
            info_tokens["tokens"]["RISKPROX"], int(coll_moc_state['x2BproAmount']) / CONTRACT_PRECISION))

        coll_tx = collection_transaction.find({"event": {
                                                        "$in": ["RiskProRedeem",
                                                                "RiskProMint",
                                                                "StableTokenMint",
                                                                "StableTokenRedeem",
                                                                "FreeStableTokenRedeem",
                                                                "RiskProxRedeem",
                                                                "RiskProxMint"
                                                                ]
                                                        },
                                               "blockNumber": {"$lte": coll_moc_state['blockHeight']}})
        if not coll_tx:
            log.error("Transaction collection not exist. Please run indexer first before running report")
            return

        count = 0
        for tx in coll_tx:
            count += 1

        log.info("{0} transactions found in protocol".format(count))

        coll_group = collection_transaction.aggregate(
            [
                {"$match": {"event": {
                                    "$in": ["RiskProRedeem",
                                            "RiskProMint",
                                            "StableTokenMint",
                                            "StableTokenRedeem",
                                            "FreeStableTokenRedeem",
                                            "RiskProxRedeem",
                                            "RiskProxMint"
                                            ]
                                    },
                            "blockNumber": {"$lte": coll_moc_state['blockHeight']}}},
                {"$group": {"_id": "$address", "count": {"$sum": 1}}}
            ]
        )

        count = 0
        for tx in coll_group:
            count += 1

        log.info("{0} user address found in protocol".format(count))

        # HISTORY

        historic_block_height = coll_moc_state['blockHeight'] - HISTORIC_BLOCK_HEIGHT_AMOUNT

        coll_moc_state_historic = collection_moc_state_history.find_one({"blockHeight": {"$lt": historic_block_height}}, sort=[("blockHeight", -1)])
        if not coll_moc_state_historic:
            log.error("Historic: MoC State collection not exist. Please run indexer first before running report")
            return

        log.info("")
        log.info("HISTORY (15 days ago)")
        log.info("block height: {0}".format(coll_moc_state_historic['blockHeight']))
        log.info("Global Coverage: {0}".format(int(coll_moc_state_historic['globalCoverage']) / CONTRACT_PRECISION))
        price_collateral_usd = int(coll_moc_state_historic['bitcoinPrice']) / CONTRACT_PRECISION
        log.info("1 {0} = {1} USD ".format(info_tokens["collateral"], price_collateral_usd))
        log.info("")
        log.info("TVL")
        total_collateral = int(coll_moc_state_historic['b0BTCAmount']) / CONTRACT_PRECISION
        log.info("Total {0} in protocol: {1} ({2} USD)".format(
            info_tokens["collateral"], total_collateral, total_collateral * price_collateral_usd))
        log.info("Total {0} in protocol: {1}".format(
            info_tokens["tokens"]["RISKPRO"], int(coll_moc_state_historic['b0BproAmount']) / CONTRACT_PRECISION))
        log.info("Total {0} in protocol: {1}".format(
            info_tokens["tokens"]["STABLE"], int(coll_moc_state_historic['b0DocAmount']) / CONTRACT_PRECISION))
        log.info("Total {0} in protocol: {1}".format(
            info_tokens["tokens"]["RISKPROX"], int(coll_moc_state_historic['x2BproAmount']) / CONTRACT_PRECISION))

        coll_tx = collection_transaction.find({"event": {
            "$in": ["RiskProRedeem",
                    "RiskProMint",
                    "StableTokenMint",
                    "StableTokenRedeem",
                    "FreeStableTokenRedeem",
                    "RiskProxRedeem",
                    "RiskProxMint"
                    ]
        },
            "blockNumber": {"$lte": coll_moc_state_historic['blockHeight']}})
        if not coll_tx:
            log.error("Transaction collection not exist. Please run indexer first before running report")
            return

        count = 0
        for tx in coll_tx:
            count += 1

        log.info("{0} transactions found in protocol".format(count))

        coll_group = collection_transaction.aggregate(
            [
                {"$match": {"event": {
                    "$in": ["RiskProRedeem",
                            "RiskProMint",
                            "StableTokenMint",
                            "StableTokenRedeem",
                            "FreeStableTokenRedeem",
                            "RiskProxRedeem",
                            "RiskProxMint"
                            ]
                },
                    "blockNumber": {"$lte": coll_moc_state_historic['blockHeight']}}},
                {"$group": {"_id": "$address", "count": {"$sum": 1}}}
            ]
        )

        count = 0
        for tx in coll_group:
            count += 1

        log.info("{0} user address found in protocol".format(count))

    def pay_tc_holders_report_console(self):

        # conect to mongo db
        m_client = mongo_manager.connect()

        # get collection from mongo
        collection_pay_tc_holders = mongo_manager.collection_bitpro_holders_interest(m_client)

        info_tokens = self.coins_and_tokens()

        coll_pay_tc_holders = collection_pay_tc_holders.find({}, sort=[("blockHeight", 1)])
        if not coll_pay_tc_holders:
            log.error("BitProHoldersInterest collection not exist. Please run indexer first before running report")
            return

        count = 0
        l_pay_tc_holders = []
        titles = ['Count', 'blockNumber', 'Amount', 'Created']
        for tx in coll_pay_tc_holders:
            count += 1
            l_pay_tc_holders.append([count, tx['blockHeight'], str(float(tx['amount']) / CONTRACT_PRECISION), tx['createdAt']])

        print(tabulate(l_pay_tc_holders, headers=titles, tablefmt="pipe"))

    def report_last_transactions(self):

        # conect to mongo db
        m_client = mongo_manager.connect()

        # get collection moc_state from mongo
        collection_moc_state = mongo_manager.collection_moc_state(m_client)
        collection_transaction = mongo_manager.collection_transaction(m_client)

        info_tokens = self.coins_and_tokens()

        coll_moc_state = collection_moc_state.find_one({}, sort=[("blockHeight", -1)])
        if not coll_moc_state:
            log.error("MoC State collection not exist. Please run indexer first before running report")
            return

        historic_block_height = coll_moc_state['blockHeight'] - HISTORIC_BLOCK_HEIGHT_AMOUNT

        # Current status
        log.info("Stable Project: {0}".format(info_tokens['project_name']))
        log.info("Collateral: {0}".format(info_tokens["collateral"]))
        log.info("")
        log.info("Current block height: {0}".format(coll_moc_state['blockHeight']))
        log.info("Up to block height: {0}".format(historic_block_height))

        coll_tx = collection_transaction.find({"event": {
            "$in": ["RiskProRedeem",
                    "RiskProMint",
                    "StableTokenMint",
                    "StableTokenRedeem",
                    "FreeStableTokenRedeem",
                    "RiskProxRedeem",
                    "RiskProxMint"
                    ]
        },
            "blockNumber": {"$gte": historic_block_height}})
        if not coll_tx:
            log.error("Transaction collection not exist. Please run indexer first before running report")
            return

        count = 0
        l_transactions = []
        titles = ['Count', 'Block Nº', 'Hash', 'Address', 'Event', 'Amount', 'Created']
        for tx in coll_tx:
            count += 1

            if 'blockHeight' in tx:
                block_height = tx['blockHeight']
            else:
                block_height = ''

            l_transactions.append(
                [
                    count,
                    block_height,
                    tx['transactionHash'],
                    tx['address'],
                    OPERATIONS_TRANSLATE[tx["event"]],
                    str(float(tx['amount']) / CONTRACT_PRECISION),
                    tx['createdAt']
                ]
            )

        print(tabulate(l_transactions, headers=titles, tablefmt="pipe"))

    def report_account_transactions(self, account_address):

        # conect to mongo db
        m_client = mongo_manager.connect()

        # get collection moc_state from mongo
        collection_transaction = mongo_manager.collection_transaction(m_client)

        rdoc_token = RIFDoC(network_manager).from_abi()

        rif_token = RIF(network_manager).from_abi()

        coll_tx = collection_transaction.find({"event": {
            "$in": ["RiskProRedeem",
                    "RiskProMint",
                    "StableTokenMint",
                    "StableTokenRedeem",
                    "FreeStableTokenRedeem",
                    "RiskProxRedeem",
                    "RiskProxMint"
                    ]
            },
            "address": account_address,
            "status": "confirmed"
        })
        if not coll_tx:
            log.error("Transaction collection not exist. Please run indexer first before running report")
            return

        count = 0
        l_transactions = []
        titles = ['Count', 'Block Nº', 'Hash', 'Address', 'Event', 'Amount', 'Reserve Price', 'Reserve Amount', 'Commissions', 'RDOC Balance', 'RIF Balance', 'Created']
        for tx in coll_tx:
            count += 1

            if 'blockNumber' in tx:
                block_height = tx['blockNumber']
                rdoc_balance = rdoc_token.balance_of(account_address, block_identifier=block_height)
                rif_balance = rif_token.balance_of(account_address, block_identifier=block_height)
            else:
                block_height = ''
                rdoc_balance = 0
                rif_balance = 0

            l_transactions.append(
                [
                    count,
                    block_height,
                    tx['transactionHash'],
                    tx['address'],
                    OPERATIONS_TRANSLATE[tx["event"]],
                    str(float(tx['amount']) / CONTRACT_PRECISION),
                    str(float(tx['reservePrice']) / CONTRACT_PRECISION),
                    str(float(tx['RBTCAmount']) / CONTRACT_PRECISION),
                    str(float(tx['rbtcCommission']) / CONTRACT_PRECISION),
                    str(rdoc_balance),
                    str(rif_balance),
                    tx['createdAt']
                ]
            )

        print(tabulate(l_transactions, headers=titles, tablefmt="pipe"))



