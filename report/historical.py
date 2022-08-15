import os

from moneyonchain.networks import network_manager
from indexer.mongo_manager import mongo_manager
from indexer.logger import log

CONTRACT_PRECISION = 10 ** 18
HISTORIC_BLOCK_HEIGHT_AMOUNT = 2880 * 15


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
        log.info("")
        log.info("TVL")
        log.info("Total {0} in protocol: {1}".format(
            info_tokens["collateral"], int(coll_moc_state['b0BTCAmount']) / CONTRACT_PRECISION))
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
        log.info("")
        log.info("TVL")
        log.info("Total {0} in protocol: {1}".format(
            info_tokens["collateral"], int(coll_moc_state_historic['b0BTCAmount']) / CONTRACT_PRECISION))
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

