import boto3
import os

from moneyonchain.networks import network_manager
from moneyonchain.moc import MoC, MoCConnector, MoCState, MoCInrate, MoCSettlement
from moneyonchain.rdoc import RDOCMoC, RDOCMoCConnector, RDOCMoCState, RDOCMoCInrate, RDOCMoCSettlement
from moneyonchain.tokens import BProToken, DoCToken, StableToken, RiskProToken, MoCToken, ReserveToken
from moneyonchain.multicall import Multicall2

from indexer.mongo_manager import mongo_manager

from .tasks_manager import TasksManager
from .logger import log
from .utils import aws_put_metric_heart_beat
from .scan_raw_txs import ScanRawTxs
from .scan_events_txs import ScanEventsTxs
from .scan_moc_prices import ScanMoCPrices
from .scan_moc_state import ScanMoCState
from .scan_moc_state_status import ScanMoCStateStatus
from .scan_transaction_status import ScanTransactionStatus
from .scan_moc_user import ScanUser
from .scan_utils import BlockchainUtils


__VERSION__ = '3.0.0'

log.info("Starting MoC Indexer version {0}".format(__VERSION__))


class MoCIndexerTasks(TasksManager):

    def __init__(self, app_config, config_net, connection_net):

        TasksManager.__init__(self)

        self.options = app_config
        self.config_network = config_net
        self.connection_network = connection_net

        self.contracts_loaded = dict()

        self.app_mode = self.options['networks'][self.config_network]['app_mode']
        self.debug_mode = self.options.get('debug', False)

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

        # get addresses from connector
        self.contracts_addresses = self.connector_addresses()

        # get the contract addresses to list
        self.contracts_addresses_list = list(self.contracts_addresses.values())

        # initialize mongo db
        mongo_manager.set_connection(uri=self.options['mongo']['uri'], db=self.options['mongo']['db'])

        if 'AWS_ACCESS_KEY_ID' in os.environ:
            # Create CloudWatch client
            self.cloudwatch = boto3.client('cloudwatch')

        # Add tasks
        self.schedule_tasks()

    def connect(self):
        """ Init connection"""

        # connection network is the brownie connection network
        # config network is our enviroment we want to connect
        network_manager.connect(connection_network=self.connection_network,
                                config_network=self.config_network)

    def connector_addresses(self):
        """ Get contract address to use later """

        log.info("Getting addresses from Main Contract...")

        moc_address = self.options['networks'][network_manager.config_network]['addresses']['MoC']

        if self.app_mode == 'RRC20':
            contract_moc = RDOCMoC(
                network_manager,
                contract_address=moc_address,
                load_sub_contract=False).from_abi()
        elif self.app_mode == 'MoC':
            contract_moc = MoC(
                network_manager,
                contract_address=moc_address,
                load_sub_contract=False).from_abi()
        else:
            raise Exception("Not valid APP Mode")

        contracts_addresses = dict()
        contracts_addresses['MoCConnector'] = contract_moc.connector()

        if self.app_mode == 'RRC20':
            conn = RDOCMoCConnector(network_manager, contract_address=contracts_addresses['MoCConnector']).from_abi()
        elif self.app_mode == 'MoC':
            conn = MoCConnector(network_manager, contract_address=contracts_addresses['MoCConnector']).from_abi()
        else:
            raise Exception("Not valid APP Mode")

        contracts_addresses = conn.contracts_addresses()

        # Get oracle address from moc_state
        if self.app_mode == 'RRC20':
            contract_moc_state = RDOCMoCState(
                network_manager,
                contract_address=contracts_addresses['MoCState']).from_abi()
            contract_moc_inrate = RDOCMoCInrate(
                network_manager,
                contract_address=contracts_addresses['MoCInrate']).from_abi()
            contract_moc_settlement = RDOCMoCSettlement(
                network_manager,
                contract_address=contracts_addresses['MoCSettlement']).from_abi()
            contract_bpro_token = RiskProToken(
                network_manager,
                contract_address=contracts_addresses['BProToken']).from_abi()
            contract_doc_token = StableToken(
                network_manager,
                contract_address=contracts_addresses['DoCToken']).from_abi()
            self.contracts_loaded["ReserveToken"] = ReserveToken(
                network_manager,
                contract_address=contracts_addresses['ReserveToken']).from_abi()
        elif self.app_mode == 'MoC':
            contract_moc_state = MoCState(
                network_manager,
                contract_address=contracts_addresses['MoCState']).from_abi()
            contract_moc_inrate = MoCInrate(
                network_manager,
                contract_address=contracts_addresses['MoCInrate']).from_abi()
            contract_moc_settlement = MoCSettlement(
                network_manager,
                contract_address=contracts_addresses['MoCSettlement']).from_abi()
            contract_bpro_token = BProToken(
                network_manager,
                contract_address=contracts_addresses['BProToken']).from_abi()
            contract_doc_token = DoCToken(
                network_manager,
                contract_address=contracts_addresses['DoCToken']).from_abi()
        else:
            raise Exception("Not valid APP Mode")

        # Load MoC Token
        contract_moc_token = MoCToken(
            network_manager,
            contract_address=contract_moc_state.moc_token()).from_abi()

        contracts_addresses['MoCMedianizer'] = contract_moc_state.price_provider()
        contracts_addresses['MoCToken'] = contract_moc_state.moc_token()
        contracts_addresses['MoCVendors'] = contract_moc_state.moc_vendors()
        contracts_addresses['Multicall2'] = self.options['networks'][self.config_network]['addresses']['Multicall2']

        if 'BProToken' in self.options['networks'][self.config_network]['addresses']:
            contracts_addresses['MoC_BProToken'] = self.options['networks'][self.config_network]['addresses']['BProToken']
            self.contracts_loaded["MoC_BProToken"] = BProToken(
                network_manager,
                contract_address=contracts_addresses['MoC_BProToken']).from_abi()

        # lower case contract addresses
        contracts_addresses = {k: v.lower() for k, v in contracts_addresses.items()}

        # cache contracts already loaded
        self.contracts_loaded["MoC"] = contract_moc
        self.contracts_loaded["MoCConnector"] = conn
        self.contracts_loaded["MoCState"] = contract_moc_state
        self.contracts_loaded["MoCInrate"] = contract_moc_inrate
        self.contracts_loaded["MoCSettlement"] = contract_moc_settlement
        self.contracts_loaded["BProToken"] = contract_bpro_token
        self.contracts_loaded["DoCToken"] = contract_doc_token
        self.contracts_loaded["MoCToken"] = contract_moc_token

        # Multicall
        self.contracts_loaded["Multicall2"] = Multicall2(
            network_manager,
            contract_address=contracts_addresses['Multicall2']).from_abi()

        return contracts_addresses

    def schedule_tasks(self):

        log.info("Starting adding indexer tasks...")

        # creating the alarm
        aws_put_metric_heart_beat(0)

        # set max workers
        self.max_workers = 1

        # Reconnect on lost chain
        log.info("Jobs add: 99. Reconnect on lost chain")
        task_reconnect_on_lost_chain = BlockchainUtils(self.options, self.config_network, self.connection_network)
        self.add_task(task_reconnect_on_lost_chain.on_task, args=[], wait=180, timeout=180)

        # 1. Scan Blocks
        if 'scan_moc_blocks' in self.options['tasks']:
            log.info("Jobs add: 1. Scan Raw Txs")
            interval = self.options['tasks']['scan_moc_blocks']['interval']
            scan_raw_txs = ScanRawTxs(self.options, self.contracts_addresses_list)
            self.add_task(scan_raw_txs.on_task,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='1. Scan Raw Txs')

        # 2. Scan Events Txs
        if 'scan_moc_events' in self.options['tasks']:
            log.info("Jobs add: 2. Scan Events Txs")
            interval = self.options['tasks']['scan_moc_events']['interval']
            scan_events_txs = ScanEventsTxs(
                self.options,
                self.app_mode,
                self.contracts_addresses)
            self.add_task(scan_events_txs.on_task,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='2. Scan Events Txs')

        # 3. Scan Prices
        if 'scan_moc_prices' in self.options['tasks']:
            log.info("Jobs add: 3. Scan MoC Prices")
            interval = self.options['tasks']['scan_moc_prices']['interval']
            task_scan_moc_prices = ScanMoCPrices(self.options,
                                                 self.app_mode,
                                                 self.contracts_loaded,
                                                 self.contracts_addresses)
            self.add_task(task_scan_moc_prices.on_task,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='3. Scan MoC Prices')

        # 4. Scan Moc State
        if 'scan_moc_state' in self.options['tasks']:
            log.info("Jobs add: 4. Scan Moc State")
            interval = self.options['tasks']['scan_moc_state']['interval']
            task_scan_moc_state = ScanMoCState(self.options,
                                               self.app_mode,
                                               self.contracts_loaded,
                                               self.contracts_addresses)
            self.add_task(task_scan_moc_state.on_task,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='4. Scan Moc State')

        # 5. Scan Moc Status
        if 'scan_moc_status' in self.options['tasks']:
            log.info("Jobs add: 5. Scan Transactions Status")
            interval = self.options['tasks']['scan_moc_status']['interval']
            task_scan_transaction_status = ScanTransactionStatus(
                self.options,
                self.app_mode,
                self.contracts_loaded,
                self.contracts_addresses)
            self.add_task(task_scan_transaction_status.on_task,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='5. Scan Transactions Status')

        # 6. Scan MocState Status
        if 'scan_moc_state_status' in self.options['tasks']:
            log.info("Jobs add: 6. Scan MocState Status")
            interval = self.options['tasks']['scan_moc_state_status']['interval']
            task_scan_moc_state_status = ScanMoCStateStatus(
                self.options,
                self.app_mode,
                self.contracts_loaded,
                self.contracts_addresses)
            self.add_task(task_scan_moc_state_status.on_task,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='6. Scan MocState Status')

        # 7. Scan User State Update
        if 'scan_user_state_update' in self.options['tasks']:
            log.info("Jobs add: 7. Scan User State Update")
            interval = self.options['tasks']['scan_user_state_update']['interval']
            task_scan_user_state_update = ScanUser(
                self.options,
                self.app_mode,
                self.contracts_loaded,
                self.contracts_addresses)
            self.add_task(task_scan_user_state_update.on_task,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='7. Scan User State Update')

        # 8. Scan Blocks not processed
        if 'scan_moc_blocks_not_processed' in self.options['tasks']:
            log.info("Jobs add: 8. Scan Blocks not processed")
            interval = self.options['tasks']['scan_moc_blocks_not_processed']['interval']
            task_scan_moc_blocks_not_processed = ScanEventsTxs(self.options, self.app_mode, self.contracts_addresses)
            self.add_task(task_scan_moc_blocks_not_processed.on_task_not_processed,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='8. Scan Blocks not processed')

        # Set max workers
        self.max_tasks = len(self.tasks)
