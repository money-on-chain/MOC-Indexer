import boto3
import os

from moneyonchain.networks import network_manager, accounts
from moneyonchain.moc import MoC, MoCConnector, MoCState, MoCInrate, MoCSettlement
from moneyonchain.rdoc import RDOCMoC, RDOCMoCConnector, RDOCMoCState, RDOCMoCInrate, RDOCMoCSettlement
from moneyonchain.medianizer import MoCMedianizer, RDOCMoCMedianizer
from moneyonchain.tokens import BProToken
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


__VERSION__ = '3.0.0'

log.info("Starting MoC Indexer version {0}".format(__VERSION__))


class MoCIndexerTasks(TasksManager):

    def __init__(self, app_config, config_net, connection_net):

        TasksManager.__init__(self)

        self.options = app_config
        self.config_network = config_net
        self.connection_network = connection_net
        self.last_block = 0
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

        # # add default account
        # accounts.add('0xca751356c37a98109fd969d8e79b42d768587efc6ba35e878bc8c093ed95d8a9')
        # self.vendor_account = self.options['vendor_account']
        #
        # if self.app_mode == "RRC20":
        #     self.contract_MoC = RDOCMoC(
        #         network_manager,
        #         load_sub_contract=False).from_abi().contracts_discovery()
        # else:
        #     self.contract_MoC = MoC(
        #         network_manager,
        #         load_sub_contract=False).from_abi().contracts_discovery()
        #
        # if self.app_mode == "RRC20":
        #     self.contract_MoCMedianizer = RDOCMoCMedianizer(
        #         network_manager,
        #         contract_address=self.contract_MoC.sc_moc_state.price_provider()).from_abi()
        #     self.contract_ReserveToken = self.contract_MoC.sc_reserve_token
        # else:
        #     self.contract_MoCMedianizer = MoCMedianizer(
        #         network_manager,
        #         contract_address=self.contract_MoC.sc_moc_state.price_provider()).from_abi()
        #
        # if self.app_mode == "RRC20":
        #     address_bpro_token = self.options['networks'][self.config_network]['addresses']['BProToken']
        #     self.contract_MoC_BProToken = BProToken(network_manager, contract_address=address_bpro_token).from_abi()

    def reconnect_on_lost_chain(self, exit_on_error=False, task=None):

        block = network_manager.block_number

        if not self.last_block:
            log.info("Task :: Reconnect on lost chain :: Ok :: [{0}/{1}]".format(
                self.last_block, block))
            last_block = block

            return last_block

        if block <= self.last_block:
            # this means no new blocks from the last call,
            # so this means a halt node, try to reconnect.

            # this means no new blocks from the last call,
            # so this means a halt node, try to reconnect.

            log.error("Task :: Reconnect on lost chain :: "
                      "[ERROR] :: Same block from the last time! Terminate Task Manager! [{0}/{1}]".format(
                        self.last_block, block))

            # Put alarm in aws
            aws_put_metric_heart_beat(1)

            if exit_on_error:
                # terminate all tasks
                return dict(shutdown=True)

            # first disconnect
            network_manager.disconnect()

            # and then reconnect all again
            self.connect()

        log.info("Task :: Reconnect on lost chain :: Ok :: [{0}/{1}]".format(
            self.last_block, block))

        # save the last block
        self.last_block = block

        return block

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
        else:
            raise Exception("Not valid APP Mode")

        contracts_addresses['MoCMedianizer'] = contract_moc_state.price_provider()
        contracts_addresses['MoCToken'] = contract_moc_state.moc_token()
        contracts_addresses['MoCVendors'] = contract_moc_state.moc_vendors()

        if 'BProToken' in self.options['networks'][self.config_network]['addresses']:
            contracts_addresses['MoC_BProToken'] = self.options['networks'][self.config_network]['addresses']['BProToken']

        # lower case contract addresses
        contracts_addresses = {k: v.lower() for k, v in contracts_addresses.items()}

        # cache contracts already loaded
        self.contracts_loaded["MoC"] = contract_moc
        self.contracts_loaded["MoCConnector"] = conn
        self.contracts_loaded["MoCState"] = contract_moc_state
        self.contracts_loaded["MoCInrate"] = contract_moc_inrate
        self.contracts_loaded["MoCSettlement"] = contract_moc_settlement

        self.contracts_loaded["Multicall2"] = Multicall2(
            network_manager,
            contract_address='0xaf7be1ef9537018feda5397d9e3bb9a1e4e27ac8').from_abi()

        return contracts_addresses

    def task_reconnect_on_lost_chain(self, exit_on_error=False, task=None):
        """ Task reconnect when lost connection on chain """

        return self.reconnect_on_lost_chain(exit_on_error=exit_on_error, task=None)

    def schedule_tasks(self):

        log.info("Starting adding indexer tasks...")

        # creating the alarm
        aws_put_metric_heart_beat(0)

        # set max workers
        self.max_workers = 1

        # Reconnect on lost chain
        log.info("Jobs add: 99. Reconnect on lost chain")
        self.add_task(self.task_reconnect_on_lost_chain, args=[], kwargs={'exit_on_error': True}, wait=180, timeout=180)

        # # 1. Scan Blocks
        # if 'scan_moc_blocks' in self.options['tasks']:
        #     log.info("Jobs add: 1. Scan Raw Txs")
        #     interval = self.options['tasks']['scan_moc_blocks']['interval']
        #     scan_raw_txs = ScanRawTxs(self.options, self.contracts_addresses_list)
        #     self.add_task(scan_raw_txs.on_task,
        #                   args=[],
        #                   wait=interval,
        #                   timeout=180,
        #                   task_name='1. Scan Raw Txs')

        # # 1. Scan Events Txs
        # if 'scan_moc_blocks' in self.options['tasks']:
        #     log.info("Jobs add: 1. Scan Events Txs")
        #     interval = self.options['tasks']['scan_moc_blocks']['interval']
        #     scan_events_txs = ScanEventsTxs(self.options, self.app_mode, self.contracts_addresses)
        #     self.add_task(scan_events_txs.on_task,
        #                   args=[],
        #                   wait=interval,
        #                   timeout=180,
        #                   task_name='1. Scan Events Txs')

        # # 2. Scan Prices
        # if 'scan_moc_prices' in self.options['tasks']:
        #     log.info("Jobs add: 2. Scan MoC Prices")
        #     interval = self.options['tasks']['scan_moc_prices']['interval']
        #     task_scan_moc_prices = ScanMoCPrices(self.options,
        #                                          self.app_mode,
        #                                          self.contracts_loaded,
        #                                          self.contracts_addresses)
        #     self.add_task(task_scan_moc_prices.on_task,
        #                   args=[],
        #                   wait=interval,
        #                   timeout=180,
        #                   task_name='2. Scan MoC Prices')
        #
        # # 3. Scan Moc State
        # if 'scan_moc_state' in self.options['tasks']:
        #     log.info("Jobs add: 3. Scan Moc State")
        #     interval = self.options['tasks']['scan_moc_state']['interval']
        #     task_scan_moc_state = ScanMoCState(self.options,
        #                                        self.app_mode,
        #                                        self.contracts_loaded,
        #                                        self.contracts_addresses)
        #     self.add_task(task_scan_moc_state.on_task,
        #                   args=[],
        #                   wait=interval,
        #                   timeout=180,
        #                   task_name='3. Scan Moc State')

        # 4. Scan Moc Status
        if 'scan_moc_status' in self.options['tasks']:
            log.info("Jobs add: 4. Scan Transactions Status")
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
                          task_name='4. Scan Transactions Status')
        #
        # # 5. Scan MocState Status
        # if 'scan_moc_state_status' in self.options['tasks']:
        #     log.info("Jobs add: 5. Scan MocState Status")
        #     interval = self.options['tasks']['scan_moc_state_status']['interval']
        #     task_scan_moc_state_status = ScanMoCStateStatus(
        #         self.options,
        #         self.app_mode,
        #         self.contracts_loaded,
        #         self.contracts_addresses)
        #     self.add_task(task_scan_moc_state_status.on_task,
        #                   args=[],
        #                   wait=interval,
        #                   timeout=180,
        #                   task_name='5. Scan MocState Status')
        #
        # # 6. Scan User State Update
        # if 'scan_user_state_update' in self.options['tasks']:
        #     log.info("Jobs add: 6. Scan User State Update")
        #     interval = self.options['tasks']['scan_user_state_update']['interval']
        #     self.add_task(self.task_scan_user_state_update,
        #                   args=[],
        #                   wait=interval,
        #                   timeout=180,
        #                   task_name='6. Scan User State Update')
        #
        # # 7. Scan Blocks not processed
        # if 'scan_moc_blocks_not_processed' in self.options['tasks']:
        #     log.info("Jobs add: 7. Scan Blocks not processed")
        #     interval = self.options['tasks']['scan_moc_blocks_not_processed']['interval']
        #     self.add_task(self.task_scan_moc_blocks_not_processed,
        #                   args=[],
        #                   wait=interval,
        #                   timeout=180,
        #                   task_name='7. Scan Blocks not processed')

        # Set max workers
        self.max_tasks = len(self.tasks)
