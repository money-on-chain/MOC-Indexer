import boto3
import os

from moneyonchain.networks import network_manager, accounts
from moneyonchain.moc import MoC
from moneyonchain.rdoc import RDOCMoC
from moneyonchain.medianizer import MoCMedianizer, RDOCMoCMedianizer
from moneyonchain.tokens import BProToken


from indexer.mongo_manager import mongo_manager
from indexer.moc import ScanBlocks, \
    ScanPrices, \
    ScanState, \
    ScanStatus, \
    ScanUser

from .tasks_manager import TasksManager
from .logger import log
from .utils import aws_put_metric_heart_beat

__VERSION__ = '2.1.0'

log.info("Starting MoC Indexer version {0}".format(__VERSION__))


class JobsIndexer(ScanBlocks, ScanPrices, ScanState, ScanStatus, ScanUser):

    def __init__(self, *tx_args, **tx_vars):

        super(ScanBlocks, self).__init__(*tx_args, **tx_vars)
        super(ScanPrices, self).__init__(*tx_args, **tx_vars)
        super(ScanState, self).__init__(*tx_args, **tx_vars)
        super(ScanStatus, self).__init__(*tx_args, **tx_vars)
        super(ScanUser, self).__init__(*tx_args, **tx_vars)


class MoCIndexerTasks(TasksManager, JobsIndexer):

    def __init__(self, app_config, config_net, connection_net):

        TasksManager.__init__(self)

        self.options = app_config
        self.config_network = config_net
        self.connection_network = connection_net
        self.last_block = 0

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

        # initialize mongo db
        mongo_manager.set_connection(uri=self.options['mongo']['uri'], db=self.options['mongo']['db'])

        if 'AWS_ACCESS_KEY_ID' in os.environ:
            # Create CloudWatch client
            self.cloudwatch = boto3.client('cloudwatch')

        # super JobIndexer
        super(JobsIndexer, self).__init__()

        # Add tasks
        self.schedule_tasks()

    def connect(self):
        """ Init connection"""

        # connection network is the brownie connection network
        # config network is our enviroment we want to connect
        network_manager.connect(connection_network=self.connection_network,
                                config_network=self.config_network)

        # add default account
        accounts.add('0xca751356c37a98109fd969d8e79b42d768587efc6ba35e878bc8c093ed95d8a9')
        self.vendor_account = self.options['vendor_account']

        if self.app_mode == "RRC20":
            self.contract_MoC = RDOCMoC(
                network_manager,
                load_sub_contract=False).from_abi().contracts_discovery()
        else:
            self.contract_MoC = MoC(
                network_manager,
                load_sub_contract=False).from_abi().contracts_discovery()

        if self.app_mode == "RRC20":
            self.contract_MoCMedianizer = RDOCMoCMedianizer(
                network_manager,
                contract_address=self.contract_MoC.sc_moc_state.price_provider()).from_abi()
            self.contract_ReserveToken = self.contract_MoC.sc_reserve_token
        else:
            self.contract_MoCMedianizer = MoCMedianizer(
                network_manager,
                contract_address=self.contract_MoC.sc_moc_state.price_provider()).from_abi()

        if self.app_mode == "RRC20":
            address_bpro_token = self.options['networks'][self.config_network]['addresses']['BProToken']
            self.contract_MoC_BProToken = BProToken(network_manager, contract_address=address_bpro_token).from_abi()

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

    def moc_contract_addresses(self):

        moc_addresses = list()
        moc_addresses.append(
            str.lower(self.contract_MoC.address()))
        moc_addresses.append(
            str.lower(self.contract_MoC.sc_moc_settlement.address()))
        moc_addresses.append(
            str.lower(self.contract_MoC.sc_moc_exchange.address()))
        moc_addresses.append(
            str.lower(self.contract_MoC.sc_moc_bpro_token.address()))
        moc_addresses.append(
            str.lower(self.contract_MoC.sc_moc_doc_token.address()))
        moc_addresses.append(
            str.lower(self.contract_MoC.sc_moc_state.address()))
        moc_addresses.append(
            str.lower(self.contract_MoC.sc_moc_inrate.address()))
        moc_addresses.append(
            str.lower(self.contract_MoCMedianizer.address()))
        moc_addresses.append(
            str.lower(self.contract_MoC.sc_moc_vendors.address()))
        moc_addresses.append(
            str.lower(self.contract_MoC.sc_moc_moc_token.address()))

        if self.app_mode == 'RRC20':
            moc_addresses.append(
                str.lower(self.contract_ReserveToken.address()))

        return moc_addresses

    def task_scan_moc_blocks(self, task=None):

        return self.scan_moc_blocks(task=task)

    def task_scan_moc_blocks_history(self, task=None):

        return self.scan_moc_blocks_history(task=task)

    def task_scan_moc_prices(self, task=None):

        return self.scan_moc_prices(task=task)

    def task_scan_moc_prices_history(self, task=None):

        return self.scan_moc_prices_history(task=task)

    def task_scan_moc_state(self, task=None):

        return self.scan_moc_state(task=task)

    def task_scan_moc_state_history(self, task=None):

        return self.scan_moc_state_history(task=task)

    def task_scan_moc_status(self, task=None):

        return self.scan_transaction_status(task=task)

    def task_scan_moc_state_status(self, task=None):

        return self.scan_moc_state_status(task=task)

    def task_scan_moc_state_status_history(self, task=None):

        return self.scan_moc_state_status_history(task=task)

    def task_scan_user_state_update(self, task=None):

        return self.scan_user_state_update(task=task)

    def task_scan_moc_blocks_not_processed(self, task=None):

        return self.scan_moc_blocks_not_processed(task=task)

    def task_reconnect_on_lost_chain(self, exit_on_error=False, task=None):
        """ Task reconnect when lost connection on chain """

        return self.reconnect_on_lost_chain(exit_on_error=exit_on_error, task=None)

    def schedule_tasks(self):

        log.info("Starting adding indexer tasks...")

        # creating the alarm
        aws_put_metric_heart_beat(0)

        # set max workers
        self.max_workers = 4

        # Reconnect on lost chain
        log.info("Jobs add: 99. Reconnect on lost chain")
        self.add_task(self.task_reconnect_on_lost_chain, args=[], kwargs={'exit_on_error': True}, wait=180, timeout=180)

        # 1. Scan Blocks
        if 'scan_moc_blocks' in self.options['tasks']:
            log.info("Jobs add: 1. Scan Blocks")
            interval = self.options['tasks']['scan_moc_blocks']['interval']
            self.add_task(self.task_scan_moc_blocks,
                          args=[],
                          wait=interval,
                          timeout=180,
                          task_name='1. Scan Blocks')

        # # 2. Scan Prices
        # if 'scan_moc_prices' in self.options['tasks']:
        #     log.info("Jobs add: 2. Scan Prices")
        #     interval = self.options['tasks']['scan_moc_prices']['interval']
        #     self.add_task(self.task_scan_moc_prices,
        #                   args=[],
        #                   wait=interval,
        #                   timeout=180,
        #                   task_name='2. Scan Prices')
        #
        # # 3. Scan Moc State
        # if 'scan_moc_state' in self.options['tasks']:
        #     log.info("Jobs add: 3. Scan Moc State")
        #     interval = self.options['tasks']['scan_moc_state']['interval']
        #     self.add_task(self.task_scan_moc_state,
        #                   args=[],
        #                   wait=interval,
        #                   timeout=180,
        #                   task_name='3. Scan Moc State')
        #
        # # 4. Scan Moc Status
        # if 'scan_moc_status' in self.options['tasks']:
        #     log.info("Jobs add: 4. Scan Moc Status")
        #     interval = self.options['tasks']['scan_moc_status']['interval']
        #     self.add_task(self.task_scan_moc_status,
        #                   args=[],
        #                   wait=interval,
        #                   timeout=180,
        #                   task_name='4. Scan Moc Status')
        #
        # # 5. Scan MocState Status
        # if 'scan_moc_state_status' in self.options['tasks']:
        #     log.info("Jobs add: 5. Scan MocState Status")
        #     interval = self.options['tasks']['scan_moc_state_status']['interval']
        #     self.add_task(self.task_scan_moc_state_status,
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
