import boto3

from moneyonchain.networks import network_manager

from moneyonchain.moc import MoC
from moneyonchain.rdoc import RDOCMoC
from moneyonchain.medianizer import MoCMedianizer, RDOCMoCMedianizer


from indexer.mongo_manager import mongo_manager

import logging
import logging.config


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

log = logging.getLogger('default')


class MoCIndexer(object):

    precision = 10 ** 18

    def __init__(self, config_app, config_net, connection_net):
        self.options = config_app
        self.config_network = config_net
        self.connection_network = connection_net

        # connection network is the brownie connection network
        # config network is our enviroment we want to connect
        network_manager.connect(connection_network=self.connection_network,
                                config_network=self.config_network)

        self.app_mode = self.options['networks'][self.config_network]['app_mode']
        self.debug_mode = self.options.get('debug', False)

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

        # initialize mongo db
        mongo_manager.set_connection(uri=self.options['mongo']['uri'], db=self.options['mongo']['db'])

        # Create CloudWatch client
        self.cloudwatch = boto3.client('cloudwatch')

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

        if self.app_mode == 'RRC20':
            moc_addresses.append(
                str.lower(self.contract_ReserveToken.address()))

        return moc_addresses
