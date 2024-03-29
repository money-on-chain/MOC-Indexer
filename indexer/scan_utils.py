from indexer.logger import log

from moneyonchain.networks import network_manager
from .utils import aws_put_metric_heart_beat


class BlockchainUtils:

    def __init__(self, options, config_net, connection_net, cl_task):
        self.options = options
        self.config_network = config_net
        self.connection_network = connection_net
        self.cl_task = cl_task

        self.last_block = 0

    def reconnect_on_lost_chain(self, task=None):

        block = network_manager.block_number

        if not self.last_block:
            log.info("[99. Reconnect on lost chain] :: Ok :: [{0}/{1}]".format(
                self.last_block, block))
            self.last_block = block

            return self.last_block

        if block <= self.last_block:
            # this means no new blocks from the last call,
            # so this means a halt node, try to reconnect.

            log.error("[99. Reconnect on lost chain] "
                      "[ERROR] :: Same block from the last time! Terminate Task Manager! [{0}/{1}]".format(
                self.last_block, block))

            # Put alarm in aws
            aws_put_metric_heart_beat(1)

            # first disconnect
            network_manager.disconnect()

            # and then reconnect all again
            network_manager.connect(connection_network=self.connection_network,
                                    config_network=self.config_network)

            # get addresses from connector
            self.cl_task.contracts_addresses = self.cl_task.connector_addresses()

            # get the contract addresses to list
            self.cl_task.contracts_addresses_list = list(self.cl_task.contracts_addresses.values())

        log.info("[99. Reconnect on lost chain] :: Ok :: [{0}/{1}]".format(
            self.last_block, block))

        # save the last block
        self.last_block = block

        return block

    def on_task(self, task=None):
        self.reconnect_on_lost_chain(task=task)
