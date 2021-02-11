from collections import OrderedDict
from requests.exceptions import HTTPError
from web3.types import BlockIdentifier

from moneyonchain.networks import network_manager
from indexer.logger import log

from .indexer import MoCIndexer


BUCKET_X2 = '0x5832000000000000000000000000000000000000000000000000000000000000'
BUCKET_C0 = '0x4330000000000000000000000000000000000000000000000000000000000000'


class Prices(MoCIndexer):

    def prices_from_sc(self, block_identifier: BlockIdentifier = 'latest'):

        bucket_x2 = BUCKET_X2

        # get block time from node
        block_ts = network_manager.block_timestamp(block_identifier)

        d_price = OrderedDict()

        # peek = self.contract_MoCMedianizer.peek(formatted=False,
        #                                        block_identifier=block_identifier)
        #
        # d_price["bitcoinPrice"] = str(peek[0])
        # d_price["isPriceValid"] = str(peek[1])

        try:
            d_price["bitcoinPrice"] = str(self.contract_MoC.sc_moc_state.bitcoin_price(
                formatted=False,
                block_identifier=block_identifier))
        except HTTPError:
            log.error("No price valid in BLOCKHEIGHT: [{0}] skipping!".format(
                block_identifier))
            return

        d_price["bproPriceInRbtc"] = str(self.contract_MoC.sc_moc_state.bpro_tec_price(
            formatted=False,
            block_identifier=block_identifier))
        d_price["bproPriceInUsd"] = str(self.contract_MoC.sc_moc_state.bpro_price(
            formatted=False,
            block_identifier=block_identifier))

        try:
            d_price["bproDiscountPrice"] = str(
                self.contract_MoC.sc_moc_state.bpro_discount_price(
                    formatted=False,
                    block_identifier=block_identifier))
        except HTTPError:
            log.error(
                "No bproDiscountPrice valid in BLOCKHEIGHT: [{0}] skipping!".format(block_identifier))
            return

        d_price["bprox2PriceInRbtc"] = str(
            self.contract_MoC.sc_moc_state.btc2x_tec_price(
                bucket_x2,
                formatted=False,
                block_identifier=block_identifier))

        try:
            d_price["bprox2PriceInBpro"] = str(
                self.contract_MoC.sc_moc_state.bprox_price(
                    bucket_x2,
                    formatted=False,
                    block_identifier=block_identifier))
        except HTTPError:
            log.error("No bprox2PriceInBpro valid in BLOCKHEIGHT: [{0}] skipping!".format(block_identifier))
            return

        d_price["reservePrecision"] = str(self.contract_MoC.reserve_precision(
            formatted=False,
            block_identifier=block_identifier))
        d_price["bprox2PriceInUsd"] = str(
            int(d_price["bprox2PriceInRbtc"]) * int(d_price["bitcoinPrice"]) / int(
                d_price["reservePrecision"]))
        d_price["createdAt"] = block_ts

        return d_price
