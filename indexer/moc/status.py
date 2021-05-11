from collections import OrderedDict
from requests.exceptions import HTTPError
from web3.types import BlockIdentifier

from moneyonchain.networks import network_manager
from indexer.logger import log
from .indexer import MoCIndexer


BUCKET_X2 = '0x5832000000000000000000000000000000000000000000000000000000000000'
BUCKET_C0 = '0x4330000000000000000000000000000000000000000000000000000000000000'


class State(MoCIndexer):

    def moc_state_from_sc(self, block_identifier: BlockIdentifier = 'latest'):

        bucket_x2 = BUCKET_X2
        bucket_c0 = BUCKET_C0

        # get block time from node
        block_ts = network_manager.block_timestamp(block_identifier)

        d_moc_state = OrderedDict()

        # peek = self.contract_MoCMedianizer.peek(formatted=False,
        #                                        block_identifier=block_identifier)
        #
        # d_moc_state["bitcoinPrice"] = str(peek[0])
        # d_moc_state["isPriceValid"] = str(peek[1])

        try:
            d_moc_state["bitcoinPrice"] = str(
                self.contract_MoC.sc_moc_state.bitcoin_price(
                    formatted=False,
                    block_identifier=block_identifier))
        except HTTPError:
            log.error("No price valid in BLOCKHEIGHT: [{0}] skipping!".format(
                block_identifier))
            return

        d_moc_state["bproAvailableToMint"] = str(
            self.contract_MoC.sc_moc_state.max_mint_bpro_available(
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["bproAvailableToRedeem"] = str(
            self.contract_MoC.sc_moc_state.absolute_max_bpro(
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["bprox2AvailableToMint"] = str(
            self.contract_MoC.sc_moc_state.max_bprox(
                bucket_x2,
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["docAvailableToMint"] = str(
            self.contract_MoC.sc_moc_state.absolute_max_doc(
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["docAvailableToRedeem"] = str(
            self.contract_MoC.sc_moc_state.free_doc(
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["b0Leverage"] = str(
            self.contract_MoC.sc_moc_state.leverage(
                bucket_c0,
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["b0TargetCoverage"] = str(
            self.contract_MoC.sc_moc_state.cobj(
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["x2Leverage"] = str(
            self.contract_MoC.sc_moc_state.leverage(
                bucket_x2,
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["totalBTCAmount"] = str(
            self.contract_MoC.sc_moc_state.rbtc_in_system(
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["bitcoinMovingAverage"] = str(
            self.contract_MoC.sc_moc_state.bitcoin_moving_average(
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["b0BTCInrateBag"] = str(
            self.contract_MoC.sc_moc_state.get_inrate_bag(
                bucket_c0,
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["b0BTCAmount"] = str(
            self.contract_MoC.sc_moc_state.bucket_nbtc(
                bucket_c0,
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["b0DocAmount"] = str(
            self.contract_MoC.sc_moc_state.bucket_ndoc(
                bucket_c0,
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["b0BproAmount"] = str(
            self.contract_MoC.sc_moc_state.bucket_nbpro(
                bucket_c0,
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["x2BTCAmount"] = str(
            self.contract_MoC.sc_moc_state.bucket_nbtc(
                bucket_x2,
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["x2DocAmount"] = str(
            self.contract_MoC.sc_moc_state.bucket_ndoc(
                bucket_x2,
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["x2BproAmount"] = str(
            self.contract_MoC.sc_moc_state.bucket_nbpro(
                bucket_x2,
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["globalCoverage"] = str(
            self.contract_MoC.sc_moc_state.global_coverage(
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["reservePrecision"] = str(
            self.contract_MoC.reserve_precision(
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["mocPrecision"] = str(
            self.contract_MoC.sc_precision(
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["x2Coverage"] = str(
            self.contract_MoC.sc_moc_state.coverage(
                bucket_x2,
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["bproPriceInRbtc"] = str(
            self.contract_MoC.sc_moc_state.bpro_tec_price(
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["bproPriceInUsd"] = str(
            self.contract_MoC.sc_moc_state.bpro_price(
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["bproDiscountRate"] = str(
            self.contract_MoC.sc_moc_state.bpro_discount_rate(
                formatted=False,
                block_identifier=block_identifier))
        try:
            d_moc_state["mocPrice"] = str(
            self.contract_MoC.moc_price(
                formatted=False,
                block_identifier=block_identifier
            ))
        except HTTPError:
            log.error("No MOC price valid in BLOCKHEIGHT: [{0}] skipping!".format(
                block_identifier))
        try:
            d_moc_state["maxBproWithDiscount"] = str(
                self.contract_MoC.sc_moc_state.max_bpro_with_discount(
                    formatted=False,
                    block_identifier=block_identifier))
        except HTTPError:
            log.error("[WARNING] maxBproWithDiscount Exception! [{0}]".format(
                block_identifier))
            d_moc_state["maxBproWithDiscount"] = '0'

        try:
            d_moc_state["bproDiscountPrice"] = str(
                self.contract_MoC.sc_moc_state.bpro_discount_price(
                    formatted=False,
                    block_identifier=block_identifier))
        except HTTPError:
            log.error("[WARNING] bproDiscountPrice Exception! [{0}]".format(
                block_identifier))
            d_moc_state["bproDiscountPrice"] = '0'

        d_moc_state["bprox2PriceInRbtc"] = str(
            self.contract_MoC.sc_moc_state.btc2x_tec_price(
                bucket_x2,
                formatted=False,
                block_identifier=block_identifier))
        try:
            d_moc_state["bprox2PriceInBpro"] = str(
                self.contract_MoC.sc_moc_state.bprox_price(
                    bucket_x2,
                    formatted=False,
                    block_identifier=block_identifier))
        except HTTPError:
            log.error("[WARNING] bprox2PriceInBpro Exception! [{0}]".format(
                block_identifier))
            d_moc_state["bprox2PriceInBpro"] = '0'
        try:
            d_moc_state["spotInrate"] = str(
                self.contract_MoC.sc_moc_inrate.spot_inrate(
                    formatted=False,
                    block_identifier=block_identifier))
        except HTTPError:
            log.error("[WARNING] spotInrate Exception [{0}]".format(
                block_identifier))
            d_moc_state["spotInrate"] = '0'

        d_moc_state["commissionRate"] = str(
            self.contract_MoC.sc_moc_inrate.commission_rate(
                formatted=False,
                block_identifier=block_identifier))
        d_moc_state["bprox2PriceInUsd"] = str(
            int(d_moc_state["bprox2PriceInRbtc"]) * int(
                d_moc_state["bitcoinPrice"]) / int(
                d_moc_state["reservePrecision"]))
        # d_moc_state["lastUpdateHeight"] = lastUpdateHeight
        d_moc_state["createdAt"] = block_ts
        d_moc_state["dayBlockSpan"] = self.contract_MoC.sc_moc_state.day_block_span(
            block_identifier=block_identifier)
        d_moc_state["blockSpan"] = self.contract_MoC.sc_moc_settlement.block_span(
            block_identifier=block_identifier)
        d_moc_state["blocksToSettlement"] = self.contract_MoC.sc_moc_state.blocks_to_settlement(
            block_identifier=block_identifier)
        d_moc_state["state"] = self.contract_MoC.sc_moc_state.state(
            block_identifier=block_identifier)
        d_moc_state["lastPriceUpdateHeight"] = 0
        # d_moc_state["priceVariation"] = dailyPriceRef
        d_moc_state["paused"] = self.contract_MoC.paused(
            block_identifier=block_identifier)

        return d_moc_state

    def state_status_from_sc(self,
                             block_identifier: BlockIdentifier = 'latest'):

        d_status = OrderedDict()

        try:
            str(self.contract_MoC.sc_moc_state.bitcoin_price(
                formatted=False,
                block_identifier=block_identifier))
        except HTTPError:
            price_active = False
        else:
            price_active = True

        d_status['price_active'] = price_active
        d_status["paused"] = self.contract_MoC.paused(
            block_identifier=block_identifier)
        d_status["state"] = self.contract_MoC.sc_moc_state.state(
            block_identifier=block_identifier)

        return d_status
