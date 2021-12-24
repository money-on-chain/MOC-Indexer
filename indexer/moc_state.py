from collections import OrderedDict
from requests.exceptions import HTTPError
from web3.types import BlockIdentifier

from moneyonchain.networks import network_manager
from indexer.logger import log

BUCKET_X2 = '0x5832000000000000000000000000000000000000000000000000000000000000'
BUCKET_C0 = '0x4330000000000000000000000000000000000000000000000000000000000000'


def moc_state_from_sc(contract_loaded, contract_addresses, block_identifier: BlockIdentifier = 'latest'):

    d_moc_state = OrderedDict()

    moc_state = contract_loaded["MoCState"]
    moc_state_address = contract_addresses["MoCState"]
    moc = contract_loaded["MoC"]
    moc_address = contract_addresses["MoC"]
    moc_inrate = contract_loaded["MoCInrate"]
    moc_inrate_address = contract_addresses["MoCInrate"]
    moc_settlement = contract_loaded["MoCSettlement"]
    moc_settlement_address = contract_addresses["MoCSettlement"]
    multicall = contract_loaded["Multicall2"]

    list_aggregate = list()
    list_aggregate.append((moc_state_address, moc_state.sc.getBitcoinPrice, [], lambda x: str(x)))  # 0
    list_aggregate.append((moc_state_address, moc_state.sc.getMoCPrice, [], lambda x: str(x)))  # 1
    list_aggregate.append((moc_state_address, moc_state.sc.absoluteMaxBPro, [], lambda x: str(x)))  # 2
    list_aggregate.append((moc_state_address, moc_state.sc.maxBProx, [BUCKET_X2], lambda x: str(x)))  # 3
    list_aggregate.append((moc_state_address, moc_state.sc.absoluteMaxDoc, [], lambda x: str(x)))  # 4
    list_aggregate.append((moc_state_address, moc_state.sc.freeDoc, [], lambda x: str(x)))  # 5
    list_aggregate.append((moc_state_address, moc_state.sc.leverage, [BUCKET_C0], lambda x: str(x)))   # 6
    list_aggregate.append((moc_state_address, moc_state.sc.cobj, [], lambda x: str(x)))  # 7
    list_aggregate.append((moc_state_address, moc_state.sc.leverage, [BUCKET_X2], lambda x: str(x)))  # 8
    list_aggregate.append((moc_state_address, moc_state.sc.rbtcInSystem, [], lambda x: str(x)))  # 9
    list_aggregate.append((moc_state_address, moc_state.sc.getBitcoinMovingAverage, [], lambda x: str(x)))  # 10
    list_aggregate.append((moc_state_address, moc_state.sc.getInrateBag, [BUCKET_C0], lambda x: str(x)))  # 11
    list_aggregate.append((moc_state_address, moc_state.sc.getBucketNBTC, [BUCKET_C0], lambda x: str(x)))  # 12
    list_aggregate.append((moc_state_address, moc_state.sc.getBucketNDoc, [BUCKET_C0], lambda x: str(x)))  # 13
    list_aggregate.append((moc_state_address, moc_state.sc.getBucketNBPro, [BUCKET_C0], lambda x: str(x)))  # 14
    list_aggregate.append((moc_state_address, moc_state.sc.getBucketNBTC, [BUCKET_X2], lambda x: str(x)))  # 15
    list_aggregate.append((moc_state_address, moc_state.sc.getBucketNDoc, [BUCKET_X2], lambda x: str(x)))  # 16
    list_aggregate.append((moc_state_address, moc_state.sc.getBucketNBPro, [BUCKET_X2], lambda x: str(x)))  # 17
    list_aggregate.append((moc_state_address, moc_state.sc.globalCoverage, [], lambda x: str(x)))  # 18
    list_aggregate.append((moc_address, moc.sc.getReservePrecision, [], lambda x: str(x)))  # 19
    list_aggregate.append((moc_address, moc.sc.getMocPrecision, [], lambda x: str(x)))  # 20
    list_aggregate.append((moc_state_address, moc_state.sc.coverage, [BUCKET_X2], lambda x: str(x)))  # 21
    list_aggregate.append((moc_state_address, moc_state.sc.bproTecPrice, [], lambda x: str(x)))  # 22
    list_aggregate.append((moc_state_address, moc_state.sc.bproUsdPrice, [], lambda x: str(x)))  # 23
    list_aggregate.append((moc_state_address, moc_state.sc.bproSpotDiscountRate, [], lambda x: str(x)))  # 24
    list_aggregate.append((moc_state_address, moc_state.sc.maxBProWithDiscount, [], lambda x: str(x)))  # 25
    list_aggregate.append((moc_state_address, moc_state.sc.bproDiscountPrice, [], lambda x: str(x)))  # 26
    list_aggregate.append((moc_state_address, moc_state.sc.bucketBProTecPrice, [BUCKET_X2], lambda x: str(x)))  # 27
    list_aggregate.append((moc_state_address, moc_state.sc.bproxBProPrice, [BUCKET_X2], lambda x: str(x)))  # 28
    list_aggregate.append((moc_inrate_address, moc_inrate.sc.spotInrate, [], lambda x: str(x)))  # 29
    list_aggregate.append((moc_inrate_address, moc_inrate.sc.MINT_BPRO_FEES_RBTC, [], lambda x: str(x)))   # 30
    list_aggregate.append((moc_inrate_address, moc_inrate.sc.REDEEM_BPRO_FEES_RBTC, [], lambda x: str(x)))  # 31
    list_aggregate.append((moc_inrate_address, moc_inrate.sc.MINT_DOC_FEES_RBTC, [], lambda x: str(x)))  # 32
    list_aggregate.append((moc_inrate_address, moc_inrate.sc.REDEEM_DOC_FEES_RBTC, [], lambda x: str(x)))  # 33
    list_aggregate.append((moc_inrate_address, moc_inrate.sc.MINT_BTCX_FEES_RBTC, [], lambda x: str(x)))  # 34
    list_aggregate.append((moc_inrate_address, moc_inrate.sc.REDEEM_BTCX_FEES_RBTC, [], lambda x: str(x)))  # 35
    list_aggregate.append((moc_inrate_address, moc_inrate.sc.MINT_BPRO_FEES_MOC, [], lambda x: str(x)))  # 36
    list_aggregate.append((moc_inrate_address, moc_inrate.sc.REDEEM_BPRO_FEES_MOC, [], lambda x: str(x)))  # 37
    list_aggregate.append((moc_inrate_address, moc_inrate.sc.MINT_DOC_FEES_MOC, [], lambda x: str(x)))  # 38
    list_aggregate.append((moc_inrate_address, moc_inrate.sc.REDEEM_DOC_FEES_MOC, [], lambda x: str(x)))  # 39
    list_aggregate.append((moc_inrate_address, moc_inrate.sc.MINT_BTCX_FEES_MOC, [], lambda x: str(x)))  # 40
    list_aggregate.append((moc_inrate_address, moc_inrate.sc.REDEEM_BTCX_FEES_MOC, [], lambda x: str(x)))  # 41
    list_aggregate.append((moc_state_address, moc_state.sc.dayBlockSpan, [], lambda x: x))  # 42
    list_aggregate.append((moc_settlement_address, moc_settlement.sc.getBlockSpan, [], lambda x: x))  # 43
    list_aggregate.append((moc_state_address, moc_state.sc.blocksToSettlement, [], lambda x: x))  # 44
    list_aggregate.append((moc_state_address, moc_state.sc.state, [], lambda x: x))  # 45
    list_aggregate.append((moc_address, moc.sc.paused, [], lambda x: x))  # 46
    list_aggregate.append((moc_state_address, moc_state.sc.getLiquidationEnabled, [], lambda x: x))  # 47
    list_aggregate.append((moc_state_address, moc_state.sc.getProtected, [], lambda x: str(x)))  # 48

    results = multicall.aggregate_multiple(list_aggregate, block_identifier=block_identifier)

    block_number = results[0]

    # get block time from node
    block_ts = network_manager.block_timestamp(block_number)

    d_moc_state["blockHeight"] = block_number
    d_moc_state["createdAt"] = block_ts

    d_moc_state["bitcoinPrice"] = results[1][0]
    d_moc_state["mocPrice"] = results[1][1]
    d_moc_state["bproAvailableToRedeem"] = results[1][2]
    d_moc_state["bprox2AvailableToMint"] = results[1][3]
    d_moc_state["docAvailableToMint"] = results[1][4]
    d_moc_state["docAvailableToRedeem"] = results[1][5]
    d_moc_state["b0Leverage"] = results[1][6]
    d_moc_state["b0TargetCoverage"] = results[1][7]
    d_moc_state["x2Leverage"] = results[1][8]
    d_moc_state["totalBTCAmount"] = results[1][9]
    d_moc_state["bitcoinMovingAverage"] = results[1][10]
    d_moc_state["b0BTCInrateBag"] = results[1][11]
    d_moc_state["b0BTCAmount"] = results[1][12]
    d_moc_state["b0DocAmount"] = results[1][13]
    d_moc_state["b0BproAmount"] = results[1][14]
    d_moc_state["x2BTCAmount"] = results[1][15]
    d_moc_state["x2DocAmount"] = results[1][16]
    d_moc_state["x2BproAmount"] = results[1][17]
    d_moc_state["globalCoverage"] = results[1][18]
    d_moc_state["reservePrecision"] = results[1][19]
    d_moc_state["mocPrecision"] = results[1][20]
    d_moc_state["x2Coverage"] = results[1][21]
    d_moc_state["bproPriceInRbtc"] = results[1][22]
    d_moc_state["bproPriceInUsd"] = results[1][23]
    d_moc_state["bproDiscountRate"] = results[1][24]
    d_moc_state["maxBproWithDiscount"] = results[1][25]
    d_moc_state["bproDiscountPrice"] = results[1][26]
    d_moc_state["bprox2PriceInRbtc"] = results[1][27]
    d_moc_state["bprox2PriceInBpro"] = results[1][28]
    d_moc_state["spotInrate"] = results[1][29]

    # Start: Commission rates by transaction types
    commission_rates = dict()
    commission_rates["MINT_BPRO_FEES_RBTC"] = results[1][30]
    commission_rates["REDEEM_BPRO_FEES_RBTC"] = results[1][31]
    commission_rates["MINT_DOC_FEES_RBTC"] = results[1][32]
    commission_rates["REDEEM_DOC_FEES_RBTC"] = results[1][33]
    commission_rates["MINT_BTCX_FEES_RBTC"] = results[1][34]
    commission_rates["REDEEM_BTCX_FEES_RBTC"] = results[1][35]
    commission_rates["MINT_BPRO_FEES_MOC"] = results[1][36]
    commission_rates["REDEEM_BPRO_FEES_MOC"] = results[1][37]
    commission_rates["MINT_DOC_FEES_MOC"] = results[1][38]
    commission_rates["REDEEM_DOC_FEES_MOC"] = results[1][39]
    commission_rates["MINT_BTCX_FEES_MOC"] = results[1][40]
    commission_rates["REDEEM_BTCX_FEES_MOC"] = results[1][41]

    d_moc_state["commissionRates"] = commission_rates
    # End: Commission rates by transaction types

    d_moc_state["bprox2PriceInUsd"] = str(
        int(d_moc_state["bprox2PriceInRbtc"]) * int(
            d_moc_state["bitcoinPrice"]) / int(
            d_moc_state["reservePrecision"]))

    d_moc_state["dayBlockSpan"] = results[1][42]
    d_moc_state["blockSpan"] = results[1][43]
    d_moc_state["blocksToSettlement"] = results[1][44]
    d_moc_state["state"] = results[1][45]
    d_moc_state["lastPriceUpdateHeight"] = 0
    d_moc_state["paused"] = results[1][46]
    d_moc_state["liquidationEnabled"] = results[1][47]
    d_moc_state["protected"] = results[1][48]


    # peek = self.contract_MoCMedianizer.peek(formatted=False,
    #                                        block_identifier=block_identifier)
    #
    # d_moc_state["bitcoinPrice"] = str(peek[0])
    # d_moc_state["isPriceValid"] = str(peek[1])

    # try:
    #     d_moc_state["bitcoinPrice"] = str(
    #         self.contract_MoC.sc_moc_state.bitcoin_price(
    #             formatted=False,
    #             block_identifier=block_identifier))
    # except (HTTPError, ValueError):
    #     log.error("No price valid in BLOCKHEIGHT: [{0}] skipping!".format(
    #         block_identifier))
    #     return
    #
    # try:
    #     d_moc_state["mocPrice"] = str(self.contract_MoC.sc_moc_state.moc_price(
    #         formatted=False,
    #         block_identifier=block_identifier))
    # except (HTTPError, ValueError):
    #     log.error("No price valid for MoC in BLOCKHEIGHT: [{0}] skipping!".format(block_identifier))
    #     return
    #
    # d_moc_state["bproAvailableToRedeem"] = str(
    #     self.contract_MoC.sc_moc_state.absolute_max_bpro(
    #         formatted=False,
    #         block_identifier=block_identifier))
    # d_moc_state["bprox2AvailableToMint"] = str(
    #     self.contract_MoC.sc_moc_state.max_bprox(
    #         bucket_x2,
    #         formatted=False,
    #         block_identifier=block_identifier))
    # d_moc_state["docAvailableToMint"] = str(
    #     self.contract_MoC.sc_moc_state.absolute_max_doc(
    #         formatted=False,
    #         block_identifier=block_identifier))
    # d_moc_state["docAvailableToRedeem"] = str(
    #     self.contract_MoC.sc_moc_state.free_doc(
    #         formatted=False,
    #         block_identifier=block_identifier))
    # d_moc_state["b0Leverage"] = str(
    #     self.contract_MoC.sc_moc_state.leverage(
    #         bucket_c0,
    #         formatted=False,
    #         block_identifier=block_identifier))
    # d_moc_state["b0TargetCoverage"] = str(
    #     self.contract_MoC.sc_moc_state.cobj(
    #         formatted=False,
    #         block_identifier=block_identifier))
    # d_moc_state["x2Leverage"] = str(
    #     self.contract_MoC.sc_moc_state.leverage(
    #         bucket_x2,
    #         formatted=False,
    #         block_identifier=block_identifier))
    # d_moc_state["totalBTCAmount"] = str(
    #     self.contract_MoC.sc_moc_state.rbtc_in_system(
    #         formatted=False,
    #         block_identifier=block_identifier))
    # d_moc_state["bitcoinMovingAverage"] = str(
    #     self.contract_MoC.sc_moc_state.bitcoin_moving_average(
    #         formatted=False,
    #         block_identifier=block_identifier))
    # d_moc_state["b0BTCInrateBag"] = str(
    #     self.contract_MoC.sc_moc_state.get_inrate_bag(
    #         bucket_c0,
    #         formatted=False,
    #         block_identifier=block_identifier))
    # d_moc_state["b0BTCAmount"] = str(
    #     self.contract_MoC.sc_moc_state.bucket_nbtc(
    #         bucket_c0,
    #         formatted=False,
    #         block_identifier=block_identifier))
    # d_moc_state["b0DocAmount"] = str(
    #     self.contract_MoC.sc_moc_state.bucket_ndoc(
    #         bucket_c0,
    #         formatted=False,
    #         block_identifier=block_identifier))
    # d_moc_state["b0BproAmount"] = str(
    #     self.contract_MoC.sc_moc_state.bucket_nbpro(
    #         bucket_c0,
    #         formatted=False,
    #         block_identifier=block_identifier))
    # d_moc_state["x2BTCAmount"] = str(
    #     self.contract_MoC.sc_moc_state.bucket_nbtc(
    #         bucket_x2,
    #         formatted=False,
    #         block_identifier=block_identifier))
    # d_moc_state["x2DocAmount"] = str(
    #     self.contract_MoC.sc_moc_state.bucket_ndoc(
    #         bucket_x2,
    #         formatted=False,
    #         block_identifier=block_identifier))
    # d_moc_state["x2BproAmount"] = str(
    #     self.contract_MoC.sc_moc_state.bucket_nbpro(
    #         bucket_x2,
    #         formatted=False,
    #         block_identifier=block_identifier))
    # d_moc_state["globalCoverage"] = str(
    #     self.contract_MoC.sc_moc_state.global_coverage(
    #         formatted=False,
    #         block_identifier=block_identifier))
    # d_moc_state["reservePrecision"] = str(
    #     self.contract_MoC.reserve_precision(
    #         formatted=False,
    #         block_identifier=block_identifier))
    # d_moc_state["mocPrecision"] = str(
    #     self.contract_MoC.sc_precision(
    #         formatted=False,
    #         block_identifier=block_identifier))
    # d_moc_state["x2Coverage"] = str(
    #     self.contract_MoC.sc_moc_state.coverage(
    #         bucket_x2,
    #         formatted=False,
    #         block_identifier=block_identifier))
    # d_moc_state["bproPriceInRbtc"] = str(
    #     self.contract_MoC.sc_moc_state.bpro_tec_price(
    #         formatted=False,
    #         block_identifier=block_identifier))
    # d_moc_state["bproPriceInUsd"] = str(
    #     self.contract_MoC.sc_moc_state.bpro_price(
    #         formatted=False,
    #         block_identifier=block_identifier))
    # d_moc_state["bproDiscountRate"] = str(
    #     self.contract_MoC.sc_moc_state.bpro_discount_rate(
    #         formatted=False,
    #         block_identifier=block_identifier))
    # try:
    #     d_moc_state["maxBproWithDiscount"] = str(
    #         self.contract_MoC.sc_moc_state.max_bpro_with_discount(
    #             formatted=False,
    #             block_identifier=block_identifier))
    # except (HTTPError, ValueError):
    #     log.error("[WARNING] maxBproWithDiscount Exception! [{0}]".format(
    #         block_identifier))
    #     d_moc_state["maxBproWithDiscount"] = '0'
    #
    # try:
    #     d_moc_state["bproDiscountPrice"] = str(
    #         self.contract_MoC.sc_moc_state.bpro_discount_price(
    #             formatted=False,
    #             block_identifier=block_identifier))
    # except (HTTPError, ValueError):
    #     log.error("[WARNING] bproDiscountPrice Exception! [{0}]".format(
    #         block_identifier))
    #     d_moc_state["bproDiscountPrice"] = '0'
    #
    # d_moc_state["bprox2PriceInRbtc"] = str(
    #     self.contract_MoC.sc_moc_state.btc2x_tec_price(
    #         bucket_x2,
    #         formatted=False,
    #         block_identifier=block_identifier))
    # try:
    #     d_moc_state["bprox2PriceInBpro"] = str(
    #         self.contract_MoC.sc_moc_state.bprox_price(
    #             bucket_x2,
    #             formatted=False,
    #             block_identifier=block_identifier))
    # except (HTTPError, ValueError):
    #     log.error("[WARNING] bprox2PriceInBpro Exception! [{0}]".format(
    #         block_identifier))
    #     d_moc_state["bprox2PriceInBpro"] = '0'
    #
    # try:
    #     d_moc_state["spotInrate"] = str(
    #         self.contract_MoC.sc_moc_inrate.spot_inrate(
    #             formatted=False,
    #             block_identifier=block_identifier))
    # except (HTTPError, ValueError):
    #     log.error("[WARNING] spotInrate Exception [{0}]".format(
    #         block_identifier))
    #     d_moc_state["spotInrate"] = '0'
    #
    # # Start: Commission rates by transaction types
    # commission_rates = {}
    #
    # if self.app_mode == 'RRC20':
    #     commission_rates["MINT_RISKPRO_FEES_RESERVE"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_mint_riskpro_fees_reserve(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    #     commission_rates["REDEEM_RISKPRO_FEES_RESERVE"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_redeem_riskpro_fees_reserve(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    #     commission_rates["MINT_STABLETOKEN_FEES_RESERVE"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_mint_stabletoken_fees_reserve(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    #     commission_rates["REDEEM_STABLETOKEN_FEES_RESERVE"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_redeem_stabletoken_fees_reserve(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    #     commission_rates["MINT_RISKPROX_FEES_RESERVE"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_mint_riskprox_fees_reserve(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    #     commission_rates["REDEEM_RISKPROX_FEES_RESERVE"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_redeem_riskprox_fees_reserve(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    #     commission_rates["MINT_RISKPRO_FEES_MOC"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_mint_riskpro_fees_moc(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    #     commission_rates["REDEEM_RISKPRO_FEES_MOC"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_redeem_riskpro_fees_moc(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    #     commission_rates["MINT_STABLETOKEN_FEES_MOC"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_mint_stabletoken_fees_moc(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    #     commission_rates["REDEEM_STABLETOKEN_FEES_MOC"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_redeem_stabletoken_fees_moc(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    #     commission_rates["MINT_RISKPROX_FEES_MOC"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_mint_riskprox_fees_moc(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    #     commission_rates["REDEEM_RISKPROX_FEES_MOC"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_redeem_riskprox_fees_moc(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    # else:
    #     commission_rates["MINT_BPRO_FEES_RBTC"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_mint_bpro_fees_rbtc(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    #     commission_rates["REDEEM_BPRO_FEES_RBTC"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_redeem_bpro_fees_rbtc(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    #     commission_rates["MINT_DOC_FEES_RBTC"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_mint_doc_fees_rbtc(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    #     commission_rates["REDEEM_DOC_FEES_RBTC"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_redeem_doc_fees_rbtc(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    #     commission_rates["MINT_BTCX_FEES_RBTC"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_mint_btcx_fees_rbtc(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    #     commission_rates["REDEEM_BTCX_FEES_RBTC"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_redeem_btcx_fees_rbtc(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    #     commission_rates["MINT_BPRO_FEES_MOC"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_mint_bpro_fees_moc(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    #     commission_rates["REDEEM_BPRO_FEES_MOC"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_redeem_bpro_fees_moc(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    #     commission_rates["MINT_DOC_FEES_MOC"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_mint_doc_fees_moc(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    #     commission_rates["REDEEM_DOC_FEES_MOC"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_redeem_doc_fees_moc(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    #     commission_rates["MINT_BTCX_FEES_MOC"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_mint_btcx_fees_moc(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    #     commission_rates["REDEEM_BTCX_FEES_MOC"] = str(self.contract_MoC.sc_moc_inrate.commission_rate_by_transaction_type(
    #         tx_type=self.contract_MoC.sc_moc_inrate.tx_type_redeem_btcx_fees_moc(),
    #         formatted=False,
    #         block_identifier=block_identifier))
    #
    # d_moc_state["commissionRates"] = commission_rates
    # # End: Commission rates by transaction types
    #
    # d_moc_state["bprox2PriceInUsd"] = str(
    #     int(d_moc_state["bprox2PriceInRbtc"]) * int(
    #         d_moc_state["bitcoinPrice"]) / int(
    #         d_moc_state["reservePrecision"]))
    # # d_moc_state["lastUpdateHeight"] = lastUpdateHeight
    # d_moc_state["createdAt"] = block_ts
    # d_moc_state["dayBlockSpan"] = self.contract_MoC.sc_moc_state.day_block_span(
    #     block_identifier=block_identifier)
    # d_moc_state["blockSpan"] = self.contract_MoC.sc_moc_settlement.block_span(
    #     block_identifier=block_identifier)
    # d_moc_state["blocksToSettlement"] = self.contract_MoC.sc_moc_state.blocks_to_settlement(
    #     block_identifier=block_identifier)
    # d_moc_state["state"] = self.contract_MoC.sc_moc_state.state(
    #     block_identifier=block_identifier)
    # d_moc_state["lastPriceUpdateHeight"] = 0
    # # d_moc_state["priceVariation"] = dailyPriceRef
    # d_moc_state["paused"] = self.contract_MoC.paused(
    #     block_identifier=block_identifier)
    #
    # d_moc_state["liquidationEnabled"] = self.contract_MoC.sc_moc_state.liquidation_enabled(block_identifier=block_identifier)
    # d_moc_state["protected"] = str(self.contract_MoC.sc_moc_state.protected(
    #     formatted=False,
    #     block_identifier=block_identifier))

    return d_moc_state

