from collections import OrderedDict
from web3.types import BlockIdentifier


BUCKET_X2 = '0x5832000000000000000000000000000000000000000000000000000000000000'
BUCKET_C0 = '0x4330000000000000000000000000000000000000000000000000000000000000'


def moc_state_from_sc(
        contract_loaded,
        contract_addresses,
        block_identifier: BlockIdentifier = 'latest',
        block_ts=None,
        app_mode=None):

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
    if app_mode == 'MoC':
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
    elif app_mode == 'RRC20':
        list_aggregate.append((moc_state_address, moc_state.sc.getReserveTokenPrice, [], lambda x: str(x)))  # 0
        list_aggregate.append((moc_state_address, moc_state.sc.getMoCPrice, [], lambda x: str(x)))  # 1
        list_aggregate.append((moc_state_address, moc_state.sc.absoluteMaxRiskPro, [], lambda x: str(x)))  # 2
        list_aggregate.append((moc_state_address, moc_state.sc.maxRiskProx, [BUCKET_X2], lambda x: str(x)))  # 3
        list_aggregate.append((moc_state_address, moc_state.sc.absoluteMaxStableToken, [], lambda x: str(x)))  # 4
        list_aggregate.append((moc_state_address, moc_state.sc.freeStableToken, [], lambda x: str(x)))  # 5
        list_aggregate.append((moc_state_address, moc_state.sc.leverage, [BUCKET_C0], lambda x: str(x)))  # 6
        list_aggregate.append((moc_state_address, moc_state.sc.cobj, [], lambda x: str(x)))  # 7
        list_aggregate.append((moc_state_address, moc_state.sc.leverage, [BUCKET_X2], lambda x: str(x)))  # 8
        list_aggregate.append((moc_state_address, moc_state.sc.reserves, [], lambda x: str(x)))  # 9
        list_aggregate.append((moc_state_address, moc_state.sc.getExponentalMovingAverage, [], lambda x: str(x)))  # 10
        list_aggregate.append((moc_state_address, moc_state.sc.getInrateBag, [BUCKET_C0], lambda x: str(x)))  # 11
        list_aggregate.append((moc_state_address, moc_state.sc.getBucketNReserve, [BUCKET_C0], lambda x: str(x)))  # 12
        list_aggregate.append((moc_state_address, moc_state.sc.getBucketNStableToken, [BUCKET_C0], lambda x: str(x)))  # 13
        list_aggregate.append((moc_state_address, moc_state.sc.getBucketNRiskPro, [BUCKET_C0], lambda x: str(x)))  # 14
        list_aggregate.append((moc_state_address, moc_state.sc.getBucketNReserve, [BUCKET_X2], lambda x: str(x)))  # 15
        list_aggregate.append((moc_state_address, moc_state.sc.getBucketNStableToken, [BUCKET_X2], lambda x: str(x)))  # 16
        list_aggregate.append((moc_state_address, moc_state.sc.getBucketNRiskPro, [BUCKET_X2], lambda x: str(x)))  # 17
        list_aggregate.append((moc_state_address, moc_state.sc.globalCoverage, [], lambda x: str(x)))  # 18
        list_aggregate.append((moc_address, moc.sc.getReservePrecision, [], lambda x: str(x)))  # 19
        list_aggregate.append((moc_address, moc.sc.getMocPrecision, [], lambda x: str(x)))  # 20
        list_aggregate.append((moc_state_address, moc_state.sc.coverage, [BUCKET_X2], lambda x: str(x)))  # 21
        list_aggregate.append((moc_state_address, moc_state.sc.riskProTecPrice, [], lambda x: str(x)))  # 22
        list_aggregate.append((moc_state_address, moc_state.sc.riskProUsdPrice, [], lambda x: str(x)))  # 23
        list_aggregate.append((moc_state_address, moc_state.sc.riskProSpotDiscountRate, [], lambda x: str(x)))  # 24
        list_aggregate.append((moc_state_address, moc_state.sc.maxRiskProWithDiscount, [], lambda x: str(x)))  # 25
        list_aggregate.append((moc_state_address, moc_state.sc.riskProDiscountPrice, [], lambda x: str(x)))  # 26
        list_aggregate.append((moc_state_address, moc_state.sc.bucketRiskProTecPrice, [BUCKET_X2], lambda x: str(x)))  # 27
        list_aggregate.append((moc_state_address, moc_state.sc.riskProxRiskProPrice, [BUCKET_X2], lambda x: str(x)))  # 28
        list_aggregate.append((moc_inrate_address, moc_inrate.sc.spotInrate, [], lambda x: str(x)))  # 29
        list_aggregate.append((moc_inrate_address, moc_inrate.sc.MINT_RISKPRO_FEES_RESERVE, [], lambda x: str(x)))  # 30
        list_aggregate.append((moc_inrate_address, moc_inrate.sc.REDEEM_RISKPRO_FEES_RESERVE, [], lambda x: str(x)))  # 31
        list_aggregate.append((moc_inrate_address, moc_inrate.sc.MINT_STABLETOKEN_FEES_RESERVE, [], lambda x: str(x)))  # 32
        list_aggregate.append((moc_inrate_address, moc_inrate.sc.REDEEM_STABLETOKEN_FEES_RESERVE, [], lambda x: str(x)))  # 33
        list_aggregate.append((moc_inrate_address, moc_inrate.sc.MINT_RISKPROX_FEES_RESERVE, [], lambda x: str(x)))  # 34
        list_aggregate.append((moc_inrate_address, moc_inrate.sc.REDEEM_RISKPROX_FEES_RESERVE, [], lambda x: str(x)))  # 35
        list_aggregate.append((moc_inrate_address, moc_inrate.sc.MINT_RISKPRO_FEES_MOC, [], lambda x: str(x)))  # 36
        list_aggregate.append((moc_inrate_address, moc_inrate.sc.REDEEM_RISKPRO_FEES_MOC, [], lambda x: str(x)))  # 37
        list_aggregate.append((moc_inrate_address, moc_inrate.sc.MINT_STABLETOKEN_FEES_MOC, [], lambda x: str(x)))  # 38
        list_aggregate.append((moc_inrate_address, moc_inrate.sc.REDEEM_STABLETOKEN_FEES_MOC, [], lambda x: str(x)))  # 39
        list_aggregate.append((moc_inrate_address, moc_inrate.sc.MINT_RISKPROX_FEES_MOC, [], lambda x: str(x)))  # 40
        list_aggregate.append((moc_inrate_address, moc_inrate.sc.REDEEM_RISKPROX_FEES_MOC, [], lambda x: str(x)))  # 41
        list_aggregate.append((moc_state_address, moc_state.sc.dayBlockSpan, [], lambda x: x))  # 42
        list_aggregate.append((moc_settlement_address, moc_settlement.sc.getBlockSpan, [], lambda x: x))  # 43
        list_aggregate.append((moc_state_address, moc_state.sc.blocksToSettlement, [], lambda x: x))  # 44
        list_aggregate.append((moc_state_address, moc_state.sc.state, [], lambda x: x))  # 45
        list_aggregate.append((moc_address, moc.sc.paused, [], lambda x: x))  # 46
        list_aggregate.append((moc_state_address, moc_state.sc.getLiquidationEnabled, [], lambda x: x))  # 47
        list_aggregate.append((moc_state_address, moc_state.sc.getProtected, [], lambda x: str(x)))  # 48
    else:
        raise Exception("Not valid APP Mode")

    results = multicall.aggregate_multiple(list_aggregate, block_identifier=block_identifier)

    # only valid results
    if not results[2]['valid']:
        return

    block_number = results[0]

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
    if app_mode == 'MoC':
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
    elif app_mode == 'RRC20':
        commission_rates["MINT_RISKPRO_FEES_RESERVE"] = results[1][30]
        commission_rates["REDEEM_RISKPRO_FEES_RESERVE"] = results[1][31]
        commission_rates["MINT_STABLETOKEN_FEES_RESERVE"] = results[1][32]
        commission_rates["REDEEM_STABLETOKEN_FEES_RESERVE"] = results[1][33]
        commission_rates["MINT_RISKPROX_FEES_RESERVE"] = results[1][34]
        commission_rates["REDEEM_RISKPROX_FEES_RESERVE"] = results[1][35]
        commission_rates["MINT_RISKPRO_FEES_MOC"] = results[1][36]
        commission_rates["REDEEM_RISKPRO_FEES_MOC"] = results[1][37]
        commission_rates["MINT_STABLETOKEN_FEES_MOC"] = results[1][38]
        commission_rates["REDEEM_STABLETOKEN_FEES_MOC"] = results[1][39]
        commission_rates["MINT_RISKPROX_FEES_MOC"] = results[1][40]
        commission_rates["REDEEM_RISKPROX_FEES_MOC"] = results[1][41]
    else:
        raise Exception("Not valid APP Mode")

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

    return d_moc_state

