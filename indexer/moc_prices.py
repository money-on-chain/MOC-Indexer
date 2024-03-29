from collections import OrderedDict
from web3.types import BlockIdentifier


BUCKET_X2 = '0x5832000000000000000000000000000000000000000000000000000000000000'
BUCKET_C0 = '0x4330000000000000000000000000000000000000000000000000000000000000'


def prices_from_sc(
        contract_loaded,
        contract_addresses,
        block_identifier: BlockIdentifier = 'latest',
        block_ts=None,
        app_mode=None):

    d_price = OrderedDict()

    moc_state = contract_loaded["MoCState"]
    moc_state_address = contract_addresses["MoCState"]
    moc = contract_loaded["MoC"]
    moc_address = contract_addresses["MoC"]
    multicall = contract_loaded["Multicall2"]

    list_aggregate = list()

    if app_mode == 'MoC':
        list_aggregate.append((moc_state_address, moc_state.sc.getBitcoinPrice, [], lambda x: str(x)))
        list_aggregate.append((moc_state_address, moc_state.sc.bproTecPrice, [], lambda x: str(x)))
        list_aggregate.append((moc_state_address, moc_state.sc.bproUsdPrice, [], lambda x: str(x)))
        list_aggregate.append((moc_state_address, moc_state.sc.bproDiscountPrice, [], lambda x: str(x)))
        list_aggregate.append((moc_state_address, moc_state.sc.bucketBProTecPrice, [BUCKET_X2], lambda x: str(x)))
        list_aggregate.append((moc_state_address, moc_state.sc.bproxBProPrice, [BUCKET_X2], lambda x: str(x)))
        list_aggregate.append((moc_address, moc.sc.getReservePrecision, [], lambda x: str(x)))
        list_aggregate.append((moc_state_address, moc_state.sc.getMoCPrice, [], lambda x: str(x)))
    elif app_mode == 'RRC20':
        list_aggregate.append((moc_state_address, moc_state.sc.getReserveTokenPrice, [], lambda x: str(x)))
        list_aggregate.append((moc_state_address, moc_state.sc.riskProTecPrice, [], lambda x: str(x)))
        list_aggregate.append((moc_state_address, moc_state.sc.riskProUsdPrice, [], lambda x: str(x)))
        list_aggregate.append((moc_state_address, moc_state.sc.riskProDiscountPrice, [], lambda x: str(x)))
        list_aggregate.append((moc_state_address, moc_state.sc.bucketRiskProTecPrice, [BUCKET_X2], lambda x: str(x)))
        list_aggregate.append((moc_state_address, moc_state.sc.riskProxRiskProPrice, [BUCKET_X2], lambda x: str(x)))
        list_aggregate.append((moc_address, moc.sc.getReservePrecision, [], lambda x: str(x)))
        list_aggregate.append((moc_state_address, moc_state.sc.getMoCPrice, [], lambda x: str(x)))
    else:
        raise Exception("Not valid APP Mode")

    results = multicall.aggregate_multiple(list_aggregate, block_identifier=block_identifier)

    # only valid results
    if not results[2]['valid']:
        return

    block_number = results[0]

    d_price["blockHeight"] = block_number
    d_price["createdAt"] = block_ts
    d_price["bitcoinPrice"] = results[1][0]
    d_price["bproPriceInRbtc"] = results[1][1]
    d_price["bproPriceInUsd"] = results[1][2]
    d_price["bproDiscountPrice"] = results[1][3]
    d_price["bprox2PriceInRbtc"] = results[1][4]
    d_price["bprox2PriceInBpro"] = results[1][5]
    d_price["reservePrecision"] = results[1][6]
    d_price["bprox2PriceInUsd"] = str(
        int(d_price["bprox2PriceInRbtc"]) * int(d_price["bitcoinPrice"]) / int(
            d_price["reservePrecision"]))
    d_price["mocPrice"] = results[1][7]

    return d_price
