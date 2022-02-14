from collections import OrderedDict
from web3.types import BlockIdentifier


BUCKET_X2 = '0x5832000000000000000000000000000000000000000000000000000000000000'
BUCKET_C0 = '0x4330000000000000000000000000000000000000000000000000000000000000'


def state_status_from_sc(
        contract_loaded,
        contract_addresses,
        block_identifier: BlockIdentifier = 'latest',
        block_ts=None,
        app_mode=None):

    d_status = OrderedDict()

    moc_state = contract_loaded["MoCState"]
    moc_state_address = contract_addresses["MoCState"]
    moc = contract_loaded["MoC"]
    moc_address = contract_addresses["MoC"]

    multicall = contract_loaded["Multicall2"]

    list_aggregate = list()
    if app_mode == 'MoC':
        list_aggregate.append((moc_state_address, moc_state.sc.getBitcoinPrice, [], lambda x: str(x)))  # 0
        list_aggregate.append((moc_state_address, moc_state.sc.getMoCPrice, [], lambda x: str(x)))  # 1
        list_aggregate.append((moc_address, moc.sc.paused, [], lambda x: x))  # 2
        list_aggregate.append((moc_state_address, moc_state.sc.state, [], lambda x: x))  # 3
    elif app_mode == 'RRC20':
        list_aggregate.append((moc_state_address, moc_state.sc.getReserveTokenPrice, [], lambda x: str(x)))  # 0
        list_aggregate.append((moc_state_address, moc_state.sc.getMoCPrice, [], lambda x: str(x)))  # 1
        list_aggregate.append((moc_address, moc.sc.paused, [], lambda x: x))  # 2
        list_aggregate.append((moc_state_address, moc_state.sc.state, [], lambda x: x))  # 3
    else:
        raise Exception("Not valid APP Mode")

    results = multicall.aggregate_multiple(list_aggregate, block_identifier=block_identifier)

    block_number = results[0]

    d_status["blockHeight"] = block_number
    d_status["createdAt"] = block_ts

    if results[1][0]:
        price_active = True
    else:
        price_active = False
    d_status['price_active'] = price_active

    if results[1][1]:
        moc_price_active = True
    else:
        moc_price_active = False
    d_status['moc_price_active'] = moc_price_active

    d_status["paused"] = results[1][2]
    d_status["state"] = results[1][3]

    return d_status
