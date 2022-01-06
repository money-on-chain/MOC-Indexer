from collections import OrderedDict
from requests.exceptions import HTTPError
from web3.types import BlockIdentifier
import datetime

from moneyonchain.networks import network_manager

from indexer.mongo_manager import mongo_manager
from indexer.logger import log


BUCKET_X2 = '0x5832000000000000000000000000000000000000000000000000000000000000'
BUCKET_C0 = '0x4330000000000000000000000000000000000000000000000000000000000000'


def balances_from_address(
        contract_loaded,
        contract_addresses,
        addresses,
        block_identifier: BlockIdentifier = 'latest',
        app_mode='MoC',
        block_ts=None):

    moc_token = contract_loaded["MoCToken"]
    moc_token_address = contract_addresses["MoCToken"]
    moc = contract_loaded["MoC"]
    moc_address = contract_addresses["MoC"]
    doc_token = contract_loaded["DoCToken"]
    doc_token_address = contract_addresses["DoCToken"]
    bpro_token = contract_loaded["BProToken"]
    bpro_token_address = contract_addresses["BProToken"]
    moc_inrate = contract_loaded["MoCInrate"]
    moc_inrate_address = contract_addresses["MoCInrate"]

    multicall = contract_loaded["Multicall2"]
    multicall_address = contract_addresses["Multicall2"]

    list_aggregate = list()
    list_aggregate.append((moc_token_address, moc_token.sc.balanceOf, [addresses], lambda x: str(x)))  # 0
    list_aggregate.append((moc_token_address, moc_token.sc.allowance, [addresses, moc_address], lambda x: str(x)))  # 1
    list_aggregate.append((doc_token_address, doc_token.sc.balanceOf, [addresses], lambda x: str(x)))  # 2
    list_aggregate.append((bpro_token_address, bpro_token.sc.balanceOf, [addresses], lambda x: str(x)))  # 3
    list_aggregate.append((multicall_address, multicall.sc.getEthBalance, [addresses], lambda x: str(x)))  # 4

    if app_mode == 'MoC':
        list_aggregate.append((moc_address, moc.sc.docAmountToRedeem, [addresses], lambda x: str(x)))  # 5
        list_aggregate.append((moc_address, moc.sc.bproxBalanceOf, [BUCKET_X2, addresses], lambda x: str(x)))  # 6
    elif app_mode == 'RRC20':
        list_aggregate.append((moc_address, moc.sc.stableTokenAmountToRedeem, [addresses], lambda x: str(x)))  # 5
        list_aggregate.append((moc_address, moc.sc.riskProxBalanceOf, [BUCKET_X2, addresses], lambda x: str(x)))  # 6
        reserve_token = contract_loaded["ReserveToken"]
        reserve_token_address = contract_addresses["ReserveToken"]
        list_aggregate.append((reserve_token_address, reserve_token.sc.balanceOf, [addresses], lambda x: str(x)))  # 7
        list_aggregate.append((reserve_token_address, reserve_token.sc.allowance, [addresses, moc_address], lambda x: str(x)))  # 8
        moc_bpro_token = contract_loaded["MoC_BProToken"]
        moc_bpro_token_address = contract_addresses["MoC_BProToken"]
        list_aggregate.append((moc_bpro_token_address, moc_bpro_token.sc.balanceOf, [addresses], lambda x: str(x)))  # 9
    else:
        raise Exception("Not valid APP Mode")

    d_user_balance = OrderedDict()

    results = multicall.aggregate_multiple(list_aggregate, block_identifier=block_identifier)

    block_number = results[0]

    d_user_balance["blockHeight"] = block_number
    d_user_balance["createdAt"] = block_ts

    d_user_balance["mocBalance"] = results[1][0]
    d_user_balance["mocAllowance"] = results[1][1]
    d_user_balance["docBalance"] = results[1][2]
    d_user_balance["bproBalance"] = results[1][3]
    d_user_balance["rbtcBalance"] = results[1][4]
    d_user_balance["docToRedeem"] = results[1][5]
    d_user_balance["bprox2Balance"] = results[1][6]
    d_user_balance["potentialBprox2MaxInterest"] = str(0)
    d_user_balance["bProHoldIncentive"] = str(0)
    d_user_balance["estimateGasMintBpro"] = str(2000000)
    d_user_balance["estimateGasMintDoc"] = str(2000000)
    d_user_balance["estimateGasMintBprox2"] = str(2000000)

    if app_mode == 'MoC':
        d_user_balance["spendableBalance"] = d_user_balance["rbtcBalance"]
        d_user_balance["reserveAllowance"] = d_user_balance["rbtcBalance"]
        d_user_balance["bproMoCBalance"] = d_user_balance["bproBalance"]
        total_reserve_amount = d_user_balance["rbtcBalance"]
    elif app_mode == 'RRC20':
        d_user_balance["spendableBalance"] = results[1][7]
        d_user_balance["reserveAllowance"] = results[1][8]
        d_user_balance["bproMoCBalance"] = results[1][9]
        total_reserve_amount = d_user_balance["spendableBalance"]
    else:
        raise Exception("Not valid APP Mode")

    # Calc of interest cannot be in multicall because we need to know amount of reserves of the account
    try:
        d_user_balance["potentialBprox2MaxInterest"] = str(moc_inrate.sc.calcMintInterestValues(
            BUCKET_X2,
            int(total_reserve_amount),
            block_identifier=block_identifier))
    except (HTTPError, ValueError):
        log.error("[WARNING] potentialBprox2MaxInterest Exception!")
        d_user_balance["potentialBprox2MaxInterest"] = '0'

    return d_user_balance


def update_balance_address(
        m_client,
        contract_loaded,
        contract_addresses,
        account_address,
        block_height,
        app_mode='MoC',
        block_ts=None):

    # get collection user state from mongo
    collection_user_state = mongo_manager.collection_user_state(m_client)

    user_state = collection_user_state.find_one(
        {"address": account_address}
    )

    if user_state:
        if 'block_height' in user_state:
            if user_state['block_height'] >= block_height:
                # not process if already have updated in this block
                return
        else:
            block_height = network_manager.block_number
    else:
        # if not exist get the last block number
        block_height = network_manager.block_number

    # get all functions state from smart contract
    d_user_balance = balances_from_address(
        contract_loaded,
        contract_addresses,
        account_address,
        block_height,
        app_mode=app_mode,
        block_ts=block_ts)
    d_user_balance['block_height'] = block_height

    if not user_state:
        # if the user not exist in the database created but default info
        d_user_balance["prefLanguage"] = 'en'
        d_user_balance["createdAt"] = datetime.datetime.now()
        d_user_balance["lastNotificationCheckAt"] = datetime.datetime.now()
        d_user_balance["showTermsAndConditions"] = True
        d_user_balance["showTutorialNoMore"] = False
        d_user_balance["createdBlockHeight"] = block_height

    # update or insert
    post_id = collection_user_state.find_one_and_update(
        {"address": account_address},
        {"$set": d_user_balance},
        upsert=True)
    d_user_balance['post_id'] = post_id

    log.info("[UPDATE USERSTATE]: [{0}] BLOCKHEIGHT: [{1}] ".format(
        account_address,
        block_height))

    return d_user_balance


def insert_update_balance_address(m_client, account_address):

    # get collection user state update from mongo
    collection_user_state_update = mongo_manager.collection_user_state_update(m_client)

    user_update = dict()
    user_update['account'] = account_address

    # update or insert
    post_id = collection_user_state_update.find_one_and_update(
        {"account": account_address},
        {"$set": user_update},
        upsert=True)

    return post_id


def stable_balances_from_address(contract_loaded,
                                 address,
                                 block_identifier: BlockIdentifier = 'latest'):

    doc_token = contract_loaded["DoCToken"]

    d_user_balance = OrderedDict()
    d_user_balance["docBalance"] = str(doc_token.sc.balanceOf(
        address,
        block_identifier=block_identifier))

    return d_user_balance


def riskprox_balances_from_address(contract_loaded,
                                   address,
                                   block_identifier: BlockIdentifier = 'latest',
                                   app_mode='MoC'):

    moc = contract_loaded["MoC"]

    d_user_balance = OrderedDict()

    if app_mode == 'MoC':
        d_user_balance["bprox2Balance"] = str(
            moc.sc.bproxBalanceOf(
                BUCKET_X2,
                address,
                block_identifier=block_identifier))
    elif app_mode == 'RRC20':
        d_user_balance["bprox2Balance"] = str(
            moc.sc.riskProxBalanceOf(
                BUCKET_X2,
                address,
                block_identifier=block_identifier))
    else:
        raise Exception("Not valid APP Mode")

    return d_user_balance
