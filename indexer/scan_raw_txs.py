import datetime
import time
from collections import OrderedDict

from moneyonchain.networks import network_manager

from indexer.mongo_manager import mongo_manager
from indexer.logger import log


from indexer.chain import block_filtered_transactions, ChainBlock


LOCAL_TIMEZONE = datetime.datetime.now().astimezone().tzinfo


def index_raw_tx(block_number, last_block_number, m_client, filter_tx=None, debug_mode=True):
    """ Receipts from blockchain to Database"""

    if debug_mode:
        log.info("[1. Scan Raw Txs] Starting to Scan RAW TX block number: [{0}] / [{1}]".format(
            block_number, last_block_number))

    collection_raw_transactions = mongo_manager.collection_raw_transactions(m_client)

    fil_txs = block_filtered_transactions(block_number, filter_tx=filter_tx)
    receipts = fil_txs["receipts"]

    count = 0
    if receipts:
        for tx_rcp in receipts:

            d_tx = OrderedDict()
            d_tx["hash"] = str(tx_rcp.txid)
            d_tx["blockNumber"] = tx_rcp.block_number
            d_tx["from"] = tx_rcp.sender
            d_tx["to"] = tx_rcp.receiver
            d_tx["value"] = str(tx_rcp.value)
            d_tx["gas"] = tx_rcp.gas_limit
            d_tx["gasPrice"] = str(tx_rcp.gas_price)
            d_tx["input"] = tx_rcp.input
            d_tx["receipt"] = True
            d_tx["processed"] = False
            d_tx["gas_used"] = tx_rcp.gas_used
            d_tx["confirmations"] = tx_rcp.confirmations
            d_tx["timestamp"] = datetime.datetime.fromtimestamp(tx_rcp.timestamp, LOCAL_TIMEZONE)
            d_tx["logs"] = tx_rcp.logs
            d_tx["status"] = tx_rcp.status
            d_tx["createdAt"] = datetime.datetime.fromtimestamp(tx_rcp.timestamp, LOCAL_TIMEZONE)
            d_tx["lastUpdatedAt"] = datetime.datetime.now()

            post_id = collection_raw_transactions.find_one_and_update(
                {"hash": str(tx_rcp.txid), "blockNumber": tx_rcp.block_number},
                {"$set": d_tx},
                upsert=True)
            count += 1

    d_info = dict()
    d_info["processed"] = count
    d_info["block_number"] = fil_txs["block_number"]
    d_info["block_ts"] = fil_txs["block_ts"]

    return d_info


def scan_raw_txs(options, filter_contracts, task=None):

    start_time = time.time()

    # conect to mongo db
    m_client = mongo_manager.connect()

    # get the block recesion is a margin of problems to not get the inmediat new instead
    # 2 older blocks from new.
    config_blocks_recession = options['scan_moc_blocks']['blocks_recession']

    # debug mode
    debug_mode = options['debug']

    # get last block from node compare 1 blocks older than new
    last_block = network_manager.block_number - config_blocks_recession

    collection_moc_indexer = mongo_manager.collection_moc_indexer(m_client)
    moc_index = collection_moc_indexer.find_one(sort=[("updatedAt", -1)])
    last_block_indexed = 0
    if moc_index:
        if 'last_raw_tx_block' in moc_index:
            last_block_indexed = moc_index['last_raw_tx_block']

    config_blocks_look_behind = options['scan_moc_blocks']['blocks_look_behind']
    from_block = last_block - config_blocks_look_behind
    if last_block_indexed > 0:
        from_block = last_block_indexed + 1

    if from_block >= last_block:
        if debug_mode:
            log.info("[1. Scan Raw Txs] Its not the time to run indexer no new blocks avalaible!")
        return

    to_block = last_block

    if from_block > to_block:
        log.error("[1. Scan Raw Txs] To block > from block!!??")
        return

    # start with from block
    current_block = from_block

    if debug_mode:
        log.info("[1. Scan Raw Txs] Starting to Scan Transactions [{0} / {1}]".format(from_block, to_block))

    processed = 0
    while current_block <= to_block:

        # index our contracts only
        block_processed = index_raw_tx(
            current_block,
            last_block,
            m_client,
            filter_tx=filter_contracts,
            debug_mode=debug_mode)

        if debug_mode:
            log.info("[1. Scan Raw Txs] OK [{0}] / [{1}]".format(current_block, to_block))

        collection_moc_indexer.update_one({},
                                          {'$set': {'last_raw_tx_block': current_block,
                                                    'updatedAt': datetime.datetime.now(),
                                                    'last_block_number': block_processed['block_number'],
                                                    'last_block_ts': block_processed['block_ts']}},
                                          upsert=True)
        processed = block_processed["processed"]

        # Go to next block
        current_block += 1

    duration = time.time() - start_time
    log.info("[1. Scan Raw Txs] Done! Processed: [{0}] in [{1} seconds]".format(processed, duration))


class ScanRawTxs:

    def __init__(self, options, filter_contracts):
        self.options = options
        self.filter_contracts = filter_contracts

    def on_init(self):
        pass

    def on_task(self, task=None):
        scan_raw_txs(self.options, self.filter_contracts, task=task)
