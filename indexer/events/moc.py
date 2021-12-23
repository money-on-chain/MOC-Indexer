import datetime
from collections import OrderedDict
from web3 import Web3

from moneyonchain.moc import MoCBucketLiquidation, MoCContractLiquidated

from indexer.mongo_manager import mongo_manager
from indexer.logger import log
from .events import BaseIndexEvent


class IndexBucketLiquidation(BaseIndexEvent):

    name = 'BucketLiquidation'

    def index_event(self, m_client, parse_receipt, tx_event):

        # status of tx
        status, confirmation_time = self.status_tx(parse_receipt)

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(m_client)

        tx_hash = parse_receipt["transactionHash"]

        # get all address who has bprox , at the time all users because
        # we dont know who hast bprox in certain block
        collection_users = mongo_manager.collection_user_state(m_client)
        users = collection_users.find()
        l_users_riskprox = list()
        for user in users:
            l_users_riskprox.append(user)
            # if float(user['bprox2Balance']) > 0.0:
            #    l_users_riskprox.append(user)

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = parse_receipt["blockNumber"]
        d_tx["event"] = 'BucketLiquidation'
        d_tx["tokenInvolved"] = 'RISKPROX'
        d_tx["bucket"] = 'X2'
        d_tx["status"] = status
        d_tx["confirmationTime"] = confirmation_time
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parse_receipt["gas_used"] * Web3.fromWei(parse_receipt["gas_price"], 'ether')
        # d_tx["gasFeeRBTC"] = str(int(gas_fee * self.precision))
        d_tx["processLogs"] = True
        d_tx["createdAt"] = parse_receipt['chain']['block_ts']

        prior_block_to_liquidation = parse_receipt["blockNumber"] - 1
        l_transactions = list()
        for user_riskprox in l_users_riskprox:
            try:
                d_user_balances = self.parent.riskprox_balances_from_address(user_riskprox["address"],
                                                                      prior_block_to_liquidation)
            except:
                continue

            if float(d_user_balances["bprox2Balance"]) > 0.0:
                d_tx["address"] = user_riskprox["address"]
                d_tx["amount"] = str(d_user_balances["bprox2Balance"])

                post_id = collection_tx.find_one_and_update(
                    {"transactionHash": tx_hash,
                     "address": d_tx["address"],
                     "event": d_tx["event"]},
                    {"$set": d_tx},
                    upsert=True)

                log.info("Tx {0} From: [{1}] Amount: {2} Tx Hash: {3}".format(
                    d_tx["event"],
                    d_tx["address"],
                    d_tx["amount"],
                    tx_hash))

                # update user balances
                #self.parent.update_balance_address(self.m_client, d_tx["address"], self.block_height)

                l_transactions.append(d_tx)

    def notifications(self, m_client, parse_receipt, tx_event):
        """Event: """

        collection_tx = mongo_manager.collection_notification(m_client)
        tx_hash = parse_receipt["transactionHash"]
        event_name = 'BucketLiquidation'

        d_tx = OrderedDict()
        d_tx["event"] = event_name
        d_tx["transactionHash"] = tx_hash
        d_tx["logIndex"] = parse_receipt["log_index"]
        d_tx["bucket"] = 'X2'
        d_tx["timestamp"] = parse_receipt["timestamp"]
        d_tx["processLogs"] = True

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash, "event": event_name, "logIndex": parse_receipt["log_index"]},
            {"$set": d_tx},
            upsert=True)

        d_tx['post_id'] = post_id

        return d_tx

    def on_event(self, m_client, parse_receipt):
        """ Event """

        cl_tx_event = MoCBucketLiquidation(parse_receipt)
        self.index_event(m_client, parse_receipt, cl_tx_event.event[self.name])
        self.notifications(m_client, parse_receipt, cl_tx_event.event[self.name])


class IndexContractLiquidated(BaseIndexEvent):

    name = 'ContractLiquidated'

    def index_event(self, m_client, parse_receipt, tx_event):

        # status of tx
        status, confirmation_time = self.status_tx(parse_receipt)

        # get collection transaction
        collection_tx = mongo_manager.collection_transaction(m_client)

        tx_hash = parse_receipt["transactionHash"]

        # get all address who has DoC, at the time all users because
        # we dont know who has DoC in certain block
        collection_users = mongo_manager.collection_user_state(m_client)
        users = collection_users.find()
        l_users_stable = list()
        for user in users:
            l_users_stable.append(user)

        d_tx = OrderedDict()
        d_tx["transactionHash"] = tx_hash
        d_tx["blockNumber"] = parse_receipt["blockNumber"]
        d_tx["event"] = 'ContractLiquidated'
        d_tx["tokenInvolved"] = 'STABLE'
        d_tx["bucket"] = 'C0'
        d_tx["status"] = status
        d_tx["confirmationTime"] = confirmation_time
        d_tx["lastUpdatedAt"] = datetime.datetime.now()
        gas_fee = parse_receipt["gas_used"] * Web3.fromWei(parse_receipt["gas_price"], 'ether')
        d_tx["processLogs"] = True
        d_tx["createdAt"] = parse_receipt['chain']['block_ts']

        prior_block_to_liquidation = tx_event.blockNumber - 1
        l_transactions = list()
        for user_stable in l_users_stable:
            try:
                d_user_balances = self.parent.stable_balances_from_address(user_stable["address"],
                                                                           prior_block_to_liquidation)
            except:
                continue

            if float(d_user_balances["docBalance"]) > 0.0:
                d_tx["address"] = user_stable["address"]
                d_tx["amount"] = str(d_user_balances["docBalance"])

                post_id = collection_tx.find_one_and_update(
                    {"transactionHash": tx_hash,
                     "address": d_tx["address"],
                     "event": d_tx["event"]},
                    {"$set": d_tx},
                    upsert=True)

                log.info("Tx {0} From: [{1}] Amount: {2} Tx Hash: {3}".format(
                    d_tx["event"],
                    d_tx["address"],
                    d_tx["amount"],
                    tx_hash))

                # update user balances
                #self.parent.update_balance_address(self.m_client, d_tx["address"], self.block_height)

                l_transactions.append(d_tx)

    def notifications(self, m_client, parse_receipt):
        """Event: """

        collection_tx = mongo_manager.collection_notification(m_client)
        tx_hash = parse_receipt["transactionHash"]
        event_name = 'ContractLiquidated'

        d_tx = OrderedDict()
        d_tx["event"] = event_name
        d_tx["transactionHash"] = tx_hash
        d_tx["logIndex"] = parse_receipt["log_index"]
        d_tx["bucket"] = 'C0'
        d_tx["timestamp"] = parse_receipt["timestamp"]
        d_tx["processLogs"] = True

        post_id = collection_tx.find_one_and_update(
            {"transactionHash": tx_hash, "event": event_name, "logIndex": parse_receipt["log_index"]},
            {"$set": d_tx},
            upsert=True)

        d_tx['post_id'] = post_id

        return d_tx

    def on_event(self, m_client, parse_receipt):
        """ Event """

        cl_tx_event = MoCContractLiquidated(parse_receipt)
        self.index_event(m_client, parse_receipt, cl_tx_event.event[self.name])
        self.notifications(m_client, parse_receipt)
