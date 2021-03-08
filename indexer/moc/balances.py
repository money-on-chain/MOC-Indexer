from collections import OrderedDict
from requests.exceptions import HTTPError
from web3.types import BlockIdentifier
import datetime

from moneyonchain.networks import network_manager

from indexer.mongo_manager import mongo_manager
from indexer.logger import log

from .indexer import MoCIndexer


class Balances(MoCIndexer):

    def balances_from_address(self, address, block_height):

        d_user_balance = OrderedDict()
        d_user_balance["mocBalance"] = str(0)
        d_user_balance["bProHoldIncentive"] = str(0)
        d_user_balance["docBalance"] = str(self.contract_MoC.doc_balance_of(
            address,
            formatted=False,
            block_identifier=block_height))
        d_user_balance["bproBalance"] = str(self.contract_MoC.bpro_balance_of(
            address,
            formatted=False,
            block_identifier=block_height))
        d_user_balance["bprox2Balance"] = str(
            self.contract_MoC.bprox_balance_of(
                address,
                formatted=False,
                block_identifier=block_height))
        if self.app_mode == "RRC20":
            d_user_balance["rbtcBalance"] = str(
                self.contract_MoC.reserve_balance_of(
                    address,
                    formatted=False,
                    block_identifier=block_height))
        else:
            d_user_balance["rbtcBalance"] = str(
                self.contract_MoC.rbtc_balance_of(
                    address,
                    formatted=False,
                    block_identifier=block_height))

        d_user_balance["docToRedeem"] = str(
            self.contract_MoC.doc_amount_to_redeem(
                address,
                formatted=False,
                block_identifier=block_height))
        d_user_balance["reserveAllowance"] = str(
            self.contract_MoC.reserve_allowance(
                address,
                formatted=False,
                block_identifier=block_height))
        d_user_balance["spendableBalance"] = str(
            self.contract_MoC.spendable_balance(
                address,
                formatted=False,
                block_identifier=block_height))
        try:
            d_user_balance["potentialBprox2MaxInterest"] = str(
                self.contract_MoC.sc_moc_inrate.calc_mint_interest_value(
                    int(d_user_balance["rbtcBalance"]),
                    formatted=False,
                    precision=False
                )
            )
        except HTTPError:
            log.error("[WARNING] potentialBprox2MaxInterest Exception!")
            d_user_balance["potentialBprox2MaxInterest"] = '0'

        d_user_balance["estimateGasMintBpro"] = str(
            self.contract_MoC.mint_bpro_gas_estimated(
                int(d_user_balance["rbtcBalance"]))
        )
        d_user_balance["estimateGasMintDoc"] = str(
            self.contract_MoC.mint_doc_gas_estimated(
                int(d_user_balance["rbtcBalance"]))
        )
        d_user_balance["estimateGasMintBprox2"] = str(
            self.contract_MoC.mint_bprox_gas_estimated(
                int(d_user_balance["rbtcBalance"]))
        )

        return d_user_balance

    def riskprox_balances_from_address(self,
                                       address,
                                       block_identifier: BlockIdentifier = 'latest'):

        d_user_balance = OrderedDict()

        d_user_balance["bprox2Balance"] = str(
            self.contract_MoC.bprox_balance_of(
                address,
                formatted=False,
                block_identifier=block_identifier))

        return d_user_balance

    def update_balance_address(self, m_client, account_address, block_height):

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
        d_user_balance = self.balances_from_address(account_address, block_height)
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

    def update_user_state_reserve(self, user_address, m_client,
                                  block_identifier: BlockIdentifier = 'latest'):
        user_state = mongo_manager.collection_user_state(m_client)
        exist_user = user_state.find_one(
            {"address": user_address}
        )
        if exist_user:
            d_user_balance = OrderedDict()
            d_user_balance["reserveAllowance"] = str(
                self.contract_MoC.reserve_allowance(
                    user_address,
                    formatted=False,
                    block_identifier=block_identifier))
            d_user_balance["spendableBalance"] = str(
                self.contract_MoC.spendable_balance(
                    user_address,
                    formatted=False,
                    block_identifier=block_identifier))

            post_id = user_state.find_one_and_update(
                {"address": user_address},
                {"$set": d_user_balance}
            )
            if self.debug_mode:
                log.info(
                    "Update user approval: [{0}] -> {1} -> Mongo _id: {2}".format(
                        user_address,
                        d_user_balance,
                        post_id))
