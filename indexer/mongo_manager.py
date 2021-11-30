import pymongo


__all__ = ["mongo_manager"]


class MongoManager:

    def __init__(self, uri='mongodb://192.168.56.2:27017/', db='local_alpha_testnet2'):

        self.uri = uri
        self.db = db

    def set_connection(self, uri='mongodb://192.168.56.2:27017/', db='local_alpha_testnet2'):

        self.uri = uri
        self.db = db

    def connect(self):

        uri = self.uri
        client = pymongo.MongoClient(uri)

        return client

    def collection_moc_state(self, client, start_index=True):

        mongo_db = self.db
        db = client[mongo_db]
        collection = db['MocState']

        # index creation
        #if start_index:
        #    collection.create_index([('block_number', pymongo.DESCENDING)], unique=True)

        return collection

    def collection_moc_state_history(self, client, start_index=True):

        mongo_db = self.db
        db = client[mongo_db]
        collection = db['MocState_history']

        # index creation
        if start_index:
            collection.create_index([('blockHeight', pymongo.DESCENDING)],
                                    unique=True)

        return collection

    def collection_moc_state_status(self, client, start_index=True):

        mongo_db = self.db
        db = client[mongo_db]
        collection = db['MocState_status']

        # index creation
        if start_index:
            collection.create_index([('blockHeight', pymongo.DESCENDING)],
                                    unique=True)

        return collection

    def collection_price(self, client, start_index=True):

        mongo_db = self.db
        db = client[mongo_db]
        collection = db['Price']

        # index creation
        if start_index:
            collection.create_index([('blockHeight', pymongo.ASCENDING)],
                                    unique=True)

        return collection

    def collection_user_state(self, client, start_index=True):

        mongo_db = self.db
        db = client[mongo_db]
        collection = db['UserState']

        # index creation
        #if start_index:
        #    collection.create_index([('block_number', pymongo.DESCENDING)], unique=True)

        return collection

    def collection_user_state_update(self, client, start_index=True):

        mongo_db = self.db
        db = client[mongo_db]
        collection = db['UserState_update']

        # index creation
        #if start_index:
        #    collection.create_index([('block_number', pymongo.DESCENDING)], unique=True)

        return collection

    def collection_users(self, client, start_index=True):

        mongo_db = self.db
        db = client[mongo_db]
        collection = db['users']

        # index creation
        #if start_index:
        #    collection.create_index([('block_number', pymongo.DESCENDING)], unique=True)

        return collection

    def collection_bitpro_holders_interest(self, client, start_index=True):

        mongo_db = self.db
        db = client[mongo_db]
        collection = db['BitProHoldersInterest']

        # index creation
        #if start_index:
        #    collection.create_index([('blockHeight', pymongo.ASCENDING)], unique=True)

        return collection

    def collection_settlement_state(self, client, start_index=True):

        mongo_db = self.db
        db = client[mongo_db]
        collection = db['SettlementState']

        # index creation
        if start_index:
            collection.create_index([('startBlockNumber', pymongo.ASCENDING)],
                                    unique=True)

        return collection

    def collection_inrate_income(self, client, start_index=True):

        mongo_db = self.db
        db = client[mongo_db]
        collection = db['InRateIncome']

        # index creation
        #if start_index:
        #    collection.create_index([('blockHeight', pymongo.ASCENDING)], unique=True)

        return collection

    def collection_transaction(self, client, start_index=True):

        mongo_db = self.db
        db = client[mongo_db]
        collection = db['Transaction']

        # index creation
        if start_index:
            collection.create_index([('transactionHash', pymongo.ASCENDING),
                                     ('address', pymongo.ASCENDING),
                                     ('event', pymongo.ASCENDING)], unique=True)

        return collection

    def collection_notification(self, client, start_index=True):

        mongo_db = self.db
        db = client[mongo_db]
        collection = db['Notification']

        # index creation
        if start_index:
            collection.create_index([('transactionHash', pymongo.ASCENDING),
                                     ('logIndex', pymongo.ASCENDING),
                                     ('event', pymongo.ASCENDING)], unique=True)

        return collection

    def collection_moc_indexer(self, client, start_index=True):

        mongo_db = self.db
        db = client[mongo_db]
        collection = db['moc_indexer']

        return collection

    def collection_moc_indexer_history(self, client, start_index=True):

        mongo_db = self.db
        db = client[mongo_db]
        collection = db['moc_indexer_history']

        return collection

    def collection_filtered_transactions(self, client, create=True):

        mongo_db = self.db
        db = client[mongo_db]
        col_name = 'filtered_transactions'

        if create:
            schema = {
                '$jsonSchema': {
                    'bsonType': 'object',
                    'additionalProperties': True,
                    'required': ['hash', 'blockNumber'],
                    'properties': {
                        'hash': {
                            'bsonType': 'string'
                        },
                        'blockNumber': {
                            'bsonType': 'int'
                        },
                        'from': {
                            'bsonType': 'string'
                        },
                        'to': {
                            'bsonType': 'string'
                        },
                        'value': {
                            'bsonType': 'string'
                        },
                        'gas': {
                            'bsonType': 'int'
                        },
                        'gasPrice': {
                            'bsonType': 'string'
                        },
                        'input': {
                            'bsonType': 'string'
                        },
                        'receipt': {
                            'bsonType': 'bool'
                        },
                        'processed': {
                            'bsonType': 'bool'
                        },
                        'gas_used': {
                            'bsonType': 'int'
                        },
                        'confirmations': {
                            'bsonType': 'int'
                        },
                        'timestamp': {
                            'bsonType': 'timestamp'
                        },
                        'logs': {
                            'bsonType': 'string'
                        },
                        'status': {
                            'bsonType': 'string'
                        },
                    }
                }
            }
            result = db.create_collection(col_name, validator=schema)
            result.create_index([('blockNumber', pymongo.DESCENDING)], unique=False)

        collection = db[col_name]

        return collection


mongo_manager = MongoManager()
