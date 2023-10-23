from pymongo import MongoClient
from config import config

class KMongoDb:
    """
        MongoDB class
    """
    def __init__(self, database):
        self.client = MongoClient(config.MONGO_CONNECTION_URI)
        self.database = self.client[database]


    def create_collection(self, collection):
        """
            Create collection if not exists
        """
        if collection not in self.database.list_collection_names():
            self.database[collection]


    def get_records(self, collection, find_query={}, select_query=None):
        """
            Get records from collection by queries params
        """
        if select_query:
            records = self.database[collection].find(find_query, select_query)
        else:
            records = self.database[collection].find(find_query)

        return records


    def count_records(self, collection, find_query={}):
        """
            Count records from collection by query
        """
        number_of_records = self.database[collection].count(find_query)
        return number_of_records


    def get_sorted_limited_records(self, 
            collection,
            sort_field,
            ascending=False,
            find_query={},
            select_query=None,
            limit=config.DEFAULT_TOP_LIMIT):
        """
            Get sorted values from collection by query & limit results
        """

        ascending = 1 if True else -1

        if select_query:
            records = self.database[collection].find(find_query, select_query).sort(sort_field, ascending).limit(limit)
        else:
            records = self.database[collection].find(find_query).sort(sort_field, ascending).limit(limit)

        return records


    def insert_records(self, collection, records, delete_records=False, delete_query={}):
        """
            Insert records with\without old records deletion
        """
        if delete_records:
            self.database[collection].delete_many(delete_query)

        self.database[collection].insert_many(records)
