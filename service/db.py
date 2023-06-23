from pymongo import MongoClient
from config import MONGO_CONNECTION_STRING, DEFAULT_TOP_LIMIT

class KMongoDb:
    def __init__(self, database):
        self.client = MongoClient(MONGO_CONNECTION_STRING)
        self.database = self.client[database]


    def create_collection(self, collection):
        if collection not in self.database.list_collection_names():
            self.database[collection]


    def get_records(self, collection, find_query={}, select_query=None):
        if select_query:
            records = self.database[collection].find(find_query, select_query)
        else:
            records = self.database[collection].find(find_query)
        
        return records


    def count_records(self, collection, find_query={}):
        number_of_records = self.database[collection].count(find_query)
        return number_of_records


    def get_sorted_limited_records(self, 
            collection, 
            sort_query, 
            ascending=False, 
            find_query={}, 
            select_query=None, 
            limit=DEFAULT_TOP_LIMIT):
    
        ascending = 1 if True else -1
        
        if select_query:
            records = self.database[collection].find(find_query, select_query).sort(sort_query, ascending).limit(limit)
        else:
            records = self.database[collection].find(find_query).sort(sort_query, ascending).limit(limit)

        return records


    def insert_records(self, collection, records, delete_records=False):
        if delete_records:
            self.database[collection].delete_many({})

        self.database[collection].insert_many(records)
