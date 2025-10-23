from pymongo import MongoClient
from time import time
import json

DEBUG = False
MONGO_CONNECTION_STRING = "mongodb://root:example@mongo-dev.eurasia.kz:27037/"
MONGO_DATABASE_NAME = "s4_dev"
TIMESTAMP_FIELD = '__metatimestamp_timestamp'


class MongoConnector:
    def __init__(self, mongo_connection_string, database_name, collection_name):
        self.mongo_connection_string = mongo_connection_string
        self.database_name = database_name
        self.collection_name = collection_name

    def add_timestamp(self, records, start_time: object) -> object:
        for record in records:
            record[TIMESTAMP_FIELD] = start_time
        return records

    def save_data_to_mongodb(self, data, system=None, collection_name=None, timestamp=None, replace=False, timeseries=False):
        if collection_name is None:
            collection_name = self.collection_name

        if timestamp is None:
            print(f"Timestamp is none ! Setting to current time.")
            timestamp = time()
        try:
            with MongoClient(self.mongo_connection_string) as client:
                db = client[self.database_name]
                if timeseries:
                    data = self.add_timestamp(data, timestamp)
                    if collection_name not in db.list_collection_names():
                        db.create_collection(collection_name, timeseries={'timeField': TIMESTAMP_FIELD})
                collection = db[collection_name]
                if replace:
                    collection.delete_many({})
                collection.insert_many(data)
        except Exception as e:
            print(f"Error saving data to MongoDB: {e}")

    def load_data_from_mongodb(self, query=None):
        data = []
        try:
            with MongoClient(self.mongo_connection_string) as client:
                db = client[self.database_name]
                collection = db[self.collection_name]
                cursor = collection.find(query) if query else collection.find()
                data = list(cursor)
        except Exception as e:
            print(f"Error loading data from MongoDB: {e}")
        return data

    def delete_last(self, number=0):
        data = []
        try:
            with MongoClient(self.mongo_connection_string) as client:
                db = client[self.database_name]
                collection = db[self.collection_name]
                # Получение последних 4000 записей
                documents_to_delete = collection.find().sort([('_id', -1)]).limit(number)

                # Удаление записей
                for document in documents_to_delete:
                    collection.delete_one({'_id': document['_id']})
                print(f"Последние {number} записей удалены успешно.")
        except Exception as e:
            print(f"Error loading data from MongoDB: {e}")
        return data

    def find(self, query):
        data = []

        try:
            with MongoClient(self.mongo_connection_string) as client:
                db = client[self.database_name]
                collection = db[self.collection_name]
                cursor = collection.find(query) if query else collection.find()
                data = list(cursor)
        except Exception as e:
            print(f"Error loading data from MongoDB: {e}")
        return data


def main():
    mongo_connector = MongoConnector(MONGO_CONNECTION_STRING, MONGO_DATABASE_NAME, 'test')
    with open("./data/zpayments2set.json", "r") as f:
        data = json.loads(f.read())
    mongo_connector.save_data_to_mongodb(data, replace=True)


if __name__ == "__main__":
    main()
