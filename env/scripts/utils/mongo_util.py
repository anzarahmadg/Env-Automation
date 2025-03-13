import logging
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from bson import ObjectId
from scripts.logger.log_module import logger as log



class MongoDBUtil:
    def __init__(self, connection_uri, database):
        try:
            self.client = MongoClient(connection_uri)
            self.db = self.client[database]
            self.connection = None
            self.loadType = None
            print("MongoDB connection initialized!")
            log.info("MongoDB connection initialized!")
        except PyMongoError as err:
            log.error(f"MongoDB connection error: {str(err)}")
            raise

    def connect(self):
        """MongoDB uses connection pooling, so explicit connect is usually not needed"""
        try:
            # Verify connection
            self.client.admin.command('ping')
            log.info("MongoDB connection verified!")
        except PyMongoError as e:
            log.error(f"Failed to connect to MongoDB: {str(e)}")
            raise

    def close(self):
        """Close the MongoDB connection"""
        try:
            self.client.close()
            log.info("MongoDB connection closed.")
        except PyMongoError as e:
            log.error(f"Error closing connection: {str(e)}")


    def get_records_count(self, collection_name, query):
        """Count documents matching a query"""
        try:
            return self.db[collection_name].count_documents(query)
        except PyMongoError as e:
            log.error(f"Count error: {str(e)}")
            return 0

    def find(self, collection_name, query):
        try:
            collection = self.db[collection_name]
            results = collection.find(query)
            return list(results)
        except Exception as err:
            print(f"Error while fetching data from {collection_name}: {str(err)}")
            log.error(f"Error while fetching data from {collection_name}: {str(err)}")
            raise Exception(str(err))


    def insert_one(self, collection_name: str, document: dict):
        """
           Insert a single document into the specified MongoDB collection.
           If a document with the same identifier exists, it will be updated.

           :param collection_name: Name of the collection to insert the document into.
           :param document: The document (dictionary) to be inserted or updated.
        """
        try:
            collection = self.db[collection_name]
            document_id = document.get('_id')

            # Convert string _id to ObjectId
            if isinstance(document_id, str):
                document_id = ObjectId(document_id)
                document['_id'] = document_id

            result = collection.update_one(
                {'_id': document_id},
                {'$set': document},
                upsert=True  # Insert if it doesn't exist
            )
            if result.upserted_id:
                print(f"Document inserted with id: {result.upserted_id}")
                return result.upserted_id
            else:
                print(f"Document with id: {document_id} updated.")
                return document_id
        except Exception as err:
            print(f"Exception when inserting document: {str(err)}")
            raise Exception(str(err))

