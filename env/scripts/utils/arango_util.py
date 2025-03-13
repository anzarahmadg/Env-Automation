import logging
from arango import ArangoClient
from arango.exceptions import DocumentInsertError, DocumentUpdateError
from scripts.logger.log_module import logger as log


class ArangoDBUtil:
    def __init__(self, connection_uri, database, username, password):
        try:
            self.client = ArangoClient(hosts=connection_uri)
            self.db = self.client.db(database, username=username, password=password)
            print("ArangoDB connection initialized!")
            log.info("ArangoDB connection initialized!")
        except Exception as err:
            log.error(f"ArangoDB connection error: {str(err)}")
            raise

    def connect(self):
        """Verify the connection to ArangoDB"""
        try:
            # Ping the database
            if self.db.name:
                print("ArangoDB connection verified!")
                log.info("ArangoDB connection verified!")
        except Exception as e:
            print(f"Failed to connect to ArangoDB: {str(e)}")
            log.error(f"Failed to connect to ArangoDB: {str(e)}")
            raise


    def find(self, collection_name, query):
        """
        Fetch documents based on a query
        """
        try:
            if query == {}:
                # If query is None, fetch all documents
                aql_query = f'FOR doc IN {collection_name} RETURN doc'
                cursor = self.db.aql.execute(aql_query)
            else:
                # Create AQL filter string
                filter_clauses = []
                bind_vars = {}

                for key, value in query.items():
                    if isinstance(value, dict) and "$in" in value:
                        # Handle $in operator
                        filter_clauses.append(f'doc.{key} IN @{key}')
                        bind_vars[key] = value["$in"]
                    else:
                        filter_clauses.append(f'doc.{key} == @{key}')
                        bind_vars[key] = value

                filter_string = ' AND '.join(filter_clauses)
                cursor = self.db.aql.execute(f'FOR doc IN {collection_name} FILTER {filter_string} RETURN doc',
                                             bind_vars=bind_vars)
            return list(cursor)
        except Exception as err:
            print(f"Error while fetching data from {collection_name}: {str(err)}")
            log.error(f"Error while fetching data from {collection_name}: {str(err)}")
            raise

    def insert_one(self, collection_name: str, document: dict):
        """
        Insert a single document into the specified ArangoDB collection.
        If a document with the same id exists, it will be updated.

        :param collection_name: Name of the collection to insert the document into.
        :param document: The document (dictionary) to be inserted or updated.
        """
        try:
            collection = self.db.collection(collection_name)
            # Insert or update the document
            result = collection.insert(document, overwrite_mode='replace')
            print(f"Document with key: {result} updated.")
            return result

        except Exception as err:
            print(f"Exception when inserting document: {str(err)}")
            raise Exception(str(err))
