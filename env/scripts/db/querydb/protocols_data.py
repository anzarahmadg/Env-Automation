from scripts.common import AppConfigurations
from scripts.constants.db_constants import DatabaseConstants
from scripts.core.schemas import GetProtocol
from scripts.utils.arango_util import ArangoDBUtil
from scripts.utils.common_util import CommonUtil
from scripts.utils.mongo_util import MongoDBUtil
from scripts.logger.log_module import logger as log



mongo_uri = AppConfigurations.DbDetails.mongo_uri
database = AppConfigurations.DbDetails.database

arango_uri = AppConfigurations.DbDetails.arango_uri
arango_db = AppConfigurations.DbDetails.arango_db
arango_user_name = AppConfigurations.DbDetails.arango_user_name
arango_password = AppConfigurations.DbDetails.arango_password

class ProtocolsData:
    def __init__(self):
        self.util_mongo_obj = MongoDBUtil(mongo_uri, database)
        self.util_arango_obj = ArangoDBUtil(arango_uri, arango_db, arango_user_name, arango_password)
        self.common_util_obj = CommonUtil()


    def fetch_protocols_data_from_mongo(self, input_json: GetProtocol):
        """
        Fetch tags data from mongo
        """
        try:
            collection_name = DatabaseConstants.PROTOCOL_LIST

            # Check if protocol_names is empty
            if not input_json.protocol_names:
                query = {}  # Fetch all documents if the list is empty
            else:
                query = {"name": {"$in": input_json.protocol_names}}

            protocol_data = list(self.util_mongo_obj.find(collection_name, query))

            return protocol_data
        except Exception as err:
            log.error(f"Exception when fetching fetch_protocol_data_from_mongo: {str(err)}")
            raise Exception(str(err))

    def fetch_protocols_data_from_arango(self, input_json: GetProtocol):
        """
        Fetch tags data from arango
        """
        try:
            collection_name = DatabaseConstants.PROTOCOL_LIST

            # Check if protocol_names is empty
            if not input_json.protocol_names:
                query = {}  # Fetch all documents if the list is empty
            else:
                query = {"name": {"$in": input_json.protocol_names}}
            protocol_data = list(self.util_arango_obj.find(collection_name, query))

            return protocol_data
        except Exception as err:
            log.error(f"Exception when fetching fetch_protocol_data_from_arango: {str(err)}")
            raise Exception(str(err))

    def insert_protocols_data_to_mongo(self, data_to_insert):
        """
        Insert protocol data back to respective collections in MongoDB from a JSON file.
        """
        try:
            results = {}

            # Iterate through the data and insert into respective collections
            for protocol in data_to_insert:
                collection_name = DatabaseConstants.PROTOCOL_LIST
                result = self.util_mongo_obj.insert_one(collection_name, protocol)
                results.setdefault(collection_name, []).append(result)

            log.info("Data inserted successfully from JSON file.")
            return results

        except Exception as err:
            print(f"Exception when inserting protocol data: {str(err)}")
            log.exception(f"Exception when inserting protocol data into MongoDB: {str(err)}")
            raise Exception(str(err))

    def insert_protocols_data_to_arango(self, data_to_insert):
        """
        Insert protocol data back to respective collections in ArangoDB from a JSON file.
        """
        try:
            results = {}

            # Iterate through the data and insert into respective collections
            for protocol in data_to_insert:
                collection_name = DatabaseConstants.PROTOCOL_LIST
                result = self.util_arango_obj.insert_one(collection_name, protocol)
                results.setdefault(collection_name, []).append(result)

            log.info("Data inserted successfully from JSON file.")
            return results

        except Exception as err:
            print(f"Exception when inserting asset model data: {str(err)}")
            log.exception(f"Exception when inserting protocol data into ArangoDB: {str(err)}")
            raise Exception(str(err))
