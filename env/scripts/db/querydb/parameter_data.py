from scripts.common import AppConfigurations
from scripts.constants.db_constants import DatabaseConstants
from scripts.core.schemas import GetParameter
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

class ParameterData:
    def __init__(self):
        self.util_mongo_obj = MongoDBUtil(mongo_uri, database)
        self.util_arango_obj = ArangoDBUtil(arango_uri, arango_db, arango_user_name, arango_password)
        self.common_util_obj = CommonUtil()


    def fetch_parameter_data_from_mongo(self, input_json: GetParameter):
        """
        Fetch tags data from mongo
        """
        try:
            collection_name = DatabaseConstants.TAGS

            query = {"tag_name": {"$in": input_json.tag_names}}
            parameter_data = list(self.util_mongo_obj.find(collection_name, query))

            return parameter_data
        except Exception as err:
            log.error(f"Exception when fetching fetch_parameter_data_from_mongo: {str(err)}")
            raise Exception(str(err))

    def fetch_parameter_data_from_arango(self, input_json: GetParameter):
        """
        Fetch tags data from arango
        """
        try:
            collection_name = DatabaseConstants.TAGS

            query = {"tag_name": {"$in": input_json.tag_names}}
            parameter_data = list(self.util_arango_obj.find(collection_name, query))

            return parameter_data
        except Exception as err:
            log.error(f"Exception when fetching fetch_parameter_data_from_arango: {str(err)}")
            raise Exception(str(err))