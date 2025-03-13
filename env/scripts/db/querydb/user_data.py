import binascii

from bson import Binary

from scripts.common import AppConfigurations
from scripts.constants.db_constants import DatabaseConstants
from scripts.core.schemas import GetUsers
from scripts.utils.arango_util import ArangoDBUtil
from scripts.utils.common_util import CommonUtil
from scripts.utils.mongo_util import MongoDBUtil
from scripts.logger.log_module import logger as log
import base64




mongo_uri = AppConfigurations.DbDetails.mongo_uri
database = AppConfigurations.DbDetails.database

arango_uri = AppConfigurations.DbDetails.arango_uri
arango_db = AppConfigurations.DbDetails.arango_db
arango_user_name = AppConfigurations.DbDetails.arango_user_name
arango_password = AppConfigurations.DbDetails.arango_password

class UserData:
    def __init__(self):
        self.util_mongo_obj = MongoDBUtil(mongo_uri, database)
        self.util_arango_obj = ArangoDBUtil(arango_uri, arango_db, arango_user_name, arango_password)
        self.common_util_obj = CommonUtil()


    def fetch_user_data_from_mongo(self, input_json: GetUsers):
        """
        Fetch user data from mongo
        """
        try:
            collection_name = DatabaseConstants.USER

            query = {"username": {"$in": input_json.user_names}}
            user_data = list(self.util_mongo_obj.find(collection_name, query))

            for user in user_data:
                user_roles = user["userrole"]
                user_role_collection = DatabaseConstants.USER_ROLE
                user_roles_data = list(self.util_mongo_obj.find(user_role_collection, {"user_role_id": {"$in": user_roles}}))
                if user_roles_data:
                    user[f"{DatabaseConstants.USER_ROLE}_records"] = user_roles_data

                user_access_group_ids = user["access_group_ids"]
                user_access_group_collection = DatabaseConstants.ACCESS_GROUP
                user_access_group_data = list(
                    self.util_mongo_obj.find(user_access_group_collection, {"access_group_id": {"$in": user_access_group_ids}}))
                if user_access_group_data:
                    user[f"{DatabaseConstants.ACCESS_GROUP}_records"] = user_access_group_data

                # Fetch node_ids from AccessLevel
                node_ids = []
                for level in user["AccessLevel"].values():
                    node_ids.extend(item["node_id"] for item in level)

                if node_ids:
                    hierarchy_details_collection = DatabaseConstants.HIERARCHY_DETAILS
                    # Fetch data from access_group collection for the node_id
                    hierarchy_details_data = list(self.util_mongo_obj.find(hierarchy_details_collection, {"node_id": {"$in": node_ids}}))
                    if hierarchy_details_data:
                        user[f"{DatabaseConstants.HIERARCHY_DETAILS}_records"] = hierarchy_details_data


            return user_data
        except Exception as err:
            log.error(f"Exception when fetching fetch_user_data_from_mongo: {str(err)}")
            raise Exception(str(err))

    def fetch_user_data_from_arango(self, input_json: GetUsers):
        """
        Fetch user data from arango
        """
        try:
            collection_name = DatabaseConstants.USER

            query = {"username": {"$in": input_json.user_names}}
            user_data = list(self.util_arango_obj.find(collection_name, query))

            for user in user_data:
                user_roles = user["userrole"]
                user_role_collection = DatabaseConstants.USER_ROLE
                user_roles_data = list(self.util_arango_obj.find(user_role_collection, {"user_role_id": {"$in": user_roles}}))
                if user_roles_data:
                    user[f"{DatabaseConstants.USER_ROLE}_records"] = user_roles_data

                user_access_group_ids = user["access_group_ids"]
                user_access_group_collection = DatabaseConstants.ACCESS_GROUP
                user_access_group_data = list(
                    self.util_arango_obj.find(user_access_group_collection, {"access_group_id": {"$in": user_access_group_ids}}))
                if user_access_group_data:
                    user[f"{DatabaseConstants.ACCESS_GROUP}_records"] = user_access_group_data

                # Fetch node_ids from AccessLevel
                node_ids = []
                for level in user["AccessLevel"].values():
                    node_ids.extend(item["node_id"] for item in level)

                if node_ids:
                    hierarchy_details_collection = DatabaseConstants.HIERARCHY_DETAILS
                    # Fetch data from access_group collection for the node_id
                    hierarchy_details_data = list(self.util_arango_obj.find(hierarchy_details_collection, {"node_id": {"$in": node_ids}}))
                    if hierarchy_details_data:
                        user[f"{DatabaseConstants.HIERARCHY_DETAILS}_records"] = hierarchy_details_data

            return user_data
        except Exception as err:
            log.error(f"Exception when fetching fetch_user_data_from_arango: {str(err)}")
            raise Exception(str(err))

    def insert_user_data_to_mongo(self, data_to_insert):
        """
        Insert user data back to respective collections in MongoDB from a JSON file.
        """
        try:
            results = {}
            records_suffix = "_records"

            # Iterate through the data and insert into respective collections
            for user in data_to_insert:

                # Decode email and phone number from base64 to bytes
                # if 'email' in user:
                #     user['email']['d']['$binary']['base64'] = base64.b64decode(user['email']['d']['$binary']['base64'])
                #     user['email']['t']['$binary']['base64'] = base64.b64decode(user['email']['t']['$binary']['base64'])
                # if 'phonenumber' in user:
                #     user['phonenumber']['d']['$binary']['base64'] = base64.b64decode(
                #         user['phonenumber']['d']['$binary']['base64'])
                #     user['phonenumber']['t']['$binary']['base64'] = base64.b64decode(
                #         user['phonenumber']['t']['$binary']['base64'])


                user = self.convert_string_object_to_base64_object(user)


                # user = self.convert_string_object_to_base64_object(user)

                clean_user = {}
                # Only keep the attributes that are relevant for insertion in assets table
                for key in user:
                    if key not in [
                        f"{DatabaseConstants.USER_ROLE}{records_suffix}",
                        f"{DatabaseConstants.ACCESS_GROUP}{records_suffix}",
                        f"{DatabaseConstants.HIERARCHY_DETAILS}{records_suffix}",
                    ]:
                        clean_user[key] = user[key]

                collection_name = DatabaseConstants.USER
                result = self.util_mongo_obj.insert_one(collection_name, clean_user)
                results.setdefault(collection_name, []).append(result)

                if f"{DatabaseConstants.USER_ROLE}{records_suffix}" in user:
                    user_role_collection = DatabaseConstants.USER_ROLE
                    for item in user[f"{DatabaseConstants.USER_ROLE}{records_suffix}"]:
                        result = self.util_mongo_obj.insert_one(user_role_collection, item)
                        results.setdefault(user_role_collection, []).append(result)

                if f"{DatabaseConstants.ACCESS_GROUP}{records_suffix}" in user:
                    access_group_collection = DatabaseConstants.ACCESS_GROUP
                    for item in user[f"{DatabaseConstants.ACCESS_GROUP}{records_suffix}"]:
                        result = self.util_mongo_obj.insert_one(access_group_collection, item)
                        results.setdefault(access_group_collection, []).append(result)

                if f"{DatabaseConstants.HIERARCHY_DETAILS}{records_suffix}" in user:
                    hierarchy_collection = DatabaseConstants.HIERARCHY_DETAILS
                    for hierarchy in user[f"{DatabaseConstants.HIERARCHY_DETAILS}{records_suffix}"]:
                        result = self.util_mongo_obj.insert_one(hierarchy_collection, hierarchy)
                        results.setdefault(hierarchy_collection, []).append(result)

            log.info("Data inserted successfully from JSON file.")
            return results

        except Exception as err:
            print(f"Exception when inserting user data: {str(err)}")
            log.exception(f"Exception when inserting user data into MongoDB: {str(err)}")
            raise Exception(str(err))

    def insert_user_data_to_arango(self, data_to_insert):
        """
        Insert user data back to respective collections in ArangoDB from a JSON file.
        """
        try:

            results = {}
            records_suffix = "_records"

            # Iterate through the data and insert into respective collections
            for user in data_to_insert:

                clean_user = {}
                # Only keep the attributes that are relevant for insertion in assets table
                for key in user:
                    if key not in [
                        f"{DatabaseConstants.USER_ROLE}{records_suffix}",
                        f"{DatabaseConstants.ACCESS_GROUP}{records_suffix}",
                        f"{DatabaseConstants.HIERARCHY_DETAILS}{records_suffix}",
                    ]:
                        clean_user[key] = user[key]

                collection_name = DatabaseConstants.USER
                result = self.util_arango_obj.insert_one(collection_name, clean_user)
                results.setdefault(collection_name, []).append(result)

                if f"{DatabaseConstants.USER_ROLE}{records_suffix}" in user:
                    user_role_collection = DatabaseConstants.USER_ROLE
                    for item in user[f"{DatabaseConstants.USER_ROLE}{records_suffix}"]:
                        result = self.util_arango_obj.insert_one(user_role_collection, item)
                        results.setdefault(user_role_collection, []).append(result)

                if f"{DatabaseConstants.ACCESS_GROUP}{records_suffix}" in user:
                    access_group_collection = DatabaseConstants.ACCESS_GROUP
                    for item in user[f"{DatabaseConstants.ACCESS_GROUP}{records_suffix}"]:
                        result = self.util_arango_obj.insert_one(access_group_collection, item)
                        results.setdefault(access_group_collection, []).append(result)

                if f"{DatabaseConstants.HIERARCHY_DETAILS}{records_suffix}" in user:
                    hierarchy_collection = DatabaseConstants.HIERARCHY_DETAILS
                    for hierarchy in user[f"{DatabaseConstants.HIERARCHY_DETAILS}{records_suffix}"]:
                        result = self.util_arango_obj.insert_one(hierarchy_collection, hierarchy)
                        results.setdefault(hierarchy_collection, []).append(result)

            log.info("Data inserted successfully from JSON file.")
            return results

        except Exception as err:
            print(f"Exception when inserting user data: {str(err)}")
            log.exception(f"Exception when inserting user data into ArangoDB: {str(err)}")
            raise Exception(str(err))

    def convert_string_object_to_base64_object(self, data):
        """
        Reverts simplified Base64 strings to MongoDB BSON binary format.
        """
        if isinstance(data, dict):
            if "d" in data and isinstance(data["d"], str) and "t" in data and isinstance(data['t'], str):
                try:
                    d_binary = Binary(base64.b64decode(data["d"]), 0)
                    t_binary = Binary(base64.b64decode(data["t"]), 0)

                    return {
                        "d": d_binary,
                        "t": t_binary
                    }
                    # d_bytes = base64.b64decode(data["d"])
                    # t_bytes = base64.b64decode(data['t'])
                    # return {
                    #     "d": {"$binary": {"base64": data["d"], "subType": "00"}},
                    #     "t": {"$binary": {"base64": data['t'], "subType": "00"}}
                    # }
                except binascii.Error:
                    return data  # Return original data if the base64 decoding fails.
            else:
                return {k: self.convert_string_object_to_base64_object(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self.convert_string_object_to_base64_object(item) for item in data]
        else:
            return data


