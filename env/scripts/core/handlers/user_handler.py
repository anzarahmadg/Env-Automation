from scripts.constants.ui_constants import DashboardConstants
from scripts.db.querydb.parameter_data import ParameterData
from scripts.db.querydb.user_data import UserData
from scripts.utils.common_util import CommonUtil
from scripts.logger.log_module import logger as log



class Users:
    def __init__(self):
        self.user_db_obj = UserData()
        self.common_util_obj = CommonUtil()


    def export_users(self, input_json):
        """
        Retrieve users data from mongo and arango, create a compressed JSON file in tar format,
        and return the file name and path if data is available.
        """
        log.info("Definition export parameter data is started.")
        try:
            user_data_mongo = self.user_db_obj.fetch_user_data_from_mongo(input_json)
            user_data_arango = self.user_db_obj.fetch_user_data_from_arango(input_json)
            json_files = []
            if user_data_mongo:
                mongo_file_path, mongo_file_name = self.common_util_obj.list_of_dicts_to_json(user_data_mongo, suffix="_mongo")

                # check records count
                db_record_count = len(user_data_mongo)
                file_record_count = self.common_util_obj.get_record_count_from_file(mongo_file_path)
                print(f"db_record_count: {db_record_count} file_record_count: {file_record_count}")
                if db_record_count != file_record_count:
                    log.error("Record count mismatch between mongo database and downloaded file")
                    raise Exception("Record count mismatch between mongo database and downloaded file")

                json_files.append(mongo_file_path)

            else:
                log.info("Data is not available for the given users in mongodb")
                return "Data is not available for the given users in mongodb"

            if user_data_arango:
                arango_file_path, arango_file_name = self.common_util_obj.list_of_dicts_to_json(user_data_arango, suffix="_arango")

                # check records count
                db_record_count = len(user_data_arango)
                file_record_count = self.common_util_obj.get_record_count_from_file(arango_file_path)
                print(f"db_record_count: {db_record_count} file_record_count: {file_record_count}")
                if db_record_count != file_record_count:
                    log.error("Record count mismatch between arango database and downloaded file")
                    raise Exception("Record count mismatch between arango database and downloaded file")

                json_files.append(arango_file_path)
            else:
                log.info("Data is not available for the given users in arango")
                return "Data is not available for the given users in arango"

            tar_file_path, tar_file_name = self.common_util_obj.compress_to_tar_gz(json_files, suffix="_user_data")
            return tar_file_path, tar_file_name

        except Exception as e:
            log.error("Exception while fetching export user data :" + str(e))
            return [], False, 0
        finally:
            log.info("Definition export user data is ended.")

    def import_users(self, db_name, file):
        """
        Retrieve users data from json files
        """

        try:
            log.info("Definition import users is started.")
            file_name = file.filename
            if file_name.endswith('.json'):
                data_from_file = self.common_util_obj.read_json_file(file)
                if 'arango' in file_name:
                    asset_data_arango = self.user_db_obj.insert_user_data_to_arango(data_from_file)
                    if asset_data_arango:
                        message = "Data Inserted Successfully in ArangoDB"
                        return asset_data_arango, message
                elif 'mongo' in file_name:
                    asset_data_mongo = self.user_db_obj.insert_user_data_to_mongo(data_from_file)
                    if asset_data_mongo:
                        message = "Data Inserted Successfully in MongoDB"
                        return asset_data_mongo, message
                else:
                    message = "Invalid file name"
                    return DashboardConstants.no_data_inserted, message
            else:
                log.error("File is not a JSON file.")
                message = "Invalid file type, must be a JSON file."
                return DashboardConstants.no_data_inserted, message

        except Exception as e:
            log.error("Exception while import_users :" + str(e))
            message = "Exception while import_users :" + str(e)
            return DashboardConstants.no_data_inserted, message

        finally:
            log.info("Definition import users data is ended.")

