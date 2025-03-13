from scripts.db.querydb.parameter_data import ParameterData
from scripts.utils.common_util import CommonUtil
from scripts.logger.log_module import logger as log



class Parameter:
    def __init__(self):
        self.parameter_db_obj = ParameterData()
        self.common_util_obj = CommonUtil()


    def get_parameter(self, input_json):
        """
        Retrieve parameter data from mongo and arango, create a compressed JSON file in tar format,
        and return the file name and path if data is available.
        """
        log.info("Definition export parameter data is started.")
        try:
            parameter_data_mongo = self.parameter_db_obj.fetch_parameter_data_from_mongo(input_json)
            parameter_data_arango = self.parameter_db_obj.fetch_parameter_data_from_arango(input_json)
            json_files = []
            if parameter_data_mongo:
                mongo_file_path, mongo_file_name = self.common_util_obj.list_of_dicts_to_json(parameter_data_mongo, suffix="_mongo")

                # check records count
                db_record_count = len(parameter_data_mongo)
                file_record_count = self.common_util_obj.get_record_count_from_file(mongo_file_path)
                print(f"db_record_count: {db_record_count} file_record_count: {file_record_count}")
                if db_record_count != file_record_count:
                    log.error("Record count mismatch between mongo database and downloaded file")
                    raise Exception("Record count mismatch between mongo database and downloaded file")

                json_files.append(mongo_file_path)

            else:
                log.info("Data is not available for the given tag names in mongodb")
                return "Data is not available for the given tag names in mongodb"

            if parameter_data_arango:
                arango_file_path, arango_file_name = self.common_util_obj.list_of_dicts_to_json(parameter_data_arango, suffix="_arango")

                # check records count
                db_record_count = len(parameter_data_arango)
                file_record_count = self.common_util_obj.get_record_count_from_file(arango_file_path)
                print(f"db_record_count: {db_record_count} file_record_count: {file_record_count}")
                if db_record_count != file_record_count:
                    log.error("Record count mismatch between arango database and downloaded file")
                    raise Exception("Record count mismatch between arango database and downloaded file")

                json_files.append(arango_file_path)
            else:
                log.info("Data is not available for the given tag names in arango")
                return "Data is not available for the given tag names in arango"

            tar_file_path, tar_file_name = self.common_util_obj.compress_to_tar_gz(json_files, suffix="_parameter_data")
            return tar_file_path, tar_file_name

        except Exception as e:
            log.error("Exception while fetching export parameter data :" + str(e))
            return [], False, 0
        finally:
            log.info("Definition export parameter data is ended.")
