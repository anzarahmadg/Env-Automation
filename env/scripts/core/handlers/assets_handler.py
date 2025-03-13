from scripts.db.querydb.asset_model_data import AssetModelData
from scripts.utils.common_util import CommonUtil
from scripts.logger.log_module import logger as log
from scripts.constants.ui_constants import DashboardConstants



class AssetModel:
    def __init__(self):
        self.asset_model_db_obj = AssetModelData()
        self.common_util_obj = CommonUtil()


    def export_asset_model_data(self, input_json):
        """
        Retrieve asset model data from mongo and arango, create a compressed JSON file in tar format,
        and return the file name and path if data is available.
        """

        try:
            log.info("Definition export asset model data is started.")
            asset_data_mongo = self.asset_model_db_obj.fetch_asset_model_data_from_mongo(input_json)
            asset_data_arango = self.asset_model_db_obj.fetch_asset_model_data_from_arango(input_json)
            json_files = []
            if asset_data_mongo:
                mongo_file_path, mongo_file_name = self.common_util_obj.list_of_dicts_to_json(asset_data_mongo, suffix="_mongo")

                # check records count
                db_record_count = len(asset_data_mongo)
                file_record_count = self.common_util_obj.get_record_count_from_file(mongo_file_path)
                print(f"db_record_count: {db_record_count} file_record_count: {file_record_count}")
                if db_record_count != file_record_count:
                    log.error("Record count mismatch between mongo database and downloaded file")
                    raise Exception("Record count mismatch between mongo database and downloaded file")

                json_files.append(mongo_file_path)
            else:
                log.info("Data is not available for given asset model names in mongodb")
                return "Data is not available for given asset model names in mongodb"

            if asset_data_arango:
                arango_file_path, arango_file_name = self.common_util_obj.list_of_dicts_to_json(asset_data_arango, suffix="_arango")

                # check records count
                db_record_count = len(asset_data_arango)
                file_record_count = self.common_util_obj.get_record_count_from_file(arango_file_path)
                print(f"db_record_count: {db_record_count} file_record_count: {file_record_count}")
                if db_record_count != file_record_count:
                    log.error("Record count mismatch between arango database and downloaded file")
                    raise Exception("Record count mismatch between arango database and downloaded file")

                json_files.append(arango_file_path)
            else:
                log.info("Data is not available for given asset model names in arango db")
                return "Data is not available for given asset model names in arango db"

            tar_file_path, tar_file_name = self.common_util_obj.compress_to_tar_gz(json_files, suffix="_asset_model_data")
            return tar_file_path, tar_file_name


        except Exception as e:
            log.error("Exception while fetching export_asset_model_data :" + str(e))
            return [], False, 0

        finally:
            log.info("Definition export asset model data is ended.")

    def import_asset_model_data(self, migrate_asset_model_param_details, migrate_asset_model_rules,db_name, file):
        """
        Retrieve asset model data from json files
        """

        try:
            log.info("Definition import asset model data is started.")
            file_name = file.filename
            if file_name.endswith('.json'):
                data_from_file = self.common_util_obj.read_json_file(file)
                if 'arango' in file_name:
                    asset_data_arango = self.asset_model_db_obj.insert_asset_model_data_to_arango(data_from_file,
                                                                                                  migrate_asset_model_param_details,
                                                                                                  migrate_asset_model_rules)
                    if asset_data_arango:
                        message = "Data Inserted Successfully in ArangoDB"
                        return asset_data_arango, message
                elif 'mongo' in file_name:
                    asset_data_mongo = self.asset_model_db_obj.insert_asset_model_data_to_mongo(data_from_file,
                                                                                                migrate_asset_model_param_details,
                                                                                                migrate_asset_model_rules)
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
            log.error("Exception while import_asset_model_data :" + str(e))
            message = "Exception while import_asset_model_data :" + str(e)
            return DashboardConstants.no_data_inserted, message

        finally:
            log.info("Definition import asset model data is ended.")

    def export_assets_data(self, input_json):
        """
        Retrieve assets data from mongo and arango, create a compressed JSON file in tar format,
        and return the file name and path if data is available.
        """

        try:
            log.info("Definition export assets data is started.")
            assets_data_mongo = self.asset_model_db_obj.fetch_assets_data_from_mongo(input_json)
            assets_data_arango = self.asset_model_db_obj.fetch_assets_data_from_arango(input_json)
            json_files = []
            if assets_data_mongo:
                mongo_file_path, mongo_file_name = self.common_util_obj.list_of_dicts_to_json(assets_data_mongo,
                                                                                              suffix="_mongo")

                # check records count
                db_record_count = len(assets_data_mongo)
                file_record_count = self.common_util_obj.get_record_count_from_file(mongo_file_path)
                print(f"db_record_count: {db_record_count} file_record_count: {file_record_count}")
                if db_record_count != file_record_count:
                    log.error("Record count mismatch between mongo database and downloaded file")
                    raise Exception("Record count mismatch between mongo database and downloaded file")

                json_files.append(mongo_file_path)
            else:
                log.info("Data is not available for given hierarchy in mongodb")
                return "Data is not available for given hierarchy in mongodb"

            if assets_data_arango:
                arango_file_path, arango_file_name = self.common_util_obj.list_of_dicts_to_json(assets_data_arango,
                                                                                                suffix="_arango")

                # check records count
                db_record_count = len(assets_data_arango)
                file_record_count = self.common_util_obj.get_record_count_from_file(arango_file_path)
                print(f"db_record_count: {db_record_count} file_record_count: {file_record_count}")
                if db_record_count != file_record_count:
                    log.error("Record count mismatch between arango database and downloaded file")
                    raise Exception("Record count mismatch between arango database and downloaded file")

                json_files.append(arango_file_path)
            else:
                log.info("Data is not available for given hierarchy arango db")
                return "Data is not available for given hierarchy in arango db"

            tar_file_path, tar_file_name = self.common_util_obj.compress_to_tar_gz(json_files,
                                                                                   suffix="_assets_data")
            return tar_file_path, tar_file_name


        except Exception as e:
            log.error("Exception while fetching export_assets_data :" + str(e))
            return [], False, 0

        finally:
            log.info("Definition export assets data is ended.")

    def import_assets_data(self, import_hierarchy_details, import_dynamic_hierarchy_details,
                                    import_tag_hierarchy, import_dynamic_tag_hierarchy, import_design_taga_data,
                                    import_dynamic_design_tag_data,db_name, file):
        """
        Retrieve assets data from json files
        """

        try:
            log.info("Definition import assets data is started.")
            file_name = file.filename
            if file_name.endswith('.json'):
                data_from_file = self.common_util_obj.read_json_file(file)
                if 'arango' in file_name:
                    asset_data_arango = self.asset_model_db_obj.insert_assets_data_to_arango(data_from_file,import_hierarchy_details, import_dynamic_hierarchy_details,
                                    import_tag_hierarchy, import_dynamic_tag_hierarchy, import_design_taga_data,
                                    import_dynamic_design_tag_data)
                    if asset_data_arango:
                        message = "Data Inserted Successfully in ArangoDB"
                        return asset_data_arango, message
                elif 'mongo' in file_name:
                    asset_data_mongo = self.asset_model_db_obj.insert_assets_data_to_mongo(data_from_file,import_hierarchy_details, import_dynamic_hierarchy_details,
                                    import_tag_hierarchy, import_dynamic_tag_hierarchy, import_design_taga_data,
                                    import_dynamic_design_tag_data)
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
            log.error("Exception while import_assets_data :" + str(e))
            message = "Exception while import_assets_data :" + str(e)
            return DashboardConstants.no_data_inserted, message

        finally:
            log.info("Definition import assets data is ended.")



