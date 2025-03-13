import json

from scripts.common import AppConfigurations
from scripts.constants.db_constants import DatabaseConstants
from scripts.core.schemas import ExportAssetModel, ExportAssets
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


class AssetModelData:
    def __init__(self):
        self.util_mongo_obj = MongoDBUtil(mongo_uri, database)
        self.util_arango_obj = ArangoDBUtil(arango_uri, arango_db, arango_user_name, arango_password)
        self.common_util_obj = CommonUtil()

    def fetch_asset_model_data_from_mongo(self, config: ExportAssetModel):
        """
        Fetch asset model data
        """
        try:
            collection_name = DatabaseConstants.ASSET_MODEL_DETAILS
            results = []
            invalid_asset_models = []

            for model_config in config.data:
                for asset_model_name in model_config.asset_model_name:
                    query = {"asset_model_name": asset_model_name}

                    # Fetch asset model details
                    asset_model_details = list(self.util_mongo_obj.find(collection_name, query))

                    if not asset_model_details:
                        invalid_asset_models.append(asset_model_name)
                        continue  # Skip to the next asset_model_name

                    # Remove tags if migrate_asset_model_params is false
                    if not model_config.migrate_asset_model_params:
                        for asset_model in asset_model_details:
                            if "parameters" in asset_model:
                                del asset_model["parameters"]

                    # Fetch additional data for each asset model
                    for asset_model in asset_model_details:
                        if model_config.migrate_asset_model_param_details and "parameters" in asset_model:
                            tag_ids = asset_model["parameters"]
                            tags_collection = DatabaseConstants.TAGS
                            tags_data = list(self.util_mongo_obj.find(tags_collection, {"tag_id": {"$in": tag_ids}}))
                            asset_model["tags_data"] = tags_data

                        if model_config.migrate_asset_model_rules and "asset_model_id" in asset_model:
                            asset_model_id = asset_model["asset_model_id"]
                            rules_collection = DatabaseConstants.ASSET_MODEL_RULE_ENGINE
                            rules_data = list(
                                self.util_mongo_obj.find(rules_collection, {"asset_model_id": asset_model_id}))
                            asset_model["rules_data"] = rules_data

                        if "industry_category_id" in asset_model:
                            industry_category_id = asset_model["industry_category_id"]
                            industry_category_collection = DatabaseConstants.INDUSTRY_CATEGORY
                            industry_category_data = list(self.util_mongo_obj.find(industry_category_collection, {
                                "industry_category_id": industry_category_id}))
                            asset_model["industry_category_data"] = industry_category_data

                        if "process_id" in asset_model:
                            process_id = asset_model["process_id"]
                            process_conf_collection = DatabaseConstants.PROCESS_CONF
                            process_conf_data = list(
                                self.util_mongo_obj.find(process_conf_collection, {"process_id": process_id}))
                            asset_model["process_conf_data"] = process_conf_data

                        results.append(asset_model)

            if invalid_asset_models:
                print(f"Invalid asset model names: {invalid_asset_models}")
                log.info(f"Invalid asset model names: {invalid_asset_models}")

            return results

        except Exception as err:
            print(f"Exception when fetching fetch_asset_model_data: {str(err)}")
            log.exception(f"Exception when fetching asset data from mongo DB: {str(err)}")
            raise Exception(str(err))

    def fetch_asset_model_data_from_arango(self, config: ExportAssetModel):
        """
        Fetch asset model data from ArangoDB
        """
        try:
            collection_name = DatabaseConstants.ASSET_MODEL_DETAILS
            results = []
            invalid_asset_models = []

            for model_config in config.data:
                for asset_model_name in model_config.asset_model_name:
                    query = {"asset_model_name": asset_model_name}

                    # Fetch asset model details from ArangoDB
                    asset_model_details = list(self.util_arango_obj.find(collection_name, query))

                    if not asset_model_details:
                        invalid_asset_models.append(asset_model_name)
                        continue  # Skip to the next asset_model_name

                    # Remove tags if migrate_asset_model_params is false
                    if not model_config.migrate_asset_model_params:
                        for asset_model in asset_model_details:
                            if "parameters" in asset_model:
                                del asset_model["parameters"]

                    # Fetch additional data for each asset model
                    for asset_model in asset_model_details:
                        if model_config.migrate_asset_model_param_details and "parameters" in asset_model:
                            tag_ids = asset_model["parameters"]
                            tags_collection = DatabaseConstants.TAGS
                            tags_data = list(self.util_arango_obj.find(tags_collection, {"tag_id": {"$in": tag_ids}}))
                            asset_model["tags_data"] = tags_data

                        if model_config.migrate_asset_model_rules and "asset_model_id" in asset_model:
                            asset_model_id = asset_model["asset_model_id"]
                            rules_collection = DatabaseConstants.ASSET_MODEL_RULE_ENGINE
                            rules_data = list(
                                self.util_arango_obj.find(rules_collection, {"asset_model_id": asset_model_id}))
                            asset_model["rules_data"] = rules_data

                        if "industry_category_id" in asset_model:
                            industry_category_id = asset_model["industry_category_id"]
                            industry_category_collection = DatabaseConstants.INDUSTRY_CATEGORY
                            industry_category_data = list(self.util_arango_obj.find(industry_category_collection, {
                                "industry_category_id": industry_category_id}))
                            asset_model["industry_category_data"] = industry_category_data

                        if "process_id" in asset_model:
                            process_id = asset_model["process_id"]
                            process_conf_collection = DatabaseConstants.PROCESS_CONF
                            process_conf_data = list(
                                self.util_arango_obj.find(process_conf_collection, {"process_id": process_id}))
                            asset_model["process_conf_data"] = process_conf_data

                        results.append(asset_model)

            if invalid_asset_models:
                print(f"Invalid asset model names: {invalid_asset_models}")
                log.info(f"Invalid asset model names: {invalid_asset_models}")

            return results

        except Exception as err:
            log.exception(f"Exception when fetching fetch_auditlog_table data: {str(err)}")
            raise Exception(str(err))

    def insert_asset_model_data_to_mongo(self, data_to_insert, migrate_asset_model_param_details, migrate_asset_model_rules):
        """
        Insert asset model data back to respective collections in MongoDB from a JSON file.
        """
        try:
            results = {}

            # Iterate through the data and insert into respective collections
            for asset_model in data_to_insert:
                clean_asset_model = {}
                for key in asset_model:
                    if key not in ["tags_data", "rules_data", "industry_category_data", "process_conf_data"]:
                        clean_asset_model[key] = asset_model[key]
                # Insert into asset model details collection
                collection_name = DatabaseConstants.ASSET_MODEL_DETAILS
                result = self.util_mongo_obj.insert_one(collection_name, clean_asset_model)
                results.setdefault(collection_name, []).append(result)

                # Insert tags data if present
                if migrate_asset_model_param_details and "tags_data" in asset_model:
                    tags_collection = DatabaseConstants.TAGS
                    for tag in asset_model["tags_data"]:
                        result = self.util_mongo_obj.insert_one(tags_collection, tag)
                        results.setdefault(tags_collection, []).append(result)

                # Insert rules data if present
                if migrate_asset_model_rules and "rules_data" in asset_model:
                    rules_collection = DatabaseConstants.ASSET_MODEL_RULE_ENGINE
                    for rule in asset_model["rules_data"]:
                        result = self.util_mongo_obj.insert_one(rules_collection, rule)
                        results.setdefault(rules_collection, []).append(result)

                # Insert industry category data if present
                if "industry_category_data" in asset_model:
                    industry_category_collection = DatabaseConstants.INDUSTRY_CATEGORY
                    for industry in asset_model["industry_category_data"]:
                        result = self.util_mongo_obj.insert_one(industry_category_collection, industry)
                        results.setdefault(industry_category_collection, []).append(result)

                # Insert process configuration data if present
                if "process_conf_data" in asset_model:
                    process_conf_collection = DatabaseConstants.PROCESS_CONF
                    for process in asset_model["process_conf_data"]:
                        result = self.util_mongo_obj.insert_one(process_conf_collection, process)
                        results.setdefault(process_conf_collection, []).append(result)

            log.info("Data inserted successfully from JSON file.")
            return results

        except Exception as err:
            print(f"Exception when inserting asset model data: {str(err)}")
            log.exception(f"Exception when inserting asset model data into MongoDB: {str(err)}")
            raise Exception(str(err))

    def insert_asset_model_data_to_arango(self, data_to_insert, migrate_asset_model_param_details, migrate_asset_model_rules):
        """
        Insert asset model data back to respective collections in ArangoDB from a JSON file.
        """
        try:
            results = {}

            # Iterate through the data and insert into respective collections
            for asset_model in data_to_insert:
                clean_asset_model = {}
                for key in asset_model:
                    if key not in ["tags_data", "rules_data", "industry_category_data", "process_conf_data"]:
                        clean_asset_model[key] = asset_model[key]
                # Insert into asset model details collection
                collection_name = DatabaseConstants.ASSET_MODEL_DETAILS
                result = self.util_arango_obj.insert_one(collection_name, clean_asset_model)
                results.setdefault(collection_name, []).append(result["_key"])

                # Insert tags data if present
                if migrate_asset_model_param_details and "tags_data" in asset_model:
                    tags_collection = DatabaseConstants.TAGS
                    for tag in asset_model["tags_data"]:
                        result = self.util_arango_obj.insert_one(tags_collection, tag)
                        results.setdefault(tags_collection, []).append(result["_key"])

                # Insert rules data if present
                if migrate_asset_model_rules and "rules_data" in asset_model:
                    rules_collection = DatabaseConstants.ASSET_MODEL_RULE_ENGINE
                    for rule in asset_model["rules_data"]:
                        result = self.util_arango_obj.insert_one(rules_collection, rule)
                        results.setdefault(rules_collection, []).append(result["_key"])
                # Insert industry category data if present
                if "industry_category_data" in asset_model:
                    industry_category_collection = DatabaseConstants.INDUSTRY_CATEGORY
                    for industry in asset_model["industry_category_data"]:
                        result = self.util_arango_obj.insert_one(industry_category_collection, industry)
                        results.setdefault(industry_category_collection, []).append(result["_key"])

                # Insert process configuration data if present
                if "process_conf_data" in asset_model:
                    process_conf_collection = DatabaseConstants.PROCESS_CONF
                    for process in asset_model["process_conf_data"]:
                        result = self.util_arango_obj.insert_one(process_conf_collection, process)
                        results.setdefault(process_conf_collection, []).append(result["_key"])

            log.info("Data inserted successfully from JSON file.")
            return results

        except Exception as err:
            print(f"Exception when inserting asset model data: {str(err)}")
            log.exception(f"Exception when inserting asset model data into ArangoDB: {str(err)}")
            raise Exception(str(err))

    def fetch_assets_data_from_mongo(self, config: ExportAssets):
        """
        Fetch hierarchy data from various collections based on flags
        """
        try:
            collection_name = DatabaseConstants.ASSETS
            results = []
            invalid_hierarchies = []

            for asset_config in config.data:
                for hierarchy in asset_config.hierarchy:
                    query = {"hierarchy": hierarchy}

                    # Fetch assets data from the assets collection
                    assets_data = list(self.util_mongo_obj.find(collection_name, query))

                    if not assets_data:
                        invalid_hierarchies.append(hierarchy)
                        continue  # Skip to the next hierarchy

                    # Fetch additional data for each asset
                    for asset in assets_data:
                        # Fetch data from hierarchy_details collection
                        if asset_config.fetch_from_hierarchy_details:
                            query = {"node_id": hierarchy}
                            data = list(self.util_mongo_obj.find(DatabaseConstants.HIERARCHY_DETAILS, query))
                            if data:
                                asset[f"{DatabaseConstants.HIERARCHY_DETAILS}_records"] = data

                        # Fetch data from dynamic_hierarchy_details collection
                        if asset_config.fetch_from_dynamic_hierarchy_details:
                            query = {"node_id": hierarchy}
                            data = list(
                                self.util_mongo_obj.find(DatabaseConstants.DYNAMIC_HIERARCHY_DETAILS, query))
                            if data:
                                asset[f"{DatabaseConstants.DYNAMIC_HIERARCHY_DETAILS}_records"] = data

                        # Fetch data from tag_hierarchy collection
                        if asset_config.fetch_from_tag_hierarchy:
                            query = {"site_id": hierarchy}
                            data = list(self.util_mongo_obj.find(DatabaseConstants.TAG_HIERARCHY, query))
                            if data:
                                asset[f"{DatabaseConstants.TAG_HIERARCHY}_records"] = data

                        # Fetch data from dynamic_tag_hierarchy collection
                        if asset_config.fetch_from_dynamic_tag_hierarchy:
                            query = {"node_id": hierarchy}
                            data = list(
                                self.util_mongo_obj.find(DatabaseConstants.DYNAMIC_TAG_HIERARCHY, query))
                            if data:
                                asset[f"{DatabaseConstants.DYNAMIC_TAG_HIERARCHY}_records"] = data

                        # Fetch data from design_taga_data collection
                        if asset_config.fetch_from_design_taga_data:
                            query = {"site_id": hierarchy}
                            data = list(self.util_mongo_obj.find(DatabaseConstants.DESIGN_TAGA_DATA, query))
                            if data:
                                asset[f"{DatabaseConstants.DESIGN_TAGA_DATA}_records"] = data

                        # Fetch data from dynamic_design_tag_data collection
                        if asset_config.fetch_from_dynamic_design_tag_data:
                            query = {"node_id": hierarchy}
                            data = list(
                                self.util_mongo_obj.find(DatabaseConstants.DYNAMIC_DESIGN_TAG_DATA, query))
                            if data:
                                asset[f"{DatabaseConstants.DYNAMIC_DESIGN_TAG_DATA}_records"] = data

                        results.append(asset)



                if invalid_hierarchies:
                    print(f"Invalid hierarchies: {invalid_hierarchies}")
                    log.info(f"Invalid hierarchies: {invalid_hierarchies}")

                return results

        except Exception as err:
            print(f"Exception when fetching hierarchy data: {str(err)}")
            log.exception(f"Exception when fetching hierarchy data from mongo DB: {str(err)}")
            raise Exception(str(err))

    def fetch_assets_data_from_arango(self, config: ExportAssets):
        """
        Fetch hierarchy data from various collections based on flags
        """
        try:
            collection_name = DatabaseConstants.ASSETS
            results = []
            invalid_hierarchies = []

            for asset_config in config.data:
                for hierarchy in asset_config.hierarchy:
                    query = {"hierarchy": hierarchy}

                    # Fetch assets data from the assets collection
                    assets_data = list(self.util_arango_obj.find(collection_name, query))

                    if not assets_data:
                        invalid_hierarchies.append(hierarchy)
                        continue  # Skip to the next hierarchy

                    # Fetch additional data for each asset
                    for asset in assets_data:
                        # Fetch data from hierarchy_details collection
                        if asset_config.fetch_from_hierarchy_details:
                            query = {"node_id": hierarchy}
                            data = list(self.util_arango_obj.find(DatabaseConstants.HIERARCHY_DETAILS, query))
                            if data:
                                asset[f"{DatabaseConstants.HIERARCHY_DETAILS}_records"] = data

                        # Fetch data from dynamic_hierarchy_details collection
                        if asset_config.fetch_from_dynamic_hierarchy_details:
                            query = {"node_id": hierarchy}
                            data = list(
                                self.util_arango_obj.find(DatabaseConstants.DYNAMIC_HIERARCHY_DETAILS, query))
                            if data:
                                asset[f"{DatabaseConstants.DYNAMIC_HIERARCHY_DETAILS}_records"] = data

                        # Fetch data from tag_hierarchy collection
                        if asset_config.fetch_from_tag_hierarchy:
                            query = {"site_id": hierarchy}
                            data = list(self.util_arango_obj.find(DatabaseConstants.TAG_HIERARCHY, query))
                            if data:
                                asset[f"{DatabaseConstants.TAG_HIERARCHY}_records"] = data

                        # Fetch data from dynamic_tag_hierarchy collection
                        if asset_config.fetch_from_dynamic_tag_hierarchy:
                            query = {"node_id": hierarchy}
                            data = list(
                                self.util_arango_obj.find(DatabaseConstants.DYNAMIC_TAG_HIERARCHY, query))
                            if data:
                                asset[f"{DatabaseConstants.DYNAMIC_TAG_HIERARCHY}_records"] = data

                        # Fetch data from design_taga_data collection
                        if asset_config.fetch_from_design_taga_data:
                            query = {"site_id": hierarchy}
                            data = list(self.util_arango_obj.find(DatabaseConstants.DESIGN_TAGA_DATA, query))
                            if data:
                                asset[f"{DatabaseConstants.DESIGN_TAGA_DATA}_records"] = data

                        # Fetch data from dynamic_design_tag_data collection
                        if asset_config.fetch_from_dynamic_design_tag_data:
                            query = {"node_id": hierarchy}
                            data = list(
                                self.util_arango_obj.find(DatabaseConstants.DYNAMIC_DESIGN_TAG_DATA, query))
                            if data:
                                asset[f"{DatabaseConstants.DYNAMIC_DESIGN_TAG_DATA}_records"] = data

                        results.append(asset)

                if invalid_hierarchies:
                    print(f"Invalid hierarchies: {invalid_hierarchies}")
                    log.info(f"Invalid hierarchies: {invalid_hierarchies}")

                return results

        except Exception as err:
            print(f"Exception when fetching hierarchy data: {str(err)}")
            log.exception(f"Exception when fetching hierarchy data from Arango DB: {str(err)}")
            raise Exception(str(err))

    def insert_assets_data_to_mongo(self, data_to_insert, import_hierarchy_details, import_dynamic_hierarchy_details,
                                    import_tag_hierarchy, import_dynamic_tag_hierarchy, import_design_taga_data,
                                    import_dynamic_design_tag_data):
        """
        Insert assets data back to respective collections in MongoDB from a JSON file.
        """
        try:
            results = {}

            # Define the suffix for records
            records_suffix = "_records"

            # Iterate through the data and insert into respective collections
            for asset in data_to_insert:
                clean_asset = {}
                # Only keep the attributes that are relevant for insertion in assets table
                for key in asset:
                    if key not in [
                        f"{DatabaseConstants.HIERARCHY_DETAILS}{records_suffix}",
                        f"{DatabaseConstants.DYNAMIC_HIERARCHY_DETAILS}{records_suffix}",
                        f"{DatabaseConstants.TAG_HIERARCHY}{records_suffix}",
                        f"{DatabaseConstants.DYNAMIC_TAG_HIERARCHY}{records_suffix}",
                        f"{DatabaseConstants.DESIGN_TAGA_DATA}{records_suffix}",
                        f"{DatabaseConstants.DYNAMIC_DESIGN_TAG_DATA}{records_suffix}"
                    ]:
                        clean_asset[key] = asset[key]

                # Insert into the assets collection
                collection_name = DatabaseConstants.ASSETS
                result = self.util_mongo_obj.insert_one(collection_name, clean_asset)
                results.setdefault(collection_name, []).append(result)

                # Insert hierarchy details
                if import_hierarchy_details and f"{DatabaseConstants.HIERARCHY_DETAILS}{records_suffix}" in asset:
                    hierarchy_collection = DatabaseConstants.HIERARCHY_DETAILS
                    for hierarchy in asset[f"{DatabaseConstants.HIERARCHY_DETAILS}{records_suffix}"]:
                        result = self.util_mongo_obj.insert_one(hierarchy_collection, hierarchy)
                        results.setdefault(hierarchy_collection, []).append(result)

                # Insert dynamic hierarchy details
                if import_dynamic_hierarchy_details and f"{DatabaseConstants.DYNAMIC_HIERARCHY_DETAILS}{records_suffix}" in asset:
                    dynamic_hierarchy_collection = DatabaseConstants.DYNAMIC_HIERARCHY_DETAILS
                    for dynamic_hierarchy in asset[f"{DatabaseConstants.DYNAMIC_HIERARCHY_DETAILS}{records_suffix}"]:
                        result = self.util_mongo_obj.insert_one(dynamic_hierarchy_collection, dynamic_hierarchy)
                        results.setdefault(dynamic_hierarchy_collection, []).append(result)

                # Insert tag hierarchy
                if import_tag_hierarchy and f"{DatabaseConstants.TAG_HIERARCHY}{records_suffix}" in asset:
                    tag_hierarchy_collection = DatabaseConstants.TAG_HIERARCHY
                    for tag in asset[f"{DatabaseConstants.TAG_HIERARCHY}{records_suffix}"]:
                        result = self.util_mongo_obj.insert_one(tag_hierarchy_collection, tag)
                        results.setdefault(tag_hierarchy_collection, []).append(result)

                # Insert dynamic tag hierarchy
                if import_dynamic_tag_hierarchy and f"{DatabaseConstants.DYNAMIC_TAG_HIERARCHY}{records_suffix}" in asset:
                    dynamic_tag_hierarchy_collection = DatabaseConstants.DYNAMIC_TAG_HIERARCHY
                    for dynamic_tag in asset[f"{DatabaseConstants.DYNAMIC_TAG_HIERARCHY}{records_suffix}"]:
                        result = self.util_mongo_obj.insert_one(dynamic_tag_hierarchy_collection, dynamic_tag)
                        results.setdefault(dynamic_tag_hierarchy_collection, []).append(result)

                # Insert design taga data
                if import_design_taga_data and f"{DatabaseConstants.DESIGN_TAGA_DATA}{records_suffix}" in asset:
                    design_taga_collection = DatabaseConstants.DESIGN_TAGA_DATA
                    for design in asset[f"{DatabaseConstants.DESIGN_TAGA_DATA}{records_suffix}"]:
                        result = self.util_mongo_obj.insert_one(design_taga_collection, design)
                        results.setdefault(design_taga_collection, []).append(result)

                # Insert dynamic design tag data
                if import_dynamic_design_tag_data and f"{DatabaseConstants.DYNAMIC_DESIGN_TAG_DATA}{records_suffix}" in asset:
                    dynamic_design_tag_collection = DatabaseConstants.DYNAMIC_DESIGN_TAG_DATA
                    for dynamic_design in asset[f"{DatabaseConstants.DYNAMIC_DESIGN_TAG_DATA}{records_suffix}"]:
                        result = self.util_mongo_obj.insert_one(dynamic_design_tag_collection, dynamic_design)
                        results.setdefault(dynamic_design_tag_collection, []).append(result)

            log.info("Assets data inserted successfully from JSON file.")
            return results

        except Exception as err:
            print(f"Exception when inserting assets data: {str(err)}")
            log.exception(f"Exception when inserting assets data into MongoDB: {str(err)}")
            raise Exception(str(err))

    def insert_assets_data_to_arango(self, data_to_insert, import_hierarchy_details, import_dynamic_hierarchy_details,
                                    import_tag_hierarchy, import_dynamic_tag_hierarchy, import_design_taga_data,
                                    import_dynamic_design_tag_data):
        """
        Insert assets data back to respective collections in ArangoDB from a JSON file.
        """
        try:
            results = {}

            # Define the suffix for records
            records_suffix = "_records"

            # Iterate through the data and insert into respective collections
            for asset in data_to_insert:
                clean_asset = {}
                # Only keep the attributes that are relevant for insertion in assets table
                for key in asset:
                    if key not in [
                        f"{DatabaseConstants.HIERARCHY_DETAILS}{records_suffix}",
                        f"{DatabaseConstants.DYNAMIC_HIERARCHY_DETAILS}{records_suffix}",
                        f"{DatabaseConstants.TAG_HIERARCHY}{records_suffix}",
                        f"{DatabaseConstants.DYNAMIC_TAG_HIERARCHY}{records_suffix}",
                        f"{DatabaseConstants.DESIGN_TAGA_DATA}{records_suffix}",
                        f"{DatabaseConstants.DYNAMIC_DESIGN_TAG_DATA}{records_suffix}"
                    ]:
                        clean_asset[key] = asset[key]

                # Insert into the assets collection
                collection_name = DatabaseConstants.ASSETS
                result = self.util_arango_obj.insert_one(collection_name, clean_asset)
                results.setdefault(collection_name, []).append(result)

                # Insert hierarchy details
                if import_hierarchy_details and f"{DatabaseConstants.HIERARCHY_DETAILS}{records_suffix}" in asset:
                    hierarchy_collection = DatabaseConstants.HIERARCHY_DETAILS
                    for hierarchy in asset[f"{DatabaseConstants.HIERARCHY_DETAILS}{records_suffix}"]:
                        result = self.util_arango_obj.insert_one(hierarchy_collection, hierarchy)
                        results.setdefault(hierarchy_collection, []).append(result)

                # Insert dynamic hierarchy details
                if import_dynamic_hierarchy_details and f"{DatabaseConstants.DYNAMIC_HIERARCHY_DETAILS}{records_suffix}" in asset:
                    dynamic_hierarchy_collection = DatabaseConstants.DYNAMIC_HIERARCHY_DETAILS
                    for dynamic_hierarchy in asset[f"{DatabaseConstants.DYNAMIC_HIERARCHY_DETAILS}{records_suffix}"]:
                        result = self.util_arango_obj.insert_one(dynamic_hierarchy_collection, dynamic_hierarchy)
                        results.setdefault(dynamic_hierarchy_collection, []).append(result)

                # Insert tag hierarchy
                if import_tag_hierarchy and f"{DatabaseConstants.TAG_HIERARCHY}{records_suffix}" in asset:
                    tag_hierarchy_collection = DatabaseConstants.TAG_HIERARCHY
                    for tag in asset[f"{DatabaseConstants.TAG_HIERARCHY}{records_suffix}"]:
                        result = self.util_arango_obj.insert_one(tag_hierarchy_collection, tag)
                        results.setdefault(tag_hierarchy_collection, []).append(result)

                # Insert dynamic tag hierarchy
                if import_dynamic_tag_hierarchy and f"{DatabaseConstants.DYNAMIC_TAG_HIERARCHY}{records_suffix}" in asset:
                    dynamic_tag_hierarchy_collection = DatabaseConstants.DYNAMIC_TAG_HIERARCHY
                    for dynamic_tag in asset[f"{DatabaseConstants.DYNAMIC_TAG_HIERARCHY}{records_suffix}"]:
                        result = self.util_arango_obj.insert_one(dynamic_tag_hierarchy_collection, dynamic_tag)
                        results.setdefault(dynamic_tag_hierarchy_collection, []).append(result)

                # Insert design taga data
                if import_design_taga_data and f"{DatabaseConstants.DESIGN_TAGA_DATA}{records_suffix}" in asset:
                    design_taga_collection = DatabaseConstants.DESIGN_TAGA_DATA
                    for design in asset[f"{DatabaseConstants.DESIGN_TAGA_DATA}{records_suffix}"]:
                        result = self.util_arango_obj.insert_one(design_taga_collection, design)
                        results.setdefault(design_taga_collection, []).append(result)

                # Insert dynamic design tag data
                if import_dynamic_design_tag_data and f"{DatabaseConstants.DYNAMIC_DESIGN_TAG_DATA}{records_suffix}" in asset:
                    dynamic_design_tag_collection = DatabaseConstants.DYNAMIC_DESIGN_TAG_DATA
                    for dynamic_design in asset[f"{DatabaseConstants.DYNAMIC_DESIGN_TAG_DATA}{records_suffix}"]:
                        result = self.util_arango_obj.insert_one(dynamic_design_tag_collection, dynamic_design)
                        results.setdefault(dynamic_design_tag_collection, []).append(result)

            log.info("Assets data inserted successfully from JSON file.")
            return results

        except Exception as err:
            print(f"Exception when inserting assets data: {str(err)}")
            log.exception(f"Exception when inserting assets data into ArangoDB: {str(err)}")
            raise Exception(str(err))




