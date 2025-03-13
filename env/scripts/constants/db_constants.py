from scripts.common import AppConfigurations


class _DatabaseConstants:
    db = AppConfigurations.DbDetails.database

    # Table Name
    ASSET_MODEL_DETAILS = "asset_model_details"
    TAGS = "tags"
    INDUSTRY_CATEGORY = "industry_category"
    ASSET_MODEL_RULE_ENGINE = "asset_model_rule_engine"
    PROCESS_CONF = "process_conf"
    ASSETS = "assets"
    HIERARCHY_DETAILS = "hierarchy_details"
    DYNAMIC_HIERARCHY_DETAILS = "dynamic_hierarchy_details"
    TAG_HIERARCHY = "tag_hierarchy"
    DYNAMIC_TAG_HIERARCHY = "dynamic_tag_hierarchy"
    DESIGN_TAGA_DATA = "design_taga_data"
    DYNAMIC_DESIGN_TAG_DATA = "dynamic_design_tag_data"
    PROTOCOL_LIST = "protocol_list"
    USER = "user"
    USER_ROLE = "user_role"
    ACCESS_GROUP = "access_group"



DatabaseConstants = _DatabaseConstants()
