from typing import Optional, List, Dict, Any

from fastapi import UploadFile, File, Form
from pydantic import BaseModel, Field


class DefaultSuccessResponse(BaseModel):
    status: str = "success"
    message: Optional[str]
    data: Optional[str]

class AssetsModelConfig(BaseModel):
    asset_model_name: List[str]
    migrate_asset_model_params: bool
    migrate_asset_model_param_details: bool
    migrate_asset_model_rules: bool

class ExportAssetModel(BaseModel):
    data: List[AssetsModelConfig]

class GetParameter(BaseModel):
    tag_names: list[str]

class ImportAssetModel(BaseModel):
    asset_model_name : List[str] = Form(...)
    migrate_asset_model_params : bool = Form(...)
    migrate_asset_model_param_details : bool = Form(...)
    migrate_asset_model_rules : bool = Form(...)

class AssetsConfig(BaseModel):
    hierarchy: List[str]
    fetch_from_hierarchy_details: bool
    fetch_from_dynamic_hierarchy_details: bool
    fetch_from_tag_hierarchy: bool
    fetch_from_dynamic_tag_hierarchy: bool
    fetch_from_design_taga_data: bool
    fetch_from_dynamic_design_tag_data: bool

class ExportAssets(BaseModel):
    data : list[AssetsConfig]

class GetProtocol(BaseModel):
    protocol_names: list[str]

class GetUsers(BaseModel):
    user_names: list[str]


