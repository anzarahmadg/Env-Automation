from typing import List

from scripts.constants.api import APIEndPoints
from fastapi import APIRouter, UploadFile, File, Form
from starlette.responses import PlainTextResponse
from fastapi.responses import FileResponse
from scripts.core.handlers.assets_handler import AssetModel
from scripts.core.schemas import ExportAssetModel, DefaultSuccessResponse, ImportAssetModel, AssetsModelConfig, \
    ExportAssets
from scripts.logger.log_module import logger as log

assetModelRouter = APIRouter(prefix=APIEndPoints.asset, tags=["Export"])


@assetModelRouter.post(APIEndPoints.export_asset_model)
def export_asset_model_data(input_json: ExportAssetModel):
    """
    API Endpoint to download asset model data
    """
    try:
        result = AssetModel().export_asset_model_data(input_json)
        if isinstance(result, tuple) and len(result) == 2:
            file_path, file_name = result
            return FileResponse(path=file_path, media_type='application/gzip', filename=file_name)
        else:
            return DefaultSuccessResponse(message=str(result))
    except Exception as e:
        log.error(f"Something went wrong while fetching asset details: {str(e)}")
        return PlainTextResponse(f"Error: {str(e)}", status_code=500)

@assetModelRouter.post(APIEndPoints.import_asset_model)
async def import_asset_model_data(migrate_asset_model_param_details : bool = Form(...), migrate_asset_model_rules : bool = Form(...), file: UploadFile = File(...)):

    try:
        # import_asset_model = ImportAssetModel(data=AssetsModelConfig.parse_raw(data))
        result, message = AssetModel().import_asset_model_data(migrate_asset_model_param_details, migrate_asset_model_rules,"db_name",  file)
        return DefaultSuccessResponse(message=message, data = str(result))
    except Exception as e:
        log.error(f"Something went wrong while inserting asset details: {str(e)}")
        return PlainTextResponse(f"Error: {str(e)}", status_code=500)


@assetModelRouter.post(APIEndPoints.export_assets)
def export_asset_model_data(input_json: ExportAssets):
    """
    API Endpoint to download assets data
    """
    try:
        result = AssetModel().export_assets_data(input_json)
        if isinstance(result, tuple) and len(result) == 2:
            file_path, file_name = result
            return FileResponse(path=file_path, media_type='application/gzip', filename=file_name)
        else:
            return DefaultSuccessResponse(message=str(result))
    except Exception as e:
        log.error(f"Something went wrong while fetching assets detail: {str(e)}")
        return PlainTextResponse(f"Error: {str(e)}", status_code=500)

@assetModelRouter.post(APIEndPoints.import_assets)
async def import_asset_model_data(import_hierarchy_details : bool = Form(...), import_dynamic_hierarchy_details : bool = Form(...), import_tag_hierarchy : bool = Form(...), import_dynamic_tag_hierarchy : bool = Form(...), import_design_taga_data : bool = Form(...), import_dynamic_design_tag_data : bool = Form(...),  file: UploadFile = File(...)):

    try:
        result, message = AssetModel().import_assets_data(import_hierarchy_details, import_dynamic_hierarchy_details,
                                    import_tag_hierarchy, import_dynamic_tag_hierarchy, import_design_taga_data,
                                    import_dynamic_design_tag_data,"db_name",  file)
        return DefaultSuccessResponse(message=message, data = str(result))
    except Exception as e:
        log.error(f"Something went wrong while inserting assets: {str(e)}")
        return PlainTextResponse(f"Error: {str(e)}", status_code=500)

