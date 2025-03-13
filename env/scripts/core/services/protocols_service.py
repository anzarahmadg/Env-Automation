from scripts.constants.api import APIEndPoints
from fastapi import APIRouter, UploadFile, File
from starlette.responses import PlainTextResponse, JSONResponse
from fastapi.responses import FileResponse
from scripts.core.handlers.protocols_handler import Protocols
from scripts.core.schemas import GetProtocol, DefaultSuccessResponse
from scripts.logger.log_module import logger as log


protocolRouter = APIRouter(prefix=APIEndPoints.protocol, tags=["Export"])


@protocolRouter.post(APIEndPoints.export_protocols)
def export_protocol_data(input_json: GetProtocol):
    """
    API Endpoint to download parameter data
    """
    try:
        result = Protocols().export_protocols(input_json)
        if isinstance(result, tuple) and len(result) == 2:
            file_path, file_name = result
            return FileResponse(path=file_path, media_type='application/gzip', filename=file_name)
        else:
            return DefaultSuccessResponse(message=str(result))
    except Exception as e:
        log.error(f"Something went wrong while fetching parameter details: {str(e)}")
        return PlainTextResponse(f"Error: {str(e)}", status_code=500)

@protocolRouter.post(APIEndPoints.import_protocols)
async def import_asset_model_data(file: UploadFile = File(...)):

    try:
        result, message = Protocols().import_protocols("db_name",  file)
        return DefaultSuccessResponse(message=message, data = str(result))
    except Exception as e:
        log.error(f"Something went wrong while inserting assets: {str(e)}")
        return PlainTextResponse(f"Error: {str(e)}", status_code=500)
