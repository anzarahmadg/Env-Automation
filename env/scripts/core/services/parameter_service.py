from scripts.constants.api import APIEndPoints
from fastapi import APIRouter
from starlette.responses import PlainTextResponse, JSONResponse
from fastapi.responses import FileResponse
from scripts.core.handlers.parameter_handler import Parameter
from scripts.core.schemas import GetParameter, DefaultSuccessResponse
from scripts.logger.log_module import logger as log


parameterRouter = APIRouter(prefix=APIEndPoints.parameter_tag, tags=["Export"])


@parameterRouter.post(APIEndPoints.export_parameter_tag)
def export_parameter_data(input_json: GetParameter):
    """
    API Endpoint to download parameter data
    """
    try:
        result = Parameter().get_parameter(input_json)
        if isinstance(result, tuple) and len(result) == 2:
            file_path, file_name = result
            return FileResponse(path=file_path, media_type='application/gzip', filename=file_name)
        else:
            return DefaultSuccessResponse(message=str(result))
    except Exception as e:
        log.error(f"Something went wrong while fetching parameter details: {str(e)}")
        return PlainTextResponse(f"Error: {str(e)}", status_code=500)
