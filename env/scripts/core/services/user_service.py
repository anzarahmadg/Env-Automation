from scripts.constants.api import APIEndPoints
from fastapi import APIRouter, UploadFile, File
from starlette.responses import PlainTextResponse, JSONResponse
from fastapi.responses import FileResponse
from scripts.core.handlers.user_handler import Users
from scripts.core.schemas import GetUsers, DefaultSuccessResponse
from scripts.logger.log_module import logger as log


userRouter = APIRouter(prefix=APIEndPoints.user, tags=["Export"])


@userRouter.post(APIEndPoints.export_users)
def export_user_data(input_json: GetUsers):
    """
    API Endpoint to download user data
    """
    try:
        result = Users().export_users(input_json)
        if isinstance(result, tuple) and len(result) == 2:
            file_path, file_name = result
            return FileResponse(path=file_path, media_type='application/gzip', filename=file_name)
        else:
            return DefaultSuccessResponse(message=str(result))
    except Exception as e:
        log.error(f"Something went wrong while fetching user details: {str(e)}")
        return PlainTextResponse(f"Error: {str(e)}", status_code=500)

@userRouter.post(APIEndPoints.import_users)
async def import_user_data(file: UploadFile = File(...)):

    try:
        result, message = Users().import_users("db_name",  file)
        return DefaultSuccessResponse(message=message, data = str(result))
    except Exception as e:
        log.error(f"Something went wrong while inserting users: {str(e)}")
        return PlainTextResponse(f"Error: {str(e)}", status_code=500)
