from fastapi import APIRouter
from scripts.core.services import assets_service, parameter_service, protocols_service, user_service

router = APIRouter()
router.include_router(assets_service.assetModelRouter)
router.include_router(parameter_service.parameterRouter)
router.include_router(protocols_service.protocolRouter)
router.include_router(user_service.userRouter)

