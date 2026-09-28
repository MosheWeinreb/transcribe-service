from fastapi import APIRouter

from src.database import check_database
from src.schemas import ErrorResponse, LivenessResponse, ReadinessResponse
from src.storage import check_disk_space

router = APIRouter(prefix="/health", responses={"default": {"model": ErrorResponse}})


@router.get("/live")
async def live() -> LivenessResponse:
    return LivenessResponse()


@router.get("/ready")
async def ready() -> ReadinessResponse:
    await check_database()
    await check_disk_space()
    return ReadinessResponse()
