import asyncio
import logging
from typing import Annotated
from uuid import UUID

import anyio
from fastapi import APIRouter, Header, Query, Request, Response

from src.exceptions import AppError
from src.schemas import ErrorDetail, ErrorResponse, JobAccepted, JobResult, PublicConfig
from src.settings.config import settings
from src.storage import check_upload_size, remove_media, save_upload, storage_errors
from src.transcription import service

router = APIRouter(prefix="/api", responses={"default": {"model": ErrorResponse}})
log = logging.getLogger(__name__)


@router.post(
    "/jobs",
    status_code=202,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {
                "application/octet-stream": {
                    "schema": {"type": "string", "format": "binary"},
                }
            },
        },
    },
)
async def upload_job(
    request: Request,
    response: Response,
    filename: Annotated[str, Query(min_length=1)] = "recording",
    content_length: Annotated[int | None, Header(ge=0)] = None,
) -> JobAccepted:
    """Stream a binary file body. Do not wrap it in multipart/form-data."""
    if request.headers.get("content-type", "").startswith("multipart/"):
        raise AppError("invalid_content_type", "Send the file as the binary request body.", 415)
    if content_length is not None:
        check_upload_size(content_length, settings.max_upload_bytes)
    job_id = await service.reserve_upload(filename)
    finalized_file = False
    try:
        with storage_errors():
            async with asyncio.timeout(settings.upload_timeout_seconds):
                size = await save_upload(job_id, request.stream())
                finalized_file = True
                await service.finalize_upload(job_id, size)
    except BaseException as exc:
        # Shield cleanup from a client disconnect or request cancellation.
        # Once the file is committed, leave it for recovery if the database outcome is uncertain.
        with anyio.CancelScope(shield=True):
            if not finalized_file:
                await remove_media(job_id)
                await service.abandon_upload(job_id)
        if isinstance(exc, TimeoutError):
            raise AppError(
                "upload_timeout", "The upload took too long. Please try again.", 408
            ) from exc
        raise
    accepted = JobAccepted(id=job_id, status_url=f"/api/jobs/{job_id}")
    response.headers["Location"] = accepted.status_url
    log.info("upload_accepted", extra={"job_id": str(job_id)})
    return accepted


@router.get("/jobs/{job_id}")
async def read_job(job_id: UUID) -> JobResult:
    job = await service.get_job(job_id)
    result = JobResult.model_validate(job)
    if job.error_code:
        result.error = ErrorDetail(
            code=job.error_code, message=job.error_message or "Processing failed. Please try again."
        )
    return result


@router.get("/config")
async def public_config() -> PublicConfig:
    return PublicConfig(
        max_upload_bytes=settings.max_upload_bytes,
        max_filename_length=settings.max_filename_length,
        max_duration_seconds=settings.max_duration_seconds,
        recording_stop_ms=settings.max_duration_seconds * 1000 - settings.recording_margin_ms,
        poll_interval_ms=settings.poll_interval_ms,
        retention_seconds=settings.retention_seconds,
    )
