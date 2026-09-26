import logging
import time

import httpx
from celery.exceptions import Reject
from pydantic import ValidationError
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeout

from src.exceptions import AppError, StorageFullError
from src.schemas import TranscriptionMessage
from src.settings.celery_app import celery_app
from src.settings.config import get_settings
from src.storage import media_path
from src.transcription.service import claim_job, finish_job, retry_job

log = logging.getLogger(__name__)


def _should_retry(error: Exception) -> bool:
    if isinstance(error, httpx.HTTPStatusError):
        return error.response.status_code in get_settings().retry_http_statuses
    return isinstance(
        error, (StorageFullError, ConnectionError, TimeoutError, httpx.TransportError)
    )


@celery_app.task(name="transcribe")
def transcribe_job(job_id: str) -> None:
    try:
        message = TranscriptionMessage.model_validate({"job_id": job_id})
    except ValidationError:
        raise Reject("Invalid transcription message", requeue=False) from None
    job = claim_job(message.job_id)
    if job is None:
        return  # Duplicate, delayed, or already completed delivery.
    started = time.monotonic()
    fields = {"job_id": str(message.job_id), "attempt": job.attempts}
    log.info("processing_started", extra=fields)
    try:
        from src.transcription.engine import transcribe

        transcript, language, duration = transcribe(media_path(job.id))
        saved = finish_job(job, transcript=transcript, language=language, duration=duration)
        log.info(
            "processing_succeeded" if saved else "processing_result_ignored",
            extra={
                **fields,
                "duration_ms": round((time.monotonic() - started) * 1000),
            },
        )
    except (OperationalError, PoolTimeout):
        # Celery logs the failure; maintenance recovers the existing lease.
        raise
    except Exception as exc:
        if _should_retry(exc):
            log.warning(
                "processing_retry",
                extra={**fields, "error_code": getattr(exc, "code", type(exc).__name__)},
            )
            retry_job(job)
        elif isinstance(exc, AppError):
            saved = finish_job(job, error=exc)
            log.info(
                "processing_rejected" if saved else "processing_result_ignored",
                extra={**fields, "error_code": exc.code},
            )
        else:
            error = AppError("processing_failed", "The recording could not be transcribed.")
            if isinstance(exc, httpx.HTTPStatusError):
                error = AppError("model_unavailable", "The model is unavailable.")
            log.exception("processing_failed", extra={**fields, "error_code": error.code})
            finish_job(job, error=error)
