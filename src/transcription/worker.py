"""Database polling and cleanup, owned by the API lifespan. Inference runs in processes."""

import logging
import time

import anyio
import httpx
from anyio import to_process
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeout

from src.exceptions import AppError
from src.settings.config import settings
from src.storage import media_path
from src.transcription import cleanup, service
from src.transcription.engine import transcribe
from src.transcription.models import Job

log = logging.getLogger(__name__)


def should_retry(error: Exception) -> bool:
    if isinstance(error, AppError):
        return error.code == "storage_full" or (
            error.code == "model_unavailable" and error.status_code in settings.retry_http_statuses
        )
    return isinstance(
        error, (ConnectionError, TimeoutError, httpx.TransportError, anyio.BrokenWorkerProcess)
    )


async def process_job(job: Job, limiter: anyio.CapacityLimiter) -> None:
    fields = {"job_id": str(job.id), "attempt": job.attempts}
    started = time.monotonic()
    log.info("processing_started", extra=fields)
    try:
        with anyio.fail_after(settings.task_timeout_seconds):
            transcript, language, duration = await to_process.run_sync(
                transcribe, media_path(job.id), cancellable=True, limiter=limiter
            )
        saved = await service.finish_job(
            job, transcript=transcript, language=language, duration=duration
        )
        log.info(
            "processing_succeeded" if saved else "processing_result_ignored",
            extra={**fields, "duration_ms": round((time.monotonic() - started) * 1000)},
        )
    except anyio.get_cancelled_exc_class():
        # AnyIO stops the child before returning cancellation. A hard crash uses lease recovery.
        with anyio.CancelScope(shield=True):
            await service.retry_job(job)
        raise
    except (OperationalError, PoolTimeout):
        # Preserve the lease if saving an outcome is uncertain; the poll loop logs once.
        raise
    except Exception as exc:
        if should_retry(exc):
            log.warning(
                "processing_retry",
                extra={**fields, "error_code": getattr(exc, "code", type(exc).__name__)},
            )
            await service.retry_job(job)
        else:
            if isinstance(exc, AppError):
                error = exc
                log.info("processing_rejected", extra={**fields, "error_code": error.code})
            else:
                error = AppError("processing_failed", "The recording could not be transcribed.")
                log.exception("processing_failed", extra=fields)
            await service.finish_job(job, error=error)


async def poll(limiter: anyio.CapacityLimiter) -> None:
    while True:
        try:
            job = await service.claim_job()
            if job is not None:
                await process_job(job, limiter)
                continue
        except Exception:
            log.exception("worker_failed")
        await anyio.sleep(settings.worker_poll_seconds)


async def housekeeping() -> None:
    while True:
        for operation in (cleanup.recover_expired, cleanup.cleanup):
            try:
                await operation()
            except Exception:
                log.exception("cleanup_failed", extra={"operation": operation.__name__})
        await anyio.sleep(settings.cleanup_interval_seconds)
