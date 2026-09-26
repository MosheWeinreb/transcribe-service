import logging
import signal
import threading
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID

from kombu import pools
from sqlalchemy import CursorResult, and_, delete, func, or_, select, update

from src.database import close_database, session_factory
from src.schemas import JobStatus, TranscriptionMessage
from src.settings.celery_app import celery_app
from src.settings.config import get_settings
from src.settings.logger import configure_logging
from src.storage import prepare_storage, remove_media
from src.transcription.models import Job

log = logging.getLogger(__name__)


def dispatch_pending() -> None:
    settings = get_settings()
    for _ in range(settings.maintenance_batch_size):
        with session_factory().begin() as session:
            job = session.scalar(
                select(Job)
                .where(
                    Job.status == JobStatus.QUEUED,
                    Job.available_at <= func.now(),
                    or_(
                        Job.dispatch_pending,
                        Job.published_at
                        < func.now() - timedelta(seconds=settings.redispatch_seconds),
                    ),
                )
                .order_by(Job.dispatch_pending.desc(), Job.available_at, Job.id)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if job is None:
                return
            # Publish confirmation precedes the DB commit. Redelivery is safe if that commit fails.
            celery_app.send_task(
                "transcribe",
                kwargs=TranscriptionMessage(job_id=job.id).model_dump(mode="json"),
                retry=False,
                ignore_result=True,
                delivery_mode=2,
                # Expire the old delivery before recovery republishes this queued job.
                expires=settings.redispatch_seconds,
                confirm_timeout=settings.broker_timeout_seconds,
            )
            job.dispatch_pending = False
            # PostgreSQL now() is the transaction start, before the broker round trip.
            job.published_at = func.clock_timestamp()
        log.info("job_dispatched", extra={"job_id": str(job.id)})


def recover_expired() -> None:
    settings = get_settings()
    with session_factory().begin() as session:
        session.execute(
            update(Job)
            .where(
                Job.status == JobStatus.PROCESSING,
                Job.deadline < func.now(),
                Job.attempts >= settings.max_attempts,
            )
            .values(
                status=JobStatus.FAILED,
                finished_at=func.now(),
                deadline=None,
                dispatch_pending=False,
                error_code="processing_timeout",
                error_message="Processing could not finish within the allowed attempts.",
            )
        )
        result = session.execute(
            update(Job)
            .where(Job.status == JobStatus.PROCESSING, Job.deadline < func.now())
            .values(
                status=JobStatus.QUEUED,
                deadline=None,
                available_at=func.now() + timedelta(seconds=settings.retry_backoff_seconds),
                dispatch_pending=True,
            )
        )
        if cast(CursorResult, result).rowcount:
            log.warning("expired_attempts_requeued")
        session.execute(
            update(Job)
            .where(
                Job.status == JobStatus.QUEUED,
                Job.available_at < func.now() - timedelta(seconds=settings.max_queue_wait_seconds),
            )
            .values(
                status=JobStatus.FAILED,
                finished_at=func.now(),
                dispatch_pending=False,
                error_code="queue_timeout",
                error_message=(
                    "Processing could not start within the queue wait limit. Please submit again."
                ),
            )
        )


def cleanup() -> None:
    settings = get_settings()
    with session_factory().begin() as session:
        expired = session.scalars(
            select(Job.id)
            .where(
                or_(
                    and_(Job.status == "uploading", Job.deadline < func.now()),
                    Job.finished_at < func.now() - timedelta(seconds=settings.retention_seconds),
                )
            )
            .with_for_update(skip_locked=True)
            .limit(settings.maintenance_batch_size)
        ).all()
        for expired_id in expired:
            remove_media(expired_id)
            session.execute(delete(Job).where(Job.id == expired_id))
            log.info("job_expired", extra={"job_id": str(expired_id)})
        known_ids = {str(job_id) for job_id in session.scalars(select(Job.id))}
    cutoff = datetime.now(UTC).timestamp() - (
        max(settings.upload_timeout_seconds, settings.task_timeout_seconds)
        + settings.recovery_grace_seconds
    )
    # ponytail: scan the bounded single-host media directory; use storage lifecycle rules at scale.
    for path in settings.media_dir.iterdir():
        # Also retire PCM files left by older deployments.
        if path.suffix not in {".part", ".media", ".pcm"}:
            continue
        try:
            job_id = str(UUID(path.name.split(".")[0]))
            if path.stat().st_mtime < cutoff and (path.suffix == ".pcm" or job_id not in known_ids):
                path.unlink(missing_ok=True)
        except (ValueError, FileNotFoundError):
            continue


def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    prepare_storage()
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    try:
        while not stop.is_set():
            # A broker outage must not prevent retention or expired-attempt recovery.
            for operation in (recover_expired, cleanup, dispatch_pending):
                try:
                    operation()
                except Exception:
                    log.exception("maintenance_failed", extra={"error_code": operation.__name__})
            stop.wait(settings.maintenance_interval_seconds)
    finally:
        pools.reset()
        close_database()


if __name__ == "__main__":
    main()
