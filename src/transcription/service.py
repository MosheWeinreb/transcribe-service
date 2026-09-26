from datetime import timedelta
from uuid import UUID, uuid4

from sqlalchemy import delete, func, select, update

from src.database import session_factory
from src.exceptions import AppError, StorageFullError
from src.schemas import JobStatus
from src.settings.config import get_settings
from src.storage import check_disk_space
from src.transcription.models import Job


def reserve_upload(filename: str) -> UUID:
    settings = get_settings()
    if not filename or len(filename) > settings.max_filename_length:
        raise AppError("invalid_filename", "Choose a file with a shorter, nonempty name.", 422)
    with session_factory().begin() as session:
        # ponytail: serialize admission; use quota rows if the short lock becomes a bottleneck.
        session.execute(select(func.pg_advisory_xact_lock(func.hashtext("transcriber.admission"))))
        counts = session.execute(
            select(
                func.count()
                .filter(Job.status.in_(["uploading", "queued", "processing"]))
                .label("active"),
                func.count().filter(Job.status == "uploading").label("uploading"),
                func.coalesce(func.sum(Job.upload_bytes), 0).label("stored"),
            )
        ).one()
        if counts.active >= settings.max_active_jobs:
            raise AppError("capacity_exceeded", "The service is busy. Please try again later.", 503)
        if counts.uploading >= settings.max_concurrent_uploads:
            raise AppError("uploads_busy", "Too many uploads. Please try again shortly.", 503)
        if counts.stored + settings.max_upload_bytes > settings.max_stored_bytes:
            raise StorageFullError()
        check_disk_space(counts.uploading + 1)
        job_id = uuid4()
        session.add(
            Job(
                id=job_id,
                filename=filename,
                status="uploading",
                upload_bytes=settings.max_upload_bytes,
                deadline=func.now()
                + timedelta(
                    seconds=settings.upload_timeout_seconds + settings.recovery_grace_seconds
                ),
            )
        )
    return job_id


def finalize_upload(job_id: UUID, size: int) -> None:
    with session_factory().begin() as session:
        updated_id = session.scalar(
            update(Job)
            .where(Job.id == job_id, Job.status == "uploading")
            .values(
                status=JobStatus.QUEUED,
                upload_bytes=size,
                available_at=func.now(),
                deadline=None,
                dispatch_pending=True,
            )
            .returning(Job.id)
        )
        if updated_id is None:
            raise AppError("upload_expired", "The upload expired. Please try again.", 408)


def abandon_upload(job_id: UUID) -> None:
    with session_factory().begin() as session:
        session.execute(delete(Job).where(Job.id == job_id, Job.status == "uploading"))


def get_job(job_id: UUID) -> Job:
    with session_factory().begin() as session:
        job = session.scalar(select(Job).where(Job.id == job_id, Job.status != "uploading"))
    if job is None:
        raise AppError("job_not_found", "This job does not exist or has expired.", 404)
    return job


def claim_job(job_id: UUID) -> Job | None:
    settings = get_settings()
    with session_factory().begin() as session:
        return session.scalar(
            update(Job)
            .where(
                Job.id == job_id,
                Job.status == JobStatus.QUEUED,
                Job.available_at <= func.now(),
                Job.attempts < settings.max_attempts,
            )
            .values(
                status=JobStatus.PROCESSING,
                attempts=Job.attempts + 1,
                attempt_id=uuid4(),
                deadline=func.now()
                + timedelta(
                    seconds=settings.task_timeout_seconds + settings.recovery_grace_seconds
                ),
                error_code=None,
                error_message=None,
            )
            .returning(Job)
        )


def finish_job(
    job: Job,
    *,
    transcript: str | None = None,
    language: str | None = None,
    duration: float | None = None,
    error: AppError | None = None,
) -> bool:
    with session_factory().begin() as session:
        updated_id = session.scalar(
            update(Job)
            .where(
                Job.id == job.id,
                Job.status == JobStatus.PROCESSING,
                Job.attempt_id == job.attempt_id,
            )
            .values(
                status=JobStatus.FAILED if error else JobStatus.SUCCEEDED,
                transcript=transcript,
                language=language,
                duration_seconds=duration,
                error_code=error.code if error else None,
                error_message=error.message if error else None,
                finished_at=func.now(),
                deadline=None,
                dispatch_pending=False,
            )
            .returning(Job.id)
        )
        return updated_id is not None


def retry_job(job: Job) -> None:
    settings = get_settings()
    if job.attempts >= settings.max_attempts:
        finish_job(
            job, error=AppError("processing_failed", "Processing failed after several attempts.")
        )
        return
    with session_factory().begin() as session:
        session.execute(
            update(Job)
            .where(
                Job.id == job.id,
                Job.status == JobStatus.PROCESSING,
                Job.attempt_id == job.attempt_id,
            )
            .values(
                status=JobStatus.QUEUED,
                dispatch_pending=True,
                deadline=None,
                available_at=func.now()
                + timedelta(seconds=settings.retry_backoff_seconds * 2 ** (job.attempts - 1)),
            )
        )
