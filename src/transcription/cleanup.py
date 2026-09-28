import logging
from datetime import timedelta
from typing import cast

from sqlalchemy import CursorResult, and_, delete, func, or_, select, update

from src import database
from src.schemas import JobStatus
from src.settings.config import settings
from src.storage import remove_media, remove_orphans
from src.transcription.models import Job

log = logging.getLogger(__name__)


async def recover_expired() -> None:
    async with database.sessions.begin() as session:
        await session.execute(
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
                error_code="processing_timeout",
                error_message="Processing could not finish within the allowed attempts.",
            )
        )
        result = await session.execute(
            update(Job)
            .where(Job.status == JobStatus.PROCESSING, Job.deadline < func.now())
            .values(
                status=JobStatus.QUEUED,
                deadline=None,
                available_at=func.now() + timedelta(seconds=settings.retry_backoff_seconds),
            )
        )
        if cast(CursorResult, result).rowcount:
            log.warning("expired_attempts_requeued")
        await session.execute(
            update(Job)
            .where(
                Job.status == JobStatus.QUEUED,
                Job.available_at < func.now() - timedelta(seconds=settings.max_queue_wait_seconds),
            )
            .values(
                status=JobStatus.FAILED,
                finished_at=func.now(),
                error_code="queue_timeout",
                error_message=(
                    "Processing could not start within the queue wait limit. Please submit again."
                ),
            )
        )


async def cleanup() -> None:
    async with database.sessions.begin() as session:
        expired = (
            await session.scalars(
                select(Job.id)
                .where(
                    or_(
                        and_(Job.status == "uploading", Job.deadline < func.now()),
                        Job.finished_at
                        < func.now() - timedelta(seconds=settings.retention_seconds),
                    )
                )
                .with_for_update(skip_locked=True)
                .limit(settings.cleanup_batch_size)
            )
        ).all()
        for expired_id in expired:
            await remove_media(expired_id)
            await session.execute(delete(Job).where(Job.id == expired_id))
            log.info("job_expired", extra={"job_id": str(expired_id)})
        known_ids = {str(job_id) for job_id in (await session.scalars(select(Job.id)))}
    await remove_orphans(known_ids)
