"""Upload files: write .part, durably commit .media, then allow the job to be queued."""

import errno
import os
import shutil
import sys
from collections.abc import AsyncIterable
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import anyio

from src.exceptions import AppError, StorageFullError
from src.settings.config import settings


@contextmanager
def storage_errors():
    """Use the same temporary-storage failure for uploads and worker file operations."""
    try:
        yield
    except OSError as exc:
        if exc.errno in (errno.ENOSPC, errno.EDQUOT):
            raise StorageFullError() from exc
        raise


def media_path(job_id: UUID | str, suffix: str = ".media") -> Path:
    return settings.media_dir / f"{UUID(str(job_id))}{suffix}"


async def prepare_storage() -> None:
    await anyio.Path(settings.media_dir).mkdir(parents=True, exist_ok=True)


async def check_disk_space(reserved_uploads: int = 1) -> None:
    if (await anyio.to_thread.run_sync(shutil.disk_usage, settings.media_dir)).free < (
        settings.min_free_disk_bytes + settings.max_upload_bytes * reserved_uploads
    ):
        raise StorageFullError()


def check_upload_size(size: int, limit: int) -> None:
    if size > limit:
        raise AppError("file_too_large", "The file exceeds the upload limit.", 413)


async def save_upload(job_id: UUID, chunks: AsyncIterable[bytes]) -> int:
    """Save and commit a bounded stream; the caller owns timeout and failure cleanup."""
    limit = settings.max_upload_bytes
    size = 0
    stream = await anyio.open_file(media_path(job_id, ".part"), "xb")
    try:
        async for chunk in chunks:
            size += len(chunk)
            check_upload_size(size, limit)
            await stream.write(chunk)
    finally:
        # Finish closing before the route removes a cancelled upload's partial file.
        with anyio.CancelScope(shield=True):
            await stream.aclose()
    if size == 0:
        raise AppError("empty_file", "Choose a file that contains audio.")
    await anyio.to_thread.run_sync(commit_file, job_id)
    return size


def commit_file(job_id: UUID) -> None:
    # File fsync persists contents; rename is atomic; directory fsync persists the new name.
    part = media_path(job_id, ".part")
    with part.open("r+b") as stream:
        os.fsync(stream.fileno())
    part.replace(media_path(job_id))
    if sys.platform != "win32":
        descriptor = os.open(str(part.parent), os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


async def remove_media(job_id: UUID | str) -> None:
    await anyio.Path(media_path(job_id)).unlink(missing_ok=True)
    await anyio.Path(media_path(job_id, ".part")).unlink(missing_ok=True)


async def remove_orphans(known_ids: set[str]) -> None:
    cutoff = datetime.now(UTC).timestamp() - (
        max(settings.upload_timeout_seconds, settings.task_timeout_seconds)
        + settings.recovery_grace_seconds
    )
    # ponytail: scan bounded single-host storage; use object lifecycle rules at scale.
    async for path in anyio.Path(settings.media_dir).iterdir():
        if path.suffix not in {".part", ".media", ".pcm"}:
            continue
        try:
            job_id = str(UUID(path.name.split(".")[0]))
            if (await path.stat()).st_mtime < cutoff and (
                path.suffix == ".pcm" or job_id not in known_ids
            ):
                await path.unlink(missing_ok=True)
        except (ValueError, FileNotFoundError):
            continue
