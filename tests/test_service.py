"""Async API/state checks against a disposable PostgreSQL database (Linux Docker)."""

import asyncio
import errno
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import anyio
import httpx
import psycopg
from psycopg import sql
from sqlalchemy import func, make_url, select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.requests import ClientDisconnect

from src import database
from src.app import create_app
from src.exceptions import AppError
from src.settings.config import Settings, settings
from src.storage import media_path, prepare_storage
from src.transcription import cleanup, service, worker
from src.transcription.models import Job


class ServiceTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.admin_url = os.environ.get("TEST_DATABASE_URL", settings.database_url)
        cls.database_name = "transcriber_test_" + uuid4().hex
        with psycopg.connect(cls.admin_url, autocommit=True) as admin:
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(cls.database_name)))
        cls.test_url = make_url(cls.admin_url).set(
            database=cls.database_name, drivername="postgresql+psycopg"
        )
        cls.addClassCleanup(cls.drop_database)

    @classmethod
    def drop_database(cls):
        with psycopg.connect(cls.admin_url, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(cls.database_name))
            )

    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.enterContext(
            patch.multiple(
                settings,
                media_dir=Path(self.directory.name),
                max_upload_bytes=128,
                max_stored_bytes=4096,
                max_active_jobs=3,
                max_concurrent_uploads=2,
                max_filename_length=12,
                min_free_disk_bytes=0,
                retry_backoff_seconds=1,
                worker_poll_seconds=0.02,
                cleanup_interval_seconds=1,
            )
        )
        engine = create_async_engine(self.test_url, pool_size=5, max_overflow=0)
        self.enterContext(patch.object(database, "engine", engine))
        self.enterContext(
            patch.object(database, "sessions", async_sessionmaker(engine, expire_on_commit=False))
        )
        self.addAsyncCleanup(engine.dispose)
        await prepare_storage()
        await database.initialize_schema()
        async with engine.begin() as connection:
            await connection.execute(text("TRUNCATE jobs"))
        self.client = await self.enterAsyncContext(
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=create_app(), raise_app_exceptions=False),
                base_url="http://test",
            )
        )

    async def accepted_job(self):
        job_id = await service.reserve_upload("test.wav")
        media_path(job_id).write_bytes(b"audio")
        await service.finalize_upload(job_id, 5)
        return job_id

    async def count_jobs(self):
        async with database.sessions.begin() as session:
            return await session.scalar(select(func.count()).select_from(Job))

    async def test_upload_and_public_contract(self):
        response = await self.client.post("/api/jobs?filename=voice.wav", content=b"audio")
        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.headers["location"], response.json()["status_url"])
        job = (await self.client.get(response.json()["status_url"])).json()
        self.assertEqual(job["status"], "queued")
        self.assertEqual(job["filename"], "voice.wav")
        self.assertEqual((await self.client.get("/api/jobs/not-a-uuid")).status_code, 422)
        self.assertEqual((await self.client.get("/api/jobs/" + str(uuid4()))).status_code, 404)
        config = (await self.client.get("/api/config")).json()
        self.assertEqual(config["max_upload_bytes"], 128)
        self.assertNotIn("database_url", config)
        schema = (await self.client.get("/openapi.json")).json()
        self.assertIn(
            "application/octet-stream",
            schema["paths"]["/api/jobs"]["post"]["requestBody"]["content"],
        )

    async def test_reject_invalid_uploads_without_reserving_space(self):
        for params, content, headers, expected in [
            ({}, b"", {}, 400),
            ({}, b"x" * 129, {}, 413),
            ({"filename": "x" * 13}, b"audio", {}, 422),
            ({}, b"audio", {"Content-Type": "multipart/form-data"}, 415),
        ]:
            response = await self.client.post(
                "/api/jobs", params=params, content=content, headers=headers
            )
            self.assertEqual(response.status_code, expected, response.text)
        self.assertEqual(await self.count_jobs(), 0)
        self.assertEqual(list(settings.media_dir.iterdir()), [])

    async def test_actual_stream_bytes_are_limited_without_content_length(self):
        async def chunks():
            yield b"x" * 100
            yield b"x" * 50

        response = await self.client.post("/api/jobs", content=chunks())
        self.assertEqual(response.status_code, 413)
        self.assertEqual(await self.count_jobs(), 0)
        self.assertEqual(list(settings.media_dir.iterdir()), [])

    async def test_interrupted_upload_cleans_partial_and_reservation(self):
        async def chunks():
            yield b"audio"
            raise ClientDisconnect()

        await self.client.post("/api/jobs", content=chunks())
        self.assertEqual(await self.count_jobs(), 0)
        self.assertEqual(list(settings.media_dir.iterdir()), [])

    async def test_upload_timeout_cleans_partial(self):
        async def chunks():
            yield b"audio"
            await anyio.sleep(10)

        with patch.object(settings, "upload_timeout_seconds", 0.05):
            response = await self.client.post("/api/jobs", content=chunks())
        self.assertEqual(response.status_code, 408)
        self.assertEqual(await self.count_jobs(), 0)
        self.assertEqual(list(settings.media_dir.iterdir()), [])

    async def test_disk_full_returns_503_and_cleans_reservation(self):
        with patch("src.storage.commit_file", side_effect=OSError(errno.ENOSPC, "full")):
            response = await self.client.post("/api/jobs", content=b"audio")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["error"]["code"], "storage_full")
        self.assertEqual(await self.count_jobs(), 0)
        self.assertEqual(list(settings.media_dir.iterdir()), [])

    async def test_uncertain_finalize_preserves_committed_file(self):
        original = service.finalize_upload

        async def uncertain(job_id, size):
            await original(job_id, size)
            raise ConnectionError("response lost after commit")

        with patch.object(service, "finalize_upload", side_effect=uncertain):
            response = await self.client.post("/api/jobs", content=b"audio")
        self.assertEqual(response.status_code, 500)
        claimed = await service.claim_job()
        self.assertIsNotNone(claimed)
        self.assertEqual(media_path(claimed.id).read_bytes(), b"audio")

    async def test_admission_is_atomic(self):
        results = await asyncio.gather(
            *(service.reserve_upload("test.wav") for _ in range(8)), return_exceptions=True
        )
        self.assertEqual(sum(not isinstance(r, Exception) for r in results), 2)
        self.assertEqual(await self.count_jobs(), 2)

    async def test_active_job_limit_and_storage_reservations(self):
        for _ in range(3):
            await self.accepted_job()
        with self.assertRaises(AppError) as error:
            await service.reserve_upload("test.wav")
        self.assertEqual(error.exception.code, "capacity_exceeded")
        async with database.engine.begin() as connection:
            await connection.execute(text("TRUNCATE jobs"))
        with patch.object(settings, "max_stored_bytes", 128):
            await service.reserve_upload("test.wav")
            with self.assertRaises(AppError) as error:
                await service.reserve_upload("test.wav")
            self.assertEqual(error.exception.code, "storage_full")

    async def test_parallel_claims_are_unique_and_do_not_wait_for_locked_row(self):
        ids = {await self.accepted_job(), await self.accepted_job()}
        async with database.sessions.begin() as session:
            locked = await session.scalar(select(Job.id).with_for_update().limit(1))
            claimed = await asyncio.wait_for(service.claim_job(), 1)
            self.assertNotEqual(claimed.id, locked)
        results = await asyncio.gather(*(service.claim_job() for _ in range(4)))
        claimed_ids = [j.id for j in [claimed, *results] if j]
        self.assertEqual(set(claimed_ids), ids)
        self.assertEqual(len(claimed_ids), 2)

    async def test_stale_attempt_cannot_finish_or_retry_new_attempt(self):
        job_id = await self.accepted_job()
        first = await service.claim_job()
        await service.retry_job(first)
        self.assertIsNone(await service.claim_job())
        async with database.sessions.begin() as session:
            await session.execute(update(Job).values(available_at=func.now()))
        second = await service.claim_job()
        self.assertNotEqual(first.attempt_id, second.attempt_id)
        self.assertFalse(await service.finish_job(first, transcript="stale"))
        await service.retry_job(first)
        self.assertEqual((await service.get_job(job_id)).status, "processing")
        self.assertTrue(await service.finish_job(second, transcript="correct"))
        self.assertEqual((await service.get_job(job_id)).transcript, "correct")

    async def test_retry_budget_and_backoff(self):
        job_id = await self.accepted_job()
        for attempt in range(1, 4):
            job = await service.claim_job()
            self.assertEqual(job.attempts, attempt)
            await service.retry_job(job)
            result = await service.get_job(job_id)
            if attempt < 3:
                self.assertEqual(result.status, "queued")
                self.assertGreater(result.available_at, result.updated_at)
                self.assertIsNone(await service.claim_job())
                async with database.sessions.begin() as session:
                    await session.execute(update(Job).values(available_at=func.now()))
            else:
                self.assertEqual(result.status, "failed")

    async def test_expired_attempt_recovery_and_queue_deadline(self):
        job_id = await self.accepted_job()
        await service.claim_job()
        async with database.engine.begin() as connection:
            await connection.execute(text("UPDATE jobs SET deadline=now()-interval '1 second'"))
        await cleanup.recover_expired()
        self.assertEqual((await service.get_job(job_id)).status, "queued")
        async with database.engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE jobs SET status='processing', attempts=3, "
                    "deadline=now()-interval '1 second'"
                )
            )
        await cleanup.recover_expired()
        self.assertEqual((await service.get_job(job_id)).error_code, "processing_timeout")
        waiting = await self.accepted_job()
        async with database.sessions.begin() as session:
            await session.execute(
                update(Job)
                .where(Job.id == waiting)
                .values(available_at=text("now()-interval '2 hours'"))
            )
        await cleanup.recover_expired()
        self.assertEqual((await service.get_job(waiting)).error_code, "queue_timeout")

    async def test_cleanup_preserves_current_and_locked_jobs(self):
        old = await self.accepted_job()
        await service.finish_job(await service.claim_job(), transcript="old")
        current = await self.accepted_job()
        async with database.sessions.begin() as session:
            await session.execute(
                update(Job).where(Job.id == old).values(finished_at=text("now()-interval '7 days'"))
            )
        async with database.sessions.begin() as session:
            await session.scalar(select(Job.id).where(Job.id == old).with_for_update())
            await cleanup.cleanup()
            self.assertTrue(media_path(old).exists())
        orphan = media_path(uuid4())
        orphan.write_bytes(b"orphan")
        os.utime(orphan, (0, 0))
        await cleanup.cleanup()
        self.assertFalse(media_path(old).exists())
        self.assertFalse(orphan.exists())
        self.assertTrue(media_path(current).exists())

    async def test_schema_initialization_preserves_jobs(self):
        job_id = await self.accepted_job()
        await database.initialize_schema()
        self.assertEqual((await service.get_job(job_id)).status, "queued")

    async def test_slow_upload_does_not_block_health_and_excess_uploads_rejected(self):
        started = [asyncio.Event(), asyncio.Event()]
        release = asyncio.Event()

        async def chunks(index):
            yield b"a"
            started[index].set()
            await release.wait()
            yield b"b"

        tasks = [
            asyncio.create_task(self.client.post("/api/jobs", content=chunks(i))) for i in range(2)
        ]
        try:
            await asyncio.wait_for(asyncio.gather(*(event.wait() for event in started)), 2)
            health = await asyncio.wait_for(self.client.get("/health/ready"), 1)
            self.assertEqual(health.status_code, 200)
            response = await self.client.post("/api/jobs", content=b"third")
            self.assertEqual(response.status_code, 503)
        finally:
            release.set()
            results = await asyncio.gather(*tasks)
        self.assertEqual([r.status_code for r in results], [202, 202])

    async def test_lifespan_polls_and_stops_owned_tasks(self):
        for _ in range(2):
            job_id = await self.accepted_job()
            called = asyncio.Event()

            async def fake_transcribe(*args, called=called, **kwargs):
                called.set()
                return "hello", "en", 1.0

            app = create_app()
            with patch.object(worker.to_process, "run_sync", side_effect=fake_transcribe):
                async with app.router.lifespan_context(app):
                    await asyncio.wait_for(called.wait(), 2)
                    for _ in range(100):
                        if (await service.get_job(job_id)).status == "succeeded":
                            break
                        await anyio.sleep(0.02)
                    self.assertEqual((await service.get_job(job_id)).transcript, "hello")

    async def test_worker_handles_permanent_and_temporary_errors(self):
        for exc, expected in [
            (AppError("invalid_media", "bad"), "failed"),
            (TimeoutError(), "queued"),
        ]:
            job_id = await self.accepted_job()
            job = await service.claim_job()
            with patch.object(worker.to_process, "run_sync", side_effect=exc):
                await worker.process_job(job, anyio.CapacityLimiter(1))
            self.assertEqual((await service.get_job(job_id)).status, expected)

    async def test_shutdown_requeues_after_execution_is_cancelled(self):
        job_id = await self.accepted_job()
        job = await service.claim_job()
        started = anyio.Event()

        async def wait(*args, **kwargs):
            started.set()
            await anyio.sleep_forever()

        with patch.object(worker.to_process, "run_sync", side_effect=wait):
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(worker.process_job, job, anyio.CapacityLimiter(1))
                await started.wait()
                tasks.cancel_scope.cancel()
        self.assertEqual((await service.get_job(job_id)).status, "queued")

    async def test_database_failure_leaves_claim_for_recovery(self):
        from sqlalchemy.exc import OperationalError

        job_id = await self.accepted_job()
        job = await service.claim_job()
        with (
            patch.object(service, "finish_job", side_effect=OperationalError("", {}, Exception())),
            patch.object(
                worker.to_process, "run_sync", new=AsyncMock(return_value=("ok", "en", 1.0))
            ),
            self.assertRaises(OperationalError),
        ):
            await worker.process_job(job, anyio.CapacityLimiter(1))
        self.assertEqual((await service.get_job(job_id)).status, "processing")

    async def test_settings_and_retry_classification(self):
        from pydantic import ValidationError

        with self.assertRaises(ValidationError):
            Settings(_env_file=None, max_concurrent_uploads=4, max_active_jobs=1)
        self.assertTrue(worker.should_retry(AppError("storage_full", "full")))
        self.assertFalse(worker.should_retry(AppError("decode_timeout", "slow")))


if __name__ == "__main__":
    unittest.main()
