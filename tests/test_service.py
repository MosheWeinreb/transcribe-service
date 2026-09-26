"""Behavior checks using disposable data. Requires Compose PostgreSQL and RabbitMQ."""

import asyncio
import errno
import io
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import wave
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import httpx
import psycopg
from amqp.exceptions import MessageNacked
from celery import Celery
from celery.exceptions import Reject
from fastapi.testclient import TestClient
from kombu import Queue, pools
from kombu.exceptions import OperationalError as BrokerError
from psycopg import sql
from pydantic import ValidationError
from sqlalchemy import make_url
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeout

from src.app import create_app
from src.database import close_database, get_engine, initialize_schema
from src.exceptions import AppError
from src.schemas import TranscriptionMessage
from src.settings.config import Settings, get_settings
from src.storage import media_path, prepare_storage
from src.transcription import service
from src.transcription.media import SAMPLE_RATE, decode_audio


def wav_bytes(seconds=1):
    output = io.BytesIO()
    with wave.open(output, "wb") as audio:
        audio.setparams((1, 2, SAMPLE_RATE, 0, "NONE", "not compressed"))
        audio.writeframes(b"\x00\x00" * int(seconds * SAMPLE_RATE))
    return output.getvalue()


class ServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.admin_url = os.environ.get("TEST_DATABASE_URL", get_settings().database_url)
        cls.database_name = "transcriber_test_" + uuid4().hex
        with psycopg.connect(cls.admin_url, autocommit=True, connect_timeout=5) as admin:
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(cls.database_name)))
        cls.storage = tempfile.TemporaryDirectory()
        cls.environment = patch.dict(
            os.environ,
            {
                "DATABASE_URL": make_url(cls.admin_url)
                .set(database=cls.database_name)
                .render_as_string(hide_password=False),
                "MEDIA_DIR": cls.storage.name,
                "MAX_UPLOAD_BYTES": "128",
                "MAX_FILENAME_LENGTH": "12",
                "MAX_STORED_BYTES": "4096",
                "MAX_ACTIVE_JOBS": "3",
                "MAX_CONCURRENT_UPLOADS": "2",
                "MAX_DURATION_SECONDS": "2",
                "MIN_FREE_DISK_BYTES": "0",
                "RETRY_BACKOFF_SECONDS": "1",
                "LOG_LEVEL": "ERROR",
            },
        )
        cls.environment.start()
        cls.addClassCleanup(cls.cleanup_resources)
        close_database()
        get_settings.cache_clear()
        prepare_storage()
        initialize_schema()

    @classmethod
    def cleanup_resources(cls):
        pools.reset()
        close_database()
        cls.environment.stop()
        get_settings.cache_clear()
        with psycopg.connect(cls.admin_url, autocommit=True, connect_timeout=5) as admin:
            admin.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(cls.database_name))
            )
        cls.storage.cleanup()

    def setUp(self):
        with get_engine().begin() as connection:
            connection.exec_driver_sql("TRUNCATE jobs")
        for path in Path(self.storage.name).iterdir():
            path.unlink()

    def accepted_job(self):
        job_id = service.reserve_upload("test.wav")
        media_path(job_id).write_bytes(b"test")
        service.finalize_upload(job_id, 4)
        return job_id

    def test_upload_errors_and_public_contract(self):
        with TestClient(create_app()) as client:
            response = client.post("/api/jobs?filename=voice.wav", content=b"audio")
            self.assertEqual(response.status_code, 202)
            self.assertEqual(response.headers["Location"], response.json()["status_url"])
            job = client.get(response.json()["status_url"]).json()
            self.assertEqual(job["status"], "queued")
            self.assertNotIn("attempt_id", job)
            self.assertEqual(media_path(response.json()["id"]).read_bytes(), b"audio")
            self.assertEqual(client.get("/health/ready").status_code, 200)
            self.assertNotIn("database_url", client.get("/api/config").json())
            self.assertEqual(client.get("/api/config").json()["max_filename_length"], 12)
            self.assertEqual(
                client.post(
                    "/api/jobs", params={"filename": "x" * 13}, content=b"audio"
                ).status_code,
                422,
            )
            self.assertEqual(client.get("/api/jobs/not-a-uuid").status_code, 422)
            self.assertEqual(client.get(f"/api/jobs/{uuid4()}").status_code, 404)
            self.assertEqual(client.post("/api/jobs", content=b"").status_code, 400)
            self.assertEqual(client.post("/api/jobs", content=b"a" * 129).status_code, 413)
            for invalid_length in ("-1", "invalid"):
                response = client.post(
                    "/api/jobs", content=b"audio", headers={"Content-Length": invalid_length}
                )
                self.assertEqual(response.status_code, 422)
                self.assertEqual(response.json()["error"]["code"], "invalid_request")

            # No Content-Length: actual streamed bytes must still be limited.
            def chunks():
                yield b"a" * 80
                yield b"b" * 80

            self.assertEqual(client.post("/api/jobs", content=chunks()).status_code, 413)
            self.assertEqual(list(Path(self.storage.name).glob("*.part")), [])

    def test_failed_job_without_message_preserves_error_response(self):
        job_id = self.accepted_job()
        with get_engine().begin() as connection:
            connection.exec_driver_sql(
                "UPDATE jobs SET status='failed', error_code='processing_failed' WHERE id=%s",
                (job_id,),
            )
        with TestClient(create_app()) as client:
            response = client.get(f"/api/jobs/{job_id}")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["error"]["code"], "processing_failed")
        self.assertEqual(
            response.json()["error"]["message"], "Processing failed. Please try again."
        )

    def test_admission_is_atomic_under_concurrency(self):
        def reserve(_):
            try:
                return service.reserve_upload("parallel.wav")
            except AppError:
                return None

        with ThreadPoolExecutor(max_workers=8) as threads:
            reservations = list(threads.map(reserve, range(8)))
        self.assertEqual(sum(item is not None for item in reservations), 2)

    def test_database_failures_keep_the_http_error_contract(self):
        with TestClient(create_app()) as client:
            for failure in (OperationalError(None, None, Exception("offline")), PoolTimeout()):
                with patch("src.api.health.check_database", side_effect=failure):
                    response = client.get("/health/ready")
                self.assertEqual(response.status_code, 503)
                self.assertEqual(response.json()["error"]["code"], "service_unavailable")

    def test_http_errors_preserve_framework_headers(self):
        with TestClient(create_app()) as client:
            response = client.post("/health/live")
        self.assertEqual(response.status_code, 405)
        self.assertEqual(response.headers["Allow"], "GET")
        self.assertEqual(response.json()["error"]["code"], "http_error")

    def test_unexpected_error_preserves_error_response(self):
        with (
            TestClient(create_app(), raise_server_exceptions=False) as client,
            patch("src.api.health.check_database", side_effect=RuntimeError("broken")),
            self.assertNoLogs("src.exceptions", level="ERROR"),
        ):
            response = client.get("/health/ready")
        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.json()["error"]["code"], "internal_error")

    def test_uvicorn_logs_unexpected_error_once(self):
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                """
import asyncio
import socket
import httpx
import uvicorn
from fastapi import FastAPI
from src.exceptions import install_error_handlers
from src.settings.logger import configure_logging

configure_logging("INFO")
app = FastAPI()
install_error_handlers(app)

@app.get("/broken")
def broken():
    raise RuntimeError("one failure")

async def check():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        server = uvicorn.Server(uvicorn.Config(app, log_config=None, lifespan="off"))
        task = asyncio.create_task(server.serve(sockets=[sock]))
        try:
            async with asyncio.timeout(10):
                while not server.started:
                    await asyncio.sleep(0.01)
                async with httpx.AsyncClient() as client:
                    response = await client.get(f"http://127.0.0.1:{sock.getsockname()[1]}/broken")
                assert response.status_code == 500
                assert response.json()["error"]["code"] == "internal_error"
        finally:
            server.should_exit = True
            await task

asyncio.run(check())
""",
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=15,
        )
        records = [json.loads(line) for line in result.stdout.splitlines()]
        errors = [record for record in records if record.get("exception")]
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["logger"], "uvicorn.error")
        self.assertIn("RuntimeError: one failure", errors[0]["exception"])

    def test_uvicorn_access_logs_are_json_without_duplicate_handlers(self):
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                """
import logging
from uuid import UUID
from src.settings.logger import configure_logging

configure_logging("INFO")
configure_logging("INFO")
logging.getLogger("uvicorn.access").info(
    '%s - "%s %s HTTP/%s" %d', "127.0.0.1:1234", "GET", "/health/live", "1.1", 200
)
configure_logging("ERROR")
logging.getLogger("uvicorn.access").info("should_be_filtered")
try:
    raise ValueError("broken")
except ValueError:
    logging.getLogger("transcription").exception(
        "processing_failed", extra={"job_id": UUID(int=1), "stage": "decode"}
    )
""",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        records = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual(len(records), 2)
        self.assertEqual(records[1]["event"], "processing_failed")
        self.assertEqual(records[1]["stage"], "decode")
        self.assertEqual(records[1]["job_id"], "00000000-0000-0000-0000-000000000001")
        self.assertIn("ValueError: broken", records[1]["exception"])
        self.assertEqual(records[0]["logger"], "uvicorn.access")
        self.assertIn('"GET /health/live HTTP/1.1" 200', records[0]["event"])

    def test_disk_exhaustion_during_upload_cleans_up_and_returns_503(self):
        with TestClient(create_app()) as client:
            for target in ("anyio.AsyncFile.write", "src.storage.os.fsync"):
                for code in (errno.ENOSPC, errno.EDQUOT):
                    with (
                        self.subTest(target=target, errno=code),
                        patch(target, side_effect=OSError(code, "storage exhausted")),
                    ):
                        response = client.post("/api/jobs", content=b"audio")
                    self.assertEqual(response.status_code, 503)
                    self.assertEqual(response.json()["error"]["code"], "storage_full")
                    self.assertEqual(list(Path(self.storage.name).iterdir()), [])
                    with get_engine().connect() as connection:
                        self.assertEqual(
                            connection.exec_driver_sql("SELECT count(*) FROM jobs").scalar(), 0
                        )

    def test_schema_initialization_preserves_existing_jobs(self):
        job_id = self.accepted_job()
        initialize_schema()
        self.assertEqual(service.get_job(job_id).status, "queued")

    def test_interrupted_upload_cleans_up_file_and_reservation(self):
        async def check(cancel):
            started = asyncio.Event()
            timeout = asyncio.timeout

            async def body():
                yield b"partial audio"
                started.set()
                await asyncio.Event().wait()

            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=create_app()), base_url="http://test"
            ) as client:
                with patch(
                    "src.api.transcriptions.asyncio.timeout",
                    side_effect=lambda _: timeout(5 if cancel else 0.1),
                ):
                    upload = asyncio.create_task(client.post("/api/jobs", content=body()))
                    if cancel:
                        await asyncio.wait_for(started.wait(), 5)
                        upload.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await upload
                    else:
                        response = await asyncio.wait_for(upload, 5)
                        self.assertEqual(response.status_code, 408)
                        self.assertEqual(response.json()["error"]["code"], "upload_timeout")

        for cancel in (False, True):
            with self.subTest(cancel=cancel):
                asyncio.run(check(cancel))
                self.assertEqual(list(Path(self.storage.name).iterdir()), [])
                with get_engine().connect() as connection:
                    self.assertEqual(
                        connection.exec_driver_sql("SELECT count(*) FROM jobs").scalar(), 0
                    )

    def test_uncertain_upload_commit_preserves_accepted_file(self):
        finalize_upload = service.finalize_upload

        def commit_then_lose_connection(job_id, size):
            finalize_upload(job_id, size)
            raise OperationalError(None, None, Exception("commit reply lost"))

        with (
            TestClient(create_app()) as client,
            patch(
                "src.transcription.service.finalize_upload", side_effect=commit_then_lose_connection
            ),
        ):
            response = client.post("/api/jobs", content=b"audio")
        self.assertEqual(response.status_code, 503)
        with get_engine().connect() as connection:
            job_id, status = connection.exec_driver_sql("SELECT id, status FROM jobs").one()
        self.assertEqual(status, "queued")
        self.assertEqual(media_path(job_id).read_bytes(), b"audio")
        self.assertFalse(media_path(job_id, ".part").exists())

    def test_duplicate_delivery_and_stale_attempt_cannot_overwrite(self):
        job_id = self.accepted_job()
        first = service.claim_job(job_id)
        self.assertIsNone(service.claim_job(job_id))
        service.retry_job(first)
        with get_engine().begin() as connection:
            connection.exec_driver_sql("UPDATE jobs SET available_at=now() WHERE id=%s", (job_id,))
        second = service.claim_job(job_id)
        self.assertFalse(service.finish_job(first, transcript="stale"))
        self.assertTrue(service.finish_job(second, transcript="correct", language="en", duration=1))
        self.assertIsNone(service.claim_job(job_id))
        self.assertEqual(service.get_job(job_id).transcript, "correct")

    def test_worker_success_and_permanent_media_failure(self):
        from src.transcription.tasks import transcribe_job

        job_id = self.accepted_job()
        with patch(
            "src.transcription.engine.transcribe", return_value=("Hello", "en", 1.0)
        ) as inference:
            transcribe_job.run(str(job_id))
        inference.assert_called_once_with(media_path(job_id))
        self.assertEqual(service.get_job(job_id).transcript, "Hello")
        job_id = self.accepted_job()
        with patch(
            "src.transcription.engine.transcribe", side_effect=AppError("invalid_media", "Bad file")
        ):
            transcribe_job.run(str(job_id))
        job = service.get_job(job_id)
        self.assertEqual((job.status, job.attempts), ("failed", 1))

    def test_queue_message_is_validated_before_claiming(self):
        from src.transcription.tasks import transcribe_job

        message = TranscriptionMessage(job_id=uuid4()).model_dump(mode="json")
        with patch("src.transcription.tasks.claim_job", return_value=None) as claim:
            transcribe_job.run(**message)
            self.assertEqual(str(claim.call_args.args[0]), message["job_id"])
            claim.reset_mock()
            for invalid in ("not-a-uuid", None, 123, {"unexpected": "value"}):
                with self.subTest(invalid=invalid), self.assertRaises(Reject) as rejected:
                    transcribe_job.run(job_id=invalid)
                self.assertFalse(rejected.exception.requeue)
            claim.assert_not_called()
            for invalid in ({}, {**message, "unexpected": "value"}):
                with self.subTest(payload=invalid), self.assertRaises(TypeError):
                    transcribe_job.run(**invalid)
            claim.assert_not_called()

    def test_celery_logs_rejected_message_once(self):
        from src.transcription.tasks import transcribe_job

        with self.assertLogs(level="WARNING") as logs:
            transcribe_job.apply(kwargs={"job_id": "invalid"}, throw=False)
        self.assertEqual(len(logs.records), 1)
        self.assertEqual(logs.records[0].name, "celery.app.trace")

    def test_openapi_documents_service_response_contracts(self):
        with TestClient(create_app()) as client:
            paths = client.get("/openapi.json").json()["paths"]
            for path, method, status, model in (
                ("/api/jobs", "post", "202", "JobAccepted"),
                ("/api/jobs/{job_id}", "get", "200", "JobResult"),
                ("/api/config", "get", "200", "PublicConfig"),
                ("/health/live", "get", "200", "LivenessResponse"),
                ("/health/ready", "get", "200", "ReadinessResponse"),
            ):
                responses = paths[path][method]["responses"]
                for code, expected in ((status, model), ("default", "ErrorResponse")):
                    schema = responses[code]["content"]["application/json"]["schema"]
                    self.assertEqual(schema["$ref"], f"#/components/schemas/{expected}")
            response = client.get("/api/jobs/not-a-uuid")
            self.assertEqual(response.status_code, 422)
            self.assertEqual(response.json()["error"]["code"], "invalid_request")

    def test_worker_does_not_log_success_for_a_stale_result(self):
        from src.transcription.tasks import transcribe_job

        job_id = self.accepted_job()

        def expire_during_inference(*_):
            service.retry_job(service.get_job(job_id))
            return "stale", "en", 1.0

        with (
            patch("src.transcription.engine.transcribe", side_effect=expire_during_inference),
            self.assertLogs("src.transcription.tasks", level="INFO") as logs,
        ):
            transcribe_job.run(str(job_id))
        job = service.get_job(job_id)
        self.assertEqual(job.status, "queued")
        self.assertIsNone(job.transcript)
        events = [record.getMessage() for record in logs.records]
        self.assertIn("processing_result_ignored", events)
        self.assertNotIn("processing_succeeded", events)

    def test_settings_validate_environment_limits(self):
        with patch.dict(
            os.environ,
            {"WORKER_CONCURRENCY": "2", "MIN_FREE_DISK_BYTES": "0", "RECORDING_MARGIN_MS": "0"},
        ):
            settings = Settings()
        self.assertEqual(settings.worker_concurrency, 2)
        self.assertEqual(settings.min_free_disk_bytes, 0)
        self.assertEqual(settings.recording_margin_ms, 0)
        invalid_overrides = (
            {"WORKER_CONCURRENCY": "0"},
            {"MAX_UPLOAD_BYTES": "-1"},
            {"MIN_FREE_DISK_BYTES": "-1"},
            {"SERVER_PORT": "65536"},
            {"MAX_ATTEMPTS": "11"},
            {"POLL_INTERVAL_MS": "499"},
            {"MAX_UPLOAD_BYTES": "128", "MAX_STORED_BYTES": "127"},
            {"MAX_ACTIVE_JOBS": "1", "MAX_CONCURRENT_UPLOADS": "2"},
            {"DECODE_TIMEOUT_SECONDS": "60", "TASK_TIMEOUT_SECONDS": "60"},
            {"REDISPATCH_SECONDS": "5", "BROKER_TIMEOUT_SECONDS": "5"},
            {"MAX_DURATION_SECONDS": "2", "RECORDING_MARGIN_MS": "2000"},
        )
        for overrides in invalid_overrides:
            with (
                self.subTest(overrides=overrides),
                patch.dict(os.environ, overrides),
                self.assertRaises(ValidationError),
            ):
                Settings()

    def test_worker_retry_policy_uses_configured_http_statuses(self):
        from src.transcription.tasks import transcribe_job

        with (
            patch.dict(os.environ, {"RETRY_HTTP_STATUSES": "[200]"}),
            self.assertRaises(ValidationError),
        ):
            Settings()
        defaults = "[429,500,502,503,504]"
        cases = (
            (400, defaults, False),
            (401, defaults, False),
            (404, defaults, False),
            (501, defaults, False),
            (503, "[429]", False),
            (503, defaults, True),
            (429, defaults, True),
            (408, "[408]", True),
        )
        for status, configured, retry in cases:
            with patch.dict(os.environ, {"RETRY_HTTP_STATUSES": configured}):
                settings = Settings()
            request = httpx.Request("GET", "https://model.example/file")
            failure = httpx.HTTPStatusError(
                "download failed", request=request, response=httpx.Response(status, request=request)
            )
            job_id = self.accepted_job()
            available_before = service.get_job(job_id).available_at
            with (
                self.subTest(status=status, configured=configured),
                patch("src.transcription.tasks.get_settings", return_value=settings),
                patch("src.transcription.engine.transcribe", side_effect=failure) as inference,
                self.assertLogs("src.transcription.tasks", level="WARNING") as logs,
            ):
                transcribe_job.run(str(job_id))
            inference.assert_called_once()
            job = service.get_job(job_id)
            self.assertEqual(job.attempts, 1)
            self.assertEqual(job.status, "queued" if retry else "failed")
            self.assertEqual(job.dispatch_pending, retry)
            if retry:
                self.assertGreater(job.available_at, available_before)
            else:
                self.assertEqual(job.error_code, "model_unavailable")
                self.assertEqual(logs.records[0].getMessage(), "processing_failed")
                self.assertEqual(logs.records[0].error_code, "model_unavailable")

    def test_worker_database_failure_keeps_lease_for_recovery(self):
        from src.transcription.tasks import transcribe_job

        job_id = self.accepted_job()
        with (
            patch(
                "src.transcription.engine.transcribe",
                side_effect=OperationalError(None, None, Exception("offline")),
            ),
            self.assertLogs(level="ERROR") as logs,
        ):
            result = transcribe_job.apply(args=[str(job_id)], throw=False)
        self.assertIsInstance(result.result, OperationalError)
        self.assertEqual(len(logs.records), 1)
        self.assertEqual(logs.records[0].name, "celery.app.trace")
        job = service.get_job(job_id)
        self.assertEqual((job.status, job.attempts), ("processing", 1))
        self.assertIsNotNone(job.deadline)

    def test_worker_filesystem_exhaustion_uses_bounded_retries(self):
        from src.transcription.tasks import transcribe_job

        job_id = self.accepted_job()
        for attempt in range(1, get_settings().max_attempts + 1):
            with get_engine().begin() as connection:
                connection.exec_driver_sql(
                    "UPDATE jobs SET available_at=now() WHERE id=%s", (job_id,)
                )
            with patch(
                "src.transcription.engine.decode_audio", side_effect=OSError(errno.ENOSPC, "full")
            ):
                transcribe_job.run(str(job_id))
            self.assertEqual(service.get_job(job_id).attempts, attempt)
        job = service.get_job(job_id)
        self.assertEqual((job.status, job.error_code), ("failed", "processing_failed"))

    def test_expired_worker_attempt_is_recovered_and_bounded(self):
        from src.transcription.maintenance import recover_expired

        job_id = self.accepted_job()
        service.claim_job(job_id)
        with get_engine().begin() as connection:
            connection.exec_driver_sql(
                "UPDATE jobs SET deadline=now()-interval '1 second' WHERE id=%s", (job_id,)
            )
        recover_expired()
        self.assertEqual(service.get_job(job_id).status, "queued")
        with get_engine().begin() as connection:
            connection.exec_driver_sql(
                """UPDATE jobs SET status='processing', attempts=%s,
                deadline=now()-interval '1 second' WHERE id=%s""",
                (get_settings().max_attempts, job_id),
            )
        recover_expired()
        self.assertEqual(service.get_job(job_id).error_code, "processing_timeout")

    def test_failed_publish_remains_pending_then_recovers(self):
        from src.transcription.maintenance import dispatch_pending

        job_id = self.accepted_job()
        with (
            patch(
                "src.transcription.maintenance.celery_app.send_task",
                side_effect=ConnectionError,
            ),
            self.assertRaises(ConnectionError),
        ):
            dispatch_pending()
        self.assertTrue(service.get_job(job_id).dispatch_pending)
        with patch("src.transcription.maintenance.celery_app.send_task") as send:
            dispatch_pending()
        self.assertEqual(send.call_count, 1)
        self.assertFalse(send.call_args.kwargs["retry"])
        self.assertFalse(service.get_job(job_id).dispatch_pending)

    def test_refused_broker_connection_leaves_work_pending(self):
        from src.settings.celery_app import celery_app
        from src.transcription.maintenance import dispatch_pending

        job_id = self.accepted_job()
        with socket.socket() as unavailable:
            unavailable.bind(("127.0.0.1", 0))
            port = unavailable.getsockname()[1]
            app = Celery("offline-test", broker=f"amqp://guest:guest@127.0.0.1:{port}//")
            app.conf.update(
                broker_connection_timeout=1,
                broker_transport_options=celery_app.conf.broker_transport_options,
            )
            try:
                started = time.monotonic()
                with (
                    patch("src.transcription.maintenance.celery_app", app),
                    self.assertRaises(BrokerError),
                ):
                    dispatch_pending()
                self.assertLess(time.monotonic() - started, 3)
            finally:
                app.close()
        self.assertTrue(service.get_job(job_id).dispatch_pending)

    def test_job_updates_refresh_timestamp(self):
        job_id = self.accepted_job()
        before = service.get_job(job_id).updated_at
        claimed = service.claim_job(job_id)
        self.assertGreater(claimed.updated_at, before)
        service.finish_job(claimed, transcript="done")
        self.assertGreater(service.get_job(job_id).updated_at, claimed.updated_at)

    def test_queue_timeout_measures_each_wait_including_retries(self):
        from src.transcription.maintenance import recover_expired

        waiting, retrying = self.accepted_job(), self.accepted_job()
        service.retry_job(service.claim_job(retrying))
        with get_engine().begin() as connection:
            connection.exec_driver_sql("UPDATE jobs SET created_at=now()-interval '2 hours'")
            connection.exec_driver_sql(
                "UPDATE jobs SET available_at=now()-interval '2 hours' WHERE id=%s", (waiting,)
            )
        recover_expired()
        self.assertEqual(service.get_job(waiting).error_code, "queue_timeout")
        self.assertEqual(service.get_job(retrying).status, "queued")
        with get_engine().begin() as connection:
            connection.exec_driver_sql(
                "UPDATE jobs SET available_at=now()-interval '2 hours' WHERE id=%s", (retrying,)
            )
        recover_expired()
        self.assertEqual(service.get_job(retrying).error_code, "queue_timeout")

    def test_new_dispatch_precedes_recovery_delivery(self):
        from src.transcription.maintenance import dispatch_pending

        previous, new = self.accepted_job(), self.accepted_job()
        with get_engine().begin() as connection:
            connection.exec_driver_sql(
                """UPDATE jobs SET dispatch_pending=false, published_at=now()-interval '1 day',
                available_at=now()-interval '1 day' WHERE id=%s""",
                (previous,),
            )
        settings = get_settings().model_copy(update={"maintenance_batch_size": 1})
        confirmed_at = []

        def publish(*args, **kwargs):
            with get_engine().connect() as connection:
                confirmed_at.append(connection.exec_driver_sql("SELECT clock_timestamp()").scalar())

        with (
            patch("src.transcription.maintenance.get_settings", return_value=settings),
            patch(
                "src.transcription.maintenance.celery_app.send_task", side_effect=publish
            ) as send,
        ):
            dispatch_pending()
        self.assertEqual(send.call_args.kwargs["kwargs"], {"job_id": str(new)})
        self.assertEqual(send.call_args.kwargs["expires"], settings.redispatch_seconds)
        self.assertGreaterEqual(service.get_job(new).published_at, confirmed_at[0])

    def test_waiting_backlog_does_not_accumulate_republished_messages(self):
        from src.transcription.maintenance import dispatch_pending
        from src.transcription.tasks import transcribe_job

        jobs = {str(self.accepted_job()), str(self.accepted_job())}
        settings = get_settings().model_copy(update={"redispatch_seconds": 1})
        queue = Queue(
            "transcriber_test_" + uuid4().hex,
            queue_arguments={"x-max-length": 2, "x-overflow": "reject-publish"},
        )
        app = Celery("backlog-test", broker=settings.broker_url)
        app.conf.update(
            task_default_queue=queue.name,
            task_queues=[queue],
            broker_transport_options={"confirm_publish": True},
        )
        try:
            with app.connection_for_write() as connection:
                bound_queue = queue(connection)
                bound_queue.declare()
                try:
                    with (
                        patch("src.transcription.maintenance.get_settings", return_value=settings),
                        patch("src.transcription.maintenance.celery_app", app),
                    ):
                        # No consumer: the queue must stay bounded across recovery cycles.
                        for cycle in range(3):
                            if cycle:
                                time.sleep(settings.redispatch_seconds + 0.2)
                            dispatch_pending()
                            self.assertEqual(
                                bound_queue.queue_declare(passive=True).message_count, 2
                            )
                        excess = self.accepted_job()
                        with self.assertRaises(BrokerError) as rejected:
                            dispatch_pending()
                        self.assertIsInstance(rejected.exception.__cause__, MessageNacked)
                        self.assertTrue(service.get_job(excess).dispatch_pending)
                    delivered = set()
                    for _ in jobs:
                        message = bound_queue.get()
                        self.assertIsNotNone(message)
                        self.assertEqual(message.properties["expiration"], "1000")
                        self.assertEqual(message.payload[0], [])
                        payload = message.payload[1]
                        delivered.add(payload["job_id"])
                        with patch(
                            "src.transcription.engine.transcribe",
                            return_value=("From the queue", "en", 1.0),
                        ):
                            transcribe_job.apply(kwargs=payload, throw=True)
                        self.assertEqual(
                            service.get_job(payload["job_id"]).transcript, "From the queue"
                        )
                        message.ack()
                    self.assertEqual(delivered, jobs)
                finally:
                    bound_queue.delete()
        finally:
            app.close()

    def test_cleanup_removes_only_expired_media_and_records(self):
        from src.transcription.maintenance import cleanup

        expired = self.accepted_job()
        service.finish_job(service.claim_job(expired), transcript="old")
        current = self.accepted_job()
        with get_engine().begin() as connection:
            connection.exec_driver_sql(
                "UPDATE jobs SET finished_at=now()-interval '7 days' WHERE id=%s", (expired,)
            )
        with get_engine().begin() as connection:
            connection.exec_driver_sql("SELECT id FROM jobs WHERE id=%s FOR UPDATE", (expired,))
            cleanup()  # A separate session must skip the locked row, not delete or wait for it.
            self.assertTrue(media_path(expired).exists())
        cleanup()
        self.assertFalse(media_path(expired).exists())
        self.assertTrue(media_path(current).exists())
        with self.assertRaises(AppError):
            service.get_job(expired)

    def test_decoder_enforces_actual_duration_and_rejects_corruption(self):
        source = Path(self.storage.name) / "test.wav"
        source.write_bytes(wav_bytes(1))
        self.assertEqual(len(decode_audio(source)), SAMPLE_RATE)
        source.write_bytes(wav_bytes(get_settings().max_duration_seconds))
        self.assertEqual(
            len(decode_audio(source)), get_settings().max_duration_seconds * SAMPLE_RATE
        )
        self.assertEqual(list(Path(self.storage.name).iterdir()), [source])
        source.write_bytes(wav_bytes(3))
        with self.assertRaises(AppError) as error:
            decode_audio(source)
        self.assertEqual(error.exception.code, "media_too_long")
        source.write_bytes(b"not media")
        with self.assertRaises(AppError) as error:
            decode_audio(source)
        self.assertEqual(error.exception.code, "invalid_media")

    def test_slow_upload_does_not_block_health(self):
        async def check():
            started, release = asyncio.Event(), asyncio.Event()

            async def body():
                yield b"a"
                started.set()
                await release.wait()
                yield b"b"

            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=create_app()), base_url="http://test"
            ) as client:
                upload = asyncio.create_task(client.post("/api/jobs", content=body()))
                await asyncio.wait_for(started.wait(), 5)
                try:
                    health = await asyncio.wait_for(client.get("/health/live"), 2)
                    self.assertEqual(health.status_code, 200)
                finally:
                    release.set()
                self.assertEqual((await upload).status_code, 202)

        asyncio.run(check())

    def test_simultaneous_streams_obey_upload_limit(self):
        async def check():
            started = [asyncio.Event(), asyncio.Event()]
            release = asyncio.Event()

            async def body(index):
                yield b"a"
                started[index].set()
                await release.wait()
                yield b"b"

            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=create_app()), base_url="http://test"
            ) as client:
                uploads = [
                    asyncio.create_task(client.post("/api/jobs", content=body(index)))
                    for index in range(2)
                ]
                try:
                    await asyncio.wait_for(asyncio.gather(*(event.wait() for event in started)), 5)
                    excess = await client.post("/api/jobs", content=b"third upload")
                    self.assertEqual(excess.status_code, 503)
                    self.assertEqual(excess.json()["error"]["code"], "uploads_busy")
                finally:
                    release.set()
                    results = await asyncio.gather(*uploads)
                self.assertEqual([response.status_code for response in results], [202, 202])

        asyncio.run(check())


if __name__ == "__main__":
    unittest.main()
