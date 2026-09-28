"""Real AnyIO process boundary checks: cancellation, reuse, errors, and responsiveness."""

import asyncio
import os
import subprocess
import tempfile
import time
import unittest
import wave
from pathlib import Path
from unittest.mock import patch

import anyio
import ffmpegio
import httpx
from anyio import to_process

from src.exceptions import AppError, StorageFullError
from src.transcription.engine import load_model
from src.transcription.media import decode_audio
from src.transcription.worker import should_retry


def slow_process(marker: Path) -> None:
    marker.write_text(str(os.getpid()))
    time.sleep(60)


def fail_storage() -> None:
    raise StorageFullError()


def worker_pid() -> int:
    return os.getpid()


def slow_decode(source: Path, marker: Path) -> None:
    class TrackedProcess(subprocess.Popen):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            marker.write_text(str(self.pid))

    read = ffmpegio.audio.read
    # Read a local recording at real-time speed so cancellation happens during FFmpeg.
    with (
        patch("subprocess.Popen", TrackedProcess),
        patch("ffmpegio.audio.read", side_effect=lambda *a, **kw: read(*a, re_in=None, **kw)),
    ):
        decode_audio(source)


def fail_model_download(status: int) -> None:
    response = httpx.Response(status, request=httpx.Request("GET", "https://example.test"))
    error = httpx.HTTPStatusError("unavailable", request=response.request, response=response)
    load_model.cache_clear()
    with patch("faster_whisper.WhisperModel", side_effect=error):
        load_model()


class ProcessTests(unittest.IsolatedAsyncioTestCase):
    @unittest.skipIf(os.name == "nt", "full runtime uses Linux containers")
    async def test_cancelling_decoder_also_stops_ffmpeg(self):
        await to_process.run_sync(worker_pid)
        with tempfile.TemporaryDirectory() as directory:
            source, marker = Path(directory) / "long.wav", Path(directory) / "ffmpeg.pid"
            with wave.open(str(source), "wb") as recording:
                recording.setparams((1, 2, 16000, 0, "NONE", "not compressed"))
                recording.writeframes(b"\0\0" * 16000 * 30)
            async with anyio.create_task_group() as tasks:

                async def decode():
                    await to_process.run_sync(slow_decode, source, marker, cancellable=True)

                tasks.start_soon(decode)
                with anyio.fail_after(10):
                    while not marker.exists():
                        await anyio.sleep(0.01)
                pid = int(marker.read_text())
                await anyio.sleep(0.2)
                tasks.cancel_scope.cancel()
            with anyio.fail_after(5):
                while Path(f"/proc/{pid}").exists():
                    await anyio.sleep(0.05)

    async def test_process_reuse_timeout_termination_and_next_job(self):
        limiter = anyio.CapacityLimiter(1)
        # Import this test module in the child before measuring the execution timeout.
        pid = await to_process.run_sync(worker_pid, limiter=limiter)
        self.assertNotEqual(pid, os.getpid())
        self.assertEqual(pid, await to_process.run_sync(os.getpid, limiter=limiter))
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "started"

            async def run():
                with anyio.fail_after(1):
                    await to_process.run_sync(
                        slow_process, marker, cancellable=True, limiter=limiter
                    )

            task = asyncio.create_task(run())
            for _ in range(100):
                if marker.exists():
                    break
                await anyio.sleep(0.01)
            with self.assertRaises(TimeoutError):
                await task
            self.assertTrue(marker.exists(), "the event loop stays responsive during execution")
            if os.name != "nt":
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)
            self.assertNotEqual(pid, await to_process.run_sync(os.getpid, limiter=limiter))

    async def test_application_errors_survive_the_process_boundary(self):
        with self.assertRaises(AppError) as error:
            await to_process.run_sync(fail_storage)
        self.assertEqual(error.exception.code, "storage_full")
        self.assertTrue(should_retry(error.exception))
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "bad.media"
            source.write_bytes(b"not media")
            with self.assertRaises(AppError) as error:
                await to_process.run_sync(decode_audio, source)
            self.assertEqual(error.exception.code, "invalid_media")
            self.assertFalse(should_retry(error.exception))

    async def test_model_http_status_keeps_retry_policy_across_processes(self):
        for status in (401, 429, 503):
            with self.assertRaises(AppError) as error:
                await to_process.run_sync(fail_model_download, status)
            self.assertEqual(error.exception.status_code, status)
            self.assertEqual(should_retry(error.exception), status != 401)


if __name__ == "__main__":
    unittest.main()
