"""Decoder boundary checks using FFmpeg, without a database or model download."""

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.exceptions import AppError
from src.settings.config import settings
from src.transcription.media import decode_audio


class DecoderTests(unittest.TestCase):
    def test_video_without_audio_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "silent.mp4"
            subprocess.run(
                [
                    settings.ffmpeg_binary,
                    "-nostdin",
                    "-v",
                    "error",
                    "-f",
                    "lavfi",
                    "-i",
                    "color=size=16x16:duration=0.1",
                    "-an",
                    "-c:v",
                    "mpeg4",
                    str(source),
                ],
                check=True,
                timeout=10,
            )
            with self.assertRaises(AppError) as error:
                decode_audio(source)
            self.assertEqual(error.exception.code, "invalid_media")

    def test_decoder_timeout_is_reported(self):
        with (
            patch(
                "src.transcription.media.ffmpegio.audio.read",
                side_effect=subprocess.TimeoutExpired("ffmpeg", 1),
            ),
            self.assertRaises(AppError) as error,
        ):
            decode_audio(Path("source.wav"))
        self.assertEqual(error.exception.code, "decode_timeout")
