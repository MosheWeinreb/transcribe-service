import shutil
import subprocess
from pathlib import Path

import ffmpegio
import numpy as np
from ffmpegio.errors import FFmpegError

from src.exceptions import AppError
from src.settings.config import settings

# Whisper's audio input contract, not a deployment tuning value.
SAMPLE_RATE = 16000


def decode_audio(source: Path) -> np.ndarray:
    max_samples = settings.max_duration_seconds * SAMPLE_RATE
    try:
        # Decode one extra sample to detect over-limit media without trusting container metadata.
        _, audio = ffmpegio.audio.read(
            str(source),
            ar=SAMPLE_RATE,
            ac=1,
            sample_fmt="flt",
            map="0:a:0",
            t=(max_samples + 1) / SAMPLE_RATE,
            protocol_whitelist_in="file,pipe",
            format_whitelist_in=settings.media_formats,
            xerror=None,
            sp_kwargs={
                "executable": shutil.which(settings.ffmpeg_binary) or settings.ffmpeg_binary,
                "timeout": settings.decode_timeout_seconds,
                "stderr": subprocess.DEVNULL,
            },
        )
    except subprocess.TimeoutExpired as exc:
        raise AppError(
            "decode_timeout", "The media could not be decoded within the time limit."
        ) from exc
    except FFmpegError as exc:
        raise AppError(
            "invalid_media", "This media file is damaged or uses an unsupported format."
        ) from exc
    if len(audio) > max_samples:
        raise AppError("media_too_long", "The recording exceeds the configured duration limit.")
    if len(audio) == 0:
        raise AppError("no_audio", "The recording has no decodable audio.")
    return audio[:, 0]
