import logging
from functools import lru_cache
from pathlib import Path

from faster_whisper import WhisperModel

from src.settings.config import get_settings
from src.storage import storage_errors
from src.transcription.media import SAMPLE_RATE, decode_audio

log = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def load_model() -> WhisperModel:
    settings = get_settings()
    model = WhisperModel(
        settings.model_id,
        revision=settings.model_revision,
        download_root=str(settings.model_cache_dir),
        device=settings.model_device,
        compute_type=settings.model_compute_type,
        cpu_threads=settings.model_cpu_threads,
    )
    log.info("model_loaded")
    return model


def transcribe(source: Path) -> tuple[str, str, float]:
    """Decode bounded audio and return the transcript, language, and duration."""
    settings = get_settings()
    with storage_errors():
        audio = decode_audio(source)
        segments, info = load_model().transcribe(
            audio,
            language=settings.model_language,
            beam_size=settings.model_beam_size,
            vad_filter=settings.model_vad_filter,
        )
        # faster-whisper evaluates the generator lazily: consume it before saving success.
        transcript = " ".join(segment.text.strip() for segment in segments).strip()
        return transcript, info.language, len(audio) / SAMPLE_RATE


if __name__ == "__main__":
    load_model()
