from functools import lru_cache
from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import Field, NonNegativeInt, PositiveInt, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", hide_input_in_errors=True)

    server_host: str = "127.0.0.1"
    server_port: int = Field(8000, ge=1, le=65535)

    database_url: str = "postgresql://transcriber:local-development@localhost:5432/transcriber"
    broker_url: str = "amqp://transcriber:local-development@localhost:5672//"
    db_pool_size: PositiveInt = 5
    db_timeout_seconds: PositiveInt = 10

    media_dir: Path = Path("data")
    model_cache_dir: Path = Path("model-cache")
    model_id: str = "Systran/faster-whisper-small"
    model_revision: str = "536b0662742c02347bc0e980a01041f333bce120"
    model_device: Literal["cpu", "cuda", "auto"] = "cpu"
    model_compute_type: str = "int8"
    model_cpu_threads: PositiveInt = 4
    model_beam_size: PositiveInt = 5
    model_language: str | None = None
    model_vad_filter: bool = True

    max_upload_bytes: PositiveInt = 100 * 1024 * 1024
    max_filename_length: PositiveInt = 180
    max_duration_seconds: PositiveInt = 600
    max_active_jobs: PositiveInt = 20
    max_concurrent_uploads: PositiveInt = 4
    max_stored_bytes: PositiveInt = 2 * 1024**3
    min_free_disk_bytes: NonNegativeInt = 512 * 1024**2
    upload_timeout_seconds: PositiveInt = 180
    decode_timeout_seconds: PositiveInt = 60
    task_timeout_seconds: PositiveInt = 900
    recovery_grace_seconds: PositiveInt = 30
    max_queue_wait_seconds: PositiveInt = 3600
    redispatch_seconds: PositiveInt = 300
    max_attempts: int = Field(3, ge=1, le=10)
    retry_backoff_seconds: PositiveInt = 10
    retry_http_statuses: tuple[Annotated[int, Field(ge=400, le=599)], ...] = (
        429,
        500,
        502,
        503,
        504,
    )
    retention_seconds: PositiveInt = 86400
    maintenance_interval_seconds: PositiveInt = 5
    maintenance_batch_size: PositiveInt = 20

    worker_concurrency: PositiveInt = 1
    worker_prefetch: PositiveInt = 1
    queue_name: str = "transcriptions"
    queue_max_length: PositiveInt = 100
    broker_timeout_seconds: PositiveInt = 5
    broker_heartbeat_seconds: PositiveInt = 30

    poll_interval_ms: int = Field(2000, ge=500)
    recording_margin_ms: NonNegativeInt = 500
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    ffmpeg_binary: str = "ffmpeg"
    media_formats: str = "wav,mp3,mov,matroska,webm,ogg,flac,aac"

    @model_validator(mode="after")
    def consistent_limits(self) -> Self:
        if self.recording_margin_ms >= self.max_duration_seconds * 1000:
            raise ValueError("RECORDING_MARGIN_MS must be smaller than the media duration limit")
        if self.max_stored_bytes < self.max_upload_bytes:
            raise ValueError("MAX_STORED_BYTES must cover at least one maximum-size upload")
        if self.decode_timeout_seconds >= self.task_timeout_seconds:
            raise ValueError("DECODE_TIMEOUT_SECONDS must be smaller than TASK_TIMEOUT_SECONDS")
        if self.max_concurrent_uploads > self.max_active_jobs:
            raise ValueError("MAX_CONCURRENT_UPLOADS must not exceed MAX_ACTIVE_JOBS")
        if self.redispatch_seconds <= self.broker_timeout_seconds:
            raise ValueError("REDISPATCH_SECONDS must exceed BROKER_TIMEOUT_SECONDS")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
