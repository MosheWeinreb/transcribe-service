"""Public HTTP responses and the internal transcription message contract."""

from datetime import datetime
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class JobStatus(StrEnum):
    QUEUED = "queued"
    PROCESSING = "processing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class ErrorDetail(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    error: ErrorDetail


class PublicConfig(BaseModel):
    max_upload_bytes: int
    max_filename_length: int
    max_duration_seconds: int
    recording_stop_ms: int
    poll_interval_ms: int
    retention_seconds: int


class LivenessResponse(BaseModel):
    status: Literal["alive"] = "alive"


class ReadinessResponse(BaseModel):
    status: Literal["ready"] = "ready"
    scope: Literal["database schema and disk space"] = "database schema and disk space"


class JobAccepted(BaseModel):
    id: UUID
    status: Literal[JobStatus.QUEUED] = JobStatus.QUEUED
    status_url: str


class JobResult(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    filename: str
    status: JobStatus
    created_at: datetime
    finished_at: datetime | None
    transcript: str | None
    language: str | None
    duration_seconds: float | None
    error: ErrorDetail | None = None
