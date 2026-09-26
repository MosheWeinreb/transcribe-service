from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, DateTime, Double, Index, Text, false, func, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    type_annotation_map = {str: Text, datetime: DateTime(timezone=True)}


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('uploading', 'queued', 'processing', 'succeeded', 'failed')",
            name="jobs_status_check",
        ),
        CheckConstraint("upload_bytes >= 0", name="jobs_upload_bytes_check"),
        Index("jobs_dispatch_idx", "available_at", postgresql_where=text("status = 'queued'")),
        Index(
            "jobs_deadline_idx",
            "deadline",
            postgresql_where=text("status IN ('uploading', 'processing')"),
        ),
        Index(
            "jobs_retention_idx", "finished_at", postgresql_where=text("finished_at IS NOT NULL")
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    filename: Mapped[str]
    status: Mapped[str]
    upload_bytes: Mapped[int] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())
    available_at: Mapped[datetime] = mapped_column(server_default=func.now())
    finished_at: Mapped[datetime | None]
    deadline: Mapped[datetime | None]
    dispatch_pending: Mapped[bool] = mapped_column(server_default=false())
    published_at: Mapped[datetime | None]
    attempts: Mapped[int] = mapped_column(server_default="0")
    attempt_id: Mapped[UUID | None]
    transcript: Mapped[str | None]
    language: Mapped[str | None]
    duration_seconds: Mapped[float | None] = mapped_column(Double)
    error_code: Mapped[str | None]
    error_message: Mapped[str | None]
