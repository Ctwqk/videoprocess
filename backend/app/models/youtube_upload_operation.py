from __future__ import annotations

import uuid as uuid_mod
from datetime import datetime

from sqlalchemy import CheckConstraint, JSON, DateTime, ForeignKey, Index, String, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class YouTubeUploadOperation(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "youtube_upload_operations"
    __table_args__ = (
        UniqueConstraint("node_execution_id", name="uq_youtube_upload_operations_node_execution"),
        CheckConstraint(
            "status NOT IN ('submitted', 'succeeded') OR manager_task_id IS NOT NULL",
            name="ck_youtube_upload_operations_manager_task",
        ),
        Index(
            "ux_youtube_upload_operations_production_task",
            "production_task_id",
            unique=True,
            postgresql_where=text("production_task_id IS NOT NULL"),
            sqlite_where=text("production_task_id IS NOT NULL"),
        ),
        Index(
            "ux_youtube_upload_operations_platform_video",
            "platform_video_id",
            unique=True,
            postgresql_where=text("platform_video_id IS NOT NULL"),
            sqlite_where=text("platform_video_id IS NOT NULL"),
        ),
    )

    production_task_id: Mapped[uuid_mod.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("production_tasks.id", ondelete="SET NULL"),
        nullable=True,
    )
    job_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
    )
    node_execution_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("node_executions.id", ondelete="CASCADE"),
        nullable=False,
    )
    input_artifact_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("artifacts.id"),
        nullable=False,
    )
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    privacy: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(16), default="reserved", nullable=False)
    manager_task_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    platform_video_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    receipt_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    request_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
