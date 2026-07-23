from __future__ import annotations
import enum
import uuid as uuid_mod
from datetime import datetime
from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    SmallInteger,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from app.models.base import Base, UUIDPrimaryKeyMixin


class JobStatus(str, enum.Enum):
    PENDING = "PENDING"
    WAITING_WINDOW = "WAITING_WINDOW"
    VALIDATING = "VALIDATING"
    PLANNING = "PLANNING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    PARTIALLY_FAILED = "PARTIALLY_FAILED"


class NodeStatus(str, enum.Enum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    CANCELLED = "CANCELLED"


class Job(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "jobs"

    pipeline_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("pipelines.id"), nullable=False
    )
    pipeline_snapshot: Mapped[dict] = mapped_column(JSON, nullable=False)
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus, name="job_status", create_constraint=True),
        default=JobStatus.PENDING,
    )
    execution_plan: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    submitted_at: Mapped[datetime] = mapped_column(
        server_default=func.now(), nullable=False
    )
    started_at: Mapped[datetime | None] = mapped_column(nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    submitted_by: Mapped[str] = mapped_column(String(255), default="system")
    parent_job_id: Mapped[uuid_mod.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id"), nullable=True
    )
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    orchestrator_owner: Mapped[str] = mapped_column(
        String(32), default="python", nullable=False
    )

    node_executions: Mapped[list["NodeExecution"]] = relationship(
        back_populates="job", cascade="all, delete-orphan"
    )


class NodeExecution(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "node_executions"

    job_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False
    )
    node_id: Mapped[str] = mapped_column(String(255), nullable=False)
    node_type: Mapped[str] = mapped_column(String(100), nullable=False)
    node_label: Mapped[str] = mapped_column(String(255), default="")
    node_config: Mapped[dict] = mapped_column(JSON, default=dict)
    status: Mapped[NodeStatus] = mapped_column(
        Enum(NodeStatus, name="node_status", create_constraint=True),
        default=NodeStatus.PENDING,
    )
    progress: Mapped[int] = mapped_column(
        SmallInteger, default=0,
    )
    worker_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    queued_at: Mapped[datetime | None] = mapped_column(nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    completed_at: Mapped[datetime | None] = mapped_column(nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_trace: Mapped[str | None] = mapped_column(Text, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    input_artifact_ids: Mapped[list[uuid_mod.UUID]] = mapped_column(
        ARRAY(UUID(as_uuid=True)).with_variant(JSON, "sqlite"), default=list
    )
    output_artifact_id: Mapped[uuid_mod.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )

    job: Mapped["Job"] = relationship(back_populates="node_executions")

    __table_args__ = (
        CheckConstraint("progress >= 0 AND progress <= 100", name="ck_progress_range"),
    )
