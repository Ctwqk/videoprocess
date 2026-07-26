from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import JSON, CheckConstraint, DateTime, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, UUIDPrimaryKeyMixin


class LegacyWorkerEventResolution(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "legacy_worker_event_resolutions"

    redis_stream: Mapped[str] = mapped_column(String(255), nullable=False)
    consumer_group: Mapped[str] = mapped_column(String(255), nullable=False)
    message_id: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    node_execution_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
    )
    resolution_reason: Mapped[str] = mapped_column(String(64), nullable=False)
    operator_id: Mapped[str] = mapped_column(String(255), nullable=False)
    observed_job_status: Mapped[str] = mapped_column(String(32), nullable=False)
    observed_node_status: Mapped[str] = mapped_column(String(32), nullable=False)
    observed_task_state: Mapped[str] = mapped_column(String(32), nullable=False)
    observed_channel_halted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    acknowledged_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    __table_args__ = (
        UniqueConstraint(
            "redis_stream",
            "consumer_group",
            "message_id",
            name="uq_legacy_worker_event_resolution_identity",
        ),
        CheckConstraint(
            "length(trim(redis_stream)) > 0",
            name="ck_legacy_worker_event_resolution_stream_nonempty",
        ),
        CheckConstraint(
            "length(trim(consumer_group)) > 0",
            name="ck_legacy_worker_event_resolution_group_nonempty",
        ),
        CheckConstraint(
            "length(trim(message_id)) > 0",
            name="ck_legacy_worker_event_resolution_message_nonempty",
        ),
        CheckConstraint(
            "length(payload_sha256) = 64",
            name="ck_legacy_worker_event_resolution_sha256_length",
        ),
        CheckConstraint(
            "event_type = 'node_failed'",
            name="ck_legacy_worker_event_resolution_event_type",
        ),
        CheckConstraint(
            "resolution_reason = 'terminal_cancelled'",
            name="ck_legacy_worker_event_resolution_reason",
        ),
        CheckConstraint(
            "length(trim(operator_id)) > 0",
            name="ck_legacy_worker_event_resolution_operator_nonempty",
        ),
        CheckConstraint(
            "observed_job_status = 'CANCELLED'",
            name="ck_legacy_worker_event_resolution_job_status",
        ),
        CheckConstraint(
            "observed_node_status = 'CANCELLED'",
            name="ck_legacy_worker_event_resolution_node_status",
        ),
        CheckConstraint(
            "observed_task_state = 'held'",
            name="ck_legacy_worker_event_resolution_task_state",
        ),
    )
