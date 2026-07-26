from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    String,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, UUIDPrimaryKeyMixin


class RegisteredWorkerEventReceipt(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "registered_worker_event_receipts"

    redis_stream: Mapped[str] = mapped_column(String(255), nullable=False)
    consumer_group: Mapped[str] = mapped_column(String(255), nullable=False)
    message_id: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    job_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
    )
    node_execution_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("node_executions.id", ondelete="CASCADE"),
        nullable=False,
    )
    worker_registration_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("worker_registrations.id", ondelete="RESTRICT"),
        nullable=False,
    )
    worker_lease_epoch: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
    )
    worker_id: Mapped[str] = mapped_column(String(255), nullable=False)
    worker_started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    source_task_stream: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )
    source_task_group: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )
    source_task_message_id: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )
    application_state: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default="accepted",
    )
    ack_state: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default="pending",
    )
    source_task_ack_state: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default="pending",
    )
    accepted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    applied_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    acknowledged_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    source_task_acknowledged_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    __table_args__ = (
        UniqueConstraint(
            "redis_stream",
            "consumer_group",
            "message_id",
            name="uq_registered_worker_event_receipt_identity",
        ),
        UniqueConstraint(
            "source_task_stream",
            "source_task_group",
            "source_task_message_id",
            name="uq_registered_worker_event_receipt_source_task",
        ),
        CheckConstraint(
            "length(trim(redis_stream)) > 0 "
            "AND length(trim(consumer_group)) > 0 "
            "AND length(trim(message_id)) > 0",
            name="ck_registered_worker_event_receipt_redis_identity",
        ),
        CheckConstraint(
            "length(payload_sha256) = 64 "
            "AND lower(payload_sha256) = payload_sha256",
            name="ck_registered_worker_event_receipt_sha256",
        ),
        CheckConstraint(
            "event_type IN ('node_completed', 'node_failed')",
            name="ck_registered_worker_event_receipt_event_type",
        ),
        CheckConstraint(
            "worker_lease_epoch > 0",
            name="ck_registered_worker_event_receipt_epoch",
        ),
        CheckConstraint(
            "length(trim(worker_id)) > 0 "
            "AND length(trim(source_task_stream)) > 0 "
            "AND length(trim(source_task_group)) > 0 "
            "AND length(trim(source_task_message_id)) > 0",
            name="ck_registered_worker_event_receipt_claim_identity",
        ),
        CheckConstraint(
            "application_state IN ('accepted', 'applied')",
            name="ck_registered_worker_event_receipt_application_state",
        ),
        CheckConstraint(
            "ack_state IN ('pending', 'acknowledged')",
            name="ck_registered_worker_event_receipt_ack_state",
        ),
        CheckConstraint(
            "source_task_ack_state IN ('pending', 'acknowledged')",
            name="ck_registered_worker_event_receipt_task_ack_state",
        ),
        CheckConstraint(
            "((application_state = 'accepted' AND applied_at IS NULL) "
            "OR (application_state = 'applied' AND applied_at IS NOT NULL))",
            name="ck_registered_worker_event_receipt_application_time",
        ),
        CheckConstraint(
            "((ack_state = 'pending' AND acknowledged_at IS NULL) "
            "OR (ack_state = 'acknowledged' "
            "AND application_state = 'applied' "
            "AND acknowledged_at IS NOT NULL))",
            name="ck_registered_worker_event_receipt_ack_time",
        ),
        CheckConstraint(
            "((source_task_ack_state = 'pending' "
            "AND source_task_acknowledged_at IS NULL) "
            "OR (source_task_ack_state = 'acknowledged' "
            "AND application_state = 'applied' "
            "AND source_task_acknowledged_at IS NOT NULL))",
            name="ck_registered_worker_event_receipt_task_ack_time",
        ),
    )


class WorkerEventDispatch(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "worker_event_dispatches"

    receipt_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey(
            "registered_worker_event_receipts.id",
            ondelete="CASCADE",
        ),
        nullable=False,
    )
    dispatch_key: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        nullable=False,
    )
    node_execution_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("node_executions.id", ondelete="CASCADE"),
        nullable=False,
    )
    redis_stream: Mapped[str] = mapped_column(String(255), nullable=False)
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    delivery_state: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default="pending",
    )
    redis_message_id: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    __table_args__ = (
        UniqueConstraint(
            "dispatch_key",
            name="uq_worker_event_dispatch_key",
        ),
        UniqueConstraint(
            "receipt_id",
            "node_execution_id",
            name="uq_worker_event_dispatch_receipt_node",
        ),
        CheckConstraint(
            "length(trim(redis_stream)) > 0",
            name="ck_worker_event_dispatch_stream",
        ),
        CheckConstraint(
            "length(payload_sha256) = 64 "
            "AND lower(payload_sha256) = payload_sha256",
            name="ck_worker_event_dispatch_sha256",
        ),
        CheckConstraint(
            "delivery_state IN ('pending', 'delivered')",
            name="ck_worker_event_dispatch_state",
        ),
        CheckConstraint(
            "((delivery_state = 'pending' "
            "AND redis_message_id IS NULL AND delivered_at IS NULL) "
            "OR (delivery_state = 'delivered' "
            "AND redis_message_id IS NOT NULL "
            "AND delivered_at IS NOT NULL))",
            name="ck_worker_event_dispatch_delivery",
        ),
    )
