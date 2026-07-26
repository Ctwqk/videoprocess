from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    String,
    UniqueConstraint,
    Uuid,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, UUIDPrimaryKeyMixin


class WorkerTaskDeliveryAttestation(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "worker_task_delivery_attestations"

    redis_stream: Mapped[str] = mapped_column(String(255), nullable=False)
    consumer_group: Mapped[str] = mapped_column(String(255), nullable=False)
    message_id: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    dispatch_key: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        nullable=False,
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("jobs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    node_execution_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("node_executions.id", ondelete="RESTRICT"),
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
    ack_state: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default="pending",
    )
    acknowledged_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    attested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint(
            "redis_stream",
            "consumer_group",
            "message_id",
            name="uq_worker_task_delivery_attestation_identity",
        ),
        UniqueConstraint(
            "node_execution_id",
            "worker_registration_id",
            "worker_lease_epoch",
            "worker_id",
            "worker_started_at",
            name="uq_worker_task_delivery_attestation_claim",
        ),
        CheckConstraint(
            "length(trim(redis_stream)) > 0 "
            "AND length(trim(consumer_group)) > 0 "
            "AND length(trim(message_id)) > 0 "
            "AND length(trim(worker_id)) > 0",
            name="ck_worker_task_delivery_attestation_identity",
        ),
        CheckConstraint(
            "length(payload_sha256) = 64 "
            "AND lower(payload_sha256) = payload_sha256",
            name="ck_worker_task_delivery_attestation_sha256",
        ),
        CheckConstraint(
            "worker_lease_epoch > 0",
            name="ck_worker_task_delivery_attestation_epoch",
        ),
        CheckConstraint(
            "ack_state IN ('pending', 'authorized', 'acknowledged')",
            name="ck_worker_task_delivery_attestation_ack_state",
        ),
        CheckConstraint(
            "((ack_state IN ('pending', 'authorized') "
            "AND acknowledged_at IS NULL) "
            "OR (ack_state = 'acknowledged' "
            "AND acknowledged_at IS NOT NULL))",
            name="ck_worker_task_delivery_attestation_ack_time",
        ),
    )


class RegisteredWorkerEventReceipt(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "registered_worker_event_receipts"

    source_task_attestation_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey(
            "worker_task_delivery_attestations.id",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    redis_stream: Mapped[str] = mapped_column(String(255), nullable=False)
    consumer_group: Mapped[str] = mapped_column(String(255), nullable=False)
    message_id: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    job_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("jobs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    node_execution_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("node_executions.id", ondelete="RESTRICT"),
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
            "source_task_attestation_id",
            name="uq_registered_worker_event_receipt_attestation",
        ),
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


class RegisteredWorkerEventDelivery(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "registered_worker_event_deliveries"

    source_task_attestation_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey(
            "worker_task_delivery_attestations.id",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    receipt_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey(
            "registered_worker_event_receipts.id",
            ondelete="RESTRICT",
        ),
        nullable=True,
    )
    redis_stream: Mapped[str] = mapped_column(String(255), nullable=False)
    consumer_group: Mapped[str] = mapped_column(String(255), nullable=False)
    message_id: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    resolution_state: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
    )
    reason_code: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    ack_state: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default="pending",
    )
    accepted_at: Mapped[datetime] = mapped_column(
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
            name="uq_registered_worker_event_delivery_identity",
        ),
        CheckConstraint(
            "length(trim(redis_stream)) > 0 "
            "AND length(trim(consumer_group)) > 0 "
            "AND length(trim(message_id)) > 0",
            name="ck_registered_worker_event_delivery_redis_identity",
        ),
        CheckConstraint(
            "length(payload_sha256) = 64 "
            "AND lower(payload_sha256) = payload_sha256",
            name="ck_registered_worker_event_delivery_sha256",
        ),
        CheckConstraint(
            "((resolution_state = 'accepted' "
            "AND receipt_id IS NOT NULL AND reason_code IS NULL) "
            "OR (resolution_state = 'quarantined' "
            "AND reason_code IS NOT NULL "
            "AND length(trim(reason_code)) > 0))",
            name="ck_registered_worker_event_delivery_resolution",
        ),
        CheckConstraint(
            "ack_state IN ('pending', 'acknowledged')",
            name="ck_registered_worker_event_delivery_ack_state",
        ),
        CheckConstraint(
            "((ack_state = 'pending' AND acknowledged_at IS NULL) "
            "OR (ack_state = 'acknowledged' "
            "AND acknowledged_at IS NOT NULL))",
            name="ck_registered_worker_event_delivery_ack_time",
        ),
    )


class WorkerTaskDispatch(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "worker_task_dispatches"

    origin_receipt_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey(
            "registered_worker_event_receipts.id",
            ondelete="RESTRICT",
        ),
        nullable=True,
    )
    dispatch_key: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        nullable=False,
    )
    job_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("jobs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    node_execution_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("node_executions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    redis_stream: Mapped[str] = mapped_column(String(255), nullable=False)
    consumer_group: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    delivery_state: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default="pending",
    )
    delivery_attempted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    delivery_error: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )
    redis_message_id: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    resolution_state: Mapped[str] = mapped_column(
        String(24),
        nullable=False,
        default="unresolved",
    )
    acknowledged_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    cancelled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
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
            name="uq_worker_task_dispatch_key",
        ),
        UniqueConstraint(
            "origin_receipt_id",
            "node_execution_id",
            name="uq_worker_task_dispatch_receipt_node",
        ),
        CheckConstraint(
            "length(trim(redis_stream)) > 0 "
            "AND length(trim(consumer_group)) > 0",
            name="ck_worker_task_dispatch_redis_identity",
        ),
        CheckConstraint(
            "length(payload_sha256) = 64 "
            "AND lower(payload_sha256) = payload_sha256",
            name="ck_worker_task_dispatch_sha256",
        ),
        CheckConstraint(
            "delivery_state IN "
            "('pending', 'attempting', 'delivered', 'uncertain', 'cancelled')",
            name="ck_worker_task_dispatch_state",
        ),
        CheckConstraint(
            "((delivery_state = 'pending' "
            "AND delivery_attempted_at IS NULL "
            "AND redis_message_id IS NULL AND delivered_at IS NULL) "
            "OR (delivery_state IN ('attempting', 'uncertain') "
            "AND delivery_attempted_at IS NOT NULL "
            "AND redis_message_id IS NULL AND delivered_at IS NULL) "
            "OR (delivery_state = 'delivered' "
            "AND delivery_attempted_at IS NOT NULL "
            "AND redis_message_id IS NOT NULL "
            "AND delivered_at IS NOT NULL) "
            "OR (delivery_state = 'cancelled' "
            "AND redis_message_id IS NULL AND delivered_at IS NULL))",
            name="ck_worker_task_dispatch_delivery",
        ),
        CheckConstraint(
            "resolution_state IN "
            "('unresolved', 'cancel_authorized', 'acknowledged', 'cancelled')",
            name="ck_worker_task_dispatch_resolution_state",
        ),
        CheckConstraint(
            "((resolution_state IN ('unresolved', 'cancel_authorized') "
            "AND acknowledged_at IS NULL AND cancelled_at IS NULL) "
            "OR (resolution_state = 'acknowledged' "
            "AND acknowledged_at IS NOT NULL AND cancelled_at IS NULL) "
            "OR (resolution_state = 'cancelled' "
            "AND acknowledged_at IS NULL AND cancelled_at IS NOT NULL))",
            name="ck_worker_task_dispatch_resolution_time",
        ),
        Index(
            "uq_worker_task_dispatch_unresolved_initial_node",
            "node_execution_id",
            unique=True,
            postgresql_where=text(
                "origin_receipt_id IS NULL "
                "AND resolution_state IN "
                "('unresolved', 'cancel_authorized')"
            ),
            sqlite_where=text(
                "origin_receipt_id IS NULL "
                "AND resolution_state IN "
                "('unresolved', 'cancel_authorized')"
            ),
        ),
    )


# Compatibility for existing imports while the undeployed migration adopts
# the broader initial/downstream task-dispatch model.
WorkerEventDispatch = WorkerTaskDispatch
