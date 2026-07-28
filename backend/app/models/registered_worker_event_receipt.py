from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean,
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
    ack_event_emission_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
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


class WorkerEventEmission(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "worker_event_emissions"

    source_task_attestation_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey(
            "worker_task_delivery_attestations.id",
            ondelete="RESTRICT",
        ),
        unique=True,
        nullable=False,
    )
    redis_stream: Mapped[str] = mapped_column(String(255), nullable=False)
    consumer_group: Mapped[str] = mapped_column(String(255), nullable=False)
    message_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
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
    worker_lease_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    worker_id: Mapped[str] = mapped_column(String(255), nullable=False)
    worker_started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    emission_state: Mapped[str] = mapped_column(
        String(16),
        default="prepared",
        nullable=False,
    )
    prepared_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    emitted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    resolved_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    __table_args__ = (
        Index(
            "uq_worker_event_emission_redis_identity",
            "redis_stream",
            "consumer_group",
            "message_id",
            unique=True,
            postgresql_where=text("message_id IS NOT NULL"),
        ),
        Index("ix_worker_event_emissions_job_id", "job_id"),
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


class WorkerRedisMarkerCleanupAuthorization(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "worker_redis_marker_cleanup_authorizations"

    marker_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    source_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        nullable=False,
    )
    marker_key: Mapped[str] = mapped_column(String(255), nullable=False)
    redis_stream: Mapped[str] = mapped_column(String(255), nullable=False)
    expected_message_id: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    authorization_state: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default="pending",
        server_default=text("'pending'"),
    )
    authorized_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    claimed_by_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        nullable=True,
    )
    claim_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    result_code: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )

    __table_args__ = (
        UniqueConstraint(
            "marker_kind",
            "source_id",
            name="uq_worker_redis_marker_cleanup_source",
        ),
        UniqueConstraint(
            "marker_key",
            name="uq_worker_redis_marker_cleanup_key",
        ),
        CheckConstraint(
            "marker_kind IN ('event_emission', 'task_dispatch')",
            name="ck_worker_redis_marker_cleanup_kind",
        ),
        CheckConstraint(
            "length(trim(marker_key)) > 0 "
            "AND length(trim(redis_stream)) > 0 "
            "AND length(trim(expected_message_id)) > 0",
            name="ck_worker_redis_marker_cleanup_identity",
        ),
        CheckConstraint(
            "payload_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_worker_redis_marker_cleanup_sha256",
        ),
        CheckConstraint(
            "authorization_state IN "
            "('pending', 'claimed', 'deleted', 'absent', 'conflict')",
            name="ck_worker_redis_marker_cleanup_state",
        ),
        CheckConstraint(
            "((authorization_state = 'pending' "
            "AND claimed_by_run_id IS NULL AND claim_expires_at IS NULL "
            "AND finished_at IS NULL AND result_code IS NULL) "
            "OR (authorization_state = 'claimed' "
            "AND claimed_by_run_id IS NOT NULL "
            "AND claim_expires_at IS NOT NULL "
            "AND finished_at IS NULL AND result_code IS NULL) "
            "OR (authorization_state IN ('deleted', 'absent', 'conflict') "
            "AND claimed_by_run_id IS NOT NULL "
            "AND claim_expires_at IS NOT NULL "
            "AND finished_at IS NOT NULL AND result_code IS NOT NULL))",
            name="ck_worker_redis_marker_cleanup_claim",
        ),
        Index(
            "ix_worker_redis_marker_cleanup_claim",
            "authorization_state",
            "authorized_at",
            "id",
        ),
    )


class WorkerRedisContinuityStatus(Base):
    __tablename__ = "worker_redis_continuity_status"

    singleton: Mapped[bool] = mapped_column(
        Boolean,
        primary_key=True,
        default=True,
        server_default=text("true"),
    )
    run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        nullable=False,
    )
    state: Mapped[str] = mapped_column(String(16), nullable=False)
    reason_code: Mapped[str] = mapped_column(String(64), nullable=False)
    redis_run_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )
    expected_count: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    checked_count: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    lease_expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    __table_args__ = (
        CheckConstraint(
            "(singleton) IS TRUE",
            name="ck_worker_redis_continuity_singleton",
        ),
        CheckConstraint(
            "state IN ('running', 'ready', 'error')",
            name="ck_worker_redis_continuity_state",
        ),
        CheckConstraint(
            "length(trim(reason_code)) > 0 "
            "AND expected_count >= 0 "
            "AND checked_count >= 0 "
            "AND checked_count <= expected_count "
            "AND lease_expires_at = "
            "started_at + interval '300 seconds'",
            name="ck_worker_redis_continuity_counts",
        ),
        CheckConstraint(
            "((state = 'running' "
            "AND reason_code = 'continuity_check_running' "
            "AND redis_run_id IS NULL "
            "AND checked_count = 0 "
            "AND finished_at IS NULL) "
            "OR (state = 'ready' "
            "AND reason_code = 'ready' "
            "AND length(trim(redis_run_id)) > 0 "
            "AND expected_count = checked_count "
            "AND finished_at IS NOT NULL) "
            "OR (state = 'error' AND finished_at IS NOT NULL)) IS TRUE",
            name="ck_worker_redis_continuity_result",
        ),
    )


class WorkerRedisContinuityExpectation(Base):
    __tablename__ = "worker_redis_continuity_expectations"

    run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
    )
    marker_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    source_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        nullable=False,
    )
    marker_key: Mapped[str] = mapped_column(
        String(255),
        primary_key=True,
    )
    redis_stream: Mapped[str] = mapped_column(String(255), nullable=False)
    expected_message_id: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    source_state: Mapped[str] = mapped_column(String(64), nullable=False)
    absence_allowed: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
    )
    observed_message_id: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    observed_payload_sha256: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    observed_by: Mapped[str | None] = mapped_column(
        String(63),
        nullable=True,
    )
    observed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )

    __table_args__ = (
        UniqueConstraint(
            "run_id",
            "marker_kind",
            "source_id",
            name="uq_worker_redis_continuity_expectation_source",
        ),
        CheckConstraint(
            "marker_kind IN ('event_emission', 'task_dispatch')",
            name="ck_worker_redis_continuity_expectation_kind",
        ),
        CheckConstraint(
            "length(trim(marker_key)) > 0 "
            "AND length(trim(redis_stream)) > 0 "
            "AND length(trim(source_state)) > 0",
            name="ck_worker_redis_continuity_expectation_identity",
        ),
        CheckConstraint(
            "payload_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_worker_redis_continuity_expectation_sha256",
        ),
        CheckConstraint(
            "((observed_message_id IS NULL "
            "AND observed_payload_sha256 IS NULL "
            "AND observed_by IS NULL AND observed_at IS NULL) "
            "OR (length(trim(observed_message_id)) > 0 "
            "AND observed_payload_sha256 ~ '^[0-9a-f]{64}$' "
            "AND length(trim(observed_by)) > 0 "
            "AND observed_at IS NOT NULL)) IS TRUE",
            name="ck_worker_redis_continuity_expectation_observation",
        ),
        Index(
            "ix_worker_redis_continuity_expectation_page",
            "run_id",
            "marker_key",
        ),
    )


class WorkerRedisMarkerRepairAudit(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "worker_redis_marker_repair_audits"

    source_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        nullable=False,
    )
    action: Mapped[str] = mapped_column(String(32), nullable=False)
    result_code: Mapped[str] = mapped_column(String(64), nullable=False)
    principal: Mapped[str] = mapped_column(String(63), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    __table_args__ = (
        CheckConstraint(
            "action IN ('restore_marker', 'promote_prepared')",
            name="ck_worker_redis_marker_repair_action",
        ),
        CheckConstraint(
            "result_code IN ('authorized', 'restored', 'promoted')",
            name="ck_worker_redis_marker_repair_result",
        ),
        CheckConstraint(
            "length(trim(principal)) > 0",
            name="ck_worker_redis_marker_repair_principal",
        ),
    )


# Compatibility for existing imports while the undeployed migration adopts
# the broader initial/downstream task-dispatch model.
WorkerEventDispatch = WorkerTaskDispatch
