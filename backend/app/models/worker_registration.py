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
    Integer,
    String,
    UniqueConstraint,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, UUIDPrimaryKeyMixin


JSON_DOCUMENT = JSON().with_variant(JSONB(), "postgresql")


class WorkerAdmissionGrant(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "worker_admission_grants"

    service_name: Mapped[str] = mapped_column(String(255), nullable=False)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    worker_type: Mapped[str] = mapped_column(String(64), nullable=False)
    worker_host: Mapped[str] = mapped_column(String(255), nullable=False)
    capabilities_json: Mapped[list[str]] = mapped_column(
        JSON_DOCUMENT,
        nullable=False,
    )
    release_commit: Mapped[str] = mapped_column(String(40), nullable=False)
    image_identity: Mapped[str] = mapped_column(String(255), nullable=False)
    database_principal: Mapped[str] = mapped_column(
        String(63),
        nullable=False,
    )
    redis_stream: Mapped[str] = mapped_column(String(255), nullable=False)
    redis_group: Mapped[str] = mapped_column(String(255), nullable=False)
    endpoint_bindings_json: Mapped[dict[str, object]] = mapped_column(
        JSON_DOCUMENT,
        nullable=False,
    )
    token_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    state: Mapped[str] = mapped_column(String(16), nullable=False)
    issued_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    issued_by: Mapped[str] = mapped_column(String(255), nullable=False)
    activated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    revoked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    revoke_reason: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint(
            "service_name",
            "generation",
            name="uq_worker_admission_grants_service_generation",
        ),
        UniqueConstraint(
            "token_sha256",
            name="uq_worker_admission_grants_token_sha256",
        ),
        CheckConstraint(
            "length(trim(service_name)) > 0",
            name="ck_worker_admission_grant_service_nonempty",
        ),
        CheckConstraint(
            "generation > 0",
            name="ck_worker_admission_grant_generation_positive",
        ),
        CheckConstraint(
            "length(trim(worker_type)) > 0",
            name="ck_worker_admission_grant_worker_type_nonempty",
        ),
        CheckConstraint(
            "length(trim(worker_host)) > 0",
            name="ck_worker_admission_grant_worker_host_nonempty",
        ),
        CheckConstraint(
            "length(release_commit) = 40 "
            "AND lower(release_commit) = release_commit",
            name="ck_worker_admission_grant_release_commit",
        ),
        CheckConstraint(
            "length(trim(image_identity)) > 0",
            name="ck_worker_admission_grant_image_identity",
        ),
        CheckConstraint(
            "length(trim(database_principal)) > 0",
            name="ck_worker_admission_grant_database_principal",
        ),
        CheckConstraint(
            "length(trim(redis_stream)) > 0",
            name="ck_worker_admission_grant_redis_stream",
        ),
        CheckConstraint(
            "length(trim(redis_group)) > 0",
            name="ck_worker_admission_grant_redis_group",
        ),
        CheckConstraint(
            "length(token_sha256) = 64 "
            "AND lower(token_sha256) = token_sha256",
            name="ck_worker_admission_grant_token_sha256",
        ),
        CheckConstraint(
            "state IN ('pending', 'active', 'revoked')",
            name="ck_worker_admission_grant_state",
        ),
        CheckConstraint(
            "(state = 'pending' AND activated_at IS NULL "
            "AND revoked_at IS NULL AND revoke_reason IS NULL) "
            "OR (state = 'active' AND activated_at IS NOT NULL "
            "AND revoked_at IS NULL AND revoke_reason IS NULL) "
            "OR (state = 'revoked' AND revoked_at IS NOT NULL "
            "AND length(trim(revoke_reason)) > 0)",
            name="ck_worker_admission_grant_lifecycle",
        ),
        Index(
            "uq_worker_admission_grants_active_service",
            "service_name",
            unique=True,
            postgresql_where=text("state = 'active'"),
            sqlite_where=text("state = 'active'"),
        ),
    )


class WorkerRegistration(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "worker_registrations"

    grant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey(
            "worker_admission_grants.id",
            name="fk_worker_registrations_grant_id",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    service_name: Mapped[str] = mapped_column(String(255), nullable=False)
    worker_type: Mapped[str] = mapped_column(String(64), nullable=False)
    worker_host: Mapped[str] = mapped_column(String(255), nullable=False)
    capabilities_json: Mapped[list[str]] = mapped_column(
        JSON_DOCUMENT,
        nullable=False,
    )
    worker_instance_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        nullable=False,
    )
    worker_slot: Mapped[int] = mapped_column(Integer, nullable=False)
    redis_consumer_id: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )
    image_identity: Mapped[str] = mapped_column(String(255), nullable=False)
    database_principal: Mapped[str] = mapped_column(
        String(63),
        nullable=False,
    )
    database_fingerprint: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )
    redis_fingerprint: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )
    storage_fingerprint: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )
    lease_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    lease_secret_sha256: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    registered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    heartbeat_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    lease_expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    revoked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    revoke_reason: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )
    superseded_by: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey(
            "worker_registrations.id",
            name="fk_worker_registrations_superseded_by",
            ondelete="RESTRICT",
        ),
        nullable=True,
    )

    __table_args__ = (
        UniqueConstraint(
            "service_name",
            "lease_epoch",
            name="uq_worker_registrations_service_epoch",
        ),
        CheckConstraint(
            "length(trim(service_name)) > 0",
            name="ck_worker_registration_service_nonempty",
        ),
        CheckConstraint(
            "length(trim(worker_type)) > 0",
            name="ck_worker_registration_worker_type_nonempty",
        ),
        CheckConstraint(
            "length(trim(worker_host)) > 0",
            name="ck_worker_registration_worker_host_nonempty",
        ),
        CheckConstraint(
            "worker_slot > 0",
            name="ck_worker_registration_slot_positive",
        ),
        CheckConstraint(
            "length(trim(redis_consumer_id)) > 0",
            name="ck_worker_registration_consumer_nonempty",
        ),
        CheckConstraint(
            "length(trim(image_identity)) > 0",
            name="ck_worker_registration_image_identity",
        ),
        CheckConstraint(
            "length(trim(database_principal)) > 0",
            name="ck_worker_registration_database_principal",
        ),
        CheckConstraint(
            "length(database_fingerprint) = 64 "
            "AND lower(database_fingerprint) = database_fingerprint",
            name="ck_worker_registration_database_fingerprint",
        ),
        CheckConstraint(
            "length(redis_fingerprint) = 64 "
            "AND lower(redis_fingerprint) = redis_fingerprint",
            name="ck_worker_registration_redis_fingerprint",
        ),
        CheckConstraint(
            "length(storage_fingerprint) = 64 "
            "AND lower(storage_fingerprint) = storage_fingerprint",
            name="ck_worker_registration_storage_fingerprint",
        ),
        CheckConstraint(
            "lease_epoch > 0",
            name="ck_worker_registration_epoch_positive",
        ),
        CheckConstraint(
            "length(lease_secret_sha256) = 64 "
            "AND lower(lease_secret_sha256) = lease_secret_sha256",
            name="ck_worker_registration_lease_secret_sha256",
        ),
        CheckConstraint(
            "status IN ('active', 'revoked', 'expired')",
            name="ck_worker_registration_status",
        ),
        CheckConstraint(
            "heartbeat_at >= registered_at",
            name="ck_worker_registration_heartbeat_order",
        ),
        CheckConstraint(
            "lease_expires_at > heartbeat_at",
            name="ck_worker_registration_expiry_order",
        ),
        CheckConstraint(
            "(status = 'revoked' AND revoked_at IS NOT NULL "
            "AND length(trim(revoke_reason)) > 0) "
            "OR (status IN ('active', 'expired') "
            "AND revoked_at IS NULL AND revoke_reason IS NULL)",
            name="ck_worker_registration_revocation_state",
        ),
        CheckConstraint(
            "superseded_by IS NULL "
            "OR (status = 'revoked' AND superseded_by <> id)",
            name="ck_worker_registration_supersession",
        ),
        Index("ix_worker_registrations_grant_id", "grant_id"),
        Index(
            "ix_worker_registrations_lease_expires_at",
            "lease_expires_at",
        ),
        Index(
            "uq_worker_registrations_active_service",
            "service_name",
            unique=True,
            postgresql_where=text("status = 'active'"),
            sqlite_where=text("status = 'active'"),
        ),
    )
