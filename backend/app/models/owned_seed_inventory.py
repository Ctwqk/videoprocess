from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import JSON, BigInteger, CheckConstraint, DateTime, ForeignKey, ForeignKeyConstraint, Index, Integer, String, Text, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class OwnedSeedInventory(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "owned_seed_inventories"
    __table_args__ = (
        UniqueConstraint("client_request_id", name="uq_owned_inventory_request"),
        UniqueConstraint("id", "platform_channel_id", name="uq_owned_inventory_platform_binding"),
        CheckConstraint("state IN ('draft','approved','held','exhausted','expired','revoked')", name="ck_owned_inventory_state"),
        CheckConstraint("privacy = 'unlisted' AND max_admissions = 7 AND minimum_interval_seconds = 86400", name="ck_owned_inventory_limits"),
        CheckConstraint("expires_at > starts_at", name="ck_owned_inventory_window"),
        CheckConstraint("expires_at = starts_at + interval '168 hours'", name="ck_owned_inventory_exact_window").ddl_if(dialect="postgresql"),
        CheckConstraint("approved_at IS NOT NULL OR state IN ('draft','revoked')", name="ck_owned_inventory_approval"),
        CheckConstraint("approved_at IS NULL OR (approved_by IS NOT NULL AND approval_reference IS NOT NULL)", name="ck_owned_inventory_approval_actor"),
        CheckConstraint("succession_released_at IS NULL OR (approved_at IS NOT NULL AND state IN ('held','exhausted','expired','revoked'))", name="ck_owned_inventory_release"),
        *(
            Index(f"uq_owned_inventory_occupied_{field}", field, unique=True,
                  postgresql_where=text("approved_at IS NOT NULL AND succession_released_at IS NULL"),
                  sqlite_where=text("approved_at IS NOT NULL AND succession_released_at IS NULL"))
            for field in ("channel_profile_id", "target_account_id", "platform_channel_id")
        ),
    )

    client_request_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    channel_profile_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("channel_profiles.id", ondelete="RESTRICT"), nullable=False)
    topic_lane_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("topic_lanes.id", ondelete="RESTRICT"), nullable=False)
    lane_format_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("lane_format_matrix.id", ondelete="RESTRICT"), nullable=False)
    target_account_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("publishing_accounts.id", ondelete="RESTRICT"), nullable=False)
    platform_channel_id: Mapped[str] = mapped_column(String(24), nullable=False)
    request_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    manifest_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    manifest_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    privacy: Mapped[str] = mapped_column(String(16), default="unlisted", nullable=False)
    max_admissions: Mapped[int] = mapped_column(Integer, default=7, nullable=False)
    minimum_interval_seconds: Mapped[int] = mapped_column(Integer, default=86400, nullable=False)
    starts_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    state: Mapped[str] = mapped_column(String(16), default="draft", nullable=False)
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    approved_by: Mapped[str | None] = mapped_column(String(255))
    approval_reference: Mapped[str | None] = mapped_column(String(512))
    predecessor_inventory_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("owned_seed_inventories.id", ondelete="RESTRICT"))
    predecessor_closeout_sha256: Mapped[str | None] = mapped_column(String(64))
    succession_released_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    revoked_by: Mapped[str | None] = mapped_column(String(255))
    hold_reason: Mapped[str | None] = mapped_column(Text)


class OwnedSeedInventoryItem(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "owned_seed_inventory_items"
    __table_args__ = (
        ForeignKeyConstraint(["inventory_id", "platform_channel_id"],
                             ["owned_seed_inventories.id", "owned_seed_inventories.platform_channel_id"],
                             ondelete="RESTRICT", name="fk_owned_item_scope"),
        UniqueConstraint("inventory_id", "ordinal", name="uq_owned_item_ordinal"),
        UniqueConstraint("inventory_id", "asset_id", name="uq_owned_item_asset"),
        UniqueConstraint("platform_channel_id", "content_sha256", name="uq_owned_item_platform_content"),
        UniqueConstraint("manual_seed_id", name="uq_owned_item_seed"),
        UniqueConstraint("production_task_id", name="uq_owned_item_task"),
        CheckConstraint("ordinal BETWEEN 1 AND 7", name="ck_owned_item_ordinal"),
        CheckConstraint("state IN ('unused','reserved','completed','held')", name="ck_owned_item_state"),
        CheckConstraint("byte_size > 0 AND byte_size <= 67108864", name="ck_owned_item_size"),
        CheckConstraint("(state = 'unused' AND production_task_id IS NULL AND consumed_at IS NULL) OR (state <> 'unused' AND production_task_id IS NOT NULL AND consumed_at IS NOT NULL)", name="ck_owned_item_consumption"),
        Index("uq_owned_item_outstanding", "inventory_id", unique=True,
              postgresql_where=text("state = 'reserved'"), sqlite_where=text("state = 'reserved'")),
    )

    inventory_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    platform_channel_id: Mapped[str] = mapped_column(String(24), nullable=False)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    manual_seed_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("manual_seeds.id", ondelete="RESTRICT"), nullable=False)
    asset_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("assets.id", ondelete="RESTRICT"), nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    byte_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    storage_descriptor_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    provenance_evidence_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    provenance_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    seed_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    state: Mapped[str] = mapped_column(String(16), default="unused", nullable=False)
    production_task_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("production_tasks.id", ondelete="RESTRICT"))
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    hold_reason: Mapped[str | None] = mapped_column(Text)
