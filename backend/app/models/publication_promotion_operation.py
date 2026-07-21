from __future__ import annotations

import uuid as uuid_mod
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, JSON, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class PublicationPromotionOperation(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "publication_promotion_operations"
    __table_args__ = (
        UniqueConstraint(
            "publication_id",
            name="uq_publication_promotion_operations_publication",
        ),
        UniqueConstraint(
            "queue_item_id",
            name="uq_publication_promotion_operations_queue_item",
        ),
        UniqueConstraint(
            "attempt_key",
            name="uq_publication_promotion_operations_attempt_key",
        ),
        CheckConstraint(
            "target_privacy IN ('private', 'unlisted')",
            name="ck_publication_promotion_operations_target_privacy",
        ),
        CheckConstraint(
            "status IN ('reserved', 'submitting', 'confirmed', 'finalized', 'uncertain')",
            name="ck_publication_promotion_operations_status",
        ),
    )

    publication_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("publication_records.id", ondelete="CASCADE"),
        nullable=False,
    )
    production_task_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("production_tasks.id", ondelete="CASCADE"),
        nullable=False,
    )
    queue_item_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    platform_video_id: Mapped[str] = mapped_column(String(255), nullable=False)
    target_privacy: Mapped[str] = mapped_column(String(32), nullable=False)
    scheduled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    attempt_key: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(16), default="reserved", nullable=False)
    decision_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    observed_privacy: Mapped[str | None] = mapped_column(String(32), nullable=True)
    observed_publish_status: Mapped[str | None] = mapped_column(String(64), nullable=True)
    evidence_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    request_attempted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    confirmed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
