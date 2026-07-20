from __future__ import annotations

import uuid as uuid_mod
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Float, ForeignKey, Index, Integer, String, Text, func, text
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class AutoFlowPlan(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "autoflow_plans"

    prompt: Mapped[str] = mapped_column(Text, nullable=False)
    request_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    intent_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    template_id: Mapped[str] = mapped_column(String(255), nullable=False)
    pipeline_definition: Mapped[dict] = mapped_column(JSON, nullable=False)
    storyboard_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    candidates_json: Mapped[list] = mapped_column(JSON, default=list, nullable=False)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    rights_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    validation_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="drafted", nullable=False)
    execution_revision: Mapped[int] = mapped_column(
        BigInteger,
        default=1,
        server_default=text("1"),
        nullable=False,
    )
    review_approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    approved_revision_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    approved_revision: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    public_approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    agent_approved_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    review_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    rejected_reason: Mapped[str | None] = mapped_column(Text, nullable=True)


class AutoFlowRun(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "autoflow_runs"

    plan_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("autoflow_plans.id", ondelete="CASCADE"), nullable=False
    )
    pipeline_id: Mapped[uuid_mod.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("pipelines.id", ondelete="SET NULL"), nullable=True
    )
    job_id: Mapped[uuid_mod.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="SET NULL"), nullable=True
    )
    status: Mapped[str] = mapped_column(String(32), default="pending", nullable=False)
    artifacts_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    publish_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    execute_idempotency_key: Mapped[str | None] = mapped_column(String(512), unique=True, nullable=True)
    request_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)


class AutoFlowUsedClip(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "autoflow_used_clips"
    __table_args__ = (
        Index("ix_autoflow_used_clips_asset_selected_at", "asset_id", "selected_at"),
        Index("ix_autoflow_used_clips_run_id", "run_id"),
    )

    run_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    asset_id: Mapped[str] = mapped_column(String(255), nullable=False)
    source_platform: Mapped[str | None] = mapped_column(String(64), nullable=True)
    candidate_title: Mapped[str | None] = mapped_column(Text, nullable=True)
    selected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)


class ContentMetric(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "content_metrics"

    run_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("autoflow_runs.id", ondelete="CASCADE"), nullable=False
    )
    platform: Mapped[str] = mapped_column(String(64), nullable=False)
    platform_content_id: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    views: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    likes: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    comments: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    shares: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    watch_time_sec: Mapped[float] = mapped_column(Float, default=0, nullable=False)
    avg_view_duration_sec: Mapped[float] = mapped_column(Float, default=0, nullable=False)
    retention_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    collected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class TrendSignal(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "trend_signals"

    source: Mapped[str] = mapped_column(String(64), nullable=False)
    keyword: Mapped[str] = mapped_column(String(255), nullable=False)
    score: Mapped[float] = mapped_column(Float, default=0, nullable=False)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
