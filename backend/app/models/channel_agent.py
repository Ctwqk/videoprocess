from __future__ import annotations

import uuid as uuid_mod
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSON, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class ChannelProfile(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "channel_profiles"

    operator_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    positioning: Mapped[str] = mapped_column(Text, default="", nullable=False)
    language: Mapped[str] = mapped_column(String(32), default="zh", nullable=False)
    default_aspect_ratio: Mapped[str] = mapped_column(String(16), default="9:16", nullable=False)
    risk_policy_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    content_mix_policy_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    cadence_policy_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    alert_policy_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    dry_run: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    halted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    halt_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    config_version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    tick_interval_minutes: Mapped[int] = mapped_column(Integer, default=60, nullable=False)


class TopicLane(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "topic_lanes"
    __table_args__ = (Index("ix_topic_lanes_channel_profile_id", "channel_profile_id"),)

    channel_profile_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("channel_profiles.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str] = mapped_column(Text, default="", nullable=False)
    weight: Mapped[float] = mapped_column(Float, default=1.0, nullable=False)
    learned_weight: Mapped[float | None] = mapped_column(Float, nullable=True)
    keywords_json: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    negative_keywords_json: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    min_posts_per_week: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    max_posts_per_day: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    max_consecutive_streak: Mapped[int] = mapped_column(Integer, default=2, nullable=False)
    cooldown_after_post_minutes: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    paused_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class PublishingAccount(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "publishing_accounts"
    __table_args__ = (Index("ix_publishing_accounts_channel_profile_id", "channel_profile_id"),)

    channel_profile_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("channel_profiles.id", ondelete="CASCADE"), nullable=False
    )
    platform: Mapped[str] = mapped_column(String(64), default="youtube", nullable=False)
    account_label: Mapped[str] = mapped_column(String(255), nullable=False)
    platform_account_id: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    credential_ref: Mapped[str] = mapped_column(String(512), default="", nullable=False)
    platform_specific_config_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    default_privacy: Mapped[str] = mapped_column(String(32), default="public", nullable=False)
    external_asset_auto_publish: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    paused_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_token_check_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_token_check_status: Mapped[str | None] = mapped_column(String(64), nullable=True)


class LaneFormatMatrix(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "lane_format_matrix"
    __table_args__ = (Index("ix_lane_format_matrix_topic_lane_id", "topic_lane_id"),)

    topic_lane_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("topic_lanes.id", ondelete="CASCADE"), nullable=False
    )
    format_key: Mapped[str] = mapped_column(String(64), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    weight: Mapped[float] = mapped_column(Float, default=1.0, nullable=False)
    target_duration_sec: Mapped[int] = mapped_column(Integer, default=30, nullable=False)
    template_pool_json: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    source_platforms_json: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    default_publish_visibility: Mapped[str] = mapped_column(String(32), default="public", nullable=False)


class ChannelOpsQueueItem(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "channel_ops_queue_items"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_channel_ops_queue_idempotency_key"),
        Index("ix_channel_ops_queue_ready", "status", "run_after", "priority"),
        Index("ix_channel_ops_queue_channel_profile_id", "channel_profile_id"),
        Index("ix_channel_ops_queue_channel_ready", "channel_profile_id", "status", "run_after", "priority"),
    )

    kind: Mapped[str] = mapped_column(String(64), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(512), nullable=False)
    channel_profile_id: Mapped[uuid_mod.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("channel_profiles.id", ondelete="SET NULL"), nullable=True
    )
    priority: Mapped[int] = mapped_column(Integer, default=100, nullable=False)
    parent_queue_item_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    payload_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="queued", nullable=False)
    run_after: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    locked_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3, nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    dead_letter_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class AgentTickAudit(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "agent_tick_audits"
    __table_args__ = (
        UniqueConstraint("channel_profile_id", "tick_id", name="uq_agent_tick_audit_channel_tick"),
        Index("ix_agent_tick_audits_channel_profile_id", "channel_profile_id"),
    )

    channel_profile_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("channel_profiles.id", ondelete="CASCADE"), nullable=False
    )
    queue_item_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    tick_id: Mapped[str] = mapped_column(String(255), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    dry_run: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    ideas_discovered: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    candidates_scored: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    tasks_selected: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    tasks_rejected: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    guards_triggered_json: Mapped[list] = mapped_column(JSON, default=list, nullable=False)
    decision_summary_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)


class DecisionAuditEntry(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "decision_audit_entries"
    __table_args__ = (
        Index("ix_decision_audit_entries_tick", "tick_audit_id"),
        Index("ix_decision_audit_entries_channel_created", "channel_profile_id", "created_at"),
        Index("ix_decision_audit_entries_task", "created_task_id"),
        Index("ix_decision_audit_entries_source_created", "candidate_source", "created_at"),
    )

    tick_audit_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("agent_tick_audits.id", ondelete="CASCADE"), nullable=False
    )
    channel_profile_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    candidate_id: Mapped[str] = mapped_column(String(255), nullable=False)
    candidate_source: Mapped[str] = mapped_column(String(64), nullable=False)
    topic_lane_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    lane_format_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    target_account_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    score_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    guard_results_json: Mapped[list] = mapped_column(JSON, default=list, nullable=False)
    pds_decision_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    learning_context_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    selected: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    rejection_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_task_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class ManualSeed(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "manual_seeds"

    channel_profile_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("channel_profiles.id", ondelete="CASCADE"), nullable=False
    )
    topic_lane_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    target_account_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    prompt: Mapped[str] = mapped_column(Text, nullable=False)
    title_seed: Mapped[str] = mapped_column(Text, default="", nullable=False)
    source_policy: Mapped[str] = mapped_column(String(64), default="remix_with_review", nullable=False)
    source_platforms_json: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    material_library_ids_json: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    constraints_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="active", nullable=False)


class DiscoverySignal(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "discovery_signals"
    __table_args__ = (
        UniqueConstraint(
            "channel_profile_id",
            "source",
            "source_external_id",
            name="uq_discovery_signal_channel_source_external",
        ),
        Index("ix_discovery_signals_channel_lane_observed", "channel_profile_id", "topic_lane_id", "observed_at"),
        Index("ix_discovery_signals_channel_status_expires", "channel_profile_id", "status", "expires_at"),
    )

    channel_profile_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("channel_profiles.id", ondelete="CASCADE"), nullable=False
    )
    topic_lane_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_external_id: Mapped[str] = mapped_column(String(255), nullable=False)
    title: Mapped[str] = mapped_column(Text, default="", nullable=False)
    summary: Mapped[str] = mapped_column(Text, default="", nullable=False)
    keywords_json: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    trend_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    novelty_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    raw_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="active", nullable=False)
    converted_task_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)


class ProductionTask(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "production_tasks"
    __table_args__ = (Index("ix_production_tasks_channel_state", "channel_profile_id", "state"),)

    task_group_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    channel_profile_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("channel_profiles.id", ondelete="CASCADE"), nullable=False
    )
    topic_lane_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    lane_format_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    target_account_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    manual_seed_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    discovery_signal_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    source: Mapped[str] = mapped_column(String(64), default="manual_seed", nullable=False)
    title_seed: Mapped[str] = mapped_column(Text, default="", nullable=False)
    prompt: Mapped[str] = mapped_column(Text, nullable=False)
    rationale_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    score_breakdown_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    portfolio_bucket: Mapped[str] = mapped_column(String(32), default="explore", nullable=False)
    source_platforms_json: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    material_library_ids_json: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    uses_external_assets: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    approval_mode: Mapped[str] = mapped_column(String(16), default="agent", nullable=False)
    agent_approval_evidence_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    autoflow_plan_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    autoflow_run_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    pipeline_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    job_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    scheduled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    priority: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    state: Mapped[str] = mapped_column(String(32), default="seeded", nullable=False)
    state_updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_category: Mapped[str | None] = mapped_column(String(64), nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    blocked_by_guard: Mapped[str | None] = mapped_column(String(255), nullable=True)
    channel_config_version_snapshot: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    channel_config_snapshot_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    transition_history_json: Mapped[list] = mapped_column(JSON, default=list, nullable=False)


class MaterialUsageLedger(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "material_usage_ledger"
    __table_args__ = (
        Index(
            "ix_material_usage_channel_lane_segment_used",
            "channel_profile_id",
            "topic_lane_id",
            "segment_signature",
            "used_at",
        ),
    )

    material_id: Mapped[str] = mapped_column(String(255), nullable=False)
    asset_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    channel_profile_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    topic_lane_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    publishing_account_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    publication_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    used_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    segment_signature: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)


class PublicationRecord(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "publication_records"
    __table_args__ = (
        Index("ix_publication_records_task", "production_task_id"),
        Index("ux_publication_records_production_task", "production_task_id", unique=True),
        Index(
            "ux_publication_records_platform_content",
            "platform",
            "platform_content_id",
            unique=True,
        ),
    )

    production_task_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    platform: Mapped[str] = mapped_column(String(64), default="youtube", nullable=False)
    account_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    platform_content_id: Mapped[str] = mapped_column(String(255), default="", nullable=False)
    permalink: Mapped[str | None] = mapped_column(Text, nullable=True)
    title: Mapped[str] = mapped_column(Text, default="", nullable=False)
    description: Mapped[str] = mapped_column(Text, default="", nullable=False)
    tags_json: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    thumbnail_storage_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    desired_privacy: Mapped[str] = mapped_column(String(32), default="public", nullable=False)
    current_privacy: Mapped[str] = mapped_column(String(32), default="private", nullable=False)
    publish_status: Mapped[str] = mapped_column(String(32), default="uploaded", nullable=False)
    uploaded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    scheduled_publish_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    public_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    compliance_disposition: Mapped[str] = mapped_column(String(64), nullable=False)
    quota_units_estimated: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_metrics_polled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    warnings_json: Mapped[list] = mapped_column(JSON, default=list, nullable=False)


class TakedownEvent(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "takedown_events"
    __table_args__ = (
        Index("ix_takedown_events_publication_event_detected", "publication_id", "event_type", "detected_at"),
    )

    publication_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    severity: Mapped[str] = mapped_column(String(32), default="info", nullable=False)
    raw_payload_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    auto_actions_taken_json: Mapped[list] = mapped_column(JSON, default=list, nullable=False)


class FeedbackSnapshot(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "feedback_snapshots"
    __table_args__ = (
        Index("ux_feedback_snapshots_publication_stage", "publication_id", "snapshot_stage", unique=True),
    )

    publication_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    snapshot_stage: Mapped[str] = mapped_column(String(16), default="24h", nullable=False)
    collected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    views: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    likes: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    comments: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    shares: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    avg_view_duration_sec: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    retention_curve_json: Mapped[list | None] = mapped_column(JSON, nullable=True)
    ctr: Mapped[float | None] = mapped_column(Float, nullable=True)
    impressions: Mapped[int | None] = mapped_column(Integer, nullable=True)
    metrics_completeness_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    available_fields_json: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    reward_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    reward_components_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    virality_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    raw_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)


class LearningState(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "learning_states"
    __table_args__ = (
        UniqueConstraint(
            "channel_profile_id",
            "dimension_type",
            "dimension_key",
            "window_days",
            name="uq_learning_state_channel_dimension_window",
        ),
        Index("ix_learning_states_channel_dimension", "channel_profile_id", "dimension_type"),
    )

    channel_profile_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    dimension_type: Mapped[str] = mapped_column(String(64), nullable=False)
    dimension_key: Mapped[str] = mapped_column(String(255), nullable=False)
    window_days: Mapped[int] = mapped_column(Integer, nullable=False)
    sample_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    avg_reward: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    recommendation_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    last_computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class InternalSchedulerRun(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "internal_scheduler_runs"
    __table_args__ = (
        UniqueConstraint("channel_profile_id", "bucket", name="uq_internal_scheduler_channel_bucket"),
        Index("ix_internal_scheduler_runs_channel_profile_id", "channel_profile_id"),
    )

    channel_profile_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    bucket: Mapped[str] = mapped_column(String(64), nullable=False)
    enqueued_queue_item_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    ran_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="succeeded", nullable=False)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
