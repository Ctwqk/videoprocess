from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, model_validator


class ChannelProfileCreate(BaseModel):
    name: str
    positioning: str = ""
    language: str = "zh"
    default_aspect_ratio: str = "9:16"
    risk_policy_json: dict[str, Any] = Field(default_factory=dict)
    content_mix_policy_json: dict[str, Any] = Field(default_factory=dict)
    cadence_policy_json: dict[str, Any] = Field(default_factory=dict)
    alert_policy_json: dict[str, Any] = Field(default_factory=dict)


class ChannelProfileRead(ChannelProfileCreate):
    id: str
    enabled: bool
    dry_run: bool
    halted_at: datetime | None = None
    halt_reason: str | None = None
    config_version: int


class TopicLaneCreate(BaseModel):
    name: str
    description: str = ""
    weight: float = 1.0
    keywords_json: list[str] = Field(default_factory=list)
    negative_keywords_json: list[str] = Field(default_factory=list)
    min_posts_per_week: int = 0
    max_posts_per_day: int = 1
    max_consecutive_streak: int = 2
    cooldown_after_post_minutes: int = 0


class TopicLaneRead(TopicLaneCreate):
    id: str
    channel_profile_id: str
    enabled: bool
    paused_until: datetime | None = None


class PublishingAccountCreate(BaseModel):
    account_label: str
    platform: str = "youtube"
    platform_account_id: str = ""
    credential_ref: str = ""
    platform_specific_config_json: dict[str, Any] = Field(default_factory=dict)
    default_privacy: str = "private"
    external_asset_auto_publish: bool = False


class PublishingAccountRead(PublishingAccountCreate):
    id: str
    channel_profile_id: str
    enabled: bool
    paused_until: datetime | None = None
    last_token_check_status: str | None = None


class LaneFormatCreate(BaseModel):
    format_key: str = "shorts_9x16"
    enabled: bool = True
    weight: float = 1.0
    target_duration_sec: int = 30
    template_pool_json: list[str] = Field(default_factory=lambda: ["material_library_remix"])
    source_platforms_json: list[str] = Field(default_factory=list)
    default_publish_visibility: str = "private"


class LaneFormatRead(LaneFormatCreate):
    id: str
    topic_lane_id: str


class ManualSeedCreate(BaseModel):
    topic_lane_id: str | None = None
    target_account_id: str | None = None
    prompt: str
    title_seed: str = ""
    source_policy: str = "remix_with_review"
    source_platforms_json: list[str] = Field(default_factory=list)
    material_library_ids_json: list[str] = Field(default_factory=list)
    constraints_json: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_owned_input_canary(self) -> ManualSeedCreate:
        if "input_asset_id" not in self.constraints_json:
            return self
        input_asset_id = self.constraints_json["input_asset_id"]
        if input_asset_id == "":
            return self
        if not isinstance(input_asset_id, str):
            raise ValueError("input_asset_id must be a canonical UUID")
        try:
            canonical_asset_id = str(UUID(input_asset_id))
        except ValueError as error:
            raise ValueError("input_asset_id must be a canonical UUID") from error
        if input_asset_id != canonical_asset_id:
            raise ValueError("input_asset_id must be a canonical UUID")
        if self.source_platforms_json:
            raise ValueError("owned input canaries cannot select external source platforms")
        if self.source_policy != "owned_only":
            raise ValueError("owned input canaries require source_policy=owned_only")
        if self.constraints_json.get("source_strategy") not in (None, "input_video"):
            raise ValueError("owned input canaries require source_strategy=input_video")
        if self.constraints_json.get("planning_mode") not in (None, "template"):
            raise ValueError("owned input canaries require planning_mode=template")
        return self


class QueueItemRead(BaseModel):
    id: str
    kind: str
    idempotency_key: str
    channel_profile_id: str | None = None
    priority: int
    status: str
    payload_json: dict[str, Any]
    attempt_count: int
    last_error: str | None = None


class HealthSummary(BaseModel):
    channel_id: str
    dry_run: bool
    halted: bool
    active_tasks: int = 0
    queued_items: int = 0
    recent_failures: int = 0
    last_successful_measured_at: datetime | None = None
    warnings: list[str] = Field(default_factory=list)


class DecisionAuditEntryRead(BaseModel):
    id: str
    tick_audit_id: str
    channel_profile_id: str
    candidate_id: str
    candidate_source: str
    topic_lane_id: str | None = None
    lane_format_id: str | None = None
    target_account_id: str | None = None
    score_json: dict[str, Any]
    guard_results_json: list[Any]
    pds_decision_json: dict[str, Any]
    learning_context_json: dict[str, Any]
    selected: bool
    rejection_reason: str | None = None
    created_task_id: str | None = None
    created_at: datetime


class LearningStateRead(BaseModel):
    id: str
    channel_profile_id: str
    dimension_type: str
    dimension_key: str
    window_days: int
    sample_count: int
    avg_reward: float
    confidence: float
    recommendation_json: dict[str, Any]
    last_computed_at: datetime
