from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from app.schemas.pipeline import PipelineDefinition


SourcePolicy = Literal[
    "owned_only",
    "licensed_only",
    "public_domain_or_cc",
    "research_only",
    "remix_with_review",
]
PublishMode = Literal["preview_only", "private_upload", "unlisted_upload", "public_after_review"]
AspectRatio = Literal["9:16", "16:9", "1:1", "auto"]


class AutoFlowRequest(BaseModel):
    prompt: str
    target_platforms: list[str] = Field(default_factory=list)
    source_platforms: list[str] = Field(default_factory=lambda: ["youtube", "bilibili", "x", "xiaohongshu"])
    duration_sec: int | None = None
    aspect_ratio: AspectRatio = "auto"
    source_policy: SourcePolicy = "owned_only"
    publish_mode: PublishMode = "preview_only"
    material_library_ids: list[str] = Field(default_factory=list)
    user_constraints: dict[str, Any] = Field(default_factory=dict)

    @field_validator("prompt")
    @classmethod
    def prompt_must_not_be_blank(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("prompt must not be blank")
        return cleaned


class AutoFlowIntent(BaseModel):
    intent_type: str
    subject: str
    style: str = "auto"
    duration_sec: int = 30
    aspect_ratio: str = "9:16"
    target_platforms: list[str] = Field(default_factory=list)
    source_policy: str = "owned_only"
    publish_mode: str = "preview_only"
    keywords: list[str] = Field(default_factory=list)
    negative_keywords: list[str] = Field(default_factory=list)
    needs_voiceover: bool = False
    needs_subtitles: bool = True
    needs_bgm: bool = True
    user_confirmation_questions: list[str] = Field(default_factory=list)


class AutoFlowClipCandidate(BaseModel):
    id: str
    title: str
    source_type: str
    url: str | None = None
    asset_id: str | None = None
    start_sec: float | None = None
    end_sec: float | None = None
    score: float = 0
    score_breakdown: dict[str, float] = Field(default_factory=dict)
    rights_status: str = "unknown"
    metadata: dict[str, Any] = Field(default_factory=dict)


class AutoFlowMetadata(BaseModel):
    title_candidates: list[str] = Field(default_factory=list)
    selected_title: str | None = None
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    hashtags: list[str] = Field(default_factory=list)
    thumbnail_text_candidates: list[str] = Field(default_factory=list)
    platform_payloads: dict[str, dict[str, Any]] = Field(default_factory=dict)


class AutoFlowPlan(BaseModel):
    plan_id: str
    request: AutoFlowRequest
    intent: AutoFlowIntent
    template_id: str
    pipeline_definition: PipelineDefinition
    candidates: list[AutoFlowClipCandidate] = Field(default_factory=list)
    metadata: AutoFlowMetadata = Field(default_factory=AutoFlowMetadata)
    validation: dict[str, Any] = Field(default_factory=dict)
    rights: dict[str, Any] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    needs_review: bool = True


class AutoFlowExecuteRequest(BaseModel):
    plan_id: str | None = None
    plan: AutoFlowPlan | None = None
    save_as_template: bool = False
    execute: bool = True
    review_approved: bool = False


class AutoFlowRun(BaseModel):
    run_id: str
    plan_id: str | None = None
    pipeline_id: str | None = None
    job_id: str | None = None
    status: str = "pending"
    artifacts: dict[str, Any] = Field(default_factory=dict)
    publish: dict[str, Any] = Field(default_factory=dict)
    error_message: str | None = None


class WorkflowTemplate(BaseModel):
    id: str
    name: str
    description: str
    intent_types: list[str]
    required_capabilities: list[str]
    default_slots: dict[str, Any] = Field(default_factory=dict)
    node_blueprint: list[dict[str, Any]]
    edge_blueprint: list[dict[str, Any]]
    slot_mapping: dict[str, Any] = Field(default_factory=dict)
