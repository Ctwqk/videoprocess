from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

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
SourceStrategy = Literal[
    "auto",
    "input_video",
    "material_library",
    "external_research",
    "generate_missing",
    "hybrid",
]
PlanningMode = Literal["auto", "template", "storyboard", "ai_graph"]
AutoFlowPlanStatus = Literal[
    "drafted",
    "review_required",
    "review_approved",
    "public_approved",
    "rejected",
    "blocked",
    "executed",
]


class AutoFlowRequest(BaseModel):
    prompt: str
    input_asset_id: str | None = None
    target_platforms: list[str] = Field(default_factory=list)
    source_platforms: list[str] = Field(default_factory=lambda: ["youtube", "bilibili", "x", "xiaohongshu"])
    duration_sec: int | None = None
    aspect_ratio: AspectRatio = "auto"
    source_policy: SourcePolicy = "owned_only"
    publish_mode: PublishMode = "preview_only"
    material_library_ids: list[str] = Field(default_factory=list)
    source_strategy: SourceStrategy = "auto"
    allow_video_generation: bool = False
    min_shots: int = Field(default=3, ge=1, le=24)
    max_shots: int = Field(default=8, ge=1, le=24)
    provider_config_id: str | None = None
    model: str | None = None
    constraints: dict[str, Any] = Field(default_factory=dict)
    user_constraints: dict[str, Any] = Field(default_factory=dict)
    planning_mode: PlanningMode = "auto"
    max_repair_attempts: int = Field(default=3, ge=0, le=5)
    allow_experimental_graph_planning: bool = False

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
    material_id: str | None = None
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


class DraftNode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    type: str
    label: str = ""
    config: dict[str, Any] = Field(default_factory=dict)
    asset_id: str | None = None
    position: dict[str, float] | None = None


class DraftEdge(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str | None = None
    source: str
    sourceHandle: str
    target: str
    targetHandle: str


class PipelineDraft(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = "AI Graph Plan"
    description: str = ""
    nodes: list[DraftNode]
    edges: list[DraftEdge]
    planner_notes: list[str] = Field(default_factory=list)
    assumptions: list[str] = Field(default_factory=list)
    risk_flags: list[str] = Field(default_factory=list)


class DraftNodeUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    label: str | None = None
    config: dict[str, Any] | None = None
    asset_id: str | None = None
    position: dict[str, float] | None = None


class DraftEdgeReplacement(BaseModel):
    model_config = ConfigDict(extra="forbid")

    remove_edge_id: str | None = None
    remove: DraftEdge | None = None
    add: DraftEdge


class PipelineDraftPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    add_nodes: list[DraftNode] = Field(default_factory=list)
    update_nodes: list[DraftNodeUpdate] = Field(default_factory=list)
    remove_node_ids: list[str] = Field(default_factory=list)
    add_edges: list[DraftEdge] = Field(default_factory=list)
    remove_edge_ids: list[str] = Field(default_factory=list)
    replace_edges: list[DraftEdgeReplacement] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class GraphPlanningAttempt(BaseModel):
    attempt: int
    source: str
    valid: bool
    errors: list[dict[str, Any]] = Field(default_factory=list)
    warnings: list[dict[str, Any]] = Field(default_factory=list)
    repairs: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class GraphPlanningResult(BaseModel):
    draft: PipelineDraft | None = None
    attempts: list[GraphPlanningAttempt] = Field(default_factory=list)
    used_fallback: bool = False
    policy: dict[str, Any] = Field(default_factory=dict)


class VideoGenerationHints(BaseModel):
    enabled: bool = False
    prompt: str = ""
    negative_prompt: str = ""
    reference_asset_ids: list[str] = Field(default_factory=list)
    reference_image_asset_id: str | None = None
    reference_video_asset_id: str | None = None
    first_frame_asset_id: str | None = None
    last_frame_asset_id: str | None = None
    model_hint: str = "auto"
    resolution: str = "auto"
    fps: int | None = None
    seed: int | None = None
    guidance_scale: float | None = None
    motion_strength: float | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class CameraSpec(BaseModel):
    shot_size: Literal[
        "extreme_close_up",
        "close_up",
        "medium",
        "wide",
        "establishing",
        "auto",
    ] = "auto"
    angle: Literal[
        "eye_level",
        "low_angle",
        "high_angle",
        "top_down",
        "dutch_angle",
        "auto",
    ] = "auto"
    movement: Literal[
        "static",
        "handheld",
        "push_in",
        "pull_out",
        "pan",
        "tilt",
        "tracking",
        "orbit",
        "auto",
    ] = "auto"
    lens: str = ""
    composition: str = ""


class VisualStyleSpec(BaseModel):
    mood: str = ""
    lighting: str = ""
    color_palette: str = ""
    realism: Literal["realistic", "cinematic", "documentary", "anime", "illustration", "auto"] = "auto"
    texture: str = ""
    platform_style: str = ""


class ShotSpec(BaseModel):
    id: str
    role: Literal[
        "hook",
        "setup",
        "action",
        "reaction",
        "detail",
        "transition",
        "ending",
        "b_roll",
    ] = "action"
    description: str = ""
    director_notes: str = ""
    search_query: str
    search_queries: list[str] = Field(default_factory=list)
    negative_queries: list[str] = Field(default_factory=list)
    must_have: list[str] = Field(default_factory=list)
    nice_to_have: list[str] = Field(default_factory=list)
    must_not_have: list[str] = Field(default_factory=list)
    target_duration: float = 4.0
    min_duration: float = 1.5
    max_duration: float = 8.0
    camera: CameraSpec = Field(default_factory=CameraSpec)
    visual_style: VisualStyleSpec = Field(default_factory=VisualStyleSpec)
    narration: str = ""
    on_screen_text: str = ""
    sound_design: str = ""
    generation: VideoGenerationHints = Field(default_factory=VideoGenerationHints)
    matched_asset_id: str | None = None
    matched_source_asset_id: str | None = None
    matched_start_sec: float | None = None
    matched_end_sec: float | None = None
    match_score: float | None = None
    match_status: Literal["pending", "matched", "missing", "generated", "skipped"] = "pending"
    extra: dict[str, Any] = Field(default_factory=dict)


class StoryboardPlan(BaseModel):
    subject: str
    title: str = ""
    logline: str = ""
    style: str = "auto"
    target_platforms: list[str] = Field(default_factory=list)
    aspect_ratio: AspectRatio = "auto"
    total_duration: float = 30
    source_strategy: Literal[
        "input_video",
        "material_library",
        "external_research",
        "generate_missing",
        "hybrid",
    ] = "input_video"
    allow_video_generation: bool = False
    shots: list[ShotSpec]
    title_candidates: list[str] = Field(default_factory=list)
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    hashtags: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    extra: dict[str, Any] = Field(default_factory=dict)


class AutoFlowStoryboardRequest(BaseModel):
    prompt: str
    input_asset_id: str | None = None
    material_library_ids: list[str] = Field(default_factory=list)
    target_duration: float = 30
    aspect_ratio: AspectRatio = "auto"
    target_platforms: list[str] = Field(default_factory=list)
    source_strategy: SourceStrategy = "input_video"
    allow_video_generation: bool = False
    max_shots: int = Field(default=8, ge=1, le=24)
    min_shots: int = Field(default=3, ge=1, le=24)
    style: str = "auto"
    provider_config_id: str | None = None
    model: str | None = None
    constraints: dict[str, Any] = Field(default_factory=dict)

    @field_validator("prompt")
    @classmethod
    def prompt_must_not_be_blank(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("prompt must not be blank")
        return cleaned


class AutoFlowStoryboardResponse(BaseModel):
    storyboard: StoryboardPlan
    raw_model_output: str | None = None
    warnings: list[str] = Field(default_factory=list)


class AutoFlowPlan(BaseModel):
    plan_id: str
    request: AutoFlowRequest
    intent: AutoFlowIntent
    template_id: str
    pipeline_definition: PipelineDefinition
    storyboard: StoryboardPlan | None = None
    candidates: list[AutoFlowClipCandidate] = Field(default_factory=list)
    metadata: AutoFlowMetadata = Field(default_factory=AutoFlowMetadata)
    validation: dict[str, Any] = Field(default_factory=dict)
    rights: dict[str, Any] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    needs_review: bool = True
    status: AutoFlowPlanStatus = "drafted"
    execution_revision: int = 1
    review_approved_at: datetime | None = None
    approved_revision_hash: str | None = None
    approved_revision: int | None = None
    public_approved_at: datetime | None = None
    agent_approved_by: str | None = None
    review_notes: str | None = None
    rejected_reason: str | None = None


class AutoFlowPlanPatch(BaseModel):
    selected_candidate_ids: list[str] | None = None
    locked_candidate_ids: list[str] | None = None
    replacement_candidates: list[AutoFlowClipCandidate] | None = None
    metadata: dict[str, Any] | None = None
    publish_mode: PublishMode | None = None
    publish_settings: dict[str, Any] = Field(default_factory=dict)
    target_platforms: list[str] | None = None
    user_constraints: dict[str, Any] | None = None
    rebuild_definition: bool = True
    run_validation: bool = Field(default=True, alias="validate")
    evaluate_rights: bool = True
    model_config = {"populate_by_name": True}


class AutoFlowApprovalRequest(BaseModel):
    review_notes: str | None = None


class AutoFlowRejectRequest(BaseModel):
    rejected_reason: str | None = None


class AutoFlowExecuteRequest(BaseModel):
    plan_id: str | None = None
    plan: AutoFlowPlan | None = None
    save_as_template: bool = False
    execute: bool = True
    review_approved: bool = False
    public_approved: bool = False
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=512)
    expected_approved_revision_hash: str | None = Field(
        default=None,
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-fA-F]{64}$",
    )
    expected_approved_revision: int | None = Field(default=None, ge=1)


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
