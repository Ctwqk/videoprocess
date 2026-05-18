from __future__ import annotations

from pydantic import BaseModel, Field

from app.autoflow.platform_media_client import SUPPORTED_SOURCE_PLATFORMS
from app.node_registry.registry import NodeTypeRegistry

SUPPORTED_TARGET_PLATFORMS = ("youtube", "youtube_shorts", "x", "xiaohongshu")


class CapabilityPort(BaseModel):
    name: str
    port_type: str
    required: bool
    description: str = ""


class CapabilityParam(BaseModel):
    name: str
    param_type: str
    default: object | None = None
    required: bool = False
    description: str = ""
    options: list[str] | None = None
    min_value: float | None = None
    max_value: float | None = None


class DynamicInputContract(BaseModel):
    pattern: str
    port_type: str
    min_count: int = 0
    max_count: int | None = None
    ordered: bool = True
    description: str = ""


class ExecutionContract(BaseModel):
    effects: list[str] = Field(default_factory=list)
    worker_type: str = "none"
    planner_only: bool = False
    auto_executable: bool = True


class PolicyContract(BaseModel):
    requires_review: bool = False
    source_policies: list[str] = Field(default_factory=list)
    rights_risk: str = "low"
    default_privacy: str | None = None
    allowed_privacy: list[str] = Field(default_factory=list)
    public_requires_approval: bool = False


class PlannerHints(BaseModel):
    tags: list[str] = Field(default_factory=list)
    use_when: list[str] = Field(default_factory=list)
    common_upstream: list[str] = Field(default_factory=list)
    common_downstream: list[str] = Field(default_factory=list)
    fallback_nodes: list[str] = Field(default_factory=list)


class CapabilityNode(BaseModel):
    type_name: str
    display_name: str
    category: str
    description: str = ""
    icon: str = ""
    inputs: list[CapabilityPort] = Field(default_factory=list)
    outputs: list[CapabilityPort] = Field(default_factory=list)
    params: list[CapabilityParam] = Field(default_factory=list)
    worker_type: str
    autoflow_tags: list[str] = Field(default_factory=list)
    suitable_for: list[str] = Field(default_factory=list)
    dynamic_inputs: list[DynamicInputContract] = Field(default_factory=list)
    execution: ExecutionContract = Field(default_factory=ExecutionContract)
    policy: PolicyContract = Field(default_factory=PolicyContract)
    planner_hints: PlannerHints = Field(default_factory=PlannerHints)


class CapabilityManifest(BaseModel):
    nodes: list[CapabilityNode]
    target_platforms: list[str] = Field(default_factory=list)
    source_platforms: list[str] = Field(default_factory=list)


TAG_OVERRIDES = {
    "source": (["source", "asset", "owned_media"], ["compilation", "remix"]),
    "url_download": (["source", "external_url", "draft_only"], ["research", "remix"]),
    "trim": (["clip", "duration", "transform"], ["compilation", "shorts", "remix"]),
    "smart_trim": (["clip", "prompt", "vision", "transform"], ["compilation", "shorts", "remix", "storyboard"]),
    "vertical_crop": (["aspect_ratio", "shorts", "transform", "vertical"], ["compilation", "shorts", "remix"]),
    "concat_timeline": (["timeline", "assembly", "transform"], ["compilation", "remix"]),
    "concat_many": (["timeline", "assembly", "transform"], ["compilation", "remix"]),
    "montage_assembler": (["timeline", "assembly", "montage", "shorts"], ["compilation", "shorts", "remix"]),
    "title_overlay": (["title", "overlay", "enhancement"], ["compilation", "shorts", "remix"]),
    "transcode": (["output", "format", "transform"], ["shorts", "export"]),
    "export": (["output", "artifact"], ["preview", "export"]),
    "youtube_upload": (["publish", "youtube", "safety"], ["private_upload", "unlisted_upload"]),
    "x_upload": (["publish", "x", "safety"], ["draft_publish"]),
    "xiaohongshu_upload": (["publish", "xiaohongshu", "safety"], ["draft_publish"]),
    "material_search": (["search", "planner", "clip_selection", "material"], ["compilation", "remix"]),
    "youtube_search": (["search", "planner", "external_url"], ["research", "compilation"]),
    "x_search": (["search", "planner", "external_url"], ["research"]),
    "xiaohongshu_search": (["search", "planner", "external_url"], ["research"]),
    "bilibili_search": (["search", "planner", "external_url"], ["research"]),
    "zip_records": (["planner", "batch", "clip_selection"], ["batch_planning"]),
    "subtitle": (["subtitle", "enhancement"], ["explainer", "shorts"]),
    "subtitle_to_speech": (["audio", "voiceover"], ["explainer"]),
    "speech_to_subtitle": (["subtitle", "audio"], ["captioning"]),
    "bgm": (["audio", "music"], ["compilation", "shorts"]),
}

DYNAMIC_INPUT_OVERRIDES = {
    "concat_many": [
        DynamicInputContract(
            pattern="video_{n}",
            port_type="video",
            min_count=2,
            max_count=64,
            ordered=True,
            description="Ordered video inputs for timeline assembly.",
        )
    ],
    "concat_timeline": [
        DynamicInputContract(
            pattern="video_{n}",
            port_type="video",
            min_count=2,
            max_count=64,
            ordered=True,
            description="Ordered video inputs for sequential timeline assembly.",
        )
    ],
}

EXECUTION_OVERRIDES = {
    "youtube_search": ExecutionContract(effects=["external_network_read"], worker_type="planner", planner_only=True),
    "bilibili_search": ExecutionContract(effects=["external_network_read"], worker_type="planner", planner_only=True),
    "x_search": ExecutionContract(effects=["external_network_read"], worker_type="planner", planner_only=True),
    "xiaohongshu_search": ExecutionContract(effects=["external_network_read"], worker_type="planner", planner_only=True),
    "material_search": ExecutionContract(effects=["material_library_read"], worker_type="planner", planner_only=True),
    "zip_records": ExecutionContract(effects=["planner_batch"], worker_type="planner", planner_only=True),
    "url_download": ExecutionContract(effects=["external_network_read", "external_media_ingest"], worker_type="ffmpeg"),
    "youtube_upload": ExecutionContract(effects=["external_platform_write"], worker_type="ffmpeg"),
    "x_upload": ExecutionContract(effects=["external_platform_write"], worker_type="ffmpeg"),
    "xiaohongshu_upload": ExecutionContract(effects=["external_platform_write"], worker_type="ffmpeg"),
}

POLICY_OVERRIDES = {
    "youtube_search": PolicyContract(requires_review=True, source_policies=["research_only", "remix_with_review"], rights_risk="medium"),
    "bilibili_search": PolicyContract(requires_review=True, source_policies=["research_only", "remix_with_review"], rights_risk="medium"),
    "x_search": PolicyContract(requires_review=True, source_policies=["research_only", "remix_with_review"], rights_risk="medium"),
    "xiaohongshu_search": PolicyContract(requires_review=True, source_policies=["research_only", "remix_with_review"], rights_risk="medium"),
    "url_download": PolicyContract(requires_review=True, source_policies=["research_only", "remix_with_review"], rights_risk="high"),
    "youtube_upload": PolicyContract(
        requires_review=True,
        rights_risk="medium",
        default_privacy="private",
        allowed_privacy=["private", "unlisted"],
        public_requires_approval=True,
    ),
    "x_upload": PolicyContract(requires_review=True, rights_risk="high", public_requires_approval=True),
    "xiaohongshu_upload": PolicyContract(requires_review=True, rights_risk="high", public_requires_approval=True),
}

PLANNER_HINT_OVERRIDES = {
    "concat_vertical_timeline": PlannerHints(
        tags=["vertical_split", "timeline", "top_bottom", "sequential"],
        use_when=[
            "Use when one video should play in the top pane and a second video should later play in the bottom pane.",
            "Use for top/bottom split-screen prompts where one pane is active while the other pane is held as a still image.",
        ],
        common_upstream=["smart_trim", "trim", "source", "url_download"],
        common_downstream=["transcode", "export", "youtube_upload"],
        fallback_nodes=["concat_vertical", "concat_timeline"],
    ),
    "concat_many": PlannerHints(
        tags=["timeline", "dynamic_inputs", "ordered"],
        use_when=["Use when two or more video clips should be joined into one sequence."],
        common_upstream=["smart_trim", "trim", "vertical_crop", "source", "url_download"],
        common_downstream=["title_overlay", "transcode", "export"],
        fallback_nodes=["montage_assembler", "concat_timeline"],
    ),
    "concat_timeline": PlannerHints(
        tags=["timeline", "dynamic_inputs", "ordered"],
        use_when=["Use when ordered video clips should be concatenated with optional transitions."],
        common_upstream=["smart_trim", "trim", "source", "url_download"],
        common_downstream=["title_overlay", "transcode", "export"],
        fallback_nodes=["concat_many", "montage_assembler"],
    ),
    "smart_trim": PlannerHints(
        tags=["clip_selection", "prompt", "vision"],
        use_when=["Use when a video should be trimmed to match a natural language description."],
        common_upstream=["source", "url_download"],
        common_downstream=["concat_many", "concat_timeline", "concat_vertical_timeline", "transcode"],
        fallback_nodes=["trim"],
    ),
}


def _default_tags(type_name: str, category: str) -> tuple[list[str], list[str]]:
    tags = [category] if category else []
    suitable_for = ["generic_video"]
    if category == "planner":
        tags.extend(["planner", "search"])
        suitable_for = ["planning"]
    if category == "output":
        tags.append("output")
        suitable_for = ["export"]
    if category in {"combine", "transform"}:
        tags.append("transform")
        suitable_for = ["remix"]
    return sorted(set(tags)), suitable_for


def _execution_contract(type_name: str, category: str, worker_type: str) -> ExecutionContract:
    override = EXECUTION_OVERRIDES.get(type_name)
    if override is not None:
        return override
    effects: list[str] = []
    planner_only = category == "planner"
    if category in {"combine", "transform", "ai_transform", "audio", "output"}:
        effects.append("media_transform")
    if category == "source":
        effects.append("media_source")
    return ExecutionContract(
        effects=effects,
        worker_type=worker_type,
        planner_only=planner_only,
        auto_executable=not planner_only,
    )


def _policy_contract(type_name: str) -> PolicyContract:
    return POLICY_OVERRIDES.get(type_name, PolicyContract())


def _planner_hints(type_name: str, tags: list[str], suitable_for: list[str]) -> PlannerHints:
    override = PLANNER_HINT_OVERRIDES.get(type_name)
    if override is not None:
        return override
    return PlannerHints(tags=sorted(set([*tags, *suitable_for])))


def get_capability_manifest() -> CapabilityManifest:
    registry = NodeTypeRegistry.get()
    nodes: list[CapabilityNode] = []
    for definition in sorted(registry.list_types(), key=lambda item: item.type_name):
        tags, suitable_for = TAG_OVERRIDES.get(
            definition.type_name,
            _default_tags(definition.type_name, definition.category),
        )
        nodes.append(
            CapabilityNode(
                type_name=definition.type_name,
                display_name=definition.display_name,
                category=definition.category,
                description=definition.description,
                icon=definition.icon,
                inputs=[
                    CapabilityPort(
                        name=port.name,
                        port_type=port.port_type.value,
                        required=port.required,
                        description=port.description,
                    )
                    for port in definition.inputs
                ],
                outputs=[
                    CapabilityPort(
                        name=port.name,
                        port_type=port.port_type.value,
                        required=port.required,
                        description=port.description,
                    )
                    for port in definition.outputs
                ],
                params=[
                    CapabilityParam(
                        name=param.name,
                        param_type=param.param_type,
                        default=param.default,
                        required=param.required,
                        description=param.description,
                        options=param.options,
                        min_value=param.min_value,
                        max_value=param.max_value,
                    )
                    for param in definition.params
                ],
                worker_type=definition.worker_type,
                autoflow_tags=list(tags),
                suitable_for=list(suitable_for),
                dynamic_inputs=list(DYNAMIC_INPUT_OVERRIDES.get(definition.type_name, [])),
                execution=_execution_contract(definition.type_name, definition.category, definition.worker_type),
                policy=_policy_contract(definition.type_name),
                planner_hints=_planner_hints(definition.type_name, list(tags), list(suitable_for)),
            )
        )
    return CapabilityManifest(
        nodes=nodes,
        target_platforms=list(SUPPORTED_TARGET_PLATFORMS),
        source_platforms=list(SUPPORTED_SOURCE_PLATFORMS),
    )
