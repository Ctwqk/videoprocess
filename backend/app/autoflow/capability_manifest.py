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
            )
        )
    return CapabilityManifest(
        nodes=nodes,
        target_platforms=list(SUPPORTED_TARGET_PLATFORMS),
        source_platforms=list(SUPPORTED_SOURCE_PLATFORMS),
    )
