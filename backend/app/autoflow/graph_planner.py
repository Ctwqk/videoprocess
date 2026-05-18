from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Protocol

import httpx
from pydantic import ValidationError

from app.autoflow.capability_manifest import CapabilityManifest, get_capability_manifest
from app.autoflow.pipeline_policy import PipelinePolicyResult, validate_pipeline_policy
from app.autoflow.validation_repair import AutoFlowRepairService
from app.config import settings
from app.node_registry.registry import NodeTypeRegistry
from app.orchestrator.dag import validate_pipeline
from app.schemas.autoflow import (
    AutoFlowClipCandidate,
    AutoFlowMetadata,
    AutoFlowRequest,
    DraftEdge,
    DraftNode,
    GraphPlanningAttempt,
    GraphPlanningResult,
    PipelineDraft,
)
from app.schemas.pipeline import PipelineDefinition, PipelineEdge, PipelineNode, PipelineNodeData, ValidationResult


class DraftCompileError(ValueError):
    """Raised when a model-produced pipeline draft cannot be compiled."""


class GraphPlanningUnavailable(RuntimeError):
    """Raised when no graph-planning provider can produce a draft."""


class GraphPlanningFailed(RuntimeError):
    """Raised when generated graph output cannot be validated."""


class GraphDraftProvider(Protocol):
    async def draft_for_request(
        self,
        request: AutoFlowRequest,
        manifest: CapabilityManifest,
    ) -> tuple[PipelineDraft, str] | None:
        ...


@dataclass(frozen=True)
class GraphPlanningOutcome:
    draft: PipelineDraft
    definition: PipelineDefinition
    validation: ValidationResult
    policy: PipelinePolicyResult
    graph_result: GraphPlanningResult
    candidates: list[AutoFlowClipCandidate]
    metadata: AutoFlowMetadata
    warnings: list[str]


class AutoFlowGraphPlanner:
    def __init__(
        self,
        repair_service: AutoFlowRepairService | None = None,
        provider: GraphDraftProvider | None = None,
    ) -> None:
        self.repair_service = repair_service or AutoFlowRepairService()
        self.provider = provider or LLMGraphDraftProvider()

    async def plan(self, request: AutoFlowRequest) -> GraphPlanningOutcome:
        draft, source = await self._draft_for_request(request)
        definition = pipeline_definition_from_draft(draft)
        validation = validate_pipeline(definition)
        repairs: list[str] = []

        if not validation.valid:
            repair = self.repair_service.repair(definition, validation.errors, candidates=[])
            definition = repair.definition
            validation = validate_pipeline(definition)
            repairs.extend(repair.applied_repairs)

        policy = validate_pipeline_policy(definition, request)
        if policy.repairs:
            definition = policy.definition
            validation = validate_pipeline(definition)
            repairs.extend(policy.repairs)

        attempt = GraphPlanningAttempt(
            attempt=1,
            source=source,
            valid=validation.valid and policy.valid,
            errors=[
                *[error.model_dump(mode="json") for error in validation.errors],
                *[error.model_dump(mode="json") for error in policy.errors],
            ],
            warnings=[
                *[warning.model_dump(mode="json") for warning in validation.warnings],
                *[warning.model_dump(mode="json") for warning in policy.warnings],
            ],
            repairs=repairs,
            notes=list(draft.planner_notes),
        )
        graph_result = GraphPlanningResult(
            draft=draft,
            attempts=[attempt],
            used_fallback=False,
            policy=policy.model_dump(mode="json", exclude={"definition"}),
        )
        if not attempt.valid:
            raise GraphPlanningFailed("Generated graph failed validation")

        return GraphPlanningOutcome(
            draft=draft,
            definition=definition,
            validation=validation,
            policy=policy,
            graph_result=graph_result,
            candidates=_candidates_from_draft(draft),
            metadata=_metadata_from_draft(draft),
            warnings=_warnings_from_outcome(draft, policy),
        )

    async def _draft_for_request(self, request: AutoFlowRequest) -> tuple[PipelineDraft, str]:
        raw_draft = request.constraints.get("pipeline_draft") if isinstance(request.constraints, dict) else None
        if raw_draft:
            return PipelineDraft.model_validate(raw_draft), "constraints.pipeline_draft"
        if request.allow_experimental_graph_planning:
            provider_result = await self.provider.draft_for_request(request, get_capability_manifest())
            if provider_result is not None:
                return provider_result
        dog_cat = _dog_cat_vertical_timeline_draft(request)
        if dog_cat is not None:
            return dog_cat, "rule.dog_cat_vertical_timeline"
        raise GraphPlanningUnavailable("No AI graph planner provider produced a draft")


class LLMGraphDraftProvider:
    async def draft_for_request(
        self,
        request: AutoFlowRequest,
        manifest: CapabilityManifest,
    ) -> tuple[PipelineDraft, str] | None:
        if not request.provider_config_id or not request.model:
            return None

        payload = {
            "model": request.model,
            "provider_config_id": request.provider_config_id,
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You generate only strict PipelineDraft JSON for VideoProcess. "
                        "Use only node types, ports, and params in the provided capability manifest. "
                        "Do not invent worker code, permissions, or node contracts."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "request": request.model_dump(mode="json"),
                            "capability_manifest": manifest.model_dump(mode="json"),
                            "required_shape": {
                                "name": "string",
                                "description": "string",
                                "nodes": ["{id,type,label,config,asset_id?,position?}"],
                                "edges": ["{source,sourceHandle,target,targetHandle,id?}"],
                                "planner_notes": ["string"],
                                "assumptions": ["string"],
                                "risk_flags": ["string"],
                            },
                        },
                        ensure_ascii=False,
                    ),
                },
            ],
        }
        url = f"{settings.exo_watchdog_url.rstrip('/')}/v1/chat/completions"
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(45.0, connect=10.0)) as client:
                response = await client.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
            raw_content = data["choices"][0]["message"]["content"]
            draft_payload = _json_object_from_text(str(raw_content))
            if "draft" in draft_payload and isinstance(draft_payload["draft"], dict):
                draft_payload = draft_payload["draft"]
            return PipelineDraft.model_validate(draft_payload), "llm.chat_completions"
        except (httpx.HTTPError, KeyError, IndexError, TypeError, ValueError, ValidationError):
            return None


def pipeline_definition_from_draft(draft: PipelineDraft) -> PipelineDefinition:
    registry = NodeTypeRegistry.get()
    seen_node_ids: set[str] = set()
    nodes: list[PipelineNode] = []

    for index, draft_node in enumerate(draft.nodes):
        if registry.get_type(draft_node.type) is None:
            raise DraftCompileError(f"Unknown node type '{draft_node.type}'")
        if draft_node.id in seen_node_ids:
            raise DraftCompileError(f"Duplicate node id '{draft_node.id}'")
        seen_node_ids.add(draft_node.id)
        config = dict(draft_node.config)
        asset_id = draft_node.asset_id or _string_or_none(config.get("asset_id"))
        nodes.append(
            PipelineNode(
                id=draft_node.id,
                type=draft_node.type,
                position=draft_node.position or _default_position(index),
                data=PipelineNodeData(
                    label=draft_node.label or draft_node.type,
                    config=config,
                    asset_id=asset_id,
                ),
            )
        )

    edges: list[PipelineEdge] = []
    seen_edge_ids: set[str] = set()
    for index, draft_edge in enumerate(draft.edges, start=1):
        if draft_edge.source not in seen_node_ids:
            raise DraftCompileError(f"Edge source '{draft_edge.source}' does not exist")
        if draft_edge.target not in seen_node_ids:
            raise DraftCompileError(f"Edge target '{draft_edge.target}' does not exist")
        edge_id = draft_edge.id or f"e-{draft_edge.source}-{draft_edge.target}-{index}"
        if edge_id in seen_edge_ids:
            raise DraftCompileError(f"Duplicate edge id '{edge_id}'")
        seen_edge_ids.add(edge_id)
        edges.append(
            PipelineEdge(
                id=edge_id,
                source=draft_edge.source,
                target=draft_edge.target,
                sourceHandle=draft_edge.sourceHandle,
                targetHandle=draft_edge.targetHandle,
            )
        )

    return PipelineDefinition(nodes=nodes, edges=edges)


def _default_position(index: int) -> dict[str, float]:
    return {"x": float((index % 6) * 260), "y": float((index // 6) * 140)}


def _string_or_none(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _dog_cat_vertical_timeline_draft(request: AutoFlowRequest) -> PipelineDraft | None:
    prompt = request.prompt.lower()
    has_dog = any(term in request.prompt for term in ("小狗", "狗狗")) or "dog" in prompt or "puppy" in prompt
    has_cat = any(term in request.prompt for term in ("小猫", "猫咪")) or "cat" in prompt or "kitten" in prompt
    has_vertical_layout = any(term in request.prompt for term in ("上半", "下半", "上面", "下面")) or (
        "top" in prompt and "bottom" in prompt
    )
    has_sequence = any(term in request.prompt for term in ("先播放", "后播放", "先播", "后播")) or "first" in prompt
    if not (has_dog and has_cat and has_vertical_layout and has_sequence):
        return None

    include_upload = request.publish_mode in {"private_upload", "unlisted_upload", "public_after_review"}
    nodes = [
        _source_node("source_dog", "Dog source", "autoflow-ai-graph-dog"),
        _smart_trim_node("smart_trim_dog", "Dog smart trim", "cute puppy, dog playing, clear subject"),
        _source_node("source_cat", "Cat source", "autoflow-ai-graph-cat"),
        _smart_trim_node("smart_trim_cat", "Cat smart trim", "cute kitten, cat playing, clear subject"),
        DraftNode(
            id="concat_vertical_timeline_1",
            type="concat_vertical_timeline",
            label="Top dog first, bottom cat second",
            config={"pane_width": 960, "pane_height": 540, "background_color": "black", "output_format": "mp4"},
        ),
        DraftNode(
            id="transcode_1",
            type="transcode",
            label="Transcode",
            config={"format": "mp4", "video_codec": "libx264", "audio_codec": "aac", "crf": 23},
        ),
        DraftNode(
            id="export_1",
            type="export",
            label="Export Preview",
            config={"output_dir": "/tmp/vp_autoflow_exports", "filename": "dog-cat-vertical-timeline.mp4"},
        ),
    ]
    edges = [
        DraftEdge(source="source_dog", sourceHandle="output", target="smart_trim_dog", targetHandle="input"),
        DraftEdge(source="source_cat", sourceHandle="output", target="smart_trim_cat", targetHandle="input"),
        DraftEdge(
            source="smart_trim_dog",
            sourceHandle="output",
            target="concat_vertical_timeline_1",
            targetHandle="video_first",
        ),
        DraftEdge(
            source="smart_trim_cat",
            sourceHandle="output",
            target="concat_vertical_timeline_1",
            targetHandle="video_second",
        ),
        DraftEdge(
            source="concat_vertical_timeline_1",
            sourceHandle="output",
            target="transcode_1",
            targetHandle="input",
        ),
        DraftEdge(source="transcode_1", sourceHandle="output", target="export_1", targetHandle="input"),
    ]
    if include_upload:
        privacy = "unlisted" if request.publish_mode == "unlisted_upload" else "private"
        nodes.append(
            DraftNode(
                id="youtube_upload_1",
                type="youtube_upload",
                label="YouTube Upload",
                config={
                    "title": "Dog and Cat Split Timeline",
                    "description": "AutoFlow AI graph plan: dog top pane first, cat bottom pane second.",
                    "privacy": privacy,
                    "made_for_kids": "not_set",
                    "tags": "dog,cat,autoflow",
                },
            )
        )
        edges.append(DraftEdge(source="transcode_1", sourceHandle="output", target="youtube_upload_1", targetHandle="input"))

    return PipelineDraft(
        name="Dog Cat Vertical Timeline",
        description="Dog clip plays in the top pane first, then cat clip plays in the bottom pane.",
        nodes=nodes,
        edges=edges,
        planner_notes=[
            "Selected concat_vertical_timeline because the prompt asks for top/bottom sequential playback.",
            "Used owned placeholder asset ids until material search selects concrete assets.",
        ],
        assumptions=["Dog is the first/top subject; cat is the second/bottom subject."],
        risk_flags=["upload_requires_review"] if include_upload else [],
    )


def _source_node(node_id: str, label: str, asset_id: str) -> DraftNode:
    return DraftNode(
        id=node_id,
        type="source",
        label=label,
        config={"asset_id": asset_id, "media_type": "video"},
        asset_id=asset_id,
    )


def _smart_trim_node(node_id: str, label: str, prompt: str) -> DraftNode:
    return DraftNode(
        id=node_id,
        type="smart_trim",
        label=label,
        config={
            "prompt": prompt,
            "negative_prompt": "watermark, unsafe scene, blurry",
            "mode": "best_clip",
            "target_duration": 4,
            "min_clip_duration": 1.5,
            "max_clip_duration": 8,
            "max_clips": 1,
            "sample_fps": 1,
            "match_threshold": 0.35,
            "return_full_threshold": 0.65,
            "padding_before": 0.5,
            "padding_after": 0.5,
            "merge_gap": 1,
            "use_visual": True,
            "use_asr": True,
            "use_vlm_verify": False,
            "language": "zh",
            "output_format": "mp4",
            "no_match_policy": "placeholder",
        },
    )


def _candidates_from_draft(draft: PipelineDraft) -> list[AutoFlowClipCandidate]:
    candidates: list[AutoFlowClipCandidate] = []
    for node in draft.nodes:
        if node.type != "source":
            continue
        asset_id = node.asset_id or _string_or_none(node.config.get("asset_id"))
        if not asset_id:
            continue
        candidates.append(
            AutoFlowClipCandidate(
                id=f"graph-{node.id}",
                title=node.label or node.id,
                source_type="asset",
                asset_id=asset_id,
                rights_status="allowed",
                metadata={"graph_node_id": node.id},
            )
        )
    return candidates


def _metadata_from_draft(draft: PipelineDraft) -> AutoFlowMetadata:
    return AutoFlowMetadata(
        title_candidates=[draft.name],
        selected_title=draft.name,
        description=draft.description,
        tags=["autoflow", "ai_graph"],
        hashtags=["#AutoFlow"],
        thumbnail_text_candidates=[draft.name],
    )


def _warnings_from_outcome(draft: PipelineDraft, policy: PipelinePolicyResult) -> list[str]:
    warnings = list(draft.risk_flags)
    warnings.extend(issue.message for issue in policy.warnings)
    if policy.requires_review:
        warnings.append("AI graph plan requires human review before platform upload or public publishing.")
    return warnings


def _json_object_from_text(value: str) -> dict:
    cleaned = value.strip()
    try:
        payload = json.loads(cleaned)
    except json.JSONDecodeError:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start < 0 or end < start:
            raise ValueError("model response did not contain a JSON object")
        payload = json.loads(cleaned[start:end + 1])
    if not isinstance(payload, dict):
        raise ValueError("model response JSON must be an object")
    return payload
