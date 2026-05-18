from __future__ import annotations

import pytest

from app.autoflow.graph_planner import (
    AutoFlowGraphPlanner,
    DraftCompileError,
    GraphPlanningUnavailable,
    pipeline_definition_from_draft,
)
from app.orchestrator.dag import validate_pipeline
from app.schemas.autoflow import AutoFlowRequest, DraftEdge, DraftNode, PipelineDraft


def _dog_cat_draft() -> PipelineDraft:
    return PipelineDraft(
        name="Dog Cat Vertical Timeline",
        description="Top puppy first, bottom kitten second.",
        nodes=[
            DraftNode(
                id="source_dog",
                type="source",
                label="Dog source",
                config={"asset_id": "asset-dog", "media_type": "video"},
                asset_id="asset-dog",
            ),
            DraftNode(
                id="trim_dog",
                type="smart_trim",
                label="Dog smart trim",
                config={
                    "prompt": "cute puppy",
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
                    "output_format": "mp4",
                    "no_match_policy": "placeholder",
                },
            ),
            DraftNode(
                id="source_cat",
                type="source",
                label="Cat source",
                config={"asset_id": "asset-cat", "media_type": "video"},
                asset_id="asset-cat",
            ),
            DraftNode(
                id="trim_cat",
                type="smart_trim",
                label="Cat smart trim",
                config={
                    "prompt": "cute kitten",
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
                    "output_format": "mp4",
                    "no_match_policy": "placeholder",
                },
            ),
            DraftNode(
                id="vertical_timeline",
                type="concat_vertical_timeline",
                label="Top dog then bottom cat",
                config={"pane_width": 960, "pane_height": 540, "output_format": "mp4"},
            ),
            DraftNode(
                id="transcode",
                type="transcode",
                label="Transcode",
                config={"format": "mp4", "video_codec": "libx264", "audio_codec": "aac", "crf": 23},
            ),
            DraftNode(
                id="export",
                type="export",
                label="Export",
                config={"output_dir": "/tmp/vp_autoflow_exports", "filename": "dog-cat.mp4"},
            ),
        ],
        edges=[
            DraftEdge(source="source_dog", sourceHandle="output", target="trim_dog", targetHandle="input"),
            DraftEdge(source="source_cat", sourceHandle="output", target="trim_cat", targetHandle="input"),
            DraftEdge(
                source="trim_dog",
                sourceHandle="output",
                target="vertical_timeline",
                targetHandle="video_first",
            ),
            DraftEdge(
                source="trim_cat",
                sourceHandle="output",
                target="vertical_timeline",
                targetHandle="video_second",
            ),
            DraftEdge(source="vertical_timeline", sourceHandle="output", target="transcode", targetHandle="input"),
            DraftEdge(source="transcode", sourceHandle="output", target="export", targetHandle="input"),
        ],
        assumptions=["Use available owned material assets."],
        risk_flags=[],
    )


def test_pipeline_draft_compiles_to_valid_pipeline_definition():
    definition = pipeline_definition_from_draft(_dog_cat_draft())

    assert validate_pipeline(definition).valid
    assert [node.type for node in definition.nodes] == [
        "source",
        "smart_trim",
        "source",
        "smart_trim",
        "concat_vertical_timeline",
        "transcode",
        "export",
    ]
    assert all(edge.id for edge in definition.edges)
    assert all("x" in node.position and "y" in node.position for node in definition.nodes)
    source_dog = next(node for node in definition.nodes if node.id == "source_dog")
    assert source_dog.data.asset_id == "asset-dog"


def test_pipeline_draft_rejects_unknown_node_type_before_graph_validation():
    draft = _dog_cat_draft().model_copy(
        update={
            "nodes": [
                *_dog_cat_draft().nodes,
                DraftNode(id="imaginary", type="made_up_node", label="Nope", config={}),
            ]
        }
    )

    with pytest.raises(DraftCompileError, match="Unknown node type 'made_up_node'"):
        pipeline_definition_from_draft(draft)


def test_pipeline_draft_preserves_invalid_port_for_validator_to_report():
    draft = _dog_cat_draft()
    broken_edges = list(draft.edges)
    broken_edges[2] = broken_edges[2].model_copy(update={"targetHandle": "video_top"})
    definition = pipeline_definition_from_draft(draft.model_copy(update={"edges": broken_edges}))

    validation = validate_pipeline(definition)

    assert validation.valid is False
    assert any(error.type == "port_type_mismatch" for error in validation.errors)


@pytest.mark.asyncio
async def test_graph_planner_uses_pipeline_draft_constraint_and_policy_repair():
    draft = _dog_cat_draft()
    draft.nodes.append(
        DraftNode(
            id="youtube_upload_1",
            type="youtube_upload",
            label="Upload",
            config={"title": "Dog Cat", "privacy": "private"},
        )
    )
    draft.edges.append(
        DraftEdge(source="transcode", sourceHandle="output", target="youtube_upload_1", targetHandle="input")
    )
    request = AutoFlowRequest(
        prompt="preview dog cat",
        planning_mode="ai_graph",
        publish_mode="preview_only",
        constraints={"pipeline_draft": draft.model_dump(mode="json")},
    )

    outcome = await AutoFlowGraphPlanner().plan(request)

    assert outcome.validation.valid is True
    assert outcome.policy.repairs == ["removed_upload:youtube_upload_1"]
    assert "youtube_upload" not in [node.type for node in outcome.definition.nodes]
    assert outcome.graph_result.attempts[0].source == "constraints.pipeline_draft"


@pytest.mark.asyncio
async def test_graph_planner_builds_dog_cat_vertical_timeline_from_prompt():
    request = AutoFlowRequest(
        prompt="生成一个视频，上半部分是小狗，下半部分是小猫视频，上半部分先播放，下半部分后播放",
        planning_mode="ai_graph",
        publish_mode="private_upload",
    )

    outcome = await AutoFlowGraphPlanner().plan(request)

    node_types = [node.type for node in outcome.definition.nodes]
    assert outcome.validation.valid is True
    assert "concat_vertical_timeline" in node_types
    assert "youtube_upload" in node_types
    upload = next(node for node in outcome.definition.nodes if node.type == "youtube_upload")
    assert upload.data.config["privacy"] == "private"
    assert outcome.policy.requires_review is True
    assert [candidate.asset_id for candidate in outcome.candidates] == ["autoflow-ai-graph-dog", "autoflow-ai-graph-cat"]


@pytest.mark.asyncio
async def test_graph_planner_reports_unavailable_for_generic_prompt():
    request = AutoFlowRequest(prompt="make a generic finance explainer", planning_mode="ai_graph")

    with pytest.raises(GraphPlanningUnavailable):
        await AutoFlowGraphPlanner().plan(request)


@pytest.mark.asyncio
async def test_graph_planner_uses_experimental_provider_before_rule_fallback():
    class FakeProvider:
        async def draft_for_request(self, request, manifest):
            return _dog_cat_draft(), "fake_llm"

    request = AutoFlowRequest(
        prompt="use a model to make this",
        planning_mode="ai_graph",
        allow_experimental_graph_planning=True,
        provider_config_id="provider-1",
        model="model-1",
    )

    outcome = await AutoFlowGraphPlanner(provider=FakeProvider()).plan(request)

    assert outcome.validation.valid is True
    assert outcome.graph_result.attempts[0].source == "fake_llm"
