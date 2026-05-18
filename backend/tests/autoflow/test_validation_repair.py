from __future__ import annotations

import pytest

from app.autoflow.service import _assert_execute_allowed
from app.autoflow.validation_repair import AutoFlowRepairService, AutoFlowUnrepairableError
from app.orchestrator.dag import validate_pipeline
from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowExecuteRequest, AutoFlowIntent, AutoFlowPlan, AutoFlowRequest
from app.schemas.pipeline import PipelineDefinition, PipelineEdge, PipelineNode, PipelineNodeData


def node(node_id: str, node_type: str, config: dict | None = None) -> PipelineNode:
    return PipelineNode(
        id=node_id,
        type=node_type,
        position={"x": 0, "y": 0},
        data=PipelineNodeData(label=node_id, config=config or {}),
    )


def edge(edge_id: str, source: str, target: str, source_handle: str, target_handle: str) -> PipelineEdge:
    return PipelineEdge(
        id=edge_id,
        source=source,
        target=target,
        sourceHandle=source_handle,
        targetHandle=target_handle,
    )


def test_repair_invalid_param_uses_registry_default():
    definition = PipelineDefinition(
        nodes=[
            node("src_1", "source", {"asset_id": "asset-1", "media_type": "video"}),
            node("trim_1", "trim", {"start_time": "0", "duration": "5"}),
            node("transcode_1", "transcode", {"format": "mp4", "crf": 99}),
            node("export_1", "export", {}),
        ],
        edges=[
            edge("e1", "src_1", "trim_1", "output", "input"),
            edge("e2", "trim_1", "transcode_1", "output", "input"),
            edge("e3", "transcode_1", "export_1", "output", "input"),
        ],
    )
    validation = validate_pipeline(definition)
    assert not validation.valid
    assert any(error.type == "invalid_param" and error.param_name == "crf" for error in validation.errors)

    result = AutoFlowRepairService().repair(definition, validation.errors, candidates=[])

    assert result.repaired is True
    assert "invalid_param:transcode_1.crf" in result.applied_repairs
    assert validate_pipeline(result.definition).valid
    transcode = next(item for item in result.definition.nodes if item.id == "transcode_1")
    assert transcode.data.config["crf"] == 20


def test_repair_missing_asset_uses_candidate_asset_id():
    definition = PipelineDefinition(nodes=[node("src_1", "source", {"media_type": "video"})], edges=[])
    validation = validate_pipeline(definition)
    assert any(error.type == "missing_asset" for error in validation.errors)

    result = AutoFlowRepairService().repair(
        definition,
        validation.errors,
        candidates=[
            AutoFlowClipCandidate(id="c1", title="素材", source_type="asset", asset_id="asset-1"),
        ],
    )

    assert result.repaired is True
    assert result.definition.nodes[0].data.config["asset_id"] == "asset-1"
    assert result.definition.nodes[0].data.asset_id == "asset-1"
    assert validate_pipeline(result.definition).valid


def test_cycle_detected_raises_unrepairable_error():
    definition = PipelineDefinition(
        nodes=[
            node("src_1", "source", {"asset_id": "asset-1", "media_type": "video"}),
            node("trim_1", "trim", {"start_time": "0", "duration": "5"}),
        ],
        edges=[
            edge("e1", "src_1", "trim_1", "output", "input"),
            edge("e2", "trim_1", "src_1", "output", "asset_input"),
        ],
    )
    validation = validate_pipeline(definition)
    assert any(error.type == "cycle_detected" for error in validation.errors)

    with pytest.raises(AutoFlowUnrepairableError) as exc_info:
        AutoFlowRepairService().repair(definition, validation.errors, candidates=[])

    assert "cycle_detected" in exc_info.value.unrepairable_errors
    assert exc_info.value.applied_repairs == []


def test_port_type_mismatch_raises_unrepairable_error():
    definition = PipelineDefinition(
        nodes=[
            node("src_1", "source", {"asset_id": "asset-1", "media_type": "audio"}),
            node("trim_1", "trim", {"start_time": "0", "duration": "5"}),
        ],
        edges=[edge("e1", "src_1", "trim_1", "output", "input")],
    )
    validation = validate_pipeline(definition)
    assert any(error.type == "port_type_mismatch" for error in validation.errors)

    with pytest.raises(AutoFlowUnrepairableError) as exc_info:
        AutoFlowRepairService().repair(definition, validation.errors, candidates=[])

    assert exc_info.value.unrepairable_errors == ["port_type_mismatch:e1"]


def test_execute_gate_rejects_invalid_autoflow_plan():
    definition = PipelineDefinition(nodes=[node("src_1", "source", {"media_type": "video"})], edges=[])
    plan = AutoFlowPlan(
        plan_id="plan-invalid",
        request=AutoFlowRequest(prompt="make a preview"),
        intent=AutoFlowIntent(intent_type="generic_video", subject="preview"),
        template_id="material_library_remix",
        pipeline_definition=definition,
        validation={"valid": False, "errors": [{"type": "missing_asset"}]},
        rights={"status": "allowed"},
        status="drafted",
        needs_review=False,
    )

    with pytest.raises(PermissionError, match="valid workflow"):
        _assert_execute_allowed(plan, AutoFlowExecuteRequest(plan_id="plan-invalid"))
