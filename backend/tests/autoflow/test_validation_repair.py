from __future__ import annotations

from app.autoflow.validation_repair import AutoFlowRepairService
from app.orchestrator.dag import validate_pipeline
from app.schemas.autoflow import AutoFlowClipCandidate
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
    assert transcode.data.config["crf"] == 23


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


def test_cycle_detected_is_not_repaired():
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

    result = AutoFlowRepairService().repair(definition, validation.errors, candidates=[])

    assert result.repaired is False
    assert result.definition == definition
    assert "cycle_detected" in result.unrepairable_errors


def test_port_type_mismatch_returns_manual_repair_reason():
    definition = PipelineDefinition(
        nodes=[
            node("src_1", "source", {"asset_id": "asset-1", "media_type": "audio"}),
            node("trim_1", "trim", {"start_time": "0", "duration": "5"}),
        ],
        edges=[edge("e1", "src_1", "trim_1", "output", "input")],
    )
    validation = validate_pipeline(definition)
    assert any(error.type == "port_type_mismatch" for error in validation.errors)

    result = AutoFlowRepairService().repair(definition, validation.errors, candidates=[])

    assert result.repaired is False
    assert "port_type_mismatch:e1" in result.unrepairable_errors
