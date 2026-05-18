from __future__ import annotations

from app.orchestrator.dag import validate_pipeline
from app.schemas.pipeline import PipelineDefinition, PipelineEdge, PipelineNode, PipelineNodeData


def _source_node(index: int) -> PipelineNode:
    return PipelineNode(
        id=f"source_{index}",
        type="source",
        position={"x": 0, "y": float(index * 80)},
        data=PipelineNodeData(
            label=f"Source {index}",
            config={"asset_id": f"asset-{index}", "media_type": "video"},
        ),
    )


def _concat_node() -> PipelineNode:
    return PipelineNode(
        id="concat_1",
        type="concat_timeline",
        position={"x": 320, "y": 0},
        data=PipelineNodeData(
            label="Timeline Concat",
            config={"input_count": 13, "transition": "none"},
        ),
    )


def test_timeline_concat_accepts_dynamic_video_handles_beyond_default_inputs():
    sources = [_source_node(index) for index in range(1, 14)]
    concat = _concat_node()
    definition = PipelineDefinition(
        nodes=[*sources, concat],
        edges=[
            PipelineEdge(
                id=f"e-source-{index}-concat",
                source=f"source_{index}",
                target=concat.id,
                sourceHandle="output",
                targetHandle=f"video_{index}",
            )
            for index in range(1, 14)
        ],
    )

    validation = validate_pipeline(definition)

    assert validation.valid, [error.message for error in validation.errors]


def test_timeline_concat_requires_at_least_two_connected_video_inputs():
    source = _source_node(1)
    concat = _concat_node()
    definition = PipelineDefinition(
        nodes=[source, concat],
        edges=[
            PipelineEdge(
                id="e-source-1-concat",
                source=source.id,
                target=concat.id,
                sourceHandle="output",
                targetHandle="video_1",
            )
        ],
    )

    validation = validate_pipeline(definition)

    assert not validation.valid
    assert any(error.type == "missing_required_input" and error.node_id == concat.id for error in validation.errors)


def test_timeline_concat_keeps_legacy_two_input_handles_valid():
    first = _source_node(1)
    second = _source_node(2)
    concat = _concat_node()
    definition = PipelineDefinition(
        nodes=[first, second, concat],
        edges=[
            PipelineEdge(
                id="e-source-1-concat",
                source=first.id,
                target=concat.id,
                sourceHandle="output",
                targetHandle="video_first",
            ),
            PipelineEdge(
                id="e-source-2-concat",
                source=second.id,
                target=concat.id,
                sourceHandle="output",
                targetHandle="video_second",
            ),
        ],
    )

    validation = validate_pipeline(definition)

    assert validation.valid, [error.message for error in validation.errors]


def test_timeline_concat_rejects_dynamic_video_handles_above_contract_limit():
    first = _source_node(1)
    second = _source_node(2)
    overflow = _source_node(65)
    concat = _concat_node()
    definition = PipelineDefinition(
        nodes=[first, second, overflow, concat],
        edges=[
            PipelineEdge(
                id="e-source-1-concat",
                source=first.id,
                target=concat.id,
                sourceHandle="output",
                targetHandle="video_1",
            ),
            PipelineEdge(
                id="e-source-2-concat",
                source=second.id,
                target=concat.id,
                sourceHandle="output",
                targetHandle="video_2",
            ),
            PipelineEdge(
                id="e-source-65-concat",
                source=overflow.id,
                target=concat.id,
                sourceHandle="output",
                targetHandle="video_65",
            ),
        ],
    )

    validation = validate_pipeline(definition)

    assert not validation.valid
    assert any(
        error.type == "invalid_dynamic_input" and error.target_port == "video_65"
        for error in validation.errors
    )
