from __future__ import annotations

import sys
from pathlib import Path

from app.orchestrator.dag import validate_pipeline
from app.schemas.pipeline import PipelineDefinition

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from scripts.go_channel_ops_integration_smoke import build_pipeline_definition, final_artifact_node_ids


def test_integration_smoke_pipeline_is_valid() -> None:
    definition = build_pipeline_definition(
        {
            "video": "00000000-0000-0000-0000-000000000001",
            "audio": "00000000-0000-0000-0000-000000000002",
            "image": "00000000-0000-0000-0000-000000000003",
            "subtitle": "00000000-0000-0000-0000-000000000004",
        }
    )

    result = validate_pipeline(PipelineDefinition(**definition))

    assert result.valid, result.errors
    assert "export_1" in final_artifact_node_ids()
    assert any(node["id"] == "trim_1" and node["type"] == "trim" for node in definition["nodes"])
