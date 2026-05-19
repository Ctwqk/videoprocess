import json
from pathlib import Path

from app.orchestrator.dag import validate_pipeline
from app.schemas.pipeline import PipelineDefinition


FIXTURE_DIR = Path(__file__).parent / "golden" / "go_migration"


def test_pipeline_basic_fixture_matches_python_validation_contract():
    definition = PipelineDefinition.model_validate_json(
        (FIXTURE_DIR / "pipeline_basic.json").read_text(encoding="utf-8")
    )

    result = validate_pipeline(definition)
    expected = json.loads(
        (FIXTURE_DIR / "pipeline_validation_basic.valid.json").read_text(encoding="utf-8")
    )

    assert result.model_dump(mode="json") == expected


def test_task_fixture_uses_existing_redis_payload_keys():
    task = json.loads((FIXTURE_DIR / "job_task_ffmpeg.json").read_text(encoding="utf-8"))

    assert sorted(task) == [
        "affinity_bounces",
        "affinity_enqueued_at",
        "config",
        "input_artifacts",
        "job_id",
        "node_execution_id",
        "node_id",
        "node_type",
        "preferred_hosts",
    ]
    assert json.loads(task["config"])["output_format"] == "mp4"
    assert json.loads(task["input_artifacts"])["input"] == "00000000-0000-0000-0000-000000000301"
