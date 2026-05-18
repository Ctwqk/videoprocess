from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest

from app.models.artifact import Artifact, ArtifactKind
from app.models.job import JobStatus, NodeStatus
from app.orchestrator.engine import JobEngine
from app.schemas.pipeline import PipelineDefinition


class _FakeDb:
    def __init__(self, artifacts: dict[uuid.UUID, SimpleNamespace]) -> None:
        self.artifacts = artifacts
        self.commits = 0

    async def commit(self) -> None:
        self.commits += 1

    async def get(self, model, object_id):
        if model is Artifact:
            return self.artifacts.get(object_id)
        return None


def _definition(edges: list[tuple[str, str]]) -> dict:
    node_ids = sorted({node_id for edge in edges for node_id in edge} | {"src", "leaf"})
    definition = PipelineDefinition.model_validate(
        {
            "nodes": [
                {
                    "id": node_id,
                    "type": "trim" if node_id != "src" else "source",
                    "position": {"x": 0, "y": 0},
                    "data": {"label": node_id, "config": {"asset_id": "asset-1"} if node_id == "src" else {}},
                }
                for node_id in node_ids
            ],
            "edges": [
                {
                    "id": f"e-{source}-{target}",
                    "source": source,
                    "target": target,
                    "sourceHandle": "output",
                    "targetHandle": "input",
                }
                for source, target in edges
            ],
        }
    )
    return definition.model_dump()


def _node(node_id: str, status: NodeStatus, output_artifact_id: uuid.UUID | None = None):
    return SimpleNamespace(
        node_id=node_id,
        node_label=node_id,
        status=status,
        output_artifact_id=output_artifact_id,
    )


@pytest.mark.asyncio
async def test_failed_leaf_makes_job_failed_and_does_not_mark_final_artifact():
    leaf_artifact_id = uuid.uuid4()
    artifact = SimpleNamespace(kind=ArtifactKind.INTERMEDIATE)
    db = _FakeDb({leaf_artifact_id: artifact})
    job = SimpleNamespace(
        id=uuid.uuid4(),
        status=JobStatus.RUNNING,
        error_message=None,
        completed_at=None,
        pipeline_snapshot=_definition([("src", "leaf")]),
        node_executions=[
            _node("src", NodeStatus.SUCCEEDED, uuid.uuid4()),
            _node("leaf", NodeStatus.FAILED, leaf_artifact_id),
        ],
    )

    finalized = await JobEngine()._maybe_finalize_job(db, job)

    assert finalized is True
    assert job.status == JobStatus.FAILED
    assert artifact.kind == ArtifactKind.INTERMEDIATE


@pytest.mark.asyncio
async def test_only_successful_leaf_artifacts_are_marked_final_for_partial_jobs():
    leaf_artifact_id = uuid.uuid4()
    failed_non_leaf_artifact_id = uuid.uuid4()
    leaf_artifact = SimpleNamespace(kind=ArtifactKind.INTERMEDIATE)
    failed_non_leaf_artifact = SimpleNamespace(kind=ArtifactKind.INTERMEDIATE)
    db = _FakeDb(
        {
            leaf_artifact_id: leaf_artifact,
            failed_non_leaf_artifact_id: failed_non_leaf_artifact,
        }
    )
    job = SimpleNamespace(
        id=uuid.uuid4(),
        status=JobStatus.RUNNING,
        error_message=None,
        completed_at=None,
        pipeline_snapshot=_definition([("src", "failed_non_leaf"), ("failed_non_leaf", "leaf")]),
        node_executions=[
            _node("src", NodeStatus.SUCCEEDED, uuid.uuid4()),
            _node("failed_non_leaf", NodeStatus.FAILED, failed_non_leaf_artifact_id),
            _node("leaf", NodeStatus.SUCCEEDED, leaf_artifact_id),
        ],
    )

    finalized = await JobEngine()._maybe_finalize_job(db, job)

    assert finalized is True
    assert job.status == JobStatus.PARTIALLY_FAILED
    assert leaf_artifact.kind == ArtifactKind.FINAL
    assert failed_non_leaf_artifact.kind == ArtifactKind.INTERMEDIATE


@pytest.mark.asyncio
async def test_skipped_leaf_makes_job_failed():
    db = _FakeDb({})
    job = SimpleNamespace(
        id=uuid.uuid4(),
        status=JobStatus.RUNNING,
        error_message=None,
        completed_at=None,
        pipeline_snapshot=_definition([("src", "leaf")]),
        node_executions=[
            _node("src", NodeStatus.SUCCEEDED, uuid.uuid4()),
            _node("leaf", NodeStatus.SKIPPED, None),
        ],
    )

    finalized = await JobEngine()._maybe_finalize_job(db, job)

    assert finalized is True
    assert job.status == JobStatus.FAILED
