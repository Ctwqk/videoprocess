from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.models.job import JobStatus, NodeStatus
from app.orchestrator.engine import JobEngine
from app.schemas.pipeline import PipelineDefinition
from app.services.job_execution_authority import NodeExecutionClaim


class _FakeRedis:
    def __init__(self) -> None:
        self.added: list[tuple[str, dict]] = []
        self.closed = False

    async def xadd(self, stream_key: str, task: dict) -> None:
        self.added.append((stream_key, task))

    async def aclose(self) -> None:
        self.closed = True


class _FakeSession:
    def __init__(self, job) -> None:
        self.job = job
        self.commits = 0
        self.rollbacks = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, _model, _id, options=None):
        return self.job

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


def _retry_pipeline_snapshot() -> dict:
    definition = PipelineDefinition.model_validate(
        {
            "nodes": [
                {
                    "id": "src",
                    "type": "source",
                    "position": {"x": 0, "y": 0},
                    "data": {
                        "label": "Source",
                        "config": {
                            "asset_id": "00000000-0000-0000-0000-000000000001",
                            "media_type": "video",
                        },
                    },
                },
                {
                    "id": "trim",
                    "type": "trim",
                    "position": {"x": 260, "y": 0},
                    "data": {
                        "label": "Trim",
                        "config": {"start_time": "00:00:00", "duration": "1"},
                    },
                },
            ],
            "edges": [
                {
                    "id": "e-src-trim",
                    "source": "src",
                    "target": "trim",
                    "sourceHandle": "output",
                    "targetHandle": "input",
                }
            ],
            "viewport": {"x": 0, "y": 0, "zoom": 1},
        }
    )
    return definition.model_dump()


def test_preferred_hosts_for_node_ranks_upstream_hosts() -> None:
    upstream_a = SimpleNamespace(worker_id="worker@mac-mini-1:worker-a")
    upstream_b = SimpleNamespace(worker_id="worker@mac-mini-2:worker-b")
    upstream_c = SimpleNamespace(worker_id="worker@mac-mini-1:worker-c")

    preferred = JobEngine._preferred_hosts_for_node(
        {
            "src_a": upstream_a,
            "src_b": upstream_b,
            "src_c": upstream_c,
        },
        ["src_a", "src_b", "src_c"],
    )

    assert preferred == ["mac-mini-1"]


@pytest.mark.asyncio
async def test_on_node_failed_retry_redispatches_with_dependency_preferred_hosts(monkeypatch) -> None:
    job_id = uuid.uuid4()
    source_output_artifact_id = uuid.uuid4()
    failed_node_execution_id = uuid.uuid4()
    started_at = datetime(2026, 7, 22, 12, 30, tzinfo=timezone.utc)
    worker_id = "ffmpeg-worker@vp-gpu:42"
    retry_node = SimpleNamespace(
        id=failed_node_execution_id,
        node_id="trim",
        node_type="trim",
        node_label="Trim",
        node_config={"start_time": "00:00:00", "duration": "1"},
        status=NodeStatus.RUNNING,
        retry_count=0,
        error_message="previous failure",
        queued_at=None,
        worker_id=worker_id,
        started_at=started_at,
    )
    source_node = SimpleNamespace(
        id=uuid.uuid4(),
        node_id="src",
        node_type="source",
        node_label="Source",
        node_config={},
        status=NodeStatus.SUCCEEDED,
        retry_count=0,
        worker_id="worker@preferred-host:source-worker",
        output_artifact_id=source_output_artifact_id,
    )
    job = SimpleNamespace(
        id=job_id,
        status=JobStatus.RUNNING,
        node_executions=[source_node, retry_node],
        execution_plan={"dependencies": {"src": [], "trim": ["src"]}},
        pipeline_snapshot=_retry_pipeline_snapshot(),
    )
    fake_session = _FakeSession(job)
    fake_redis = _FakeRedis()

    from app.orchestrator import engine as engine_module

    async def lock_authority(*args, **kwargs):
        return SimpleNamespace(
            channel=None,
            schedule=SimpleNamespace(state="OPEN", guarded_job_id=None),
            task=None,
            job=job,
            node=retry_node,
        )

    monkeypatch.setattr(engine_module, "async_session", lambda: fake_session)
    monkeypatch.setattr(engine_module, "_redis", lambda: fake_redis)
    monkeypatch.setattr(engine_module, "lock_job_execution_authority", lock_authority)

    await JobEngine().on_node_failed(
        job_id,
        failed_node_execution_id,
        "boom",
        claim=NodeExecutionClaim(
            job_id=job_id,
            node_execution_id=failed_node_execution_id,
            worker_id=worker_id,
            started_at=started_at,
        ),
    )

    assert retry_node.retry_count == 1
    assert retry_node.status == NodeStatus.QUEUED
    assert retry_node.error_message is None
    assert fake_session.commits == 2
    assert fake_redis.closed
    assert len(fake_redis.added) == 1

    stream_key, task = fake_redis.added[0]
    assert stream_key == "vp:tasks:ffmpeg_go"
    assert task["node_id"] == "trim"
    assert json.loads(task["input_artifacts"]) == {"input": str(source_output_artifact_id)}
    assert json.loads(task["preferred_hosts"]) == ["preferred-host"]


@pytest.mark.asyncio
async def test_on_node_failed_ignores_delayed_event_from_replaced_claim(monkeypatch) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    old_started_at = datetime(2026, 7, 22, 12, 30, tzinfo=timezone.utc)
    replacement_started_at = old_started_at + timedelta(minutes=5)
    replacement_node = SimpleNamespace(
        id=node_execution_id,
        node_id="trim",
        node_type="trim",
        node_label="Trim",
        node_config={"start_time": "00:00:00", "duration": "1"},
        status=NodeStatus.RUNNING,
        retry_count=0,
        error_message=None,
        worker_id="ffmpeg-worker@replacement:84",
        started_at=replacement_started_at,
    )
    job = SimpleNamespace(
        id=job_id,
        status=JobStatus.RUNNING,
        node_executions=[replacement_node],
        execution_plan={"dependencies": {"trim": []}},
        pipeline_snapshot=_retry_pipeline_snapshot(),
    )
    fake_session = _FakeSession(job)
    fake_redis = _FakeRedis()

    from app.orchestrator import engine as engine_module

    async def lock_authority(*args, **kwargs):
        return SimpleNamespace(
            channel=None,
            schedule=SimpleNamespace(state="OPEN", guarded_job_id=None),
            task=None,
            job=job,
            node=replacement_node,
        )

    monkeypatch.setattr(engine_module, "async_session", lambda: fake_session)
    monkeypatch.setattr(engine_module, "_redis", lambda: fake_redis)
    monkeypatch.setattr(engine_module, "lock_job_execution_authority", lock_authority)

    await JobEngine().on_node_failed(
        job_id,
        node_execution_id,
        "old worker failed",
        claim=NodeExecutionClaim(
            job_id=job_id,
            node_execution_id=node_execution_id,
            worker_id="ffmpeg-worker@old:21",
            started_at=old_started_at,
        ),
    )

    assert replacement_node.status == NodeStatus.RUNNING
    assert replacement_node.retry_count == 0
    assert fake_session.commits == 0
    assert fake_session.rollbacks == 1
    assert fake_redis.added == []


@pytest.mark.asyncio
async def test_on_node_completed_ignores_delayed_event_from_replaced_claim(
    monkeypatch,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    output_artifact_id = uuid.uuid4()
    old_started_at = datetime(2026, 7, 22, 12, 30, tzinfo=timezone.utc)
    replacement_node = SimpleNamespace(
        id=node_execution_id,
        node_id="trim",
        status=NodeStatus.RUNNING,
        retry_count=0,
        output_artifact_id=None,
        worker_id="ffmpeg-worker@replacement:84",
        started_at=old_started_at + timedelta(minutes=5),
    )
    job = SimpleNamespace(
        id=job_id,
        status=JobStatus.RUNNING,
        node_executions=[replacement_node],
    )
    fake_session = _FakeSession(job)

    from app.orchestrator import engine as engine_module

    async def lock_authority(*args, **kwargs):
        return SimpleNamespace(
            channel=None,
            schedule=SimpleNamespace(state="OPEN", guarded_job_id=None),
            task=None,
            job=job,
            node=replacement_node,
        )

    monkeypatch.setattr(engine_module, "async_session", lambda: fake_session)
    monkeypatch.setattr(engine_module, "lock_job_execution_authority", lock_authority)

    await JobEngine().on_node_completed(
        job_id,
        node_execution_id,
        output_artifact_id,
        claim=NodeExecutionClaim(
            job_id=job_id,
            node_execution_id=node_execution_id,
            worker_id="ffmpeg-worker@old:21",
            started_at=old_started_at,
        ),
    )

    assert replacement_node.status == NodeStatus.RUNNING
    assert replacement_node.output_artifact_id is None
    assert fake_session.commits == 0
    assert fake_session.rollbacks == 1
