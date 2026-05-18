from __future__ import annotations

import json
import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import selectinload

from app.models.artifact import Artifact, ArtifactKind, IntermediateArtifactCache
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.orchestrator.artifact_cache import IntermediateArtifactCacheService
from app.orchestrator.engine import JobEngine


class FakeRedis:
    def __init__(self) -> None:
        self.tasks: list[tuple[str, dict]] = []

    async def xadd(self, stream_key: str, task: dict) -> None:
        self.tasks.append((stream_key, task))

    async def aclose(self) -> None:
        return None


@pytest.fixture
async def engine_cache_db_session():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        json_serializer=lambda value: json.dumps(value, default=str),
    )
    async with engine.begin() as conn:
        for table in (
            Job.__table__,
            NodeExecution.__table__,
            Artifact.__table__,
            IntermediateArtifactCache.__table__,
        ):
            await conn.run_sync(table.create)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session
    await engine.dispose()


def _definition(node_type: str = "trim") -> dict:
    return {
        "nodes": [
            {
                "id": "source_1",
                "type": "source",
                "position": {"x": 0, "y": 0},
                "data": {"label": "Source", "config": {"asset_id": str(uuid.uuid4())}},
            },
            {
                "id": "node_1",
                "type": node_type,
                "position": {"x": 100, "y": 0},
                "data": {"label": "Node", "config": {"duration": 5}},
            },
        ],
        "edges": [
            {
                "id": "e-source-node",
                "source": "source_1",
                "target": "node_1",
                "sourceHandle": "output",
                "targetHandle": "input",
            }
        ],
    }


def _artifact(job_id: uuid.UUID, node_execution_id: uuid.UUID, storage_path: str) -> Artifact:
    return Artifact(
        job_id=job_id,
        node_execution_id=node_execution_id,
        kind=ArtifactKind.INTERMEDIATE,
        filename=storage_path.rsplit("/", 1)[-1],
        mime_type="video/mp4",
        file_size=123,
        storage_backend="local",
        storage_path=storage_path,
        media_info={"width": 1080, "height": 1920},
    )


async def _seed_job(db, *, node_type: str = "trim"):
    job = Job(
        pipeline_id=uuid.uuid4(),
        pipeline_snapshot=_definition(node_type),
        status=JobStatus.RUNNING,
        execution_plan={"dependencies": {"source_1": [], "node_1": ["source_1"]}},
    )
    db.add(job)
    await db.flush()
    source_ne = NodeExecution(
        job_id=job.id,
        node_id="source_1",
        node_type="source",
        node_label="Source",
        node_config={"asset_id": str(uuid.uuid4())},
        status=NodeStatus.SUCCEEDED,
        progress=100,
    )
    node_ne = NodeExecution(
        job_id=job.id,
        node_id="node_1",
        node_type=node_type,
        node_label="Node",
        node_config={"duration": 5},
        status=NodeStatus.PENDING,
    )
    db.add_all([source_ne, node_ne])
    await db.flush()
    input_artifact = _artifact(job.id, source_ne.id, "artifacts/input.mp4")
    db.add(input_artifact)
    await db.flush()
    source_ne.output_artifact_id = input_artifact.id
    await db.commit()
    job = (
        await db.execute(select(Job).where(Job.id == job.id).options(selectinload(Job.node_executions)))
    ).scalar_one()
    source_ne = next(ne for ne in job.node_executions if ne.node_id == "source_1")
    node_ne = next(ne for ne in job.node_executions if ne.node_id == "node_1")
    return job, source_ne, node_ne, input_artifact


@pytest.mark.asyncio
async def test_cache_hit_marks_node_succeeded_without_redis_dispatch(engine_cache_db_session, monkeypatch):
    fake_redis = FakeRedis()
    monkeypatch.setattr("app.orchestrator.engine._redis", lambda: fake_redis)
    job, _source_ne, _node_ne, input_artifact = await _seed_job(engine_cache_db_session)
    output_artifact = _artifact(job.id, _node_ne.id, "artifacts/cached-output.mp4")
    engine_cache_db_session.add(output_artifact)
    await engine_cache_db_session.flush()
    await IntermediateArtifactCacheService().store(
        engine_cache_db_session,
        node_type="trim",
        node_config={"duration": 5},
        input_artifacts={"input": input_artifact},
        output_artifact=output_artifact,
        node_id="node_1",
        job_id=job.id,
    )
    await engine_cache_db_session.commit()

    await JobEngine()._dispatch_ready_nodes(engine_cache_db_session, job, {"source_1": [], "node_1": ["source_1"]})

    refreshed = await engine_cache_db_session.get(NodeExecution, _node_ne.id)
    assert refreshed.status == NodeStatus.SUCCEEDED
    assert refreshed.output_artifact_id == output_artifact.id
    assert fake_redis.tasks == []


@pytest.mark.asyncio
async def test_cache_miss_dispatches_redis_task(engine_cache_db_session, monkeypatch):
    fake_redis = FakeRedis()
    monkeypatch.setattr("app.orchestrator.engine._redis", lambda: fake_redis)
    job, _source_ne, node_ne, _input_artifact = await _seed_job(engine_cache_db_session)

    await JobEngine()._dispatch_ready_nodes(engine_cache_db_session, job, {"source_1": [], "node_1": ["source_1"]})

    refreshed = await engine_cache_db_session.get(NodeExecution, node_ne.id)
    assert refreshed.status == NodeStatus.QUEUED
    assert len(fake_redis.tasks) == 1


@pytest.mark.asyncio
async def test_node_completion_writes_cache_for_allowlisted_node(engine_cache_db_session):
    job, _source_ne, node_ne, input_artifact = await _seed_job(engine_cache_db_session)
    output_artifact = _artifact(job.id, node_ne.id, "artifacts/output.mp4")
    engine_cache_db_session.add(output_artifact)
    await engine_cache_db_session.flush()
    node_ne.input_artifact_ids = [input_artifact.id]
    node_ne.output_artifact_id = output_artifact.id
    await engine_cache_db_session.commit()

    await JobEngine()._write_artifact_cache_for_node(engine_cache_db_session, job, node_ne)

    entries = (await engine_cache_db_session.execute(select(IntermediateArtifactCache))).scalars().all()
    assert len(entries) == 1
    assert entries[0].output_artifact_id == output_artifact.id


@pytest.mark.asyncio
async def test_non_allowlisted_node_completion_does_not_write_cache(engine_cache_db_session):
    job, _source_ne, node_ne, input_artifact = await _seed_job(engine_cache_db_session, node_type="youtube_upload")
    output_artifact = _artifact(job.id, node_ne.id, "artifacts/output.json")
    engine_cache_db_session.add(output_artifact)
    await engine_cache_db_session.flush()
    node_ne.input_artifact_ids = [input_artifact.id]
    node_ne.output_artifact_id = output_artifact.id
    await engine_cache_db_session.commit()

    await JobEngine()._write_artifact_cache_for_node(engine_cache_db_session, job, node_ne)

    entries = (await engine_cache_db_session.execute(select(IntermediateArtifactCache))).scalars().all()
    assert entries == []
