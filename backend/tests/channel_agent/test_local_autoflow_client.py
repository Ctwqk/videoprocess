from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.channel_agent.clients import LocalAutoFlowClient
from app.models.artifact import Artifact, ArtifactKind
from app.models.autoflow import AutoFlowPlan as AutoFlowPlanModel
from app.models.autoflow import AutoFlowRun as AutoFlowRunModel
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.pipeline import Pipeline  # noqa: F401


async def _create_local_autoflow_tables(conn):
    await conn.exec_driver_sql("CREATE TABLE pipelines (id CHAR(32) PRIMARY KEY)")
    await conn.run_sync(Job.__table__.create)
    await conn.run_sync(AutoFlowPlanModel.__table__.create)
    await conn.run_sync(AutoFlowRunModel.__table__.create)
    await conn.run_sync(NodeExecution.__table__.create)
    await conn.run_sync(Artifact.__table__.create)


def _autoflow_plan(**overrides) -> AutoFlowPlanModel:
    data = {
        "prompt": "make a test short",
        "request_json": {"prompt": "make a test short"},
        "intent_json": {"intent_type": "generic"},
        "template_id": "test_template",
        "pipeline_definition": {"nodes": [], "edges": []},
        "candidates_json": [],
        "metadata_json": {},
        "rights_json": {"status": "allowed"},
        "validation_json": {"valid": True},
        "status": "executed",
    }
    data.update(overrides)
    return AutoFlowPlanModel(**data)


@pytest.mark.asyncio
async def test_local_autoflow_client_reads_youtube_video_id_from_artifact_media_info():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await _create_local_autoflow_tables(conn)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as db:
        plan = _autoflow_plan()
        db.add(plan)
        await db.flush()
        job = Job(pipeline_id=uuid.uuid4(), pipeline_snapshot={}, status=JobStatus.SUCCEEDED)
        db.add(job)
        await db.flush()
        run = AutoFlowRunModel(
            plan_id=plan.id,
            pipeline_id=job.pipeline_id,
            job_id=job.id,
            status="SUCCEEDED",
            artifacts_json={},
            publish_json={},
        )
        db.add(run)
        await db.flush()
        node = NodeExecution(
            job_id=job.id,
            node_id="youtube_upload_1",
            node_type="youtube_upload",
            node_config={},
            status=NodeStatus.SUCCEEDED,
        )
        db.add(node)
        await db.flush()
        artifact = Artifact(
            job_id=job.id,
            node_execution_id=node.id,
            kind=ArtifactKind.FINAL,
            filename="upload.mp4",
            storage_backend="local",
            storage_path="/tmp/upload.mp4",
            media_info={"youtube": {"video_id": "yt-local-1"}},
        )
        db.add(artifact)
        await db.flush()
        node.output_artifact_id = artifact.id
        await db.commit()

        observation = await LocalAutoFlowClient().observe_job(db, run_id=str(run.id), job_id=str(job.id))

    await engine.dispose()
    assert observation.status == "succeeded"
    assert observation.pipeline_id == str(run.pipeline_id)
    assert observation.youtube["video_id"] == "yt-local-1"


@pytest.mark.asyncio
async def test_local_autoflow_client_rejects_run_job_mismatch():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await _create_local_autoflow_tables(conn)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as db:
        plan = _autoflow_plan()
        db.add(plan)
        await db.flush()
        expected_job = Job(pipeline_id=uuid.uuid4(), pipeline_snapshot={}, status=JobStatus.RUNNING)
        other_job = Job(pipeline_id=uuid.uuid4(), pipeline_snapshot={}, status=JobStatus.RUNNING)
        db.add_all([expected_job, other_job])
        await db.flush()
        run = AutoFlowRunModel(
            plan_id=plan.id,
            pipeline_id=expected_job.pipeline_id,
            job_id=expected_job.id,
            status="RUNNING",
            artifacts_json={},
            publish_json={},
        )
        db.add(run)
        await db.commit()

        observation = await LocalAutoFlowClient().observe_job(db, run_id=str(run.id), job_id=str(other_job.id))

    await engine.dispose()
    assert observation.status == "failed"
    assert "mismatch" in observation.error_message


@pytest.mark.asyncio
async def test_local_autoflow_client_rejects_unlinked_run_with_supplied_job():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await _create_local_autoflow_tables(conn)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as db:
        plan = _autoflow_plan()
        db.add(plan)
        await db.flush()
        job = Job(pipeline_id=uuid.uuid4(), pipeline_snapshot={}, status=JobStatus.SUCCEEDED)
        db.add(job)
        await db.flush()
        run = AutoFlowRunModel(
            plan_id=plan.id,
            pipeline_id=None,
            job_id=None,
            status="SUCCEEDED",
            artifacts_json={},
            publish_json={},
        )
        db.add(run)
        await db.flush()
        node = NodeExecution(
            job_id=job.id,
            node_id="youtube_upload_1",
            node_type="youtube_upload",
            node_config={},
            status=NodeStatus.SUCCEEDED,
        )
        db.add(node)
        await db.flush()
        artifact = Artifact(
            job_id=job.id,
            node_execution_id=node.id,
            kind=ArtifactKind.FINAL,
            filename="upload.mp4",
            storage_backend="local",
            storage_path="/tmp/upload.mp4",
            media_info={"youtube": {"video_id": "yt-wrong-job"}},
        )
        db.add(artifact)
        await db.flush()
        node.output_artifact_id = artifact.id
        await db.commit()

        observation = await LocalAutoFlowClient().observe_job(db, run_id=str(run.id), job_id=str(job.id))

    await engine.dispose()
    assert observation.status == "failed"
    assert "mismatch" in observation.error_message
    assert "no linked job" in observation.error_message


@pytest.mark.asyncio
async def test_local_autoflow_client_execute_task_returns_failed_for_expected_refusal(monkeypatch):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    from app.autoflow.service import autoflow_service

    async def refuse_execute(request, db):
        raise PermissionError("public publish requires review approval")

    monkeypatch.setattr(autoflow_service, "execute", refuse_execute)
    task = SimpleNamespace(autoflow_plan_id=uuid.uuid4())

    observation = await LocalAutoFlowClient(session_factory=session_factory).execute_task(task, {})

    await engine.dispose()
    assert observation.status == "failed"
    assert observation.run_id == ""
    assert "review approval" in observation.error_message
