from __future__ import annotations

import uuid

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.channel_agent.clients import LocalAutoFlowClient
from app.models.artifact import Artifact, ArtifactKind
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.pipeline import Pipeline  # noqa: F401


@pytest.mark.asyncio
async def test_local_autoflow_client_reads_youtube_video_id_from_artifact_media_info():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.exec_driver_sql("CREATE TABLE pipelines (id CHAR(32) PRIMARY KEY)")
        await conn.run_sync(Job.__table__.create)
        await conn.run_sync(NodeExecution.__table__.create)
        await conn.run_sync(Artifact.__table__.create)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as db:
        job = Job(pipeline_id=uuid.uuid4(), pipeline_snapshot={}, status=JobStatus.SUCCEEDED)
        db.add(job)
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

        observation = await LocalAutoFlowClient().observe_job(db, run_id=str(uuid.uuid4()), job_id=str(job.id))

    await engine.dispose()
    assert observation.status == "succeeded"
    assert observation.youtube["video_id"] == "yt-local-1"
