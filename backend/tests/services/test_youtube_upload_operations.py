from __future__ import annotations

import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models.artifact import Artifact
from app.models.channel_agent import ChannelProfile, ProductionTask
from app.models.job import Job, NodeExecution
from app.models.youtube_upload_operation import YouTubeUploadOperation
from app.services.youtube_upload_operations import (
    UploadOperationConflictError,
    UploadOperationContext,
    YouTubeUploadOperationStore,
)


@pytest.fixture
async def operation_session_factory():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        for table in (
            Job.__table__,
            NodeExecution.__table__,
            Artifact.__table__,
            ChannelProfile.__table__,
            ProductionTask.__table__,
            YouTubeUploadOperation.__table__,
        ):
            await conn.run_sync(table.create)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    yield session_factory
    await engine.dispose()


async def _context_for(
    db: AsyncSession,
    *,
    production_task: ProductionTask | None = None,
) -> UploadOperationContext:
    job = Job(pipeline_id=uuid.uuid4(), pipeline_snapshot={})
    db.add(job)
    await db.flush()

    node = NodeExecution(
        job_id=job.id,
        node_id=f"youtube_upload_{uuid.uuid4().hex}",
        node_type="youtube_upload",
    )
    db.add(node)
    await db.flush()

    artifact = Artifact(
        job_id=job.id,
        node_execution_id=node.id,
        filename="canary.mp4",
        storage_path="artifacts/canary.mp4",
    )
    db.add(artifact)
    await db.flush()

    if production_task is None:
        channel = ChannelProfile(name=f"canary-{uuid.uuid4()}")
        db.add(channel)
        await db.flush()
        production_task = ProductionTask(
            channel_profile_id=channel.id,
            target_account_id=uuid.uuid4(),
            prompt="Upload the owned canary video",
            job_id=job.id,
        )
        db.add(production_task)
    else:
        production_task.job_id = job.id
    await db.commit()

    return UploadOperationContext(
        job_id=job.id,
        node_execution_id=node.id,
        input_artifact_id=artifact.id,
        content_sha256="a" * 64,
        title="Owned canary",
        privacy="unlisted",
    )


@pytest.mark.asyncio
async def test_claim_reserves_once_then_resumes_and_replays(operation_session_factory):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)

    claim = await store.claim(context)
    assert claim.action == "submit"
    assert claim.operation.status == "reserved"

    again = await store.claim(context)
    assert again.action == "block"
    assert again.operation.id == claim.operation.id

    await store.mark_submitted(claim.operation.id, "manager-task-1")
    assert (await store.claim(context)).action == "resume"

    receipt = {
        "video_id": "abcdefghijk",
        "url": "https://youtu.be/abcdefghijk",
        "title": "Owned canary",
        "privacy": "unlisted",
        "tags": ["canary"],
        "quota_estimate": 1600,
        "access_token": "must-not-persist",
    }
    succeeded = await store.mark_succeeded(claim.operation.id, "abcdefghijk", receipt)
    assert succeeded.receipt_json == {
        "video_id": "abcdefghijk",
        "url": "https://youtu.be/abcdefghijk",
        "title": "Owned canary",
        "privacy": "unlisted",
        "tags": ["canary"],
        "quota_estimate": 1600,
    }
    assert (await store.claim(context)).action == "replay"


@pytest.mark.asyncio
async def test_existing_reserved_uncertain_and_failed_operations_never_submit(operation_session_factory):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        reserved_context = await _context_for(db)

    reserved = await store.claim(reserved_context)
    assert (await store.claim(reserved_context)).action == "block"

    await store.mark_uncertain(reserved.operation.id, "upload response was ambiguous")
    assert (await store.claim(reserved_context)).action == "block"

    async with operation_session_factory() as db:
        failed_context = await _context_for(db)
    failed = await store.claim(failed_context)
    await store.mark_failed(failed.operation.id, "manager rejected upload")
    assert (await store.claim(failed_context)).action == "block"


@pytest.mark.asyncio
async def test_claim_rejects_second_node_for_the_same_production_task(operation_session_factory):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    claim = await store.claim(context)

    async with operation_session_factory() as db:
        production_task = await db.get(ProductionTask, claim.operation.production_task_id)
        assert production_task is not None
        second_context = await _context_for(db, production_task=production_task)

    with pytest.raises(
        UploadOperationConflictError,
        match="production task already has a YouTube upload operation",
    ):
        await store.claim(second_context)


@pytest.mark.asyncio
async def test_mark_succeeded_rejects_duplicate_platform_video_id(operation_session_factory):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        first_context = await _context_for(db)
    async with operation_session_factory() as db:
        second_context = await _context_for(db)
    first = await store.claim(first_context)
    second = await store.claim(second_context)

    await store.mark_submitted(first.operation.id, "manager-task-1")
    await store.mark_succeeded(first.operation.id, "abcdefghijk", {"video_id": "abcdefghijk"})
    await store.mark_submitted(second.operation.id, "manager-task-2")

    with pytest.raises(
        UploadOperationConflictError,
        match="platform video id already belongs to a YouTube upload operation",
    ):
        await store.mark_succeeded(second.operation.id, "abcdefghijk", {"video_id": "abcdefghijk"})
