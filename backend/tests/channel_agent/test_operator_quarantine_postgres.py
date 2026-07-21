from __future__ import annotations

import asyncio
import os
import uuid

import pytest
from fastapi import HTTPException
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import selectinload

from app.api.channel_agent import HumanReviewRequest, promote_publication, release_human_review
from app.models.autoflow import AutoFlowPlan
from app.models.artifact import Artifact
from app.models.channel_agent import (
    ChannelOpsQueueItem,
    ChannelProfile,
    ProductionTask,
    PublicationRecord,
    PublishingAccount,
)
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.pipeline import Pipeline
from app.models.schedule import RuntimeSchedule
from app.orchestrator.engine import JobEngine
from app.services.channelops_quarantine import QUARANTINE_REASON, quarantine_channelops_backlog
from app.services.job_execution_authority import JobExecutionAuthorityBlocked
from app.services.schedule_service import VIDEO_SCHEDULE_SERVICE, VideoScheduleState
from app.services.youtube_upload_operations import (
    UploadOperationContext,
    YouTubeUploadOperationStore,
)
from worker import main as worker_main


POSTGRES_URL = os.getenv("CHANNEL_OPS_POSTGRES_TEST_URL", "")


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(not POSTGRES_URL, reason="set CHANNEL_OPS_POSTGRES_TEST_URL for PostgreSQL race tests"),
]


def _review_plan() -> AutoFlowPlan:
    return AutoFlowPlan(
        prompt="Review this external asset plan",
        request_json={
            "prompt": "Review this external asset plan",
            "target_platforms": ["youtube_shorts"],
            "duration_sec": 30,
            "aspect_ratio": "9:16",
            "source_policy": "remix_with_review",
            "publish_mode": "private_upload",
            "material_library_ids": [],
            "user_constraints": {},
        },
        intent_json={
            "intent_type": "generic_video",
            "subject": "external review",
            "style": "documentary",
            "duration_sec": 30,
            "aspect_ratio": "9:16",
            "target_platforms": ["youtube_shorts"],
            "source_policy": "remix_with_review",
            "publish_mode": "private_upload",
            "keywords": [],
            "negative_keywords": [],
            "needs_voiceover": False,
            "needs_subtitles": True,
            "needs_bgm": False,
            "user_confirmation_questions": [],
        },
        template_id="material_library_remix",
        pipeline_definition={"nodes": [], "edges": [], "viewport": {"x": 0, "y": 0, "zoom": 1}},
        candidates_json=[],
        metadata_json={},
        rights_json={
            "status": "review_required",
            "reasons": ["human review required"],
            "allowed_publish_modes": ["private_upload", "unlisted_upload"],
            "execute_allowed": True,
            "publish_allowed": True,
        },
        validation_json={"valid": True, "errors": [], "warnings": [], "repairs": []},
        status="review_required",
    )


@pytest.fixture
async def postgres_race_db():
    engine = create_async_engine(POSTGRES_URL, pool_size=8, max_overflow=0)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "TRUNCATE channel_ops_queue_items, publication_records, production_tasks, "
                "publishing_accounts, autoflow_runs, autoflow_plans, node_executions, jobs, "
                "pipelines, runtime_schedules RESTART IDENTITY CASCADE"
            )
        )
    try:
        yield engine, factory
    finally:
        await engine.dispose()


async def _wait_until_lock_wait(engine, query_fragment: str, operation: asyncio.Task) -> None:
    deadline = asyncio.get_running_loop().time() + 5
    while asyncio.get_running_loop().time() < deadline:
        if operation.done():
            result = await operation
            pytest.fail(
                f"operation completed before reaching the expected PostgreSQL lock for {query_fragment}: {result!r}"
            )
        async with engine.connect() as conn:
            waiting = await conn.scalar(
                text(
                    "SELECT EXISTS ("
                    "SELECT 1 FROM pg_stat_activity "
                    "WHERE wait_event_type = 'Lock' AND query ILIKE '%' || :fragment || '%'"
                    ")"
                ),
                {"fragment": query_fragment},
            )
        if waiting:
            return
        await asyncio.sleep(0.01)
    pytest.fail(f"operation did not reach the expected PostgreSQL lock for {query_fragment}")


async def _cancel_operations(blocker: AsyncSession, *operations: asyncio.Task) -> None:
    await blocker.rollback()
    for operation in operations:
        if not operation.done():
            operation.cancel()
    await asyncio.gather(*operations, return_exceptions=True)


async def _seed_worker_authority(factory, *, node_status: NodeStatus, job_status: JobStatus):
    definition = {
        "nodes": [
            {
                "id": "trim_1",
                "type": "trim",
                "position": {"x": 0, "y": 0},
                "data": {"label": "Trim", "config": {"start_time": 0, "end_time": 1}},
            }
        ],
        "edges": [],
        "viewport": {"x": 0, "y": 0, "zoom": 1},
    }
    async with factory() as db:
        channel = ChannelProfile(name="worker authority", enabled=True, dry_run=False)
        schedule = RuntimeSchedule(
            service_name=VIDEO_SCHEDULE_SERVICE,
            state=VideoScheduleState.OPEN.value,
            updated_by="worker-authority-test",
        )
        pipeline = Pipeline(name="worker authority", definition=definition)
        db.add_all([channel, schedule, pipeline])
        await db.flush()
        job = Job(
            pipeline_id=pipeline.id,
            pipeline_snapshot=definition,
            status=job_status,
            execution_plan={"topo_order": ["trim_1"], "dependencies": {"trim_1": []}},
        )
        db.add(job)
        await db.flush()
        node = NodeExecution(
            job_id=job.id,
            node_id="trim_1",
            node_type="trim",
            node_label="Trim",
            node_config={"start_time": 0, "end_time": 1},
            status=node_status,
        )
        task = ProductionTask(
            channel_profile_id=channel.id,
            target_account_id=uuid.uuid4(),
            prompt="worker authority",
            job_id=job.id,
            state="producing",
            channel_config_snapshot_json={},
        )
        db.add_all([node, task])
        await db.commit()
        return channel.id, task.id, job.id, node.id


async def test_worker_claim_cannot_revive_quarantine_first_node(postgres_race_db):
    engine, factory = postgres_race_db
    channel_id, task_id, job_id, node_id = await _seed_worker_authority(
        factory,
        node_status=NodeStatus.QUEUED,
        job_status=JobStatus.RUNNING,
    )

    blocker = factory()
    claim = None
    try:
        channel = (
            await blocker.execute(
                select(ChannelProfile).where(ChannelProfile.id == channel_id).with_for_update()
            )
        ).scalar_one()
        schedule = (
            await blocker.execute(
                select(RuntimeSchedule)
                .where(RuntimeSchedule.service_name == VIDEO_SCHEDULE_SERVICE)
                .with_for_update()
            )
        ).scalar_one()
        task = (
            await blocker.execute(
                select(ProductionTask).where(ProductionTask.id == task_id).with_for_update()
            )
        ).scalar_one()
        job = (await blocker.execute(select(Job).where(Job.id == job_id).with_for_update())).scalar_one()
        node = (
            await blocker.execute(
                select(NodeExecution).where(NodeExecution.id == node_id).with_for_update()
            )
        ).scalar_one()
        channel.halted_at = channel.created_at
        channel.halt_reason = QUARANTINE_REASON
        schedule.state = VideoScheduleState.CLOSED.value
        task.state = "held"
        task.blocked_by_guard = QUARANTINE_REASON
        job.status = JobStatus.CANCELLED
        node.status = NodeStatus.CANCELLED
        await blocker.flush()

        claim = asyncio.create_task(
            worker_main._claim_node_execution(
                str(job_id),
                str(node_id),
                session_factory=factory,
            )
        )
        await _wait_until_lock_wait(engine, "channel_profiles", claim)
        await blocker.commit()
        assert await asyncio.wait_for(claim, timeout=5) is False
    finally:
        if blocker.in_transaction():
            await blocker.rollback()
        await blocker.close()
        if claim is not None and not claim.done():
            claim.cancel()
        if claim is not None:
            await asyncio.wait_for(asyncio.gather(claim, return_exceptions=True), timeout=5)

    async with factory() as db:
        stored = await db.get(NodeExecution, node_id)
    assert stored is not None and stored.status == NodeStatus.CANCELLED


async def test_repeated_quarantine_repairs_active_node_under_cancelled_job(postgres_race_db):
    _engine, factory = postgres_race_db
    channel_id, task_id, job_id, node_id = await _seed_worker_authority(
        factory,
        node_status=NodeStatus.RUNNING,
        job_status=JobStatus.CANCELLED,
    )
    async with factory() as db:
        channel = await db.get(ChannelProfile, channel_id)
        task = await db.get(ProductionTask, task_id)
        assert channel is not None and task is not None
        channel.halted_at = channel.created_at
        channel.halt_reason = QUARANTINE_REASON
        task.state = "held"
        task.blocked_by_guard = QUARANTINE_REASON
        task.failure_reason = QUARANTINE_REASON
        await db.commit()

    async with factory() as db:
        result = await quarantine_channelops_backlog(
            db,
            channel_id,
            apply=True,
            close_schedule=True,
        )

    assert result["changed_ids"]["job_ids"] == []
    assert result["changed_ids"]["node_execution_ids"] == [str(node_id)]
    async with factory() as db:
        stored_job = await db.get(Job, job_id)
        stored_node = await db.get(NodeExecution, node_id)
    assert stored_job is not None and stored_job.status == JobStatus.CANCELLED
    assert stored_node is not None and stored_node.status == NodeStatus.CANCELLED


async def _stage_worker_quarantine(
    blocker: AsyncSession,
    *,
    channel_id,
    task_id,
    job_id,
    node_id,
) -> None:
    channel = (
        await blocker.execute(
            select(ChannelProfile).where(ChannelProfile.id == channel_id).with_for_update()
        )
    ).scalar_one()
    schedule = (
        await blocker.execute(
            select(RuntimeSchedule)
            .where(RuntimeSchedule.service_name == VIDEO_SCHEDULE_SERVICE)
            .with_for_update()
        )
    ).scalar_one()
    task = (
        await blocker.execute(
            select(ProductionTask).where(ProductionTask.id == task_id).with_for_update()
        )
    ).scalar_one()
    job = (await blocker.execute(select(Job).where(Job.id == job_id).with_for_update())).scalar_one()
    node = (
        await blocker.execute(
            select(NodeExecution).where(NodeExecution.id == node_id).with_for_update()
        )
    ).scalar_one()
    channel.halted_at = channel.created_at
    channel.halt_reason = QUARANTINE_REASON
    schedule.state = VideoScheduleState.CLOSED.value
    task.state = "held"
    task.blocked_by_guard = QUARANTINE_REASON
    task.failure_reason = QUARANTINE_REASON
    job.status = JobStatus.CANCELLED
    node.status = NodeStatus.CANCELLED
    await blocker.flush()


async def test_quarantine_first_blocks_stale_worker_completion(postgres_race_db, monkeypatch):
    engine, factory = postgres_race_db
    channel_id, task_id, job_id, node_id = await _seed_worker_authority(
        factory,
        node_status=NodeStatus.RUNNING,
        job_status=JobStatus.RUNNING,
    )
    async with factory() as db:
        artifact = Artifact(
            job_id=job_id,
            node_execution_id=node_id,
            filename="completed.mp4",
            storage_path="artifacts/completed.mp4",
        )
        db.add(artifact)
        await db.commit()
        artifact_id = artifact.id

    dispatches: list[uuid.UUID] = []
    job_engine = JobEngine()

    async def no_cache(*args, **kwargs):
        return None

    async def record_dispatch(_db, job, _dep_map, **kwargs):
        dispatches.append(job.id)

    monkeypatch.setattr("app.orchestrator.engine.async_session", factory)
    monkeypatch.setattr(job_engine, "_write_artifact_cache_for_node", no_cache)
    monkeypatch.setattr(job_engine, "_dispatch_ready_nodes", record_dispatch)

    blocker = factory()
    completion = None
    try:
        await _stage_worker_quarantine(
            blocker,
            channel_id=channel_id,
            task_id=task_id,
            job_id=job_id,
            node_id=node_id,
        )
        completion = asyncio.create_task(job_engine.on_node_completed(job_id, node_id, artifact_id))
        await _wait_until_lock_wait(engine, "", completion)
        await blocker.commit()
        await asyncio.wait_for(completion, timeout=5)
    finally:
        if blocker.in_transaction():
            await blocker.rollback()
        await blocker.close()
        if completion is not None and not completion.done():
            completion.cancel()
        if completion is not None:
            await asyncio.wait_for(asyncio.gather(completion, return_exceptions=True), timeout=5)

    async with factory() as db:
        stored_job = await db.get(Job, job_id)
        stored_node = await db.get(NodeExecution, node_id)
    assert stored_job is not None and stored_job.status == JobStatus.CANCELLED
    assert stored_node is not None and stored_node.status == NodeStatus.CANCELLED
    assert stored_node.output_artifact_id is None
    assert dispatches == []


async def test_quarantine_first_blocks_stale_worker_retry(postgres_race_db, monkeypatch):
    engine, factory = postgres_race_db
    channel_id, task_id, job_id, node_id = await _seed_worker_authority(
        factory,
        node_status=NodeStatus.RUNNING,
        job_status=JobStatus.RUNNING,
    )
    redis_dispatches: list[tuple[str, dict]] = []

    class RecordingRedis:
        async def xadd(self, stream_key, payload):
            redis_dispatches.append((stream_key, payload))

        async def aclose(self):
            return None

    monkeypatch.setattr("app.orchestrator.engine.async_session", factory)
    monkeypatch.setattr("app.orchestrator.engine._redis", lambda: RecordingRedis())
    job_engine = JobEngine()

    blocker = factory()
    failure = None
    try:
        await _stage_worker_quarantine(
            blocker,
            channel_id=channel_id,
            task_id=task_id,
            job_id=job_id,
            node_id=node_id,
        )
        failure = asyncio.create_task(job_engine.on_node_failed(job_id, node_id, "late failure"))
        await _wait_until_lock_wait(engine, "", failure)
        await blocker.commit()
        await asyncio.wait_for(failure, timeout=5)
    finally:
        if blocker.in_transaction():
            await blocker.rollback()
        await blocker.close()
        if failure is not None and not failure.done():
            failure.cancel()
        if failure is not None:
            await asyncio.wait_for(asyncio.gather(failure, return_exceptions=True), timeout=5)

    async with factory() as db:
        stored_job = await db.get(Job, job_id)
        stored_node = await db.get(NodeExecution, node_id)
    assert stored_job is not None and stored_job.status == JobStatus.CANCELLED
    assert stored_node is not None and stored_node.status == NodeStatus.CANCELLED
    assert stored_node.retry_count == 0
    assert redis_dispatches == []


async def test_quarantine_between_queue_commit_and_downstream_dispatch_blocks_redis(
    postgres_race_db,
    monkeypatch,
):
    _engine, factory = postgres_race_db
    definition = {
        "nodes": [
            {
                "id": "source_1",
                "type": "source",
                "position": {"x": 0, "y": 0},
                "data": {"label": "Source", "config": {"asset_id": str(uuid.uuid4())}},
            },
            {
                "id": "trim_1",
                "type": "trim",
                "position": {"x": 200, "y": 0},
                "data": {"label": "Trim", "config": {"start_time": 0, "end_time": 1}},
            },
        ],
        "edges": [
            {
                "id": "source-to-trim",
                "source": "source_1",
                "target": "trim_1",
                "sourceHandle": "output",
                "targetHandle": "input",
            }
        ],
    }
    async with factory() as db:
        channel = ChannelProfile(name="downstream dispatch race", enabled=True, dry_run=False)
        schedule = RuntimeSchedule(
            service_name=VIDEO_SCHEDULE_SERVICE,
            state=VideoScheduleState.OPEN.value,
            updated_by="downstream-dispatch-race-test",
        )
        pipeline = Pipeline(name="downstream dispatch race", description="", definition=definition)
        db.add_all([channel, schedule, pipeline])
        await db.flush()
        account = PublishingAccount(
            channel_profile_id=channel.id,
            account_label="downstream dispatch race",
            credential_ref="youtube/downstream-dispatch-race",
            default_privacy="unlisted",
        )
        job = Job(
            pipeline_id=pipeline.id,
            pipeline_snapshot=definition,
            status=JobStatus.RUNNING,
            execution_plan={
                "topo_order": ["source_1", "trim_1"],
                "dependencies": {"source_1": [], "trim_1": ["source_1"]},
            },
            orchestrator_owner="python",
        )
        db.add_all([account, job])
        await db.flush()
        source = NodeExecution(
            job_id=job.id,
            node_id="source_1",
            node_type="source",
            node_label="Source",
            node_config={"asset_id": str(uuid.uuid4())},
            status=NodeStatus.SUCCEEDED,
            progress=100,
        )
        downstream = NodeExecution(
            job_id=job.id,
            node_id="trim_1",
            node_type="trim",
            node_label="Trim",
            node_config={"start_time": 0, "end_time": 1},
            status=NodeStatus.PENDING,
        )
        task = ProductionTask(
            channel_profile_id=channel.id,
            target_account_id=account.id,
            prompt="downstream dispatch race",
            state="producing",
            job_id=job.id,
            channel_config_snapshot_json={},
        )
        db.add_all([source, downstream, task])
        await db.flush()
        artifact = Artifact(
            job_id=job.id,
            node_execution_id=source.id,
            filename="source.mp4",
            storage_path="artifacts/source.mp4",
        )
        db.add(artifact)
        await db.flush()
        source.output_artifact_id = artifact.id
        await db.commit()
        channel_id = channel.id
        job_id = job.id
        downstream_id = downstream.id

    queued_before_authority = asyncio.Event()
    release_dispatch = asyncio.Event()

    class RecordingRedis:
        def __init__(self) -> None:
            self.dispatches: list[tuple[str, dict]] = []

        async def xadd(self, stream_key: str, payload: dict) -> None:
            self.dispatches.append((stream_key, payload))

        async def aclose(self) -> None:
            return None

    async def pause_after_queue_commit(_job_id, node_id):
        if node_id == "trim_1":
            queued_before_authority.set()
            await release_dispatch.wait()

    redis = RecordingRedis()
    job_engine = JobEngine()
    monkeypatch.setattr("app.orchestrator.engine._redis", lambda: redis)
    monkeypatch.setattr(job_engine, "_before_node_dispatch_recheck", pause_after_queue_commit)

    async def dispatch_downstream() -> None:
        async with factory() as dispatch_db:
            stored_job = (
                await dispatch_db.execute(
                    select(Job)
                    .where(Job.id == job_id)
                    .options(selectinload(Job.node_executions))
                )
            ).scalar_one()
            await job_engine._dispatch_ready_nodes(
                dispatch_db,
                stored_job,
                {"source_1": [], "trim_1": ["source_1"]},
            )

    dispatcher = asyncio.create_task(dispatch_downstream())
    try:
        await asyncio.wait_for(queued_before_authority.wait(), timeout=5)
        async with factory() as quarantine_db:
            result = await asyncio.wait_for(
                quarantine_channelops_backlog(
                    quarantine_db,
                    channel_id,
                    apply=True,
                    close_schedule=True,
                ),
                timeout=5,
            )
        assert result["schedule"]["final_state"] == VideoScheduleState.CLOSED.value
    finally:
        release_dispatch.set()
    await asyncio.wait_for(dispatcher, timeout=5)

    async with factory() as db:
        stored_job = await db.get(Job, job_id)
        stored_downstream = await db.get(NodeExecution, downstream_id)
    assert stored_job is not None and stored_job.status == JobStatus.CANCELLED
    assert stored_downstream is not None and stored_downstream.status == NodeStatus.CANCELLED
    assert redis.dispatches == []


async def test_quarantine_first_blocks_youtube_submission_fence(postgres_race_db):
    engine, factory = postgres_race_db
    channel_id, task_id, job_id, node_id = await _seed_worker_authority(
        factory,
        node_status=NodeStatus.RUNNING,
        job_status=JobStatus.RUNNING,
    )
    context = UploadOperationContext(
        job_id=job_id,
        node_execution_id=node_id,
        input_artifact_id=uuid.uuid4(),
        content_sha256="a" * 64,
        title="quarantine-first fence",
        privacy="unlisted",
    )
    store = YouTubeUploadOperationStore(factory)
    submissions: list[str] = []

    async def guarded_submission() -> None:
        async with store.submission_fence(context):
            submissions.append("posted")

    blocker = factory()
    submission = None
    try:
        await _stage_worker_quarantine(
            blocker,
            channel_id=channel_id,
            task_id=task_id,
            job_id=job_id,
            node_id=node_id,
        )
        submission = asyncio.create_task(guarded_submission())
        await _wait_until_lock_wait(engine, "channel_profiles", submission)
        await blocker.commit()
        with pytest.raises(JobExecutionAuthorityBlocked):
            await asyncio.wait_for(submission, timeout=5)
    finally:
        if blocker.in_transaction():
            await blocker.rollback()
        await blocker.close()
        if submission is not None and not submission.done():
            submission.cancel()
        if submission is not None:
            await asyncio.wait_for(asyncio.gather(submission, return_exceptions=True), timeout=5)

    assert submissions == []


async def test_youtube_submission_fence_makes_quarantine_wait(postgres_race_db):
    engine, factory = postgres_race_db
    channel_id, task_id, job_id, node_id = await _seed_worker_authority(
        factory,
        node_status=NodeStatus.RUNNING,
        job_status=JobStatus.RUNNING,
    )
    context = UploadOperationContext(
        job_id=job_id,
        node_execution_id=node_id,
        input_artifact_id=uuid.uuid4(),
        content_sha256="b" * 64,
        title="submission-first fence",
        privacy="unlisted",
    )
    store = YouTubeUploadOperationStore(factory)
    submission_started = asyncio.Event()
    release_submission = asyncio.Event()
    submissions: list[str] = []

    async def guarded_submission() -> None:
        async with store.submission_fence(context):
            submission_started.set()
            await release_submission.wait()
            submissions.append("posted")

    async def apply_quarantine():
        async with factory() as db:
            return await quarantine_channelops_backlog(
                db,
                channel_id,
                apply=True,
                close_schedule=True,
            )

    submission = asyncio.create_task(guarded_submission())
    quarantine = None
    try:
        await asyncio.wait_for(submission_started.wait(), timeout=5)
        quarantine = asyncio.create_task(apply_quarantine())
        await _wait_until_lock_wait(engine, "channel_profiles", quarantine)
        assert not quarantine.done()
    finally:
        release_submission.set()
        await asyncio.wait_for(asyncio.gather(submission, return_exceptions=True), timeout=5)
        if quarantine is not None:
            await asyncio.wait_for(asyncio.gather(quarantine, return_exceptions=True), timeout=5)

    assert submission.exception() is None
    assert quarantine is not None and quarantine.exception() is None
    assert submissions == ["posted"]
    async with factory() as db:
        stored_job = await db.get(Job, job_id)
        stored_node = await db.get(NodeExecution, node_id)
    assert stored_job is not None and stored_job.status == JobStatus.CANCELLED
    assert stored_node is not None and stored_node.status == NodeStatus.CANCELLED


async def _seed_review_release(factory) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    async with factory() as db:
        channel = ChannelProfile(name="release race", enabled=True, dry_run=False)
        db.add(channel)
        await db.flush()
        account = PublishingAccount(
            channel_profile_id=channel.id,
            account_label="release",
            credential_ref="youtube/release",
            default_privacy="unlisted",
        )
        plan = _review_plan()
        db.add_all([account, plan])
        await db.flush()
        task = ProductionTask(
            channel_profile_id=channel.id,
            target_account_id=account.id,
            source="trend_youtube",
            prompt="review release race",
            uses_external_assets=True,
            approval_mode="human",
            autoflow_plan_id=plan.id,
            state="held",
            blocked_by_guard="human_approval_required",
            channel_config_snapshot_json={},
        )
        db.add(task)
        await db.commit()
        return channel.id, task.id, plan.id


async def _seed_promotion(factory) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    async with factory() as db:
        channel = ChannelProfile(name="promotion race", enabled=True, dry_run=False)
        db.add(channel)
        await db.flush()
        account = PublishingAccount(
            channel_profile_id=channel.id,
            account_label="promotion",
            credential_ref="youtube/promotion",
            default_privacy="unlisted",
        )
        db.add(account)
        await db.flush()
        task = ProductionTask(
            channel_profile_id=channel.id,
            target_account_id=account.id,
            prompt="promotion race",
            state="uploaded_private",
            channel_config_snapshot_json={},
        )
        db.add(task)
        await db.flush()
        publication = PublicationRecord(
            production_task_id=task.id,
            account_id=account.id,
            platform_content_id=f"race-{uuid.uuid4()}",
            title="promotion race",
            desired_privacy="unlisted",
            current_privacy="private",
            publish_status="uploaded",
            compliance_disposition="owned",
        )
        db.add(publication)
        await db.commit()
        return channel.id, task.id, publication.id


async def _assert_quarantined(factory, task_id: uuid.UUID, *, queue_kind: str) -> None:
    async with factory() as db:
        task = await db.get(ProductionTask, task_id)
        assert task is not None
        assert task.state == "held"
        assert task.blocked_by_guard == QUARANTINE_REASON
        rows = (
            await db.execute(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == queue_kind))
        ).scalars().all()
        assert all(row.status == "dead_lettered" for row in rows)


async def test_review_release_first_commits_atomically_then_quarantine_holds_it(postgres_race_db):
    engine, factory = postgres_race_db
    channel_id, task_id, plan_id = await _seed_review_release(factory)
    async with factory() as blocker, factory() as operator_db, factory() as quarantine_db:
        await blocker.execute(select(AutoFlowPlan).where(AutoFlowPlan.id == plan_id).with_for_update())
        operator = asyncio.create_task(
            release_human_review(
                str(task_id),
                HumanReviewRequest(human_actor="reviewer"),
                db=operator_db,
            )
        )
        quarantine = None
        try:
            await _wait_until_lock_wait(engine, "autoflow_plans", operator)
            quarantine = asyncio.create_task(quarantine_channelops_backlog(quarantine_db, channel_id, apply=True))
            await _wait_until_lock_wait(engine, "channel_profiles", quarantine)
            await blocker.commit()
            result = await operator
            await quarantine
        finally:
            await _cancel_operations(blocker, operator, *(operation for operation in [quarantine] if operation))

    assert result["state"] == "planning"
    await _assert_quarantined(factory, task_id, queue_kind="execute_task")


async def test_quarantine_first_makes_review_release_conflict_without_enqueue(postgres_race_db):
    engine, factory = postgres_race_db
    channel_id, task_id, _plan_id = await _seed_review_release(factory)
    async with factory() as blocker, factory() as operator_db, factory() as quarantine_db:
        await blocker.execute(select(ProductionTask).where(ProductionTask.id == task_id).with_for_update())
        quarantine = asyncio.create_task(quarantine_channelops_backlog(quarantine_db, channel_id, apply=True))
        operator = None
        try:
            await _wait_until_lock_wait(engine, "production_tasks", quarantine)
            operator = asyncio.create_task(
                release_human_review(
                    str(task_id),
                    HumanReviewRequest(human_actor="reviewer"),
                    db=operator_db,
                )
            )
            await _wait_until_lock_wait(engine, "channel_profiles", operator)
            await blocker.commit()
            await quarantine
            with pytest.raises(HTTPException) as exc_info:
                await operator
        finally:
            await _cancel_operations(blocker, quarantine, *(operation for operation in [operator] if operation))

    assert exc_info.value.status_code == 409
    async with factory() as db:
        task = await db.get(ProductionTask, task_id)
        assert task is not None
        assert task.human_review_evidence_json == {}
        assert await db.scalar(select(ChannelOpsQueueItem.id).where(ChannelOpsQueueItem.kind == "execute_task")) is None


async def test_manual_promotion_first_commits_then_quarantine_dead_letters_it(postgres_race_db):
    engine, factory = postgres_race_db
    channel_id, task_id, publication_id = await _seed_promotion(factory)
    async with factory() as blocker, factory() as operator_db, factory() as quarantine_db:
        await blocker.execute(
            select(PublicationRecord).where(PublicationRecord.id == publication_id).with_for_update()
        )
        operator = asyncio.create_task(
            promote_publication(
                str(publication_id),
                HumanReviewRequest(human_actor="reviewer"),
                db=operator_db,
            )
        )
        quarantine = None
        try:
            await _wait_until_lock_wait(engine, "publication_records", operator)
            quarantine = asyncio.create_task(quarantine_channelops_backlog(quarantine_db, channel_id, apply=True))
            await _wait_until_lock_wait(engine, "channel_profiles", quarantine)
            await blocker.commit()
            result = await operator
            await quarantine
        finally:
            await _cancel_operations(blocker, operator, *(operation for operation in [quarantine] if operation))

    assert result.kind == "promote_publication"
    await _assert_quarantined(factory, task_id, queue_kind="promote_publication")


async def test_quarantine_first_makes_manual_promotion_conflict_without_evidence(postgres_race_db):
    engine, factory = postgres_race_db
    channel_id, task_id, publication_id = await _seed_promotion(factory)
    async with factory() as blocker, factory() as operator_db, factory() as quarantine_db:
        await blocker.execute(select(ProductionTask).where(ProductionTask.id == task_id).with_for_update())
        quarantine = asyncio.create_task(quarantine_channelops_backlog(quarantine_db, channel_id, apply=True))
        operator = None
        try:
            await _wait_until_lock_wait(engine, "production_tasks", quarantine)
            operator = asyncio.create_task(
                promote_publication(
                    str(publication_id),
                    HumanReviewRequest(human_actor="reviewer"),
                    db=operator_db,
                )
            )
            await _wait_until_lock_wait(engine, "channel_profiles", operator)
            await blocker.commit()
            await quarantine
            with pytest.raises(HTTPException) as exc_info:
                await operator
        finally:
            await _cancel_operations(blocker, quarantine, *(operation for operation in [operator] if operation))

    assert exc_info.value.status_code == 409
    async with factory() as db:
        task = await db.get(ProductionTask, task_id)
        assert task is not None
        assert task.human_review_evidence_json == {}
        assert (
            await db.scalar(select(ChannelOpsQueueItem.id).where(ChannelOpsQueueItem.kind == "promote_publication"))
            is None
        )


async def test_quarantine_after_running_commit_prevents_stale_initial_dispatch(
    postgres_race_db,
    monkeypatch,
):
    _engine, factory = postgres_race_db
    definition = {
        "nodes": [
            {
                "id": "trim_1",
                "type": "trim",
                "position": {"x": 0, "y": 0},
                "data": {"label": "Trim", "config": {"start_time": 0, "end_time": 1}},
            }
        ],
        "edges": [],
    }
    async with factory() as db:
        channel = ChannelProfile(name="dispatch race", enabled=True, dry_run=False)
        db.add(channel)
        await db.flush()
        account = PublishingAccount(
            channel_profile_id=channel.id,
            account_label="dispatch race",
            credential_ref="youtube/dispatch-race",
            default_privacy="unlisted",
        )
        pipeline = Pipeline(name="dispatch race", description="", definition=definition)
        db.add_all([account, pipeline])
        await db.flush()
        job = Job(
            pipeline_id=pipeline.id,
            pipeline_snapshot=definition,
            status=JobStatus.PENDING,
            orchestrator_owner="python",
        )
        db.add(job)
        await db.flush()
        node = NodeExecution(
            job_id=job.id,
            node_id="trim_1",
            node_type="trim",
            node_label="Trim",
            node_config={"start_time": 0, "end_time": 1},
            status=NodeStatus.PENDING,
        )
        task = ProductionTask(
            channel_profile_id=channel.id,
            target_account_id=account.id,
            prompt="dispatch race",
            state="producing",
            job_id=job.id,
            channel_config_snapshot_json={},
        )
        db.add_all([node, task])
        await db.commit()
        channel_id = channel.id
        task_id = task.id
        job_id = job.id
        node_id = node.id

    entered_after_running_commit = asyncio.Event()
    release_starter = asyncio.Event()

    async def pause_after_running_commit(_job_id):
        entered_after_running_commit.set()
        await release_starter.wait()

    class RecordingRedis:
        def __init__(self) -> None:
            self.dispatches: list[tuple[str, dict]] = []

        async def xadd(self, stream_key: str, payload: dict) -> None:
            self.dispatches.append((stream_key, payload))

        async def aclose(self) -> None:
            return None

    redis = RecordingRedis()
    job_engine = JobEngine()
    monkeypatch.setattr("app.orchestrator.engine.async_session", factory)
    monkeypatch.setattr("app.orchestrator.engine._redis", lambda: redis)
    monkeypatch.setattr(
        job_engine,
        "_before_initial_launch_recheck",
        pause_after_running_commit,
        raising=False,
    )

    starter = asyncio.create_task(job_engine.start_job(job_id))
    await asyncio.wait_for(entered_after_running_commit.wait(), timeout=5)
    try:
        async with factory() as quarantine_db:
            result = await quarantine_channelops_backlog(
                quarantine_db,
                channel_id,
                apply=True,
                close_schedule=True,
            )
        assert result["schedule"]["final_state"] == VideoScheduleState.CLOSED.value
    finally:
        release_starter.set()
        await asyncio.wait_for(starter, timeout=5)

    async with factory() as db:
        stored_task = await db.get(ProductionTask, task_id)
        stored_job = await db.get(Job, job_id)
        stored_node = await db.get(NodeExecution, node_id)
        stored_schedule = await db.get(RuntimeSchedule, VIDEO_SCHEDULE_SERVICE)
    assert stored_task is not None and stored_task.state == "held"
    assert stored_job is not None and stored_job.status == JobStatus.CANCELLED
    assert stored_node is not None and stored_node.status == NodeStatus.CANCELLED
    assert stored_schedule is not None and stored_schedule.state == VideoScheduleState.CLOSED.value
    assert redis.dispatches == []


async def test_initial_start_locks_channel_before_mutating_job(
    postgres_race_db,
    monkeypatch,
):
    engine, factory = postgres_race_db
    definition = {
        "nodes": [
            {
                "id": "trim_1",
                "type": "trim",
                "position": {"x": 0, "y": 0},
                "data": {"label": "Trim", "config": {"start_time": 0, "end_time": 1}},
            }
        ],
        "edges": [],
    }
    async with factory() as db:
        channel = ChannelProfile(name="initial lock order", enabled=True, dry_run=False)
        schedule = RuntimeSchedule(
            service_name=VIDEO_SCHEDULE_SERVICE,
            state=VideoScheduleState.OPEN.value,
            updated_by="initial-lock-order-test",
        )
        pipeline = Pipeline(name="initial lock order", description="", definition=definition)
        db.add_all([channel, schedule, pipeline])
        await db.flush()
        account = PublishingAccount(
            channel_profile_id=channel.id,
            account_label="initial lock order",
            credential_ref="youtube/initial-lock-order",
            default_privacy="unlisted",
        )
        job = Job(
            pipeline_id=pipeline.id,
            pipeline_snapshot=definition,
            status=JobStatus.PENDING,
            orchestrator_owner="python",
        )
        db.add_all([account, job])
        await db.flush()
        node = NodeExecution(
            job_id=job.id,
            node_id="trim_1",
            node_type="trim",
            node_label="Trim",
            node_config={"start_time": 0, "end_time": 1},
            status=NodeStatus.PENDING,
        )
        task = ProductionTask(
            channel_profile_id=channel.id,
            target_account_id=account.id,
            prompt="initial lock order",
            state="producing",
            job_id=job.id,
            channel_config_snapshot_json={},
        )
        db.add_all([node, task])
        await db.commit()
        channel_id = channel.id
        job_id = job.id

    class RecordingRedis:
        def __init__(self) -> None:
            self.dispatches: list[tuple[str, dict]] = []

        async def xadd(self, stream_key: str, payload: dict) -> None:
            self.dispatches.append((stream_key, payload))

        async def aclose(self) -> None:
            return None

    redis = RecordingRedis()
    monkeypatch.setattr("app.orchestrator.engine.async_session", factory)
    monkeypatch.setattr("app.orchestrator.engine._redis", lambda: redis)

    blocker = factory()
    starter = None
    try:
        await blocker.execute(
            select(ChannelProfile).where(ChannelProfile.id == channel_id).with_for_update()
        )
        starter = asyncio.create_task(JobEngine().start_job(job_id))
        await _wait_until_lock_wait(engine, "channel_profiles", starter)

        async with factory() as observer:
            stored_job = await observer.get(Job, job_id)
        assert stored_job is not None and stored_job.status == JobStatus.PENDING
        assert redis.dispatches == []
    finally:
        await blocker.rollback()
        await blocker.close()
        if starter is not None:
            await asyncio.wait_for(asyncio.gather(starter, return_exceptions=True), timeout=5)


async def test_quarantine_between_initial_roots_does_not_revive_second_root(
    postgres_race_db,
    monkeypatch,
):
    _engine, factory = postgres_race_db
    definition = {
        "nodes": [
            {
                "id": "trim_a",
                "type": "trim",
                "position": {"x": 0, "y": 0},
                "data": {"label": "Trim A", "config": {"start_time": 0, "end_time": 1}},
            },
            {
                "id": "trim_b",
                "type": "trim",
                "position": {"x": 200, "y": 0},
                "data": {"label": "Trim B", "config": {"start_time": 1, "end_time": 2}},
            },
        ],
        "edges": [],
    }
    async with factory() as db:
        channel = ChannelProfile(name="two-root dispatch race", enabled=True, dry_run=False)
        db.add(channel)
        await db.flush()
        account = PublishingAccount(
            channel_profile_id=channel.id,
            account_label="two-root dispatch race",
            credential_ref="youtube/two-root-dispatch-race",
            default_privacy="unlisted",
        )
        pipeline = Pipeline(name="two-root dispatch race", description="", definition=definition)
        db.add_all([account, pipeline])
        await db.flush()
        job = Job(
            pipeline_id=pipeline.id,
            pipeline_snapshot=definition,
            status=JobStatus.PENDING,
            orchestrator_owner="python",
        )
        db.add(job)
        await db.flush()
        root_a = NodeExecution(
            job_id=job.id,
            node_id="trim_a",
            node_type="trim",
            node_label="Trim A",
            node_config={"start_time": 0, "end_time": 1},
            status=NodeStatus.PENDING,
        )
        root_b = NodeExecution(
            job_id=job.id,
            node_id="trim_b",
            node_type="trim",
            node_label="Trim B",
            node_config={"start_time": 1, "end_time": 2},
            status=NodeStatus.PENDING,
        )
        task = ProductionTask(
            channel_profile_id=channel.id,
            target_account_id=account.id,
            prompt="two-root dispatch race",
            state="producing",
            job_id=job.id,
            channel_config_snapshot_json={},
        )
        db.add_all([root_a, root_b, task])
        await db.commit()
        channel_id = channel.id
        task_id = task.id
        job_id = job.id
        root_b_id = root_b.id

    before_root_b_authority = asyncio.Event()
    release_starter = asyncio.Event()
    used_per_root_recheck = False

    class RecordingRedis:
        def __init__(self) -> None:
            self.dispatches: list[tuple[str, dict]] = []

        async def xadd(self, stream_key: str, payload: dict) -> None:
            self.dispatches.append((stream_key, payload))

        async def aclose(self) -> None:
            return None

    redis = RecordingRedis()
    job_engine = JobEngine()
    original_cache_check = job_engine._apply_cached_artifact_if_available

    async def pause_before_root_recheck(_job_id, node_id):
        nonlocal used_per_root_recheck
        if node_id != "trim_b":
            return
        used_per_root_recheck = True
        before_root_b_authority.set()
        await release_starter.wait()

    async def pause_old_path_before_root_b_queue(db, current_job, node, input_artifacts):
        if node.node_id == "trim_b" and not used_per_root_recheck:
            before_root_b_authority.set()
            await release_starter.wait()
        return await original_cache_check(db, current_job, node, input_artifacts)

    monkeypatch.setattr("app.orchestrator.engine.async_session", factory)
    monkeypatch.setattr("app.orchestrator.engine._redis", lambda: redis)
    monkeypatch.setattr(
        job_engine,
        "_before_initial_node_launch_recheck",
        pause_before_root_recheck,
        raising=False,
    )
    monkeypatch.setattr(
        job_engine,
        "_apply_cached_artifact_if_available",
        pause_old_path_before_root_b_queue,
    )

    starter = asyncio.create_task(job_engine.start_job(job_id))
    try:
        await asyncio.wait_for(before_root_b_authority.wait(), timeout=5)
        assert [payload["node_id"] for _stream, payload in redis.dispatches] == ["trim_a"]
        async with factory() as quarantine_db:
            result = await asyncio.wait_for(
                quarantine_channelops_backlog(
                    quarantine_db,
                    channel_id,
                    apply=True,
                    close_schedule=True,
                ),
                timeout=5,
            )
        assert result["schedule"]["final_state"] == VideoScheduleState.CLOSED.value
    finally:
        release_starter.set()
        await asyncio.wait_for(starter, timeout=5)

    async with factory() as db:
        stored_task = await db.get(ProductionTask, task_id)
        stored_job = await db.get(Job, job_id)
        stored_root_b = await db.get(NodeExecution, root_b_id)
        stored_schedule = await db.get(RuntimeSchedule, VIDEO_SCHEDULE_SERVICE)
    assert [payload["node_id"] for _stream, payload in redis.dispatches] == ["trim_a"]
    assert stored_task is not None and stored_task.state == "held"
    assert stored_job is not None and stored_job.status == JobStatus.CANCELLED
    assert stored_root_b is not None and stored_root_b.status == NodeStatus.CANCELLED
    assert stored_schedule is not None and stored_schedule.state == VideoScheduleState.CLOSED.value
