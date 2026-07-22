from __future__ import annotations

import asyncio
import hashlib
import json
import os
import uuid
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.autoflow.clip_ranker import ClipRanker
from app.autoflow.service import AutoFlowService, execute_request_fingerprint
from app.models.autoflow import AutoFlowPlan, AutoFlowRun
from app.models.channel_agent import ChannelOpsQueueItem, ChannelProfile, ProductionTask
from app.models.job import Job, JobStatus
from app.models.pipeline import Pipeline
from app.models.schedule import RuntimeSchedule
from app.orchestrator.engine import JobEngine
from app.schemas.autoflow import (
    AutoFlowClipCandidate,
    AutoFlowExecuteRequest,
    AutoFlowPlanPatch,
    AutoFlowRequest,
)
from app.services.schedule_service import VIDEO_SCHEDULE_SERVICE, VideoScheduleState


POSTGRES_URL = os.getenv("CHANNEL_OPS_POSTGRES_TEST_URL", "")

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(not POSTGRES_URL, reason="set CHANNEL_OPS_POSTGRES_TEST_URL for PostgreSQL idempotency tests"),
]


class StaticSelector:
    async def find_candidates(self, intent, request: AutoFlowRequest, db=None):
        return [
            AutoFlowClipCandidate(
                id="owned-idempotency-clip",
                title=f"{intent.subject} owned clip",
                source_type="asset",
                asset_id="owned-idempotency-asset",
                start_sec=0,
                end_sec=5,
                rights_status="allowed",
                metadata={"duration": 5, "aspect_ratio": "9:16"},
            )
        ]


@pytest_asyncio.fixture
async def postgres_idempotency_db(monkeypatch):
    engine = create_async_engine(POSTGRES_URL, pool_size=8, max_overflow=0)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "TRUNCATE TABLE autoflow_used_clips, autoflow_runs, node_executions, jobs, "
                "pipelines, runtime_schedules, channel_ops_queue_items, production_tasks, "
                "channel_profiles, autoflow_plans CASCADE"
            )
        )

    starts: list[str] = []

    async def fake_start_jobs_background(job_ids):
        starts.extend(str(job_id) for job_id in job_ids)

    monkeypatch.setattr("app.autoflow.service.start_jobs_background", fake_start_jobs_background, raising=False)
    try:
        yield engine, factory, starts
    finally:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "TRUNCATE TABLE autoflow_used_clips, autoflow_runs, node_executions, jobs, "
                    "pipelines, runtime_schedules, channel_ops_queue_items, production_tasks, "
                    "channel_profiles, autoflow_plans CASCADE"
                )
            )
        await engine.dispose()


async def _approved_plan(factory, *, prompt: str):
    service = AutoFlowService(material_selector=StaticSelector(), clip_ranker=ClipRanker())
    async with factory() as db:
        plan = await service.plan(
            AutoFlowRequest(
                prompt=prompt,
                target_platforms=["youtube_shorts"],
                publish_mode="private_upload",
            ),
            db=db,
        )
        approved = await service.approve(plan.plan_id, db)
    assert approved is not None
    assert approved.approved_revision_hash
    return service, approved


async def _counts(factory) -> tuple[int, int, int]:
    async with factory() as db:
        return (
            int((await db.scalar(select(func.count()).select_from(AutoFlowRun))) or 0),
            int((await db.scalar(select(func.count()).select_from(Pipeline))) or 0),
            int((await db.scalar(select(func.count()).select_from(Job))) or 0),
        )


async def _install_guarded_job(factory, plan) -> uuid.UUID:
    definition = plan.pipeline_definition.model_dump(mode="json")
    async with factory() as db:
        pipeline = Pipeline(
            name="guard authority",
            description="",
            definition=definition,
        )
        db.add(pipeline)
        await db.flush()
        job = Job(
            pipeline_id=pipeline.id,
            pipeline_snapshot=definition,
            status=JobStatus.RUNNING,
            orchestrator_owner="python",
        )
        db.add(job)
        await db.flush()
        schedule = RuntimeSchedule(
            service_name=VIDEO_SCHEDULE_SERVICE,
            state=VideoScheduleState.OPEN.value,
            guarded_job_id=job.id,
            updated_by="guarded-autoflow-test",
        )
        db.add(schedule)
        await db.commit()
        return job.id


async def _bound_execute_request(
    factory,
    plan,
    *,
    halted: bool = False,
    queue_status: str = "running",
    queue_claimed: bool = True,
) -> tuple[uuid.UUID, uuid.UUID, AutoFlowExecuteRequest]:
    requested_locked_by = "bound-execute-worker"
    requested_locked_at = datetime.now(timezone.utc)
    queue_locked_by = requested_locked_by if queue_status == "running" and queue_claimed else None
    queue_locked_at = requested_locked_at if queue_locked_by is not None else None
    async with factory() as db:
        channel = ChannelProfile(
            name="bound execute channel",
            enabled=True,
            dry_run=False,
            halted_at=datetime.now(timezone.utc) if halted else None,
            halt_reason="test quarantine" if halted else None,
        )
        db.add(channel)
        await db.flush()
        task = ProductionTask(
            channel_profile_id=channel.id,
            target_account_id=uuid.uuid4(),
            prompt="bound execute task",
            rationale_json={
                "autoflow_plan_payload": {
                    "plan_id": plan.plan_id,
                    "expected_approved_revision_hash": plan.approved_revision_hash,
                    "expected_approved_revision": plan.approved_revision,
                }
            },
            autoflow_plan_id=uuid.UUID(plan.plan_id),
            state="planning",
            channel_config_snapshot_json={},
        )
        db.add(task)
        await db.flush()
        queue = ChannelOpsQueueItem(
            kind="execute_task",
            idempotency_key=f"execute_task:{task.id}",
            channel_profile_id=channel.id,
            payload_json={
                "production_task_id": str(task.id),
                "autoflow_plan_id": plan.plan_id,
                "expected_approved_revision_hash": plan.approved_revision_hash,
                "expected_approved_revision": plan.approved_revision,
            },
            status=queue_status,
            locked_by=queue_locked_by,
            locked_at=queue_locked_at,
            attempt_count=1 if queue_status == "running" and queue_claimed else 0,
        )
        db.add(queue)
        await db.commit()

    request = AutoFlowExecuteRequest(
        plan_id=plan.plan_id,
        idempotency_key=(
            f"channelops-execute:{task.id}:{plan.plan_id}:"
            f"{plan.approved_revision}:{plan.approved_revision_hash}"
        ),
        expected_approved_revision_hash=plan.approved_revision_hash,
        expected_approved_revision=plan.approved_revision,
        production_task_id=str(task.id),
        channelops_queue_item_id=str(queue.id),
        channelops_queue_locked_by=requested_locked_by,
        channelops_queue_locked_at=requested_locked_at,
    )
    return task.id, queue.id, request


async def test_bound_execute_links_task_before_start_handoff(
    postgres_idempotency_db,
    monkeypatch,
):
    _engine, factory, _starts = postgres_idempotency_db
    service, plan = await _approved_plan(factory, prompt="Bound execution is linked before start")
    task_id, _queue_id, request = await _bound_execute_request(factory, plan)
    observed_links: list[tuple[str, str, str, str]] = []

    async def assert_linked_before_start(job_ids):
        async with factory() as db:
            task = await db.get(ProductionTask, task_id)
            assert task is not None
            observed_links.append(
                (
                    str(task.autoflow_run_id),
                    str(task.pipeline_id),
                    str(task.job_id),
                    task.state,
                )
            )
            assert [str(task.job_id)] == [str(job_id) for job_id in job_ids]

    monkeypatch.setattr(
        "app.autoflow.service.start_jobs_background",
        assert_linked_before_start,
    )
    async with factory() as db:
        run = await service.execute(request, db)

    assert observed_links == [(run.run_id, run.pipeline_id, run.job_id, "producing")]
    assert await _counts(factory) == (1, 1, 1)


async def test_bound_execute_replay_recovers_lost_start_handoff_without_duplicate_rows(
    postgres_idempotency_db,
    monkeypatch,
):
    _engine, factory, _starts = postgres_idempotency_db
    service, plan = await _approved_plan(factory, prompt="Bound execution start handoff recovery")
    task_id, _queue_id, request = await _bound_execute_request(factory, plan)
    start_attempts: list[list[str]] = []

    async def fail_first_start(job_ids):
        start_attempts.append([str(job_id) for job_id in job_ids])
        if len(start_attempts) == 1:
            async with factory() as start_db:
                job = await start_db.get(Job, uuid.UUID(start_attempts[0][0]))
                assert job is not None
                job.status = JobStatus.RUNNING
                await start_db.commit()
            raise RuntimeError("lost start handoff")

    monkeypatch.setattr("app.autoflow.service.start_jobs_background", fail_first_start)
    async with factory() as first_db:
        with pytest.raises(RuntimeError, match="lost start handoff"):
            await service.execute(request, first_db)
    async with factory() as replay_db:
        replay = await service.execute(request, replay_db)

    assert start_attempts == [[replay.job_id], [replay.job_id]]
    assert await _counts(factory) == (1, 1, 1)
    async with factory() as db:
        task = await db.get(ProductionTask, task_id)
        assert task is not None
        assert str(task.autoflow_run_id) == replay.run_id
        assert str(task.pipeline_id) == replay.pipeline_id
        assert str(task.job_id) == replay.job_id
        assert task.state == "producing"


async def test_bound_autoflow_guard_rejects_new_job_without_rows(postgres_idempotency_db):
    _engine, factory, starts = postgres_idempotency_db
    service, plan = await _approved_plan(factory, prompt="Bound execution blocked by exact guard")
    await _install_guarded_job(factory, plan)
    before = await _counts(factory)
    _task_id, _queue_id, request = await _bound_execute_request(factory, plan)

    async with factory() as db:
        with pytest.raises(PermissionError, match="guarded"):
            await service.execute(request, db)

    assert await _counts(factory) == before
    assert starts == []


async def test_bound_autoflow_replay_resumes_only_exact_guarded_job(
    postgres_idempotency_db,
):
    _engine, factory, starts = postgres_idempotency_db
    service, plan = await _approved_plan(factory, prompt="Bound guarded replay")
    _task_id, _queue_id, request = await _bound_execute_request(factory, plan)
    async with factory() as db:
        first = await service.execute(request, db)

    async with factory() as db:
        schedule = await db.get(RuntimeSchedule, VIDEO_SCHEDULE_SERVICE)
        assert schedule is not None
        schedule.guarded_job_id = uuid.UUID(first.job_id)
        await db.commit()
    async with factory() as db:
        replay = await service.execute(request, db)

    mismatching_job_id = await _install_additional_guarded_job(factory, plan)
    async with factory() as db:
        schedule = await db.get(RuntimeSchedule, VIDEO_SCHEDULE_SERVICE)
        assert schedule is not None
        schedule.guarded_job_id = mismatching_job_id
        await db.commit()
    async with factory() as db:
        with pytest.raises(PermissionError, match="guarded"):
            await service.execute(request, db)

    assert replay.job_id == first.job_id
    assert starts == [first.job_id, first.job_id]


async def _install_additional_guarded_job(factory, plan) -> uuid.UUID:
    definition = plan.pipeline_definition.model_dump(mode="json")
    async with factory() as db:
        pipeline = Pipeline(
            name="mismatching guard authority",
            description="",
            definition=definition,
        )
        db.add(pipeline)
        await db.flush()
        job = Job(
            pipeline_id=pipeline.id,
            pipeline_snapshot=definition,
            status=JobStatus.RUNNING,
            orchestrator_owner="python",
        )
        db.add(job)
        await db.commit()
        return job.id


@pytest.mark.parametrize(
    ("halted", "queue_status", "error"),
    [
        (True, "running", "channel execution blocked"),
        (False, "dead_letter", "execute queue item is not claimed"),
    ],
)
async def test_bound_execute_rejects_revoked_queue_or_channel_authority_without_rows(
    postgres_idempotency_db,
    halted,
    queue_status,
    error,
):
    _engine, factory, starts = postgres_idempotency_db
    service, plan = await _approved_plan(factory, prompt="Bound execution revoked authority")
    _task_id, _queue_id, request = await _bound_execute_request(
        factory,
        plan,
        halted=halted,
        queue_status=queue_status,
    )

    async with factory() as db:
        with pytest.raises(PermissionError, match=error):
            await service.execute(request, db)

    assert await _counts(factory) == (0, 0, 0)
    assert starts == []


async def test_bound_execute_rejects_running_queue_without_claim_lease(
    postgres_idempotency_db,
):
    _engine, factory, starts = postgres_idempotency_db
    service, plan = await _approved_plan(factory, prompt="Bound execution requires a real queue lease")
    _task_id, _queue_id, request = await _bound_execute_request(
        factory,
        plan,
        queue_claimed=False,
    )

    async with factory() as db:
        with pytest.raises(PermissionError, match="execute queue item is not claimed"):
            await service.execute(request, db)

    assert await _counts(factory) == (0, 0, 0)
    assert starts == []


async def test_bound_execute_rejects_stale_lease_after_queue_reclaim(
    postgres_idempotency_db,
):
    _engine, factory, starts = postgres_idempotency_db
    service, plan = await _approved_plan(factory, prompt="Bound execution rejects stale queue leases")
    _task_id, queue_id, request = await _bound_execute_request(factory, plan)

    async with factory() as db:
        queue = await db.get(ChannelOpsQueueItem, queue_id)
        assert queue is not None
        queue.locked_by = "replacement-worker"
        queue.locked_at = datetime.now(timezone.utc)
        queue.attempt_count += 1
        await db.commit()

    async with factory() as db:
        with pytest.raises(PermissionError, match="execute queue lease authority changed"):
            await service.execute(request, db)

    assert await _counts(factory) == (0, 0, 0)
    assert starts == []


async def test_bound_execute_quarantine_first_commits_before_any_execution_rows(
    postgres_idempotency_db,
):
    engine, factory, starts = postgres_idempotency_db
    service, plan = await _approved_plan(factory, prompt="Bound execution quarantine-first race")
    task_id, _queue_id, request = await _bound_execute_request(factory, plan)

    blocker = factory()
    execution = None
    try:
        task = await blocker.get(ProductionTask, task_id)
        assert task is not None
        channel = (
            await blocker.execute(
                select(ChannelProfile)
                .where(ChannelProfile.id == task.channel_profile_id)
                .with_for_update()
            )
        ).scalar_one()
        channel.halted_at = datetime.now(timezone.utc)
        channel.halt_reason = "quarantine wins bound execution"
        await blocker.flush()

        async def execute_after_quarantine_lock():
            async with factory() as execute_db:
                return await service.execute(request, execute_db)

        execution = asyncio.create_task(execute_after_quarantine_lock())
        await _wait_until_lock_wait(engine, "channel_profiles", execution)
        await blocker.commit()
        with pytest.raises(PermissionError, match="channel execution blocked"):
            await asyncio.wait_for(execution, timeout=5)
    finally:
        if blocker.in_transaction():
            await blocker.rollback()
        await blocker.close()
        if execution is not None and not execution.done():
            execution.cancel()
        if execution is not None:
            await asyncio.wait_for(asyncio.gather(execution, return_exceptions=True), timeout=5)

    assert await _counts(factory) == (0, 0, 0)
    assert starts == []


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


async def test_concurrent_duplicate_execute_creates_one_run_pipeline_job(postgres_idempotency_db):
    _engine, factory, starts = postgres_idempotency_db
    service, plan = await _approved_plan(factory, prompt="Concurrent idempotent private upload")
    request = AutoFlowExecuteRequest(
        plan_id=plan.plan_id,
        idempotency_key=f"channelops-execute:task-1:{plan.plan_id}:{plan.approved_revision_hash}",
    )

    async def execute_once():
        async with factory() as db:
            return await service.execute(request, db)

    first, second = await asyncio.gather(execute_once(), execute_once())

    assert (first.run_id, first.pipeline_id, first.job_id) == (second.run_id, second.pipeline_id, second.job_id)
    assert await _counts(factory) == (1, 1, 1)
    assert starts == [first.job_id]


async def test_response_loss_replay_returns_same_durable_execution(postgres_idempotency_db):
    _engine, factory, starts = postgres_idempotency_db
    service, plan = await _approved_plan(factory, prompt="Response loss idempotent private upload")
    request = AutoFlowExecuteRequest(
        plan_id=plan.plan_id,
        idempotency_key=f"channelops-execute:task-2:{plan.plan_id}:{plan.approved_revision_hash}",
    )

    async with factory() as first_db:
        first = await service.execute(request, first_db)
    async with factory() as replay_db:
        replay = await service.execute(request, replay_db)

    assert (first.run_id, first.pipeline_id, first.job_id) == (replay.run_id, replay.pipeline_id, replay.job_id)
    assert await _counts(factory) == (1, 1, 1)
    assert starts == [first.job_id]


async def test_ordinary_autoflow_created_during_guard_is_parked(postgres_idempotency_db):
    _engine, factory, starts = postgres_idempotency_db
    service, plan = await _approved_plan(factory, prompt="Ordinary execution under exact guard")
    guarded_job_id = await _install_guarded_job(factory, plan)

    async with factory() as db:
        run = await service.execute(
            AutoFlowExecuteRequest(
                plan_id=plan.plan_id,
                idempotency_key=f"ordinary-guard:{plan.plan_id}",
            ),
            db,
        )

    assert run.job_id != str(guarded_job_id)
    assert run.status == JobStatus.WAITING_WINDOW.value
    assert starts == []
    async with factory() as db:
        job = await db.get(Job, uuid.UUID(run.job_id))
        assert job is not None and job.status == JobStatus.WAITING_WINDOW


async def test_pre_expected_authority_fingerprint_remains_replayable(postgres_idempotency_db):
    _engine, factory, starts = postgres_idempotency_db
    service, plan = await _approved_plan(factory, prompt="Legacy fingerprint response loss")
    request = AutoFlowExecuteRequest(
        plan_id=plan.plan_id,
        idempotency_key=f"legacy-fingerprint:{plan.plan_id}",
    )

    async with factory() as first_db:
        first = await service.execute(request, first_db)

    legacy_payload = request.model_dump(
        mode="json",
        exclude={
            "idempotency_key",
            "plan",
                "expected_approved_revision_hash",
                "expected_approved_revision",
                "production_task_id",
                "channelops_queue_item_id",
                "channelops_queue_locked_by",
                "channelops_queue_locked_at",
            },
    )
    legacy_payload["plan_id"] = str(uuid.UUID(plan.plan_id))
    legacy_canonical = json.dumps(legacy_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    legacy_fingerprint = hashlib.sha256(legacy_canonical.encode("utf-8")).hexdigest()
    async with factory() as legacy_db:
        await legacy_db.execute(
            text("UPDATE autoflow_runs SET request_fingerprint = :fingerprint WHERE id = :run_id"),
            {"fingerprint": legacy_fingerprint, "run_id": first.run_id},
        )
        await legacy_db.commit()

    async with factory() as replay_db:
        replay = await service.execute(request, replay_db)

    assert (first.run_id, first.pipeline_id, first.job_id) == (replay.run_id, replay.pipeline_id, replay.job_id)
    assert await _counts(factory) == (1, 1, 1)
    assert starts == [first.job_id]


async def test_observed_r1_rejects_first_execute_after_r2_reapproval(postgres_idempotency_db):
    _engine, factory, starts = postgres_idempotency_db
    service, observed_r1 = await _approved_plan(factory, prompt="Observed R1 must not execute R2")
    request_r1 = AutoFlowExecuteRequest(
        plan_id=observed_r1.plan_id,
        expected_approved_revision_hash=observed_r1.approved_revision_hash,
        expected_approved_revision=observed_r1.approved_revision,
        idempotency_key=f"observed-r1:{observed_r1.plan_id}",
    )

    async with factory() as patch_db:
        current_r2 = await service.patch_plan(
            observed_r1.plan_id,
            AutoFlowPlanPatch(
                metadata={"selected_title": "R2 title"},
                rebuild_definition=False,
                validate=False,
                evaluate_rights=False,
            ),
            patch_db,
        )
        assert current_r2 is not None
        current_r2 = await service.approve(current_r2.plan_id, patch_db)
    assert current_r2 is not None
    assert current_r2.approved_revision != observed_r1.approved_revision

    async with factory() as execute_db:
        with pytest.raises(ValueError, match="expected approved revision"):
            await service.execute(request_r1, execute_db)

    assert await _counts(factory) == (0, 0, 0)
    assert starts == []


async def test_r1_response_loss_then_r2_exact_retry_returns_r1_execution(postgres_idempotency_db):
    _engine, factory, starts = postgres_idempotency_db
    service, observed_r1 = await _approved_plan(factory, prompt="R1 response loss before R2")
    request_r1 = AutoFlowExecuteRequest(
        plan_id=observed_r1.plan_id,
        expected_approved_revision_hash=observed_r1.approved_revision_hash,
        expected_approved_revision=observed_r1.approved_revision,
        idempotency_key=f"response-loss-r1:{observed_r1.plan_id}",
    )

    async with factory() as first_db:
        first_r1 = await service.execute(request_r1, first_db)

    async with factory() as patch_db:
        current_r2 = await service.patch_plan(
            observed_r1.plan_id,
            AutoFlowPlanPatch(
                metadata={"selected_title": "R2 after committed R1"},
                rebuild_definition=False,
                validate=False,
                evaluate_rights=False,
            ),
            patch_db,
        )
        assert current_r2 is not None
        current_r2 = await service.approve(current_r2.plan_id, patch_db)
    assert current_r2 is not None
    assert current_r2.approved_revision != observed_r1.approved_revision

    async with factory() as replay_db:
        replay_r1 = await service.execute(request_r1, replay_db)

    assert (replay_r1.run_id, replay_r1.pipeline_id, replay_r1.job_id) == (
        first_r1.run_id,
        first_r1.pipeline_id,
        first_r1.job_id,
    )
    assert await _counts(factory) == (1, 1, 1)
    assert starts == [first_r1.job_id]


async def test_r1_retry_rechecks_committed_key_before_live_r2_authority(postgres_idempotency_db):
    engine, factory, starts = postgres_idempotency_db
    service, observed_r1 = await _approved_plan(factory, prompt="R1 commits while retry waits")
    request_r1 = AutoFlowExecuteRequest(
        plan_id=observed_r1.plan_id,
        expected_approved_revision_hash=observed_r1.approved_revision_hash,
        expected_approved_revision=observed_r1.approved_revision,
        idempotency_key=f"waiting-r1-retry:{observed_r1.plan_id}",
    )
    durable_r1_id = uuid.uuid4()

    async with factory() as schedule_db:
        schedule_db.add(
            RuntimeSchedule(
                service_name=VIDEO_SCHEDULE_SERVICE,
                state=VideoScheduleState.OPEN.value,
                updated_by="test",
            )
        )
        await schedule_db.commit()

    async with factory() as patch_db:
        current_r2 = await service.patch_plan(
            observed_r1.plan_id,
            AutoFlowPlanPatch(
                metadata={"selected_title": "R2 before delayed R1 visibility"},
                rebuild_definition=False,
                validate=False,
                evaluate_rights=False,
            ),
            patch_db,
        )
        assert current_r2 is not None
        current_r2 = await service.approve(current_r2.plan_id, patch_db)
    assert current_r2 is not None
    assert current_r2.approved_revision != observed_r1.approved_revision

    blocker = factory()
    try:
        await blocker.execute(
            select(RuntimeSchedule)
            .where(RuntimeSchedule.service_name == VIDEO_SCHEDULE_SERVICE)
            .with_for_update()
        )
        blocker.add(
            AutoFlowRun(
                id=durable_r1_id,
                plan_id=uuid.UUID(observed_r1.plan_id),
                status="pending",
                artifacts_json={},
                publish_json={
                    "approved_revision_hash": observed_r1.approved_revision_hash,
                    "approved_revision": observed_r1.approved_revision,
                },
                execute_idempotency_key=request_r1.idempotency_key,
                request_fingerprint=execute_request_fingerprint(request_r1, observed_r1.plan_id),
            )
        )
        await blocker.flush()

        async def replay_waiting_on_schedule():
            async with factory() as replay_db:
                return await service.execute(request_r1, replay_db)

        retry = asyncio.create_task(replay_waiting_on_schedule())
        await _wait_until_lock_wait(engine, "runtime_schedules", retry)

        await blocker.commit()
        replay = await asyncio.wait_for(retry, timeout=5)
    finally:
        if blocker.in_transaction():
            await blocker.rollback()
        await blocker.close()

    assert replay.run_id == str(durable_r1_id)
    assert await _counts(factory) == (1, 0, 0)
    assert starts == []


async def test_whitespace_idempotency_key_is_rejected_without_effects(postgres_idempotency_db):
    _engine, factory, starts = postgres_idempotency_db
    service, observed_plan = await _approved_plan(factory, prompt="Whitespace execute key")

    async with factory() as db:
        with pytest.raises(ValueError, match="idempotency_key must not be blank"):
            await service.execute(
                AutoFlowExecuteRequest(plan_id=observed_plan.plan_id, idempotency_key="   "),
                db,
            )

    assert await _counts(factory) == (0, 0, 0)
    assert starts == []


async def test_failure_after_reservation_rolls_back_all_execution_rows(postgres_idempotency_db, monkeypatch):
    _engine, factory, starts = postgres_idempotency_db
    service, plan = await _approved_plan(factory, prompt="Rollback idempotent private upload")

    async def fail_job_creation(db, pipeline_id, input_overrides=None, *, commit=True):
        raise RuntimeError("failure after reservation")

    monkeypatch.setattr("app.autoflow.service.create_job", fail_job_creation)
    request = AutoFlowExecuteRequest(
        plan_id=plan.plan_id,
        idempotency_key=f"channelops-execute:task-3:{plan.plan_id}:{plan.approved_revision_hash}",
    )
    async with factory() as db:
        with pytest.raises(RuntimeError, match="failure after reservation"):
            await service.execute(request, db)

    assert await _counts(factory) == (0, 0, 0)
    assert starts == []
    async with factory() as db:
        stored_plan = await db.get(AutoFlowPlan, plan.plan_id)
        assert stored_plan is not None
        assert stored_plan.status != "executed"


async def test_idempotency_key_cannot_be_reused_for_different_plan(postgres_idempotency_db):
    _engine, factory, starts = postgres_idempotency_db
    first_service, first_plan = await _approved_plan(factory, prompt="First idempotency plan")
    second_service, second_plan = await _approved_plan(factory, prompt="Second idempotency plan")
    key = "channelops-execute:shared-key"

    async with factory() as first_db:
        first = await first_service.execute(
            AutoFlowExecuteRequest(plan_id=first_plan.plan_id, idempotency_key=key),
            first_db,
        )
    async with factory() as second_db:
        with pytest.raises(ValueError, match="different plan or revision"):
            await second_service.execute(
                AutoFlowExecuteRequest(plan_id=second_plan.plan_id, idempotency_key=key),
                second_db,
            )

    assert await _counts(factory) == (1, 1, 1)
    assert starts == [first.job_id]


async def test_legacy_execute_without_key_remains_non_idempotent(postgres_idempotency_db):
    _engine, factory, starts = postgres_idempotency_db
    service, plan = await _approved_plan(factory, prompt="Legacy no-key private upload")

    async with factory() as first_db:
        first = await service.execute(AutoFlowExecuteRequest(plan_id=plan.plan_id), first_db)
    async with factory() as second_db:
        second = await service.execute(AutoFlowExecuteRequest(plan_id=plan.plan_id), second_db)

    assert first.run_id != second.run_id
    assert first.pipeline_id != second.pipeline_id
    assert first.job_id != second.job_id
    assert await _counts(factory) == (2, 2, 2)
    assert len(starts) == 2


async def test_idempotent_execute_preserves_closed_window_and_replay_does_not_start(postgres_idempotency_db):
    _engine, factory, starts = postgres_idempotency_db
    service, plan = await _approved_plan(factory, prompt="Closed-window idempotent private upload")
    async with factory() as db:
        await db.execute(
            text(
                "INSERT INTO runtime_schedules (service_name, state, updated_by) "
                "VALUES ('videoprocess', 'CLOSED', 'idempotency-test')"
            )
        )
        await db.commit()

    request = AutoFlowExecuteRequest(
        plan_id=plan.plan_id,
        idempotency_key=f"channelops-execute:task-closed:{plan.plan_id}:{plan.approved_revision_hash}",
    )
    async with factory() as first_db:
        first = await service.execute(request, first_db)
    async with factory() as replay_db:
        replay = await service.execute(request, replay_db)

    assert replay.run_id == first.run_id
    assert first.status == JobStatus.WAITING_WINDOW.value
    assert starts == []
    async with factory() as db:
        job = await db.get(Job, first.job_id)
        assert job is not None
        assert job.status == JobStatus.WAITING_WINDOW


async def test_execute_first_holds_plan_authority_until_patch_commits_afterward(
    postgres_idempotency_db,
    monkeypatch,
):
    engine, factory, starts = postgres_idempotency_db
    service, plan = await _approved_plan(factory, prompt="Execute-first plan authority race")
    request = AutoFlowExecuteRequest(
        plan_id=plan.plan_id,
        idempotency_key=f"execute-first:{plan.plan_id}",
    )
    entered_pipeline_create = asyncio.Event()
    release_pipeline_create = asyncio.Event()
    from app.autoflow import service as service_module

    original_create_pipeline = service_module.create_pipeline

    async def blocked_create_pipeline(db, payload, *, commit=True):
        entered_pipeline_create.set()
        await release_pipeline_create.wait()
        return await original_create_pipeline(db, payload, commit=commit)

    monkeypatch.setattr(service_module, "create_pipeline", blocked_create_pipeline)

    async with factory() as execute_db, factory() as patch_db:
        execution = asyncio.create_task(service.execute(request, execute_db))
        await asyncio.wait_for(entered_pipeline_create.wait(), timeout=5)
        patch = asyncio.create_task(
            service.patch_plan(
                plan.plan_id,
                AutoFlowPlanPatch(
                    metadata={"selected_title": "patched after execution"},
                    rebuild_definition=False,
                    validate=False,
                    evaluate_rights=False,
                ),
                patch_db,
            )
        )
        try:
            await _wait_until_lock_wait(engine, "autoflow_plans", patch)
            release_pipeline_create.set()
            run = await execution
            patched = await patch
        finally:
            release_pipeline_create.set()
            await asyncio.gather(execution, patch, return_exceptions=True)

    assert run.job_id is not None
    assert patched is not None
    assert patched.approved_revision_hash is None
    assert patched.approved_revision is None
    assert patched.execution_revision == plan.execution_revision + 1
    assert starts == [run.job_id]


async def test_reject_first_revokes_authority_before_execute_can_claim_it(postgres_idempotency_db):
    engine, factory, starts = postgres_idempotency_db
    service, plan = await _approved_plan(factory, prompt="Reject-first plan authority race")
    request = AutoFlowExecuteRequest(
        plan_id=plan.plan_id,
        idempotency_key=f"reject-first:{plan.plan_id}",
    )

    async with factory() as reject_db, factory() as execute_db:
        rejected = (
            await reject_db.execute(
                select(AutoFlowPlan).where(AutoFlowPlan.id == uuid.UUID(plan.plan_id)).with_for_update()
            )
        ).scalar_one()
        rejected.status = "rejected"
        rejected.rejected_reason = "concurrent reviewer rejection"
        execution = asyncio.create_task(service.execute(request, execute_db))
        try:
            await _wait_until_lock_wait(engine, "autoflow_plans", execution)
            await reject_db.commit()
            with pytest.raises(PermissionError, match="rejected"):
                await execution
        finally:
            await reject_db.rollback()
            if not execution.done():
                execution.cancel()
            await asyncio.gather(execution, return_exceptions=True)

    assert await _counts(factory) == (0, 0, 0)
    assert starts == []


async def test_schedule_close_between_creation_and_starter_parks_without_dispatch(
    postgres_idempotency_db,
    monkeypatch,
):
    engine, factory, _starts = postgres_idempotency_db
    service, plan = await _approved_plan(factory, prompt="Schedule close before durable starter")
    async with factory() as execute_db:
        run = await service.execute(
            AutoFlowExecuteRequest(
                plan_id=plan.plan_id,
                idempotency_key=f"schedule-race:{plan.plan_id}",
            ),
            execute_db,
        )
    assert run.job_id is not None

    job_engine = JobEngine()
    dispatches: list[uuid.UUID] = []

    async def no_resolve(_db, _job):
        return None

    async def record_dispatch(_db, job, _dep_map):
        dispatches.append(job.id)

    monkeypatch.setattr("app.orchestrator.engine.async_session", factory)
    monkeypatch.setattr(job_engine, "_resolve_source_nodes", no_resolve)
    monkeypatch.setattr(job_engine, "_dispatch_ready_nodes", record_dispatch)

    async with factory() as close_db:
        schedule = (
            await close_db.execute(
                select(RuntimeSchedule)
                .where(RuntimeSchedule.service_name == VIDEO_SCHEDULE_SERVICE)
                .with_for_update()
            )
        ).scalar_one()
        schedule.state = VideoScheduleState.CLOSED.value
        schedule.updated_by = "schedule-race-test"
        await close_db.flush()
        starter = asyncio.create_task(job_engine.start_job(uuid.UUID(run.job_id)))
        try:
            await _wait_until_lock_wait(engine, "runtime_schedules", starter)
            await close_db.commit()
            await starter
        finally:
            await close_db.rollback()
            if not starter.done():
                starter.cancel()
            await asyncio.gather(starter, return_exceptions=True)

    async with factory() as db:
        job = await db.get(Job, uuid.UUID(run.job_id))
        assert job is not None
        assert job.status == JobStatus.WAITING_WINDOW
        assert job.started_at is None
    assert dispatches == []


async def test_idempotency_key_rejects_execute_flag_change(postgres_idempotency_db):
    _engine, factory, starts = postgres_idempotency_db
    service, plan = await _approved_plan(factory, prompt="Execute flag request fingerprint")
    key = f"request-fingerprint-execute:{plan.plan_id}"

    async with factory() as first_db:
        first = await service.execute(
            AutoFlowExecuteRequest(plan_id=plan.plan_id, execute=False, idempotency_key=key),
            first_db,
        )
    async with factory() as replay_db:
        with pytest.raises(ValueError, match="different request"):
            await service.execute(
                AutoFlowExecuteRequest(plan_id=plan.plan_id, execute=True, idempotency_key=key),
                replay_db,
            )

    assert first.job_id is None
    assert await _counts(factory) == (1, 0, 0)
    assert starts == []


async def test_idempotency_key_rejects_save_as_template_change(postgres_idempotency_db):
    _engine, factory, starts = postgres_idempotency_db
    service, plan = await _approved_plan(factory, prompt="Template flag request fingerprint")
    key = f"request-fingerprint-template:{plan.plan_id}"

    async with factory() as first_db:
        first = await service.execute(
            AutoFlowExecuteRequest(plan_id=plan.plan_id, save_as_template=False, idempotency_key=key),
            first_db,
        )
    async with factory() as replay_db:
        with pytest.raises(ValueError, match="different request"):
            await service.execute(
                AutoFlowExecuteRequest(plan_id=plan.plan_id, save_as_template=True, idempotency_key=key),
                replay_db,
            )

    assert await _counts(factory) == (1, 1, 1)
    assert starts == [first.job_id]
    async with factory() as db:
        pipeline = await db.get(Pipeline, uuid.UUID(first.pipeline_id))
        assert pipeline is not None
        assert pipeline.is_template is False


async def test_exact_request_fingerprint_replay_returns_existing_run(postgres_idempotency_db):
    _engine, factory, starts = postgres_idempotency_db
    service, plan = await _approved_plan(factory, prompt="Exact request fingerprint replay")
    request = AutoFlowExecuteRequest(
        plan_id=plan.plan_id,
        execute=True,
        save_as_template=True,
        review_approved=True,
        public_approved=False,
        idempotency_key=f"request-fingerprint-exact:{plan.plan_id}",
    )

    async with factory() as first_db:
        first = await service.execute(request, first_db)
    async with factory() as replay_db:
        replay = await service.execute(request, replay_db)

    assert replay.run_id == first.run_id
    assert await _counts(factory) == (1, 1, 1)
    assert starts == [first.job_id]


async def test_legacy_keyed_run_without_fingerprint_fails_closed(postgres_idempotency_db):
    _engine, factory, starts = postgres_idempotency_db
    service, plan = await _approved_plan(factory, prompt="Legacy request fingerprint")
    key = f"legacy-no-fingerprint:{plan.plan_id}"
    legacy_run_id = uuid.uuid4()
    async with factory() as db:
        await db.execute(
            text(
                "INSERT INTO autoflow_runs "
                "(id, plan_id, status, artifacts_json, publish_json, execute_idempotency_key, request_fingerprint) "
                "VALUES (:id, :plan_id, 'pending', '{}'::json, :publish, :key, NULL)"
            ),
            {
                "id": legacy_run_id,
                "plan_id": uuid.UUID(plan.plan_id),
                "publish": json.dumps(
                    {
                        "approved_revision_hash": plan.approved_revision_hash,
                        "approved_revision": plan.approved_revision,
                    }
                ),
                "key": key,
            },
        )
        await db.commit()

    async with factory() as replay_db:
        with pytest.raises(ValueError, match="request fingerprint"):
            await service.execute(
                AutoFlowExecuteRequest(plan_id=plan.plan_id, idempotency_key=key),
                replay_db,
            )

    assert await _counts(factory) == (1, 0, 0)
    assert starts == []
