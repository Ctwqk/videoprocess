from __future__ import annotations

import asyncio
import json
import os
import uuid

import pytest
import pytest_asyncio
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.autoflow.clip_ranker import ClipRanker
from app.autoflow.service import AutoFlowService
from app.models.autoflow import AutoFlowPlan, AutoFlowRun
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
                "pipelines, runtime_schedules, autoflow_plans CASCADE"
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
                    "pipelines, runtime_schedules, autoflow_plans CASCADE"
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
