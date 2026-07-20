from __future__ import annotations

import asyncio
import os

import pytest
import pytest_asyncio
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.autoflow.clip_ranker import ClipRanker
from app.autoflow.service import AutoFlowService
from app.models.autoflow import AutoFlowPlan, AutoFlowRun
from app.models.job import Job, JobStatus
from app.models.pipeline import Pipeline
from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowExecuteRequest, AutoFlowRequest


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

    async def fake_legacy_start_or_defer_jobs(db, jobs):
        starts.extend(str(job.id) for job in jobs)

    monkeypatch.setattr("app.autoflow.service.start_jobs_background", fake_start_jobs_background, raising=False)
    monkeypatch.setattr("app.autoflow.service.start_or_defer_jobs", fake_legacy_start_or_defer_jobs)
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
