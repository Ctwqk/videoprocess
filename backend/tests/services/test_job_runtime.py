from __future__ import annotations

import asyncio
import uuid
from types import SimpleNamespace

import pytest

from app.models.job import JobStatus
from app.services import job_runtime
from app.services.job_execution_authority import (
    JobExecutionAuthorityBlocked,
    LockedJobExecutionAuthority,
    require_active_execution_authority,
)
from app.services.job_runtime import start_jobs_background, start_or_defer_jobs
from app.services.schedule_service import VideoScheduleState


@pytest.mark.asyncio
async def test_start_jobs_background_waits_for_durable_launch_handoff(monkeypatch):
    job_id = uuid.uuid4()
    started = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    async def blocking_start(observed_job_id: uuid.UUID) -> None:
        assert observed_job_id == job_id
        started.set()
        try:
            await release.wait()
        finally:
            finished.set()

    monkeypatch.setattr("app.orchestrator.engine.engine.start_job", blocking_start)
    launch = asyncio.create_task(start_jobs_background([job_id]))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        assert not launch.done()
    finally:
        release.set()
        await asyncio.wait_for(launch, timeout=1)
        await asyncio.wait_for(finished.wait(), timeout=1)


@pytest.mark.asyncio
async def test_start_jobs_background_shields_launch_handoff_from_caller_cancellation(monkeypatch):
    job_id = uuid.uuid4()
    started = asyncio.Event()
    release = asyncio.Event()
    child_cancelled = asyncio.Event()

    async def blocking_start(observed_job_id: uuid.UUID) -> None:
        assert observed_job_id == job_id
        started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            child_cancelled.set()
            raise

    monkeypatch.setattr("app.orchestrator.engine.engine.start_job", blocking_start)
    launch = asyncio.create_task(start_jobs_background([job_id]))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        launch.cancel()
        await asyncio.sleep(0)

        assert not launch.done()
        assert not child_cancelled.is_set()

        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(launch, timeout=1)
        assert not child_cancelled.is_set()
    finally:
        release.set()
        if not launch.done():
            launch.cancel()
        await asyncio.gather(launch, return_exceptions=True)


@pytest.mark.asyncio
async def test_start_or_defer_partitions_exact_guard_from_mismatches(monkeypatch):
    guarded_job = SimpleNamespace(id=uuid.uuid4(), status=JobStatus.PENDING)
    mismatching_job = SimpleNamespace(id=uuid.uuid4(), status=JobStatus.PENDING)
    schedule = SimpleNamespace(
        state=VideoScheduleState.OPEN.value,
        guarded_job_id=guarded_job.id,
    )
    events: list[tuple[str, list[uuid.UUID]]] = []

    class RecordingSession:
        async def commit(self):
            events.append(("commit", []))

    db = RecordingSession()

    async def load_schedule(_db, *, commit=True):
        assert _db is db
        assert commit is False
        events.append(("load_schedule", []))
        return schedule

    async def park_jobs(_db, jobs, *, commit=True):
        assert _db is db
        assert commit is False
        materialized = list(jobs)
        events.append(("park", [job.id for job in materialized]))
        for job in materialized:
            job.status = JobStatus.WAITING_WINDOW

    async def start_jobs(job_ids):
        events.append(("start", list(job_ids)))

    monkeypatch.setattr(job_runtime, "get_video_schedule_record", load_schedule, raising=False)
    monkeypatch.setattr(job_runtime, "park_jobs_for_window", park_jobs)
    monkeypatch.setattr(job_runtime, "start_jobs_background", start_jobs)

    state = await start_or_defer_jobs(db, [guarded_job, mismatching_job])

    assert state == VideoScheduleState.OPEN
    assert mismatching_job.status == JobStatus.WAITING_WINDOW
    assert events == [
        ("load_schedule", []),
        ("park", [mismatching_job.id]),
        ("commit", []),
        ("start", [guarded_job.id]),
    ]


def test_execution_authority_rejects_guarded_mismatch_before_node_work():
    guarded_job_id = uuid.uuid4()
    authority = LockedJobExecutionAuthority(
        channel=None,
        schedule=SimpleNamespace(
            state=VideoScheduleState.OPEN.value,
            guarded_job_id=guarded_job_id,
        ),
        task=None,
        job=SimpleNamespace(id=uuid.uuid4(), status=JobStatus.RUNNING),
        node=None,
    )

    with pytest.raises(JobExecutionAuthorityBlocked, match="guarded"):
        require_active_execution_authority(
            authority,
            job_statuses={JobStatus.RUNNING},
        )


def test_execution_authority_rejects_missing_guard_field():
    authority = LockedJobExecutionAuthority(
        channel=None,
        schedule=SimpleNamespace(state=VideoScheduleState.OPEN.value),
        task=None,
        job=SimpleNamespace(id=uuid.uuid4(), status=JobStatus.RUNNING),
        node=None,
    )

    with pytest.raises(JobExecutionAuthorityBlocked, match="guarded"):
        require_active_execution_authority(
            authority,
            job_statuses={JobStatus.RUNNING},
        )
