from __future__ import annotations

import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from app import main
from app.models.job import JobStatus
from app.services.schedule_service import VideoScheduleState


class _RecoverySession:
    def __init__(self, events):
        self.events = events

    async def __aenter__(self):
        self.events.append(("enter", None))
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        self.events.append(("exit", None))
        return None

    async def commit(self) -> None:
        self.events.append(("commit", None))


def _job(*, status=JobStatus.PENDING, started_at=None, node_statuses=()):
    return SimpleNamespace(
        id=uuid.uuid4(),
        status=status,
        started_at=started_at,
        node_executions=[SimpleNamespace(status=node_status) for node_status in node_statuses],
    )


@pytest.mark.asyncio
async def test_startup_recovery_restarts_only_exact_guarded_job(monkeypatch):
    guarded_job = _job()
    mismatching_job = _job()
    schedule = SimpleNamespace(
        state=VideoScheduleState.OPEN.value,
        guarded_job_id=guarded_job.id,
    )
    events: list[tuple[str, object]] = []
    session = _RecoverySession(events)

    async def load_schedule(_db, *, commit=True):
        assert _db is session
        assert commit is False
        events.append(("load_schedule", None))
        return schedule

    async def load_jobs(_db):
        assert _db is session
        events.append(("load_jobs", None))
        return [guarded_job, mismatching_job]

    async def defer_job(_db, job, *, commit=True):
        assert _db is session
        assert commit is False
        events.append(("defer", job.id))
        job.status = JobStatus.WAITING_WINDOW

    def start_job(job_id):
        events.append(("start", job_id))
        return ("start", job_id)

    def create_task(awaitable):
        events.append(("create_task", awaitable[1]))

    monkeypatch.setattr(main, "async_session", lambda: session)
    monkeypatch.setattr(main, "get_video_schedule_record", load_schedule, raising=False)
    monkeypatch.setattr(main, "load_video_jobs_for_recovery", load_jobs)
    monkeypatch.setattr(main, "defer_job_until_next_window", defer_job)
    monkeypatch.setattr(main.engine, "start_job", start_job)
    monkeypatch.setattr(main.asyncio, "create_task", create_task)

    await main._recover_stale_jobs()

    assert mismatching_job.status == JobStatus.WAITING_WINDOW
    assert events == [
        ("enter", None),
        ("load_schedule", None),
        ("load_jobs", None),
        ("defer", mismatching_job.id),
        ("commit", None),
        ("exit", None),
        ("start", guarded_job.id),
        ("create_task", guarded_job.id),
    ]


@pytest.mark.asyncio
async def test_startup_recovery_draining_parks_fresh_and_restarts_started_pending(monkeypatch):
    fresh_job = _job()
    resumed_job = _job(started_at=datetime.now(timezone.utc))
    schedule = SimpleNamespace(
        state=VideoScheduleState.DRAINING.value,
        guarded_job_id=None,
    )
    events: list[tuple[str, object]] = []
    session = _RecoverySession(events)

    async def load_schedule(_db, *, commit=True):
        assert _db is session
        assert commit is False
        events.append(("load_schedule", None))
        return schedule

    async def load_jobs(_db):
        events.append(("load_jobs", None))
        return [fresh_job, resumed_job]

    async def defer_job(_db, job, *, commit=True):
        assert commit is False
        events.append(("defer", job.id))
        job.status = JobStatus.WAITING_WINDOW

    def start_job(job_id):
        events.append(("start", job_id))
        return ("start", job_id)

    monkeypatch.setattr(main, "async_session", lambda: session)
    monkeypatch.setattr(main, "get_video_schedule_record", load_schedule)
    monkeypatch.setattr(main, "load_video_jobs_for_recovery", load_jobs)
    monkeypatch.setattr(main, "defer_job_until_next_window", defer_job)
    monkeypatch.setattr(main.engine, "start_job", start_job)
    monkeypatch.setattr(main.asyncio, "create_task", lambda awaitable: None)

    await main._recover_stale_jobs()

    assert fresh_job.status == JobStatus.WAITING_WINDOW
    assert events == [
        ("enter", None),
        ("load_schedule", None),
        ("load_jobs", None),
        ("defer", fresh_job.id),
        ("commit", None),
        ("exit", None),
        ("start", resumed_job.id),
    ]


@pytest.mark.asyncio
async def test_startup_recovery_finalizes_only_after_classification_commit(monkeypatch):
    completed_job = _job(
        status=JobStatus.RUNNING,
        started_at=datetime.now(timezone.utc),
    )
    schedule = SimpleNamespace(
        state=VideoScheduleState.OPEN.value,
        guarded_job_id=None,
    )
    events: list[tuple[str, object]] = []
    session = _RecoverySession(events)

    async def load_schedule(_db, *, commit=True):
        assert commit is False
        events.append(("load_schedule", None))
        return schedule

    async def load_jobs(_db):
        events.append(("load_jobs", None))
        return [completed_job]

    async def finalize(_db, job):
        events.append(("finalize", job.id))
        await _db.commit()
        return True

    monkeypatch.setattr(main, "async_session", lambda: session)
    monkeypatch.setattr(main, "get_video_schedule_record", load_schedule)
    monkeypatch.setattr(main, "load_video_jobs_for_recovery", load_jobs)
    monkeypatch.setattr(main.engine, "_maybe_finalize_job", finalize)

    await main._recover_stale_jobs()

    assert events == [
        ("enter", None),
        ("load_schedule", None),
        ("load_jobs", None),
        ("commit", None),
        ("finalize", completed_job.id),
        ("commit", None),
        ("exit", None),
    ]
