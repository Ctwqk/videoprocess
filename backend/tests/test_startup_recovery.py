from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest

from app import main
from app.models.job import JobStatus
from app.services.schedule_service import VideoScheduleState


class _RecoverySession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    async def commit(self) -> None:
        return None


@pytest.mark.asyncio
async def test_startup_recovery_restarts_only_exact_guarded_job(monkeypatch):
    guarded_job = SimpleNamespace(
        id=uuid.uuid4(),
        status=JobStatus.PENDING,
        node_executions=[],
    )
    mismatching_job = SimpleNamespace(
        id=uuid.uuid4(),
        status=JobStatus.PENDING,
        node_executions=[],
    )
    schedule = SimpleNamespace(
        state=VideoScheduleState.OPEN.value,
        guarded_job_id=guarded_job.id,
    )
    scheduled: list[tuple[str, uuid.UUID]] = []

    async def load_schedule(_db):
        return schedule

    async def load_jobs(_db):
        return [guarded_job, mismatching_job]

    async def defer_job(_db, job):
        job.status = JobStatus.WAITING_WINDOW

    monkeypatch.setattr(main, "async_session", _RecoverySession)
    monkeypatch.setattr(main, "get_video_schedule_record", load_schedule, raising=False)
    monkeypatch.setattr(main, "load_video_jobs_for_recovery", load_jobs)
    monkeypatch.setattr(main, "defer_job_until_next_window", defer_job)
    monkeypatch.setattr(main.engine, "start_job", lambda job_id: ("start", job_id))
    monkeypatch.setattr(main.asyncio, "create_task", scheduled.append)

    await main._recover_stale_jobs()

    assert mismatching_job.status == JobStatus.WAITING_WINDOW
    assert scheduled == [("start", guarded_job.id)]
