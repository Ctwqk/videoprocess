from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from app import main
from app.models.job import JobStatus, NodeStatus
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


def _registered_stale_job() -> tuple[SimpleNamespace, SimpleNamespace]:
    registration_id = uuid.uuid4()
    node = SimpleNamespace(
        id=uuid.uuid4(),
        node_id="vision-1",
        status=NodeStatus.RUNNING,
        worker_id="vision-worker@127:1",
        worker_registration_id=registration_id,
        worker_lease_epoch=11,
        queued_at=None,
        started_at=datetime(2026, 7, 26, tzinfo=timezone.utc),
        completed_at=None,
        progress=25,
        error_message=None,
        input_artifact_ids=[uuid.uuid4()],
    )
    job = SimpleNamespace(
        id=uuid.uuid4(),
        status=JobStatus.RUNNING,
        started_at=node.started_at,
        submitted_at=node.started_at,
        completed_at=None,
        error_message=None,
        node_executions=[node],
    )
    return job, node


@pytest.mark.asyncio
async def test_registered_startup_recovery_keeps_live_old_claim(
    monkeypatch,
) -> None:
    job, node = _registered_stale_job()
    original = dict(vars(node))

    async def recover(_db, job_id, node_execution_id):
        assert job_id == job.id
        assert node_execution_id == node.id
        return "live"

    monkeypatch.setattr(
        main,
        "recover_registered_worker_node",
        recover,
        raising=False,
    )

    assert await main._prepare_job_for_recovery(object(), job) is False
    assert vars(node) == original
    assert job.status == JobStatus.RUNNING


@pytest.mark.asyncio
async def test_registered_startup_recovery_holds_expired_unresolved_claim(
    monkeypatch,
) -> None:
    job, node = _registered_stale_job()
    node.started_at = datetime(2020, 1, 1, tzinfo=timezone.utc)
    job.started_at = node.started_at
    job.submitted_at = node.started_at
    original = dict(vars(node))

    async def recover(_db, job_id, node_execution_id):
        return "held_unresolved"

    monkeypatch.setattr(
        main,
        "recover_registered_worker_node",
        recover,
        raising=False,
    )

    assert await main._prepare_job_for_recovery(object(), job) is False
    assert vars(node) == original
    assert job.status == JobStatus.RUNNING


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome",
    ("terminal", "held_unresolved_event"),
)
async def test_registered_startup_recovery_preserves_new_terminal_outcomes(
    monkeypatch,
    caplog,
    outcome,
) -> None:
    caplog.set_level(logging.INFO, logger=main.__name__)
    job, node = _registered_stale_job()
    node.started_at = datetime(2020, 1, 1, tzinfo=timezone.utc)
    job.started_at = node.started_at
    job.submitted_at = node.started_at
    original = dict(vars(node))

    async def recover(_db, job_id, node_execution_id):
        assert job_id == job.id
        assert node_execution_id == node.id
        return outcome

    monkeypatch.setattr(
        main,
        "recover_registered_worker_node",
        recover,
        raising=False,
    )

    assert await main._prepare_job_for_recovery(object(), job) is False
    assert vars(node) == original
    assert job.status == JobStatus.RUNNING
    assert outcome in caplog.text


@pytest.mark.asyncio
async def test_registered_startup_recovery_resets_resolved_expired_once(
    monkeypatch,
) -> None:
    job, node = _registered_stale_job()
    calls = 0

    async def recover(_db, job_id, node_execution_id):
        nonlocal calls
        calls += 1
        if calls == 1:
            node.status = NodeStatus.PENDING
            node.worker_id = None
            node.worker_registration_id = None
            node.worker_lease_epoch = None
            node.started_at = None
            node.progress = 0
            return "recovered"
        return "not_registered"

    monkeypatch.setattr(
        main,
        "recover_registered_worker_node",
        recover,
        raising=False,
    )

    assert await main._prepare_job_for_recovery(object(), job) is True
    assert node.status == NodeStatus.PENDING
    assert node.worker_registration_id is None
    assert job.status == JobStatus.PENDING


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
