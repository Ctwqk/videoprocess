from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
import uuid

import pytest

from app.models.job import JobStatus, NodeStatus
from app.services.schedule_service import (
    VideoScheduleState,
    defer_job_until_next_window,
    is_job_fresh_submission,
    should_defer_job_start,
)


def _job(*, id=None, started_at=None, status=JobStatus.PENDING, node_statuses=None):
    statuses = node_statuses or [NodeStatus.PENDING]
    return SimpleNamespace(
        id=id or uuid.uuid4(),
        started_at=started_at,
        status=status,
        node_executions=[SimpleNamespace(status=node_status) for node_status in statuses],
    )


def test_is_job_fresh_submission_for_new_job():
    assert is_job_fresh_submission(_job())


def test_is_job_fresh_submission_false_after_start():
    assert not is_job_fresh_submission(_job(started_at=datetime.now(timezone.utc)))


def test_drain_defers_new_jobs_only():
    fresh_job = _job()
    resumed_job = _job(
        started_at=datetime.now(timezone.utc),
        status=JobStatus.RUNNING,
        node_statuses=[NodeStatus.SUCCEEDED, NodeStatus.PENDING],
    )

    assert should_defer_job_start(fresh_job, VideoScheduleState.DRAINING)
    assert not should_defer_job_start(resumed_job, VideoScheduleState.DRAINING)


def test_closed_defers_everything():
    assert should_defer_job_start(_job(), VideoScheduleState.CLOSED)
    assert should_defer_job_start(
        _job(
            started_at=datetime.now(timezone.utc),
            status=JobStatus.RUNNING,
            node_statuses=[NodeStatus.RUNNING],
        ),
        VideoScheduleState.CLOSED,
    )


def test_open_guard_allows_only_exact_job():
    guarded_job_id = uuid.uuid4()
    assert not should_defer_job_start(
        _job(id=guarded_job_id), VideoScheduleState.OPEN, guarded_job_id
    )
    assert should_defer_job_start(
        _job(id=uuid.uuid4()), VideoScheduleState.OPEN, guarded_job_id
    )


def test_legacy_open_without_guard_remains_unrestricted():
    assert not should_defer_job_start(_job(id=uuid.uuid4()), VideoScheduleState.OPEN)


@pytest.mark.asyncio
async def test_defer_job_without_commit_flushes_and_does_not_refresh():
    events: list[str] = []

    class RecordingSession:
        async def flush(self):
            events.append("flush")

        async def commit(self):
            events.append("commit")

        async def refresh(self, *_args, **_kwargs):
            events.append("refresh")

    node = SimpleNamespace(
        status=NodeStatus.RUNNING,
        worker_id="worker-1",
        queued_at=datetime.now(timezone.utc),
        started_at=datetime.now(timezone.utc),
        completed_at=None,
        progress=30,
        error_message="retrying",
        error_trace="trace",
    )
    job = _job(status=JobStatus.RUNNING, node_statuses=[])
    job.node_executions = [node]

    result = await defer_job_until_next_window(RecordingSession(), job, commit=False)

    assert result is job
    assert job.status == JobStatus.WAITING_WINDOW
    assert node.status == NodeStatus.PENDING
    assert events == ["flush"]
