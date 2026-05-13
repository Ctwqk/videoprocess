from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from app.models.job import JobStatus, NodeStatus
from app.services.schedule_service import (
    VideoScheduleState,
    is_job_fresh_submission,
    should_defer_job_start,
)


def _job(*, started_at=None, status=JobStatus.PENDING, node_statuses=None):
    statuses = node_statuses or [NodeStatus.PENDING]
    return SimpleNamespace(
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
