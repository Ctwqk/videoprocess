from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models.channel_agent import (
    ChannelOpsQueueItem,
    ChannelProfile,
    FeedbackSnapshot,
    ProductionTask,
    PublicationRecord,
)
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.services.channelops_quarantine import (
    QUARANTINE_REASON,
    UnknownChannelError,
    quarantine_channelops_backlog,
)


TABLES = (
    ChannelProfile.__table__,
    ChannelOpsQueueItem.__table__,
    ProductionTask.__table__,
    PublicationRecord.__table__,
    FeedbackSnapshot.__table__,
    Job.__table__,
    NodeExecution.__table__,
)
NOW = datetime(2026, 7, 12, 18, 0, tzinfo=timezone.utc)


@pytest.fixture
async def quarantine_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        for table in TABLES:
            await connection.run_sync(table.create)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        yield session
    await engine.dispose()


def _task(channel_id: uuid.UUID, *, state: str, job_id: uuid.UUID | None = None) -> ProductionTask:
    return ProductionTask(
        channel_profile_id=channel_id,
        target_account_id=uuid.uuid4(),
        prompt=f"{state} task",
        state=state,
        job_id=job_id,
    )


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=value.tzinfo or timezone.utc).astimezone(timezone.utc)


def _job(status: JobStatus, node_status: NodeStatus) -> tuple[Job, NodeExecution]:
    job = Job(
        pipeline_id=uuid.uuid4(),
        pipeline_snapshot={"nodes": [], "edges": []},
        status=status,
    )
    node = NodeExecution(
        job=job,
        node_id=f"node-{uuid.uuid4()}",
        node_type="youtube_upload",
        status=node_status,
        worker_id="publisher-1",
        queued_at=NOW,
        started_at=NOW,
    )
    return job, node


async def _seed_graph(session):
    target = ChannelProfile(name="old soak", dry_run=False)
    other = ChannelProfile(name="other channel", dry_run=False)
    session.add_all([target, other])
    await session.flush()

    active_job, active_node = _job(JobStatus.RUNNING, NodeStatus.RUNNING)
    measured_job, measured_node = _job(JobStatus.SUCCEEDED, NodeStatus.SUCCEEDED)
    published_job, published_node = _job(JobStatus.RUNNING, NodeStatus.RUNNING)
    other_job, other_node = _job(JobStatus.RUNNING, NodeStatus.RUNNING)
    session.add_all(
        [
            active_job,
            active_node,
            measured_job,
            measured_node,
            published_job,
            published_node,
            other_job,
            other_node,
        ]
    )
    await session.flush()

    active_task = _task(target.id, state="producing", job_id=active_job.id)
    measured_task = _task(target.id, state="measured", job_id=measured_job.id)
    published_task = _task(target.id, state="producing", job_id=published_job.id)
    other_task = _task(other.id, state="producing", job_id=other_job.id)
    session.add_all([active_task, measured_task, published_task, other_task])
    await session.flush()

    publication = PublicationRecord(
        production_task_id=published_task.id,
        account_id=published_task.target_account_id,
        platform_content_id="retained-video",
        title="retained publication",
        desired_privacy="unlisted",
        current_privacy="unlisted",
        publish_status="published",
        compliance_disposition="owned",
    )
    session.add(publication)
    await session.flush()
    feedback = FeedbackSnapshot(publication_id=publication.id, snapshot_stage="24h")

    queued = ChannelOpsQueueItem(
        kind="observe_job",
        idempotency_key=f"target-queued-{uuid.uuid4()}",
        channel_profile_id=target.id,
        status="queued",
    )
    running = ChannelOpsQueueItem(
        kind="execute_task",
        idempotency_key=f"target-running-{uuid.uuid4()}",
        channel_profile_id=target.id,
        status="running",
        locked_at=NOW,
        locked_by="runner-1",
    )
    succeeded = ChannelOpsQueueItem(
        kind="agent_tick",
        idempotency_key=f"target-succeeded-{uuid.uuid4()}",
        channel_profile_id=target.id,
        status="succeeded",
    )
    other_queued = ChannelOpsQueueItem(
        kind="agent_tick",
        idempotency_key=f"other-queued-{uuid.uuid4()}",
        channel_profile_id=other.id,
        status="queued",
    )
    session.add_all([feedback, queued, running, succeeded, other_queued])
    await session.commit()
    return {
        "target": target,
        "other": other,
        "active_task": active_task,
        "active_job": active_job,
        "active_node": active_node,
        "measured_task": measured_task,
        "measured_job": measured_job,
        "published_task": published_task,
        "published_job": published_job,
        "published_node": published_node,
        "publication": publication,
        "feedback": feedback,
        "other_task": other_task,
        "other_job": other_job,
        "other_node": other_node,
        "queued": queued,
        "running": running,
        "succeeded": succeeded,
        "other_queued": other_queued,
    }


@pytest.mark.asyncio
async def test_dry_run_reports_exact_changes_without_mutating(quarantine_session):
    rows = await _seed_graph(quarantine_session)

    report = await quarantine_channelops_backlog(
        quarantine_session,
        rows["target"].id,
        apply=False,
        now=NOW,
    )

    assert report["applied"] is False
    assert report["reason"] == QUARANTINE_REASON
    assert report["changed_ids"] == {
        "channel_ids": [str(rows["target"].id)],
        "task_ids": [str(rows["active_task"].id)],
        "job_ids": [str(rows["active_job"].id)],
        "node_execution_ids": [str(rows["active_node"].id)],
        "queue_item_ids": sorted([str(rows["queued"].id), str(rows["running"].id)]),
    }
    assert report["counts"]["changed"] == {
        key: len(value) for key, value in report["changed_ids"].items()
    }
    await quarantine_session.refresh(rows["target"])
    await quarantine_session.refresh(rows["active_task"])
    await quarantine_session.refresh(rows["active_job"])
    await quarantine_session.refresh(rows["active_node"])
    await quarantine_session.refresh(rows["running"])
    assert rows["target"].halted_at is None
    assert rows["active_task"].state == "producing"
    assert rows["active_job"].status == JobStatus.RUNNING
    assert rows["active_node"].status == NodeStatus.RUNNING
    assert rows["running"].status == "running"
    assert rows["running"].locked_by == "runner-1"


@pytest.mark.asyncio
async def test_apply_is_channel_specific_and_retains_publication_evidence(quarantine_session):
    rows = await _seed_graph(quarantine_session)

    report = await quarantine_channelops_backlog(
        quarantine_session,
        rows["target"].id,
        apply=True,
        now=NOW,
    )

    for key in (
        "target",
        "active_task",
        "active_job",
        "active_node",
        "measured_task",
        "measured_job",
        "published_task",
        "published_job",
        "published_node",
        "publication",
        "feedback",
        "other_task",
        "other_job",
        "other_node",
        "queued",
        "running",
        "succeeded",
        "other_queued",
    ):
        await quarantine_session.refresh(rows[key])

    assert report["applied"] is True
    assert _as_utc(rows["target"].halted_at) == NOW
    assert rows["target"].halt_reason == QUARANTINE_REASON
    assert rows["active_task"].state == "held"
    assert rows["active_task"].blocked_by_guard == QUARANTINE_REASON
    assert rows["active_task"].failure_reason == QUARANTINE_REASON
    assert rows["active_task"].transition_history_json[-1] == {
        "from": "producing",
        "to": "held",
        "actor": QUARANTINE_REASON,
        "at": NOW.isoformat(),
    }
    assert rows["active_job"].status == JobStatus.CANCELLED
    assert rows["active_node"].status == NodeStatus.CANCELLED
    assert _as_utc(rows["active_job"].completed_at) == NOW
    assert _as_utc(rows["active_node"].completed_at) == NOW
    assert rows["active_node"].worker_id is None

    assert rows["measured_task"].state == "measured"
    assert rows["measured_job"].status == JobStatus.SUCCEEDED
    assert rows["published_task"].state == "producing"
    assert rows["published_job"].status == JobStatus.RUNNING
    assert rows["published_node"].status == NodeStatus.RUNNING
    assert report["retained_ids"]["publication_ids"] == [str(rows["publication"].id)]
    assert report["retained_ids"]["feedback_snapshot_ids"] == [str(rows["feedback"].id)]
    assert str(rows["published_task"].id) in report["retained_ids"]["task_ids"]
    assert str(rows["published_job"].id) in report["retained_ids"]["job_ids"]

    assert rows["queued"].status == "dead_lettered"
    assert rows["running"].status == "dead_lettered"
    assert rows["running"].locked_at is None
    assert rows["running"].locked_by is None
    assert rows["running"].last_error == QUARANTINE_REASON
    assert rows["succeeded"].status == "succeeded"

    assert rows["other"].halted_at is None
    assert rows["other_task"].state == "producing"
    assert rows["other_job"].status == JobStatus.RUNNING
    assert rows["other_node"].status == NodeStatus.RUNNING
    assert rows["other_queued"].status == "queued"


@pytest.mark.asyncio
async def test_second_apply_is_idempotent(quarantine_session):
    rows = await _seed_graph(quarantine_session)
    await quarantine_channelops_backlog(quarantine_session, rows["target"].id, apply=True, now=NOW)

    second = await quarantine_channelops_backlog(
        quarantine_session,
        rows["target"].id,
        apply=True,
        now=NOW,
    )

    assert second["changed_ids"] == {
        "channel_ids": [],
        "task_ids": [],
        "job_ids": [],
        "node_execution_ids": [],
        "queue_item_ids": [],
    }
    assert second["counts"]["changed"] == {
        "channel_ids": 0,
        "task_ids": 0,
        "job_ids": 0,
        "node_execution_ids": 0,
        "queue_item_ids": 0,
    }
    await quarantine_session.refresh(rows["active_task"])
    assert len(rows["active_task"].transition_history_json) == 1


@pytest.mark.asyncio
async def test_unknown_channel_fails(quarantine_session):
    with pytest.raises(UnknownChannelError, match="Unknown channel"):
        await quarantine_channelops_backlog(quarantine_session, uuid.uuid4(), apply=False, now=NOW)
