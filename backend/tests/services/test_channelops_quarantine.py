from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models.channel_agent import (
    ChannelOpsQueueItem,
    ChannelProfile,
    FeedbackSnapshot,
    ProductionTask,
    PublicationRecord,
)
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.schedule import RuntimeSchedule
from app.services import channelops_quarantine as channelops_quarantine_service
from app.services.channelops_quarantine import (
    QUARANTINE_REASON,
    UnknownChannelError,
    _cancel_job,
    _cancel_node,
    quarantine_channelops_backlog,
)
from app.services.schedule_service import VIDEO_SCHEDULE_SERVICE, VideoScheduleState


TABLES = (
    ChannelProfile.__table__,
    ChannelOpsQueueItem.__table__,
    ProductionTask.__table__,
    PublicationRecord.__table__,
    FeedbackSnapshot.__table__,
    Job.__table__,
    NodeExecution.__table__,
    RuntimeSchedule.__table__,
)
NOW = datetime(2026, 7, 12, 18, 0, tzinfo=timezone.utc)
SOAK_REASON = "automated_channelops_soak_guard"


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


@pytest.fixture
async def concurrent_quarantine_session_factory(tmp_path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'channelops-quarantine.sqlite3'}",
        connect_args={"timeout": 10},
    )

    @event.listens_for(engine.sync_engine, "connect")
    def configure_sqlite_connection(dbapi_connection, _):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA busy_timeout = 10000")
        cursor.close()

    async with engine.begin() as connection:
        await connection.exec_driver_sql("PRAGMA journal_mode = WAL")
        for table in TABLES:
            await connection.run_sync(table.create)

    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield factory
    finally:
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


def test_cancel_timestamps_follow_naive_utc_job_model_contract():
    job, node = _job(JobStatus.RUNNING, NodeStatus.RUNNING)

    _cancel_job(job, NOW)
    _cancel_node(node, NOW)

    expected = NOW.replace(tzinfo=None)
    assert job.completed_at == expected
    assert node.completed_at == expected


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
        "task_ids": sorted(
            [str(rows["active_task"].id), str(rows["published_task"].id)]
        ),
        "job_ids": sorted(
            [str(rows["active_job"].id), str(rows["published_job"].id)]
        ),
        "node_execution_ids": sorted(
            [str(rows["active_node"].id), str(rows["published_node"].id)]
        ),
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
    assert rows["published_task"].state == "held"
    assert rows["published_job"].status == JobStatus.CANCELLED
    assert rows["published_node"].status == NodeStatus.CANCELLED
    assert report["retained_ids"]["publication_ids"] == [str(rows["publication"].id)]
    assert report["retained_ids"]["feedback_snapshot_ids"] == [str(rows["feedback"].id)]
    assert str(rows["published_task"].id) in report["changed_ids"]["task_ids"]
    assert str(rows["published_job"].id) in report["changed_ids"]["job_ids"]

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
async def test_nonterminal_task_with_publication_is_held_but_evidence_is_retained(
    quarantine_session,
):
    rows = await _seed_graph(quarantine_session)

    report = await quarantine_channelops_backlog(
        quarantine_session,
        rows["target"].id,
        apply=True,
        now=NOW,
    )

    for key in (
        "published_task",
        "published_job",
        "published_node",
        "publication",
        "feedback",
    ):
        await quarantine_session.refresh(rows[key])

    assert rows["published_task"].state == "held"
    assert rows["published_job"].status == JobStatus.CANCELLED
    assert rows["published_node"].status == NodeStatus.CANCELLED
    assert str(rows["published_task"].id) in report["changed_ids"]["task_ids"]
    assert str(rows["published_job"].id) in report["changed_ids"]["job_ids"]
    assert report["retained_ids"]["publication_ids"] == [str(rows["publication"].id)]
    assert report["retained_ids"]["feedback_snapshot_ids"] == [str(rows["feedback"].id)]
    assert rows["publication"].platform_content_id == "retained-video"
    assert rows["feedback"].snapshot_stage == "24h"


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
async def test_apply_uses_custom_reason_and_closes_schedule_atomically(quarantine_session):
    rows = await _seed_graph(quarantine_session)

    report = await quarantine_channelops_backlog(
        quarantine_session,
        rows["target"].id,
        apply=True,
        now=NOW,
        reason=SOAK_REASON,
        close_schedule=True,
    )

    schedule = await quarantine_session.get(RuntimeSchedule, VIDEO_SCHEDULE_SERVICE)
    assert schedule is not None
    assert schedule.state == VideoScheduleState.CLOSED.value
    assert schedule.updated_by == SOAK_REASON
    assert report["schedule"] == {
        "requested_close": True,
        "changed": True,
        "previous_state": None,
        "final_state": "CLOSED",
    }
    assert rows["target"].halt_reason == SOAK_REASON
    assert rows["active_task"].blocked_by_guard == SOAK_REASON
    assert rows["active_job"].error_message == SOAK_REASON
    assert rows["active_node"].error_message == SOAK_REASON
    assert rows["running"].last_error == SOAK_REASON
    assert rows["active_task"].transition_history_json[-1] == {
        "from": "producing",
        "to": "held",
        "actor": SOAK_REASON,
        "at": NOW.isoformat(),
    }
    await quarantine_session.commit()

    second = await quarantine_channelops_backlog(
        quarantine_session,
        rows["target"].id,
        apply=True,
        now=NOW,
        reason=SOAK_REASON,
        close_schedule=True,
    )

    assert second["schedule"] == {
        "requested_close": True,
        "changed": False,
        "previous_state": "CLOSED",
        "final_state": "CLOSED",
    }
    await quarantine_session.refresh(rows["active_task"])
    assert len(rows["active_task"].transition_history_json) == 1


@pytest.mark.asyncio
async def test_close_schedule_reuses_conflicting_schedule_row(quarantine_session, monkeypatch):
    rows = await _seed_graph(quarantine_session)
    quarantine_session.add(
        RuntimeSchedule(
            service_name=VIDEO_SCHEDULE_SERVICE,
            state=VideoScheduleState.OPEN.value,
            updated_by="scheduler",
            updated_at=NOW,
        )
    )
    await quarantine_session.commit()

    schedule_inserts = []
    original_execute = quarantine_session.execute

    async def track_schedule_insert(statement, *args, **kwargs):
        if getattr(getattr(statement, "table", None), "name", None) == RuntimeSchedule.__tablename__:
            schedule_inserts.append(statement)
        return await original_execute(statement, *args, **kwargs)

    monkeypatch.setattr(quarantine_session, "execute", track_schedule_insert)

    report = await quarantine_channelops_backlog(
        quarantine_session,
        rows["target"].id,
        apply=True,
        now=NOW,
        reason=SOAK_REASON,
        close_schedule=True,
    )

    assert schedule_inserts
    compiled = str(schedule_inserts[0].compile(dialect=quarantine_session.bind.dialect))
    assert "ON CONFLICT" in compiled
    assert "DO NOTHING" in compiled
    assert report["schedule"] == {
        "requested_close": True,
        "changed": True,
        "previous_state": "OPEN",
        "final_state": "CLOSED",
    }
    schedule = await quarantine_session.get(RuntimeSchedule, VIDEO_SCHEDULE_SERVICE)
    assert schedule is not None
    assert schedule.state == VideoScheduleState.CLOSED.value
    assert schedule.updated_by == SOAK_REASON
    assert rows["target"].halt_reason == SOAK_REASON
    assert rows["active_task"].blocked_by_guard == SOAK_REASON


@pytest.mark.asyncio
async def test_concurrent_schedule_close_commits_both_quarantines(
    concurrent_quarantine_session_factory,
    monkeypatch,
):
    async with concurrent_quarantine_session_factory() as seed_session:
        rows = await _seed_graph(seed_session)
        channel_ids = (rows["target"].id, rows["other"].id)
        assert await seed_session.get(RuntimeSchedule, VIDEO_SCHEDULE_SERVICE) is None

    schedule_insert_barrier = asyncio.Barrier(2)
    original_create_or_lock = channelops_quarantine_service._create_or_lock_runtime_schedule

    async def coordinate_schedule_create(*args, **kwargs):
        await schedule_insert_barrier.wait()
        return await original_create_or_lock(*args, **kwargs)

    monkeypatch.setattr(
        channelops_quarantine_service,
        "_create_or_lock_runtime_schedule",
        coordinate_schedule_create,
    )

    async def quarantine(channel_id):
        async with concurrent_quarantine_session_factory() as session:
            return await quarantine_channelops_backlog(
                session,
                channel_id,
                apply=True,
                now=NOW,
                reason=SOAK_REASON,
                close_schedule=True,
            )

    reports = await asyncio.wait_for(
        asyncio.gather(*(quarantine(channel_id) for channel_id in channel_ids)),
        timeout=15,
    )

    assert all(report["applied"] for report in reports)
    assert sorted(report["schedule"]["changed"] for report in reports) == [False, True]
    assert all(report["schedule"]["final_state"] == "CLOSED" for report in reports)
    assert sorted(
        report["schedule"]["previous_state"] for report in reports if report["schedule"]["previous_state"]
    ) == ["CLOSED"]
    assert sum(report["schedule"]["previous_state"] is None for report in reports) == 1
    async with concurrent_quarantine_session_factory() as verification_session:
        schedules = list(
            (
                await verification_session.execute(
                    select(RuntimeSchedule).where(
                        RuntimeSchedule.service_name == VIDEO_SCHEDULE_SERVICE
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(schedules) == 1
        assert schedules[0].state == VideoScheduleState.CLOSED.value
        for channel_id in channel_ids:
            channel = await verification_session.get(ChannelProfile, channel_id)
            assert channel is not None
            assert channel.halt_reason == SOAK_REASON


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["", "x" * 256])
async def test_rejects_invalid_reason_before_mutation(quarantine_session, reason):
    rows = await _seed_graph(quarantine_session)

    with pytest.raises(ValueError, match="reason"):
        await quarantine_channelops_backlog(
            quarantine_session,
            rows["target"].id,
            apply=True,
            now=NOW,
            reason=reason,
            close_schedule=True,
        )

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
    assert await quarantine_session.get(RuntimeSchedule, VIDEO_SCHEDULE_SERVICE) is None


@pytest.mark.asyncio
async def test_unknown_channel_fails(quarantine_session):
    with pytest.raises(UnknownChannelError, match="Unknown channel"):
        await quarantine_channelops_backlog(quarantine_session, uuid.uuid4(), apply=False, now=NOW)
