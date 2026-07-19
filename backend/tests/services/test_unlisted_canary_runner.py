from __future__ import annotations

import importlib.util
import uuid
from datetime import timezone
from pathlib import Path
from types import ModuleType

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from app.models.asset import Asset
from app.models.base import Base
from app.models.channel_agent import (
    ChannelOpsQueueItem,
    ChannelProfile,
    LaneFormatMatrix,
    ManualSeed,
    ProductionTask,
    PublicationRecord,
    PublishingAccount,
    TopicLane,
)
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.services.schedule_service import load_video_jobs_for_recovery, release_waiting_video_jobs


TABLES = [
    Asset.__table__,
    ChannelProfile.__table__,
    TopicLane.__table__,
    PublishingAccount.__table__,
    LaneFormatMatrix.__table__,
    ManualSeed.__table__,
    Job.__table__,
    NodeExecution.__table__,
    ProductionTask.__table__,
    PublicationRecord.__table__,
    ChannelOpsQueueItem.__table__,
]


def load_runner() -> ModuleType:
    path = Path(__file__).resolve().parents[3] / "scripts" / "run_vp_unlisted_canary.py"
    spec = importlib.util.spec_from_file_location("vp_unlisted_canary_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
async def db():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(lambda sync_connection: Base.metadata.create_all(sync_connection, tables=TABLES))
    async with AsyncSession(engine, expire_on_commit=False) as session:
        yield session
    await engine.dispose()


async def add_asset(
    db: AsyncSession,
    *,
    license_value: str = "owned",
    provenance: str = "generated",
    mime_type: str = "video/mp4",
) -> Asset:
    asset = Asset(
        filename="canary.mp4",
        original_name="canary.mp4",
        mime_type=mime_type,
        file_size=42,
        storage_backend="s3",
        storage_path="assets/canary.mp4",
        media_info={"license": license_value, "provenance": provenance},
    )
    db.add(asset)
    await db.commit()
    return asset


@pytest.mark.anyio
async def test_create_graph_is_atomic_unlisted_and_enqueues_one_tick(db: AsyncSession):
    runner = load_runner()
    asset = await add_asset(db)

    graph = await runner.create_canary_graph(db, "run-123", str(asset.id))

    channel = await db.get(ChannelProfile, uuid.UUID(graph["channel_id"]))
    account = await db.get(PublishingAccount, uuid.UUID(graph["account_id"]))
    lane_format = await db.get(LaneFormatMatrix, uuid.UUID(graph["lane_format_id"]))
    seed = await db.get(ManualSeed, uuid.UUID(graph["manual_seed_id"]))
    ticks = list(
        await db.scalars(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.channel_profile_id == channel.id)
        )
    )

    assert channel.enabled is True
    assert channel.dry_run is False
    assert channel.risk_policy_json["publication_privacy"] == "unlisted"
    assert account.default_privacy == "unlisted"
    assert account.external_asset_auto_publish is False
    assert lane_format.default_publish_visibility == "unlisted"
    assert lane_format.source_platforms_json == []
    assert seed.source_policy == "owned_only"
    assert seed.constraints_json["input_asset_id"] == str(asset.id)
    assert [(row.kind, row.status) for row in ticks] == [("agent_tick", "queued")]
    assert ticks[0].payload_json == {
        "channel_id": str(channel.id),
        "plan_delay_seconds": 300,
    }
    assert graph["agent_tick_id"] == str(ticks[0].id)


@pytest.mark.anyio
async def test_create_graph_rejects_asset_without_owned_generated_video_attestation(db: AsyncSession):
    runner = load_runner()
    asset = await add_asset(db, provenance="external")

    with pytest.raises(runner.CanaryError, match="owned generated video"):
        await runner.create_canary_graph(db, "run-unsafe", str(asset.id))

    assert await db.scalar(select(func.count()).select_from(ChannelProfile)) == 0
    assert await db.scalar(select(func.count()).select_from(ChannelOpsQueueItem)) == 0


async def add_uploaded_publication(db: AsyncSession) -> tuple[ChannelProfile, ProductionTask, PublicationRecord]:
    channel = ChannelProfile(name="canary", dry_run=False)
    db.add(channel)
    await db.flush()
    account_id = uuid.uuid4()
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=account_id,
        prompt="owned canary",
        state="uploaded_private",
    )
    db.add(task)
    await db.flush()
    publication = PublicationRecord(
        production_task_id=task.id,
        account_id=account_id,
        platform_content_id="video-123",
        desired_privacy="unlisted",
        current_privacy="unlisted",
        publish_status="uploaded",
        compliance_disposition="approved",
    )
    db.add(publication)
    await db.flush()
    db.add(
        ChannelOpsQueueItem(
            kind="promote_publication",
            idempotency_key=f"promote_publication:{publication.id}:unlisted:delayed",
            channel_profile_id=channel.id,
            priority=70,
            payload_json={"publication_id": str(publication.id), "target_visibility": "unlisted"},
        )
    )
    await db.commit()
    return channel, task, publication


@pytest.mark.anyio
async def test_replace_auto_promotion_is_atomic_and_unlisted(db: AsyncSession):
    runner = load_runner()
    channel, _task, publication = await add_uploaded_publication(db)

    cancelled_ids, immediate = await runner.replace_auto_promotion_with_immediate(
        db,
        channel.id,
        publication.id,
    )

    rows = list(
        await db.scalars(
            select(ChannelOpsQueueItem)
            .where(ChannelOpsQueueItem.kind == "promote_publication")
            .order_by(ChannelOpsQueueItem.created_at.asc())
        )
    )
    assert len(cancelled_ids) == 1
    assert [row.status for row in rows] == ["cancelled", "queued"]
    assert immediate.id == rows[1].id
    assert immediate.priority == 70
    assert immediate.payload_json == {
        "publication_id": str(publication.id),
        "target_visibility": "unlisted",
        "channel_profile_id": str(channel.id),
    }
    assert immediate.idempotency_key == f"promote_publication:{publication.id}:unlisted:manual"


@pytest.mark.anyio
async def test_metrics_probe_uses_api_equivalent_hour_bucket_idempotency(db: AsyncSession):
    runner = load_runner()
    channel, _task, publication = await add_uploaded_publication(db)

    first = await runner.enqueue_metrics_probe(db, publication.id)
    second = await runner.enqueue_metrics_probe(db, publication.id)

    assert second.id == first.id
    assert first.kind == "collect_metrics"
    assert first.channel_profile_id == channel.id
    assert first.priority == 90
    assert first.payload_json == {"publication_id": str(publication.id)}
    assert first.idempotency_key.startswith(f"collect_metrics:{publication.id}:")


def test_schedule_close_failure_marks_evidence_failed_without_overwriting_root_failure():
    runner = load_runner()
    evidence = {
        "status": "failed",
        "failure": {"type": "CanaryError", "message": "root failure"},
        "schedule": {"final_state": None},
    }

    runner.mark_schedule_close_failure(evidence, RuntimeError("sensitive detail"))

    assert evidence["status"] == "failed"
    assert evidence["failure"] == {"type": "CanaryError", "message": "root failure"}
    assert evidence["schedule"]["final_state"] == "UNKNOWN"
    assert evidence["schedule"]["close_error"] == "RuntimeError"


def test_schedule_close_failure_creates_sanitized_failure_after_success():
    runner = load_runner()
    evidence = {"status": "succeeded", "schedule": {"final_state": None}}

    runner.mark_schedule_close_failure(evidence, RuntimeError("postgresql://user:secret@example/db"))

    assert evidence["status"] == "failed"
    assert evidence["failure"] == {
        "type": "RuntimeError",
        "message": "final schedule close failed",
    }


def test_runner_task_wait_covers_deployed_daytime_throttle():
    runner = load_runner()

    wait_seconds = runner.runner_task_wait_seconds(
        "\n".join(
            (
                "CHANNELOPS_RUNNER_POLL_SECONDS=5",
                "CHANNELOPS_THROTTLE_ENABLED=true",
                "CHANNELOPS_THROTTLE_RUNNER_POLL_SECONDS=300",
            )
        )
    )

    assert wait_seconds == 360


def test_channelops_wait_budget_covers_deployed_runner_poll():
    runner = load_runner()

    wait_seconds = runner.channelops_wait_seconds(
        timeout_seconds=1_200,
        deployed_wait_seconds=360,
    )

    assert wait_seconds == 360


@pytest.mark.anyio
async def test_failure_cleanup_uses_naive_utc_for_job_and_node_columns(db: AsyncSession):
    runner = load_runner()
    channel = ChannelProfile(name="failed canary", dry_run=False)
    db.add(channel)
    await db.flush()
    job = Job(
        pipeline_id=uuid.uuid4(),
        pipeline_snapshot={"version": "1.0", "nodes": [], "edges": []},
        status=JobStatus.PENDING,
    )
    db.add(job)
    await db.flush()
    node = NodeExecution(
        job_id=job.id,
        node_id="source_1",
        node_type="source",
        status=NodeStatus.PENDING,
    )
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=uuid.uuid4(),
        prompt="owned canary",
        state="producing",
        job_id=job.id,
    )
    queue_item = ChannelOpsQueueItem(
        kind="observe_job",
        idempotency_key=f"observe_job:{job.id}:test",
        channel_profile_id=channel.id,
        payload_json={"job_id": str(job.id)},
    )
    db.add_all((node, task, queue_item))
    await db.commit()

    report = await runner.failure_cleanup(db, channel.id)

    assert report["cancelled_job_ids"] == [str(job.id)]
    assert report["cancelled_node_execution_ids"] == [str(node.id)]
    assert job.status == JobStatus.CANCELLED
    assert job.completed_at is not None and job.completed_at.tzinfo is None
    assert node.status == NodeStatus.CANCELLED
    assert node.completed_at is not None and node.completed_at.tzinfo is None
    assert task.state == "held"
    assert task.state_updated_at.tzinfo is timezone.utc
    assert queue_item.status == "dead_lettered"


@pytest.mark.anyio
async def test_python_schedule_releases_only_python_owned_jobs(db: AsyncSession):
    python_job = Job(
        pipeline_id=uuid.uuid4(),
        pipeline_snapshot={"version": "1.0", "nodes": [], "edges": []},
        status=JobStatus.WAITING_WINDOW,
        orchestrator_owner="python",
    )
    go_job = Job(
        pipeline_id=uuid.uuid4(),
        pipeline_snapshot={"version": "1.0", "nodes": [], "edges": []},
        status=JobStatus.WAITING_WINDOW,
        orchestrator_owner="go",
    )
    db.add_all((python_job, go_job))
    await db.commit()

    released = await release_waiting_video_jobs(db)

    assert released == [str(python_job.id)]
    assert python_job.status == JobStatus.PENDING
    assert go_job.status == JobStatus.WAITING_WINDOW


@pytest.mark.anyio
async def test_python_recovery_loads_only_python_owned_jobs(db: AsyncSession):
    python_job = Job(
        pipeline_id=uuid.uuid4(),
        pipeline_snapshot={"version": "1.0", "nodes": [], "edges": []},
        status=JobStatus.PENDING,
        orchestrator_owner="python",
    )
    go_job = Job(
        pipeline_id=uuid.uuid4(),
        pipeline_snapshot={"version": "1.0", "nodes": [], "edges": []},
        status=JobStatus.PENDING,
        orchestrator_owner="go",
    )
    db.add_all((python_job, go_job))
    await db.commit()

    jobs = await load_video_jobs_for_recovery(db)

    assert [job.id for job in jobs] == [python_job.id]


@pytest.mark.anyio
async def test_backlog_ignores_only_global_cleanup_maintenance(db: AsyncSession):
    runner = load_runner()
    cleanup = ChannelOpsQueueItem(
        kind="cleanup_expired",
        idempotency_key="cleanup_expired:2026-07-12",
        channel_profile_id=None,
        payload_json={},
    )
    db.add(cleanup)
    await db.commit()

    report = await runner.active_backlog(db)

    assert report["unsafe_queue_item_ids"] == []

    unsafe = ChannelOpsQueueItem(
        kind="agent_tick",
        idempotency_key="agent_tick:global:2026-07-12-18",
        channel_profile_id=None,
        payload_json={"channel_id": str(uuid.uuid4())},
    )
    db.add(unsafe)
    await db.commit()

    report = await runner.active_backlog(db)

    assert report["unsafe_queue_item_ids"] == [str(unsafe.id)]
