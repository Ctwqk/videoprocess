from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.channel_agent.clock import FakeClock
from app.channel_agent.clients import FakeAutoFlowClient, FakeMiniMaxClient, FakeYouTubeClient
from app.channel_agent.queue import ChannelOpsQueueService
from app.channel_agent.service import ChannelAgentService
from app.models.channel_agent import (
    AgentTickAudit,
    ChannelOpsQueueItem,
    ChannelProfile,
    FeedbackSnapshot,
    LaneFormatMatrix,
    ManualSeed,
    MaterialUsageLedger,
    ProductionTask,
    PublicationRecord,
    PublishingAccount,
    TakedownEvent,
    TopicLane,
)


CHANNEL_AGENT_TABLES = (
    ChannelProfile.__table__,
    TopicLane.__table__,
    PublishingAccount.__table__,
    LaneFormatMatrix.__table__,
    ChannelOpsQueueItem.__table__,
    AgentTickAudit.__table__,
    ManualSeed.__table__,
    ProductionTask.__table__,
    MaterialUsageLedger.__table__,
    PublicationRecord.__table__,
    TakedownEvent.__table__,
    FeedbackSnapshot.__table__,
)


@pytest.fixture
async def service_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        for table in CHANNEL_AGENT_TABLES:
            await conn.run_sync(table.create)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session

    await engine.dispose()


async def _channel_graph(db, *, dry_run: bool = True, external_auto: bool = True):
    channel = ChannelProfile(name="Channel", language="zh", dry_run=dry_run)
    db.add(channel)
    await db.flush()
    lane = TopicLane(channel_profile_id=channel.id, name="lane", keywords_json=["cartoon"])
    account = PublishingAccount(
        channel_profile_id=channel.id,
        account_label="main",
        platform_account_id="yt-1",
        credential_ref="youtube/main",
        external_asset_auto_publish=external_auto,
    )
    db.add_all([lane, account])
    await db.flush()
    lane_format = LaneFormatMatrix(
        topic_lane_id=lane.id,
        format_key="shorts_9x16",
        target_duration_sec=30,
        template_pool_json=["material_library_remix"],
    )
    db.add(lane_format)
    await db.commit()
    return channel, lane, account, lane_format


def _service(*, clock=None, autoflow=None, youtube=None, minimax=None) -> ChannelAgentService:
    clock = clock or FakeClock(datetime(2026, 5, 18, 9, 0, tzinfo=timezone.utc))
    queue = ChannelOpsQueueService(clock=clock)
    return ChannelAgentService(
        queue=queue,
        clock=clock,
        autoflow_client=autoflow or FakeAutoFlowClient(),
        youtube_client=youtube or FakeYouTubeClient(),
        minimax_client=minimax or FakeMiniMaxClient(),
    )


@pytest.mark.asyncio
async def test_dry_run_tick_writes_audit_without_tasks(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=True)
    seed = ManualSeed(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        prompt="make a test short",
        title_seed="test short",
        source_platforms_json=["youtube", "bilibili"],
    )
    service_session.add(seed)
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)

    assert audit.dry_run is True
    assert audit.ideas_discovered == 1
    assert audit.tasks_selected == 0
    tasks = (await service_session.execute(select(ProductionTask))).scalars().all()
    assert tasks == []
    assert audit.decision_summary_json["per_lane_eligible_count"][str(lane.id)] == 1


@pytest.mark.asyncio
async def test_active_tick_creates_task_and_plan_queue_item(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="make a test short",
            title_seed="test short",
        )
    )
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 1
    task = (await service_session.execute(select(ProductionTask))).scalar_one()
    assert task.state == "selected"
    assert task.channel_config_snapshot_json["channel"]["dry_run"] is False
    queue_item = (
        await service_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "plan_task")
        )
    ).scalar_one()
    assert queue_item.idempotency_key == f"plan_task:{task.id}"
    assert queue_item.channel_profile_id == channel.id


@pytest.mark.asyncio
async def test_plan_task_holds_when_autoflow_omits_youtube_upload(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        title_seed="test",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.commit()

    item = ChannelOpsQueueItem(
        kind="plan_task",
        idempotency_key=f"plan_task:{task.id}",
        payload_json={"production_task_id": str(task.id)},
    )
    service_session.add(item)
    await service_session.commit()

    await _service(autoflow=FakeAutoFlowClient(include_upload=False)).handle_plan_task(service_session, item)
    await service_session.refresh(task)

    assert task.state == "held"
    assert task.blocked_by_guard == "missing_youtube_upload_node"


@pytest.mark.asyncio
async def test_plan_task_enqueues_execute_with_channel_scope(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        title_seed="test",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.commit()

    item = ChannelOpsQueueItem(
        kind="plan_task",
        idempotency_key=f"plan_task:{task.id}",
        payload_json={"production_task_id": str(task.id)},
    )
    service_session.add(item)
    await service_session.commit()

    await _service().handle_plan_task(service_session, item)

    execute = (
        await service_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "execute_task")
        )
    ).scalar_one()
    assert execute.channel_profile_id == channel.id


@pytest.mark.asyncio
async def test_publish_task_observes_upload_and_auto_enqueues_promote(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        title_seed="test",
        state="uploaded_private",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.commit()
    item = ChannelOpsQueueItem(
        kind="publish_task",
        idempotency_key=f"publish_task:{task.id}",
        payload_json={"production_task_id": str(task.id), "youtube": {"video_id": "yt-video-1"}},
    )
    service_session.add(item)
    await service_session.commit()

    publication = await _service().handle_publish_task(service_session, item)

    assert publication.platform_content_id == "yt-video-1"
    assert publication.current_privacy == "private"
    assert publication.publish_status == "uploaded"
    promote = (
        await service_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "promote_publication")
        )
    ).scalar_one()
    assert str(publication.id) in promote.idempotency_key
    assert promote.channel_profile_id == channel.id


@pytest.mark.asyncio
async def test_promote_publication_schedules_youtube_publish_at(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        state="uploaded_private",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.flush()
    publication = PublicationRecord(
        production_task_id=task.id,
        platform="youtube",
        account_id=account.id,
        platform_content_id="yt-video-1",
        title="test",
        desired_privacy="public",
        current_privacy="private",
        publish_status="uploaded",
        compliance_disposition="assumed_fair_use",
    )
    service_session.add(publication)
    await service_session.commit()
    item = ChannelOpsQueueItem(
        kind="promote_publication",
        idempotency_key=f"promote_publication:{publication.id}:public:2026-05-18T10:00:00+00:00",
        payload_json={"publication_id": str(publication.id), "scheduled_at": "2026-05-18T10:00:00+00:00"},
    )
    service_session.add(item)
    await service_session.commit()
    youtube = FakeYouTubeClient()

    await _service(youtube=youtube).handle_promote_publication(service_session, item)
    await service_session.refresh(publication)

    assert publication.publish_status == "scheduled"
    assert publication.scheduled_publish_at is not None
    assert youtube.scheduled[0]["video_id"] == "yt-video-1"


@pytest.mark.asyncio
async def test_quota_exhaustion_holds_publish_and_alerts(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        state="uploaded_private",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.commit()
    item = ChannelOpsQueueItem(
        kind="publish_task",
        idempotency_key=f"publish_task:{task.id}",
        payload_json={"production_task_id": str(task.id), "youtube": {"video_id": "yt-video-1"}},
    )
    service_session.add(item)
    await service_session.commit()

    await _service(youtube=FakeYouTubeClient(quota_remaining_fraction=0.05)).handle_publish_task(
        service_session,
        item,
    )
    await service_session.refresh(task)

    assert task.state == "held"
    alert = (
        await service_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "send_alert")
        )
    ).scalar_one()
    assert alert.payload_json["type"] == "quota_below_20pct"
    assert alert.channel_profile_id == channel.id


@pytest.mark.asyncio
async def test_token_refresh_failure_pauses_account_and_alerts(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    item = ChannelOpsQueueItem(
        kind="account_health",
        idempotency_key=f"account_health:{account.id}:2026-05-18-09",
        payload_json={"account_id": str(account.id)},
    )
    service_session.add(item)
    await service_session.commit()

    await _service(youtube=FakeYouTubeClient(token_valid=False)).handle_account_health(service_session, item)
    await service_session.refresh(account)

    assert account.enabled is False
    assert account.last_token_check_status == "invalid"
    alert = (
        await service_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "send_alert")
        )
    ).scalar_one()
    assert alert.payload_json["type"] == "token_expiring_24h"
    assert alert.channel_profile_id == channel.id


@pytest.mark.asyncio
async def test_severe_takedown_pauses_account_and_alerts(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        state="published",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.flush()
    publication = PublicationRecord(
        production_task_id=task.id,
        platform="youtube",
        account_id=account.id,
        platform_content_id="yt-video-1",
        title="test",
        desired_privacy="public",
        current_privacy="public",
        publish_status="public",
        compliance_disposition="known_risk_accepted",
    )
    service_session.add(publication)
    await service_session.commit()

    event = await _service().log_takedown_event(
        service_session,
        publication_id=publication.id,
        event_type="strike",
        severity="severe",
        raw_payload={"reason": "copyright strike"},
    )
    await service_session.refresh(account)

    assert event.severity == "severe"
    assert account.enabled is False
    alert = (
        await service_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "send_alert")
        )
    ).scalar_one()
    assert alert.payload_json["type"] == "takedown_event_logged"
    assert alert.channel_profile_id == channel.id
