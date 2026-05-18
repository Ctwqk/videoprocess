from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.channel_agent.alerts import build_alert_payload
from app.channel_agent.clock import FakeClock
from app.channel_agent.queue import ChannelOpsQueueService, utc_hour_bucket
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
async def channel_agent_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        for table in CHANNEL_AGENT_TABLES:
            await conn.run_sync(table.create)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session

    await engine.dispose()


@pytest.mark.asyncio
async def test_channel_agent_models_persist_defaults(channel_agent_session):
    channel = ChannelProfile(
        name="Shorts Lab",
        positioning="Auto shorts channel",
        language="zh",
    )
    channel_agent_session.add(channel)
    await channel_agent_session.flush()

    lane = TopicLane(
        channel_profile_id=channel.id,
        name="cartoon clips",
        keywords_json=["tom", "jerry"],
    )
    account = PublishingAccount(
        channel_profile_id=channel.id,
        platform="youtube",
        account_label="yt-main",
        platform_account_id="channel-123",
        credential_ref="youtube/main",
    )
    channel_agent_session.add_all([lane, account])
    await channel_agent_session.flush()

    lane_format = LaneFormatMatrix(
        topic_lane_id=lane.id,
        format_key="shorts_9x16",
        target_duration_sec=30,
        template_pool_json=["material_library_remix"],
    )
    channel_agent_session.add(lane_format)
    await channel_agent_session.flush()

    task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        lane_format_id=lane_format.id,
        target_account_id=account.id,
        source="manual_seed",
        title_seed="funny chase",
        prompt="Make a 30 second chase short",
        channel_config_version_snapshot=channel.config_version,
        channel_config_snapshot_json={"risk_policy_json": channel.risk_policy_json},
    )
    channel_agent_session.add(task)
    await channel_agent_session.flush()

    publication = PublicationRecord(
        production_task_id=task.id,
        platform="youtube",
        account_id=account.id,
        platform_content_id="video-123",
        title="funny chase",
        desired_privacy="public",
        current_privacy="private",
        publish_status="uploaded",
        compliance_disposition="assumed_fair_use",
    )
    channel_agent_session.add(publication)
    await channel_agent_session.commit()

    assert channel.dry_run is True
    assert channel.enabled is True
    assert account.external_asset_auto_publish is False
    assert lane_format.default_publish_visibility == "public"
    assert task.state == "seeded"
    assert task.transition_history_json == []
    assert publication.compliance_disposition == "assumed_fair_use"


@pytest.mark.asyncio
async def test_queue_service_idempotency_priority_and_dead_letter(channel_agent_session):
    clock = FakeClock(datetime(2026, 5, 18, 8, 30, tzinfo=timezone.utc))
    queue = ChannelOpsQueueService(clock=clock)

    first = await queue.enqueue(
        channel_agent_session,
        kind="agent_tick",
        idempotency_key="agent_tick:channel:2026-05-18-08",
        payload={"channel_id": "channel"},
        priority=50,
    )
    duplicate = await queue.enqueue(
        channel_agent_session,
        kind="agent_tick",
        idempotency_key="agent_tick:channel:2026-05-18-08",
        payload={"ignored": True},
        priority=1,
    )
    second = await queue.enqueue(
        channel_agent_session,
        kind="send_alert",
        idempotency_key="send_alert:quota:account:2026-05-18-08",
        payload={"type": "quota"},
        priority=10,
    )

    assert duplicate.id == first.id
    assert duplicate.payload_json == {"channel_id": "channel"}

    claimed = await queue.claim_next(channel_agent_session, worker_id="worker-1")
    assert claimed is not None
    assert claimed.id == second.id
    assert claimed.status == "running"
    assert claimed.locked_by == "worker-1"

    await queue.mark_failed_or_retry(channel_agent_session, claimed, "boom", max_attempts=1)
    await channel_agent_session.refresh(claimed)
    assert claimed.status == "dead_lettered"
    assert claimed.dead_letter_at is not None


def test_utc_hour_bucket_and_alert_payloads_are_stable():
    now = datetime(2026, 5, 18, 8, 45, 12, tzinfo=timezone.utc)
    assert utc_hour_bucket(now) == "2026-05-18-08"

    payload = build_alert_payload(
        "token_expiring_24h",
        resource_id="account-1",
        severity="warning",
        message="YouTube token expires soon",
        details={"hours_remaining": 23},
        now=now,
    )

    assert payload["type"] == "token_expiring_24h"
    assert payload["resource_id"] == "account-1"
    assert payload["severity"] == "warning"
    assert payload["details"]["hours_remaining"] == 23
    assert payload["dedupe_key"] == "send_alert:token_expiring_24h:account-1:2026-05-18-08"
