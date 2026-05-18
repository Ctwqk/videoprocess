from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.channel_agent.alerts import AlertService, build_alert_payload
from app.channel_agent.clients import MiniMaxImageClient
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
from app.schemas.channel_agent import QueueItemRead


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


def _sqlite_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


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


@pytest.mark.asyncio
async def test_lane_format_source_platforms_and_queue_channel_scope(channel_agent_session):
    channel = ChannelProfile(
        name="Platform Lab",
        positioning="Cross-platform shorts",
        language="zh",
    )
    channel_agent_session.add(channel)
    await channel_agent_session.flush()

    lane = TopicLane(
        channel_profile_id=channel.id,
        name="platform clips",
    )
    channel_agent_session.add(lane)
    await channel_agent_session.flush()

    lane_format = LaneFormatMatrix(
        topic_lane_id=lane.id,
        format_key="shorts_9x16",
        source_platforms_json=["bilibili", "youtube"],
    )
    channel_agent_session.add(lane_format)
    await channel_agent_session.commit()
    await channel_agent_session.refresh(lane_format)

    queue = ChannelOpsQueueService()
    item = await queue.enqueue(
        channel_agent_session,
        kind="agent_tick",
        idempotency_key=f"agent_tick:{channel.id}:2026-05-18-08",
        payload={"channel_id": str(channel.id)},
        channel_profile_id=channel.id,
    )
    await channel_agent_session.refresh(item)

    assert lane_format.source_platforms_json == ["bilibili", "youtube"]
    assert item.channel_profile_id == channel.id

    queue_read = QueueItemRead(
        id=str(item.id),
        kind=item.kind,
        idempotency_key=item.idempotency_key,
        priority=item.priority,
        status=item.status,
        payload_json=dict(item.payload_json or {}),
        attempt_count=item.attempt_count,
        channel_profile_id=str(item.channel_profile_id),
    )
    assert queue_read.channel_profile_id == str(channel.id)


@pytest.mark.asyncio
async def test_enqueue_recovers_from_idempotency_integrity_race(channel_agent_session, monkeypatch):
    existing = ChannelOpsQueueItem(
        kind="agent_tick",
        idempotency_key="agent_tick:race:2026-05-18-08",
        payload_json={"channel_id": "race"},
        priority=20,
        run_after=datetime(2026, 5, 18, 8, 0, tzinfo=timezone.utc),
    )
    channel_agent_session.add(existing)
    await channel_agent_session.commit()
    await channel_agent_session.refresh(existing)

    queue = ChannelOpsQueueService()
    lookups = 0

    async def fake_get_by_key(db, idempotency_key):
        nonlocal lookups
        lookups += 1
        assert idempotency_key == "agent_tick:race:2026-05-18-08"
        return None if lookups == 1 else existing

    async def fake_commit():
        raise IntegrityError("insert channel_ops_queue_items", {}, Exception("duplicate idempotency key"))

    monkeypatch.setattr(queue, "get_by_key", fake_get_by_key)
    monkeypatch.setattr(channel_agent_session, "commit", fake_commit)

    item = await queue.enqueue(
        channel_agent_session,
        kind="agent_tick",
        idempotency_key="agent_tick:race:2026-05-18-08",
        payload={"channel_id": "race"},
        priority=20,
    )

    assert item.id == existing.id
    assert item.payload_json == {"channel_id": "race"}
    assert lookups == 2


@pytest.mark.asyncio
async def test_queue_retry_uses_exponential_backoff(channel_agent_session):
    clock = FakeClock(datetime(2026, 5, 18, 8, 0, tzinfo=timezone.utc))
    queue = ChannelOpsQueueService(clock=clock)

    await queue.enqueue(
        channel_agent_session,
        kind="agent_tick",
        idempotency_key="agent_tick:retry:2026-05-18-08",
        payload={"channel_id": "retry"},
        priority=20,
        max_attempts=3,
    )

    first_claim = await queue.claim_next(channel_agent_session, worker_id="worker-1")
    assert first_claim is not None
    await queue.mark_failed_or_retry(channel_agent_session, first_claim, "first failure")
    await channel_agent_session.refresh(first_claim)
    assert _sqlite_utc(first_claim.run_after) == datetime(2026, 5, 18, 8, 5, tzinfo=timezone.utc)

    clock.advance(timedelta(minutes=5))
    second_claim = await queue.claim_next(channel_agent_session, worker_id="worker-1")
    assert second_claim is not None
    await queue.mark_failed_or_retry(channel_agent_session, second_claim, "second failure")
    await channel_agent_session.refresh(second_claim)
    assert _sqlite_utc(second_claim.run_after) == datetime(2026, 5, 18, 8, 15, tzinfo=timezone.utc)


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


@pytest.mark.asyncio
async def test_alert_service_posts_slack_webhook_payload():
    requests: list[dict[str, str]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append({"url": str(request.url), "body": request.content.decode("utf-8")})
        return httpx.Response(200, json={"ok": True})

    service = AlertService(
        slack_webhook_url="https://hooks.slack.test/channel-ops",
        transport=httpx.MockTransport(handler),
    )
    result = await service.send(
        {
            "type": "quota_below_20pct",
            "resource_id": "account-1",
            "severity": "warning",
            "message": "YouTube quota remaining below 20%",
            "details": {"remaining_fraction": 0.12},
        }
    )

    assert result["status"] == "sent"
    assert result["slack_status_code"] == 200
    assert requests[0]["url"] == "https://hooks.slack.test/channel-ops"
    assert "quota_below_20pct" in requests[0]["body"]


@pytest.mark.asyncio
async def test_minimax_image_client_posts_cn_endpoint_and_parses_url():
    requests: list[dict[str, str]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append({"url": str(request.url), "auth": request.headers["Authorization"], "body": request.content.decode()})
        return httpx.Response(
            200,
            json={
                "id": "image-task-1",
                "data": {"image_urls": ["https://cdn.example/thumb.png"]},
                "base_resp": {"status_code": 0, "status_msg": "success"},
            },
        )

    client = MiniMaxImageClient(
        api_key="test-key",
        endpoint="https://api.minimaxi.com/v1/image_generation",
        model="image-01",
        timeout_seconds=30,
        retry_count=1,
        max_qps=0,
        transport=httpx.MockTransport(handler),
    )
    result = await client.generate_thumbnail(prompt="make a test short", title="test")

    assert result["image_url"] == "https://cdn.example/thumb.png"
    assert result["request_id"] == "image-task-1"
    assert requests[0]["url"] == "https://api.minimaxi.com/v1/image_generation"
    assert requests[0]["auth"] == "Bearer test-key"
    assert '"model":"image-01"' in requests[0]["body"]
    assert '"response_format":"url"' in requests[0]["body"]
