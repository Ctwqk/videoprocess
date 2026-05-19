from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

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
from app.schemas.autoflow import AutoFlowRequest


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


def test_lane_prompt_template_is_structured():
    from app.channel_agent.lane_prompts import build_lane_prompt

    prompt = build_lane_prompt(
        lane_name="Tech News",
        lane_description="Daily AI product updates",
        keywords=["AI", "robots"],
        format_key="shorts_9x16",
        duration_sec=30,
        aspect_ratio="9:16",
    )

    assert 'Create a shorts_9x16 video for the "Tech News" topic.' in prompt
    assert "Theme: Daily AI product updates" in prompt
    assert "Keywords: AI, robots" in prompt
    assert "Target duration: 30s, aspect ratio 9:16." in prompt


def test_autoflow_request_uses_task_snapshot_configuration():
    task = ProductionTask(
        channel_profile_id=uuid.uuid4(),
        target_account_id=uuid.uuid4(),
        source="lane_seed",
        prompt="Create a tech short",
        title_seed="AI news",
        source_platforms_json=["bilibili", "youtube"],
        material_library_ids_json=["library-1"],
        uses_external_assets=True,
        channel_config_snapshot_json={
            "channel": {
                "id": "channel-1",
                "default_aspect_ratio": "16:9",
                "risk_policy_json": {
                    "source_strategy": "external_search",
                    "planning_mode": "ai_graph",
                },
            },
            "lane": {"id": "lane-1", "name": "Tech"},
            "lane_format": {
                "id": "format-1",
                "format_key": "long_16x9",
                "target_duration_sec": 60,
                "template_pool_json": ["news_remix"],
                "default_publish_visibility": "unlisted",
            },
            "manual_seed": {"constraints_json": {"tone": "calm"}},
        },
    )
    request = _service()._autoflow_request(task)

    assert request["duration_sec"] == 60
    assert request["aspect_ratio"] == "16:9"
    assert request["source_platforms"] == ["bilibili", "youtube"]
    assert request["source_strategy"] == "external_research"
    assert request["planning_mode"] == "ai_graph"
    assert request["constraints"]["template_pool_json"] == ["news_remix"]
    assert request["constraints"]["tone"] == "calm"
    assert request["publish_mode"] == "unlisted_upload"
    AutoFlowRequest.model_validate(request)


def test_autoflow_request_falls_back_for_invalid_strategy_and_planning_mode():
    task = ProductionTask(
        channel_profile_id=uuid.uuid4(),
        target_account_id=uuid.uuid4(),
        source="lane_seed",
        prompt="Create a tech short",
        title_seed="AI news",
        source_platforms_json="youtube",
        material_library_ids_json="library-1",
        uses_external_assets=False,
        channel_config_snapshot_json={
            "channel": {
                "id": "channel-1",
                "default_aspect_ratio": "9:16",
                "risk_policy_json": {
                    "source_strategy": "unknown_source",
                    "planning_mode": "surprise_me",
                },
            },
            "lane_format": {
                "id": "format-1",
                "target_duration_sec": -5,
                "template_pool_json": "news_remix",
            },
            "manual_seed": {"constraints_json": "not-a-dict"},
        },
    )

    request = _service()._autoflow_request(task)

    assert request["source_strategy"] == "auto"
    assert request["planning_mode"] == "auto"
    assert request["source_platforms"] == ["youtube"]
    assert request["material_library_ids"] == ["library-1"]
    assert request["duration_sec"] == 30
    assert request["constraints"]["template_pool_json"] == ["news_remix"]
    AutoFlowRequest.model_validate(request)


def test_autoflow_request_uses_lane_format_platforms_for_source_policy():
    task = ProductionTask(
        channel_profile_id=uuid.uuid4(),
        target_account_id=uuid.uuid4(),
        source="lane_seed",
        prompt="Create a tech short",
        title_seed="AI news",
        source_platforms_json=[],
        uses_external_assets=False,
        channel_config_snapshot_json={
            "channel": {"id": "channel-1", "default_aspect_ratio": "9:16"},
            "lane_format": {
                "id": "format-1",
                "source_platforms_json": ["bilibili"],
                "target_duration_sec": 45,
            },
        },
    )

    request = _service()._autoflow_request(task)

    assert request["source_platforms"] == ["bilibili"]
    assert request["source_policy"] == "remix_with_review"
    AutoFlowRequest.model_validate(request)


def test_desired_privacy_falls_back_to_unlisted_not_public():
    task = ProductionTask(
        channel_profile_id=uuid.uuid4(),
        target_account_id=uuid.uuid4(),
        source="lane_seed",
        prompt="Create a short",
        channel_config_snapshot_json={"lane_format": {}},
    )
    account = PublishingAccount(
        channel_profile_id=uuid.uuid4(),
        account_label="main",
        platform_account_id="yt",
        credential_ref="youtube/main",
        default_privacy="public",
    )

    assert _service()._desired_privacy(task, account) == "unlisted"


def test_desired_privacy_preserves_private_account_default():
    task = ProductionTask(
        channel_profile_id=uuid.uuid4(),
        target_account_id=uuid.uuid4(),
        source="lane_seed",
        prompt="Create a short",
        channel_config_snapshot_json={"lane_format": {"default_publish_visibility": "public"}},
    )
    account = PublishingAccount(
        channel_profile_id=uuid.uuid4(),
        account_label="main",
        platform_account_id="yt",
        credential_ref="youtube/main",
        default_privacy="private",
    )

    assert _service()._desired_privacy(task, account) == "private"


def test_autoflow_publish_mode_uses_safe_account_snapshot_default():
    task = ProductionTask(
        channel_profile_id=uuid.uuid4(),
        target_account_id=uuid.uuid4(),
        source="lane_seed",
        prompt="Create a short",
        channel_config_snapshot_json={
            "account": {"default_privacy": "private"},
            "lane_format": {"default_publish_visibility": "public"},
        },
    )

    request = _service()._autoflow_request(task)

    assert request["publish_mode"] == "private_upload"
    AutoFlowRequest.model_validate(request)


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
async def test_active_tick_stores_lane_format_platforms_when_seed_has_none(service_session):
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)
    lane_format.source_platforms_json = ["bilibili"]
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="make a test short",
            title_seed="test short",
            source_platforms_json=[],
        )
    )
    await service_session.commit()

    await _service().tick(service_session, channel_id=channel.id)

    task = (await service_session.execute(select(ProductionTask))).scalar_one()
    assert task.source_platforms_json == ["bilibili"]
    assert task.uses_external_assets is True


@pytest.mark.asyncio
async def test_lane_driven_tick_creates_task_without_manual_seed(service_session):
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)
    lane.description = "Daily AI updates"
    lane.keywords_json = ["AI"]
    lane_format.source_platforms_json = ["bilibili", "youtube"]
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 1
    task = (await service_session.execute(select(ProductionTask))).scalar_one()
    assert task.source == "lane_seed"
    assert task.manual_seed_id is None
    assert task.topic_lane_id == lane.id
    assert task.lane_format_id == lane_format.id
    assert task.source_platforms_json == ["bilibili", "youtube"]
    assert task.uses_external_assets is True
    assert "Daily AI updates" in task.prompt


@pytest.mark.asyncio
async def test_manual_seed_consumes_first_then_lane_driven_fills_budget(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    lane.max_posts_per_day = 3
    second_account = PublishingAccount(
        channel_profile_id=channel.id,
        account_label="second",
        platform_account_id="yt-2",
        credential_ref="youtube/second",
        external_asset_auto_publish=True,
    )
    service_session.add(second_account)
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=second_account.id,
            prompt="manual short",
            title_seed="manual",
        )
    )
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)
    tasks = (
        await service_session.execute(select(ProductionTask).order_by(ProductionTask.created_at.asc()))
    ).scalars().all()

    assert audit.tasks_selected == 2
    assert [task.source for task in tasks] == ["manual_seed", "lane_seed"]
    assert {task.target_account_id for task in tasks} == {account.id, second_account.id}


@pytest.mark.asyncio
async def test_dry_run_evaluates_candidates_without_creating_tasks_or_queue(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=True)
    service_session.add(
        ProductionTask(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            source="manual_seed",
            prompt="held task",
            state="held",
            channel_config_snapshot_json={},
        )
    )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="dry run candidate",
            title_seed="dry",
        )
    )
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)

    tasks = (await service_session.execute(select(ProductionTask))).scalars().all()
    queue_items = (await service_session.execute(select(ChannelOpsQueueItem))).scalars().all()
    rejected = audit.decision_summary_json["rejected_candidates"]

    assert len(tasks) == 1
    assert queue_items == []
    assert audit.candidates_scored >= 1
    assert rejected[0]["guard"] == "account_concurrency"


@pytest.mark.asyncio
async def test_dry_run_low_supply_records_guard_without_alert_queue(service_session):
    channel = ChannelProfile(name="Channel", language="zh", dry_run=True)
    service_session.add(channel)
    await service_session.flush()
    lane = TopicLane(channel_profile_id=channel.id, name="empty lane")
    account = PublishingAccount(
        channel_profile_id=channel.id,
        account_label="main",
        platform_account_id="yt-1",
        credential_ref="youtube/main",
    )
    service_session.add_all([lane, account])
    await service_session.flush()
    for offset in (2, 1):
        service_session.add(
            AgentTickAudit(
                channel_profile_id=channel.id,
                tick_id=f"prior:{offset}",
                dry_run=True,
                started_at=datetime(2026, 5, 18, 9 - offset, 0, tzinfo=timezone.utc),
                finished_at=datetime(2026, 5, 18, 9 - offset, 1, tzinfo=timezone.utc),
                decision_summary_json={"per_lane_eligible_count": {str(lane.id): 0}},
            )
        )
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)

    queue_items = (await service_session.execute(select(ChannelOpsQueueItem))).scalars().all()
    assert queue_items == []
    assert audit.guards_triggered_json == [{"guard": "material_supply_low", "lane_id": str(lane.id)}]


@pytest.mark.asyncio
async def test_untargeted_manual_seed_uses_free_account_before_lane_candidate(service_session):
    channel, lane, busy_account, _lane_format = await _channel_graph(service_session, dry_run=False)
    lane.max_posts_per_day = 2
    free_account = PublishingAccount(
        channel_profile_id=channel.id,
        account_label="free",
        platform_account_id="yt-2",
        credential_ref="youtube/free",
        external_asset_auto_publish=True,
    )
    service_session.add(free_account)
    service_session.add(
        ProductionTask(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=busy_account.id,
            source="lane_seed",
            prompt="busy task",
            state="held",
            channel_config_snapshot_json={},
        )
    )
    seed = ManualSeed(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        prompt="manual short",
        title_seed="manual",
    )
    service_session.add(seed)
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)

    created = (
        await service_session.execute(
            select(ProductionTask)
            .where(ProductionTask.prompt != "busy task")
            .order_by(ProductionTask.created_at.asc())
        )
    ).scalars().all()
    await service_session.refresh(seed)

    assert audit.tasks_selected == 1
    assert len(created) == 1
    assert created[0].source == "manual_seed"
    assert created[0].target_account_id == free_account.id
    assert created[0].manual_seed_id == seed.id
    assert seed.status == "exhausted"


@pytest.mark.asyncio
async def test_producing_task_blocks_same_account_candidate(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    service_session.add(
        ProductionTask(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            source="lane_seed",
            prompt="producing task",
            state="producing",
            channel_config_snapshot_json={},
        )
    )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="manual blocked by producing",
            title_seed="blocked",
        )
    )
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)

    tasks = (await service_session.execute(select(ProductionTask))).scalars().all()
    queue_items = (await service_session.execute(select(ChannelOpsQueueItem))).scalars().all()
    rejected = audit.decision_summary_json["rejected_candidates"]
    assert len(tasks) == 1
    assert queue_items == []
    assert audit.tasks_selected == 0
    assert rejected[0]["guard"] == "account_concurrency"


@pytest.mark.asyncio
async def test_account_concurrency_guard_blocks_active_account(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    service_session.add(
        ProductionTask(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            source="manual_seed",
            prompt="held task",
            state="held",
            channel_config_snapshot_json={},
        )
    )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="new task",
            title_seed="new",
        )
    )
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 0
    assert audit.tasks_rejected >= 1
    assert audit.decision_summary_json["rejected_candidates"][0]["guard"] == "account_concurrency"


@pytest.mark.asyncio
async def test_consecutive_upload_failure_guard_uses_recent_window_and_alerts(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    for index, reason in enumerate(
        ["youtube upload failed", "ok", "quota exhausted", "thumbnail failed", "publish failed"]
    ):
        state = "failed" if index != 1 else "measured"
        task = ProductionTask(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            source="manual_seed",
            prompt=f"task {index}",
            state=state,
            failure_reason=reason if state == "failed" else None,
            channel_config_snapshot_json={},
            created_at=clock.now() - timedelta(hours=5 - index),
        )
        service_session.add(task)
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="blocked",
            title_seed="blocked",
        )
    )
    await service_session.commit()

    audit = await _service(clock=clock).tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 0
    rejected = audit.decision_summary_json["rejected_candidates"][0]
    assert rejected["guard"] == "consecutive_upload_failure"
    alert = (
        await service_session.execute(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "send_alert"))
    ).scalar_one()
    assert "pause the account" in alert.payload_json["message"]
    assert str(account.id) in alert.payload_json["message"]


@pytest.mark.asyncio
async def test_consecutive_upload_failure_guard_allows_old_recent_window(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    for index, reason in enumerate(
        ["youtube upload failed", "quota exhausted", "thumbnail failed", "publish failed", "oauth failed"]
    ):
        task = ProductionTask(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            source="manual_seed",
            prompt=f"task {index}",
            state="failed",
            failure_reason=reason,
            channel_config_snapshot_json={},
            created_at=clock.now() - timedelta(hours=30 - index),
        )
        service_session.add(task)
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="allowed",
            title_seed="allowed",
        )
    )
    await service_session.commit()

    audit = await _service(clock=clock).tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 1
    assert audit.decision_summary_json["rejected_candidates"] == []
    alerts = (await service_session.execute(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "send_alert"))).scalars().all()
    assert alerts == []


@pytest.mark.asyncio
async def test_lane_cadence_guard_counts_publications_not_created_tasks(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)
    lane.max_posts_per_day = 1
    published_task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        lane_format_id=lane_format.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="published",
        state="published",
        channel_config_snapshot_json={},
    )
    service_session.add(published_task)
    await service_session.flush()
    service_session.add(
        PublicationRecord(
            production_task_id=published_task.id,
            platform="youtube",
            account_id=account.id,
            platform_content_id="yt-1",
            title="published",
            desired_privacy="unlisted",
            current_privacy="private",
            publish_status="scheduled",
            scheduled_publish_at=clock.now() - timedelta(hours=1),
            compliance_disposition="assumed_fair_use",
        )
    )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="new",
            title_seed="new",
        )
    )
    await service_session.commit()

    audit = await _service(clock=clock).tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 0
    assert audit.decision_summary_json["rejected_candidates"][0]["guard"] == "lane_cadence"


@pytest.mark.asyncio
async def test_lane_cadence_guard_ignores_held_tasks_without_publications(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)
    lane.max_posts_per_day = 1
    busy_account = PublishingAccount(
        channel_profile_id=channel.id,
        account_label="busy",
        platform_account_id="yt-2",
        credential_ref="youtube/busy",
        external_asset_auto_publish=True,
    )
    service_session.add(busy_account)
    await service_session.flush()
    for index in range(4):
        service_session.add(
            ProductionTask(
                channel_profile_id=channel.id,
                topic_lane_id=lane.id,
                lane_format_id=lane_format.id,
                target_account_id=busy_account.id,
                source="manual_seed",
                prompt=f"held {index}",
                state="held",
                channel_config_snapshot_json={},
                created_at=clock.now() - timedelta(hours=index),
            )
        )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="new",
            title_seed="new",
        )
    )
    await service_session.commit()

    audit = await _service(clock=clock).tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 1
    assert audit.decision_summary_json["rejected_candidates"] == []


@pytest.mark.asyncio
async def test_dry_run_without_enabled_account_audits_rejection(service_session):
    channel = ChannelProfile(name="Channel", language="zh", dry_run=True)
    service_session.add(channel)
    await service_session.flush()
    lane = TopicLane(channel_profile_id=channel.id, name="lane")
    service_session.add(lane)
    await service_session.flush()
    service_session.add(
        LaneFormatMatrix(
            topic_lane_id=lane.id,
            format_key="shorts_9x16",
            target_duration_sec=30,
            template_pool_json=["material_library_remix"],
        )
    )
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)

    tasks = (await service_session.execute(select(ProductionTask))).scalars().all()
    queue_items = (await service_session.execute(select(ChannelOpsQueueItem))).scalars().all()
    rejected = audit.decision_summary_json["rejected_candidates"]
    assert tasks == []
    assert queue_items == []
    assert audit.tasks_rejected >= 1
    assert rejected[0]["guard"] == "no_enabled_account"


@pytest.mark.asyncio
async def test_manual_seed_candidate_ids_include_seed_id(service_session):
    channel, lane, first_account, lane_format = await _channel_graph(service_session, dry_run=True)
    second_account = PublishingAccount(
        channel_profile_id=channel.id,
        account_label="second",
        platform_account_id="yt-2",
        credential_ref="youtube/second",
    )
    first_seed = ManualSeed(channel_profile_id=channel.id, topic_lane_id=lane.id, prompt="first manual")
    second_seed = ManualSeed(channel_profile_id=channel.id, topic_lane_id=lane.id, prompt="second manual")
    service_session.add_all([second_account, first_seed, second_seed])
    await service_session.commit()

    candidates = await _service()._build_tick_candidates(
        service_session,
        channel=channel,
        lanes=[lane],
        accounts=[first_account, second_account],
        seeds=[first_seed, second_seed],
        lane_formats_by_lane={str(lane.id): [lane_format]},
        bucket="2026-05-18-09",
    )

    manual_ids = [candidate["candidate_id"] for candidate in candidates if candidate["source"] == "manual_seed"]
    assert manual_ids == [
        f"manual_seed:{first_seed.id}:lane:{lane.id}:format:{lane_format.id}:2026-05-18-09",
        f"manual_seed:{second_seed.id}:lane:{lane.id}:format:{lane_format.id}:2026-05-18-09",
    ]


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
async def test_publish_task_holds_when_external_platforms_come_from_snapshot(service_session):
    channel, lane, account, _lane_format = await _channel_graph(
        service_session,
        dry_run=False,
        external_auto=False,
    )
    task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        title_seed="test",
        source_platforms_json=[],
        uses_external_assets=False,
        state="uploaded_private",
        channel_config_snapshot_json={"lane_format": {"source_platforms_json": ["bilibili"]}},
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
    await service_session.refresh(task)

    assert publication is not None
    assert task.state == "held"
    assert task.blocked_by_guard == "external_asset_auto_publish_required"
    assert publication.publish_status == "held"


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
