from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.channel_agent import router
from app.db import get_db
from app.models.channel_agent import (
    AgentTickAudit,
    ChannelOpsQueueItem,
    ChannelProfile,
    DecisionAuditEntry,
    DiscoverySignal,
    FeedbackSnapshot,
    LaneFormatMatrix,
    LearningState,
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
    DiscoverySignal.__table__,
    ProductionTask.__table__,
    MaterialUsageLedger.__table__,
    PublicationRecord.__table__,
    TakedownEvent.__table__,
    FeedbackSnapshot.__table__,
    DecisionAuditEntry.__table__,
    LearningState.__table__,
)


@pytest.fixture
async def api_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        for table in CHANNEL_AGENT_TABLES:
            await conn.run_sync(table.create)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session

    await engine.dispose()


def _app(db_session):
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: db_session
    return app


@pytest.mark.asyncio
async def test_channel_agent_api_config_seed_enqueue_and_status(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        created_channel = await client.post(
            "/api/v1/channel-agent/channels",
            json={"name": "Ops Channel", "language": "zh"},
        )
        assert created_channel.status_code == 200
        channel = created_channel.json()
        assert channel["dry_run"] is True

        lane_response = await client.post(
            f"/api/v1/channel-agent/channels/{channel['id']}/lanes",
            json={"name": "Cartoons", "keywords_json": ["tom", "jerry"]},
        )
        assert lane_response.status_code == 200
        lane = lane_response.json()

        account_response = await client.post(
            f"/api/v1/channel-agent/channels/{channel['id']}/accounts",
            json={
                "account_label": "yt-main",
                "platform_account_id": "yt-1",
                "credential_ref": "youtube/main",
                "external_asset_auto_publish": True,
            },
        )
        assert account_response.status_code == 200
        account = account_response.json()

        format_response = await client.post(
            f"/api/v1/channel-agent/lanes/{lane['id']}/formats",
            json={"format_key": "shorts_9x16", "template_pool_json": ["material_library_remix"]},
        )
        assert format_response.status_code == 200

        seed_response = await client.post(
            f"/api/v1/channel-agent/channels/{channel['id']}/manual-seeds",
            json={
                "topic_lane_id": lane["id"],
                "target_account_id": account["id"],
                "prompt": "make a 30 second short",
                "title_seed": "test short",
                "source_platforms_json": ["youtube", "bilibili"],
            },
        )
        assert seed_response.status_code == 200

        tick_response = await client.post(f"/api/v1/channel-agent/channels/{channel['id']}/enqueue-tick")
        assert tick_response.status_code == 200
        tick_item = tick_response.json()
        assert tick_item["kind"] == "agent_tick"
        assert tick_item["channel_profile_id"] == channel["id"]

        dry_run_response = await client.patch(
            f"/api/v1/channel-agent/channels/{channel['id']}/dry-run",
            json={"dry_run": False},
        )
        assert dry_run_response.status_code == 200
        assert dry_run_response.json()["dry_run"] is False

        halt_response = await client.post(
            f"/api/v1/channel-agent/channels/{channel['id']}/halt",
            json={"reason": "operator stop"},
        )
        assert halt_response.status_code == 200
        assert halt_response.json()["halted_at"] is not None

        resume_response = await client.post(f"/api/v1/channel-agent/channels/{channel['id']}/resume")
        assert resume_response.status_code == 200
        assert resume_response.json()["halted_at"] is None

        health_response = await client.get(f"/api/v1/channel-agent/channels/{channel['id']}/health")
        assert health_response.status_code == 200
        health = health_response.json()
        assert health["channel_id"] == channel["id"]
        assert health["queued_items"] == 1

        queue_response = await client.get(f"/api/v1/channel-agent/channels/{channel['id']}/queue")
        assert queue_response.status_code == 200
        queue_items = queue_response.json()
        assert len(queue_items) == 1
        assert queue_items[0]["channel_profile_id"] == channel["id"]

        tasks_response = await client.get(f"/api/v1/channel-agent/channels/{channel['id']}/tasks")
        assert tasks_response.status_code == 200
        assert tasks_response.json() == []


@pytest.mark.asyncio
async def test_enqueue_metrics_returns_channel_scoped_queue_item(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        created_channel = await client.post(
            "/api/v1/channel-agent/channels",
            json={"name": "Metrics Channel", "language": "zh"},
        )
        assert created_channel.status_code == 200
        channel = created_channel.json()

        account_response = await client.post(
            f"/api/v1/channel-agent/channels/{channel['id']}/accounts",
            json={
                "account_label": "yt-main",
                "platform_account_id": "yt-1",
                "credential_ref": "youtube/main",
            },
        )
        assert account_response.status_code == 200
        account = account_response.json()

        task = ProductionTask(
            channel_profile_id=uuid.UUID(channel["id"]),
            target_account_id=uuid.UUID(account["id"]),
            source="manual_seed",
            prompt="measure this short",
            channel_config_snapshot_json={},
        )
        api_session.add(task)
        await api_session.flush()
        publication = PublicationRecord(
            production_task_id=task.id,
            account_id=uuid.UUID(account["id"]),
            platform_content_id="yt-video-1",
            title="metrics short",
            compliance_disposition="assumed_fair_use",
        )
        api_session.add(publication)
        await api_session.commit()

        response = await client.post(f"/api/v1/channel-agent/publications/{publication.id}/enqueue-metrics")

        assert response.status_code == 200
        queue_item = response.json()
        assert queue_item["kind"] == "collect_metrics"
        assert queue_item["channel_profile_id"] == channel["id"]


@pytest.mark.asyncio
async def test_decisions_failures_task_audit_and_learning_api(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        channel = (await client.post("/api/v1/channel-agent/channels", json={"name": "Audit"})).json()
        account = (
            await client.post(
                f"/api/v1/channel-agent/channels/{channel['id']}/accounts",
                json={"account_label": "main", "platform_account_id": "yt", "credential_ref": "youtube/main"},
            )
        ).json()
        task = ProductionTask(
            channel_profile_id=uuid.UUID(channel["id"]),
            target_account_id=uuid.UUID(account["id"]),
            source="manual_seed",
            prompt="audit task",
            title_seed="audit",
            state="failed",
            failure_reason="quota exhausted",
            failure_category="quota",
            channel_config_snapshot_json={},
        )
        api_session.add(task)
        await api_session.flush()
        tick = AgentTickAudit(channel_profile_id=uuid.UUID(channel["id"]), tick_id="tick:audit", dry_run=False)
        api_session.add(tick)
        await api_session.flush()
        decision = DecisionAuditEntry(
            tick_audit_id=tick.id,
            channel_profile_id=uuid.UUID(channel["id"]),
            candidate_id="manual_seed:example",
            candidate_source="manual_seed",
            target_account_id=uuid.UUID(account["id"]),
            selected=True,
            created_task_id=task.id,
            guard_results_json=[{"guard": "account_concurrency", "verdict": "allow", "reason": "idle"}],
        )
        publication = PublicationRecord(
            production_task_id=task.id,
            account_id=uuid.UUID(account["id"]),
            platform_content_id="yt-audit",
            title="audit",
            compliance_disposition="assumed_fair_use",
        )
        state = LearningState(
            channel_profile_id=uuid.UUID(channel["id"]),
            dimension_type="source",
            dimension_key="manual_seed",
            window_days=7,
            sample_count=12,
            avg_reward=0.42,
            confidence=0.6,
            recommendation_json={"action": "observe"},
        )
        api_session.add_all([decision, publication, state])
        await api_session.commit()

        decisions = await client.get(f"/api/v1/channel-agent/channels/{channel['id']}/decisions")
        assert decisions.status_code == 200
        assert decisions.json()[0]["candidate_id"] == "manual_seed:example"

        failures = await client.get(f"/api/v1/channel-agent/channels/{channel['id']}/failures?days=7")
        assert failures.status_code == 200
        assert failures.json()["categories"]["quota"] == 1

        audit = await client.get(f"/api/v1/channel-agent/tasks/{task.id}/audit")
        assert audit.status_code == 200
        assert audit.json()["task"]["failure_category"] == "quota"
        assert audit.json()["decision"]["candidate_id"] == "manual_seed:example"

        learning = await client.get(f"/api/v1/channel-agent/channels/{channel['id']}/learning")
        assert learning.status_code == 200
        assert learning.json()["states"][0]["dimension_type"] == "source"


@pytest.mark.asyncio
async def test_channel_queue_and_health_are_channel_scoped(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        first = (await client.post("/api/v1/channel-agent/channels", json={"name": "A"})).json()
        second = (await client.post("/api/v1/channel-agent/channels", json={"name": "B"})).json()
        await client.post(f"/api/v1/channel-agent/channels/{first['id']}/enqueue-tick")
        await client.post(f"/api/v1/channel-agent/channels/{second['id']}/enqueue-tick")

        first_queue = (await client.get(f"/api/v1/channel-agent/channels/{first['id']}/queue")).json()
        first_health = (await client.get(f"/api/v1/channel-agent/channels/{first['id']}/health")).json()

        assert len(first_queue) == 1
        assert first_queue[0]["payload_json"]["channel_id"] == first["id"]
        assert first_health["queued_items"] == 1


@pytest.mark.asyncio
async def test_create_manual_seed_rejects_cross_channel_target_account(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        first = (await client.post("/api/v1/channel-agent/channels", json={"name": "A"})).json()
        second = (await client.post("/api/v1/channel-agent/channels", json={"name": "B"})).json()
        account = (
            await client.post(
                f"/api/v1/channel-agent/channels/{second['id']}/accounts",
                json={
                    "account_label": "b-main",
                    "platform_account_id": "yt-b",
                    "credential_ref": "youtube/b",
                },
            )
        ).json()

        response = await client.post(
            f"/api/v1/channel-agent/channels/{first['id']}/manual-seeds",
            json={
                "target_account_id": account["id"],
                "prompt": "this seed should not cross channels",
            },
        )

        assert response.status_code == 400


@pytest.mark.asyncio
async def test_channel_health_does_not_count_published_tasks_as_active(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        channel = (await client.post("/api/v1/channel-agent/channels", json={"name": "Health"})).json()
        account = (
            await client.post(
                f"/api/v1/channel-agent/channels/{channel['id']}/accounts",
                json={
                    "account_label": "main",
                    "platform_account_id": "yt",
                    "credential_ref": "youtube/main",
                },
            )
        ).json()
        published = ProductionTask(
            channel_profile_id=uuid.UUID(channel["id"]),
            target_account_id=uuid.UUID(account["id"]),
            source="manual_seed",
            prompt="published task",
            state="published",
            channel_config_snapshot_json={},
        )
        active = ProductionTask(
            channel_profile_id=uuid.UUID(channel["id"]),
            target_account_id=uuid.UUID(account["id"]),
            source="manual_seed",
            prompt="active task",
            state="scheduled",
            channel_config_snapshot_json={},
        )
        api_session.add_all([published, active])
        await api_session.commit()

        health = (await client.get(f"/api/v1/channel-agent/channels/{channel['id']}/health")).json()

        assert health["active_tasks"] == 1


@pytest.mark.asyncio
async def test_channel_health_reports_last_successful_measured_at(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        channel = (await client.post("/api/v1/channel-agent/channels", json={"name": "Health"})).json()
        account = (
            await client.post(
                f"/api/v1/channel-agent/channels/{channel['id']}/accounts",
                json={
                    "account_label": "main",
                    "platform_account_id": "yt",
                    "credential_ref": "youtube/main",
                },
            )
        ).json()
        task = ProductionTask(
            channel_profile_id=uuid.UUID(channel["id"]),
            target_account_id=uuid.UUID(account["id"]),
            source="manual_seed",
            prompt="measured task",
            state="measured",
            channel_config_snapshot_json={},
        )
        api_session.add(task)
        await api_session.flush()
        publication = PublicationRecord(
            production_task_id=task.id,
            account_id=uuid.UUID(account["id"]),
            platform_content_id="yt-video-1",
            title="measured",
            desired_privacy="private",
            current_privacy="private",
            publish_status="scheduled",
            compliance_disposition="assumed_fair_use",
        )
        api_session.add(publication)
        await api_session.flush()
        older = datetime(2026, 5, 19, 10, 0, tzinfo=timezone.utc)
        latest = older + timedelta(hours=2)
        api_session.add_all(
            [
                FeedbackSnapshot(publication_id=publication.id, snapshot_stage="1h", collected_at=older, views=10),
                FeedbackSnapshot(publication_id=publication.id, snapshot_stage="24h", collected_at=latest, views=20),
            ]
        )
        await api_session.commit()

        health = (await client.get(f"/api/v1/channel-agent/channels/{channel['id']}/health")).json()

        assert health["last_successful_measured_at"] == "2026-05-19T12:00:00Z"


@pytest.mark.asyncio
async def test_pause_resume_lane_account_and_publication_controls(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        channel = (await client.post("/api/v1/channel-agent/channels", json={"name": "Ops"})).json()
        lane = (await client.post(f"/api/v1/channel-agent/channels/{channel['id']}/lanes", json={"name": "Tech"})).json()
        account = (
            await client.post(
                f"/api/v1/channel-agent/channels/{channel['id']}/accounts",
                json={
                    "account_label": "main",
                    "platform_account_id": "yt",
                    "credential_ref": "youtube/main",
                },
            )
        ).json()

        task = ProductionTask(
            channel_profile_id=uuid.UUID(channel["id"]),
            target_account_id=uuid.UUID(account["id"]),
            source="manual_seed",
            prompt="publication control",
            state="uploaded_private",
            channel_config_snapshot_json={},
        )
        api_session.add(task)
        await api_session.flush()
        publication = PublicationRecord(
            production_task_id=task.id,
            account_id=uuid.UUID(account["id"]),
            platform_content_id="yt-video-1",
            title="publication control",
            desired_privacy="unlisted",
            current_privacy="private",
            publish_status="uploaded",
            compliance_disposition="assumed_fair_use",
        )
        api_session.add(publication)
        await api_session.commit()

        paused_account = (
            await client.post(f"/api/v1/channel-agent/accounts/{account['id']}/pause", json={"reason": "operator"})
        ).json()
        resumed_account = (await client.post(f"/api/v1/channel-agent/accounts/{account['id']}/resume")).json()
        paused_lane = (
            await client.post(f"/api/v1/channel-agent/lanes/{lane['id']}/pause", json={"reason": "operator"})
        ).json()
        resumed_lane = (await client.post(f"/api/v1/channel-agent/lanes/{lane['id']}/resume")).json()
        promoted = (await client.post(f"/api/v1/channel-agent/publications/{publication.id}/promote")).json()
        rejected = (await client.post(f"/api/v1/channel-agent/publications/{publication.id}/reject")).json()
        refreshed_task = await api_session.get(ProductionTask, task.id)

        assert paused_account["enabled"] is False
        assert resumed_account["enabled"] is True
        assert paused_lane["enabled"] is False
        assert resumed_lane["enabled"] is True
        assert resumed_lane["paused_until"] is None
        assert promoted["kind"] == "promote_publication"
        assert promoted["channel_profile_id"] == channel["id"]
        assert promoted["payload_json"]["publication_id"] == str(publication.id)
        assert promoted["payload_json"]["target_visibility"] == "unlisted"
        assert rejected["publish_status"] == "rejected"
        assert refreshed_task is not None
        assert refreshed_task.state == "rejected"


@pytest.mark.asyncio
async def test_pause_resume_lane_toggles_enabled(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        channel = (await client.post("/api/v1/channel-agent/channels", json={"name": "Ops"})).json()
        lane = (await client.post(f"/api/v1/channel-agent/channels/{channel['id']}/lanes", json={"name": "Tech"})).json()

        paused = (await client.post(f"/api/v1/channel-agent/lanes/{lane['id']}/pause", json={"reason": "operator"})).json()
        resumed = (await client.post(f"/api/v1/channel-agent/lanes/{lane['id']}/resume")).json()

        assert paused["enabled"] is False
        assert resumed["enabled"] is True
        assert resumed["paused_until"] is None


@pytest.mark.asyncio
async def test_promote_clamps_public_desired_privacy_to_unlisted(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        channel = (await client.post("/api/v1/channel-agent/channels", json={"name": "Ops"})).json()
        account = (
            await client.post(
                f"/api/v1/channel-agent/channels/{channel['id']}/accounts",
                json={
                    "account_label": "main",
                    "platform_account_id": "yt",
                    "credential_ref": "youtube/main",
                },
            )
        ).json()
        task = ProductionTask(
            channel_profile_id=uuid.UUID(channel["id"]),
            target_account_id=uuid.UUID(account["id"]),
            source="manual_seed",
            prompt="publication control",
            state="uploaded_private",
            channel_config_snapshot_json={},
        )
        api_session.add(task)
        await api_session.flush()
        publication = PublicationRecord(
            production_task_id=task.id,
            account_id=uuid.UUID(account["id"]),
            platform_content_id="yt-video-1",
            title="publication control",
            desired_privacy="public",
            current_privacy="private",
            publish_status="uploaded",
            compliance_disposition="assumed_fair_use",
        )
        api_session.add(publication)
        await api_session.commit()

        response = await client.post(f"/api/v1/channel-agent/publications/{publication.id}/promote")

        assert response.status_code == 200
        queue_item = response.json()
        assert queue_item["payload_json"]["target_visibility"] == "unlisted"


@pytest.mark.asyncio
async def test_reject_cancels_queued_promote_and_marks_records_rejected(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        channel = (await client.post("/api/v1/channel-agent/channels", json={"name": "Ops"})).json()
        account = (
            await client.post(
                f"/api/v1/channel-agent/channels/{channel['id']}/accounts",
                json={
                    "account_label": "main",
                    "platform_account_id": "yt",
                    "credential_ref": "youtube/main",
                },
            )
        ).json()
        task = ProductionTask(
            channel_profile_id=uuid.UUID(channel["id"]),
            target_account_id=uuid.UUID(account["id"]),
            source="manual_seed",
            prompt="publication control",
            state="uploaded_private",
            channel_config_snapshot_json={},
        )
        api_session.add(task)
        await api_session.flush()
        publication = PublicationRecord(
            production_task_id=task.id,
            account_id=uuid.UUID(account["id"]),
            platform_content_id="yt-video-1",
            title="publication control",
            desired_privacy="unlisted",
            current_privacy="private",
            publish_status="uploaded",
            compliance_disposition="assumed_fair_use",
        )
        api_session.add(publication)
        await api_session.commit()

        promote_response = await client.post(f"/api/v1/channel-agent/publications/{publication.id}/promote")
        metrics_queue = ChannelOpsQueueItem(
            kind="collect_metrics",
            idempotency_key=f"collect_metrics:{publication.id}:manual",
            payload_json={"publication_id": str(publication.id)},
            channel_profile_id=uuid.UUID(channel["id"]),
        )
        api_session.add(metrics_queue)
        await api_session.commit()

        reject_response = await client.post(f"/api/v1/channel-agent/publications/{publication.id}/reject")
        queued = await api_session.get(ChannelOpsQueueItem, uuid.UUID(promote_response.json()["id"]))
        metrics_queued = await api_session.get(ChannelOpsQueueItem, metrics_queue.id)
        refreshed_task = await api_session.get(ProductionTask, task.id)

        assert reject_response.status_code == 200
        assert reject_response.json()["publish_status"] == "rejected"
        assert queued is not None
        assert queued.status == "cancelled"
        assert metrics_queued is not None
        assert metrics_queued.status == "cancelled"
        assert refreshed_task is not None
        assert refreshed_task.state == "rejected"


@pytest.mark.asyncio
async def test_reject_scheduled_publication_returns_conflict_without_state_change(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        channel = (await client.post("/api/v1/channel-agent/channels", json={"name": "Ops"})).json()
        account = (
            await client.post(
                f"/api/v1/channel-agent/channels/{channel['id']}/accounts",
                json={
                    "account_label": "main",
                    "platform_account_id": "yt",
                    "credential_ref": "youtube/main",
                },
            )
        ).json()
        task = ProductionTask(
            channel_profile_id=uuid.UUID(channel["id"]),
            target_account_id=uuid.UUID(account["id"]),
            source="manual_seed",
            prompt="publication control",
            state="scheduled",
            channel_config_snapshot_json={},
        )
        api_session.add(task)
        await api_session.flush()
        publication = PublicationRecord(
            production_task_id=task.id,
            account_id=uuid.UUID(account["id"]),
            platform_content_id="yt-video-1",
            title="publication control",
            desired_privacy="unlisted",
            current_privacy="private",
            publish_status="scheduled",
            scheduled_publish_at=datetime(2026, 5, 18, 10, 0, tzinfo=timezone.utc),
            compliance_disposition="assumed_fair_use",
        )
        api_session.add(publication)
        await api_session.commit()

        response = await client.post(f"/api/v1/channel-agent/publications/{publication.id}/reject")
        await api_session.refresh(publication)
        refreshed_task = await api_session.get(ProductionTask, task.id)

        assert response.status_code == 409
        assert publication.publish_status == "scheduled"
        assert refreshed_task is not None
        assert refreshed_task.state == "scheduled"


@pytest.mark.asyncio
async def test_promote_rejected_publication_returns_conflict(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        channel = (await client.post("/api/v1/channel-agent/channels", json={"name": "Ops"})).json()
        account = (
            await client.post(
                f"/api/v1/channel-agent/channels/{channel['id']}/accounts",
                json={
                    "account_label": "main",
                    "platform_account_id": "yt",
                    "credential_ref": "youtube/main",
                },
            )
        ).json()
        task = ProductionTask(
            channel_profile_id=uuid.UUID(channel["id"]),
            target_account_id=uuid.UUID(account["id"]),
            source="manual_seed",
            prompt="publication control",
            state="rejected",
            channel_config_snapshot_json={},
        )
        api_session.add(task)
        await api_session.flush()
        publication = PublicationRecord(
            production_task_id=task.id,
            account_id=uuid.UUID(account["id"]),
            platform_content_id="yt-video-1",
            title="publication control",
            desired_privacy="unlisted",
            current_privacy="private",
            publish_status="rejected",
            compliance_disposition="assumed_fair_use",
        )
        api_session.add(publication)
        await api_session.commit()

        response = await client.post(f"/api/v1/channel-agent/publications/{publication.id}/promote")

        assert response.status_code == 409


@pytest.mark.asyncio
async def test_ticks_and_funnel_return_real_data(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        channel = (await client.post("/api/v1/channel-agent/channels", json={"name": "Metrics"})).json()
        await client.post(f"/api/v1/channel-agent/channels/{channel['id']}/enqueue-tick")

        tick = AgentTickAudit(
            channel_profile_id=uuid.UUID(channel["id"]),
            tick_id="tick-1",
            dry_run=True,
            started_at=datetime.now(timezone.utc),
            tasks_selected=2,
            tasks_rejected=1,
            guards_triggered_json=[
                {"guard": "repetition_rejected"},
                {"guard": "cross_account_rejected"},
                "cadence",
            ],
            decision_summary_json={"selected": 2},
        )
        api_session.add(tick)
        seed = ManualSeed(
            channel_profile_id=uuid.UUID(channel["id"]),
            prompt="active seed",
            status="active",
        )
        api_session.add(seed)

        account = PublishingAccount(
            channel_profile_id=uuid.UUID(channel["id"]),
            account_label="main",
            platform_account_id="yt",
            credential_ref="youtube/main",
        )
        api_session.add(account)
        await api_session.flush()
        selected_task = ProductionTask(
            channel_profile_id=uuid.UUID(channel["id"]),
            target_account_id=account.id,
            source="manual_seed",
            prompt="selected",
            state="selected",
            channel_config_snapshot_json={},
        )
        seeded_task = ProductionTask(
            channel_profile_id=uuid.UUID(channel["id"]),
            target_account_id=account.id,
            source="manual_seed",
            prompt="seeded",
            state="seeded",
            channel_config_snapshot_json={},
        )
        scheduled_task = ProductionTask(
            channel_profile_id=uuid.UUID(channel["id"]),
            target_account_id=account.id,
            source="manual_seed",
            prompt="scheduled",
            state="scheduled",
            channel_config_snapshot_json={},
        )
        rejected_task = ProductionTask(
            channel_profile_id=uuid.UUID(channel["id"]),
            target_account_id=account.id,
            source="manual_seed",
            prompt="rejected",
            state="rejected",
            channel_config_snapshot_json={},
        )
        unknown_task = ProductionTask(
            channel_profile_id=uuid.UUID(channel["id"]),
            target_account_id=account.id,
            source="manual_seed",
            prompt="unknown",
            state="surprise",
            channel_config_snapshot_json={},
        )
        api_session.add_all([selected_task, seeded_task, scheduled_task, rejected_task, unknown_task])
        await api_session.commit()

        ticks = (await client.get(f"/api/v1/channel-agent/channels/{channel['id']}/ticks")).json()
        funnel = (await client.get(f"/api/v1/channel-agent/channels/{channel['id']}/metrics/funnel?days=7")).json()
        clamped_funnel = (
            await client.get(f"/api/v1/channel-agent/channels/{channel['id']}/metrics/funnel?days=-1")
        ).json()

        assert isinstance(ticks, list)
        assert ticks[0]["tick_id"] == "tick-1"
        assert ticks[0]["tasks_selected"] == 2
        assert ticks[0]["guards_triggered_json"][2] == "cadence"
        assert clamped_funnel["days"] >= 0
        assert "selected" in funnel
        assert "scheduled" in funnel
        assert "rejected" in funnel
        assert "cancelled" in funnel
        assert "other" in funnel
        assert funnel["selected"] >= 1
        assert funnel["seeded"] == 2
        assert funnel["scheduled"] >= 1
        assert funnel["rejected"] >= 1
        assert funnel["other"] >= 1
        assert funnel["repetition_rejected"] == 1
        assert funnel["cross_account_rejected"] == 1


@pytest.mark.asyncio
async def test_channel_prefixed_patch_routes_reject_cross_channel_children(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        first = (await client.post("/api/v1/channel-agent/channels", json={"name": "A"})).json()
        second = (await client.post("/api/v1/channel-agent/channels", json={"name": "B"})).json()
        first_lane = (await client.post(f"/api/v1/channel-agent/channels/{first['id']}/lanes", json={"name": "A lane"})).json()
        first_format = (
            await client.post(
                f"/api/v1/channel-agent/lanes/{first_lane['id']}/formats",
                json={"format_key": "shorts_9x16"},
            )
        ).json()
        first_account = (
            await client.post(
                f"/api/v1/channel-agent/channels/{first['id']}/accounts",
                json={
                    "account_label": "main",
                    "platform_account_id": "yt",
                    "credential_ref": "youtube/main",
                },
            )
        ).json()

        lane_response = await client.patch(
            f"/api/v1/channel-agent/channels/{second['id']}/lanes/{first_lane['id']}",
            json={"name": "wrong channel"},
        )
        account_response = await client.patch(
            f"/api/v1/channel-agent/channels/{second['id']}/accounts/{first_account['id']}",
            json={"account_label": "wrong channel"},
        )
        format_response = await client.patch(
            f"/api/v1/channel-agent/channels/{second['id']}/lanes/{first_lane['id']}/formats/{first_format['id']}",
            json={"format_key": "wrong_channel"},
        )
        unscoped_format_response = await client.patch(
            f"/api/v1/channel-agent/lane-formats/{first_format['id']}",
            json={"format_key": "unscoped"},
        )
        scoped_format_response = await client.patch(
            f"/api/v1/channel-agent/channels/{first['id']}/lanes/{first_lane['id']}/formats/{first_format['id']}",
            json={"format_key": "longform_16x9", "source_platforms_json": ["youtube", "bilibili"]},
        )

        assert lane_response.status_code == 404
        assert account_response.status_code == 404
        assert format_response.status_code == 404
        assert unscoped_format_response.status_code == 410
        assert scoped_format_response.status_code == 200
        assert scoped_format_response.json()["format_key"] == "longform_16x9"
        assert scoped_format_response.json()["source_platforms_json"] == ["youtube", "bilibili"]
