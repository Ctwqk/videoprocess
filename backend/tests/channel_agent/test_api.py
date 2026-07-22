from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import app.api.channel_agent as channel_agent_api
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
from app.models.asset import Asset
from app.models.autoflow import AutoFlowPlan


CHANNEL_AGENT_TABLES = (
    Asset.__table__,
    AutoFlowPlan.__table__,
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


def _review_plan(*, status: str = "review_required", rights_status: str = "review_required") -> AutoFlowPlan:
    return AutoFlowPlan(
        prompt="Review this external asset plan",
        request_json={
            "prompt": "Review this external asset plan",
            "target_platforms": ["youtube_shorts"],
            "duration_sec": 30,
            "aspect_ratio": "9:16",
            "source_policy": "remix_with_review",
            "publish_mode": "private_upload",
            "material_library_ids": [],
            "user_constraints": {},
        },
        intent_json={
            "intent_type": "generic_video",
            "subject": "external review",
            "style": "documentary",
            "duration_sec": 30,
            "aspect_ratio": "9:16",
            "target_platforms": ["youtube_shorts"],
            "source_policy": "remix_with_review",
            "publish_mode": "private_upload",
            "keywords": [],
            "negative_keywords": [],
            "needs_voiceover": False,
            "needs_subtitles": True,
            "needs_bgm": False,
            "user_confirmation_questions": [],
        },
        template_id="material_library_remix",
        pipeline_definition={"nodes": [], "edges": [], "viewport": {"x": 0, "y": 0, "zoom": 1}},
        candidates_json=[],
        metadata_json={},
        rights_json={
            "status": rights_status,
            "reasons": ["human review required"],
            "allowed_publish_modes": ["private_upload", "unlisted_upload"],
            "execute_allowed": True,
            "publish_allowed": True,
        },
        validation_json={"valid": True, "errors": [], "warnings": [], "repairs": []},
        status=status,
        execution_revision=1,
        approved_revision_hash="a" * 64 if status == "review_approved" else None,
        approved_revision=1 if status == "review_approved" else None,
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
async def test_intake_paused_channel_is_visible_but_rejects_manual_tick(api_session):
    paused_at = datetime(2026, 7, 22, 15, 0, tzinfo=timezone.utc)
    channel = ChannelProfile(
        name="Paused canary",
        enabled=True,
        dry_run=False,
        intake_paused_at=paused_at,
        intake_pause_reason="operator_preapproved_live_unlisted_canary",
    )
    api_session.add(channel)
    await api_session.commit()

    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        response = await client.get(f"/api/v1/channel-agent/channels/{channel.id}")
        tick = await client.post(f"/api/v1/channel-agent/channels/{channel.id}/enqueue-tick")

    assert response.status_code == 200
    assert response.json()["halted_at"] is None
    assert response.json()["intake_paused_at"] == paused_at.isoformat()
    assert response.json()["intake_pause_reason"] == "operator_preapproved_live_unlisted_canary"
    assert tick.status_code == 409
    assert tick.json() == {"detail": "Channel intake is paused"}
    assert await api_session.scalar(select(func.count()).select_from(ChannelOpsQueueItem)) == 0


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
async def test_learning_recompute_endpoint_triggers_runner_admin(api_session, monkeypatch):
    calls: list[tuple[str, int]] = []

    async def fake_trigger(channel_id: str, window_days: int = 7):
        calls.append((channel_id, window_days))
        return {"channel_id": channel_id, "window_days": window_days, "recomputed": True}

    monkeypatch.setattr(channel_agent_api, "_trigger_runner_learning_recompute", fake_trigger)

    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        channel = (await client.post("/api/v1/channel-agent/channels", json={"name": "Learn"})).json()

        response = await client.post(f"/api/v1/channel-agent/channels/{channel['id']}/learning/recompute")

        assert response.status_code == 200
        assert response.json()["channel_id"] == channel["id"]
        assert response.json()["recomputed"] is True
        assert calls == [(channel["id"], 7)]


@pytest.mark.asyncio
async def test_audit_learning_and_failure_endpoints_do_not_leak_cross_channel_rows(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        first = (await client.post("/api/v1/channel-agent/channels", json={"name": "Audit A"})).json()
        second = (await client.post("/api/v1/channel-agent/channels", json={"name": "Audit B"})).json()
        first_account = (
            await client.post(
                f"/api/v1/channel-agent/channels/{first['id']}/accounts",
                json={"account_label": "a-main", "platform_account_id": "yt-a", "credential_ref": "youtube/a"},
            )
        ).json()
        second_account = (
            await client.post(
                f"/api/v1/channel-agent/channels/{second['id']}/accounts",
                json={"account_label": "b-main", "platform_account_id": "yt-b", "credential_ref": "youtube/b"},
            )
        ).json()
        now = datetime.now(timezone.utc)
        first_task = ProductionTask(
            channel_profile_id=uuid.UUID(first["id"]),
            target_account_id=uuid.UUID(first_account["id"]),
            source="manual_seed",
            prompt="first audit",
            state="failed",
            failure_category="quota",
            channel_config_snapshot_json={},
            created_at=now - timedelta(hours=1),
        )
        second_task = ProductionTask(
            channel_profile_id=uuid.UUID(second["id"]),
            target_account_id=uuid.UUID(second_account["id"]),
            source="manual_seed",
            prompt="second audit",
            state="failed",
            failure_category="token",
            channel_config_snapshot_json={},
            created_at=now - timedelta(hours=1),
        )
        older_publication_task = ProductionTask(
            channel_profile_id=uuid.UUID(first["id"]),
            target_account_id=uuid.UUID(first_account["id"]),
            source="manual_seed",
            prompt="older publication audit",
            channel_config_snapshot_json={},
        )
        newer_publication_task = ProductionTask(
            channel_profile_id=uuid.UUID(first["id"]),
            target_account_id=uuid.UUID(first_account["id"]),
            source="manual_seed",
            prompt="newer publication audit",
            channel_config_snapshot_json={},
        )
        api_session.add_all(
            [first_task, second_task, older_publication_task, newer_publication_task]
        )
        await api_session.flush()

        first_tick = AgentTickAudit(channel_profile_id=uuid.UUID(first["id"]), tick_id="tick:first", dry_run=False)
        second_tick = AgentTickAudit(channel_profile_id=uuid.UUID(second["id"]), tick_id="tick:second", dry_run=False)
        api_session.add_all([first_tick, second_tick])
        await api_session.flush()

        older_decision = DecisionAuditEntry(
            tick_audit_id=first_tick.id,
            channel_profile_id=uuid.UUID(first["id"]),
            candidate_id="a:older",
            candidate_source="manual_seed",
            target_account_id=uuid.UUID(first_account["id"]),
            selected=False,
            created_task_id=first_task.id,
            created_at=now - timedelta(minutes=3),
        )
        newer_decision = DecisionAuditEntry(
            id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
            tick_audit_id=first_tick.id,
            channel_profile_id=uuid.UUID(first["id"]),
            candidate_id="a:newer",
            candidate_source="manual_seed",
            target_account_id=uuid.UUID(first_account["id"]),
            selected=True,
            created_task_id=first_task.id,
            created_at=now - timedelta(minutes=1),
        )
        second_decision = DecisionAuditEntry(
            tick_audit_id=second_tick.id,
            channel_profile_id=uuid.UUID(second["id"]),
            candidate_id="b:decision",
            candidate_source="manual_seed",
            target_account_id=uuid.UUID(second_account["id"]),
            selected=True,
            created_task_id=second_task.id,
            created_at=now,
        )
        older_publication = PublicationRecord(
            production_task_id=older_publication_task.id,
            account_id=uuid.UUID(first_account["id"]),
            platform_content_id="yt-a-old",
            title="old",
            compliance_disposition="assumed_fair_use",
            created_at=now - timedelta(minutes=3),
        )
        newer_publication = PublicationRecord(
            id=uuid.UUID("00000000-0000-0000-0000-000000000002"),
            production_task_id=newer_publication_task.id,
            account_id=uuid.UUID(first_account["id"]),
            platform_content_id="yt-a-new",
            title="new",
            compliance_disposition="assumed_fair_use",
            created_at=now - timedelta(minutes=1),
        )
        second_publication = PublicationRecord(
            production_task_id=second_task.id,
            account_id=uuid.UUID(second_account["id"]),
            platform_content_id="yt-b",
            title="second",
            compliance_disposition="assumed_fair_use",
            created_at=now,
        )
        first_learning = LearningState(
            channel_profile_id=uuid.UUID(first["id"]),
            dimension_type="source",
            dimension_key="manual_seed:first",
            window_days=7,
            sample_count=2,
            avg_reward=0.2,
            confidence=0.5,
            recommendation_json={"channel": "first"},
        )
        second_learning = LearningState(
            channel_profile_id=uuid.UUID(second["id"]),
            dimension_type="source",
            dimension_key="manual_seed:second",
            window_days=7,
            sample_count=3,
            avg_reward=0.3,
            confidence=0.6,
            recommendation_json={"channel": "second"},
        )
        api_session.add_all(
            [
                older_decision,
                newer_decision,
                second_decision,
                older_publication,
                newer_publication,
                second_publication,
                first_learning,
                second_learning,
            ]
        )
        await api_session.flush()
        tie_decision_id = uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
        tie_publication_id = uuid.UUID("ffffffff-ffff-ffff-ffff-fffffffffffe")
        tie_material_id = uuid.UUID("ffffffff-ffff-ffff-ffff-fffffffffffd")
        api_session.add_all(
            [
                DecisionAuditEntry(
                    id=tie_decision_id,
                    tick_audit_id=first_tick.id,
                    channel_profile_id=uuid.UUID(first["id"]),
                    candidate_id="a:tie",
                    candidate_source="manual_seed",
                    target_account_id=uuid.UUID(first_account["id"]),
                    selected=True,
                    created_task_id=first_task.id,
                    created_at=newer_decision.created_at,
                ),
                PublicationRecord(
                    id=tie_publication_id,
                    production_task_id=first_task.id,
                    account_id=uuid.UUID(first_account["id"]),
                    platform_content_id="yt-a-tie",
                    title="tie",
                    compliance_disposition="assumed_fair_use",
                    created_at=newer_publication.created_at,
                ),
                MaterialUsageLedger(
                    id=tie_material_id,
                    material_id="a-material-tie",
                    channel_profile_id=uuid.UUID(first["id"]),
                    publication_id=tie_publication_id,
                    used_at=now + timedelta(seconds=200),
                ),
            ]
        )
        api_session.add_all(
            [
                    MaterialUsageLedger(
                        material_id=f"a-material-{index:03d}",
                        channel_profile_id=uuid.UUID(first["id"]),
                        publication_id=tie_publication_id,
                        used_at=now + timedelta(seconds=index),
                    )
                    for index in range(201)
            ]
            + [
                MaterialUsageLedger(
                    material_id="b-leak",
                    channel_profile_id=uuid.UUID(second["id"]),
                    publication_id=tie_publication_id,
                    used_at=now + timedelta(seconds=999),
                ),
                MaterialUsageLedger(
                    material_id="shared-material",
                    channel_profile_id=uuid.UUID(second["id"]),
                    publication_id=second_publication.id,
                    used_at=now + timedelta(seconds=998),
                ),
            ]
        )
        await api_session.commit()

        decisions = (await client.get(f"/api/v1/channel-agent/channels/{first['id']}/decisions")).json()
        failures = (await client.get(f"/api/v1/channel-agent/channels/{first['id']}/failures?days=7")).json()
        learning = (await client.get(f"/api/v1/channel-agent/channels/{first['id']}/learning")).json()
        audit = (await client.get(f"/api/v1/channel-agent/tasks/{first_task.id}/audit")).json()

        assert {row["candidate_id"] for row in decisions} == {"a:older", "a:newer", "a:tie"}
        assert failures["categories"] == {"quota": 1}
        assert [row["dimension_key"] for row in learning["states"]] == ["manual_seed:first"]
        assert audit["decision"]["id"] == str(tie_decision_id)
        assert audit["publication"]["id"] == str(tie_publication_id)
        material_ids = [row["material_id"] for row in audit["material_usage"]]
        assert len(material_ids) == 200
        assert material_ids[0] == "a-material-tie"
        assert audit["material_usage"][0]["id"] == str(tie_material_id)
        assert "a-material-000" not in material_ids
        assert "b-leak" not in material_ids
        assert "shared-material" not in material_ids


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
async def test_create_manual_seed_validates_owned_generated_video_input_asset(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        channel = (await client.post("/api/v1/channel-agent/channels", json={"name": "Owned input"})).json()
        unowned_asset = Asset(
            id=uuid.uuid4(),
            filename="unowned.mp4",
            original_name="unowned.mp4",
            mime_type="video/mp4",
            storage_path="assets/unowned.mp4",
            media_info={"license": "external", "provenance": "generated"},
        )
        image_asset = Asset(
            id=uuid.uuid4(),
            filename="image.png",
            original_name="image.png",
            mime_type="image/png",
            storage_path="assets/image.png",
            media_info={"license": "owned", "provenance": "generated"},
        )
        owned_video_asset = Asset(
            id=uuid.uuid4(),
            filename="owned.mp4",
            original_name="owned.mp4",
            mime_type="video/mp4",
            storage_path="assets/owned.mp4",
            media_info={"license": "owned", "provenance": "generated"},
        )
        api_session.add_all([unowned_asset, image_asset, owned_video_asset])
        await api_session.flush()

        def payload(asset_id: uuid.UUID) -> dict[str, object]:
            return {
                "prompt": "Create an owned canary",
                "source_policy": "owned_only",
                "constraints_json": {
                    "input_asset_id": str(asset_id),
                    "source_strategy": "input_video",
                    "planning_mode": "template",
                },
            }

        missing = await client.post(
            f"/api/v1/channel-agent/channels/{channel['id']}/manual-seeds",
            json=payload(uuid.uuid4()),
        )
        unowned = await client.post(
            f"/api/v1/channel-agent/channels/{channel['id']}/manual-seeds",
            json=payload(unowned_asset.id),
        )
        wrong_type = await client.post(
            f"/api/v1/channel-agent/channels/{channel['id']}/manual-seeds",
            json=payload(image_asset.id),
        )
        accepted = await client.post(
            f"/api/v1/channel-agent/channels/{channel['id']}/manual-seeds",
            json=payload(owned_video_asset.id),
        )

        assert missing.status_code == 400
        assert unowned.status_code == 400
        assert wrong_type.status_code == 400
        assert accepted.status_code == 200


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
async def test_human_review_release_approves_exact_plan_and_enqueues_execution(api_session):
    channel = ChannelProfile(name="review release", enabled=True, dry_run=False)
    api_session.add(channel)
    await api_session.flush()
    account = PublishingAccount(
        channel_profile_id=channel.id,
        account_label="review account",
        credential_ref="youtube/review",
        default_privacy="unlisted",
    )
    plan = _review_plan()
    api_session.add_all([account, plan])
    await api_session.flush()
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=account.id,
        source="trend_youtube",
        prompt="review me",
        uses_external_assets=True,
        approval_mode="human",
        autoflow_plan_id=plan.id,
        state="held",
        blocked_by_guard="human_approval_required",
        channel_config_snapshot_json={},
    )
    api_session.add(task)
    await api_session.commit()

    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        response = await client.post(
            f"/api/v1/channel-agent/tasks/{task.id}/review-release",
            json={"human_actor": "operator@example.com", "review_notes": "assets checked"},
        )

    assert response.status_code == 200, response.text
    await api_session.refresh(task)
    await api_session.refresh(plan)
    assert task.state == "planning"
    assert task.blocked_by_guard is None
    assert plan.review_approved_at is not None
    assert plan.approved_revision == plan.execution_revision
    evidence = task.human_review_evidence_json["pre_upload"]
    assert evidence["kind"] == "human_review"
    assert evidence["scope"] == "external_asset_pre_upload"
    assert evidence["human_actor"] == "operator@example.com"
    assert evidence["autoflow_plan_id"] == str(plan.id)
    persisted_token = plan.review_approved_at
    if persisted_token.tzinfo is None:
        persisted_token = persisted_token.replace(tzinfo=timezone.utc)
    assert datetime.fromisoformat(evidence["plan_review_approved_at"]) == persisted_token
    assert evidence["plan_approved_revision"] == plan.approved_revision
    assert evidence["review_notes"] == "assets checked"
    task_authority = task.rationale_json["autoflow_plan_payload"]
    assert task_authority["plan_id"] == str(plan.id)
    assert task_authority["expected_approved_revision_hash"] == plan.approved_revision_hash
    assert task_authority["expected_approved_revision"] == plan.approved_revision
    queue_rows = (
        await api_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.idempotency_key == f"execute_task:{task.id}")
        )
    ).scalars().all()
    assert len(queue_rows) == 1
    assert queue_rows[0].channel_profile_id == channel.id
    assert queue_rows[0].payload_json["expected_approved_revision_hash"] == plan.approved_revision_hash
    assert queue_rows[0].payload_json["expected_approved_revision"] == plan.approved_revision


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("channel_enabled", "halted", "guard"),
    [
        (False, False, "human_approval_required"),
        (True, True, "human_approval_required"),
        (True, False, "automated_channelops_soak_guard"),
        (True, False, "metrics_unavailable"),
    ],
)
async def test_human_review_release_rejects_disabled_halted_or_unrelated_holds(
    api_session,
    channel_enabled,
    halted,
    guard,
):
    channel = ChannelProfile(
        name="blocked release",
        enabled=channel_enabled,
        dry_run=False,
        halted_at=datetime.now(timezone.utc) if halted else None,
    )
    api_session.add(channel)
    await api_session.flush()
    account = PublishingAccount(channel_profile_id=channel.id, account_label="blocked", credential_ref="youtube/x")
    plan = _review_plan()
    api_session.add_all([account, plan])
    await api_session.flush()
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=account.id,
        prompt="blocked",
        uses_external_assets=True,
        autoflow_plan_id=plan.id,
        state="held",
        blocked_by_guard=guard,
        channel_config_snapshot_json={},
    )
    api_session.add(task)
    await api_session.commit()

    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        response = await client.post(
            f"/api/v1/channel-agent/tasks/{task.id}/review-release",
            json={"human_actor": "operator"},
        )

    assert response.status_code == 409
    await api_session.refresh(task)
    assert task.state == "held"
    assert task.human_review_evidence_json == {}


@pytest.mark.asyncio
async def test_human_review_release_rejects_blank_actor(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        response = await client.post(
            f"/api/v1/channel-agent/tasks/{uuid.uuid4()}/review-release",
            json={"human_actor": "   "},
        )
    assert response.status_code in {400, 422}


@pytest.mark.asyncio
async def test_manual_promotion_restores_pds_held_task_and_persists_review(api_session):
    channel = ChannelProfile(name="PDS held", enabled=True, dry_run=False)
    api_session.add(channel)
    await api_session.flush()
    account = PublishingAccount(channel_profile_id=channel.id, account_label="pds", credential_ref="youtube/pds")
    api_session.add(account)
    await api_session.flush()
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=account.id,
        prompt="pds held upload",
        state="held",
        blocked_by_guard="pds_flagged_for_review",
        failure_category="pds",
        channel_config_snapshot_json={},
    )
    api_session.add(task)
    await api_session.flush()
    publication = PublicationRecord(
        production_task_id=task.id,
        account_id=account.id,
        platform_content_id="yt-pds-held",
        title="pds held upload",
        desired_privacy="unlisted",
        current_privacy="private",
        publish_status="uploaded",
        compliance_disposition="owned",
    )
    api_session.add(publication)
    await api_session.commit()

    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        response = await client.post(
            f"/api/v1/channel-agent/publications/{publication.id}/promote",
            json={"human_actor": "release-operator", "review_notes": "PDS concerns reviewed"},
        )

    assert response.status_code == 200, response.text
    await api_session.refresh(task)
    assert task.state == "uploaded_private"
    assert task.blocked_by_guard is None
    evidence = task.human_review_evidence_json["promotion"]
    assert evidence["scope"] == "publication_promotion"
    assert evidence["human_actor"] == "release-operator"
    assert evidence["production_task_id"] == str(task.id)
    assert evidence["publication_id"] == str(publication.id)
    assert evidence["target_visibility"] == "unlisted"
    assert response.json()["payload_json"]["manual_review"] is True


@pytest.mark.asyncio
async def test_manual_promotion_requeues_after_prior_terminal_review_attempt(api_session):
    channel = ChannelProfile(name="PDS retry", enabled=True, dry_run=False)
    api_session.add(channel)
    await api_session.flush()
    account = PublishingAccount(channel_profile_id=channel.id, account_label="retry", credential_ref="youtube/retry")
    api_session.add(account)
    await api_session.flush()
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=account.id,
        prompt="retry PDS review",
        state="held",
        blocked_by_guard="pds_blocked",
        channel_config_snapshot_json={},
    )
    api_session.add(task)
    await api_session.flush()
    publication = PublicationRecord(
        production_task_id=task.id,
        account_id=account.id,
        platform_content_id="yt-pds-retry",
        title="retry",
        desired_privacy="unlisted",
        current_privacy="private",
        publish_status="uploaded",
        compliance_disposition="owned",
    )
    api_session.add(publication)
    await api_session.flush()
    prior = ChannelOpsQueueItem(
        kind="promote_publication",
        idempotency_key=f"promote_publication:{publication.id}:unlisted:manual-review",
        channel_profile_id=channel.id,
        payload_json={"publication_id": str(publication.id), "manual_review": True},
        status="succeeded",
    )
    api_session.add(prior)
    await api_session.commit()

    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        response = await client.post(
            f"/api/v1/channel-agent/publications/{publication.id}/promote",
            json={"human_actor": "retry-operator"},
        )

    assert response.status_code == 200, response.text
    assert response.json()["status"] == "queued"
    assert response.json()["id"] != str(prior.id)
    queue_count = await api_session.scalar(
        select(func.count(ChannelOpsQueueItem.id)).where(ChannelOpsQueueItem.kind == "promote_publication")
    )
    assert queue_count == 2


@pytest.mark.asyncio
async def test_manual_promotion_preserves_external_plan_review_token(api_session):
    approved_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    channel = ChannelProfile(name="reviewed external", enabled=True, dry_run=False)
    api_session.add(channel)
    await api_session.flush()
    account = PublishingAccount(
        channel_profile_id=channel.id,
        account_label="reviewed",
        credential_ref="youtube/reviewed",
        default_privacy="unlisted",
    )
    plan = _review_plan(status="review_approved")
    plan.review_approved_at = approved_at
    api_session.add_all([account, plan])
    await api_session.flush()
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=account.id,
        prompt="reviewed external upload",
        uses_external_assets=True,
        approval_mode="human",
        autoflow_plan_id=plan.id,
        human_review_evidence_json={
            "pre_upload": {
                "kind": "human_review",
                "scope": "external_asset_pre_upload",
                "human_actor": "preupload-operator",
                "reviewed_at": approved_at.isoformat(),
                "autoflow_plan_id": str(plan.id),
                "plan_review_approved_at": approved_at.isoformat(),
                "plan_approved_revision_hash": plan.approved_revision_hash,
                "plan_approved_revision": plan.approved_revision,
            }
        },
        state="uploaded_private",
        channel_config_snapshot_json={},
    )
    api_session.add(task)
    await api_session.flush()
    publication = PublicationRecord(
        production_task_id=task.id,
        account_id=account.id,
        platform_content_id="yt-reviewed-external",
        title="reviewed external",
        desired_privacy="unlisted",
        current_privacy="private",
        publish_status="uploaded",
        compliance_disposition="known_risk_accepted",
    )
    api_session.add(publication)
    await api_session.commit()

    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        response = await client.post(
            f"/api/v1/channel-agent/publications/{publication.id}/promote",
            json={"human_actor": "promotion-operator"},
        )

    assert response.status_code == 200, response.text
    await api_session.refresh(task)
    promotion = task.human_review_evidence_json["promotion"]
    assert promotion["autoflow_plan_id"] == str(plan.id)
    assert datetime.fromisoformat(promotion["plan_review_approved_at"]) == approved_at
    assert promotion["plan_approved_revision_hash"] == plan.approved_revision_hash
    assert promotion["plan_approved_revision"] == plan.approved_revision
    assert promotion["publication_id"] == str(publication.id)


@pytest.mark.asyncio
async def test_manual_promotion_rejects_external_asset_with_stale_preupload_review(api_session):
    approved_at = datetime.now(timezone.utc) - timedelta(minutes=5)
    channel = ChannelProfile(name="stale external", enabled=True, dry_run=False)
    api_session.add(channel)
    await api_session.flush()
    account = PublishingAccount(channel_profile_id=channel.id, account_label="stale", credential_ref="youtube/stale")
    plan = _review_plan(status="review_approved")
    plan.review_approved_at = approved_at
    api_session.add_all([account, plan])
    await api_session.flush()
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=account.id,
        prompt="stale review",
        uses_external_assets=True,
        autoflow_plan_id=plan.id,
        human_review_evidence_json={
            "pre_upload": {
                "kind": "human_review",
                "scope": "external_asset_pre_upload",
                "human_actor": "operator",
                "reviewed_at": approved_at.isoformat(),
                "autoflow_plan_id": str(plan.id),
                "plan_review_approved_at": (approved_at - timedelta(seconds=1)).isoformat(),
            }
        },
        state="uploaded_private",
        channel_config_snapshot_json={},
    )
    api_session.add(task)
    await api_session.flush()
    publication = PublicationRecord(
        production_task_id=task.id,
        account_id=account.id,
        platform_content_id="yt-stale-external",
        title="stale",
        desired_privacy="unlisted",
        current_privacy="private",
        publish_status="uploaded",
        compliance_disposition="known_risk_accepted",
    )
    api_session.add(publication)
    await api_session.commit()

    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        response = await client.post(
            f"/api/v1/channel-agent/publications/{publication.id}/promote",
            json={"human_actor": "operator"},
        )

    assert response.status_code == 409
    queue_count = await api_session.scalar(
        select(func.count(ChannelOpsQueueItem.id)).where(ChannelOpsQueueItem.kind == "promote_publication")
    )
    assert queue_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("task_state", "guard"),
    [
        ("held", "automated_channelops_soak_guard"),
        ("held", "human_approval_required"),
        ("held", "metrics_unavailable"),
        ("held", "platform_rejected"),
        ("rejected", None),
    ],
)
async def test_manual_promotion_rejects_ineligible_held_work(api_session, task_state, guard):
    channel = ChannelProfile(name="ineligible", enabled=True, dry_run=False)
    api_session.add(channel)
    await api_session.flush()
    account = PublishingAccount(channel_profile_id=channel.id, account_label="x", credential_ref="youtube/x")
    api_session.add(account)
    await api_session.flush()
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=account.id,
        prompt="ineligible",
        state=task_state,
        blocked_by_guard=guard,
        channel_config_snapshot_json={},
    )
    api_session.add(task)
    await api_session.flush()
    publication = PublicationRecord(
        production_task_id=task.id,
        account_id=account.id,
        platform_content_id=f"yt-{uuid.uuid4()}",
        title="ineligible",
        desired_privacy="unlisted",
        current_privacy="private",
        publish_status="uploaded",
        compliance_disposition="owned",
    )
    api_session.add(publication)
    await api_session.commit()

    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        response = await client.post(
            f"/api/v1/channel-agent/publications/{publication.id}/promote",
            json={"human_actor": "operator"},
        )

    assert response.status_code == 409
    queue_count = await api_session.scalar(
        select(func.count(ChannelOpsQueueItem.id)).where(ChannelOpsQueueItem.kind == "promote_publication")
    )
    assert queue_count == 0


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
