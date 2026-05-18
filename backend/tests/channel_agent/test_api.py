from __future__ import annotations

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.channel_agent import router
from app.db import get_db
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
