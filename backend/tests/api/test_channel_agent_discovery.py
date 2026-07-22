from __future__ import annotations

import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import app.api.channel_agent as channel_agent_api
from app.api.channel_agent import router
from app.db import get_db
from app.models.channel_agent import ChannelOpsQueueItem, ChannelProfile, DiscoveryIngestionRun
from app.services import discovery_ingestion
from app.services.discovery_ingestion import (
    DiscoveryIngestionAuthorityError,
    DiscoveryIngestionInProgressError,
    DiscoveryIngestionPolicyError,
    DiscoveryIngestionProviderError,
)


DISCOVERY_TABLES = (
    ChannelProfile.__table__,
    ChannelOpsQueueItem.__table__,
    DiscoveryIngestionRun.__table__,
)
DISCOVERY_PATH = "/api/v1/channel-agent/internal/discovery/ingest"
POLICY = {
    "youtube_discovery": {
        "enabled": True,
        "interval_minutes": 360,
        "max_queries_per_run": 3,
        "max_results_per_query": 10,
        "min_view_count": 1000,
        "region_code": "US",
    }
}


@pytest.fixture
async def api_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        for table in DISCOVERY_TABLES:
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


async def _channel_and_queue(
    session,
    *,
    enabled: bool = True,
    halted: bool = False,
    policy: dict | None = None,
    queue_kind: str = "ingest_discovery",
    queue_status: str = "running",
    queue_channel_id: uuid.UUID | None = None,
    payload: dict | None = None,
) -> tuple[ChannelProfile, ChannelOpsQueueItem]:
    channel = ChannelProfile(
        name="Discovery API",
        enabled=enabled,
        halted_at=datetime.now(timezone.utc) if halted else None,
        content_mix_policy_json=POLICY if policy is None else policy,
    )
    session.add(channel)
    await session.flush()
    bucket = "2026-07-21-18"
    queue = ChannelOpsQueueItem(
        kind=queue_kind,
        idempotency_key=f"ingest_discovery:{uuid.uuid4()}",
        channel_profile_id=queue_channel_id or channel.id,
        status=queue_status,
        payload_json=payload
        or {
            "channel_id": str(channel.id),
            "source": "youtube_search",
            "scheduler_bucket": bucket,
        },
    )
    session.add(queue)
    await session.commit()
    return channel, queue


def _request(channel: ChannelProfile, queue: ChannelOpsQueueItem) -> dict[str, str]:
    return {
        "channel_id": str(channel.id),
        "queue_item_id": str(queue.id),
        "source": "youtube_search",
        "scheduler_bucket": "2026-07-21-18",
    }


@pytest.mark.asyncio
async def test_discovery_ingest_rejects_missing_queue_before_provider_call(api_session, monkeypatch):
    channel = ChannelProfile(name="Discovery API", content_mix_policy_json=POLICY)
    api_session.add(channel)
    await api_session.commit()
    calls = 0

    def fake_client():
        nonlocal calls
        calls += 1
        return object()

    monkeypatch.setattr(channel_agent_api, "_create_discovery_youtube_client", fake_client, raising=False)
    request = {
        "channel_id": str(channel.id),
        "queue_item_id": str(uuid.uuid4()),
        "source": "youtube_search",
        "scheduler_bucket": "2026-07-21-18",
    }

    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        response = await client.post(DISCOVERY_PATH, json=request)

    assert response.status_code == 404
    assert response.json() == {"detail": "discovery_queue_item_not_found"}
    assert calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("queue_kind", "queue_status", "queue_channel", "payload"),
    [
        ("agent_tick", "running", "same", None),
        ("ingest_discovery", "queued", "same", None),
        ("ingest_discovery", "running", "other", None),
        (
            "ingest_discovery",
            "running",
            "same",
            {"channel_id": "wrong", "source": "youtube_search", "scheduler_bucket": "2026-07-21-18"},
        ),
        (
            "ingest_discovery",
            "running",
            "same",
            {"channel_id": "request", "source": "wrong", "scheduler_bucket": "2026-07-21-18"},
        ),
        (
            "ingest_discovery",
            "running",
            "same",
            {"channel_id": "request", "source": "youtube_search", "scheduler_bucket": "wrong"},
        ),
    ],
)
async def test_discovery_ingest_rejects_invalid_queue_authority_before_provider_call(
    api_session,
    monkeypatch,
    queue_kind,
    queue_status,
    queue_channel,
    payload,
):
    other_channel = uuid.uuid4() if queue_channel == "other" else None
    channel, queue = await _channel_and_queue(
        api_session,
        queue_kind=queue_kind,
        queue_status=queue_status,
        queue_channel_id=other_channel,
        payload=payload,
    )
    if payload and payload["channel_id"] == "request":
        queue.payload_json["channel_id"] = str(channel.id)
        await api_session.commit()
    calls = 0

    def fake_client():
        nonlocal calls
        calls += 1
        return object()

    monkeypatch.setattr(channel_agent_api, "_create_discovery_youtube_client", fake_client, raising=False)

    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        response = await client.post(DISCOVERY_PATH, json=_request(channel, queue))

    assert response.status_code == 409
    assert response.json() == {"detail": "discovery_queue_authority_invalid"}
    assert calls == 0


@pytest.mark.asyncio
async def test_discovery_ingest_rejects_missing_channel_before_provider_call(api_session, monkeypatch):
    missing_channel_id = uuid.uuid4()
    queue = ChannelOpsQueueItem(
        kind="ingest_discovery",
        idempotency_key=f"ingest_discovery:{uuid.uuid4()}",
        channel_profile_id=missing_channel_id,
        status="running",
        payload_json={
            "channel_id": str(missing_channel_id),
            "source": "youtube_search",
            "scheduler_bucket": "2026-07-21-18",
        },
    )
    api_session.add(queue)
    await api_session.commit()
    calls = 0

    def fake_client():
        nonlocal calls
        calls += 1
        return object()

    monkeypatch.setattr(channel_agent_api, "_create_discovery_youtube_client", fake_client, raising=False)
    request = {
        "channel_id": str(missing_channel_id),
        "queue_item_id": str(queue.id),
        "source": "youtube_search",
        "scheduler_bucket": "2026-07-21-18",
    }

    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        response = await client.post(DISCOVERY_PATH, json=request)

    assert response.status_code == 404
    assert response.json() == {"detail": "discovery_channel_not_found"}
    assert calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("enabled", "halted", "policy", "status", "detail"),
    [
        (False, False, POLICY, 409, "discovery_channel_unavailable"),
        (True, True, POLICY, 409, "discovery_channel_unavailable"),
        (True, False, {"youtube_discovery": {"enabled": False}}, 409, "discovery_policy_disabled"),
        (True, False, {"youtube_discovery": {"enabled": "yes"}}, 409, "discovery_policy_invalid"),
    ],
)
async def test_discovery_ingest_rejects_channel_or_policy_before_provider_call(
    api_session,
    monkeypatch,
    enabled,
    halted,
    policy,
    status,
    detail,
):
    channel, queue = await _channel_and_queue(api_session, enabled=enabled, halted=halted, policy=policy)
    calls = 0

    def fake_client():
        nonlocal calls
        calls += 1
        return object()

    monkeypatch.setattr(channel_agent_api, "_create_discovery_youtube_client", fake_client, raising=False)

    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        response = await client.post(DISCOVERY_PATH, json=_request(channel, queue))

    assert response.status_code == status
    assert response.json() == {"detail": detail}
    assert calls == 0


@pytest.mark.asyncio
async def test_discovery_ingest_returns_durable_result_and_does_not_complete_queue(api_session, monkeypatch):
    channel, queue = await _channel_and_queue(api_session)
    channel_id = str(channel.id)
    queue_id = str(queue.id)
    provider_calls = 0

    monkeypatch.setattr(channel_agent_api, "_create_discovery_youtube_client", lambda: object(), raising=False)

    async def fake_ingest_channel(self, db, channel_id: str, now):
        nonlocal provider_calls
        provider_calls += 1
        return SimpleNamespace(
            query_count=2,
            created_count=3,
            refreshed_count=4,
            expired_count=5,
        )

    monkeypatch.setattr(discovery_ingestion.YouTubeTrendIngester, "ingest_channel", fake_ingest_channel)

    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        first = await client.post(DISCOVERY_PATH, json=_request(channel, queue))
        replay = await client.post(DISCOVERY_PATH, json=_request(channel, queue))

    assert first.status_code == 200
    assert first.json() == {
        "run_id": first.json()["run_id"],
        "channel_id": channel_id,
        "queue_item_id": queue_id,
        "source": "youtube_search",
        "scheduler_bucket": "2026-07-21-18",
        "status": "succeeded",
        "query_count": 2,
        "created_count": 3,
        "refreshed_count": 4,
        "expired_count": 5,
        "quota_units_estimated": 200,
    }
    assert replay.status_code == 200
    assert replay.json()["run_id"] == first.json()["run_id"]
    assert provider_calls == 1
    await api_session.refresh(queue)
    assert queue.status == "running"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "detail"),
    [
        (DiscoveryIngestionAuthorityError("secret"), "discovery_authority_conflict"),
        (DiscoveryIngestionPolicyError("secret"), "discovery_policy_conflict"),
        (DiscoveryIngestionInProgressError("secret"), "discovery_run_in_progress"),
        (DiscoveryIngestionProviderError("secret-provider-body"), "discovery_provider_error"),
    ],
)
async def test_discovery_ingest_maps_service_errors_without_provider_details(
    api_session,
    monkeypatch,
    error,
    detail,
):
    channel, queue = await _channel_and_queue(api_session)

    async def fake_ingest(self, db, request, now):
        raise error

    monkeypatch.setattr(channel_agent_api, "_create_discovery_youtube_client", lambda: object(), raising=False)
    monkeypatch.setattr(discovery_ingestion.DiscoveryIngestionService, "ingest", fake_ingest)

    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        response = await client.post(DISCOVERY_PATH, json=_request(channel, queue))

    assert response.status_code == (502 if isinstance(error, DiscoveryIngestionProviderError) else 409)
    assert response.json() == {"detail": detail}
    assert "secret" not in response.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"channel_id": "not-a-uuid", "queue_item_id": str(uuid.uuid4()), "source": "youtube_search", "scheduler_bucket": "bucket"},
        {"channel_id": str(uuid.uuid4()), "queue_item_id": str(uuid.uuid4()), "source": "youtube", "scheduler_bucket": "bucket"},
        {"channel_id": str(uuid.uuid4()), "queue_item_id": str(uuid.uuid4()), "source": "youtube_search", "scheduler_bucket": "   "},
        {"channel_id": str(uuid.uuid4()), "queue_item_id": str(uuid.uuid4()), "source": "youtube_search", "scheduler_bucket": "bucket", "extra": True},
    ],
)
async def test_discovery_ingest_request_schema_is_strict(api_session, payload):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        response = await client.post(DISCOVERY_PATH, json=payload)

    assert response.status_code == 422
