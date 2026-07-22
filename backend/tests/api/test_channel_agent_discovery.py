from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
import pytest
from sqlalchemy import select
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
    DiscoveryIngestionResult,
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
LEASED_AT = datetime(2026, 7, 21, 18, 0, tzinfo=timezone.utc)


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
    request_session_factory = async_sessionmaker(db_session.bind, expire_on_commit=False)

    async def get_request_db():
        async with request_session_factory() as request_session:
            yield request_session

    app.dependency_overrides[get_db] = get_request_db
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
    payload: object | None = None,
    queue_locked_by: str | None = "discovery-runner",
    queue_locked_at: datetime | None = LEASED_AT,
    queue_attempt_count: int = 1,
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
        attempt_count=queue_attempt_count,
        locked_by=queue_locked_by,
        locked_at=queue_locked_at,
        payload_json=payload if payload is not None else {
            "channel_id": str(channel.id),
            "source": "youtube_search",
            "scheduler_bucket": bucket,
        },
    )
    session.add(queue)
    await session.commit()
    return channel, queue


def _request(channel: ChannelProfile, queue: ChannelOpsQueueItem) -> dict[str, object]:
    return {
        "channel_id": str(channel.id),
        "queue_item_id": str(queue.id),
        "source": "youtube_search",
        "scheduler_bucket": "2026-07-21-18",
        "attempt_count": 1,
        "locked_by": "discovery-runner",
        "locked_at": LEASED_AT.isoformat(),
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

    monkeypatch.setattr(channel_agent_api, "build_youtube_manager_client", fake_client)
    request = {
        "channel_id": str(channel.id),
        "queue_item_id": str(uuid.uuid4()),
        "source": "youtube_search",
        "scheduler_bucket": "2026-07-21-18",
        "attempt_count": 1,
        "locked_by": "discovery-runner",
        "locked_at": LEASED_AT.isoformat(),
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
        (
            "ingest_discovery",
            "running",
            "same",
            {"channel_id": "request", "scheduler_bucket": "2026-07-21-18"},
        ),
        ("ingest_discovery", "running", "same", ["not", "an", "object"]),
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
    if isinstance(payload, dict) and payload.get("channel_id") == "request":
        queue.payload_json = {**queue.payload_json, "channel_id": str(channel.id)}
        await api_session.commit()
    calls = 0

    def fake_client():
        nonlocal calls
        calls += 1
        return object()

    monkeypatch.setattr(channel_agent_api, "build_youtube_manager_client", fake_client)

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
        attempt_count=1,
        locked_by="discovery-runner",
        locked_at=LEASED_AT,
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

    monkeypatch.setattr(channel_agent_api, "build_youtube_manager_client", fake_client)
    request = {
        "channel_id": str(missing_channel_id),
        "queue_item_id": str(queue.id),
        "source": "youtube_search",
        "scheduler_bucket": "2026-07-21-18",
        "attempt_count": 1,
        "locked_by": "discovery-runner",
        "locked_at": LEASED_AT.isoformat(),
    }

    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        response = await client.post(DISCOVERY_PATH, json=request)

    assert response.status_code == 404
    assert response.json() == {"detail": "discovery_channel_not_found"}
    assert calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("locked_by", "locked_at"),
    [
        (None, LEASED_AT),
        ("   ", LEASED_AT),
        ("discovery-runner", None),
    ],
    ids=["missing-owner", "blank-owner", "missing-lock-time"],
)
async def test_discovery_ingest_rejects_missing_or_blank_queue_lease_before_provider(
    api_session,
    monkeypatch,
    locked_by,
    locked_at,
):
    channel, queue = await _channel_and_queue(
        api_session,
        queue_locked_by=locked_by,
        queue_locked_at=locked_at,
    )
    factory_calls = 0
    provider_calls = 0

    def fake_client():
        nonlocal factory_calls
        factory_calls += 1
        return object()

    async def fake_ingest_channel(self, db, channel_id: str, now):
        nonlocal provider_calls
        provider_calls += 1
        raise AssertionError("provider must not be called without queue lease authority")

    monkeypatch.setattr(channel_agent_api, "build_youtube_manager_client", fake_client)
    monkeypatch.setattr(
        discovery_ingestion.YouTubeTrendIngester,
        "ingest_channel",
        fake_ingest_channel,
    )

    async with AsyncClient(
        transport=ASGITransport(app=_app(api_session)),
        base_url="http://test",
    ) as client:
        response = await client.post(DISCOVERY_PATH, json=_request(channel, queue))

    assert response.status_code == 409
    assert response.json() == {"detail": "discovery_queue_authority_invalid"}
    assert factory_calls == 0
    assert provider_calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("attempt_count", 2),
        ("locked_by", "replacement-runner"),
        ("locked_at", (LEASED_AT + timedelta(microseconds=1)).isoformat()),
    ],
)
async def test_discovery_ingest_rejects_exact_lease_token_mismatch_before_provider(
    api_session,
    monkeypatch,
    field,
    value,
):
    channel, queue = await _channel_and_queue(api_session)
    factory_calls = 0
    provider_calls = 0

    def fake_client():
        nonlocal factory_calls
        factory_calls += 1
        return object()

    async def fake_ingest_channel(self, db, channel_id: str, now):
        nonlocal provider_calls
        provider_calls += 1
        raise AssertionError("provider must not be called for a stale queue lease token")

    monkeypatch.setattr(channel_agent_api, "build_youtube_manager_client", fake_client)
    monkeypatch.setattr(
        discovery_ingestion.YouTubeTrendIngester,
        "ingest_channel",
        fake_ingest_channel,
    )
    request = _request(channel, queue)
    request[field] = value

    async with AsyncClient(
        transport=ASGITransport(app=_app(api_session)),
        base_url="http://test",
    ) as client:
        response = await client.post(DISCOVERY_PATH, json=request)

    assert response.status_code == 409
    assert response.json() == {"detail": "discovery_queue_authority_invalid"}
    assert factory_calls == 0
    assert provider_calls == 0
    runs = (await api_session.execute(select(DiscoveryIngestionRun))).scalars().all()
    assert runs == []


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

    monkeypatch.setattr(channel_agent_api, "build_youtube_manager_client", fake_client)

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

    factory_calls = 0

    def fake_client():
        nonlocal factory_calls
        factory_calls += 1
        if factory_calls > 1:
            raise RuntimeError("factory must not run for a replay")
        return object()

    monkeypatch.setattr(channel_agent_api, "build_youtube_manager_client", fake_client)

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
    assert factory_calls == 1
    await api_session.refresh(queue)
    assert queue.status == "running"


@pytest.mark.asyncio
async def test_discovery_ingest_maps_client_construction_failure_to_sanitized_502(api_session, monkeypatch):
    channel, queue = await _channel_and_queue(api_session)

    def unavailable_client():
        raise RuntimeError("sensitive provider configuration")

    monkeypatch.setattr(channel_agent_api, "build_youtube_manager_client", unavailable_client)

    async with AsyncClient(
        transport=ASGITransport(app=_app(api_session), raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        response = await client.post(DISCOVERY_PATH, json=_request(channel, queue))

    assert response.status_code == 502
    assert response.json() == {"detail": "discovery_provider_error"}
    assert "sensitive" not in response.text
    persisted = await api_session.scalar(
        select(DiscoveryIngestionRun).where(DiscoveryIngestionRun.queue_item_id == queue.id)
    )
    assert persisted is not None
    assert persisted.status == "failed"
    assert persisted.last_error_code == "provider_unavailable"


@pytest.mark.asyncio
async def test_discovery_ingest_rejects_negative_service_counters_in_response(api_session, monkeypatch):
    channel, queue = await _channel_and_queue(api_session)

    async def negative_result(self, db, request, now):
        return DiscoveryIngestionResult(
            run_id=uuid.uuid4(),
            channel_id=channel.id,
            source="youtube_search",
            scheduler_bucket="2026-07-21-18",
            status="succeeded",
            query_count=-1,
            created_count=0,
            refreshed_count=0,
            expired_count=0,
            quota_units_estimated=0,
        )

    monkeypatch.setattr(channel_agent_api, "build_youtube_manager_client", lambda: object())
    monkeypatch.setattr(discovery_ingestion.DiscoveryIngestionService, "ingest", negative_result)

    async with AsyncClient(
        transport=ASGITransport(app=_app(api_session), raise_app_exceptions=False),
        base_url="http://test",
    ) as client:
        response = await client.post(DISCOVERY_PATH, json=_request(channel, queue))

    assert response.status_code == 500


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

    monkeypatch.setattr(channel_agent_api, "build_youtube_manager_client", lambda: object())
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
        {"channel_id": str(uuid.uuid4()), "queue_item_id": "not-a-uuid", "source": "youtube_search", "scheduler_bucket": "bucket"},
        {"channel_id": str(uuid.uuid4()), "queue_item_id": str(uuid.uuid4()), "source": "youtube", "scheduler_bucket": "bucket"},
        {"channel_id": str(uuid.uuid4()), "queue_item_id": str(uuid.uuid4()), "source": "youtube_search", "scheduler_bucket": "   "},
        {"channel_id": str(uuid.uuid4()), "queue_item_id": str(uuid.uuid4()), "source": "youtube_search", "scheduler_bucket": "x" * 65},
        {"channel_id": str(uuid.uuid4()), "queue_item_id": str(uuid.uuid4()), "source": "youtube_search", "scheduler_bucket": "bucket", "extra": True},
    ],
)
async def test_discovery_ingest_request_schema_is_strict(api_session, payload):
    payload = {
        "attempt_count": 1,
        "locked_by": "discovery-runner",
        "locked_at": LEASED_AT.isoformat(),
        **payload,
    }
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        response = await client.post(DISCOVERY_PATH, json=payload)

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_discovery_ingest_request_schema_rejects_invalid_lease_tokens(api_session):
    valid = {
        "channel_id": str(uuid.uuid4()),
        "queue_item_id": str(uuid.uuid4()),
        "source": "youtube_search",
        "scheduler_bucket": "bucket",
        "attempt_count": 1,
        "locked_by": "discovery-runner",
        "locked_at": LEASED_AT.isoformat(),
    }
    invalid = []
    for missing in ("attempt_count", "locked_by", "locked_at"):
        payload = dict(valid)
        del payload[missing]
        invalid.append(payload)
    invalid.extend(
        [
            {**valid, "attempt_count": 0},
            {**valid, "attempt_count": True},
            {**valid, "locked_by": "   "},
            {**valid, "locked_by": "x" * 256},
            {**valid, "locked_at": "2026-07-21T18:00:00"},
        ]
    )

    async with AsyncClient(
        transport=ASGITransport(app=_app(api_session)),
        base_url="http://test",
    ) as client:
        for payload in invalid:
            response = await client.post(DISCOVERY_PATH, json=payload)
            assert response.status_code == 422, payload
