from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

import httpx
import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.models.channel_agent import (
    ChannelOpsQueueItem,
    ChannelProfile,
    DiscoveryIngestionRun,
    DiscoverySignal,
    TopicLane,
)
from app.services.discovery_ingestion import (
    DiscoveryIngestionAuthorityError,
    DiscoveryIngestionInProgressError,
    DiscoveryIngestionPolicyError,
    DiscoveryIngestionProviderError,
    DiscoveryIngestionRequest,
    DiscoveryIngestionService,
)


ProviderHook = Callable[[], Awaitable[list[dict[str, Any]]]]
LEASED_AT = datetime(2026, 7, 21, 18, 0, 0, 123456, tzinfo=timezone.utc)


class FakeDiscoveryYouTubeClient:
    def __init__(
        self,
        responses: list[list[dict[str, Any]] | BaseException | ProviderHook],
    ) -> None:
        self.responses = list(responses)
        self.requests: list[dict[str, object]] = []

    async def search_videos(
        self,
        *,
        query: str,
        published_after: datetime,
        region_code: str,
        max_results: int,
    ) -> list[dict[str, Any]]:
        self.requests.append(
            {
                "query": query,
                "published_after": published_after,
                "region_code": region_code,
                "max_results": max_results,
            }
        )
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        if callable(response):
            return await response()
        return [dict(item) for item in response]


@dataclass(frozen=True)
class DiscoveryHarness:
    session_factory: async_sessionmaker[AsyncSession]


@pytest.fixture
async def discovery_harness(tmp_path: Path):
    database_path = tmp_path / "discovery-ingestion.db"
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{database_path}",
        connect_args={"timeout": 10},
    )
    async with engine.begin() as conn:
        await conn.exec_driver_sql("PRAGMA journal_mode=WAL")
        await conn.run_sync(ChannelProfile.__table__.create)
        await conn.run_sync(ChannelOpsQueueItem.__table__.create)
        await conn.run_sync(TopicLane.__table__.create)
        await conn.run_sync(DiscoverySignal.__table__.create)
        await conn.run_sync(DiscoveryIngestionRun.__table__.create)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    yield DiscoveryHarness(session_factory=session_factory)
    await engine.dispose()


async def _seed_request(
    harness: DiscoveryHarness,
    *,
    lane_count: int = 1,
    content_mix_policy_json: dict[str, Any] | None = None,
) -> tuple[DiscoveryIngestionRequest, list[TopicLane]]:
    policy = (
        {
            "youtube_discovery": {
                "enabled": True,
                "interval_minutes": 360,
                "max_queries_per_run": 3,
                "max_results_per_query": 10,
                "min_view_count": 1000,
                "region_code": "US",
            }
        }
        if content_mix_policy_json is None
        else content_mix_policy_json
    )
    async with harness.session_factory() as db:
        channel = ChannelProfile(
            name="discovery",
            content_mix_policy_json=policy,
            enabled=True,
        )
        db.add(channel)
        await db.flush()
        lanes = [
            TopicLane(
                channel_profile_id=channel.id,
                name=f"lane-{index}",
                weight=float(lane_count - index),
                keywords_json=[f"keyword-{index}"],
            )
            for index in range(lane_count)
        ]
        db.add_all(lanes)
        queue_item = ChannelOpsQueueItem(
            channel_profile_id=channel.id,
            kind="ingest_discovery",
            idempotency_key=f"ingest:{channel.id}:2026-07-21-18",
            payload_json={
                "channel_id": str(channel.id),
                "source": "youtube_search",
                "bucket": "2026-07-21-18",
                "scheduler_bucket": "2026-07-21-18",
            },
            status="running",
            attempt_count=1,
            locked_by="discovery-service-runner",
            locked_at=LEASED_AT,
        )
        db.add(queue_item)
        await db.commit()
        return (
            DiscoveryIngestionRequest(
                channel_id=channel.id,
                queue_item_id=queue_item.id,
                source="youtube_search",
                scheduler_bucket="2026-07-21-18",
                attempt_count=1,
                locked_by="discovery-service-runner",
                locked_at=LEASED_AT,
            ),
            lanes,
        )


async def _add_run(
    harness: DiscoveryHarness,
    request: DiscoveryIngestionRequest,
    *,
    status: str,
    started_at: datetime,
    attempt_count: int = 1,
    finished_at: datetime | None = None,
    last_error_code: str | None = None,
) -> DiscoveryIngestionRun:
    async with harness.session_factory() as db:
        run = DiscoveryIngestionRun(
            channel_profile_id=request.channel_id,
            queue_item_id=request.queue_item_id,
            source=request.source,
            scheduler_bucket=request.scheduler_bucket,
            query_version="youtube-lane-keyword-v1",
            status=status,
            attempt_count=attempt_count,
            policy_snapshot_json={"enabled": True},
            started_at=started_at,
            finished_at=finished_at,
            last_error_code=last_error_code,
        )
        db.add(run)
        await db.commit()
        return run


def _provider_result(video_id: str = "new-hot") -> list[dict[str, Any]]:
    return [
        {
            "video_id": video_id,
            "title": "Provider title must stay out of the run row",
            "description": "metadata only",
            "view_count": 2500,
            "url": f"https://youtu.be/{video_id}",
        }
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("content_mix_policy_json", "expected_code"),
    [
        ({}, "discovery_disabled"),
        ({"youtube_discovery": {"enabled": 1}}, "invalid_discovery_policy"),
    ],
)
async def test_discovery_ingestion_rejects_disabled_or_invalid_policy_before_provider(
    discovery_harness: DiscoveryHarness,
    content_mix_policy_json: dict[str, Any],
    expected_code: str,
) -> None:
    now = datetime(2026, 7, 21, 18, 5, tzinfo=timezone.utc)
    request, _ = await _seed_request(
        discovery_harness,
        content_mix_policy_json=content_mix_policy_json,
    )
    client = FakeDiscoveryYouTubeClient([])
    service = DiscoveryIngestionService(youtube_client=client)

    async with discovery_harness.session_factory() as db:
        with pytest.raises(DiscoveryIngestionPolicyError) as exc_info:
            await service.ingest(db, request, now)

    assert str(exc_info.value) == expected_code
    assert client.requests == []
    async with discovery_harness.session_factory() as db:
        runs = (await db.execute(select(DiscoveryIngestionRun))).scalars().all()
    assert runs == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"attempt_count": 0},
        {"locked_by": "   "},
        {"locked_by": "x" * 256},
        {"locked_at": datetime(2026, 7, 21, 18, 0)},
    ],
)
async def test_discovery_ingestion_rejects_invalid_lease_token_before_provider(
    discovery_harness: DiscoveryHarness,
    changes: dict[str, Any],
) -> None:
    now = datetime(2026, 7, 21, 18, 5, tzinfo=timezone.utc)
    request, _ = await _seed_request(discovery_harness)
    client = FakeDiscoveryYouTubeClient([])
    service = DiscoveryIngestionService(youtube_client=client)

    async with discovery_harness.session_factory() as db:
        with pytest.raises(ValueError):
            await service.ingest(db, replace(request, **changes), now)

    assert client.requests == []
    async with discovery_harness.session_factory() as db:
        runs = (await db.execute(select(DiscoveryIngestionRun))).scalars().all()
    assert runs == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"attempt_count": 2},
        {"locked_by": "replacement-runner"},
        {"locked_at": LEASED_AT + timedelta(microseconds=1)},
    ],
)
async def test_discovery_ingestion_revalidates_exact_queue_lease_before_provider(
    discovery_harness: DiscoveryHarness,
    changes: dict[str, Any],
) -> None:
    now = datetime(2026, 7, 21, 18, 5, tzinfo=timezone.utc)
    request, _ = await _seed_request(discovery_harness)
    client = FakeDiscoveryYouTubeClient([])
    service = DiscoveryIngestionService(youtube_client=client)

    async with discovery_harness.session_factory() as db:
        with pytest.raises(DiscoveryIngestionAuthorityError) as exc_info:
            await service.ingest(db, replace(request, **changes), now)

    assert str(exc_info.value) == "queue_authority_changed"
    assert client.requests == []
    async with discovery_harness.session_factory() as db:
        runs = (await db.execute(select(DiscoveryIngestionRun))).scalars().all()
    assert runs == []


@pytest.mark.asyncio
@pytest.mark.parametrize("bucket_value", [None, "wrong"])
async def test_discovery_ingestion_rejects_noncanonical_queue_bucket_before_provider(
    discovery_harness: DiscoveryHarness,
    bucket_value: str | None,
) -> None:
    now = datetime(2026, 7, 21, 18, 5, tzinfo=timezone.utc)
    request, _ = await _seed_request(discovery_harness)
    async with discovery_harness.session_factory() as db:
        queue_item = await db.get(ChannelOpsQueueItem, request.queue_item_id)
        assert queue_item is not None
        payload = dict(queue_item.payload_json)
        if bucket_value is None:
            payload.pop("bucket")
        else:
            payload["bucket"] = bucket_value
        queue_item.payload_json = payload
        await db.commit()

    client = FakeDiscoveryYouTubeClient([])
    service = DiscoveryIngestionService(youtube_client=client)
    async with discovery_harness.session_factory() as db:
        with pytest.raises(DiscoveryIngestionAuthorityError) as exc_info:
            await service.ingest(db, request, now)

    assert str(exc_info.value) == "queue_authority_changed"
    assert client.requests == []
    async with discovery_harness.session_factory() as db:
        runs = (await db.execute(select(DiscoveryIngestionRun))).scalars().all()
    assert runs == []


@pytest.mark.asyncio
async def test_discovery_ingestion_succeeds_and_replays_without_provider_call(
    discovery_harness: DiscoveryHarness,
) -> None:
    now = datetime(2026, 7, 21, 18, 5, tzinfo=timezone.utc)
    request, _ = await _seed_request(discovery_harness)
    client = FakeDiscoveryYouTubeClient([_provider_result()])
    service = DiscoveryIngestionService(youtube_client=client)

    async with discovery_harness.session_factory() as db:
        first = await service.ingest(db, request, now)
    async with discovery_harness.session_factory() as db:
        replay = await service.ingest(db, request, now + timedelta(minutes=1))

    assert replay == first
    assert first.status == "succeeded"
    assert first.channel_id == request.channel_id
    assert first.source == request.source
    assert first.scheduler_bucket == request.scheduler_bucket
    assert first.query_count == 1
    assert first.created_count == 1
    assert first.refreshed_count == 0
    assert first.expired_count == 0
    assert first.quota_units_estimated == 100
    assert len(client.requests) == 1
    async with discovery_harness.session_factory() as db:
        runs = (await db.execute(select(DiscoveryIngestionRun))).scalars().all()
    assert len(runs) == 1
    assert runs[0].policy_snapshot_json == {
        "enabled": True,
        "interval_minutes": 360,
        "max_queries_per_run": 3,
        "max_results_per_query": 10,
        "min_view_count": 1000,
        "region_code": "US",
    }


@pytest.mark.asyncio
async def test_discovery_ingestion_constructs_client_once_after_claim_and_replays_without_factory(
    discovery_harness: DiscoveryHarness,
) -> None:
    now = datetime(2026, 7, 21, 18, 5, tzinfo=timezone.utc)
    request, _ = await _seed_request(discovery_harness)
    factory_calls = 0

    def client_factory() -> FakeDiscoveryYouTubeClient:
        nonlocal factory_calls
        factory_calls += 1
        if factory_calls > 1:
            raise RuntimeError("factory must not run for a replay")
        return FakeDiscoveryYouTubeClient([_provider_result()])

    service = DiscoveryIngestionService(youtube_client_factory=client_factory)

    async with discovery_harness.session_factory() as db:
        first = await service.ingest(db, request, now)
    async with discovery_harness.session_factory() as db:
        replay = await service.ingest(db, request, now + timedelta(minutes=1))

    assert replay == first
    assert factory_calls == 1


@pytest.mark.asyncio
async def test_discovery_ingestion_sanitizes_client_factory_failure_and_marks_run_failed(
    discovery_harness: DiscoveryHarness,
) -> None:
    now = datetime(2026, 7, 21, 18, 5, tzinfo=timezone.utc)
    request, _ = await _seed_request(discovery_harness)

    def unavailable_client() -> FakeDiscoveryYouTubeClient:
        raise RuntimeError("sensitive provider configuration")

    service = DiscoveryIngestionService(youtube_client_factory=unavailable_client)

    async with discovery_harness.session_factory() as db:
        with pytest.raises(DiscoveryIngestionProviderError) as exc_info:
            await service.ingest(db, request, now)

    assert exc_info.value.error_code == "provider_unavailable"
    assert "sensitive" not in str(exc_info.value)
    async with discovery_harness.session_factory() as db:
        run = (await db.execute(select(DiscoveryIngestionRun))).scalar_one()
    assert run.status == "failed"
    assert run.last_error_code == "provider_unavailable"


@pytest.mark.asyncio
async def test_discovery_ingestion_rejects_a_recent_running_run_without_provider_call(
    discovery_harness: DiscoveryHarness,
) -> None:
    now = datetime(2026, 7, 21, 18, 5, tzinfo=timezone.utc)
    request, _ = await _seed_request(discovery_harness)
    run = await _add_run(
        discovery_harness,
        request,
        status="running",
        started_at=now - timedelta(minutes=14, seconds=59),
        attempt_count=2,
    )
    client = FakeDiscoveryYouTubeClient([])
    service = DiscoveryIngestionService(youtube_client=client)

    async with discovery_harness.session_factory() as db:
        with pytest.raises(DiscoveryIngestionInProgressError):
            await service.ingest(db, request, now)

    assert client.requests == []
    async with discovery_harness.session_factory() as db:
        persisted = await db.get(DiscoveryIngestionRun, run.id)
    assert persisted is not None
    assert persisted.attempt_count == 2
    assert persisted.status == "running"


@pytest.mark.asyncio
@pytest.mark.parametrize("prior_status", ["running", "failed"])
async def test_discovery_ingestion_reclaims_stale_or_failed_runs(
    discovery_harness: DiscoveryHarness,
    prior_status: str,
) -> None:
    now = datetime(2026, 7, 21, 18, 5, tzinfo=timezone.utc)
    request, _ = await _seed_request(discovery_harness)
    old_run = await _add_run(
        discovery_harness,
        request,
        status=prior_status,
        started_at=now - timedelta(minutes=15),
        attempt_count=2,
        finished_at=now - timedelta(minutes=10) if prior_status == "failed" else None,
        last_error_code="provider_timeout" if prior_status == "failed" else None,
    )
    client = FakeDiscoveryYouTubeClient([_provider_result()])
    service = DiscoveryIngestionService(youtube_client=client)

    async with discovery_harness.session_factory() as db:
        result = await service.ingest(db, request, now)

    assert result.run_id == old_run.id
    assert result.status == "succeeded"
    assert len(client.requests) == 1
    async with discovery_harness.session_factory() as db:
        persisted = await db.get(DiscoveryIngestionRun, old_run.id)
    assert persisted is not None
    assert persisted.attempt_count == 3
    assert persisted.status == "succeeded"
    assert persisted.last_error_code is None


@pytest.mark.asyncio
async def test_concurrent_discovery_ingestion_uses_one_committed_running_claim(
    discovery_harness: DiscoveryHarness,
) -> None:
    now = datetime(2026, 7, 21, 18, 5, tzinfo=timezone.utc)
    request, _ = await _seed_request(discovery_harness)
    provider_entered = asyncio.Event()
    release_provider = asyncio.Event()

    async def blocked_result() -> list[dict[str, Any]]:
        provider_entered.set()
        await release_provider.wait()
        return _provider_result()

    client = FakeDiscoveryYouTubeClient([blocked_result])
    service = DiscoveryIngestionService(youtube_client=client)

    async def first_ingest():
        async with discovery_harness.session_factory() as db:
            return await service.ingest(db, request, now)

    first_task = asyncio.create_task(first_ingest())
    await asyncio.wait_for(provider_entered.wait(), timeout=2)
    async with discovery_harness.session_factory() as db:
        with pytest.raises(DiscoveryIngestionInProgressError):
            await service.ingest(db, request, now)
    release_provider.set()
    first = await asyncio.wait_for(first_task, timeout=2)

    assert first.status == "succeeded"
    assert len(client.requests) == 1
    async with discovery_harness.session_factory() as db:
        runs = (await db.execute(select(DiscoveryIngestionRun))).scalars().all()
    assert len(runs) == 1


def _http_status_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://youtube-manager.invalid/search")
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError(
        "sensitive provider response token=do-not-store",
        request=request,
        response=response,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider_error", "expected_code"),
    [
        (_http_status_error(401), "provider_auth"),
        (_http_status_error(429), "provider_quota"),
        (
            httpx.ReadTimeout(
                "sensitive timeout token=do-not-store",
                request=httpx.Request("GET", "https://youtube-manager.invalid/search"),
            ),
            "provider_timeout",
        ),
        (TypeError("sensitive malformed provider contract"), "provider_contract"),
    ],
)
async def test_provider_failure_rolls_back_signals_and_stores_only_fixed_error_code(
    discovery_harness: DiscoveryHarness,
    provider_error: BaseException,
    expected_code: str,
) -> None:
    now = datetime(2026, 7, 21, 18, 5, tzinfo=timezone.utc)
    request, lanes = await _seed_request(discovery_harness, lane_count=2)
    async with discovery_harness.session_factory() as db:
        db.add(
            DiscoverySignal(
                channel_profile_id=request.channel_id,
                topic_lane_id=lanes[0].id,
                source="youtube_search",
                source_external_id="stale",
                title="stale",
                status="active",
                expires_at=now - timedelta(minutes=1),
            )
        )
        await db.commit()
    client = FakeDiscoveryYouTubeClient([_provider_result("partial"), provider_error])
    service = DiscoveryIngestionService(youtube_client=client)

    async with discovery_harness.session_factory() as db:
        with pytest.raises(DiscoveryIngestionProviderError) as exc_info:
            await service.ingest(db, request, now)

    assert exc_info.value.error_code == expected_code
    assert str(exc_info.value) == expected_code
    async with discovery_harness.session_factory() as db:
        run = (await db.execute(select(DiscoveryIngestionRun))).scalar_one()
        signals = (await db.execute(select(DiscoverySignal))).scalars().all()
    assert run.status == "failed"
    assert run.last_error_code == expected_code
    assert "sensitive" not in str(run.last_error_code)
    assert [(signal.source_external_id, signal.status) for signal in signals] == [
        ("stale", "active")
    ]


@pytest.mark.asyncio
async def test_terminal_channel_recheck_rolls_back_signals_when_authority_changes(
    discovery_harness: DiscoveryHarness,
) -> None:
    now = datetime(2026, 7, 21, 18, 5, tzinfo=timezone.utc)
    request, _ = await _seed_request(discovery_harness)

    async with discovery_harness.session_factory() as db:
        async def disable_channel_in_current_transaction() -> list[dict[str, Any]]:
            await db.execute(
                update(ChannelProfile)
                .where(ChannelProfile.id == request.channel_id)
                .values(enabled=False)
            )
            return _provider_result()

        client = FakeDiscoveryYouTubeClient([disable_channel_in_current_transaction])
        service = DiscoveryIngestionService(youtube_client=client)
        with pytest.raises(DiscoveryIngestionAuthorityError) as exc_info:
            await service.ingest(db, request, now)

    assert str(exc_info.value) == "channel_unavailable"
    async with discovery_harness.session_factory() as db:
        channel = await db.get(ChannelProfile, request.channel_id)
        run = (await db.execute(select(DiscoveryIngestionRun))).scalar_one()
        signals = (await db.execute(select(DiscoverySignal))).scalars().all()
    assert channel is not None
    assert channel.enabled is True
    assert run.status == "failed"
    assert run.last_error_code == "channel_unavailable"
    assert signals == []
