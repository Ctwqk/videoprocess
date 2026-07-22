from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from typing import Any
import uuid

import asyncpg  # type: ignore[import-untyped]
from fastapi import FastAPI
import httpx
from httpx import ASGITransport, AsyncClient
import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

import app.api.channel_agent as channel_agent_api
from app.api.channel_agent import router
from app.db import get_db
from app.models.channel_agent import (
    ChannelOpsQueueItem,
    ChannelProfile,
    DiscoveryIngestionRun,
    DiscoverySignal,
    TopicLane,
)
from app.services import discovery_ingestion
from app.services.discovery_ingestion import (
    DiscoveryIngestionAuthorityError,
    DiscoveryIngestionConflictError,
    DiscoveryIngestionInProgressError,
    DiscoveryIngestionRequest,
    DiscoveryIngestionResult,
    DiscoveryIngestionService,
)


POSTGRES_URL = os.getenv(
    "DISCOVERY_INGESTION_POSTGRES_TEST_URL",
    os.getenv(
        "TEST_POSTGRES_DSN",
        os.getenv("CHANNEL_OPS_POSTGRES_TEST_URL", ""),
    ),
)
BACKEND_ROOT = Path(__file__).resolve().parents[2]
DISCOVERY_PATH = "/api/v1/channel-agent/internal/discovery/ingest"
LEASED_AT = datetime(2026, 7, 21, 18, 0, 0, 123456, tzinfo=timezone.utc)

pytestmark = pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set DISCOVERY_INGESTION_POSTGRES_TEST_URL for PostgreSQL race tests",
)


@dataclass(frozen=True)
class PostgresHarness:
    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]


class BlockingYouTubeClient:
    def __init__(
        self,
        *,
        entered: asyncio.Event,
        release: asyncio.Event,
        video_id: str,
        error: Exception | None = None,
    ) -> None:
        self.entered = entered
        self.release = release
        self.video_id = video_id
        self.error = error
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
        self.entered.set()
        await self.release.wait()
        if self.error is not None:
            raise self.error
        return [_provider_item(self.video_id)]


class IntegrityAuthorityChangeSession(AsyncSession):
    def __init__(
        self,
        *args: Any,
        session_factory: async_sessionmaker[AsyncSession],
        request: DiscoveryIngestionRequest,
        now: datetime,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._session_factory = session_factory
        self._request = request
        self._now = now
        self._conflict_inserted = False
        self._authority_changed = False

    async def flush(self, objects=None) -> None:
        if not self._conflict_inserted and any(
            isinstance(item, DiscoveryIngestionRun) for item in self.new
        ):
            self._conflict_inserted = True
            async with self._session_factory() as conflict_db:
                conflict_db.add(
                    DiscoveryIngestionRun(
                        channel_profile_id=self._request.channel_id,
                        queue_item_id=None,
                        source=self._request.source,
                        scheduler_bucket=self._request.scheduler_bucket,
                        query_version="youtube-lane-keyword-v1",
                        status="running",
                        attempt_count=1,
                        policy_snapshot_json={"enabled": True},
                        started_at=self._now,
                    )
                )
                await conflict_db.commit()
        await super().flush(objects)

    async def rollback(self) -> None:
        await super().rollback()
        if self._conflict_inserted and not self._authority_changed:
            self._authority_changed = True
            async with self._session_factory() as recovery_db:
                await recovery_db.execute(
                    update(ChannelOpsQueueItem)
                    .where(ChannelOpsQueueItem.id == self._request.queue_item_id)
                    .values(
                        status="queued",
                        locked_by=None,
                        locked_at=None,
                        last_error="discovery_lease_recovered",
                    )
                )
                await recovery_db.commit()


def _asyncpg_url(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


def _database_url(database: str) -> str:
    base = POSTGRES_URL.rsplit("/", 1)[0]
    return f"{base}/{database}"


def _run_alembic(database_url: str, *args: str) -> None:
    completed = subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=BACKEND_ROOT,
        env={**os.environ, "DATABASE_URL": database_url},
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


async def _create_database(database: str) -> None:
    admin = await asyncpg.connect(_asyncpg_url(_database_url("postgres")))
    try:
        version_num = int(await admin.fetchval("SHOW server_version_num"))
        assert version_num // 10_000 == 16
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()


async def _drop_database(database: str) -> None:
    admin = await asyncpg.connect(_asyncpg_url(_database_url("postgres")))
    try:
        await admin.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1",
            database,
        )
        await admin.execute(f'DROP DATABASE IF EXISTS "{database}"')
    finally:
        await admin.close()


@pytest.fixture(scope="module")
def discovery_postgres_url() -> Iterator[str]:
    database = f"vp_discovery_service_{uuid.uuid4().hex}"
    asyncio.run(_create_database(database))
    target_url = _database_url(database)
    try:
        _run_alembic(target_url, "upgrade", "head")
        yield target_url
    finally:
        asyncio.run(_drop_database(database))


@pytest.fixture
async def postgres_harness(
    discovery_postgres_url: str,
) -> AsyncIterator[PostgresHarness]:
    engine = create_async_engine(discovery_postgres_url)
    harness = PostgresHarness(
        engine=engine,
        session_factory=async_sessionmaker(engine, expire_on_commit=False),
    )
    yield harness
    await engine.dispose()


async def _seed_request(
    harness: PostgresHarness,
    *,
    bucket: str | None = None,
    max_attempts: int = 3,
) -> DiscoveryIngestionRequest:
    scheduler_bucket = bucket or f"race-{uuid.uuid4().hex[:12]}"
    async with harness.session_factory() as db:
        channel = ChannelProfile(
            name="PostgreSQL discovery race",
            content_mix_policy_json={
                "youtube_discovery": {
                    "enabled": True,
                    "interval_minutes": 360,
                    "max_queries_per_run": 1,
                    "max_results_per_query": 10,
                    "min_view_count": 1000,
                    "region_code": "US",
                }
            },
            enabled=True,
        )
        db.add(channel)
        await db.flush()
        db.add(
            TopicLane(
                channel_profile_id=channel.id,
                name="race lane",
                weight=1.0,
                keywords_json=["race keyword"],
            )
        )
        queue_item = ChannelOpsQueueItem(
            channel_profile_id=channel.id,
            kind="ingest_discovery",
            idempotency_key=f"ingest:{channel.id}:{scheduler_bucket}",
            payload_json={
                "channel_id": str(channel.id),
                "source": "youtube_search",
                "scheduler_bucket": scheduler_bucket,
            },
            status="running",
            attempt_count=1,
            max_attempts=max_attempts,
            locked_by="discovery-postgres-runner",
            locked_at=LEASED_AT,
        )
        db.add(queue_item)
        await db.commit()
        return DiscoveryIngestionRequest(
            channel_id=channel.id,
            queue_item_id=queue_item.id,
            source="youtube_search",
            scheduler_bucket=scheduler_bucket,
            attempt_count=1,
            locked_by="discovery-postgres-runner",
            locked_at=LEASED_AT,
        )


def _provider_item(video_id: str) -> dict[str, Any]:
    return {
        "video_id": video_id,
        "title": f"provider result {video_id}",
        "description": "metadata only",
        "view_count": 2500,
        "url": f"https://youtu.be/{video_id}",
    }


async def _ingest(
    harness: PostgresHarness,
    service: DiscoveryIngestionService,
    request: DiscoveryIngestionRequest,
    now: datetime,
) -> DiscoveryIngestionResult:
    async with harness.session_factory() as db:
        return await service.ingest(db, request, now)


def _postgres_app(harness: PostgresHarness) -> FastAPI:
    app = FastAPI()
    app.include_router(router)

    async def get_request_db():
        async with harness.session_factory() as request_db:
            yield request_db

    app.dependency_overrides[get_db] = get_request_db
    return app


def _api_payload(request: DiscoveryIngestionRequest) -> dict[str, object]:
    return {
        "channel_id": str(request.channel_id),
        "queue_item_id": str(request.queue_item_id),
        "source": request.source,
        "scheduler_bucket": request.scheduler_bucket,
        "attempt_count": request.attempt_count,
        "locked_by": request.locked_by,
        "locked_at": request.locked_at.isoformat(),
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("recovery_status", "max_attempts"),
    [("queued", 3), ("dead_lettered", 1)],
)
async def test_api_revalidates_stale_session_after_queue_recovery_without_run(
    postgres_harness: PostgresHarness,
    monkeypatch: pytest.MonkeyPatch,
    recovery_status: str,
    max_attempts: int,
) -> None:
    request = await _seed_request(postgres_harness, max_attempts=max_attempts)
    initial_authority_observed = asyncio.Event()
    resume_request = asyncio.Event()
    factory_calls = 0
    provider_calls = 0
    original_ingest = DiscoveryIngestionService.ingest

    async def delayed_ingest(self, db, service_request, now):
        initial_authority_observed.set()
        await resume_request.wait()
        return await original_ingest(self, db, service_request, now)

    def fake_client_factory():
        nonlocal factory_calls
        factory_calls += 1
        return object()

    async def fake_ingest_channel(self, db, channel_id: str, now):
        nonlocal provider_calls
        provider_calls += 1
        return SimpleNamespace(
            query_count=1,
            created_count=0,
            refreshed_count=0,
            expired_count=0,
        )

    monkeypatch.setattr(DiscoveryIngestionService, "ingest", delayed_ingest)
    monkeypatch.setattr(
        channel_agent_api,
        "build_youtube_manager_client",
        fake_client_factory,
    )
    monkeypatch.setattr(
        discovery_ingestion.YouTubeTrendIngester,
        "ingest_channel",
        fake_ingest_channel,
    )

    async with AsyncClient(
        transport=ASGITransport(app=_postgres_app(postgres_harness)),
        base_url="http://test",
    ) as client:
        response_task = asyncio.create_task(
            client.post(DISCOVERY_PATH, json=_api_payload(request))
        )
        await asyncio.wait_for(initial_authority_observed.wait(), timeout=5)
        recovered_at = datetime(2026, 7, 21, 18, 20, tzinfo=timezone.utc)
        async with postgres_harness.session_factory() as recovery_db:
            await recovery_db.execute(
                update(ChannelOpsQueueItem)
                .where(ChannelOpsQueueItem.id == request.queue_item_id)
                .values(
                    status=recovery_status,
                    run_after=recovered_at,
                    locked_by=None,
                    locked_at=None,
                    last_error="discovery_lease_recovered",
                    dead_letter_at=(
                        recovered_at if recovery_status == "dead_lettered" else None
                    ),
                )
            )
            await recovery_db.commit()
        resume_request.set()
        response = await asyncio.wait_for(response_task, timeout=5)

    assert response.status_code == 409
    assert response.json() == {"detail": "discovery_authority_conflict"}
    assert factory_calls == 0
    assert provider_calls == 0
    async with postgres_harness.session_factory() as db:
        runs = (
            await db.execute(
                select(DiscoveryIngestionRun).where(
                    DiscoveryIngestionRun.queue_item_id == request.queue_item_id
                )
            )
        ).scalars().all()
    assert runs == []


@pytest.mark.asyncio
async def test_integrity_retry_revalidates_queue_authority_after_rollback(
    postgres_harness: PostgresHarness,
) -> None:
    now = datetime(2026, 7, 21, 18, 0, tzinfo=timezone.utc)
    request = await _seed_request(postgres_harness)
    factory_calls = 0

    def provider_factory():
        nonlocal factory_calls
        factory_calls += 1
        return object()

    service = DiscoveryIngestionService(youtube_client_factory=provider_factory)
    async with IntegrityAuthorityChangeSession(
        postgres_harness.engine,
        expire_on_commit=False,
        session_factory=postgres_harness.session_factory,
        request=request,
        now=now,
    ) as db:
        with pytest.raises(DiscoveryIngestionAuthorityError) as exc_info:
            await service.ingest(db, request, now)

    assert str(exc_info.value) == "queue_authority_changed"
    assert factory_calls == 0
    async with postgres_harness.session_factory() as db:
        queue = await db.get(ChannelOpsQueueItem, request.queue_item_id)
        request_run = await db.scalar(
            select(DiscoveryIngestionRun).where(
                DiscoveryIngestionRun.queue_item_id == request.queue_item_id
            )
        )
    assert queue is not None
    assert queue.status == "queued"
    assert request_run is None


@pytest.mark.asyncio
@pytest.mark.parametrize("superseded_outcome", ["success", "failure"])
async def test_stale_reclaim_fences_superseded_attempt_terminal_updates(
    postgres_harness: PostgresHarness,
    superseded_outcome: str,
) -> None:
    attempt_one_now = datetime(2026, 7, 21, 18, 0, tzinfo=timezone.utc)
    attempt_two_now = attempt_one_now + timedelta(minutes=15)
    request = await _seed_request(postgres_harness)
    first_entered = asyncio.Event()
    release_first = asyncio.Event()
    second_entered = asyncio.Event()
    release_second = asyncio.Event()
    first_error = (
        httpx.ReadTimeout(
            "superseded timeout must not fail the replacement attempt",
            request=httpx.Request("GET", "https://youtube-manager.invalid/search"),
        )
        if superseded_outcome == "failure"
        else None
    )
    first_client = BlockingYouTubeClient(
        entered=first_entered,
        release=release_first,
        video_id="attempt-one",
        error=first_error,
    )
    second_client = BlockingYouTubeClient(
        entered=second_entered,
        release=release_second,
        video_id="attempt-two",
    )
    first_task = asyncio.create_task(
        _ingest(
            postgres_harness,
            DiscoveryIngestionService(youtube_client=first_client),
            request,
            attempt_one_now,
        )
    )
    await asyncio.wait_for(first_entered.wait(), timeout=5)
    second_task = asyncio.create_task(
        _ingest(
            postgres_harness,
            DiscoveryIngestionService(youtube_client=second_client),
            request,
            attempt_two_now,
        )
    )
    await asyncio.wait_for(second_entered.wait(), timeout=5)

    release_first.set()
    first_outcome = (
        await asyncio.wait_for(
            asyncio.gather(first_task, return_exceptions=True),
            timeout=5,
        )
    )[0]
    release_second.set()
    second_outcome = (
        await asyncio.wait_for(
            asyncio.gather(second_task, return_exceptions=True),
            timeout=5,
        )
    )[0]

    assert isinstance(first_outcome, DiscoveryIngestionConflictError)
    assert str(first_outcome) == "run_authority_changed"
    assert isinstance(second_outcome, DiscoveryIngestionResult)
    assert second_outcome.status == "succeeded"
    async with postgres_harness.session_factory() as db:
        run = (
            await db.execute(
                select(DiscoveryIngestionRun).where(
                    DiscoveryIngestionRun.channel_profile_id == request.channel_id
                )
            )
        ).scalar_one()
        signals = (
            await db.execute(
                select(DiscoverySignal).where(
                    DiscoverySignal.channel_profile_id == request.channel_id
                )
            )
        ).scalars().all()
    assert run.attempt_count == 2
    assert run.started_at == attempt_two_now
    assert run.status == "succeeded"
    assert run.last_error_code is None
    assert [signal.source_external_id for signal in signals] == ["attempt-two"]


@pytest.mark.asyncio
async def test_concurrent_same_queue_claim_observes_committed_running_claim(
    postgres_harness: PostgresHarness,
) -> None:
    now = datetime(2026, 7, 21, 18, 0, tzinfo=timezone.utc)
    request = await _seed_request(postgres_harness)
    provider_entered = asyncio.Event()
    release_provider = asyncio.Event()
    client = BlockingYouTubeClient(
        entered=provider_entered,
        release=release_provider,
        video_id="insert-winner",
    )
    service = DiscoveryIngestionService(youtube_client=client)

    async def raced_ingest() -> DiscoveryIngestionResult:
        async with postgres_harness.session_factory() as db:
            return await service.ingest(db, request, now)

    first_task = asyncio.create_task(raced_ingest())
    await asyncio.wait_for(provider_entered.wait(), timeout=5)
    async with postgres_harness.session_factory() as db:
        with pytest.raises(DiscoveryIngestionInProgressError):
            await service.ingest(db, request, now)
    release_provider.set()
    outcome = await asyncio.wait_for(first_task, timeout=5)

    assert isinstance(outcome, DiscoveryIngestionResult)
    assert len(client.requests) == 1
    async with postgres_harness.session_factory() as db:
        runs = (
            await db.execute(
                select(DiscoveryIngestionRun).where(
                    DiscoveryIngestionRun.channel_profile_id == request.channel_id
                )
            )
        ).scalars().all()
    assert len(runs) == 1
    assert runs[0].attempt_count == 1
    assert runs[0].status == "succeeded"


@pytest.mark.asyncio
async def test_terminal_channel_lock_observes_separately_committed_halt(
    postgres_harness: PostgresHarness,
) -> None:
    now = datetime(2026, 7, 21, 18, 0, tzinfo=timezone.utc)
    request = await _seed_request(postgres_harness)
    provider_entered = asyncio.Event()
    release_provider = asyncio.Event()
    client = BlockingYouTubeClient(
        entered=provider_entered,
        release=release_provider,
        video_id="halted-result",
    )
    task = asyncio.create_task(
        _ingest(
            postgres_harness,
            DiscoveryIngestionService(youtube_client=client),
            request,
            now,
        )
    )
    await asyncio.wait_for(provider_entered.wait(), timeout=5)
    async with postgres_harness.session_factory() as db:
        await db.execute(
            update(ChannelProfile)
            .where(ChannelProfile.id == request.channel_id)
            .values(halted_at=now, halt_reason="PostgreSQL race test")
        )
        await db.commit()
    release_provider.set()
    outcome = (
        await asyncio.wait_for(
            asyncio.gather(task, return_exceptions=True),
            timeout=5,
        )
    )[0]

    assert isinstance(outcome, DiscoveryIngestionConflictError)
    assert str(outcome) == "channel_unavailable"
    async with postgres_harness.session_factory() as db:
        run = (
            await db.execute(
                select(DiscoveryIngestionRun).where(
                    DiscoveryIngestionRun.channel_profile_id == request.channel_id
                )
            )
        ).scalar_one()
        signals = (
            await db.execute(
                select(DiscoverySignal).where(
                    DiscoverySignal.channel_profile_id == request.channel_id
                )
            )
        ).scalars().all()
    assert run.status == "failed"
    assert run.last_error_code == "channel_unavailable"
    assert signals == []
