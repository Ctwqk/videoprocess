from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import subprocess
import sys
from typing import Any
import uuid

import asyncpg  # type: ignore[import-untyped]
import httpx
import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
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
    DiscoveryIngestionConflictError,
    DiscoveryIngestionInProgressError,
    DiscoveryIngestionRequest,
    DiscoveryIngestionResult,
    DiscoveryIngestionService,
)


POSTGRES_URL = os.getenv(
    "DISCOVERY_INGESTION_POSTGRES_TEST_URL",
    os.getenv("CHANNEL_OPS_POSTGRES_TEST_URL", ""),
)
BACKEND_ROOT = Path(__file__).resolve().parents[2]

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


class ClaimBarrierSession(AsyncSession):
    def __init__(
        self,
        *args: Any,
        claim_barrier: asyncio.Barrier,
        claim_flushes: list[int],
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._claim_barrier = claim_barrier
        self._claim_flushes = claim_flushes
        self._claim_waited = False

    async def flush(self, objects: Sequence[Any] | None = None) -> None:
        if not self._claim_waited and any(
            isinstance(item, DiscoveryIngestionRun) for item in self.new
        ):
            self._claim_waited = True
            self._claim_flushes.append(1)
            await self._claim_barrier.wait()
        await super().flush(objects)


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
        )
        db.add(queue_item)
        await db.commit()
        return DiscoveryIngestionRequest(
            channel_id=channel.id,
            queue_item_id=queue_item.id,
            source="youtube_search",
            scheduler_bucket=scheduler_bucket,
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
async def test_concurrent_unique_insert_loser_reloads_committed_running_claim(
    postgres_harness: PostgresHarness,
) -> None:
    now = datetime(2026, 7, 21, 18, 0, tzinfo=timezone.utc)
    request = await _seed_request(postgres_harness)
    claim_barrier = asyncio.Barrier(2)
    claim_flushes: list[int] = []
    provider_entered = asyncio.Event()
    release_provider = asyncio.Event()
    client = BlockingYouTubeClient(
        entered=provider_entered,
        release=release_provider,
        video_id="insert-winner",
    )
    service = DiscoveryIngestionService(youtube_client=client)

    async def raced_ingest() -> DiscoveryIngestionResult:
        async with ClaimBarrierSession(
            postgres_harness.engine,
            expire_on_commit=False,
            claim_barrier=claim_barrier,
            claim_flushes=claim_flushes,
        ) as db:
            return await service.ingest(db, request, now)

    tasks = [asyncio.create_task(raced_ingest()) for _ in range(2)]
    await asyncio.wait_for(provider_entered.wait(), timeout=5)
    done, pending = await asyncio.wait(
        tasks,
        timeout=5,
        return_when=asyncio.FIRST_COMPLETED,
    )
    assert len(done) == 1
    loser_outcome = next(iter(done)).exception()
    assert isinstance(loser_outcome, DiscoveryIngestionInProgressError)
    release_provider.set()
    outcomes = await asyncio.wait_for(
        asyncio.gather(*tasks, return_exceptions=True),
        timeout=5,
    )

    assert len(claim_flushes) == 2
    assert sum(isinstance(item, DiscoveryIngestionResult) for item in outcomes) == 1
    assert len(pending) == 1
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
