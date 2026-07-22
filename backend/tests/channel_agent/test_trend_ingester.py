from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.channel_agent.trend_ingesters.youtube_search import YouTubeTrendIngester
from app.models.channel_agent import ChannelProfile, DiscoverySignal, ManualSeed, TopicLane


class FakeTrendYouTubeClient:
    def __init__(self, results: list[dict[str, Any]] | None = None):
        self.requests: list[dict[str, object]] = []
        self.results = results or []

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
        return [dict(result) for result in self.results]


@pytest.fixture
async def trend_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(ChannelProfile.__table__.create)
        await conn.run_sync(TopicLane.__table__.create)
        await conn.run_sync(ManualSeed.__table__.create)
        await conn.run_sync(DiscoverySignal.__table__.create)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session
    await engine.dispose()


@pytest.mark.asyncio
async def test_youtube_trend_ingester_orders_and_limits_lanes_and_bounds_provider_arguments(
    trend_session,
):
    now = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
    channel = ChannelProfile(name="trends", language="zh")
    trend_session.add(channel)
    await trend_session.flush()
    trend_session.add_all(
        [
            TopicLane(
                channel_profile_id=channel.id,
                name="low",
                weight=1.0,
                keywords_json=["low keyword"],
                created_at=now - timedelta(hours=3),
            ),
            TopicLane(
                channel_profile_id=channel.id,
                name="high",
                weight=3.0,
                keywords_json=["high keyword", "unused keyword"],
                created_at=now - timedelta(hours=1),
            ),
            TopicLane(
                channel_profile_id=channel.id,
                name="middle fallback",
                weight=2.0,
                keywords_json=[],
                created_at=now - timedelta(hours=2),
            ),
            TopicLane(
                channel_profile_id=channel.id,
                name="disabled",
                weight=10.0,
                keywords_json=["disabled keyword"],
                enabled=False,
                created_at=now - timedelta(hours=4),
            ),
        ]
    )
    await trend_session.commit()
    client = FakeTrendYouTubeClient()
    ingester = YouTubeTrendIngester(
        youtube_client=client,
        min_view_count=100,
        max_results=7,
        max_queries=2,
        region_code="GB",
    )

    result = await ingester.ingest_channel(
        trend_session,
        channel_id=str(channel.id),
        now=now,
    )

    assert [request["query"] for request in client.requests] == [
        "high keyword",
        "middle fallback",
    ]
    assert all(
        request["published_after"] == now - timedelta(hours=24)
        for request in client.requests
    )
    assert all(request["region_code"] == "GB" for request in client.requests)
    assert all(request["max_results"] == 7 for request in client.requests)
    assert result.query_count == 2


@pytest.mark.asyncio
async def test_youtube_trend_ingester_reports_create_refresh_expiry_and_preserves_converted(
    trend_session,
):
    now = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
    channel = ChannelProfile(name="trends", language="zh")
    trend_session.add(channel)
    await trend_session.flush()
    lane = TopicLane(channel_profile_id=channel.id, name="AI", keywords_json=["ai"])
    trend_session.add(lane)
    await trend_session.flush()
    stale = DiscoverySignal(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        source="youtube_search",
        source_external_id="old-hot",
        title="old",
        status="active",
        expires_at=now - timedelta(days=1),
    )
    converted = DiscoverySignal(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        source="youtube_search",
        source_external_id="converted-hot",
        title="converted old title",
        status="converted",
        expires_at=now - timedelta(hours=1),
    )
    expired = DiscoverySignal(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        source="youtube_search",
        source_external_id="expired-hot",
        title="expired old title",
        status="expired",
        expires_at=now - timedelta(hours=1),
    )
    trend_session.add_all([stale, converted, expired])
    await trend_session.commit()
    client = FakeTrendYouTubeClient(
        [
            {
                "video_id": "new-hot",
                "title": "New hot AI clip",
                "description": "A useful trend",
                "view_count": 2500,
                "url": "https://youtu.be/new-hot",
            },
            {
                "video_id": "converted-hot",
                "title": "Converted refreshed",
                "description": "Keep its terminal state",
                "view_count": 3000,
                "url": "https://youtu.be/converted-hot",
            },
            {
                "video_id": "expired-hot",
                "title": "Expired refreshed",
                "description": "Reactivate this one",
                "view_count": 3500,
                "url": "https://youtu.be/expired-hot",
            },
            {
                "video_id": "too-small",
                "title": "Small clip",
                "description": "Low signal",
                "view_count": 10,
            },
        ]
    )
    ingester = YouTubeTrendIngester(youtube_client=client, min_view_count=100)

    result = await ingester.ingest_channel(
        trend_session,
        channel_id=str(channel.id),
        now=now,
    )

    signals = (
        await trend_session.execute(
            select(DiscoverySignal).order_by(DiscoverySignal.created_at.asc())
        )
    ).scalars().all()
    by_external_id = {signal.source_external_id: signal for signal in signals}
    active_trend_seeds = (
        await trend_session.execute(
            select(ManualSeed)
            .where(ManualSeed.source_policy == "trend_youtube")
            .where(ManualSeed.status == "active")
        )
    ).scalars().all()
    assert result.created_count == 1
    assert result.refreshed_count == 2
    assert result.expired_count == 1
    assert result.query_count == 1
    assert by_external_id["old-hot"].status == "expired"
    assert by_external_id["converted-hot"].status == "converted"
    assert by_external_id["converted-hot"].title == "Converted refreshed"
    assert by_external_id["expired-hot"].status == "active"
    assert by_external_id["new-hot"].raw_json["view_count"] == 2500
    assert active_trend_seeds == []


@pytest.mark.asyncio
async def test_youtube_trend_ingester_flushes_without_committing(trend_session):
    now = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
    channel = ChannelProfile(name="trends", language="zh")
    trend_session.add(channel)
    await trend_session.flush()
    lane = TopicLane(channel_profile_id=channel.id, name="AI", keywords_json=["ai"])
    trend_session.add(lane)
    await trend_session.flush()
    stale = DiscoverySignal(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        source="youtube_search",
        source_external_id="old-hot",
        title="old",
        status="active",
        expires_at=now - timedelta(days=1),
    )
    trend_session.add(stale)
    await trend_session.commit()
    ingester = YouTubeTrendIngester(
        youtube_client=FakeTrendYouTubeClient(
            [{"video_id": "new-hot", "view_count": 2500}]
        ),
        min_view_count=100,
    )

    await ingester.ingest_channel(
        trend_session,
        channel_id=str(channel.id),
        now=now,
    )

    assert trend_session.in_transaction()
    await trend_session.rollback()
    persisted = (
        await trend_session.execute(select(DiscoverySignal))
    ).scalars().all()
    assert [(signal.source_external_id, signal.status) for signal in persisted] == [
        ("old-hot", "active")
    ]
