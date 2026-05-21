from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.channel_agent.trend_ingesters.youtube_search import YouTubeTrendIngester
from app.models.channel_agent import ChannelProfile, DiscoverySignal, ManualSeed, TopicLane


class FakeTrendYouTubeClient:
    def __init__(self):
        self.requests: list[dict[str, object]] = []

    async def search_videos(self, *, query: str, published_after: datetime, region_code: str, max_results: int):
        self.requests.append(
            {
                "query": query,
                "published_after": published_after,
                "region_code": region_code,
                "max_results": max_results,
            }
        )
        return [
            {
                "video_id": "new-hot",
                "title": "New hot AI clip",
                "description": "A useful trend",
                "view_count": 2500,
                "url": "https://youtu.be/new-hot",
            },
            {
                "video_id": "too-small",
                "title": "Small clip",
                "description": "Low signal",
                "view_count": 10,
            },
        ]


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
async def test_youtube_trend_ingester_materializes_discovery_signal_and_expires_stale_signal(trend_session):
    now = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
    channel = ChannelProfile(name="trends", language="zh", content_mix_policy_json={"region_code": "US"})
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

    ingester = YouTubeTrendIngester(youtube_client=FakeTrendYouTubeClient(), min_view_count=100)
    result = await ingester.ingest_channel(trend_session, channel_id=str(channel.id), now=now)

    signals = (await trend_session.execute(select(DiscoverySignal).order_by(DiscoverySignal.created_at.asc()))).scalars().all()
    active_signals = [signal for signal in signals if signal.status == "active"]
    active_trend_seeds = (
        await trend_session.execute(
            select(ManualSeed).where(ManualSeed.source_policy == "trend_youtube").where(ManualSeed.status == "active")
        )
    ).scalars().all()
    assert result.created_count == 1
    assert result.expired_count == 1
    assert stale.status == "expired"
    assert active_trend_seeds == []
    assert len(active_signals) == 1
    assert active_signals[0].title == "New hot AI clip"
    assert active_signals[0].source_external_id == "new-hot"
    assert active_signals[0].source_url == "https://youtu.be/new-hot"
    assert active_signals[0].raw_json["view_count"] == 2500
