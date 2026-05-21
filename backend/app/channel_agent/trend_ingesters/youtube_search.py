from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.channel_agent import ChannelProfile, DiscoverySignal, TopicLane


@dataclass(frozen=True)
class TrendIngestResult:
    created_count: int
    expired_count: int


class YouTubeTrendIngester:
    def __init__(
        self,
        *,
        youtube_client,
        min_view_count: int = 1000,
        max_results: int = 25,
        seed_ttl: timedelta = timedelta(days=1),
    ) -> None:
        self.youtube_client = youtube_client
        self.min_view_count = min_view_count
        self.max_results = max_results
        self.seed_ttl = seed_ttl

    async def ingest_channel(self, db: AsyncSession, *, channel_id: str, now: datetime) -> TrendIngestResult:
        channel = await db.get(ChannelProfile, _uuid(channel_id))
        if channel is None:
            raise ValueError("Channel not found")
        current = _as_utc(now)
        expired = await self._expire_stale(db, channel=channel, now=current)
        lanes = (
            await db.execute(
                select(TopicLane)
                .where(TopicLane.channel_profile_id == channel.id)
                .where(TopicLane.enabled.is_(True))
                .order_by(TopicLane.weight.desc(), TopicLane.created_at.asc())
            )
        ).scalars().all()
        created = 0
        for lane in lanes:
            query = _lane_query(lane)
            results = await self.youtube_client.search_videos(
                query=query,
                published_after=current - timedelta(hours=24),
                region_code=_region_code(channel),
                max_results=self.max_results,
            )
            for result in results:
                if _view_count(result) < self.min_view_count:
                    continue
                source_external_id = _source_external_id(result)
                if not source_external_id:
                    continue
                existing = await self._existing_signal(
                    db,
                    channel=channel,
                    source_external_id=source_external_id,
                )
                if existing is not None:
                    existing.topic_lane_id = lane.id
                    existing.source_url = str(result.get("url") or "")
                    existing.title = str(result.get("title") or "YouTube trend")
                    existing.summary = str(result.get("description") or "")
                    existing.keywords_json = list(lane.keywords_json or [])
                    existing.observed_at = current
                    existing.expires_at = current + self.seed_ttl
                    existing.trend_score = float(_view_count(result))
                    existing.raw_json = dict(result)
                    if existing.status == "expired":
                        existing.status = "active"
                    continue
                db.add(
                    DiscoverySignal(
                        channel_profile_id=channel.id,
                        topic_lane_id=lane.id,
                        source="youtube_search",
                        source_url=str(result.get("url") or ""),
                        source_external_id=source_external_id,
                        title=str(result.get("title") or "YouTube trend"),
                        summary=str(result.get("description") or ""),
                        keywords_json=list(lane.keywords_json or []),
                        observed_at=current,
                        expires_at=current + self.seed_ttl,
                        trend_score=float(_view_count(result)),
                        novelty_score=0.0,
                        raw_json=dict(result),
                        status="active",
                    )
                )
                created += 1
        await db.commit()
        return TrendIngestResult(created_count=created, expired_count=expired)

    async def _expire_stale(self, db: AsyncSession, *, channel: ChannelProfile, now: datetime) -> int:
        signals = (
            await db.execute(
                select(DiscoverySignal)
                .where(DiscoverySignal.channel_profile_id == channel.id)
                .where(DiscoverySignal.source == "youtube_search")
                .where(DiscoverySignal.status == "active")
            )
        ).scalars().all()
        expired = 0
        for signal in signals:
            expires_at = _parse_datetime(signal.expires_at)
            if expires_at is not None and expires_at < now:
                signal.status = "expired"
                expired += 1
        return expired

    async def _existing_signal(
        self,
        db: AsyncSession,
        *,
        channel: ChannelProfile,
        source_external_id: str,
    ) -> DiscoverySignal | None:
        if not source_external_id:
            return None
        return (
            await db.execute(
                select(DiscoverySignal)
                .where(DiscoverySignal.channel_profile_id == channel.id)
                .where(DiscoverySignal.source == "youtube_search")
                .where(DiscoverySignal.source_external_id == source_external_id)
                .limit(1)
            )
        ).scalars().first()


def _lane_query(lane: TopicLane) -> str:
    keywords = list(lane.keywords_json or [])
    return str(keywords[0] if keywords else lane.name)


def _region_code(channel: ChannelProfile) -> str:
    policy = dict(channel.content_mix_policy_json or {})
    return str(policy.get("region_code") or "US")


def _view_count(result: dict[str, Any]) -> int:
    try:
        return int(result.get("view_count") or result.get("views") or 0)
    except (TypeError, ValueError):
        return 0


def _source_external_id(result: dict[str, Any]) -> str:
    return str(result.get("video_id") or result.get("id") or result.get("url") or "").strip()


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _uuid(value: Any) -> uuid.UUID:
    return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
