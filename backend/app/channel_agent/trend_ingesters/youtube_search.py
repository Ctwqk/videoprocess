from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.channel_agent import ChannelProfile, ManualSeed, TopicLane


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
                if await self._existing_seed(db, channel=channel, source_video_id=str(result.get("video_id") or "")):
                    continue
                db.add(
                    ManualSeed(
                        channel_profile_id=channel.id,
                        topic_lane_id=lane.id,
                        prompt=_prompt(result),
                        title_seed=str(result.get("title") or "YouTube trend"),
                        source_policy="trend_youtube",
                        source_platforms_json=["youtube"],
                        constraints_json={
                            "source_video_id": str(result.get("video_id") or ""),
                            "source_url": str(result.get("url") or ""),
                            "view_count": _view_count(result),
                            "expires_at": (current + self.seed_ttl).isoformat(),
                        },
                    )
                )
                created += 1
        await db.commit()
        return TrendIngestResult(created_count=created, expired_count=expired)

    async def _expire_stale(self, db: AsyncSession, *, channel: ChannelProfile, now: datetime) -> int:
        seeds = (
            await db.execute(
                select(ManualSeed)
                .where(ManualSeed.channel_profile_id == channel.id)
                .where(ManualSeed.source_policy == "trend_youtube")
                .where(ManualSeed.status == "active")
            )
        ).scalars().all()
        expired = 0
        for seed in seeds:
            expires_at = _parse_datetime((seed.constraints_json or {}).get("expires_at"))
            if expires_at is not None and expires_at < now:
                seed.status = "expired"
                expired += 1
        return expired

    async def _existing_seed(self, db: AsyncSession, *, channel: ChannelProfile, source_video_id: str) -> bool:
        if not source_video_id:
            return False
        seeds = (
            await db.execute(
                select(ManualSeed)
                .where(ManualSeed.channel_profile_id == channel.id)
                .where(ManualSeed.source_policy == "trend_youtube")
                .where(ManualSeed.status == "active")
            )
        ).scalars().all()
        return any(str((seed.constraints_json or {}).get("source_video_id") or "") == source_video_id for seed in seeds)


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


def _prompt(result: dict[str, Any]) -> str:
    title = str(result.get("title") or "YouTube trend")
    description = str(result.get("description") or "").strip()
    if description:
        return f"Create a short video inspired by this YouTube trend: {title}. Source summary: {description}"
    return f"Create a short video inspired by this YouTube trend: {title}."


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
