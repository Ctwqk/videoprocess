from __future__ import annotations

import redis.asyncio as redis

from feature_aggregator.store import dedupe_key


class RedisWindowStore:
    def __init__(self, client: redis.Redis, *, dedupe_ttl_seconds: int) -> None:
        self.client = client
        self.dedupe_ttl_seconds = dedupe_ttl_seconds

    async def mark_seen(self, topic_version: str, event_id: str) -> bool:
        key = f"risk:dedupe:{dedupe_key(topic_version, event_id)}"
        created = await self.client.set(key, "1", nx=True, ex=self.dedupe_ttl_seconds)
        return bool(created)

    async def increment_bucket(self, actor_id: str, metric: str, bucket: str) -> None:
        await self.client.hincrby(f"risk:actor:{actor_id}:{metric}", bucket, 1)
