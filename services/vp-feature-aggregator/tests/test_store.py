from __future__ import annotations

from feature_aggregator.store.redis import RedisWindowStore


class FakeRedis:
    def __init__(self) -> None:
        self.set_calls: list[tuple[str, str, bool, int]] = []
        self.keys: set[str] = set()

    async def set(self, key: str, value: str, *, nx: bool, ex: int) -> bool:
        self.set_calls.append((key, value, nx, ex))
        if nx and key in self.keys:
            return False
        self.keys.add(key)
        return True

    async def hincrby(self, key: str, bucket: str, amount: int) -> None:
        pass


async def test_redis_mark_seen_namespaces_event_ids_by_topic():
    client = FakeRedis()
    store = RedisWindowStore(client, dedupe_ttl_seconds=604800)

    created = await store.mark_seen("vp.actor.actions.v1", "event-1")

    assert created is True
    assert client.set_calls == [
        ("risk:dedupe:vp.actor.actions.v1:event-1", "1", True, 604800)
    ]


async def test_redis_mark_seen_returns_false_for_duplicate_namespaced_key():
    client = FakeRedis()
    store = RedisWindowStore(client, dedupe_ttl_seconds=604800)

    first = await store.mark_seen("vp.actor.actions.v1", "event-1")
    second = await store.mark_seen("vp.actor.actions.v1", "event-1")

    assert first is True
    assert second is False
    assert client.set_calls == [
        ("risk:dedupe:vp.actor.actions.v1:event-1", "1", True, 604800),
        ("risk:dedupe:vp.actor.actions.v1:event-1", "1", True, 604800),
    ]
