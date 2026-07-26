from __future__ import annotations

import os
import uuid

import pytest
import redis.asyncio as redis

from app.services import vision_consumer_cutover as cutover
from app.services.vision_consumer_cutover import (
    VisionConsumerCutoverError,
    reconcile_vision_consumers,
    vision_consumers_converged,
)


class FakeRedis:
    def __init__(
        self,
        snapshots: list[list[dict[str, object]]],
        *,
        deleted_pending: dict[str, int] | None = None,
    ):
        self.snapshots = snapshots
        self.deleted_pending = deleted_pending or {}
        self.deleted: list[str] = []
        self.reads = 0

    async def xinfo_consumers(self, stream: str, group: str):
        assert stream == "vp:tasks:vision"
        assert group == "vision-workers"
        index = min(self.reads, len(self.snapshots) - 1)
        self.reads += 1
        return self.snapshots[index]

    async def xgroup_delconsumer(self, stream: str, group: str, consumer: str):
        assert stream == "vp:tasks:vision"
        assert group == "vision-workers"
        self.deleted.append(consumer)
        return self.deleted_pending.get(consumer, 0)

    async def eval(
        self,
        _script: str,
        key_count: int,
        stream: str,
        group: str,
        _managed_pattern: str,
    ):
        assert key_count == 1
        assert stream == "vp:tasks:vision"
        assert group == "vision-workers"
        records = self.snapshots[max(0, self.reads - 1)]
        managed = [
            row["name"]
            for row in records
            if str(row["name"]).startswith("vision-worker@150-vision:")
        ]
        legacy = [row["name"] for row in records if row["name"] not in managed]
        if any(self.deleted_pending.get(str(name), 0) for name in legacy):
            raise redis.ResponseError("VISION_PENDING")
        self.deleted.extend(str(name) for name in legacy)
        return [*managed, *legacy]


def consumer(name: str, *, pending: int = 0) -> dict[str, object]:
    return {
        "name": name,
        "pending": pending,
        "idle": 500,
        "inactive": 500,
    }


@pytest.mark.anyio
async def test_converged_requires_only_one_zero_pending_managed_consumer():
    managed = consumer("vision-worker@150-vision:1")

    assert await vision_consumers_converged(FakeRedis([[managed]])) is True
    assert (
        await vision_consumers_converged(
            FakeRedis([[managed, consumer("vision-worker@legacy:1")]])
        )
        is False
    )
    assert (
        await vision_consumers_converged(
            FakeRedis([[consumer("vision-worker@150-vision:1", pending=1)]])
        )
        is False
    )


@pytest.mark.anyio
async def test_reconcile_removes_only_zero_pending_legacy_consumers():
    managed = consumer("vision-worker@150-vision:1")
    legacy = [
        consumer("vision-worker@17add19d51d0:1"),
        consumer("vision-worker@7a2bcf87f570:1"),
    ]
    redis = FakeRedis([legacy + [managed], [managed]])

    result = await reconcile_vision_consumers(redis, wait_attempts=1)

    assert result == {
        "managed_consumer": "vision-worker@150-vision:1",
        "removed_consumers": [
            "vision-worker@17add19d51d0:1",
            "vision-worker@7a2bcf87f570:1",
        ],
    }
    assert redis.deleted == [
        "vision-worker@17add19d51d0:1",
        "vision-worker@7a2bcf87f570:1",
    ]


@pytest.mark.anyio
async def test_reconcile_waits_for_managed_consumer_before_deleting(monkeypatch):
    sleeps: list[float] = []

    async def fake_sleep(delay: float):
        sleeps.append(delay)

    monkeypatch.setattr("app.services.vision_consumer_cutover.asyncio.sleep", fake_sleep)
    legacy = consumer("vision-worker@7a2bcf87f570:1")
    managed = consumer("vision-worker@150-vision:1")
    redis = FakeRedis([[legacy], [legacy, managed], [managed]])

    await reconcile_vision_consumers(redis, wait_attempts=2, wait_delay_seconds=0.25)

    assert sleeps == [0.25]
    assert redis.deleted == ["vision-worker@7a2bcf87f570:1"]


@pytest.mark.anyio
async def test_reconcile_rejects_pending_without_deleting():
    redis = FakeRedis(
        [[consumer("vision-worker@7a2bcf87f570:1", pending=1), consumer("vision-worker@150-vision:1")]]
    )

    with pytest.raises(VisionConsumerCutoverError, match="pending"):
        await reconcile_vision_consumers(redis, wait_attempts=1)

    assert redis.deleted == []


@pytest.mark.anyio
async def test_reconcile_rejects_duplicate_managed_consumers_without_deleting():
    redis = FakeRedis(
        [
            [
                consumer("vision-worker@150-vision:1"),
                consumer("vision-worker@150-vision:2"),
            ]
        ]
    )

    with pytest.raises(VisionConsumerCutoverError, match="exactly one"):
        await reconcile_vision_consumers(redis, wait_attempts=1)

    assert redis.deleted == []


@pytest.mark.anyio
async def test_reconcile_rejects_pending_detected_by_atomic_cutover():
    legacy_name = "vision-worker@7a2bcf87f570:1"
    redis = FakeRedis(
        [[consumer(legacy_name), consumer("vision-worker@150-vision:1")]],
        deleted_pending={legacy_name: 1},
    )

    with pytest.raises(VisionConsumerCutoverError, match="pending"):
        await reconcile_vision_consumers(redis, wait_attempts=1)

    assert redis.deleted == []


@pytest.mark.anyio
async def test_reconcile_rejects_malformed_consumer_records():
    redis = FakeRedis([[{"name": "", "pending": 0}]])

    with pytest.raises(VisionConsumerCutoverError, match="malformed"):
        await reconcile_vision_consumers(redis, wait_attempts=1)


@pytest.mark.anyio
async def test_reconcile_atomically_preserves_consumer_when_pending_appears(
    monkeypatch: pytest.MonkeyPatch,
):
    redis_url = os.environ.get("VISION_CUTOVER_REDIS_TEST_URL")
    if not redis_url:
        pytest.skip("VISION_CUTOVER_REDIS_TEST_URL is not configured")

    stream = f"vp:test:vision-cutover:{uuid.uuid4()}"
    group = "vision-workers"
    legacy_name = "vision-worker@legacy-race:1"
    managed_name = "vision-worker@150-vision:1"
    primary = redis.from_url(redis_url, decode_responses=True)
    racing = redis.from_url(redis_url, decode_responses=True)

    class RacingRedis:
        def __init__(self):
            self.raced = False

        async def xinfo_consumers(self, target_stream: str, target_group: str):
            records = await primary.xinfo_consumers(target_stream, target_group)
            if not self.raced:
                self.raced = True
                claimed = await racing.xreadgroup(
                    target_group,
                    legacy_name,
                    {target_stream: ">"},
                    count=1,
                )
                assert claimed
            return records

        async def xgroup_delconsumer(
            self,
            target_stream: str,
            target_group: str,
            consumer_name: str,
        ):
            return await primary.xgroup_delconsumer(
                target_stream,
                target_group,
                consumer_name,
            )

        async def eval(self, *args):
            return await primary.eval(*args)

    monkeypatch.setattr(cutover, "VISION_STREAM", stream)
    monkeypatch.setattr(cutover, "VISION_GROUP", group)
    try:
        await primary.xadd(stream, {"task": "race"})
        await primary.xgroup_create(stream, group, id="0")
        await primary.xgroup_createconsumer(stream, group, legacy_name)
        await primary.xgroup_createconsumer(stream, group, managed_name)

        with pytest.raises(VisionConsumerCutoverError, match="pending"):
            await reconcile_vision_consumers(RacingRedis(), wait_attempts=1)

        consumers = await primary.xinfo_consumers(stream, group)
        assert any(
            row["name"] == legacy_name and row["pending"] == 1 for row in consumers
        )
    finally:
        await primary.delete(stream)
        await primary.aclose()
        await racing.aclose()
