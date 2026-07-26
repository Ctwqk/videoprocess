from __future__ import annotations

import json
import os
import time
import uuid
from types import SimpleNamespace

import pytest
import redis.asyncio as aioredis

from worker import main as worker_main


REDIS_TEST_URL = os.getenv("WORKER_REDIS_TEST_URL", "")


@pytest.mark.asyncio
@pytest.mark.skipif(
    not REDIS_TEST_URL,
    reason="set WORKER_REDIS_TEST_URL for Redis integration tests",
)
async def test_preferred_worker_reclaims_exact_pel_message_before_relaxation(
    monkeypatch,
) -> None:
    redis = aioredis.from_url(REDIS_TEST_URL, decode_responses=True)
    stream = f"test:worker-affinity:{uuid.uuid4()}"
    group = f"group-{uuid.uuid4()}"
    nonpreferred = f"consumer-nonpreferred-{uuid.uuid4()}"
    preferred = f"consumer-preferred-{uuid.uuid4()}"
    dispatch_key = uuid.uuid4()
    payload = {
        "job_id": str(uuid.uuid4()),
        "node_execution_id": str(uuid.uuid4()),
        "node_id": "affinity",
        "node_type": "vision",
        "config": "{}",
        "input_artifacts": "{}",
        "dispatch_key": str(dispatch_key),
        "preferred_hosts": json.dumps(["worker-127"]),
        "affinity_enqueued_at": str(int(time.time())),
        "affinity_bounces": "0",
    }
    processed: list[tuple[str, dict[str, str]]] = []
    try:
        await redis.xgroup_create(stream, group, id="0", mkstream=True)
        message_id = await redis.xadd(stream, payload)
        delivered = await redis.xreadgroup(
            group,
            nonpreferred,
            {stream: ">"},
            count=1,
        )
        assert delivered[0][1][0][0] == message_id

        monkeypatch.setattr(worker_main, "TASK_STREAM", stream)
        monkeypatch.setattr(worker_main, "CONSUMER_GROUP", group)
        monkeypatch.setattr(worker_main, "WORKER_HOST", "worker-150")
        assert await worker_main._maybe_defer_for_affinity(
            redis,
            message_id,
            payload,
            worker_lease=SimpleNamespace(),
        )
        assert await redis.xlen(stream) == 1

        async def process(
            _redis,
            claimed_id,
            claimed_payload,
            *,
            worker_lease,
        ):
            processed.append((claimed_id, claimed_payload))

        monkeypatch.setattr(worker_main, "WORKER_HOST", "worker-127")
        monkeypatch.setattr(worker_main, "WORKER_ID", preferred)
        monkeypatch.setattr(
            worker_main,
            "AFFINITY_RECLAIM_MIN_IDLE_MS",
            0,
        )
        monkeypatch.setattr(worker_main, "_process_message", process)
        await worker_main._reclaim_preferred_pending(
            redis,
            worker_lease=SimpleNamespace(),
        )
        await worker_main._reclaim_preferred_pending(
            redis,
            worker_lease=SimpleNamespace(),
        )

        assert processed == [(message_id, payload)]
        pending = await redis.xpending_range(
            stream,
            group,
            message_id,
            message_id,
            1,
        )
        assert pending[0]["consumer"] == preferred
        assert await redis.xlen(stream) == 1
    finally:
        await redis.delete(stream)
        await redis.aclose()
