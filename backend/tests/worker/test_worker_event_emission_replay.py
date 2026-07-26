from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
import redis.asyncio as aioredis

from app.services.job_execution_authority import NodeExecutionClaim
from worker import main as worker_main


REDIS_TEST_URL = os.getenv("WORKER_REDIS_TEST_URL", "")


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None


class _Session:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    def begin(self):
        return _Transaction()


def _session_factory():
    return _Session()


@pytest.mark.asyncio
async def test_prepared_emission_retries_pre_xadd_and_post_xadd_windows_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    emission_id = uuid.uuid4()
    claim = NodeExecutionClaim(
        job_id=uuid.uuid4(),
        node_execution_id=uuid.uuid4(),
        worker_id="vision-worker@127:1",
        started_at=datetime(2026, 7, 26, tzinfo=timezone.utc),
        worker_registration_id=uuid.uuid4(),
        worker_lease_epoch=7,
    )
    payload = {
        "event": "node_failed",
        "job_id": str(claim.job_id),
        "node_execution_id": str(claim.node_execution_id),
    }
    payload_sha256 = worker_main._canonical_task_payload_sha256(payload)
    expected_emission_id = emission_id
    loads = 0
    marks = 0

    async def load_emission(
        db,
        requested_emission_id,
        *,
        registration_id,
        lease_epoch,
    ):
        nonlocal loads
        loads += 1
        assert requested_emission_id == emission_id
        assert registration_id == claim.worker_registration_id
        assert lease_epoch == claim.worker_lease_epoch
        return SimpleNamespace(
            id=emission_id,
            claim=claim,
            redis_stream="vp:events",
            consumer_group="orchestrator",
            payload_sha256=payload_sha256,
            payload=dict(payload),
            event_type="node_failed",
        )

    async def mark_emitted(
        db,
        loaded_claim,
        *,
        emission_id: uuid.UUID,
        message_id: str,
    ):
        nonlocal marks
        marks += 1
        assert loaded_claim == claim
        assert emission_id == expected_emission_id
        assert message_id == "1710000000001-0"
        if marks == 1:
            raise RuntimeError("database mark crashed")

    class Redis:
        def __init__(self) -> None:
            self.calls = 0
            self.marker: str | None = None
            self.stream_entries: list[tuple[str, ...]] = []

        async def eval(
            self,
            script,
            key_count,
            stream,
            marker,
            *fields,
        ):
            self.calls += 1
            assert script == worker_main._IDEMPOTENT_EVENT_XADD_SCRIPT
            assert key_count == 2
            assert stream == "vp:events"
            assert marker == f"vp:worker-event-emission:{emission_id}"
            if self.calls == 1:
                raise ConnectionError("definite pre-XADD failure")
            if self.marker is None:
                self.marker = "1710000000001-0"
                self.stream_entries.append(tuple(fields))
            return self.marker

    monkeypatch.setattr(
        worker_main,
        "load_prepared_worker_event_emission",
        load_emission,
        raising=False,
    )
    monkeypatch.setattr(
        worker_main,
        "mark_worker_event_emitted",
        mark_emitted,
    )
    redis = Redis()

    result = await worker_main._send_prepared_event_emission(
        redis,
        emission_id,
        registration_id=claim.worker_registration_id,
        lease_epoch=claim.worker_lease_epoch,
        max_attempts=3,
        session_factory=_session_factory,
    )

    assert result == claim
    assert loads == 3
    assert marks == 2
    assert redis.calls == 3
    assert len(redis.stream_entries) == 1


@pytest.mark.asyncio
@pytest.mark.skipif(
    not REDIS_TEST_URL,
    reason="set WORKER_REDIS_TEST_URL for Redis integration tests",
)
async def test_event_emission_marker_returns_one_real_redis_entry() -> None:
    redis = aioredis.from_url(REDIS_TEST_URL, decode_responses=True)
    stream = f"test:worker-events:{uuid.uuid4()}"
    marker = f"test:worker-event-emission:{uuid.uuid4()}"
    fields = ("event", "node_completed", "job_id", str(uuid.uuid4()))
    try:
        first_id = await redis.eval(
            worker_main._IDEMPOTENT_EVENT_XADD_SCRIPT,
            2,
            stream,
            marker,
            *fields,
        )
        second_id = await redis.eval(
            worker_main._IDEMPOTENT_EVENT_XADD_SCRIPT,
            2,
            stream,
            marker,
            *fields,
        )

        assert second_id == first_id
        assert await redis.xlen(stream) == 1
        assert await redis.get(marker) == first_id
    finally:
        await redis.delete(stream, marker)
        await redis.aclose()
