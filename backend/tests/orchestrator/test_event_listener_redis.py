from __future__ import annotations

from datetime import datetime, timezone
import uuid

import pytest

from app.orchestrator import event_listener
from app.services.job_execution_authority import NodeExecutionClaim


def test_event_listener_socket_timeout_exceeds_blocking_read():
    client = event_listener._redis()
    options = client.connection_pool.connection_kwargs

    assert options["socket_timeout"] > event_listener.REDIS_BLOCK_MILLISECONDS / 1_000


@pytest.mark.asyncio
async def test_handle_completed_event_forwards_execution_claim(monkeypatch) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    output_artifact_id = uuid.uuid4()
    started_at = datetime(2026, 7, 22, 12, 30, tzinfo=timezone.utc)
    handled: list[tuple[uuid.UUID, uuid.UUID, uuid.UUID, NodeExecutionClaim]] = []

    class FakeEngine:
        async def on_node_completed(
            self,
            handled_job_id: uuid.UUID,
            handled_node_id: uuid.UUID,
            handled_artifact_id: uuid.UUID,
            *,
            claim: NodeExecutionClaim,
        ) -> None:
            handled.append(
                (handled_job_id, handled_node_id, handled_artifact_id, claim)
            )

    monkeypatch.setattr(event_listener, "engine", FakeEngine())

    await event_listener._handle_event(
        {
            "event": "node_completed",
            "job_id": str(job_id),
            "node_execution_id": str(node_execution_id),
            "output_artifact_id": str(output_artifact_id),
            "worker_id": "ffmpeg-worker@vp-gpu:42",
            "started_at": started_at.isoformat(),
        }
    )

    assert handled == [
        (
            job_id,
            node_execution_id,
            output_artifact_id,
            NodeExecutionClaim(
                job_id=job_id,
                node_execution_id=node_execution_id,
                worker_id="ffmpeg-worker@vp-gpu:42",
                started_at=started_at,
            ),
        )
    ]


@pytest.mark.asyncio
async def test_handle_failed_event_forwards_execution_claim(monkeypatch) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    started_at = datetime(2026, 7, 22, 12, 30, tzinfo=timezone.utc)
    handled: list[tuple[uuid.UUID, uuid.UUID, str, NodeExecutionClaim]] = []

    class FakeEngine:
        async def on_node_failed(
            self,
            handled_job_id: uuid.UUID,
            handled_node_id: uuid.UUID,
            error: str,
            *,
            claim: NodeExecutionClaim,
        ) -> None:
            handled.append((handled_job_id, handled_node_id, error, claim))

    monkeypatch.setattr(event_listener, "engine", FakeEngine())

    await event_listener._handle_event(
        {
            "event": "node_failed",
            "job_id": str(job_id),
            "node_execution_id": str(node_execution_id),
            "error": "render failed",
            "worker_id": "ffmpeg-worker@vp-gpu:42",
            "started_at": started_at.isoformat(),
        }
    )

    assert handled == [
        (
            job_id,
            node_execution_id,
            "render failed",
            NodeExecutionClaim(
                job_id=job_id,
                node_execution_id=node_execution_id,
                worker_id="ffmpeg-worker@vp-gpu:42",
                started_at=started_at,
            ),
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("worker_id", "started_at"),
    [
        ("", "2026-07-22T12:30:00+00:00"),
        ("ffmpeg-worker@vp-gpu:42", "not-a-timestamp"),
        ("ffmpeg-worker@vp-gpu:42", "2026-07-22T12:30:00"),
    ],
)
async def test_handle_event_ignores_missing_or_malformed_execution_claim(
    monkeypatch,
    worker_id: str | None,
    started_at: str | None,
) -> None:
    calls: list[str] = []

    class FakeEngine:
        async def on_node_completed(self, *args, **kwargs) -> None:
            calls.append("completed")

        async def on_node_failed(self, *args, **kwargs) -> None:
            calls.append("failed")

    monkeypatch.setattr(event_listener, "engine", FakeEngine())
    data = {
        "event": "node_completed",
        "job_id": str(uuid.uuid4()),
        "node_execution_id": str(uuid.uuid4()),
        "output_artifact_id": str(uuid.uuid4()),
    }
    if worker_id is not None:
        data["worker_id"] = worker_id
    if started_at is not None:
        data["started_at"] = started_at

    await event_listener._handle_event(data)

    assert calls == []


@pytest.mark.asyncio
async def test_handle_legacy_event_leaves_it_pending(monkeypatch) -> None:
    calls: list[str] = []

    class FakeEngine:
        async def on_node_completed(self, *args, **kwargs) -> None:
            calls.append("completed")

    monkeypatch.setattr(event_listener, "engine", FakeEngine())

    with pytest.raises(
        event_listener.UnverifiableExecutionClaimEvent,
        match="missing execution claim",
    ):
        await event_listener._handle_event(
            {
                "event": "node_completed",
                "job_id": str(uuid.uuid4()),
                "node_execution_id": str(uuid.uuid4()),
                "output_artifact_id": str(uuid.uuid4()),
            }
        )

    assert calls == []
