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
async def test_registered_event_xack_holds_registration_fence(
    monkeypatch,
) -> None:
    transaction_active = False
    lease_checked = False
    registration_id = uuid.uuid4()
    acknowledgements: list[str] = []

    class Transaction:
        async def __aenter__(self):
            nonlocal transaction_active
            transaction_active = True
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            nonlocal transaction_active
            transaction_active = False
            return False

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def begin(self):
            return Transaction()

    class Redis:
        async def xack(self, stream, group, message_id):
            assert transaction_active and lease_checked
            acknowledgements.append(message_id)

    async def require_identity(_db, checked_id, checked_epoch):
        nonlocal lease_checked
        assert transaction_active
        assert checked_id == registration_id
        assert checked_epoch == 23
        lease_checked = True

    monkeypatch.setattr(
        event_listener,
        "async_session",
        lambda: Session(),
        raising=False,
    )
    monkeypatch.setattr(
        event_listener,
        "require_worker_registration_identity",
        require_identity,
        raising=False,
    )

    await event_listener._ack_event(
        Redis(),
        "1-0",
        {
            "event": "node_completed",
            "job_id": str(uuid.uuid4()),
            "node_execution_id": str(uuid.uuid4()),
            "worker_id": "ffmpeg-worker@vp-gpu:42",
            "started_at": "2026-07-22T12:30:00+00:00",
            "worker_registration_id": str(registration_id),
            "worker_lease_epoch": 23,
        },
    )

    assert acknowledgements == ["1-0"]


@pytest.mark.asyncio
async def test_handle_completed_event_forwards_execution_claim(monkeypatch) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    output_artifact_id = uuid.uuid4()
    started_at = datetime(2026, 7, 22, 12, 30, tzinfo=timezone.utc)
    registration_id = uuid.uuid4()
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
            "worker_registration_id": str(registration_id),
            "worker_lease_epoch": "17",
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
                worker_registration_id=registration_id,
                worker_lease_epoch=17,
            ),
        )
    ]


@pytest.mark.asyncio
async def test_handle_failed_event_forwards_execution_claim(monkeypatch) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    started_at = datetime(2026, 7, 22, 12, 30, tzinfo=timezone.utc)
    registration_id = uuid.uuid4()
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
            "worker_registration_id": str(registration_id),
            "worker_lease_epoch": 18,
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
                worker_registration_id=registration_id,
                worker_lease_epoch=18,
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
@pytest.mark.parametrize(
    ("registration_id", "lease_epoch"),
    [
        (str(uuid.uuid4()), None),
        (None, 3),
        ("not-a-uuid", 3),
        (str(uuid.uuid4()), 0),
        (str(uuid.uuid4()), True),
    ],
)
async def test_handle_event_ignores_half_or_malformed_registration_claim(
    monkeypatch,
    registration_id,
    lease_epoch,
) -> None:
    calls: list[str] = []

    class FakeEngine:
        async def on_node_completed(self, *args, **kwargs) -> None:
            calls.append("completed")

    monkeypatch.setattr(event_listener, "engine", FakeEngine())
    data = {
        "event": "node_completed",
        "job_id": str(uuid.uuid4()),
        "node_execution_id": str(uuid.uuid4()),
        "output_artifact_id": str(uuid.uuid4()),
        "worker_id": "ffmpeg-worker@vp-gpu:42",
        "started_at": "2026-07-22T12:30:00+00:00",
    }
    if registration_id is not None:
        data["worker_registration_id"] = registration_id
    if lease_epoch is not None:
        data["worker_lease_epoch"] = lease_epoch

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
