from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models.job import JobStatus, NodeStatus
from app.models.registered_worker_event_receipt import (
    RegisteredWorkerEventReceipt,
    WorkerEventDispatch,
)
from app.services.registered_worker_event_receipt import (
    RegisteredWorkerEventError,
    RegisteredWorkerEventReceiptService,
    canonical_redis_payload_sha256,
    parse_registered_worker_event,
)


@pytest.fixture
async def receipt_session_factory(tmp_path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'registered-events.sqlite3'}",
        json_serializer=lambda value: json.dumps(value, default=str),
    )
    async with engine.begin() as connection:
        await connection.run_sync(
            RegisteredWorkerEventReceipt.__table__.create
        )
        await connection.run_sync(WorkerEventDispatch.__table__.create)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


def _payload(**overrides: str) -> dict[str, str]:
    payload = {
        "event": "node_completed",
        "job_id": "00000000-0000-4000-8000-000000000201",
        "node_execution_id": "00000000-0000-4000-8000-000000000202",
        "output_artifact_id": "00000000-0000-4000-8000-000000000203",
        "worker_id": "vision-worker@127:42:instance",
        "started_at": "2026-07-26T12:00:00+00:00",
        "worker_registration_id": (
            "00000000-0000-4000-8000-000000000204"
        ),
        "worker_lease_epoch": "9",
        "task_stream": "vp:tasks:vision",
        "task_group": "vision-workers",
        "task_message_id": "1710000000000-4",
    }
    payload.update(overrides)
    return payload


def _authority(event):
    return SimpleNamespace(
        channel=None,
        task=None,
        schedule=SimpleNamespace(
            state="OPEN",
            guarded_job_id=event.job_id,
        ),
        job=SimpleNamespace(id=event.job_id, status=JobStatus.RUNNING),
        node=SimpleNamespace(
            id=event.node_execution_id,
            status=NodeStatus.RUNNING,
            worker_id=event.claim.worker_id,
            started_at=event.claim.started_at,
            worker_registration_id=event.claim.worker_registration_id,
            worker_lease_epoch=event.claim.worker_lease_epoch,
        ),
    )


def test_canonical_payload_hash_is_order_stable_and_string_only() -> None:
    payload = _payload()
    expected = canonical_redis_payload_sha256(payload)

    assert canonical_redis_payload_sha256(
        dict(reversed(tuple(payload.items())))
    ) == expected
    with pytest.raises(RegisteredWorkerEventError, match="strings"):
        canonical_redis_payload_sha256({**payload, "worker_lease_epoch": 9})


def test_registered_event_parser_requires_exact_task_and_worker_claim() -> None:
    event = parse_registered_worker_event(
        redis_stream="vp:events",
        consumer_group="orchestrator",
        message_id="1710000001000-0",
        payload=_payload(),
    )

    assert event.claim.worker_lease_epoch == 9
    assert event.source_task_message_id == "1710000000000-4"
    assert event.payload_sha256 == canonical_redis_payload_sha256(_payload())

    for missing in (
        "worker_registration_id",
        "worker_lease_epoch",
        "task_stream",
        "task_group",
        "task_message_id",
    ):
        payload = _payload()
        del payload[missing]
        with pytest.raises(RegisteredWorkerEventError):
            parse_registered_worker_event(
                redis_stream="vp:events",
                consumer_group="orchestrator",
                message_id="1710000001000-0",
                payload=payload,
            )


@pytest.mark.asyncio
async def test_accept_and_apply_holds_node_and_observer_authority_atomically(
    receipt_session_factory,
) -> None:
    event = parse_registered_worker_event(
        redis_stream="vp:events",
        consumer_group="orchestrator",
        message_id="1710000001000-0",
        payload=_payload(),
    )
    order: list[str] = []
    apply_count = 0

    async def lock_authority(db, job_id, **kwargs):
        assert db.in_transaction()
        assert job_id == event.job_id
        assert kwargs == {
            "node_execution_id": event.node_execution_id,
            "lock_all_nodes": True,
        }
        order.append("node-lock")
        return _authority(event)

    async def observe(db, claim):
        assert db.in_transaction()
        assert claim == event.claim
        order.append("observer")

    async def apply(db, receipt, accepted_event):
        nonlocal apply_count
        assert db.in_transaction()
        assert receipt.application_state == "accepted"
        assert accepted_event == event
        apply_count += 1
        order.append("apply")

    service = RegisteredWorkerEventReceiptService(
        receipt_session_factory,
        authority_locker=lock_authority,
        lease_observer=observe,
    )
    receipt_id = await service.accept_and_apply(event, apply)
    repeated_id = await service.accept_and_apply(event, apply)

    assert repeated_id == receipt_id
    assert apply_count == 1
    assert order == ["node-lock", "observer", "apply"]
    async with receipt_session_factory() as db:
        receipt = await db.get(RegisteredWorkerEventReceipt, receipt_id)
    assert receipt is not None
    assert receipt.application_state == "applied"
    assert receipt.applied_at is not None


@pytest.mark.asyncio
async def test_existing_receipt_fact_mismatch_fails_closed(
    receipt_session_factory,
) -> None:
    first = parse_registered_worker_event(
        redis_stream="vp:events",
        consumer_group="orchestrator",
        message_id="1710000001000-0",
        payload=_payload(),
    )

    async def lock_authority(*args, **kwargs):
        return _authority(first)

    async def observe(*args, **kwargs):
        return None

    async def apply(*args, **kwargs):
        return None

    service = RegisteredWorkerEventReceiptService(
        receipt_session_factory,
        authority_locker=lock_authority,
        lease_observer=observe,
    )
    await service.accept_and_apply(first, apply)
    changed = parse_registered_worker_event(
        redis_stream="vp:events",
        consumer_group="orchestrator",
        message_id=first.message_id,
        payload=_payload(
            output_artifact_id=(
                "00000000-0000-4000-8000-000000000299"
            )
        ),
    )

    with pytest.raises(RegisteredWorkerEventError, match="mismatch"):
        await service.accept_and_apply(changed, apply)


@pytest.mark.asyncio
async def test_applied_receipt_authorizes_ack_without_rechecking_worker_lease(
    receipt_session_factory,
) -> None:
    event = parse_registered_worker_event(
        redis_stream="vp:events",
        consumer_group="orchestrator",
        message_id="1710000001000-0",
        payload=_payload(),
    )
    service = RegisteredWorkerEventReceiptService(
        receipt_session_factory,
        authority_locker=lambda *args, **kwargs: None,
        lease_observer=lambda *args, **kwargs: None,
    )
    receipt = RegisteredWorkerEventReceipt(
        id=uuid.uuid4(),
        **event.receipt_facts(),
        application_state="applied",
        ack_state="pending",
        applied_at=datetime.now(timezone.utc),
    )
    async with receipt_session_factory() as db:
        db.add(receipt)
        await db.commit()
        receipt_id = receipt.id

    class Redis:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, str]] = []

        async def xack(self, stream, group, message_id):
            self.calls.append((stream, group, message_id))
            return 1

    redis = Redis()
    await service.acknowledge_applied(redis, event)

    assert redis.calls == [
        ("vp:tasks:vision", "vision-workers", "1710000000000-4"),
        ("vp:events", "orchestrator", "1710000001000-0")
    ]
    async with receipt_session_factory() as db:
        stored = await db.get(RegisteredWorkerEventReceipt, receipt_id)
    assert stored is not None
    assert stored.source_task_ack_state == "acknowledged"
    assert stored.source_task_acknowledged_at is not None
    assert stored.ack_state == "acknowledged"
    assert stored.acknowledged_at is not None

    changed = parse_registered_worker_event(
        redis_stream=event.redis_stream,
        consumer_group=event.consumer_group,
        message_id=event.message_id,
        payload=_payload(worker_id="stale-worker"),
    )
    with pytest.raises(RegisteredWorkerEventError, match="mismatch"):
        await service.acknowledge_applied(redis, changed)
    assert len(redis.calls) == 2


@pytest.mark.asyncio
async def test_task_ack_failure_leaves_event_pending_and_retry_is_idempotent(
    receipt_session_factory,
) -> None:
    event = parse_registered_worker_event(
        redis_stream="vp:events",
        consumer_group="orchestrator",
        message_id="1710000001000-0",
        payload=_payload(),
    )
    receipt = RegisteredWorkerEventReceipt(
        id=uuid.uuid4(),
        **event.receipt_facts(),
        application_state="applied",
        ack_state="pending",
        applied_at=datetime.now(timezone.utc),
    )
    async with receipt_session_factory() as db:
        db.add(receipt)
        await db.commit()

    class Redis:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, str]] = []
            self.fail_event_once = True

        async def xack(self, stream, group, message_id):
            self.calls.append((stream, group, message_id))
            if stream == event.redis_stream and self.fail_event_once:
                self.fail_event_once = False
                raise RuntimeError("event XACK unavailable")
            return 1 if len(self.calls) <= 2 else 0

    redis = Redis()
    service = RegisteredWorkerEventReceiptService(receipt_session_factory)

    with pytest.raises(RuntimeError, match="event XACK unavailable"):
        await service.acknowledge_applied(redis, event)
    async with receipt_session_factory() as db:
        stored = await db.get(RegisteredWorkerEventReceipt, receipt.id)
    assert stored is not None
    assert stored.source_task_ack_state == "pending"
    assert stored.ack_state == "pending"

    await service.acknowledge_applied(redis, event)

    assert redis.calls == [
        ("vp:tasks:vision", "vision-workers", "1710000000000-4"),
        ("vp:events", "orchestrator", "1710000001000-0"),
        ("vp:tasks:vision", "vision-workers", "1710000000000-4"),
        ("vp:events", "orchestrator", "1710000001000-0"),
    ]
    async with receipt_session_factory() as db:
        stored = await db.get(RegisteredWorkerEventReceipt, receipt.id)
    assert stored is not None
    assert stored.source_task_ack_state == "acknowledged"
    assert stored.ack_state == "acknowledged"


@pytest.mark.asyncio
async def test_ack_commit_failure_is_recovered_by_receipt_reconciliation(
    receipt_session_factory,
    monkeypatch,
) -> None:
    event = parse_registered_worker_event(
        redis_stream="vp:events",
        consumer_group="orchestrator",
        message_id="1710000001000-0",
        payload=_payload(),
    )
    receipt = RegisteredWorkerEventReceipt(
        id=uuid.uuid4(),
        **event.receipt_facts(),
        application_state="applied",
        ack_state="pending",
        applied_at=datetime.now(timezone.utc),
    )
    async with receipt_session_factory() as db:
        db.add(receipt)
        await db.commit()

    class Redis:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, str]] = []

        async def xack(self, stream, group, message_id):
            self.calls.append((stream, group, message_id))
            return 1 if len(self.calls) <= 2 else 0

    redis = Redis()
    service = RegisteredWorkerEventReceiptService(receipt_session_factory)
    original_ack = service._acknowledge_locked_receipt
    attempts = 0

    async def fail_first_commit(db, redis_client, locked_receipt):
        nonlocal attempts
        attempts += 1
        await original_ack(db, redis_client, locked_receipt)
        if attempts == 1:
            raise RuntimeError("simulated database failure after XACK")

    monkeypatch.setattr(
        service,
        "_acknowledge_locked_receipt",
        fail_first_commit,
    )

    with pytest.raises(RuntimeError, match="after XACK"):
        await service.acknowledge_applied(redis, event)
    await service.reconcile_pending_acknowledgements(redis)

    assert redis.calls == [
        ("vp:tasks:vision", "vision-workers", "1710000000000-4"),
        ("vp:events", "orchestrator", "1710000001000-0"),
        ("vp:tasks:vision", "vision-workers", "1710000000000-4"),
        ("vp:events", "orchestrator", "1710000001000-0"),
    ]
    async with receipt_session_factory() as db:
        stored = await db.get(RegisteredWorkerEventReceipt, receipt.id)
    assert stored is not None
    assert stored.source_task_ack_state == "acknowledged"
    assert stored.ack_state == "acknowledged"


@pytest.mark.asyncio
async def test_dispatch_retry_reuses_atomic_redis_delivery_marker(
    receipt_session_factory,
    monkeypatch,
) -> None:
    event = parse_registered_worker_event(
        redis_stream="vp:events",
        consumer_group="orchestrator",
        message_id="1710000001000-0",
        payload=_payload(),
    )
    receipt = RegisteredWorkerEventReceipt(
        id=uuid.uuid4(),
        **event.receipt_facts(),
        application_state="applied",
        ack_state="pending",
        applied_at=datetime.now(timezone.utc),
    )
    dispatch = WorkerEventDispatch(
        receipt_id=receipt.id,
        dispatch_key=uuid.uuid4(),
        node_execution_id=uuid.uuid4(),
        redis_stream="vp:tasks:ffmpeg_go",
        payload_json={"job_id": str(event.job_id), "node_id": "next"},
        payload_sha256=canonical_redis_payload_sha256(
            {"job_id": str(event.job_id), "node_id": "next"}
        ),
        delivery_state="pending",
    )
    async with receipt_session_factory() as db:
        db.add_all([receipt, dispatch])
        await db.commit()

    class Redis:
        def __init__(self) -> None:
            self.markers: dict[str, str] = {}
            self.xadd_count = 0

        async def eval(self, script, key_count, stream, marker, *fields):
            assert "XADD" in script
            assert key_count == 2
            if marker not in self.markers:
                self.xadd_count += 1
                self.markers[marker] = "1710000002000-0"
            return self.markers[marker]

    service = RegisteredWorkerEventReceiptService(receipt_session_factory)
    original_mark = service._mark_dispatch_delivered
    attempts = 0

    async def fail_first_mark(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("simulated database failure after XADD")
        return await original_mark(*args, **kwargs)

    monkeypatch.setattr(
        service,
        "_mark_dispatch_delivered",
        fail_first_mark,
    )
    redis = Redis()

    with pytest.raises(RuntimeError, match="after XADD"):
        await service.deliver_pending_dispatches(redis, receipt.id)
    await service.deliver_pending_dispatches(redis, receipt.id)

    assert redis.xadd_count == 1
    async with receipt_session_factory() as db:
        stored = await db.get(WorkerEventDispatch, dispatch.id)
    assert stored is not None
    assert stored.delivery_state == "delivered"
    assert stored.redis_message_id == "1710000002000-0"
