from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.registered_worker_event_receipt import (
    RegisteredWorkerEventDelivery,
    RegisteredWorkerEventReceipt,
    WorkerTaskDispatch,
    WorkerTaskDeliveryAttestation,
)
from app.services.registered_worker_event_receipt import (
    RegisteredWorkerEventError,
    RegisteredWorkerEventReceiptService,
    canonical_redis_payload_sha256,
    parse_registered_worker_event,
    stage_worker_task_dispatch,
)


@pytest.fixture
async def receipt_session_factory(tmp_path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'registered-events.sqlite3'}",
        json_serializer=lambda value: json.dumps(value, default=str),
    )
    async with engine.begin() as connection:
        await connection.run_sync(Job.__table__.create)
        await connection.run_sync(NodeExecution.__table__.create)
        await connection.run_sync(
            WorkerTaskDeliveryAttestation.__table__.create
        )
        await connection.run_sync(
            RegisteredWorkerEventReceipt.__table__.create
        )
        await connection.run_sync(
            RegisteredWorkerEventDelivery.__table__.create
        )
        await connection.run_sync(WorkerTaskDispatch.__table__.create)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


def _payload(**overrides: str) -> dict[str, str]:
    source_task_payload = {
        "job_id": "00000000-0000-4000-8000-000000000201",
        "node_execution_id": "00000000-0000-4000-8000-000000000202",
        "node_id": "vision-1",
        "node_type": "vision",
        "config": "{}",
        "input_artifacts": "{}",
    }
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
        "task_payload_sha256": canonical_redis_payload_sha256(
            source_task_payload
        ),
        "task_dispatch_key": (
            "00000000-0000-4000-8000-000000000205"
        ),
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


async def _seed_applied_receipt(
    receipt_session_factory,
    event,
) -> tuple[
    RegisteredWorkerEventReceipt,
    RegisteredWorkerEventDelivery,
]:
    attestation = WorkerTaskDeliveryAttestation(
        id=uuid.uuid4(),
        redis_stream=event.source_task_stream,
        consumer_group=event.source_task_group,
        message_id=event.source_task_message_id,
        payload_sha256=event.source_task_payload_sha256,
        dispatch_key=event.source_task_dispatch_key,
        job_id=event.job_id,
        node_execution_id=event.node_execution_id,
        worker_registration_id=event.claim.worker_registration_id,
        worker_lease_epoch=event.claim.worker_lease_epoch,
        worker_id=event.claim.worker_id,
        worker_started_at=event.claim.started_at,
    )
    receipt = RegisteredWorkerEventReceipt(
        id=uuid.uuid4(),
        **event.receipt_facts(
            source_task_attestation_id=attestation.id,
        ),
        application_state="applied",
        ack_state="pending",
        applied_at=datetime.now(timezone.utc),
    )
    delivery = RegisteredWorkerEventDelivery(
        source_task_attestation_id=attestation.id,
        receipt_id=receipt.id,
        redis_stream=event.redis_stream,
        consumer_group=event.consumer_group,
        message_id=event.message_id,
        payload_sha256=event.payload_sha256,
        resolution_state="accepted",
        reason_code=None,
        ack_state="pending",
    )
    dispatch = WorkerTaskDispatch(
        dispatch_key=event.source_task_dispatch_key,
        job_id=event.job_id,
        node_execution_id=event.node_execution_id,
        redis_stream=event.source_task_stream,
        consumer_group=event.source_task_group,
        payload_sha256=event.source_task_payload_sha256,
        payload_json={
            "dispatch_key": str(event.source_task_dispatch_key),
        },
        delivery_state="delivered",
        delivery_attempted_at=datetime.now(timezone.utc),
        redis_message_id=event.source_task_message_id,
        delivered_at=datetime.now(timezone.utc),
    )
    async with receipt_session_factory() as db:
        db.add_all([dispatch, attestation, receipt, delivery])
        await db.commit()
    return receipt, delivery


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
    assert event.source_task_payload_sha256 == _payload()[
        "task_payload_sha256"
    ]
    assert event.source_task_dispatch_key == uuid.UUID(
        _payload()["task_dispatch_key"]
    )
    assert event.payload_sha256 == canonical_redis_payload_sha256(_payload())

    for missing in (
        "worker_registration_id",
        "worker_lease_epoch",
        "task_stream",
        "task_group",
        "task_message_id",
        "task_payload_sha256",
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

    registered_without_dispatch = _payload()
    del registered_without_dispatch["task_dispatch_key"]
    with pytest.raises(RegisteredWorkerEventError, match="dispatch"):
        parse_registered_worker_event(
            redis_stream="vp:events",
            consumer_group="orchestrator",
            message_id="1710000001000-0",
            payload=registered_without_dispatch,
        )


def test_dispatch_payload_must_carry_its_exact_database_key() -> None:
    dispatch = WorkerTaskDispatch(
        dispatch_key=uuid.uuid4(),
        job_id=uuid.uuid4(),
        node_execution_id=uuid.uuid4(),
        redis_stream="vp:tasks:vision",
        consumer_group="vision-workers",
        payload_json={"dispatch_key": str(uuid.uuid4())},
        payload_sha256=canonical_redis_payload_sha256(
            {"dispatch_key": str(uuid.uuid4())}
        ),
        delivery_state="pending",
    )
    dispatch.payload_sha256 = canonical_redis_payload_sha256(
        dispatch.payload_json
    )

    with pytest.raises(RegisteredWorkerEventError, match="dispatch key"):
        RegisteredWorkerEventReceiptService._validated_dispatch_payload(
            dispatch
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

    attestation_id = uuid.uuid4()

    async def observe(db, accepted_event):
        assert db.in_transaction()
        assert accepted_event == event
        order.append("delivery-observer")
        db.add(
            WorkerTaskDeliveryAttestation(
                id=attestation_id,
                redis_stream=event.source_task_stream,
                consumer_group=event.source_task_group,
                message_id=event.source_task_message_id,
                payload_sha256=event.source_task_payload_sha256,
                dispatch_key=event.source_task_dispatch_key,
                job_id=event.job_id,
                node_execution_id=event.node_execution_id,
                worker_registration_id=event.claim.worker_registration_id,
                worker_lease_epoch=event.claim.worker_lease_epoch,
                worker_id=event.claim.worker_id,
                worker_started_at=event.claim.started_at,
            )
        )
        await db.flush()
        return attestation_id

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
        delivery_observer=observe,
    )
    receipt_id = await service.accept_and_apply(event, apply)
    repeated_id = await service.accept_and_apply(event, apply)

    assert repeated_id == receipt_id
    assert apply_count == 1
    assert order == [
        "node-lock",
        "delivery-observer",
        "apply",
        "node-lock",
    ]
    async with receipt_session_factory() as db:
        receipt = await db.get(RegisteredWorkerEventReceipt, receipt_id)
    assert receipt is not None
    assert receipt.source_task_attestation_id == attestation_id
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
        return uuid.uuid4()

    async def apply(*args, **kwargs):
        return None

    service = RegisteredWorkerEventReceiptService(
        receipt_session_factory,
        authority_locker=lock_authority,
        delivery_observer=observe,
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
async def test_same_source_task_new_event_id_aliases_without_reapplying(
    receipt_session_factory,
) -> None:
    first = parse_registered_worker_event(
        redis_stream="vp:events",
        consumer_group="orchestrator",
        message_id="1710000001000-0",
        payload=_payload(),
    )
    duplicate = parse_registered_worker_event(
        redis_stream=first.redis_stream,
        consumer_group=first.consumer_group,
        message_id="1710000001001-0",
        payload=_payload(),
    )
    attestation_id = uuid.uuid4()
    apply_count = 0

    async def apply(*args, **kwargs):
        nonlocal apply_count
        apply_count += 1

    async def observe(db, accepted_event):
        db.add(
            WorkerTaskDeliveryAttestation(
                id=attestation_id,
                redis_stream=accepted_event.source_task_stream,
                consumer_group=accepted_event.source_task_group,
                message_id=accepted_event.source_task_message_id,
                payload_sha256=accepted_event.source_task_payload_sha256,
                dispatch_key=accepted_event.source_task_dispatch_key,
                job_id=accepted_event.job_id,
                node_execution_id=accepted_event.node_execution_id,
                worker_registration_id=(
                    accepted_event.claim.worker_registration_id
                ),
                worker_lease_epoch=accepted_event.claim.worker_lease_epoch,
                worker_id=accepted_event.claim.worker_id,
                worker_started_at=accepted_event.claim.started_at,
            )
        )
        await db.flush()
        return attestation_id

    async def lock_authority(*args, **kwargs):
        return _authority(first)

    service = RegisteredWorkerEventReceiptService(
        receipt_session_factory,
        authority_locker=lock_authority,
        delivery_observer=observe,
    )

    first_receipt_id = await service.accept_and_apply(first, apply)
    duplicate_receipt_id = await service.accept_and_apply(duplicate, apply)

    assert duplicate_receipt_id == first_receipt_id
    assert apply_count == 1
    async with receipt_session_factory() as db:
        aliases = list(
            (
                await db.execute(
                    select(RegisteredWorkerEventDelivery).order_by(
                        RegisteredWorkerEventDelivery.message_id
                    )
                )
            ).scalars()
        )
    assert [alias.message_id for alias in aliases] == [
        "1710000001000-0",
        "1710000001001-0",
    ]
    assert {alias.receipt_id for alias in aliases} == {first_receipt_id}


@pytest.mark.asyncio
async def test_same_source_task_mismatched_duplicate_is_quarantined(
    receipt_session_factory,
) -> None:
    first = parse_registered_worker_event(
        redis_stream="vp:events",
        consumer_group="orchestrator",
        message_id="1710000001000-0",
        payload=_payload(),
    )
    mismatch = parse_registered_worker_event(
        redis_stream=first.redis_stream,
        consumer_group=first.consumer_group,
        message_id="1710000001001-0",
        payload=_payload(
            output_artifact_id=(
                "00000000-0000-4000-8000-000000000299"
            )
        ),
    )
    attestation_id = uuid.uuid4()

    async def apply(*args, **kwargs):
        return None

    async def observe(db, accepted_event):
        db.add(
            WorkerTaskDeliveryAttestation(
                id=attestation_id,
                redis_stream=accepted_event.source_task_stream,
                consumer_group=accepted_event.source_task_group,
                message_id=accepted_event.source_task_message_id,
                payload_sha256=accepted_event.source_task_payload_sha256,
                dispatch_key=accepted_event.source_task_dispatch_key,
                job_id=accepted_event.job_id,
                node_execution_id=accepted_event.node_execution_id,
                worker_registration_id=(
                    accepted_event.claim.worker_registration_id
                ),
                worker_lease_epoch=accepted_event.claim.worker_lease_epoch,
                worker_id=accepted_event.claim.worker_id,
                worker_started_at=accepted_event.claim.started_at,
            )
        )
        await db.flush()
        return attestation_id

    async def lock_authority(*args, **kwargs):
        return _authority(first)

    service = RegisteredWorkerEventReceiptService(
        receipt_session_factory,
        authority_locker=lock_authority,
        delivery_observer=observe,
    )

    await service.accept_and_apply(first, apply)
    assert await service.accept_and_apply(mismatch, apply) is None

    async with receipt_session_factory() as db:
        aliases = list(
            (
                await db.execute(select(RegisteredWorkerEventDelivery))
            ).scalars()
        )
    assert len(aliases) == 2
    quarantined = next(
        alias
        for alias in aliases
        if alias.message_id == mismatch.message_id
    )
    assert quarantined.resolution_state == "quarantined"
    assert quarantined.reason_code == "event_payload_mismatch"
    assert quarantined.ack_state == "pending"

    class Redis:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, str]] = []

        async def xack(self, stream, group, message_id):
            self.calls.append((stream, group, message_id))
            return 1

    redis = Redis()
    await service.acknowledge_applied(redis, mismatch)
    assert redis.calls == [
        (
            mismatch.redis_stream,
            mismatch.consumer_group,
            mismatch.message_id,
        )
    ]
    assert await service.accept_and_apply(mismatch, apply) is None


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
    )
    receipt, delivery = await _seed_applied_receipt(
        receipt_session_factory,
        event,
    )
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
        attestation = await db.get(
            WorkerTaskDeliveryAttestation,
            stored.source_task_attestation_id if stored is not None else None,
        )
        dispatch = (
            await db.execute(
                select(WorkerTaskDispatch).where(
                    WorkerTaskDispatch.dispatch_key
                    == event.source_task_dispatch_key
                )
            )
        ).scalar_one()
    assert stored is not None
    assert attestation is not None
    assert attestation.ack_state == "acknowledged"
    assert attestation.acknowledged_at is not None
    assert dispatch.resolution_state == "acknowledged"
    assert dispatch.acknowledged_at is not None
    assert stored.source_task_ack_state == "acknowledged"
    assert stored.source_task_acknowledged_at is not None
    assert stored.ack_state == "acknowledged"
    assert stored.acknowledged_at is not None
    async with receipt_session_factory() as db:
        stored_delivery = await db.get(
            RegisteredWorkerEventDelivery,
            delivery.id,
        )
    assert stored_delivery is not None
    assert stored_delivery.ack_state == "acknowledged"

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
    receipt, _ = await _seed_applied_receipt(
        receipt_session_factory,
        event,
    )

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
        attestation = await db.get(
            WorkerTaskDeliveryAttestation,
            stored.source_task_attestation_id if stored is not None else None,
        )
    assert stored is not None
    assert attestation is not None
    assert attestation.ack_state == "pending"
    assert attestation.acknowledged_at is None
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
    receipt, _ = await _seed_applied_receipt(
        receipt_session_factory,
        event,
    )

    class Redis:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, str]] = []

        async def xack(self, stream, group, message_id):
            self.calls.append((stream, group, message_id))
            return 1 if len(self.calls) <= 2 else 0

    redis = Redis()
    service = RegisteredWorkerEventReceiptService(receipt_session_factory)
    original_ack = service._acknowledge_locked_delivery
    attempts = 0

    async def fail_first_commit(
        db,
        redis_client,
        locked_attestation,
        locked_emission,
        locked_receipt,
        locked_delivery,
    ):
        nonlocal attempts
        attempts += 1
        await original_ack(
            db,
            redis_client,
            locked_attestation,
            locked_emission,
            locked_receipt,
            locked_delivery,
        )
        if attempts == 1:
            raise RuntimeError("simulated database failure after XACK")

    monkeypatch.setattr(
        service,
        "_acknowledge_locked_delivery",
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
async def test_authorized_task_ack_is_recovered_without_worker_event(
    receipt_session_factory,
) -> None:
    event = parse_registered_worker_event(
        redis_stream="vp:events",
        consumer_group="orchestrator",
        message_id="1710000001000-0",
        payload=_payload(),
    )
    attestation = WorkerTaskDeliveryAttestation(
        redis_stream=event.source_task_stream,
        consumer_group=event.source_task_group,
        message_id=event.source_task_message_id,
        payload_sha256=event.source_task_payload_sha256,
        dispatch_key=event.source_task_dispatch_key,
        job_id=event.job_id,
        node_execution_id=event.node_execution_id,
        worker_registration_id=event.claim.worker_registration_id,
        worker_lease_epoch=event.claim.worker_lease_epoch,
        worker_id=event.claim.worker_id,
        worker_started_at=event.claim.started_at,
        ack_state="authorized",
    )
    dispatch = WorkerTaskDispatch(
        dispatch_key=event.source_task_dispatch_key,
        job_id=event.job_id,
        node_execution_id=event.node_execution_id,
        redis_stream=event.source_task_stream,
        consumer_group=event.source_task_group,
        payload_sha256=event.source_task_payload_sha256,
        payload_json={
            "dispatch_key": str(event.source_task_dispatch_key),
        },
        delivery_state="delivered",
        delivery_attempted_at=datetime.now(timezone.utc),
        redis_message_id=event.source_task_message_id,
        delivered_at=datetime.now(timezone.utc),
    )
    async with receipt_session_factory() as db:
        db.add_all([dispatch, attestation])
        await db.commit()
        attestation_id = attestation.id

    class Redis:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, str]] = []

        async def xack(self, stream, group, message_id):
            self.calls.append((stream, group, message_id))
            return 0

    redis = Redis()
    service = RegisteredWorkerEventReceiptService(receipt_session_factory)
    await service.reconcile_authorized_task_acknowledgements(redis)

    assert redis.calls == [
        (
            event.source_task_stream,
            event.source_task_group,
            event.source_task_message_id,
        )
    ]
    async with receipt_session_factory() as db:
        stored = await db.get(
            WorkerTaskDeliveryAttestation,
            attestation_id,
        )
        stored_dispatch = await db.get(WorkerTaskDispatch, dispatch.id)
    assert stored is not None
    assert stored.ack_state == "acknowledged"
    assert stored.acknowledged_at is not None
    assert stored_dispatch is not None
    assert stored_dispatch.resolution_state == "acknowledged"
    assert stored_dispatch.acknowledged_at is not None


@pytest.mark.asyncio
async def test_dispatch_attempt_is_durable_before_nonexpiring_redis_marker(
    receipt_session_factory,
) -> None:
    event = parse_registered_worker_event(
        redis_stream="vp:events",
        consumer_group="orchestrator",
        message_id="1710000001000-0",
        payload=_payload(),
    )
    receipt, _ = await _seed_applied_receipt(
        receipt_session_factory,
        event,
    )
    downstream_dispatch_key = uuid.uuid4()
    downstream_payload = {
        "job_id": str(event.job_id),
        "node_id": "next",
        "dispatch_key": str(downstream_dispatch_key),
    }
    dispatch = WorkerTaskDispatch(
        origin_receipt_id=receipt.id,
        dispatch_key=downstream_dispatch_key,
        job_id=event.job_id,
        node_execution_id=uuid.uuid4(),
        redis_stream="vp:tasks:ffmpeg_go",
        consumer_group="ffmpeg-go-workers",
        payload_json=downstream_payload,
        payload_sha256=canonical_redis_payload_sha256(downstream_payload),
        delivery_state="pending",
    )
    async with receipt_session_factory() as db:
        db.add(dispatch)
        await db.commit()

    class Redis:
        def __init__(self) -> None:
            self.markers: dict[str, str] = {}
            self.xadd_count = 0

        async def eval(
            self,
            script,
            key_count,
            stream,
            marker,
            *fields,
        ):
            assert "XADD" in script
            assert "'EX'" not in script
            assert key_count == 2
            async with receipt_session_factory() as db:
                attempting = await db.get(WorkerTaskDispatch, dispatch.id)
                assert attempting is not None
                assert attempting.delivery_state == "attempting"
                assert attempting.delivery_attempted_at is not None
            if marker not in self.markers:
                self.xadd_count += 1
                self.markers[marker] = "1710000002000-0"
            return self.markers[marker]

    service = RegisteredWorkerEventReceiptService(receipt_session_factory)
    redis = Redis()

    await service.deliver_pending_dispatches(redis, receipt.id)

    assert redis.xadd_count == 1
    async with receipt_session_factory() as db:
        stored = await db.get(WorkerTaskDispatch, dispatch.id)
    assert stored is not None
    assert stored.delivery_state == "delivered"
    assert stored.redis_message_id == "1710000002000-0"


@pytest.mark.asyncio
async def test_dispatch_redis_success_db_failure_recovers_existing_marker(
    receipt_session_factory,
    monkeypatch,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    async with receipt_session_factory() as db:
        async with db.begin():
            dispatch = await stage_worker_task_dispatch(
                db,
                origin_receipt_id=None,
                job_id=job_id,
                node_execution_id=node_execution_id,
                redis_stream="vp:tasks:vision",
                consumer_group="vision-workers",
                payload={
                    "job_id": str(job_id),
                    "node_execution_id": str(node_execution_id),
                },
            )
            dispatch_id = dispatch.id

    class Redis:
        def __init__(self) -> None:
            self.marker: str | None = None
            self.xadd_count = 0

        async def eval(self, script, key_count, stream, marker, *fields):
            if self.marker is None:
                self.xadd_count += 1
                self.marker = "1710000002001-0"
            return self.marker

        async def get(self, marker):
            return self.marker

    service = RegisteredWorkerEventReceiptService(
        receipt_session_factory,
        dispatch_attempt_recovery_seconds=0,
    )
    original_mark = service._mark_dispatch_delivered
    marks = 0

    async def fail_first_mark(*args, **kwargs):
        nonlocal marks
        marks += 1
        if marks == 1:
            raise RuntimeError("simulated database failure after XADD")
        return await original_mark(*args, **kwargs)

    monkeypatch.setattr(service, "_mark_dispatch_delivered", fail_first_mark)
    redis = Redis()
    with pytest.raises(RuntimeError, match="after XADD"):
        await service.reconcile_pending_dispatches(redis)

    await service.reconcile_pending_dispatches(redis)

    assert redis.xadd_count == 1
    async with receipt_session_factory() as db:
        stored = await db.get(WorkerTaskDispatch, dispatch_id)
    assert stored is not None
    assert stored.delivery_state == "delivered"
    assert stored.redis_message_id == "1710000002001-0"


@pytest.mark.asyncio
async def test_attempt_with_missing_marker_is_held_uncertain_without_xadd(
    receipt_session_factory,
) -> None:
    dispatch_key = uuid.uuid4()
    dispatch = WorkerTaskDispatch(
        dispatch_key=dispatch_key,
        job_id=uuid.uuid4(),
        node_execution_id=uuid.uuid4(),
        redis_stream="vp:tasks:vision",
        consumer_group="vision-workers",
        payload_json={"dispatch_key": str(dispatch_key)},
        payload_sha256=canonical_redis_payload_sha256(
            {"dispatch_key": str(dispatch_key)}
        ),
        delivery_state="attempting",
        delivery_attempted_at=datetime.now(timezone.utc),
    )
    async with receipt_session_factory() as db:
        db.add(dispatch)
        await db.commit()

    class Redis:
        eval_calls = 0

        async def eval(self, *args):
            self.eval_calls += 1

        async def get(self, marker):
            return None

    redis = Redis()
    service = RegisteredWorkerEventReceiptService(
        receipt_session_factory,
        dispatch_attempt_recovery_seconds=0,
    )
    await service.reconcile_pending_dispatches(redis)

    assert redis.eval_calls == 0
    async with receipt_session_factory() as db:
        stored = await db.get(WorkerTaskDispatch, dispatch.id)
    assert stored is not None
    assert stored.delivery_state == "uncertain"
    assert stored.delivery_error == "dispatch_marker_missing_after_attempt"


@pytest.mark.asyncio
async def test_concurrent_dispatch_reconcilers_mint_one_message(
    receipt_session_factory,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    async with receipt_session_factory() as db:
        async with db.begin():
            await stage_worker_task_dispatch(
                db,
                origin_receipt_id=None,
                job_id=job_id,
                node_execution_id=node_execution_id,
                redis_stream="vp:tasks:vision",
                consumer_group="vision-workers",
                payload={
                    "job_id": str(job_id),
                    "node_execution_id": str(node_execution_id),
                },
            )

    entered = asyncio.Event()
    release = asyncio.Event()

    class Redis:
        xadd_count = 0

        async def eval(self, script, key_count, stream, marker, *fields):
            self.xadd_count += 1
            entered.set()
            await release.wait()
            return "1710000002002-0"

        async def get(self, marker):
            return None

    redis = Redis()
    first = RegisteredWorkerEventReceiptService(receipt_session_factory)
    second = RegisteredWorkerEventReceiptService(receipt_session_factory)
    first_run = asyncio.create_task(first.reconcile_pending_dispatches(redis))
    await entered.wait()
    await second.reconcile_pending_dispatches(redis)
    release.set()
    await first_run

    assert redis.xadd_count == 1


@pytest.mark.asyncio
async def test_initial_dispatch_is_durable_and_independently_reconciled(
    receipt_session_factory,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    payload = {
        "job_id": str(job_id),
        "node_execution_id": str(node_execution_id),
        "node_id": "vision-1",
        "node_type": "vision",
        "config": "{}",
        "input_artifacts": "{}",
    }
    async with receipt_session_factory() as db:
        async with db.begin():
            dispatch = await stage_worker_task_dispatch(
                db,
                origin_receipt_id=None,
                job_id=job_id,
                node_execution_id=node_execution_id,
                redis_stream="vp:tasks:vision",
                consumer_group="vision-workers",
                payload=payload,
            )
            dispatch_id = dispatch.id
            dispatch_key = dispatch.dispatch_key

    class Redis:
        async def eval(
            self,
            script,
            key_count,
            stream,
            marker,
            *fields,
        ):
            assert "XADD" in script
            assert key_count == 2
            assert stream == "vp:tasks:vision"
            assert marker == f"vp:worker-task-dispatch:{dispatch_key}"
            delivered = dict(zip(fields[::2], fields[1::2], strict=True))
            assert delivered["dispatch_key"] == str(dispatch_key)
            return "1710000003000-0"

    service = RegisteredWorkerEventReceiptService(receipt_session_factory)
    await service.reconcile_pending_dispatches(Redis())

    async with receipt_session_factory() as db:
        stored = await db.get(WorkerTaskDispatch, dispatch_id)
    assert stored is not None
    assert stored.origin_receipt_id is None
    assert stored.delivery_state == "delivered"
    assert stored.redis_message_id == "1710000003000-0"
    assert stored.payload_sha256 == canonical_redis_payload_sha256(
        stored.payload_json
    )


@pytest.mark.asyncio
async def test_initial_dispatch_rejects_competing_unresolved_row(
    receipt_session_factory,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    payload = {
        "job_id": str(job_id),
        "node_execution_id": str(node_execution_id),
    }
    async with receipt_session_factory() as db:
        async with db.begin():
            first = await stage_worker_task_dispatch(
                db,
                origin_receipt_id=None,
                job_id=job_id,
                node_execution_id=node_execution_id,
                redis_stream="vp:tasks:vision",
                consumer_group="vision-workers",
                payload=payload,
            )

    async with receipt_session_factory() as db:
        async with db.begin():
            with pytest.raises(
                RegisteredWorkerEventError,
                match="unresolved initial dispatch",
            ):
                await stage_worker_task_dispatch(
                    db,
                    origin_receipt_id=None,
                    job_id=job_id,
                    node_execution_id=node_execution_id,
                    redis_stream="vp:tasks:vision",
                    consumer_group="vision-workers",
                    payload=payload,
                )

    async with receipt_session_factory() as db:
        async with db.begin():
            stored = await db.get(WorkerTaskDispatch, first.id)
            assert stored is not None
            stored.delivery_state = "cancelled"
            stored.resolution_state = "cancelled"
            stored.cancelled_at = datetime.now(timezone.utc)

    async with receipt_session_factory() as db:
        async with db.begin():
            replacement = await stage_worker_task_dispatch(
                db,
                origin_receipt_id=None,
                job_id=job_id,
                node_execution_id=node_execution_id,
                redis_stream="vp:tasks:vision",
                consumer_group="vision-workers",
                payload=payload,
            )
    assert replacement.id != first.id


async def _seed_cancelled_dispatch(
    receipt_session_factory,
    *,
    delivery_state: str,
    with_attestation: bool = False,
) -> tuple[uuid.UUID, uuid.UUID | None]:
    now = datetime.now(timezone.utc)
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    dispatch_key = uuid.uuid4()
    message_id = (
        "1710000004000-0" if delivery_state == "delivered" else None
    )
    payload = {
        "job_id": str(job_id),
        "node_execution_id": str(node_execution_id),
        "dispatch_key": str(dispatch_key),
    }
    attestation_id = uuid.uuid4() if with_attestation else None
    async with receipt_session_factory() as db:
        db.add(
            Job(
                id=job_id,
                pipeline_id=uuid.uuid4(),
                pipeline_snapshot={"nodes": [], "edges": []},
                status=JobStatus.CANCELLED,
            )
        )
        db.add(
            NodeExecution(
                id=node_execution_id,
                job_id=job_id,
                node_id="vision-1",
                node_type="vision",
                node_label="Vision",
                node_config={},
                status=NodeStatus.CANCELLED,
            )
        )
        dispatch = WorkerTaskDispatch(
            dispatch_key=dispatch_key,
            job_id=job_id,
            node_execution_id=node_execution_id,
            redis_stream="vp:tasks:vision",
            consumer_group="vision-workers",
            payload_json=payload,
            payload_sha256=canonical_redis_payload_sha256(payload),
            delivery_state=delivery_state,
            delivery_attempted_at=(
                now if delivery_state == "delivered" else None
            ),
            redis_message_id=message_id,
            delivered_at=now if delivery_state == "delivered" else None,
        )
        db.add(dispatch)
        if attestation_id is not None:
            db.add(
                WorkerTaskDeliveryAttestation(
                    id=attestation_id,
                    redis_stream=dispatch.redis_stream,
                    consumer_group=dispatch.consumer_group,
                    message_id=message_id,
                    payload_sha256=dispatch.payload_sha256,
                    dispatch_key=dispatch.dispatch_key,
                    job_id=job_id,
                    node_execution_id=node_execution_id,
                    worker_registration_id=uuid.uuid4(),
                    worker_lease_epoch=3,
                    worker_id="vision-worker@127:cancel",
                    worker_started_at=now,
                )
            )
        await db.commit()
        return dispatch.id, attestation_id


@pytest.mark.asyncio
async def test_cancel_before_dispatch_delivery_never_calls_redis(
    receipt_session_factory,
) -> None:
    dispatch_id, _ = await _seed_cancelled_dispatch(
        receipt_session_factory,
        delivery_state="pending",
    )

    class Redis:
        calls: list[str] = []

        async def eval(self, *args):
            self.calls.append("eval")

        async def xack(self, *args):
            self.calls.append("xack")

    redis = Redis()
    service = RegisteredWorkerEventReceiptService(receipt_session_factory)
    await service.reconcile_cancelled_dispatches(redis)
    await service.reconcile_pending_dispatches(redis)

    assert redis.calls == []
    async with receipt_session_factory() as db:
        stored = await db.get(WorkerTaskDispatch, dispatch_id)
    assert stored is not None
    assert stored.delivery_state == "cancelled"
    assert stored.resolution_state == "cancelled"
    assert stored.cancelled_at is not None


@pytest.mark.asyncio
async def test_cancel_after_delivery_exactly_acknowledges_unclaimed_dispatch(
    receipt_session_factory,
) -> None:
    dispatch_id, _ = await _seed_cancelled_dispatch(
        receipt_session_factory,
        delivery_state="delivered",
    )

    class Redis:
        calls: list[tuple[str, str, str]] = []

        async def xack(self, stream, group, message_id):
            self.calls.append((stream, group, message_id))
            return 1

    redis = Redis()
    service = RegisteredWorkerEventReceiptService(receipt_session_factory)
    await service.reconcile_cancelled_dispatches(redis)

    assert redis.calls == [
        ("vp:tasks:vision", "vision-workers", "1710000004000-0")
    ]
    async with receipt_session_factory() as db:
        stored = await db.get(WorkerTaskDispatch, dispatch_id)
    assert stored is not None
    assert stored.resolution_state == "acknowledged"
    assert stored.acknowledged_at is not None


@pytest.mark.asyncio
async def test_cancelled_claim_without_event_acknowledges_attestation(
    receipt_session_factory,
) -> None:
    dispatch_id, attestation_id = await _seed_cancelled_dispatch(
        receipt_session_factory,
        delivery_state="delivered",
        with_attestation=True,
    )

    class Redis:
        async def xack(self, stream, group, message_id):
            return 1

    service = RegisteredWorkerEventReceiptService(receipt_session_factory)
    await service.reconcile_cancelled_dispatches(Redis())

    async with receipt_session_factory() as db:
        dispatch = await db.get(WorkerTaskDispatch, dispatch_id)
        attestation = await db.get(
            WorkerTaskDeliveryAttestation,
            attestation_id,
        )
    assert dispatch is not None
    assert dispatch.resolution_state == "acknowledged"
    assert attestation is not None
    assert attestation.ack_state == "acknowledged"
    assert attestation.acknowledged_at is not None


@pytest.mark.asyncio
async def test_cancel_xack_db_failure_recovers_authorized_dispatch(
    receipt_session_factory,
    monkeypatch,
) -> None:
    dispatch_id, _ = await _seed_cancelled_dispatch(
        receipt_session_factory,
        delivery_state="delivered",
    )

    class Redis:
        calls = 0

        async def xack(self, stream, group, message_id):
            self.calls += 1
            return 1 if self.calls == 1 else 0

    service = RegisteredWorkerEventReceiptService(receipt_session_factory)
    original_mark = service._mark_cancelled_dispatch_acknowledged
    marks = 0

    async def fail_first_mark(*args, **kwargs):
        nonlocal marks
        marks += 1
        if marks == 1:
            raise RuntimeError("simulated cancellation DB failure")
        return await original_mark(*args, **kwargs)

    monkeypatch.setattr(
        service,
        "_mark_cancelled_dispatch_acknowledged",
        fail_first_mark,
    )
    redis = Redis()
    with pytest.raises(RuntimeError, match="cancellation DB failure"):
        await service.reconcile_cancelled_dispatches(redis)

    async with receipt_session_factory() as db:
        authorized = await db.get(WorkerTaskDispatch, dispatch_id)
    assert authorized is not None
    assert authorized.resolution_state == "cancel_authorized"

    await service.reconcile_cancelled_dispatches(redis)

    assert redis.calls == 2
    async with receipt_session_factory() as db:
        acknowledged = await db.get(WorkerTaskDispatch, dispatch_id)
    assert acknowledged is not None
    assert acknowledged.resolution_state == "acknowledged"
