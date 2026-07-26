from __future__ import annotations

import asyncio
import hashlib
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.services.worker_admission import WorkerAdmissionError
from app.services.worker_registration import WorkerLease, WorkerRegistrationError
from worker import main as worker_main


def execution_claim(
    job_id: uuid.UUID,
    node_execution_id: uuid.UUID,
) -> worker_main.NodeExecutionClaim:
    return worker_main.NodeExecutionClaim(
        job_id=job_id,
        node_execution_id=node_execution_id,
        worker_id="test-worker@localhost:1",
        started_at=datetime(2026, 7, 22, 12, 0, 0),
    )


def registered_execution_claim(
    job_id: uuid.UUID,
    node_execution_id: uuid.UUID,
) -> worker_main.NodeExecutionClaim:
    return worker_main.NodeExecutionClaim(
        job_id=job_id,
        node_execution_id=node_execution_id,
        worker_id="test-worker@localhost:1",
        started_at=datetime(2026, 7, 22, 12, 0, 0, tzinfo=timezone.utc),
        worker_registration_id=uuid.uuid4(),
        worker_lease_epoch=9,
    )


def worker_lease_for(claim: worker_main.NodeExecutionClaim) -> WorkerLease:
    assert claim.worker_registration_id is not None
    assert claim.worker_lease_epoch is not None
    return WorkerLease(
        registration_id=claim.worker_registration_id,
        grant_id=uuid.uuid4(),
        service_name="vp-ffmpeg-worker-gpu-swarm",
        worker_instance_id=uuid.uuid4(),
        worker_slot=1,
        redis_consumer_id=claim.worker_id,
        lease_epoch=claim.worker_lease_epoch,
        lease_secret="lease-secret",
        lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=180),
    )


def test_worker_database_is_not_configured_at_import() -> None:
    assert worker_main.engine_db is None
    assert worker_main.worker_session is None


def test_node_execution_started_at_orm_type_is_timezone_aware() -> None:
    assert worker_main.NodeExecution.__table__.c.started_at.type.timezone is True


@pytest.mark.asyncio
async def test_worker_events_include_canonical_execution_claim(monkeypatch) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    output_artifact_id = uuid.uuid4()
    claim = execution_claim(job_id, node_execution_id)
    events: list[tuple[str, dict]] = []
    close_calls = 0

    class FakeRedis:
        async def xadd(self, stream: str, payload: dict) -> None:
            events.append((stream, dict(payload)))

        async def aclose(self) -> None:
            nonlocal close_calls
            close_calls += 1

    monkeypatch.setattr(worker_main, "_redis", lambda: FakeRedis())

    await worker_main._report_success(
        str(job_id),
        str(node_execution_id),
        str(output_artifact_id),
        claim,
    )
    await worker_main._report_failure(
        str(job_id),
        str(node_execution_id),
        "render failed",
        claim,
    )

    expected_claim = {
        "worker_id": claim.worker_id,
        "started_at": "2026-07-22T12:00:00+00:00",
    }
    assert events == [
        (
            worker_main.EVENT_STREAM,
            {
                "event": "node_completed",
                "job_id": str(job_id),
                "node_execution_id": str(node_execution_id),
                "output_artifact_id": str(output_artifact_id),
                **expected_claim,
            },
        ),
        (
            worker_main.EVENT_STREAM,
            {
                "event": "node_failed",
                "job_id": str(job_id),
                "node_execution_id": str(node_execution_id),
                "error": "render failed",
                **expected_claim,
            },
        ),
    ]
    assert close_calls == 2


@pytest.mark.asyncio
async def test_claim_persists_worker_registration_id_and_epoch(monkeypatch) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    registration_id = uuid.uuid4()
    node = SimpleNamespace(
        id=node_execution_id,
        status=worker_main.NodeStatus.QUEUED,
        started_at=None,
        worker_id=None,
        worker_registration_id=None,
        worker_lease_epoch=None,
    )
    lease = WorkerLease(
        registration_id=registration_id,
        grant_id=uuid.uuid4(),
        service_name="vp-vision-worker-swarm",
        worker_instance_id=uuid.uuid4(),
        worker_slot=1,
        redis_consumer_id="vision-worker@127:1:instance",
        lease_epoch=12,
        lease_secret="lease-secret",
        lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=180),
    )
    dispatch_key = uuid.uuid4()
    payload_sha256 = hashlib.sha256(b"canonical-task").hexdigest()
    delivery = worker_main.WorkerTaskDelivery(
        redis_stream="vp:tasks:vision",
        consumer_group="vision-workers",
        message_id="1710000000000-4",
        payload_sha256=payload_sha256,
        dispatch_key=dispatch_key,
    )
    attestation_id = uuid.uuid4()
    claims: list[dict[str, object]] = []

    class Transaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def begin(self):
            return Transaction()

        async def flush(self):
            return None

    async def lock_authority(_db, locked_job_id, *, node_execution_id):
        assert locked_job_id == job_id
        return SimpleNamespace(
            job=SimpleNamespace(id=job_id, status=worker_main.JobStatus.RUNNING),
            node=node,
            channel=None,
            task=None,
            schedule=SimpleNamespace(state="OPEN", guarded_job_id=job_id),
        )

    claimed_at = datetime(2026, 7, 26, 12, 0, tzinfo=timezone.utc)

    async def claim_registered(_db, **facts):
        assert node.status == worker_main.NodeStatus.QUEUED
        assert node.worker_registration_id is None
        assert facts == {
            "job_id": job_id,
            "node_execution_id": node_execution_id,
            "registration_id": registration_id,
            "lease_epoch": 12,
            "worker_id": lease.redis_consumer_id,
            "redis_stream": delivery.redis_stream,
            "consumer_group": delivery.consumer_group,
            "message_id": delivery.message_id,
            "payload_sha256": payload_sha256,
            "dispatch_key": dispatch_key,
        }
        claims.append(facts)
        return worker_main.NodeExecutionClaim(
            job_id=job_id,
            node_execution_id=node_execution_id,
            worker_id=lease.redis_consumer_id,
            started_at=claimed_at,
            worker_registration_id=registration_id,
            worker_lease_epoch=12,
        ), attestation_id

    monkeypatch.setattr(worker_main, "lock_job_execution_authority", lock_authority)
    monkeypatch.setattr(
        worker_main,
        "claim_registered_worker_node",
        claim_registered,
        raising=False,
    )

    delivery_token = worker_main._current_task_delivery.set(delivery)
    try:
        claim = await worker_main._claim_node_execution(
            str(job_id),
            str(node_execution_id),
            worker_lease=lease,
            session_factory=lambda: Session(),
        )
    finally:
        worker_main._current_task_delivery.reset(delivery_token)

    assert claim is not None
    assert claim.started_at == claimed_at
    assert claim.worker_registration_id == registration_id
    assert claim.worker_lease_epoch == 12
    assert node.status == worker_main.NodeStatus.QUEUED
    assert node.worker_registration_id is None
    assert node.worker_lease_epoch is None
    assert claims
    assert delivery.attestation_id == attestation_id


@pytest.mark.asyncio
async def test_event_xadd_occurs_while_exact_claim_and_lease_transaction_are_held(
    monkeypatch,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    claim = registered_execution_claim(job_id, node_execution_id)
    transaction_active = False
    lease_checked = False
    events: list[dict] = []

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
        async def xadd(self, stream, payload):
            assert transaction_active
            assert lease_checked
            events.append(dict(payload))

        async def aclose(self):
            return None

    async def lock_authority(_db, locked_job_id, *, node_execution_id):
        return SimpleNamespace(
            job=SimpleNamespace(id=job_id, status=worker_main.JobStatus.RUNNING),
            node=SimpleNamespace(
                id=node_execution_id,
                status=worker_main.NodeStatus.RUNNING,
                worker_id=claim.worker_id,
                started_at=claim.started_at,
                worker_registration_id=claim.worker_registration_id,
                worker_lease_epoch=claim.worker_lease_epoch,
            ),
            channel=None,
            task=None,
            schedule=SimpleNamespace(state="OPEN", guarded_job_id=job_id),
        )

    async def require_lease(_db, checked_claim):
        nonlocal lease_checked
        assert transaction_active
        assert checked_claim == claim
        lease_checked = True

    monkeypatch.setattr(worker_main, "get_worker_session", lambda: lambda: Session())
    monkeypatch.setattr(worker_main, "lock_job_execution_authority", lock_authority)
    monkeypatch.setattr(
        worker_main,
        "require_worker_registration_lease",
        require_lease,
        raising=False,
    )
    monkeypatch.setattr(
        worker_main,
        "_current_task_delivery",
        SimpleNamespace(
            get=lambda: worker_main.WorkerTaskDelivery(
                redis_stream="vp:tasks:vision",
                consumer_group="vision-workers",
                message_id="1710000000000-4",
                payload_sha256=hashlib.sha256(b"task").hexdigest(),
                dispatch_key=uuid.uuid4(),
            )
        ),
        raising=False,
    )
    monkeypatch.setattr(worker_main, "_redis", lambda: Redis())

    await worker_main._report_success(
        str(job_id),
        str(node_execution_id),
        str(uuid.uuid4()),
        claim,
    )

    assert len(events) == 1
    assert events[0]["worker_registration_id"] == str(
        claim.worker_registration_id
    )
    assert events[0]["worker_lease_epoch"] == str(claim.worker_lease_epoch)
    assert events[0]["task_stream"] == "vp:tasks:vision"
    assert events[0]["task_group"] == "vision-workers"
    assert events[0]["task_message_id"] == "1710000000000-4"
    assert events[0]["task_payload_sha256"] == hashlib.sha256(
        b"task"
    ).hexdigest()
    assert uuid.UUID(events[0]["task_dispatch_key"])


@pytest.mark.asyncio
async def test_final_xack_uses_exact_claim_fence(monkeypatch) -> None:
    claim = registered_execution_claim(uuid.uuid4(), uuid.uuid4())
    acknowledgements: list[tuple[object, str, object]] = []

    class Redis:
        async def xack(self, *args):
            raise AssertionError("raw XACK must not bypass the database fence")

    async def process(_data, *, worker_lease=None):
        return claim

    async def heartbeat(_redis, _message_id):
        await asyncio.Event().wait()

    async def ack(redis, message_id, handled_claim):
        acknowledgements.append((redis, message_id, handled_claim))

    monkeypatch.setattr(worker_main, "process_task", process)
    monkeypatch.setattr(worker_main, "_heartbeat_message", heartbeat)
    monkeypatch.setattr(
        worker_main,
        "_ack_message_for_claim",
        ack,
        raising=False,
    )

    redis = Redis()
    await worker_main._process_message(
        redis,
        "1-0",
        {
            "job_id": str(claim.job_id),
            "node_execution_id": str(claim.node_execution_id),
            "dispatch_key": str(uuid.uuid4()),
        },
        worker_lease=worker_lease_for(claim),
    )

    assert acknowledgements == [(redis, "1-0", claim)]


@pytest.mark.asyncio
async def test_final_xack_accepts_exact_claim_after_orchestrator_finalizes_node(
    monkeypatch,
) -> None:
    claim = registered_execution_claim(uuid.uuid4(), uuid.uuid4())
    lease_checked = False
    ack_authorized = False
    ack_authority_checked = False
    attestation_marked = False
    acknowledgements: list[str] = []
    delivery = worker_main.WorkerTaskDelivery(
        redis_stream=worker_main.TASK_STREAM,
        consumer_group=worker_main.CONSUMER_GROUP,
        message_id="1-0",
        payload_sha256="a" * 64,
        dispatch_key=uuid.uuid4(),
        attestation_id=uuid.uuid4(),
    )

    class Transaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
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
            acknowledgements.append(message_id)
            return 1

    async def lock_authority(_db, job_id, *, node_execution_id):
        return SimpleNamespace(
            channel=None,
            schedule=SimpleNamespace(state="CLOSED", guarded_job_id=None),
            task=None,
            job=SimpleNamespace(
                id=claim.job_id,
                status=worker_main.JobStatus.SUCCEEDED,
            ),
            node=SimpleNamespace(
                id=claim.node_execution_id,
                status=worker_main.NodeStatus.SUCCEEDED,
                worker_id=claim.worker_id,
                started_at=claim.started_at,
                worker_registration_id=claim.worker_registration_id,
                worker_lease_epoch=claim.worker_lease_epoch,
            ),
        )

    async def require_lease(_db, checked_claim):
        nonlocal lease_checked
        assert checked_claim == claim
        lease_checked = True

    async def mark_ack(_db, checked_claim, **exact_delivery):
        nonlocal attestation_marked
        assert checked_claim == claim
        assert exact_delivery["attestation_id"] == delivery.attestation_id
        attestation_marked = True

    async def authorize_ack(_db, checked_claim, *, attestation_id):
        nonlocal ack_authorized
        assert checked_claim == claim
        assert attestation_id == delivery.attestation_id
        ack_authorized = True

    async def require_ack_authority(_db, checked_claim, **exact_delivery):
        nonlocal ack_authority_checked
        assert checked_claim == claim
        assert ack_authorized
        assert exact_delivery["dispatch_key"] == delivery.dispatch_key
        ack_authority_checked = True

    monkeypatch.setattr(worker_main, "get_worker_session", lambda: lambda: Session())
    monkeypatch.setattr(worker_main, "lock_job_execution_authority", lock_authority)
    monkeypatch.setattr(
        worker_main,
        "require_worker_registration_lease",
        require_lease,
    )
    monkeypatch.setattr(
        worker_main,
        "acknowledge_worker_task_delivery",
        mark_ack,
        raising=False,
    )
    monkeypatch.setattr(
        worker_main,
        "authorize_worker_task_ack",
        authorize_ack,
        raising=False,
    )
    monkeypatch.setattr(
        worker_main,
        "require_worker_task_ack_receipt",
        require_ack_authority,
        raising=False,
    )

    token = worker_main._current_task_delivery.set(delivery)
    try:
        await worker_main._ack_message_for_claim(Redis(), "1-0", claim)
    finally:
        worker_main._current_task_delivery.reset(token)

    assert lease_checked
    assert ack_authorized
    assert ack_authority_checked
    assert attestation_marked
    assert acknowledgements == ["1-0"]


@pytest.mark.asyncio
async def test_final_xack_uses_exact_applied_receipt_after_lease_loss(
    monkeypatch,
) -> None:
    claim = registered_execution_claim(uuid.uuid4(), uuid.uuid4())
    receipt_checks: list[dict[str, str]] = []
    marked: list[dict[str, object]] = []
    acknowledgements: list[tuple[str, str, str]] = []
    delivery = worker_main.WorkerTaskDelivery(
        redis_stream=worker_main.TASK_STREAM,
        consumer_group=worker_main.CONSUMER_GROUP,
        message_id="1710000000000-4",
        payload_sha256="b" * 64,
        dispatch_key=uuid.uuid4(),
        attestation_id=uuid.uuid4(),
    )

    class Transaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
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
            acknowledgements.append((stream, group, message_id))
            return 1

    async def lock_authority(_db, _job_id, *, node_execution_id):
        return SimpleNamespace(
            channel=None,
            schedule=SimpleNamespace(state="CLOSED", guarded_job_id=None),
            task=None,
            job=SimpleNamespace(
                id=claim.job_id,
                status=worker_main.JobStatus.SUCCEEDED,
            ),
            node=SimpleNamespace(
                id=node_execution_id,
                status=worker_main.NodeStatus.SUCCEEDED,
                worker_id=claim.worker_id,
                started_at=claim.started_at,
                worker_registration_id=claim.worker_registration_id,
                worker_lease_epoch=claim.worker_lease_epoch,
            ),
        )

    async def reject_expired_lease(_db, _claim):
        raise worker_main.JobExecutionAuthorityBlocked("lease expired")

    async def require_receipt(_db, checked_claim, **delivery):
        assert checked_claim == claim
        receipt_checks.append(delivery)

    async def mark_ack(_db, checked_claim, **exact_delivery):
        assert checked_claim == claim
        marked.append(exact_delivery)

    monkeypatch.setattr(worker_main, "get_worker_session", lambda: lambda: Session())
    monkeypatch.setattr(worker_main, "lock_job_execution_authority", lock_authority)
    monkeypatch.setattr(
        worker_main,
        "require_worker_registration_lease",
        reject_expired_lease,
    )
    monkeypatch.setattr(
        worker_main,
        "require_worker_task_ack_receipt",
        require_receipt,
        raising=False,
    )
    monkeypatch.setattr(
        worker_main,
        "acknowledge_worker_task_delivery",
        mark_ack,
        raising=False,
    )

    token = worker_main._current_task_delivery.set(delivery)
    try:
        await worker_main._ack_message_for_claim(
            Redis(),
            "1710000000000-4",
            claim,
        )
    finally:
        worker_main._current_task_delivery.reset(token)

    assert receipt_checks == [
        {
            "redis_stream": worker_main.TASK_STREAM,
            "consumer_group": worker_main.CONSUMER_GROUP,
            "message_id": "1710000000000-4",
            "payload_sha256": delivery.payload_sha256,
            "dispatch_key": delivery.dispatch_key,
        }
    ]
    assert marked == [
        {
            "attestation_id": delivery.attestation_id,
            "redis_stream": worker_main.TASK_STREAM,
            "consumer_group": worker_main.CONSUMER_GROUP,
            "message_id": "1710000000000-4",
            "payload_sha256": delivery.payload_sha256,
            "dispatch_key": delivery.dispatch_key,
        }
    ]
    assert acknowledgements == [
        (
            worker_main.TASK_STREAM,
            worker_main.CONSUMER_GROUP,
            "1710000000000-4",
        )
    ]


@pytest.mark.asyncio
async def test_final_xack_after_lease_loss_without_receipt_stays_pending(
    monkeypatch,
) -> None:
    claim = registered_execution_claim(uuid.uuid4(), uuid.uuid4())
    receipt_checks = 0
    acknowledgements: list[str] = []
    delivery = worker_main.WorkerTaskDelivery(
        redis_stream=worker_main.TASK_STREAM,
        consumer_group=worker_main.CONSUMER_GROUP,
        message_id="1710000000000-4",
        payload_sha256="c" * 64,
        dispatch_key=uuid.uuid4(),
        attestation_id=uuid.uuid4(),
    )

    class Transaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def begin(self):
            return Transaction()

    class Redis:
        async def xack(self, _stream, _group, message_id):
            acknowledgements.append(message_id)

    async def lock_authority(_db, _job_id, *, node_execution_id):
        return SimpleNamespace(
            node=SimpleNamespace(
                id=node_execution_id,
                worker_id=claim.worker_id,
                started_at=claim.started_at,
                worker_registration_id=claim.worker_registration_id,
                worker_lease_epoch=claim.worker_lease_epoch,
            ),
            job=SimpleNamespace(id=claim.job_id),
        )

    async def reject_expired_lease(_db, _claim):
        raise worker_main.JobExecutionAuthorityBlocked("lease expired")

    async def reject_missing_receipt(_db, _claim, **_delivery):
        nonlocal receipt_checks
        receipt_checks += 1
        raise worker_main.JobExecutionAuthorityBlocked("receipt missing")

    monkeypatch.setattr(worker_main, "get_worker_session", lambda: lambda: Session())
    monkeypatch.setattr(worker_main, "lock_job_execution_authority", lock_authority)
    monkeypatch.setattr(
        worker_main,
        "require_worker_registration_lease",
        reject_expired_lease,
    )
    monkeypatch.setattr(
        worker_main,
        "require_worker_task_ack_receipt",
        reject_missing_receipt,
        raising=False,
    )

    token = worker_main._current_task_delivery.set(delivery)
    try:
        with pytest.raises(
            worker_main.JobExecutionAuthorityBlocked,
            match="receipt missing",
        ):
            await worker_main._ack_message_for_claim(
                Redis(),
                "1710000000000-4",
                claim,
            )
    finally:
        worker_main._current_task_delivery.reset(token)

    assert receipt_checks == 1
    assert acknowledgements == []


@pytest.mark.asyncio
async def test_registration_loss_cancels_consumer_and_propagates_stable_error() -> None:
    consumer_cancelled = asyncio.Event()
    loss = WorkerRegistrationError("lease_fenced")

    class Registration:
        async def wait_lost(self):
            await asyncio.sleep(0)
            return loss

    async def consume():
        try:
            await asyncio.Event().wait()
        finally:
            consumer_cancelled.set()

    with pytest.raises(WorkerRegistrationError) as exc:
        await worker_main._run_until_registration_loss(
            Registration(),
            consume(),
        )

    assert exc.value.code == "lease_fenced"
    assert consumer_cancelled.is_set()


@pytest.mark.asyncio
async def test_registered_affinity_defer_leaves_exact_delivery_pending(
    monkeypatch,
) -> None:
    claim = registered_execution_claim(uuid.uuid4(), uuid.uuid4())
    lease = worker_lease_for(claim)
    redis_calls: list[str] = []

    class Redis:
        async def xadd(self, stream, payload):
            redis_calls.append("xadd")

        async def xack(self, stream, group, message_id):
            redis_calls.append("xack")

    monkeypatch.setattr(worker_main, "WORKER_HOST", "127")
    now = int(worker_main.time.time())
    payload = {
        "job_id": str(claim.job_id),
        "node_execution_id": str(claim.node_execution_id),
        "dispatch_key": str(uuid.uuid4()),
        "preferred_hosts": json.dumps(["150"]),
        "affinity_enqueued_at": str(now),
        "affinity_bounces": "0",
    }
    original = dict(payload)
    deferred = await worker_main._maybe_defer_for_affinity(
        Redis(),
        "1-0",
        payload,
        worker_lease=lease,
    )

    assert deferred is True
    assert redis_calls == []
    assert payload == original
    assert worker_main._canonical_task_payload_sha256(payload) == (
        worker_main._canonical_task_payload_sha256(original)
    )

    monkeypatch.setattr(
        worker_main.time,
        "time",
        lambda: now + worker_main.AFFINITY_WAIT_SECONDS,
    )
    assert await worker_main._maybe_defer_for_affinity(
        Redis(),
        "1-0",
        payload,
        worker_lease=lease,
    ) is False
    assert redis_calls == []


@pytest.mark.asyncio
async def test_registered_dispatch_is_validated_before_affinity(
    monkeypatch,
) -> None:
    claim = registered_execution_claim(uuid.uuid4(), uuid.uuid4())
    lease = worker_lease_for(claim)
    monkeypatch.setattr(worker_main, "WORKER_HOST", "127")

    with pytest.raises(
        worker_main.JobExecutionAuthorityBlocked,
        match="dispatch key is invalid",
    ):
        await worker_main._process_message(
            object(),
            "1-0",
            {
                "job_id": str(claim.job_id),
                "node_execution_id": str(claim.node_execution_id),
                "dispatch_key": "not-a-uuid",
                "preferred_hosts": json.dumps(["150"]),
                "affinity_enqueued_at": str(int(worker_main.time.time())),
            },
            worker_lease=lease,
        )


@pytest.mark.asyncio
async def test_preferred_registered_worker_reclaims_exact_pending_affinity_message(
    monkeypatch,
) -> None:
    message_id = "1710000000000-31"
    now = int(worker_main.time.time())
    payload = {
        "job_id": str(uuid.uuid4()),
        "node_execution_id": str(uuid.uuid4()),
        "node_id": "preferred-reclaim",
        "node_type": "vision",
        "config": "{}",
        "input_artifacts": "{}",
        "dispatch_key": str(uuid.uuid4()),
        "preferred_hosts": json.dumps(["worker-127"]),
        "affinity_enqueued_at": str(now),
        "affinity_bounces": "0",
    }
    processed: list[tuple[str, dict]] = []

    class Redis:
        async def xpending_range(self, stream, group, start, end, count):
            assert (stream, group, start, end, count) == (
                worker_main.TASK_STREAM,
                worker_main.CONSUMER_GROUP,
                "-",
                "+",
                50,
            )
            return [
                {
                    "message_id": message_id,
                    "consumer": "vision-worker@worker-150:other",
                    "time_since_delivered": 1000,
                    "times_delivered": 1,
                }
            ]

        async def xrange(self, stream, min, max, count):
            assert (stream, min, max, count) == (
                worker_main.TASK_STREAM,
                message_id,
                message_id,
                1,
            )
            return [(message_id, payload)]

        async def xclaim(
            self,
            stream,
            group,
            consumer,
            min_idle_time,
            message_ids,
        ):
            assert (stream, group, consumer, message_ids) == (
                worker_main.TASK_STREAM,
                worker_main.CONSUMER_GROUP,
                worker_main.WORKER_ID,
                [message_id],
            )
            assert min_idle_time <= 1000
            return [(message_id, payload)]

    lease = SimpleNamespace()

    async def process(redis, claimed_id, claimed_payload, *, worker_lease):
        assert worker_lease is lease
        processed.append((claimed_id, claimed_payload))

    monkeypatch.setattr(worker_main, "WORKER_HOST", "worker-127")
    monkeypatch.setattr(worker_main, "_process_message", process)

    await worker_main._reclaim_preferred_pending(
        Redis(),
        worker_lease=lease,
    )

    assert processed == [(message_id, payload)]


@pytest.mark.asyncio
async def test_nonpreferred_registered_worker_does_not_reclaim_affinity_message(
    monkeypatch,
) -> None:
    message_id = "1710000000000-32"
    payload = {
        "preferred_hosts": json.dumps(["worker-127"]),
        "affinity_enqueued_at": str(int(worker_main.time.time())),
    }
    claimed: list[str] = []

    class Redis:
        async def xpending_range(self, *args, **kwargs):
            return [{"message_id": message_id, "time_since_delivered": 1000}]

        async def xrange(self, *args, **kwargs):
            return [(message_id, payload)]

        async def xclaim(self, *args, **kwargs):
            claimed.append(message_id)
            return [(message_id, payload)]

    async def reject_process(*args, **kwargs):
        raise AssertionError("non-preferred worker must not process the message")

    monkeypatch.setattr(worker_main, "WORKER_HOST", "worker-150")
    monkeypatch.setattr(worker_main, "_process_message", reject_process)

    await worker_main._reclaim_preferred_pending(
        Redis(),
        worker_lease=SimpleNamespace(),
    )

    assert claimed == []


@pytest.mark.asyncio
async def test_consumer_cancellation_cancels_inflight_messages_without_xack(
    monkeypatch,
) -> None:
    claim = registered_execution_claim(uuid.uuid4(), uuid.uuid4())
    lease = worker_lease_for(claim)
    message_started = asyncio.Event()
    message_cancelled = asyncio.Event()
    reads = 0

    class Registration:
        def __init__(self, current_lease):
            self.lease = current_lease
            self.redis_consumer_id = current_lease.redis_consumer_id
            self.redis_stream = "vp:tasks:admitted-vision"
            self.redis_group = "admitted-vision-workers"
            self.worker_host = "127"

        async def heartbeat_now(self, *, minimum_margin_seconds: float = 0):
            return self.lease

    class Redis:
        async def xgroup_create(self, stream, group, *args, **kwargs):
            assert stream == "vp:tasks:admitted-vision"
            assert group == "admitted-vision-workers"
            return None

        async def xreadgroup(self, group, consumer, streams, **kwargs):
            nonlocal reads
            assert group == "admitted-vision-workers"
            assert streams == {"vp:tasks:admitted-vision": ">"}
            reads += 1
            if reads == 1:
                return [
                    (
                        worker_main.TASK_STREAM,
                        [
                            (
                                "1-0",
                                {
                                    "job_id": str(claim.job_id),
                                    "node_execution_id": str(
                                        claim.node_execution_id
                                    ),
                                },
                            )
                        ],
                    )
                ]
            await asyncio.Event().wait()

        async def xack(self, *args):
            raise AssertionError("lease-loss cancellation must leave the PEL pending")

    async def process_message(*args, **kwargs):
        message_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            message_cancelled.set()

    async def no_reclaim(*args, **kwargs):
        return None

    monkeypatch.setattr(worker_main, "_process_message", process_message)
    monkeypatch.setattr(worker_main, "_reclaim_pending", no_reclaim)

    consumer = asyncio.create_task(
        worker_main._consume_registered_worker(Redis(), Registration(lease))
    )
    await asyncio.wait_for(message_started.wait(), timeout=0.2)
    consumer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await consumer

    assert message_cancelled.is_set()


@pytest.mark.asyncio
async def test_process_task_downloads_missing_local_artifact_through_api(
    monkeypatch,
    tmp_path: Path,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    input_artifact_id = uuid.uuid4()
    expected_content = b"cross-node-owned-video"
    missing_local_path = tmp_path / "gpu-scratch" / "assets" / "input.mp4"
    handled_inputs: list[bytes] = []
    handled_paths: list[str] = []
    output_paths: list[str] = []
    requested: list[tuple[str, str]] = []
    authority_locks: list[tuple[uuid.UUID, uuid.UUID]] = []
    succeeded: list[tuple[str, str, str, object | None]] = []
    failed: list[tuple[str, str, str]] = []
    claim = execution_claim(job_id, node_execution_id)

    class CopyHandler:
        async def execute(self, config, input_paths, output_path):
            input_path = input_paths["input"]
            handled_paths.append(input_path)
            handled_inputs.append(Path(input_path).read_bytes())
            output_paths.append(output_path)
            Path(output_path).write_bytes(expected_content)
            return {}

        def cancel(self) -> None:
            return None

    input_artifact = SimpleNamespace(
        id=input_artifact_id,
        job_id=job_id,
        media_info={"content_sha256": hashlib.sha256(expected_content).hexdigest()},
        storage_backend="local",
        storage_path="assets/input.mp4",
        filename="input.mp4",
        file_size=len(expected_content),
    )

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def begin(self):
            return FakeTransaction()

        async def get(self, model, item_id):
            if model is worker_main.Artifact and item_id == input_artifact_id:
                return input_artifact
            return None

        def add(self, item) -> None:
            return None

        async def flush(self) -> None:
            return None

        async def commit(self) -> None:
            return None

    class FakeTransaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class LocalStorage:
        def get_local_path(self, path: str) -> str:
            assert path == "assets/input.mp4"
            return str(missing_local_path)

        async def read(self, path: str) -> bytes:
            raise AssertionError(f"local artifact must use the authoritative API: {path}")

    class DownloadResponse:
        status_code = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def aiter_bytes(self):
            yield expected_content[:7]
            yield expected_content[7:]

    class FakeAsyncClient:
        def __init__(self, **kwargs):
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def stream(self, method: str, url: str):
            requested.append((method, url))
            return DownloadResponse()

    def session_factory():
        return FakeSession()

    async def claim_node(*args, **kwargs):
        return claim

    async def lock_authority(_db, locked_job_id, *, node_execution_id):
        authority_locks.append((locked_job_id, node_execution_id))
        return SimpleNamespace(
            channel=None,
            schedule=SimpleNamespace(state="OPEN", guarded_job_id=job_id),
            task=None,
            job=SimpleNamespace(id=job_id, status=worker_main.JobStatus.RUNNING),
            node=SimpleNamespace(
                id=node_execution_id,
                status=worker_main.NodeStatus.RUNNING,
                worker_id=claim.worker_id,
                started_at=claim.started_at,
            ),
        )

    async def report_success(job: str, node: str, artifact: str, *args) -> None:
        succeeded.append((job, node, artifact, args[0] if args else None))

    async def report_failure(job: str, node: str, error: str) -> None:
        failed.append((job, node, error))

    monkeypatch.setattr(worker_main, "HANDLER_MAP", {"smart_trim": CopyHandler})
    monkeypatch.setattr(worker_main, "get_worker_session", lambda: session_factory)
    monkeypatch.setattr(worker_main, "_claim_node_execution", claim_node)
    monkeypatch.setattr(worker_main, "lock_job_execution_authority", lock_authority)
    monkeypatch.setattr(worker_main, "get_storage", lambda _backend: LocalStorage())
    monkeypatch.setattr(worker_main, "_report_success", report_success)
    monkeypatch.setattr(worker_main, "_report_failure", report_failure)
    monkeypatch.setattr(worker_main.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setattr(
        worker_main,
        "ARTIFACT_DOWNLOAD_BASE_URL",
        "http://vp-api-swarm:8080/api/v1",
        raising=False,
    )
    monkeypatch.setattr(worker_main.settings, "storage_backend", "local")
    monkeypatch.setattr(worker_main.settings, "storage_local_root", str(tmp_path / "storage"))

    await worker_main.process_task(
        {
            "job_id": str(job_id),
            "node_execution_id": str(node_execution_id),
            "node_id": "smart_trim_1",
            "node_type": "smart_trim",
            "config": json.dumps({"prompt": "owned canary"}),
            "input_artifacts": json.dumps({"input": str(input_artifact_id)}),
        }
    )

    assert requested == [
        (
            "GET",
            f"http://vp-api-swarm:8080/api/v1/artifacts/{input_artifact_id}/download",
        )
    ]
    assert handled_inputs == [expected_content]
    assert Path(output_paths[0]).name.startswith(f"{node_execution_id}-")
    assert authority_locks == [(job_id, node_execution_id)] * 3
    assert len(succeeded) == 1
    assert succeeded[0][3] == claim
    assert failed == []
    assert handled_paths and not Path(handled_paths[0]).exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("file_size", (5, None))
async def test_artifact_api_download_stops_at_size_limit_and_cleans_temp_file(
    monkeypatch,
    tmp_path: Path,
    file_size: int | None,
) -> None:
    artifact_id = uuid.uuid4()
    temp_path = tmp_path / "bounded-download.mp4"
    yielded_chunks: list[int] = []
    client_kwargs: dict = {}
    artifact = SimpleNamespace(
        id=artifact_id,
        filename="input.mp4",
        file_size=file_size,
        media_info={},
    )

    class DownloadResponse:
        status_code = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def aiter_bytes(self):
            yielded_chunks.append(6)
            yield b"123456"
            yielded_chunks.append(1)
            yield b"7"

    class FakeAsyncClient:
        def __init__(self, **kwargs):
            client_kwargs.update(kwargs)

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def stream(self, method: str, url: str):
            return DownloadResponse()

    def make_temp_file(*, suffix: str, prefix: str):
        fd = os.open(temp_path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        return fd, str(temp_path)

    monkeypatch.setattr(worker_main.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setattr(worker_main.tempfile, "mkstemp", make_temp_file)
    monkeypatch.setattr(worker_main, "ARTIFACT_DOWNLOAD_MAX_BYTES", 5, raising=False)
    monkeypatch.setattr(worker_main, "ARTIFACT_DOWNLOAD_TOTAL_TIMEOUT_SECONDS", 30.0, raising=False)

    with pytest.raises(RuntimeError, match="exceeds"):
        await worker_main._download_artifact_via_api(artifact)

    assert yielded_chunks == [6]
    assert not temp_path.exists()
    assert client_kwargs["follow_redirects"] is False


@pytest.mark.asyncio
async def test_artifact_api_download_has_total_deadline_and_cleans_temp_file(
    monkeypatch,
    tmp_path: Path,
) -> None:
    temp_path = tmp_path / "timed-out-download.mp4"
    artifact = SimpleNamespace(
        id=uuid.uuid4(),
        filename="input.mp4",
        file_size=10,
        media_info={},
    )

    class DownloadResponse:
        status_code = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def aiter_bytes(self):
            await asyncio.sleep(60)
            yield b"never"

    class FakeAsyncClient:
        def __init__(self, **kwargs):
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def stream(self, method: str, url: str):
            return DownloadResponse()

    def make_temp_file(*, suffix: str, prefix: str):
        fd = os.open(temp_path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        return fd, str(temp_path)

    monkeypatch.setattr(worker_main.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setattr(worker_main.tempfile, "mkstemp", make_temp_file)
    monkeypatch.setattr(worker_main, "ARTIFACT_DOWNLOAD_MAX_BYTES", 100, raising=False)
    monkeypatch.setattr(worker_main, "ARTIFACT_DOWNLOAD_TOTAL_TIMEOUT_SECONDS", 0.01, raising=False)

    with pytest.raises(RuntimeError, match="timed out"):
        await asyncio.wait_for(worker_main._download_artifact_via_api(artifact), timeout=0.2)

    assert not temp_path.exists()


@pytest.mark.asyncio
async def test_process_task_cancels_cross_node_download_after_closing_database_session(
    monkeypatch,
    tmp_path: Path,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    input_artifact_id = uuid.uuid4()
    claim = execution_claim(job_id, node_execution_id)
    missing_local_path = tmp_path / "gpu-scratch" / "assets" / "input.mp4"
    download_started = asyncio.Event()
    session_active = False
    session_state_during_stream: list[bool] = []
    handler_calls: list[str] = []
    succeeded: list[tuple[str, str, str]] = []
    failed: list[tuple[str, str, str]] = []

    class NeverRunHandler:
        async def execute(self, config, input_paths, output_path):
            handler_calls.append("execute")
            raise AssertionError("cancelled node must not execute its handler")

        def cancel(self) -> None:
            handler_calls.append("cancel")

    input_artifact = SimpleNamespace(
        id=input_artifact_id,
        media_info={},
        storage_backend="local",
        storage_path="assets/input.mp4",
        filename="input.mp4",
        file_size=100,
    )

    class FakeSession:
        async def __aenter__(self):
            nonlocal session_active
            session_active = True
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            nonlocal session_active
            session_active = False
            return False

        async def get(self, model, item_id):
            if model is worker_main.Artifact and item_id == input_artifact_id:
                return input_artifact
            return None

    class LocalStorage:
        def get_local_path(self, path: str) -> str:
            return str(missing_local_path)

    class DownloadResponse:
        status_code = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def aiter_bytes(self):
            download_started.set()
            await asyncio.Event().wait()
            yield b"never"

    class FakeAsyncClient:
        def __init__(self, **kwargs):
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def stream(self, method: str, url: str):
            session_state_during_stream.append(session_active)
            return DownloadResponse()

    def session_factory():
        return FakeSession()

    async def claim_node(*args, **kwargs):
        return claim

    async def load_cancel_state(_node_execution_id: str):
        await download_started.wait()
        return worker_main.CancelState(
            job_id=job_id,
            node_status=worker_main.NodeStatus.CANCELLED,
            job_status=worker_main.JobStatus.CANCELLED,
            is_cancelled=True,
            cancel_reason="test cancellation",
        )

    async def report_success(job: str, node: str, artifact: str) -> None:
        succeeded.append((job, node, artifact))

    async def report_failure(job: str, node: str, error: str) -> None:
        failed.append((job, node, error))

    monkeypatch.setattr(worker_main, "HANDLER_MAP", {"smart_trim": NeverRunHandler})
    monkeypatch.setattr(worker_main, "get_worker_session", lambda: session_factory)
    monkeypatch.setattr(worker_main, "_claim_node_execution", claim_node)
    monkeypatch.setattr(worker_main, "_load_cancel_state", load_cancel_state)
    monkeypatch.setattr(worker_main, "get_storage", lambda _backend: LocalStorage())
    monkeypatch.setattr(worker_main, "_report_success", report_success)
    monkeypatch.setattr(worker_main, "_report_failure", report_failure)
    monkeypatch.setattr(worker_main.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setattr(worker_main.settings, "storage_local_root", str(tmp_path / "storage"))

    returned_claim = await asyncio.wait_for(
        worker_main.process_task(
            {
                "job_id": str(job_id),
                "node_execution_id": str(node_execution_id),
                "node_id": "smart_trim_1",
                "node_type": "smart_trim",
                "config": json.dumps({"prompt": "owned canary"}),
                "input_artifacts": json.dumps({"input": str(input_artifact_id)}),
            }
        ),
        timeout=0.5,
    )

    assert session_state_during_stream == [False]
    assert returned_claim == claim
    assert handler_calls == ["cancel"]
    assert succeeded == []
    assert failed == []


@pytest.mark.asyncio
async def test_process_task_stops_before_handler_when_claim_changes_during_download(
    monkeypatch,
    tmp_path: Path,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    input_artifact_id = uuid.uuid4()
    claimed_at = datetime(2026, 7, 22, 12, 0, 0)
    replacement_started_at = claimed_at + timedelta(minutes=11)
    missing_local_path = tmp_path / "gpu-scratch" / "assets" / "input.mp4"
    downloaded_path = tmp_path / "downloaded-input.mp4"
    handler_calls: list[str] = []
    authority_locks: list[tuple[uuid.UUID, uuid.UUID]] = []
    succeeded: list[tuple[str, str, str]] = []
    failed: list[tuple[str, str, str]] = []

    claim = SimpleNamespace(
        job_id=job_id,
        node_execution_id=node_execution_id,
        worker_id="gpu-worker@150:old",
        started_at=claimed_at,
    )
    input_artifact = SimpleNamespace(
        id=input_artifact_id,
        media_info={},
        storage_backend="local",
        storage_path="assets/input.mp4",
        filename="input.mp4",
        file_size=5,
    )

    class NeverRunHandler:
        async def execute(self, config, input_paths, output_path):
            handler_calls.append("execute")
            raise AssertionError("a stale execution claim must not reach the handler")

        def cancel(self) -> None:
            handler_calls.append("cancel")

    class FakeTransaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def begin(self):
            return FakeTransaction()

        async def get(self, model, item_id):
            if model is worker_main.Artifact and item_id == input_artifact_id:
                return input_artifact
            return None

    class LocalStorage:
        def get_local_path(self, path: str) -> str:
            return str(missing_local_path)

    def session_factory():
        return FakeSession()

    async def claim_node(*args, **kwargs):
        return claim

    async def download_artifact(_artifact, _cancel_event):
        downloaded_path.write_bytes(b"video")
        return str(downloaded_path)

    async def lock_authority(_db, locked_job_id, *, node_execution_id):
        authority_locks.append((locked_job_id, node_execution_id))
        return SimpleNamespace(
            channel=None,
            schedule=SimpleNamespace(state="OPEN", guarded_job_id=job_id),
            task=None,
            job=SimpleNamespace(id=job_id, status=worker_main.JobStatus.RUNNING),
            node=SimpleNamespace(
                id=node_execution_id,
                status=worker_main.NodeStatus.RUNNING,
                worker_id="gpu-worker@150:replacement",
                started_at=replacement_started_at,
            ),
        )

    async def not_cancelled(_node_execution_id: str):
        return worker_main.CancelState(
            job_id=job_id,
            node_status=worker_main.NodeStatus.RUNNING,
            job_status=worker_main.JobStatus.RUNNING,
            is_cancelled=False,
            cancel_reason=None,
        )

    async def report_success(job: str, node: str, artifact: str) -> None:
        succeeded.append((job, node, artifact))

    async def report_failure(job: str, node: str, error: str) -> None:
        failed.append((job, node, error))

    monkeypatch.setattr(worker_main, "HANDLER_MAP", {"smart_trim": NeverRunHandler})
    monkeypatch.setattr(worker_main, "get_worker_session", lambda: session_factory)
    monkeypatch.setattr(worker_main, "_claim_node_execution", claim_node)
    monkeypatch.setattr(worker_main, "_download_artifact_with_cancel", download_artifact)
    monkeypatch.setattr(worker_main, "lock_job_execution_authority", lock_authority)
    monkeypatch.setattr(worker_main, "_load_cancel_state", not_cancelled)
    monkeypatch.setattr(worker_main, "get_storage", lambda _backend: LocalStorage())
    monkeypatch.setattr(worker_main, "_report_success", report_success)
    monkeypatch.setattr(worker_main, "_report_failure", report_failure)
    monkeypatch.setattr(worker_main.settings, "storage_local_root", str(tmp_path / "storage"))

    await worker_main.process_task(
        {
            "job_id": str(job_id),
            "node_execution_id": str(node_execution_id),
            "node_id": "smart_trim_1",
            "node_type": "smart_trim",
            "config": "{}",
            "input_artifacts": json.dumps({"input": str(input_artifact_id)}),
        }
    )

    assert authority_locks == [(job_id, node_execution_id)]
    assert handler_calls == ["cancel"]
    assert succeeded == []
    assert failed == []


@pytest.mark.asyncio
async def test_claim_recheck_accepts_naive_and_utc_aware_same_instant(
    monkeypatch,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    claimed_at = datetime(2026, 7, 22, 12, 0, 0)
    claim = worker_main.NodeExecutionClaim(
        job_id=job_id,
        node_execution_id=node_execution_id,
        worker_id="gpu-worker@150:1",
        started_at=claimed_at,
    )

    class FakeTransaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def begin(self):
            return FakeTransaction()

    def session_factory():
        return FakeSession()

    async def lock_authority(_db, locked_job_id, *, node_execution_id):
        assert locked_job_id == job_id
        return SimpleNamespace(
            channel=None,
            schedule=SimpleNamespace(state="OPEN", guarded_job_id=job_id),
            task=None,
            job=SimpleNamespace(id=job_id, status=worker_main.JobStatus.RUNNING),
            node=SimpleNamespace(
                id=node_execution_id,
                status=worker_main.NodeStatus.RUNNING,
                worker_id=claim.worker_id,
                started_at=claimed_at.replace(tzinfo=timezone.utc),
            ),
        )

    monkeypatch.setattr(worker_main, "lock_job_execution_authority", lock_authority)

    await worker_main._require_current_node_execution_claim(
        claim,
        session_factory=session_factory,
    )


@pytest.mark.asyncio
async def test_failure_claim_database_error_propagates_without_event(
    monkeypatch,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    claim = execution_claim(job_id, node_execution_id)
    events: list[tuple] = []

    async def fail_claim_check(_claim) -> None:
        raise RuntimeError("database unavailable")

    async def report_failure(*args) -> None:
        events.append(args)

    monkeypatch.setattr(
        worker_main,
        "_require_current_node_execution_claim",
        fail_claim_check,
    )
    monkeypatch.setattr(worker_main, "_report_failure", report_failure)

    with pytest.raises(RuntimeError, match="database unavailable"):
        await worker_main._report_failure_for_current_claim(
            claim,
            str(job_id),
            str(node_execution_id),
            "handler failed",
        )

    assert events == []


@pytest.mark.asyncio
async def test_remote_artifact_save_and_pointer_flush_share_worker_lease_transaction(
    monkeypatch,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    claim = registered_execution_claim(job_id, node_execution_id)
    transaction_active = False
    calls: list[str] = []

    class FakeTransaction:
        async def __aenter__(self):
            nonlocal transaction_active
            transaction_active = True
            calls.append("begin")
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            nonlocal transaction_active
            calls.append("end")
            transaction_active = False
            return False

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def begin(self):
            return FakeTransaction()

        def add(self, _item) -> None:
            assert transaction_active
            calls.append("add")

        async def flush(self) -> None:
            assert transaction_active
            calls.append("flush")

    async def lock_authority(_db, locked_job_id, *, node_execution_id):
        calls.append("lock")
        return SimpleNamespace(
            channel=None,
            schedule=SimpleNamespace(state="OPEN", guarded_job_id=job_id),
            task=None,
            job=SimpleNamespace(
                id=locked_job_id,
                status=worker_main.JobStatus.RUNNING,
            ),
            node=SimpleNamespace(
                id=node_execution_id,
                status=worker_main.NodeStatus.RUNNING,
                worker_id=claim.worker_id,
                started_at=claim.started_at,
                worker_registration_id=claim.worker_registration_id,
                worker_lease_epoch=claim.worker_lease_epoch,
            ),
        )

    async def require_lease(_db, checked_claim) -> None:
        assert transaction_active
        assert checked_claim == claim
        calls.append("lease")

    async def save_remote_object() -> None:
        assert transaction_active
        assert calls[-1] == "lease"
        calls.append("save")

    monkeypatch.setattr(worker_main, "lock_job_execution_authority", lock_authority)
    monkeypatch.setattr(
        worker_main,
        "require_worker_registration_lease",
        require_lease,
    )

    await worker_main._persist_artifact_for_current_claim(
        claim,
        filename="output.mp4",
        mime_type="video/mp4",
        file_size=42,
        storage_backend="minio",
        storage_path="artifacts/output.mp4",
        media_info={},
        before_persist=save_remote_object,
        session_factory=lambda: FakeSession(),
    )

    assert calls == [
        "begin",
        "lock",
        "lease",
        "save",
        "lease",
        "add",
        "flush",
        "end",
    ]


@pytest.mark.asyncio
async def test_lease_expiry_during_remote_save_rolls_back_before_pointer_insert(
    monkeypatch,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    claim = registered_execution_claim(job_id, node_execution_id)
    calls: list[str] = []
    lease_checks = 0

    class FakeTransaction:
        async def __aenter__(self):
            calls.append("begin")
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            calls.append("rollback" if exc_type is not None else "commit")
            return False

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def begin(self):
            return FakeTransaction()

        def add(self, _item) -> None:
            calls.append("add")

        async def flush(self) -> None:
            calls.append("flush")

    async def lock_authority(_db, locked_job_id, *, node_execution_id):
        calls.append("lock")
        return SimpleNamespace(
            channel=None,
            schedule=SimpleNamespace(state="OPEN", guarded_job_id=job_id),
            task=None,
            job=SimpleNamespace(
                id=locked_job_id,
                status=worker_main.JobStatus.RUNNING,
            ),
            node=SimpleNamespace(
                id=node_execution_id,
                status=worker_main.NodeStatus.RUNNING,
                worker_id=claim.worker_id,
                started_at=claim.started_at,
                worker_registration_id=claim.worker_registration_id,
                worker_lease_epoch=claim.worker_lease_epoch,
            ),
        )

    async def require_lease(_db, _claim) -> None:
        nonlocal lease_checks
        lease_checks += 1
        calls.append(f"lease-{lease_checks}")
        if lease_checks == 2:
            raise worker_main.JobExecutionAuthorityBlocked(
                "worker registration lease expired during save"
            )

    async def save_remote_object() -> None:
        calls.append("save")

    monkeypatch.setattr(worker_main, "lock_job_execution_authority", lock_authority)
    monkeypatch.setattr(
        worker_main,
        "require_worker_registration_lease",
        require_lease,
    )

    with pytest.raises(
        worker_main.JobExecutionAuthorityBlocked,
        match="expired during save",
    ):
        await worker_main._persist_artifact_for_current_claim(
            claim,
            filename="output.mp4",
            mime_type="video/mp4",
            file_size=42,
            storage_backend="minio",
            storage_path="artifacts/output.mp4",
            media_info={},
            before_persist=save_remote_object,
            session_factory=lambda: FakeSession(),
        )

    assert calls == [
        "begin",
        "lock",
        "lease-1",
        "save",
        "lease-2",
        "rollback",
    ]


@pytest.mark.asyncio
async def test_artifact_persistence_rejects_replaced_execution_claim(
    monkeypatch,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    claim = execution_claim(job_id, node_execution_id)
    added: list[object] = []

    class FakeTransaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def begin(self):
            return FakeTransaction()

        def add(self, item) -> None:
            added.append(item)

        async def flush(self) -> None:
            raise AssertionError("a stale execution must not flush an artifact")

    def session_factory():
        return FakeSession()

    async def lock_authority(_db, locked_job_id, *, node_execution_id):
        return SimpleNamespace(
            channel=None,
            schedule=SimpleNamespace(state="OPEN", guarded_job_id=job_id),
            task=None,
            job=SimpleNamespace(id=locked_job_id, status=worker_main.JobStatus.RUNNING),
            node=SimpleNamespace(
                id=node_execution_id,
                status=worker_main.NodeStatus.RUNNING,
                worker_id="gpu-worker@150:replacement",
                started_at=claim.started_at + timedelta(minutes=1),
            ),
        )

    monkeypatch.setattr(worker_main, "lock_job_execution_authority", lock_authority)

    with pytest.raises(
        worker_main.JobExecutionAuthorityBlocked,
        match="claim changed",
    ):
        await worker_main._persist_artifact_for_current_claim(
            claim,
            filename="output.mp4",
            mime_type="video/mp4",
            file_size=42,
            storage_backend="local",
            storage_path="artifacts/output.mp4",
            media_info={},
            session_factory=session_factory,
        )

    assert added == []


@pytest.mark.asyncio
async def test_lease_expiry_after_remote_save_cleans_generation_output(
    monkeypatch,
    tmp_path: Path,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    claim = execution_claim(job_id, node_execution_id)
    saved: list[tuple[str, bytes]] = []
    deleted: list[str] = []
    output_paths: list[str] = []
    handler_calls: list[str] = []

    class SuccessfulHandler:
        async def execute(self, config, input_paths, output_path):
            handler_calls.append("execute")
            output_paths.append(output_path)
            Path(output_path).write_bytes(b"generation output")
            return {}

        def cancel(self) -> None:
            handler_calls.append("cancel")

    class RemoteStorage:
        async def save(self, path: str, data) -> int:
            content = data.read()
            saved.append((path, content))
            return len(content)

        async def delete(self, path: str) -> None:
            deleted.append(path)

    async def claim_node(*args, **kwargs):
        return claim

    async def require_current_claim(_claim) -> None:
        return None

    async def not_cancelled(_node_execution_id: str):
        return worker_main.CancelState(
            job_id=job_id,
            node_status=worker_main.NodeStatus.RUNNING,
            job_status=worker_main.JobStatus.RUNNING,
            is_cancelled=False,
            cancel_reason=None,
        )

    async def reject_artifact(_claim, **kwargs) -> str:
        await kwargs["before_persist"]()
        raise worker_main.JobExecutionAuthorityBlocked(
            "node execution claim changed"
        )

    async def suppress_stale_failure(*args) -> bool:
        return False

    monkeypatch.setattr(worker_main, "HANDLER_MAP", {"smart_trim": SuccessfulHandler})
    monkeypatch.setattr(worker_main, "_claim_node_execution", claim_node)
    monkeypatch.setattr(
        worker_main,
        "_require_current_node_execution_claim",
        require_current_claim,
    )
    monkeypatch.setattr(worker_main, "_load_cancel_state", not_cancelled)
    monkeypatch.setattr(
        worker_main,
        "_persist_artifact_for_current_claim",
        reject_artifact,
    )
    monkeypatch.setattr(
        worker_main,
        "_report_failure_for_current_claim",
        suppress_stale_failure,
    )
    monkeypatch.setattr(worker_main, "get_storage", lambda _backend: RemoteStorage())
    monkeypatch.setattr(worker_main.settings, "storage_backend", "minio")
    monkeypatch.setattr(
        worker_main.settings,
        "storage_local_root",
        str(tmp_path / "storage"),
    )

    await worker_main.process_task(
        {
            "job_id": str(job_id),
            "node_execution_id": str(node_execution_id),
            "node_id": "smart_trim_1",
            "node_type": "smart_trim",
            "config": "{}",
            "input_artifacts": "{}",
        }
    )

    expected_storage_path = (
        f"staging/artifacts/{job_id}/{Path(output_paths[0]).name}"
    )
    assert saved == [(expected_storage_path, b"generation output")]
    assert deleted == [expected_storage_path]
    assert not Path(output_paths[0]).exists()
    assert handler_calls == ["execute", "cancel"]


@pytest.mark.asyncio
async def test_uncommitted_remote_output_cleanup_is_bounded(
    monkeypatch,
) -> None:
    delete_started = asyncio.Event()

    class HangingStorage:
        async def delete(self, _path: str) -> None:
            delete_started.set()
            await asyncio.Event().wait()

    monkeypatch.setattr(
        worker_main,
        "REMOTE_ARTIFACT_CLEANUP_TIMEOUT_SECONDS",
        0.01,
    )

    await asyncio.wait_for(
        worker_main._cleanup_uncommitted_remote_output(
            HangingStorage(),
            "artifacts/uncommitted.mp4",
        ),
        timeout=0.1,
    )

    assert delete_started.is_set()


@pytest.mark.asyncio
async def test_handler_failure_after_claim_loss_does_not_emit_node_failed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    claim = execution_claim(job_id, node_execution_id)
    claim_checks: list[str] = []
    handler_calls: list[str] = []
    failed: list[tuple[str, str, str]] = []

    class FailingHandler:
        async def execute(self, config, input_paths, output_path):
            handler_calls.append("execute")
            raise RuntimeError("handler failed after replacement worker took over")

        def cancel(self) -> None:
            handler_calls.append("cancel")

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    def session_factory():
        return FakeSession()

    async def claim_node(*args, **kwargs):
        return claim

    async def require_current_claim(_claim) -> None:
        claim_checks.append("checked")
        if len(claim_checks) > 1:
            raise worker_main.JobExecutionAuthorityBlocked("node execution claim changed")

    async def not_cancelled(_node_execution_id: str):
        return worker_main.CancelState(
            job_id=job_id,
            node_status=worker_main.NodeStatus.RUNNING,
            job_status=worker_main.JobStatus.RUNNING,
            is_cancelled=False,
            cancel_reason=None,
        )

    async def report_failure(job: str, node: str, error: str) -> None:
        failed.append((job, node, error))

    monkeypatch.setattr(worker_main, "HANDLER_MAP", {"smart_trim": FailingHandler})
    monkeypatch.setattr(worker_main, "get_worker_session", lambda: session_factory)
    monkeypatch.setattr(worker_main, "_claim_node_execution", claim_node)
    monkeypatch.setattr(
        worker_main,
        "_require_current_node_execution_claim",
        require_current_claim,
    )
    monkeypatch.setattr(worker_main, "_load_cancel_state", not_cancelled)
    monkeypatch.setattr(worker_main, "_report_failure", report_failure)
    monkeypatch.setattr(worker_main.settings, "storage_local_root", str(tmp_path / "storage"))

    await worker_main.process_task(
        {
            "job_id": str(job_id),
            "node_execution_id": str(node_execution_id),
            "node_id": "smart_trim_1",
            "node_type": "smart_trim",
            "config": "{}",
            "input_artifacts": "{}",
        }
    )

    assert claim_checks == ["checked", "checked"]
    assert handler_calls == ["execute", "cancel"]
    assert failed == []


@pytest.mark.asyncio
async def test_handler_success_after_claim_loss_does_not_emit_node_completed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    claim = execution_claim(job_id, node_execution_id)
    claim_checks: list[str] = []
    handler_calls: list[str] = []
    succeeded: list[tuple[str, str, str]] = []
    failed: list[tuple[str, str, str]] = []

    class SuccessfulHandler:
        async def execute(self, config, input_paths, output_path):
            handler_calls.append("execute")
            Path(output_path).write_bytes(b"stale output")
            return {}

        def cancel(self) -> None:
            handler_calls.append("cancel")

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def add(self, item) -> None:
            return None

        async def flush(self) -> None:
            return None

        async def commit(self) -> None:
            return None

    def session_factory():
        return FakeSession()

    async def claim_node(*args, **kwargs):
        return claim

    async def require_current_claim(_claim) -> None:
        claim_checks.append("checked")
        if len(claim_checks) > 1:
            raise worker_main.JobExecutionAuthorityBlocked("node execution claim changed")

    async def not_cancelled(_node_execution_id: str):
        return worker_main.CancelState(
            job_id=job_id,
            node_status=worker_main.NodeStatus.RUNNING,
            job_status=worker_main.JobStatus.RUNNING,
            is_cancelled=False,
            cancel_reason=None,
        )

    async def report_success(job: str, node: str, artifact: str) -> None:
        succeeded.append((job, node, artifact))

    async def report_failure(job: str, node: str, error: str) -> None:
        failed.append((job, node, error))

    monkeypatch.setattr(worker_main, "HANDLER_MAP", {"smart_trim": SuccessfulHandler})
    monkeypatch.setattr(worker_main, "get_worker_session", lambda: session_factory)
    monkeypatch.setattr(worker_main, "_claim_node_execution", claim_node)
    monkeypatch.setattr(
        worker_main,
        "_require_current_node_execution_claim",
        require_current_claim,
    )
    monkeypatch.setattr(worker_main, "_load_cancel_state", not_cancelled)
    monkeypatch.setattr(worker_main, "_report_success", report_success)
    monkeypatch.setattr(worker_main, "_report_failure", report_failure)
    monkeypatch.setattr(worker_main.settings, "storage_local_root", str(tmp_path / "storage"))

    await worker_main.process_task(
        {
            "job_id": str(job_id),
            "node_execution_id": str(node_execution_id),
            "node_id": "smart_trim_1",
            "node_type": "smart_trim",
            "config": "{}",
            "input_artifacts": "{}",
        }
    )

    assert claim_checks == ["checked", "checked"]
    assert handler_calls == ["execute", "cancel"]
    assert succeeded == []
    assert failed == []


@pytest.mark.asyncio
async def test_completed_artifact_download_is_removed_when_cancel_wins_race(
    monkeypatch,
    tmp_path: Path,
) -> None:
    temp_path = tmp_path / "completed-before-cancel.mp4"
    artifact = worker_main.InputArtifactSnapshot(
        id=uuid.uuid4(),
        media_info={},
        storage_backend="local",
        storage_path="assets/input.mp4",
        filename="input.mp4",
        file_size=5,
    )
    cancel_event = asyncio.Event()
    cancel_event.set()

    async def complete_download(_artifact):
        temp_path.write_bytes(b"video")
        return str(temp_path)

    monkeypatch.setattr(worker_main, "_download_artifact_via_api", complete_download)

    with pytest.raises(worker_main.CancelledError):
        await worker_main._download_artifact_with_cancel(artifact, cancel_event)

    assert not temp_path.exists()


@pytest.mark.asyncio
async def test_main_restores_process_globals_for_repeated_in_process_runs(
    monkeypatch,
) -> None:
    original_worker_id = "ffmpeg-worker@original:1"
    disposed: list[str] = []

    class StopRun(RuntimeError):
        pass

    class Engine:
        async def dispose(self):
            disposed.append("engine")

    class Registration:
        redis_consumer_id = "ffmpeg-worker@127:2:registered"

        async def close(self):
            return None

    class Redis:
        async def aclose(self):
            return None

    def configure(_database_url):
        worker_main.engine_db = Engine()
        worker_main.worker_session = object()

    async def start_registration(*_args):
        return Registration()

    async def stop_run(_registration, _consumer):
        if hasattr(_consumer, "close"):
            _consumer.close()
        raise StopRun

    async def consume(_redis, _registration):
        return None

    monkeypatch.setattr(worker_main, "WORKER_ID", original_worker_id)
    monkeypatch.setattr(worker_main, "engine_db", None)
    monkeypatch.setattr(worker_main, "worker_session", None)
    monkeypatch.setattr(
        worker_main,
        "enforce_worker_admission_from_env",
        lambda: None,
    )
    monkeypatch.setattr(
        worker_main,
        "load_worker_database_url",
        lambda _env: "postgresql+asyncpg://worker@db/vp",
    )
    monkeypatch.setattr(worker_main, "configure_worker_database", configure)
    monkeypatch.setattr(
        worker_main,
        "load_worker_admission_token",
        lambda _env: "token",
    )
    monkeypatch.setattr(
        worker_main,
        "_start_worker_registration",
        start_registration,
    )
    monkeypatch.setattr(worker_main, "_redis", Redis)
    monkeypatch.setattr(worker_main, "_consume_registered_worker", consume)
    monkeypatch.setattr(
        worker_main,
        "_run_until_registration_loss",
        stop_run,
    )

    with pytest.raises(StopRun):
        await worker_main.main()

    assert worker_main.WORKER_ID == original_worker_id
    assert worker_main.engine_db is None
    assert worker_main.worker_session is None
    assert disposed == ["engine"]


@pytest.mark.asyncio
async def test_worker_admission_runs_before_database_and_redis(monkeypatch) -> None:
    events: list[str] = []

    class StopStartup(RuntimeError):
        pass

    monkeypatch.setattr(
        worker_main,
        "enforce_worker_admission_from_env",
        lambda env=None: events.append("admission"),
        raising=False,
    )
    monkeypatch.setattr(
        worker_main,
        "load_worker_database_url",
        lambda env: events.append("database-secret") or "postgresql+asyncpg://worker@db/vp",
        raising=False,
    )
    monkeypatch.setattr(
        worker_main,
        "configure_worker_database",
        lambda database_url=None: events.append("database"),
        raising=False,
    )
    monkeypatch.setattr(
        worker_main,
        "load_worker_admission_token",
        lambda env: events.append("token-secret") or "token",
        raising=False,
    )

    class Registration:
        redis_consumer_id = "ffmpeg-worker@127:1:instance"

        async def close(self):
            events.append("revoke")

    async def register(env, database_url, admission_token):
        events.append("registration")
        return Registration()

    monkeypatch.setattr(
        worker_main,
        "_start_worker_registration",
        register,
        raising=False,
    )

    def stop_at_redis() -> None:
        events.append("redis")
        raise StopStartup

    monkeypatch.setattr(worker_main, "_redis", stop_at_redis)

    with pytest.raises(StopStartup):
        await worker_main.main()

    assert events == [
        "admission",
        "database-secret",
        "database",
        "token-secret",
        "registration",
        "redis",
        "revoke",
    ]


@pytest.mark.asyncio
async def test_denied_durable_registration_performs_zero_redis_calls(
    monkeypatch,
    caplog,
) -> None:
    touched: list[str] = []
    credential = "postgresql+asyncpg://runtime:never-log-me@vp-postgres/vp"

    monkeypatch.setattr(
        worker_main,
        "enforce_worker_admission_from_env",
        lambda env=None: None,
    )
    monkeypatch.setattr(
        worker_main,
        "load_worker_database_url",
        lambda env: credential,
        raising=False,
    )
    monkeypatch.setattr(
        worker_main,
        "configure_worker_database",
        lambda database_url=None: touched.append("database"),
    )
    monkeypatch.setattr(
        worker_main,
        "load_worker_admission_token",
        lambda env: "admission-token",
        raising=False,
    )

    async def deny_registration(env, database_url, admission_token):
        raise worker_main.WorkerRegistrationError("token_invalid")

    monkeypatch.setattr(
        worker_main,
        "_start_worker_registration",
        deny_registration,
        raising=False,
    )
    monkeypatch.setattr(
        worker_main,
        "_redis",
        lambda: touched.append("redis"),
    )

    with pytest.raises(SystemExit) as exc:
        await worker_main.main()

    assert exc.value.code == 2
    assert touched == ["database"]
    assert credential not in caplog.text


@pytest.mark.asyncio
async def test_unhandled_task_error_leaves_message_pending_without_generationless_failure(
    monkeypatch,
) -> None:
    acknowledgements: list[tuple[str, str, str]] = []
    failure_events: list[tuple] = []

    class FakeRedis:
        async def xack(self, stream: str, group: str, message_id: str) -> None:
            acknowledgements.append((stream, group, message_id))

    async def fail_before_claim(_data):
        raise RuntimeError("worker failed before a durable claim was returned")

    async def heartbeat(_redis, _message_id):
        await asyncio.Event().wait()

    async def report_failure(*args) -> None:
        failure_events.append(args)

    monkeypatch.setattr(worker_main, "process_task", fail_before_claim)
    monkeypatch.setattr(worker_main, "_heartbeat_message", heartbeat)
    monkeypatch.setattr(worker_main, "_report_failure", report_failure)

    await worker_main._process_message(
        FakeRedis(),
        "1-0",
        {
            "job_id": str(uuid.uuid4()),
            "node_execution_id": str(uuid.uuid4()),
        },
    )

    assert failure_events == []
    assert acknowledgements == []


@pytest.mark.asyncio
async def test_handler_constructor_failure_reports_for_exact_claim(monkeypatch) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    claim = execution_claim(job_id, node_execution_id)
    failures: list[tuple[object, str, str, str]] = []

    class BrokenHandler:
        def __init__(self) -> None:
            raise RuntimeError("handler constructor failed")

    async def claim_node(*args, **kwargs):
        return claim

    async def report_failure(
        handled_claim,
        handled_job_id: str,
        handled_node_id: str,
        error: str,
    ) -> bool:
        failures.append(
            (handled_claim, handled_job_id, handled_node_id, error)
        )
        return True

    monkeypatch.setattr(worker_main, "HANDLER_MAP", {"smart_trim": BrokenHandler})
    monkeypatch.setattr(worker_main, "_claim_node_execution", claim_node)
    monkeypatch.setattr(
        worker_main,
        "_report_failure_for_current_claim",
        report_failure,
    )

    await worker_main.process_task(
        {
            "job_id": str(job_id),
            "node_execution_id": str(node_execution_id),
            "node_id": "smart_trim_1",
            "node_type": "smart_trim",
            "config": "{}",
            "input_artifacts": "{}",
        }
    )

    assert failures == [
        (
            claim,
            str(job_id),
            str(node_execution_id),
            "handler constructor failed",
        )
    ]


@pytest.mark.asyncio
async def test_denied_worker_stops_before_database_or_redis(monkeypatch) -> None:
    touched: list[str] = []

    def deny_worker() -> None:
        raise WorkerAdmissionError("unsafe worker configuration")

    monkeypatch.setattr(
        worker_main,
        "enforce_worker_admission_from_env",
        deny_worker,
        raising=False,
    )
    monkeypatch.setattr(
        worker_main,
        "configure_worker_database",
        lambda: touched.append("database"),
        raising=False,
    )
    monkeypatch.setattr(worker_main, "_redis", lambda: touched.append("redis"))

    with pytest.raises(SystemExit) as exc:
        await worker_main.main()

    assert exc.value.code == 2
    assert touched == []


@pytest.mark.asyncio
async def test_process_task_injects_youtube_context_without_changing_other_handler_constructors(
    monkeypatch,
    tmp_path: Path,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    input_artifact_id = uuid.uuid4()
    input_path = tmp_path / "input.mp4"
    input_path.write_bytes(b"input")
    created: list[tuple[str, object | None]] = []
    lease_refreshers: list[object | None] = []
    executed_configs: list[dict] = []

    class YouTubeHandler:
        def __init__(self, *, session_factory, lease_refresher=None):
            created.append(("youtube", session_factory))
            lease_refreshers.append(lease_refresher)

        async def execute(self, config, input_paths, output_path):
            executed_configs.append(dict(config))
            Path(output_path).write_bytes(Path(input_paths["input"]).read_bytes())
            return {}

        def cancel(self) -> None:
            return None

    class OtherHandler:
        def __init__(self):
            created.append(("other", None))

        async def execute(self, config, input_paths, output_path):
            Path(output_path).write_bytes(Path(input_paths["input"]).read_bytes())
            return {}

        def cancel(self) -> None:
            return None

    input_artifact = SimpleNamespace(
        job_id=job_id,
        media_info={},
        storage_backend="local",
        storage_path=str(input_path),
        filename="input.mp4",
    )
    node_execution = SimpleNamespace(
        job_id=job_id,
        node_id="youtube_upload_1",
        node_type="youtube_upload",
        node_config={"title": "Canary"},
        input_artifact_ids=[input_artifact_id],
        status=None,
        started_at=None,
        worker_id=None,
    )

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def get(self, model, item_id):
            if model is worker_main.NodeExecution:
                return node_execution
            if model is worker_main.Artifact:
                return input_artifact
            return None

        def add(self, item) -> None:
            return None

        async def flush(self) -> None:
            return None

        async def commit(self) -> None:
            return None

    class LocalStorage:
        def get_local_path(self, path: str) -> str:
            return path

    def process_session_factory():
        return FakeSession()

    async def not_cancelled(_node_execution_id: str):
        return worker_main.CancelState(None, None, None, False, None)

    async def report_success(*args) -> None:
        return None

    claim = registered_execution_claim(job_id, node_execution_id)

    async def claim_node(*args, **kwargs):
        return claim

    async def refresh_worker_lease(*, minimum_margin_seconds: float):
        return None

    async def require_current_claim(_claim) -> None:
        return None

    async def persist_artifact(_claim, **kwargs) -> str:
        return str(uuid.uuid4())

    monkeypatch.setattr(
        worker_main,
        "HANDLER_MAP",
        {"youtube_upload": object, "source": OtherHandler},
    )
    monkeypatch.setattr(worker_main, "YouTubeUploadHandler", YouTubeHandler)
    monkeypatch.setattr(worker_main, "get_worker_session", lambda: process_session_factory)
    monkeypatch.setattr(worker_main, "_claim_node_execution", claim_node)
    monkeypatch.setattr(
        worker_main,
        "_require_current_node_execution_claim",
        require_current_claim,
    )
    monkeypatch.setattr(
        worker_main,
        "_persist_artifact_for_current_claim",
        persist_artifact,
    )
    monkeypatch.setattr(worker_main, "_load_cancel_state", not_cancelled)
    monkeypatch.setattr(worker_main, "get_storage", lambda _backend: LocalStorage())
    monkeypatch.setattr(worker_main, "_report_success", report_success)
    monkeypatch.setattr(worker_main.settings, "storage_backend", "local")
    monkeypatch.setattr(worker_main.settings, "storage_local_root", str(tmp_path / "storage"))

    data = {
        "job_id": str(job_id),
        "node_execution_id": str(node_execution_id),
        "node_id": "youtube_upload_1",
        "node_type": "youtube_upload",
        "config": json.dumps({"title": "Canary"}),
        "input_artifacts": json.dumps({"input": str(input_artifact_id)}),
    }
    await worker_main.process_task(
        data,
        worker_lease=worker_lease_for(claim),
        lease_refresher=refresh_worker_lease,
    )

    await worker_main.process_task({**data, "node_id": "source_1", "node_type": "source"})

    assert created == [("youtube", process_session_factory), ("other", None)]
    assert lease_refreshers == [refresh_worker_lease]
    assert executed_configs == [
        {
            "title": "Canary",
            "_job_id": str(job_id),
            "_node_execution_id": str(node_execution_id),
            "_input_artifact_ids": {"input": str(input_artifact_id)},
                "_execution_claim": {
                    "worker_id": "test-worker@localhost:1",
                    "started_at": "2026-07-22T12:00:00+00:00",
                    "worker_registration_id": str(
                        claim.worker_registration_id
                    ),
                    "worker_lease_epoch": claim.worker_lease_epoch,
                },
            "_input_artifact_meta": {"input": {}},
        }
    ]
