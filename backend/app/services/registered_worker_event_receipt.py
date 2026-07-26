from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.job import JobStatus, NodeStatus
from app.models.registered_worker_event_receipt import (
    RegisteredWorkerEventReceipt,
    WorkerEventDispatch,
)
from app.services.job_execution_authority import (
    NodeExecutionClaim,
    lock_job_execution_authority,
    observe_worker_registration_lease,
    require_active_execution_authority,
    require_matching_node_execution_claim,
)


_IDEMPOTENT_XADD_SCRIPT = """
local existing = redis.call('GET', KEYS[2])
if existing then
    return existing
end
local message_id = redis.call('XADD', KEYS[1], '*', unpack(ARGV))
redis.call('SET', KEYS[2], message_id)
return message_id
"""


class RegisteredWorkerEventError(RuntimeError):
    """A registered event could not be proven or safely resumed."""


@dataclass(frozen=True)
class RegisteredWorkerEvent:
    redis_stream: str
    consumer_group: str
    message_id: str
    payload: dict[str, str]
    payload_sha256: str
    event_type: str
    job_id: uuid.UUID
    node_execution_id: uuid.UUID
    claim: NodeExecutionClaim
    source_task_stream: str
    source_task_group: str
    source_task_message_id: str

    def receipt_facts(self) -> dict[str, object]:
        registration_id = self.claim.worker_registration_id
        lease_epoch = self.claim.worker_lease_epoch
        if registration_id is None or lease_epoch is None:
            raise RegisteredWorkerEventError(
                "registered event has no worker registration claim"
            )
        return {
            "redis_stream": self.redis_stream,
            "consumer_group": self.consumer_group,
            "message_id": self.message_id,
            "payload_sha256": self.payload_sha256,
            "payload_json": dict(self.payload),
            "event_type": self.event_type,
            "job_id": self.job_id,
            "node_execution_id": self.node_execution_id,
            "worker_registration_id": registration_id,
            "worker_lease_epoch": lease_epoch,
            "worker_id": self.claim.worker_id,
            "worker_started_at": self.claim.started_at,
            "source_task_stream": self.source_task_stream,
            "source_task_group": self.source_task_group,
            "source_task_message_id": self.source_task_message_id,
        }


def canonical_redis_payload_sha256(payload: Mapping[str, str]) -> str:
    if any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in payload.items()
    ):
        raise RegisteredWorkerEventError(
            "Redis event payload must contain strings"
        )
    encoded = json.dumps(
        dict(payload),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def parse_registered_worker_event(
    *,
    redis_stream: str,
    consumer_group: str,
    message_id: str,
    payload: Mapping[str, str],
) -> RegisteredWorkerEvent:
    for label, value in (
        ("redis stream", redis_stream),
        ("consumer group", consumer_group),
        ("message id", message_id),
    ):
        if not isinstance(value, str) or not value.strip():
            raise RegisteredWorkerEventError(f"{label} is invalid")
    canonical_payload_sha256 = canonical_redis_payload_sha256(payload)
    event_type = payload.get("event")
    if event_type not in {"node_completed", "node_failed"}:
        raise RegisteredWorkerEventError("registered event type is invalid")
    try:
        job_id = uuid.UUID(payload["job_id"])
        node_execution_id = uuid.UUID(payload["node_execution_id"])
        registration_id = uuid.UUID(payload["worker_registration_id"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RegisteredWorkerEventError(
            "registered event identifiers are invalid"
        ) from exc

    worker_id = payload.get("worker_id")
    started_at_raw = payload.get("started_at")
    lease_epoch_raw = payload.get("worker_lease_epoch")
    source_task_stream = payload.get("task_stream")
    source_task_group = payload.get("task_group")
    source_task_message_id = payload.get("task_message_id")
    if not all(
        isinstance(value, str) and value.strip()
        for value in (
            worker_id,
            started_at_raw,
            lease_epoch_raw,
            source_task_stream,
            source_task_group,
            source_task_message_id,
        )
    ):
        raise RegisteredWorkerEventError(
            "registered event claim or task delivery is invalid"
        )
    assert worker_id is not None
    assert started_at_raw is not None
    assert lease_epoch_raw is not None
    assert source_task_stream is not None
    assert source_task_group is not None
    assert source_task_message_id is not None
    if not lease_epoch_raw.isdigit() or int(lease_epoch_raw) <= 0:
        raise RegisteredWorkerEventError(
            "registered event lease epoch is invalid"
        )
    try:
        started_at = datetime.fromisoformat(started_at_raw)
    except ValueError as exc:
        raise RegisteredWorkerEventError(
            "registered event worker start time is invalid"
        ) from exc
    if started_at.tzinfo is None or started_at.utcoffset() is None:
        raise RegisteredWorkerEventError(
            "registered event worker start time is invalid"
        )
    claim = NodeExecutionClaim(
        job_id=job_id,
        node_execution_id=node_execution_id,
        worker_id=worker_id.strip(),
        started_at=started_at,
        worker_registration_id=registration_id,
        worker_lease_epoch=int(lease_epoch_raw),
    )
    return RegisteredWorkerEvent(
        redis_stream=redis_stream.strip(),
        consumer_group=consumer_group.strip(),
        message_id=message_id.strip(),
        payload=dict(payload),
        payload_sha256=canonical_payload_sha256,
        event_type=event_type,
        job_id=job_id,
        node_execution_id=node_execution_id,
        claim=claim,
        source_task_stream=source_task_stream.strip(),
        source_task_group=source_task_group.strip(),
        source_task_message_id=source_task_message_id.strip(),
    )


class RegisteredWorkerEventReceiptService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        authority_locker: Callable[..., Awaitable[Any]] = (
            lock_job_execution_authority
        ),
        lease_observer: Callable[..., Awaitable[None]] = (
            observe_worker_registration_lease
        ),
    ) -> None:
        self._session_factory = session_factory
        self._authority_locker = authority_locker
        self._lease_observer = lease_observer

    async def accept_and_apply(
        self,
        event: RegisteredWorkerEvent,
        apply_event: Callable[
            [AsyncSession, RegisteredWorkerEventReceipt, RegisteredWorkerEvent],
            Awaitable[None],
        ],
    ) -> uuid.UUID:
        async with self._session_factory() as db:
            async with db.begin():
                existing = await self._locked_receipt(db, event)
                if existing is not None:
                    self._require_matching_receipt(existing, event)
                    if existing.application_state != "applied":
                        raise RegisteredWorkerEventError(
                            "registered event receipt is not durably applied"
                        )
                    return existing.id

                authority = await self._authority_locker(
                    db,
                    event.job_id,
                    node_execution_id=event.node_execution_id,
                    lock_all_nodes=True,
                )
                require_active_execution_authority(
                    authority,
                    job_statuses={JobStatus.RUNNING},
                    node_statuses={NodeStatus.RUNNING},
                )
                require_matching_node_execution_claim(
                    authority,
                    event.claim,
                )

                existing = await self._locked_receipt(db, event)
                if existing is not None:
                    self._require_matching_receipt(existing, event)
                    if existing.application_state != "applied":
                        raise RegisteredWorkerEventError(
                            "registered event receipt is not durably applied"
                        )
                    return existing.id

                await self._lease_observer(db, event.claim)
                receipt = RegisteredWorkerEventReceipt(
                    **event.receipt_facts(),
                    application_state="accepted",
                    ack_state="pending",
                    source_task_ack_state="pending",
                )
                db.add(receipt)
                await db.flush()
                await apply_event(db, receipt, event)
                receipt.application_state = "applied"
                receipt.applied_at = datetime.now(timezone.utc)
                await db.flush()
                receipt_id = receipt.id
            return receipt_id

    async def acknowledge_applied(
        self,
        redis: Any,
        event: RegisteredWorkerEvent,
    ) -> None:
        async with self._session_factory() as db:
            async with db.begin():
                receipt = await self._locked_receipt(db, event)
                if receipt is None:
                    raise RegisteredWorkerEventError(
                        "registered event receipt is missing"
                    )
                self._require_matching_receipt(receipt, event)
                if receipt.application_state != "applied":
                    raise RegisteredWorkerEventError(
                        "registered event receipt is not durably applied"
                    )
                await self._acknowledge_locked_receipt(
                    db,
                    redis,
                    receipt,
                )

    async def reconcile_pending_acknowledgements(
        self,
        redis: Any,
        *,
        limit: int = 100,
    ) -> None:
        if type(limit) is not int or limit <= 0:
            raise ValueError("limit must be positive")
        async with self._session_factory() as db:
            receipt_ids = list(
                (
                    await db.execute(
                        select(RegisteredWorkerEventReceipt.id)
                        .where(
                            RegisteredWorkerEventReceipt.application_state
                            == "applied",
                            (
                                RegisteredWorkerEventReceipt.ack_state
                                != "acknowledged"
                            )
                            | (
                                RegisteredWorkerEventReceipt.source_task_ack_state
                                != "acknowledged"
                            ),
                        )
                        .order_by(
                            RegisteredWorkerEventReceipt.accepted_at
                        )
                        .limit(limit)
                    )
                ).scalars()
            )
        for receipt_id in receipt_ids:
            await self._acknowledge_receipt_id(redis, receipt_id)

    async def _acknowledge_receipt_id(
        self,
        redis: Any,
        receipt_id: uuid.UUID,
    ) -> None:
        async with self._session_factory() as db:
            async with db.begin():
                receipt = (
                    await db.execute(
                        select(RegisteredWorkerEventReceipt)
                        .where(
                            RegisteredWorkerEventReceipt.id == receipt_id
                        )
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if receipt is None:
                    raise RegisteredWorkerEventError(
                        "registered event receipt is missing"
                    )
                if receipt.application_state != "applied":
                    raise RegisteredWorkerEventError(
                        "registered event receipt is not durably applied"
                    )
                await self._acknowledge_locked_receipt(
                    db,
                    redis,
                    receipt,
                )

    @staticmethod
    async def _acknowledge_locked_receipt(
        db: AsyncSession,
        redis: Any,
        receipt: RegisteredWorkerEventReceipt,
    ) -> None:
        if receipt.source_task_ack_state != "acknowledged":
            task_acknowledged = await redis.xack(
                receipt.source_task_stream,
                receipt.source_task_group,
                receipt.source_task_message_id,
            )
            RegisteredWorkerEventReceiptService._require_xack_result(
                task_acknowledged,
                label="worker task",
            )
        if receipt.ack_state != "acknowledged":
            event_acknowledged = await redis.xack(
                receipt.redis_stream,
                receipt.consumer_group,
                receipt.message_id,
            )
            RegisteredWorkerEventReceiptService._require_xack_result(
                event_acknowledged,
                label="worker event",
            )

        acknowledged_at = datetime.now(timezone.utc)
        if receipt.source_task_ack_state != "acknowledged":
            receipt.source_task_ack_state = "acknowledged"
            receipt.source_task_acknowledged_at = acknowledged_at
        if receipt.ack_state != "acknowledged":
            receipt.ack_state = "acknowledged"
            receipt.acknowledged_at = acknowledged_at
        await db.flush()

    @staticmethod
    def _require_xack_result(result: object, *, label: str) -> None:
        if type(result) is not int or result not in {0, 1}:
            raise RegisteredWorkerEventError(
                f"Redis {label} acknowledgement result is invalid"
            )

    async def deliver_pending_dispatches(
        self,
        redis: Any,
        receipt_id: uuid.UUID,
    ) -> None:
        async with self._session_factory() as db:
            receipt = await db.get(
                RegisteredWorkerEventReceipt,
                receipt_id,
            )
            if receipt is None or receipt.application_state != "applied":
                raise RegisteredWorkerEventError(
                    "dispatch has no applied event receipt"
                )
            dispatches = list(
                (
                    await db.execute(
                        select(WorkerEventDispatch)
                        .where(
                            WorkerEventDispatch.receipt_id == receipt_id,
                            WorkerEventDispatch.delivery_state == "pending",
                        )
                        .order_by(WorkerEventDispatch.created_at)
                    )
                ).scalars()
            )

        for dispatch in dispatches:
            payload = self._validated_dispatch_payload(dispatch)
            fields: list[str] = []
            for key, value in sorted(payload.items()):
                fields.extend((key, value))
            marker = f"vp:worker-event-dispatch:{dispatch.dispatch_key}"
            message_id = await redis.eval(
                _IDEMPOTENT_XADD_SCRIPT,
                2,
                dispatch.redis_stream,
                marker,
                *fields,
            )
            if isinstance(message_id, bytes):
                message_id = message_id.decode()
            if not isinstance(message_id, str) or not message_id.strip():
                raise RegisteredWorkerEventError(
                    "Redis dispatch message id is invalid"
                )
            await self._mark_dispatch_delivered(
                dispatch.id,
                message_id.strip(),
            )

    async def _mark_dispatch_delivered(
        self,
        dispatch_id: uuid.UUID,
        message_id: str,
    ) -> None:
        async with self._session_factory() as db:
            async with db.begin():
                dispatch = (
                    await db.execute(
                        select(WorkerEventDispatch)
                        .where(WorkerEventDispatch.id == dispatch_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if dispatch is None:
                    raise RegisteredWorkerEventError(
                        "worker event dispatch is missing"
                    )
                if dispatch.delivery_state == "delivered":
                    if dispatch.redis_message_id != message_id:
                        raise RegisteredWorkerEventError(
                            "worker event dispatch message mismatch"
                        )
                    return
                dispatch.delivery_state = "delivered"
                dispatch.redis_message_id = message_id
                dispatch.delivered_at = datetime.now(timezone.utc)
                await db.flush()

    @staticmethod
    async def _locked_receipt(
        db: AsyncSession,
        event: RegisteredWorkerEvent,
    ) -> RegisteredWorkerEventReceipt | None:
        return (
            await db.execute(
                select(RegisteredWorkerEventReceipt)
                .where(
                    RegisteredWorkerEventReceipt.redis_stream
                    == event.redis_stream,
                    RegisteredWorkerEventReceipt.consumer_group
                    == event.consumer_group,
                    RegisteredWorkerEventReceipt.message_id
                    == event.message_id,
                )
                .with_for_update()
            )
        ).scalar_one_or_none()

    @staticmethod
    def _require_matching_receipt(
        receipt: RegisteredWorkerEventReceipt,
        event: RegisteredWorkerEvent,
    ) -> None:
        facts = event.receipt_facts()
        for name, expected in facts.items():
            actual = getattr(receipt, name)
            if name == "worker_started_at":
                if _utc(actual) != _utc(expected):
                    break
            elif name == "payload_json":
                if canonical_redis_payload_sha256(actual) != (
                    event.payload_sha256
                ):
                    break
            elif actual != expected:
                break
        else:
            return
        raise RegisteredWorkerEventError(
            "registered event receipt fact or payload hash mismatch"
        )

    @staticmethod
    def _validated_dispatch_payload(
        dispatch: WorkerEventDispatch,
    ) -> dict[str, str]:
        payload = dispatch.payload_json
        if not isinstance(payload, dict):
            raise RegisteredWorkerEventError(
                "worker event dispatch payload is invalid"
            )
        payload_hash = canonical_redis_payload_sha256(payload)
        if payload_hash != dispatch.payload_sha256:
            raise RegisteredWorkerEventError(
                "worker event dispatch payload hash mismatch"
            )
        return dict(payload)


def _utc(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise RegisteredWorkerEventError(
            "registered event receipt timestamp is invalid"
        )
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
