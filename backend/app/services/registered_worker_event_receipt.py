from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.registered_worker_event_receipt import (
    RegisteredWorkerEventDelivery,
    RegisteredWorkerEventReceipt,
    WorkerEventEmission,
    WorkerTaskDispatch,
    WorkerTaskDeliveryAttestation,
)
from app.services.job_execution_authority import (
    NodeExecutionClaim,
    lock_job_execution_authority,
    observe_worker_event_emission,
    observe_worker_task_delivery,
    require_active_execution_authority,
    require_matching_node_execution_claim,
)


_IDEMPOTENT_XADD_SCRIPT = """
local existing = redis.call('GET', KEYS[2])
if existing then
    return existing
end
local message_id = redis.call('XADD', KEYS[1], '*', unpack(ARGV, 1))
redis.call('SET', KEYS[2], message_id)
return message_id
"""
_DISPATCH_ATTEMPT_RECOVERY_SECONDS = 60


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
    source_task_payload_sha256: str
    source_task_dispatch_key: uuid.UUID

    def receipt_facts(
        self,
        *,
        source_task_attestation_id: uuid.UUID,
    ) -> dict[str, object]:
        registration_id = self.claim.worker_registration_id
        lease_epoch = self.claim.worker_lease_epoch
        if registration_id is None or lease_epoch is None:
            raise RegisteredWorkerEventError(
                "registered event has no worker registration claim"
            )
        return {
            "source_task_attestation_id": source_task_attestation_id,
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
    source_task_payload_sha256 = payload.get("task_payload_sha256")
    if not all(
        isinstance(value, str) and value.strip()
        for value in (
            worker_id,
            started_at_raw,
            lease_epoch_raw,
            source_task_stream,
            source_task_group,
            source_task_message_id,
            source_task_payload_sha256,
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
    assert source_task_payload_sha256 is not None
    if (
        len(source_task_payload_sha256) != 64
        or any(
            character not in "0123456789abcdef"
            for character in source_task_payload_sha256
        )
    ):
        raise RegisteredWorkerEventError(
            "registered event task payload hash is invalid"
        )
    dispatch_key_raw = payload.get("task_dispatch_key")
    if not isinstance(dispatch_key_raw, str) or not dispatch_key_raw.strip():
        raise RegisteredWorkerEventError(
            "registered event task dispatch key is invalid"
        )
    try:
        source_task_dispatch_key = uuid.UUID(dispatch_key_raw)
    except (TypeError, ValueError) as exc:
        raise RegisteredWorkerEventError(
            "registered event task dispatch key is invalid"
        ) from exc
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
        source_task_payload_sha256=source_task_payload_sha256,
        source_task_dispatch_key=source_task_dispatch_key,
    )


async def stage_worker_task_dispatch(
    db: AsyncSession,
    *,
    origin_receipt_id: uuid.UUID | None,
    job_id: uuid.UUID,
    node_execution_id: uuid.UUID,
    redis_stream: str,
    consumer_group: str,
    payload: Mapping[str, str],
) -> WorkerTaskDispatch:
    if "dispatch_key" in payload:
        raise RegisteredWorkerEventError(
            "worker task payload already has a dispatch key"
        )
    if not all(
        isinstance(value, str) and value.strip()
        for value in (redis_stream, consumer_group)
    ):
        raise RegisteredWorkerEventError(
            "worker task dispatch Redis identity is invalid"
        )
    dispatch_key = uuid.uuid4()
    if origin_receipt_id is None:
        existing_initial = (
            await db.execute(
                select(WorkerTaskDispatch.id).where(
                    WorkerTaskDispatch.node_execution_id
                    == node_execution_id,
                    WorkerTaskDispatch.origin_receipt_id.is_(None),
                    WorkerTaskDispatch.resolution_state.in_(
                        ("unresolved", "cancel_authorized")
                    ),
                )
            )
        ).scalar_one_or_none()
        if existing_initial is not None:
            raise RegisteredWorkerEventError(
                "node already has an unresolved initial dispatch"
            )
    dispatched_payload = {
        **dict(payload),
        "dispatch_key": str(dispatch_key),
    }
    dispatch = WorkerTaskDispatch(
        origin_receipt_id=origin_receipt_id,
        dispatch_key=dispatch_key,
        job_id=job_id,
        node_execution_id=node_execution_id,
        redis_stream=redis_stream.strip(),
        consumer_group=consumer_group.strip(),
        payload_sha256=canonical_redis_payload_sha256(dispatched_payload),
        payload_json=dispatched_payload,
        delivery_state="pending",
    )
    db.add(dispatch)
    await db.flush()
    return dispatch


class RegisteredWorkerEventReceiptService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        authority_locker: Callable[..., Awaitable[Any]] = (
            lock_job_execution_authority
        ),
        delivery_observer: Callable[
            [AsyncSession, RegisteredWorkerEvent],
            Awaitable[uuid.UUID],
        ] = (
            lambda db, event: _observe_registered_worker_task_delivery(
                db,
                event,
            )
        ),
        dispatch_attempt_recovery_seconds: int = (
            _DISPATCH_ATTEMPT_RECOVERY_SECONDS
        ),
    ) -> None:
        if (
            type(dispatch_attempt_recovery_seconds) is not int
            or dispatch_attempt_recovery_seconds < 0
        ):
            raise ValueError(
                "dispatch attempt recovery seconds must be non-negative"
            )
        self._session_factory = session_factory
        self._authority_locker = authority_locker
        self._delivery_observer = delivery_observer
        self._dispatch_attempt_recovery_seconds = (
            dispatch_attempt_recovery_seconds
        )

    async def accept_and_apply(
        self,
        event: RegisteredWorkerEvent,
        apply_event: Callable[
            [AsyncSession, RegisteredWorkerEventReceipt, RegisteredWorkerEvent],
            Awaitable[None],
        ],
    ) -> uuid.UUID | None:
        async with self._session_factory() as db:
            async with db.begin():
                authority = await self._authority_locker(
                    db,
                    event.job_id,
                    node_execution_id=event.node_execution_id,
                    lock_all_nodes=True,
                )
                registration_id = event.claim.worker_registration_id
                if not isinstance(registration_id, uuid.UUID):
                    raise RegisteredWorkerEventError(
                        "registered event worker registration is invalid"
                    )
                await self._lock_registration_fence(
                    db,
                    registration_id,
                )
                attestation = await self._locked_task_attestation(db, event)
                observed_attestation_id: uuid.UUID | None = None
                if attestation is None:
                    observed_attestation_id = await self._delivery_observer(
                        db,
                        event,
                    )
                    attestation_id = observed_attestation_id
                else:
                    attestation_id = attestation.id
                await self._lock_attestation_identity(db, attestation_id)
                await self._lock_event_identity(db, event)
                existing_delivery = await self._locked_event_delivery(
                    db,
                    event,
                )
                if existing_delivery is not None:
                    self._require_matching_event_delivery_identity(
                        existing_delivery,
                        event,
                    )
                    if existing_delivery.resolution_state == "quarantined":
                        return None
                    delivery_receipt = await self._receipt_for_delivery(
                        db,
                        existing_delivery,
                    )
                    self._require_matching_delivery(
                        existing_delivery,
                        delivery_receipt,
                        event,
                    )
                    if delivery_receipt.application_state != "applied":
                        raise RegisteredWorkerEventError(
                            "registered event receipt is not durably applied"
                        )
                    return delivery_receipt.id

                if attestation is not None:
                    mismatch_reason = self._attestation_mismatch_reason(
                        attestation,
                        event,
                    )
                    if mismatch_reason is not None:
                        existing = await self._locked_receipt_for_attestation(
                            db,
                            attestation_id,
                        )
                        await self._add_event_delivery(
                            db,
                            receipt_id=(
                                existing.id if existing is not None else None
                            ),
                            attestation_id=attestation_id,
                            event=event,
                            resolution_state="quarantined",
                            reason_code=mismatch_reason,
                        )
                        return None
                existing = await self._locked_receipt_for_attestation(
                    db,
                    attestation_id,
                )
                if existing is not None:
                    mismatch_reason = self._source_receipt_mismatch_reason(
                        existing,
                        event,
                        attestation_id,
                    )
                    if mismatch_reason is not None:
                        await self._add_event_delivery(
                            db,
                            receipt_id=existing.id,
                            attestation_id=attestation_id,
                            event=event,
                            resolution_state="quarantined",
                            reason_code=mismatch_reason,
                        )
                        return None
                    if existing.application_state != "applied":
                        raise RegisteredWorkerEventError(
                            "registered event receipt is not durably applied"
                        )
                    await self._add_event_delivery(
                        db,
                        receipt_id=existing.id,
                        attestation_id=attestation_id,
                        event=event,
                    )
                    return existing.id

                if observed_attestation_id is None:
                    observed_attestation_id = await self._delivery_observer(
                        db,
                        event,
                    )
                if observed_attestation_id != attestation_id:
                    raise RegisteredWorkerEventError(
                        "registered event task delivery attestation mismatch"
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

                existing = await self._locked_receipt_for_attestation(
                    db,
                    attestation_id,
                )
                if existing is not None:
                    mismatch_reason = self._source_receipt_mismatch_reason(
                        existing,
                        event,
                        attestation_id,
                    )
                    if mismatch_reason is not None:
                        await self._add_event_delivery(
                            db,
                            receipt_id=existing.id,
                            attestation_id=attestation_id,
                            event=event,
                            resolution_state="quarantined",
                            reason_code=mismatch_reason,
                        )
                        return None
                    if existing.application_state != "applied":
                        raise RegisteredWorkerEventError(
                            "registered event receipt is not durably applied"
                        )
                    await self._add_event_delivery(
                        db,
                        receipt_id=existing.id,
                        attestation_id=attestation_id,
                        event=event,
                    )
                    return existing.id

                receipt = RegisteredWorkerEventReceipt(
                    **event.receipt_facts(
                        source_task_attestation_id=attestation_id,
                    ),
                    application_state="accepted",
                    ack_state="pending",
                    source_task_ack_state="pending",
                )
                db.add(receipt)
                await db.flush()
                await self._add_event_delivery(
                    db,
                    receipt_id=receipt.id,
                    attestation_id=attestation_id,
                    event=event,
                )
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
                delivery_id = (
                    await db.execute(
                        select(RegisteredWorkerEventDelivery.id).where(
                            RegisteredWorkerEventDelivery.redis_stream
                            == event.redis_stream,
                            RegisteredWorkerEventDelivery.consumer_group
                            == event.consumer_group,
                            RegisteredWorkerEventDelivery.message_id
                            == event.message_id,
                        )
                    )
                ).scalar_one_or_none()
                if delivery_id is None:
                    raise RegisteredWorkerEventError(
                        "registered event delivery is missing"
                    )
                attestation, emission, receipt, delivery = (
                    await self._lock_delivery_ack_authority(
                        db,
                        delivery_id,
                    )
                )
                self._require_matching_event_delivery_identity(
                    delivery,
                    event,
                )
                if delivery.resolution_state != "accepted":
                    receipt = None
                elif receipt is None:
                    raise RegisteredWorkerEventError(
                        "accepted registered event delivery has no receipt"
                    )
                if receipt is not None:
                    self._require_matching_delivery(
                        delivery,
                        receipt,
                        event,
                    )
                if (
                    receipt is not None
                    and receipt.application_state != "applied"
                ):
                    raise RegisteredWorkerEventError(
                        "registered event receipt is not durably applied"
                    )
                await self._acknowledge_locked_delivery(
                    db,
                    redis,
                    attestation,
                    emission,
                    receipt,
                    delivery,
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
            delivery_ids = list(
                (
                    await db.execute(
                        select(RegisteredWorkerEventDelivery.id)
                        .join(
                            RegisteredWorkerEventReceipt,
                            RegisteredWorkerEventReceipt.id
                            == RegisteredWorkerEventDelivery.receipt_id,
                            isouter=True,
                        )
                        .where(
                            RegisteredWorkerEventDelivery.ack_state
                            != "acknowledged",
                            or_(
                                RegisteredWorkerEventDelivery.resolution_state
                                == "quarantined",
                                RegisteredWorkerEventReceipt.application_state
                                == "applied",
                            ),
                        )
                        .order_by(
                            RegisteredWorkerEventDelivery.accepted_at
                        )
                        .limit(limit)
                    )
                ).scalars()
            )
        for delivery_id in delivery_ids:
            await self._acknowledge_delivery_id(redis, delivery_id)

    async def reconcile_authorized_task_acknowledgements(
        self,
        redis: Any,
        *,
        limit: int = 100,
    ) -> None:
        if type(limit) is not int or limit <= 0:
            raise ValueError("limit must be positive")
        async with self._session_factory() as db:
            attestation_ids = list(
                (
                    await db.execute(
                        select(WorkerTaskDeliveryAttestation.id)
                        .where(
                            WorkerTaskDeliveryAttestation.ack_state
                            == "authorized"
                        )
                        .order_by(
                            WorkerTaskDeliveryAttestation.attested_at
                        )
                        .limit(limit)
                    )
                ).scalars()
            )
        for attestation_id in attestation_ids:
            await self._acknowledge_authorized_task(
                redis,
                attestation_id,
            )

    async def _acknowledge_authorized_task(
        self,
        redis: Any,
        attestation_id: uuid.UUID,
    ) -> None:
        async with self._session_factory() as db:
            async with db.begin():
                identity = (
                    await db.execute(
                        select(
                            WorkerTaskDeliveryAttestation.job_id,
                            WorkerTaskDeliveryAttestation.node_execution_id,
                            WorkerTaskDeliveryAttestation.worker_registration_id,
                        )
                        .where(
                            WorkerTaskDeliveryAttestation.id
                            == attestation_id
                        )
                    )
                ).one_or_none()
                if identity is None:
                    return
                await self._lock_job_node_registration(
                    db,
                    job_id=identity.job_id,
                    node_execution_id=identity.node_execution_id,
                    registration_id=identity.worker_registration_id,
                )
                attestation = (
                    await db.execute(
                        select(WorkerTaskDeliveryAttestation)
                        .where(
                            WorkerTaskDeliveryAttestation.id
                            == attestation_id
                        )
                        .with_for_update()
                    )
                ).scalar_one()
                if attestation.ack_state == "acknowledged":
                    return
                if attestation.ack_state != "authorized":
                    raise RegisteredWorkerEventError(
                        "worker task acknowledgement is not authorized"
                    )
                await self._lock_emission_for_attestation(
                    db,
                    attestation.id,
                )
                await self._lock_receipts_and_deliveries_for_attestation(
                    db,
                    attestation.id,
                )
                await self._lock_dispatch_for_attestation(db, attestation)
                result = await redis.xack(
                    attestation.redis_stream,
                    attestation.consumer_group,
                    attestation.message_id,
                )
                self._require_xack_result(result, label="worker task")
                acknowledged_at = datetime.now(timezone.utc)
                await self._mark_proven_task_acknowledged(
                    db,
                    attestation,
                    acknowledged_at,
                )
                await db.flush()

    async def _acknowledge_delivery_id(
        self,
        redis: Any,
        delivery_id: uuid.UUID,
    ) -> None:
        async with self._session_factory() as db:
            async with db.begin():
                attestation, emission, receipt, delivery = (
                    await self._lock_delivery_ack_authority(
                        db,
                        delivery_id,
                    )
                )
                if delivery.resolution_state != "accepted":
                    receipt = None
                if (
                    receipt is not None
                    and receipt.application_state != "applied"
                ):
                    raise RegisteredWorkerEventError(
                        "registered event receipt is not durably applied"
                    )
                await self._acknowledge_locked_delivery(
                    db,
                    redis,
                    attestation,
                    emission,
                    receipt,
                    delivery,
                )

    @staticmethod
    async def _acknowledge_locked_delivery(
        db: AsyncSession,
        redis: Any,
        attestation: WorkerTaskDeliveryAttestation,
        emission: WorkerEventEmission | None,
        receipt: RegisteredWorkerEventReceipt | None,
        delivery: RegisteredWorkerEventDelivery,
    ) -> None:
        if receipt is not None:
            if (
                receipt.source_task_attestation_id
                != delivery.source_task_attestation_id
            ):
                raise RegisteredWorkerEventError(
                    "registered event task attestation mismatch"
                )

        if receipt is not None and attestation.ack_state != "acknowledged":
            task_acknowledged = await redis.xack(
                attestation.redis_stream,
                attestation.consumer_group,
                attestation.message_id,
            )
            RegisteredWorkerEventReceiptService._require_xack_result(
                task_acknowledged,
                label="worker task",
            )
        if delivery.ack_state != "acknowledged":
            event_acknowledged = await redis.xack(
                delivery.redis_stream,
                delivery.consumer_group,
                delivery.message_id,
            )
            RegisteredWorkerEventReceiptService._require_xack_result(
                event_acknowledged,
                label="worker event",
            )

        acknowledged_at = datetime.now(timezone.utc)
        task_acknowledged_at = attestation.acknowledged_at
        if receipt is not None and attestation.ack_state != "acknowledged":
            task_acknowledged_at = await (
                RegisteredWorkerEventReceiptService
                ._mark_proven_task_acknowledged(
                    db,
                    attestation,
                    acknowledged_at,
                )
            )
        if receipt is not None:
            receipt.source_task_ack_state = "acknowledged"
            receipt.source_task_acknowledged_at = (
                task_acknowledged_at
                if task_acknowledged_at is not None
                else acknowledged_at
            )
        if delivery.ack_state != "acknowledged":
            delivery.ack_state = "acknowledged"
            delivery.acknowledged_at = acknowledged_at
            if (
                receipt is not None
                and
                receipt.redis_stream == delivery.redis_stream
                and receipt.consumer_group == delivery.consumer_group
                and receipt.message_id == delivery.message_id
            ):
                receipt.ack_state = "acknowledged"
                receipt.acknowledged_at = acknowledged_at
        if (
            emission is not None
            and emission.redis_stream == delivery.redis_stream
            and emission.consumer_group == delivery.consumer_group
            and emission.message_id == delivery.message_id
            and emission.payload_sha256 == delivery.payload_sha256
            and emission.emission_state == "emitted"
        ):
            emission.emission_state = "resolved"
            emission.resolved_at = acknowledged_at
        await db.flush()

    @classmethod
    async def _lock_delivery_ack_authority(
        cls,
        db: AsyncSession,
        delivery_id: uuid.UUID,
    ) -> tuple[
        WorkerTaskDeliveryAttestation,
        WorkerEventEmission | None,
        RegisteredWorkerEventReceipt | None,
        RegisteredWorkerEventDelivery,
    ]:
        identity = (
            await db.execute(
                select(
                    RegisteredWorkerEventDelivery.source_task_attestation_id,
                    RegisteredWorkerEventDelivery.receipt_id,
                    WorkerTaskDeliveryAttestation.job_id,
                    WorkerTaskDeliveryAttestation.node_execution_id,
                    WorkerTaskDeliveryAttestation.worker_registration_id,
                )
                .join(
                    WorkerTaskDeliveryAttestation,
                    WorkerTaskDeliveryAttestation.id
                    == RegisteredWorkerEventDelivery.source_task_attestation_id,
                )
                .where(RegisteredWorkerEventDelivery.id == delivery_id)
            )
        ).one_or_none()
        if identity is None:
            raise RegisteredWorkerEventError(
                "registered event delivery is missing"
            )
        await cls._lock_job_node_registration(
            db,
            job_id=identity.job_id,
            node_execution_id=identity.node_execution_id,
            registration_id=identity.worker_registration_id,
        )
        attestation = (
            await db.execute(
                select(WorkerTaskDeliveryAttestation)
                .where(
                    WorkerTaskDeliveryAttestation.id
                    == identity.source_task_attestation_id
                )
                .with_for_update()
            )
        ).scalar_one()
        emission = await cls._lock_emission_for_attestation(
            db,
            attestation.id,
        )
        receipts, deliveries = (
            await cls._lock_receipts_and_deliveries_for_attestation(
                db,
                attestation.id,
            )
        )
        receipt = next(
            (
                candidate
                for candidate in receipts
                if candidate.id == identity.receipt_id
            ),
            None,
        )
        if identity.receipt_id is not None and receipt is None:
            raise RegisteredWorkerEventError(
                "registered event receipt is missing"
            )
        delivery = next(
            (
                candidate
                for candidate in deliveries
                if candidate.id == delivery_id
            ),
            None,
        )
        if delivery is None:
            raise RegisteredWorkerEventError(
                "registered event delivery is missing"
            )
        await cls._lock_dispatch_for_attestation(db, attestation)
        return attestation, emission, receipt, delivery

    @classmethod
    async def _lock_job_node_registration(
        cls,
        db: AsyncSession,
        *,
        job_id: uuid.UUID,
        node_execution_id: uuid.UUID,
        registration_id: uuid.UUID,
    ) -> None:
        job = (
            await db.execute(
                select(Job).where(Job.id == job_id).with_for_update()
            )
        ).scalar_one_or_none()
        node = (
            await db.execute(
                select(NodeExecution)
                .where(
                    NodeExecution.id == node_execution_id,
                    NodeExecution.job_id == job_id,
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if (
            (job is None or node is None)
            and db.get_bind().dialect.name == "postgresql"
        ):
            raise RegisteredWorkerEventError(
                "worker event job or node authority is missing"
            )
        await cls._lock_registration_fence(db, registration_id)

    @staticmethod
    async def _lock_emission_for_attestation(
        db: AsyncSession,
        attestation_id: uuid.UUID,
    ) -> WorkerEventEmission | None:
        if db.get_bind().dialect.name != "postgresql":
            return None
        return (
            await db.execute(
                select(WorkerEventEmission)
                .where(
                    WorkerEventEmission.source_task_attestation_id
                    == attestation_id
                )
                .with_for_update()
            )
        ).scalar_one_or_none()

    @staticmethod
    async def _lock_receipts_and_deliveries_for_attestation(
        db: AsyncSession,
        attestation_id: uuid.UUID,
    ) -> tuple[
        list[RegisteredWorkerEventReceipt],
        list[RegisteredWorkerEventDelivery],
    ]:
        receipts = list(
            (
                await db.execute(
                    select(RegisteredWorkerEventReceipt)
                    .where(
                        RegisteredWorkerEventReceipt
                        .source_task_attestation_id
                        == attestation_id
                    )
                    .order_by(RegisteredWorkerEventReceipt.id)
                    .with_for_update()
                )
            ).scalars()
        )
        deliveries = list(
            (
                await db.execute(
                    select(RegisteredWorkerEventDelivery)
                    .where(
                        RegisteredWorkerEventDelivery
                        .source_task_attestation_id
                        == attestation_id
                    )
                    .order_by(RegisteredWorkerEventDelivery.id)
                    .with_for_update()
                )
            ).scalars()
        )
        return receipts, deliveries

    @staticmethod
    async def _lock_dispatch_for_attestation(
        db: AsyncSession,
        attestation: WorkerTaskDeliveryAttestation,
    ) -> None:
        dispatch = (
            await db.execute(
                select(WorkerTaskDispatch)
                .where(
                    WorkerTaskDispatch.dispatch_key
                    == attestation.dispatch_key,
                    WorkerTaskDispatch.redis_stream
                    == attestation.redis_stream,
                    WorkerTaskDispatch.consumer_group
                    == attestation.consumer_group,
                    WorkerTaskDispatch.redis_message_id
                    == attestation.message_id,
                    WorkerTaskDispatch.payload_sha256
                    == attestation.payload_sha256,
                    WorkerTaskDispatch.job_id == attestation.job_id,
                    WorkerTaskDispatch.node_execution_id
                    == attestation.node_execution_id,
                    WorkerTaskDispatch.delivery_state == "delivered",
                    WorkerTaskDispatch.resolution_state.in_(
                        (
                            "unresolved",
                            "cancel_authorized",
                            "acknowledged",
                        )
                    ),
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if dispatch is None:
            if db.get_bind().dialect.name != "postgresql":
                return
            raise RegisteredWorkerEventError(
                "worker task dispatch acknowledgement identity mismatch"
            )

    @staticmethod
    async def _mark_proven_task_acknowledged(
        db: AsyncSession,
        attestation: WorkerTaskDeliveryAttestation,
        acknowledged_at: datetime,
    ) -> datetime:
        if db.get_bind().dialect.name == "postgresql":
            result = await db.execute(
                text(
                    "SELECT public."
                    "vp_acknowledge_proven_worker_task_dispatch("
                    ":attestation_id)"
                ),
                {"attestation_id": attestation.id},
            )
            persisted_at = result.scalar_one()
            if not isinstance(persisted_at, datetime):
                raise RegisteredWorkerEventError(
                    "worker task acknowledgement timestamp is invalid"
                )
            return persisted_at
        attestation.ack_state = "acknowledged"
        attestation.acknowledged_at = acknowledged_at
        dispatch = (
            await db.execute(
                select(WorkerTaskDispatch)
                .where(
                    WorkerTaskDispatch.dispatch_key
                    == attestation.dispatch_key,
                    WorkerTaskDispatch.redis_stream
                    == attestation.redis_stream,
                    WorkerTaskDispatch.consumer_group
                    == attestation.consumer_group,
                    WorkerTaskDispatch.redis_message_id
                    == attestation.message_id,
                    WorkerTaskDispatch.payload_sha256
                    == attestation.payload_sha256,
                    WorkerTaskDispatch.job_id == attestation.job_id,
                    WorkerTaskDispatch.node_execution_id
                    == attestation.node_execution_id,
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if dispatch is None or dispatch.delivery_state != "delivered":
            raise RegisteredWorkerEventError(
                "worker task dispatch acknowledgement identity mismatch"
            )
        if dispatch.resolution_state == "cancelled":
            raise RegisteredWorkerEventError(
                "cancelled-undelivered dispatch cannot be acknowledged"
            )
        if dispatch.resolution_state != "acknowledged":
            dispatch.resolution_state = "acknowledged"
            dispatch.acknowledged_at = acknowledged_at
        return acknowledged_at

    @staticmethod
    def _require_xack_result(result: object, *, label: str) -> None:
        if type(result) is not int or result not in {0, 1}:
            raise RegisteredWorkerEventError(
                f"Redis {label} acknowledgement result is invalid"
            )

    async def deliver_pending_dispatches(
        self,
        redis: Any,
        receipt_id: uuid.UUID | None = None,
        *,
        limit: int = 100,
    ) -> None:
        if type(limit) is not int or limit <= 0:
            raise ValueError("limit must be positive")
        await self._recover_stale_dispatch_attempts(
            redis,
            receipt_id=receipt_id,
            limit=limit,
        )
        dispatches = await self._begin_dispatch_attempts(
            receipt_id=receipt_id,
            limit=limit,
        )

        for dispatch in dispatches:
            payload = self._validated_dispatch_payload(dispatch)
            fields: list[str] = []
            for key, value in sorted(payload.items()):
                fields.extend((key, value))
            marker = f"vp:worker-task-dispatch:{dispatch.dispatch_key}"
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

    async def _begin_dispatch_attempts(
        self,
        *,
        receipt_id: uuid.UUID | None,
        limit: int,
    ) -> list[WorkerTaskDispatch]:
        async with self._session_factory() as db:
            async with db.begin():
                if receipt_id is not None:
                    receipt = await db.get(
                        RegisteredWorkerEventReceipt,
                        receipt_id,
                    )
                    if (
                        receipt is None
                        or receipt.application_state != "applied"
                    ):
                        raise RegisteredWorkerEventError(
                            "dispatch has no applied event receipt"
                        )
                statement = (
                    select(WorkerTaskDispatch)
                    .where(WorkerTaskDispatch.delivery_state == "pending")
                    .order_by(WorkerTaskDispatch.created_at)
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                )
                if receipt_id is not None:
                    statement = statement.where(
                        WorkerTaskDispatch.origin_receipt_id == receipt_id
                    )
                dispatches = list(
                    (await db.execute(statement)).scalars()
                )
                attempted_at = datetime.now(timezone.utc)
                for dispatch in dispatches:
                    dispatch.delivery_state = "attempting"
                    dispatch.delivery_attempted_at = attempted_at
                    dispatch.delivery_error = None
                await db.flush()
                return dispatches

    async def _recover_stale_dispatch_attempts(
        self,
        redis: Any,
        *,
        receipt_id: uuid.UUID | None,
        limit: int,
    ) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(
            seconds=self._dispatch_attempt_recovery_seconds
        )
        async with self._session_factory() as db:
            statement = (
                select(WorkerTaskDispatch)
                .where(
                    WorkerTaskDispatch.delivery_state == "attempting",
                    WorkerTaskDispatch.delivery_attempted_at <= cutoff,
                )
                .order_by(WorkerTaskDispatch.delivery_attempted_at)
                .limit(limit)
            )
            if receipt_id is not None:
                statement = statement.where(
                    WorkerTaskDispatch.origin_receipt_id == receipt_id
                )
            attempts = list((await db.execute(statement)).scalars())

        for dispatch in attempts:
            marker = f"vp:worker-task-dispatch:{dispatch.dispatch_key}"
            message_id = await redis.get(marker)
            if isinstance(message_id, bytes):
                message_id = message_id.decode()
            if isinstance(message_id, str) and message_id.strip():
                await self._mark_dispatch_delivered(
                    dispatch.id,
                    message_id.strip(),
                )
                continue
            await self._mark_dispatch_uncertain(
                dispatch.id,
                "dispatch_marker_missing_after_attempt",
            )

    async def reconcile_pending_dispatches(
        self,
        redis: Any,
        *,
        limit: int = 100,
    ) -> None:
        await self.deliver_pending_dispatches(redis, limit=limit)

    async def reconcile_cancelled_dispatches(
        self,
        redis: Any,
        *,
        limit: int = 100,
    ) -> None:
        if type(limit) is not int or limit <= 0:
            raise ValueError("limit must be positive")
        await self._recover_stale_dispatch_attempts(
            redis,
            receipt_id=None,
            limit=limit,
        )
        await self._cancel_pending_dispatches(limit=limit)
        dispatch_ids = await self._authorize_cancelled_dispatches(
            limit=limit,
        )
        for dispatch_id in dispatch_ids:
            await self._acknowledge_cancelled_dispatch(redis, dispatch_id)

    async def _cancel_pending_dispatches(self, *, limit: int) -> None:
        async with self._session_factory() as db:
            async with db.begin():
                is_postgresql = (
                    db.get_bind().dialect.name == "postgresql"
                )
                statement = (
                    select(WorkerTaskDispatch)
                    .join(
                        Job,
                        Job.id == WorkerTaskDispatch.job_id,
                    )
                    .join(
                        NodeExecution,
                        NodeExecution.id
                        == WorkerTaskDispatch.node_execution_id,
                    )
                    .where(
                        WorkerTaskDispatch.delivery_state == "pending",
                        WorkerTaskDispatch.resolution_state
                        == "unresolved",
                        Job.status == JobStatus.CANCELLED,
                        NodeExecution.status == NodeStatus.CANCELLED,
                    )
                    .order_by(WorkerTaskDispatch.created_at)
                    .limit(limit)
                )
                if not is_postgresql:
                    statement = statement.with_for_update(
                        skip_locked=True
                    )
                dispatches = list(
                    (await db.execute(statement)).scalars()
                )
                if is_postgresql:
                    for dispatch in dispatches:
                        await db.execute(
                            text(
                                "SELECT public."
                                "vp_authorize_cancelled_worker_task_ack("
                                ":dispatch_id)"
                            ),
                            {"dispatch_id": dispatch.id},
                        )
                    return
                cancelled_at = datetime.now(timezone.utc)
                for dispatch in dispatches:
                    dispatch.delivery_state = "cancelled"
                    dispatch.resolution_state = "cancelled"
                    dispatch.cancelled_at = cancelled_at
                await db.flush()

    async def _authorize_cancelled_dispatches(
        self,
        *,
        limit: int,
    ) -> list[uuid.UUID]:
        async with self._session_factory() as db:
            async with db.begin():
                is_postgresql = (
                    db.get_bind().dialect.name == "postgresql"
                )
                statement = (
                    select(WorkerTaskDispatch)
                    .join(
                        Job,
                        Job.id == WorkerTaskDispatch.job_id,
                    )
                    .join(
                        NodeExecution,
                        NodeExecution.id
                        == WorkerTaskDispatch.node_execution_id,
                    )
                    .where(
                        WorkerTaskDispatch.delivery_state == "delivered",
                        WorkerTaskDispatch.resolution_state.in_(
                            ("unresolved", "cancel_authorized")
                        ),
                        Job.status == JobStatus.CANCELLED,
                        NodeExecution.status == NodeStatus.CANCELLED,
                    )
                    .order_by(WorkerTaskDispatch.created_at)
                    .limit(limit)
                )
                if not is_postgresql:
                    statement = statement.with_for_update(
                        skip_locked=True
                    )
                dispatches = list(
                    (await db.execute(statement)).scalars()
                )
                if is_postgresql:
                    authorized_ids: list[uuid.UUID] = []
                    for dispatch in dispatches:
                        state = (
                            await db.execute(
                                text(
                                    "SELECT public."
                                    "vp_authorize_cancelled_worker_task_ack("
                                    ":dispatch_id)"
                                ),
                                {"dispatch_id": dispatch.id},
                            )
                        ).scalar_one()
                        if state == "cancel_authorized":
                            authorized_ids.append(dispatch.id)
                    return authorized_ids
                for dispatch in dispatches:
                    if dispatch.resolution_state == "unresolved":
                        dispatch.resolution_state = "cancel_authorized"
                await db.flush()
                return [dispatch.id for dispatch in dispatches]

    async def _acknowledge_cancelled_dispatch(
        self,
        redis: Any,
        dispatch_id: uuid.UUID,
    ) -> None:
        async with self._session_factory() as db:
            async with db.begin():
                is_postgresql = (
                    db.get_bind().dialect.name == "postgresql"
                )
                statement = select(WorkerTaskDispatch).where(
                    WorkerTaskDispatch.id == dispatch_id
                )
                if not is_postgresql:
                    statement = statement.with_for_update()
                dispatch = (
                    await db.execute(statement)
                ).scalar_one_or_none()
                if dispatch is None:
                    return
                if dispatch.resolution_state == "acknowledged":
                    return
                if (
                    dispatch.delivery_state != "delivered"
                    or dispatch.resolution_state != "cancel_authorized"
                    or dispatch.redis_message_id is None
                ):
                    raise RegisteredWorkerEventError(
                        "cancelled worker task acknowledgement is not authorized"
                    )
                if is_postgresql:
                    identity = {
                        "dispatch_id": dispatch.id,
                        "redis_stream": dispatch.redis_stream,
                        "consumer_group": dispatch.consumer_group,
                        "message_id": dispatch.redis_message_id,
                        "payload_sha256": dispatch.payload_sha256,
                        "dispatch_key": dispatch.dispatch_key,
                    }
                    function_arguments = (
                        ":dispatch_id, :redis_stream, :consumer_group, "
                        ":message_id, :payload_sha256, :dispatch_key"
                    )
                    await db.execute(
                        text(
                            "SELECT public."
                            "vp_require_cancelled_worker_task_ack("
                            f"{function_arguments})"
                        ),
                        identity,
                    )
                    result = await redis.xack(
                        dispatch.redis_stream,
                        dispatch.consumer_group,
                        dispatch.redis_message_id,
                    )
                    self._require_xack_result(
                        result,
                        label="cancelled worker task",
                    )
                    await db.execute(
                        text(
                            "SELECT public."
                            "vp_acknowledge_cancelled_worker_task("
                            f"{function_arguments})"
                        ),
                        identity,
                    )
                    return
                job = (
                    await db.execute(
                        select(Job)
                        .where(Job.id == dispatch.job_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                node = (
                    await db.execute(
                        select(NodeExecution)
                        .where(
                            NodeExecution.id
                            == dispatch.node_execution_id
                        )
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if (
                    job is None
                    or node is None
                    or job.status != JobStatus.CANCELLED
                    or node.status != NodeStatus.CANCELLED
                ):
                    raise RegisteredWorkerEventError(
                        "worker task cancellation authority changed"
                    )
                attestations = list(
                    (
                        await db.execute(
                            select(WorkerTaskDeliveryAttestation)
                            .where(
                                WorkerTaskDeliveryAttestation.dispatch_key
                                == dispatch.dispatch_key
                            )
                            .with_for_update()
                        )
                    ).scalars()
                )
                for registration_id in sorted(
                    {
                        attestation.worker_registration_id
                        for attestation in attestations
                    },
                    key=str,
                ):
                    await self._lock_registration_fence(
                        db,
                        registration_id,
                    )
                result = await redis.xack(
                    dispatch.redis_stream,
                    dispatch.consumer_group,
                    dispatch.redis_message_id,
                )
                self._require_xack_result(result, label="cancelled worker task")
                await self._mark_cancelled_dispatch_acknowledged(
                    db,
                    dispatch,
                    attestations,
                )

    @staticmethod
    async def _mark_cancelled_dispatch_acknowledged(
        db: AsyncSession,
        dispatch: WorkerTaskDispatch,
        attestations: list[WorkerTaskDeliveryAttestation],
    ) -> None:
        acknowledged_at = datetime.now(timezone.utc)
        dispatch.resolution_state = "acknowledged"
        dispatch.acknowledged_at = acknowledged_at
        for attestation in attestations:
            if attestation.ack_state != "acknowledged":
                attestation.ack_state = "acknowledged"
                attestation.acknowledged_at = acknowledged_at
        await db.flush()

    async def _mark_dispatch_delivered(
        self,
        dispatch_id: uuid.UUID,
        message_id: str,
    ) -> None:
        async with self._session_factory() as db:
            async with db.begin():
                dispatch = (
                    await db.execute(
                        select(WorkerTaskDispatch)
                        .where(WorkerTaskDispatch.id == dispatch_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if dispatch is None:
                    raise RegisteredWorkerEventError(
                        "worker task dispatch is missing"
                    )
                if dispatch.delivery_state == "delivered":
                    if dispatch.redis_message_id != message_id:
                        raise RegisteredWorkerEventError(
                            "worker task dispatch message mismatch"
                        )
                    return
                if dispatch.delivery_state != "attempting":
                    raise RegisteredWorkerEventError(
                        "worker task dispatch is not attempting delivery"
                    )
                dispatch.delivery_state = "delivered"
                dispatch.redis_message_id = message_id
                dispatch.delivered_at = datetime.now(timezone.utc)
                await db.flush()

    async def _mark_dispatch_uncertain(
        self,
        dispatch_id: uuid.UUID,
        reason: str,
    ) -> None:
        async with self._session_factory() as db:
            async with db.begin():
                dispatch = (
                    await db.execute(
                        select(WorkerTaskDispatch)
                        .where(WorkerTaskDispatch.id == dispatch_id)
                        .with_for_update()
                    )
                ).scalar_one_or_none()
                if dispatch is None:
                    raise RegisteredWorkerEventError(
                        "worker task dispatch is missing"
                    )
                if dispatch.delivery_state == "delivered":
                    return
                if dispatch.delivery_state != "attempting":
                    raise RegisteredWorkerEventError(
                        "worker task dispatch is not attempting delivery"
                    )
                dispatch.delivery_state = "uncertain"
                dispatch.delivery_error = reason
                await db.flush()

    @staticmethod
    async def _lock_event_identity(
        db: AsyncSession,
        event: RegisteredWorkerEvent,
    ) -> None:
        await RegisteredWorkerEventReceiptService._postgres_advisory_lock(
            db,
            "vp-worker-event:"
            + json.dumps(
                [
                    event.redis_stream,
                    event.consumer_group,
                    event.message_id,
                ],
                separators=(",", ":"),
            ),
        )

    @staticmethod
    async def _lock_attestation_identity(
        db: AsyncSession,
        attestation_id: uuid.UUID,
    ) -> None:
        await RegisteredWorkerEventReceiptService._postgres_advisory_lock(
            db,
            f"vp-worker-task-attestation:{attestation_id}",
        )

    @staticmethod
    async def _postgres_advisory_lock(
        db: AsyncSession,
        identity: str,
    ) -> None:
        bind = db.get_bind()
        if bind.dialect.name != "postgresql":
            return
        await db.execute(
            text(
                "SELECT pg_catalog.pg_advisory_xact_lock("
                "pg_catalog.hashtextextended(:identity, 0))"
            ),
            {"identity": identity},
        )

    @staticmethod
    async def _lock_registration_fence(
        db: AsyncSession,
        registration_id: uuid.UUID,
    ) -> None:
        if db.get_bind().dialect.name != "postgresql":
            return
        await db.execute(
            text(
                "SELECT pg_catalog.pg_advisory_xact_lock_shared("
                "pg_catalog.hashtextextended("
                "'vp-worker-registration:' || CAST(:registration_id AS text),"
                "0))"
            ),
            {"registration_id": str(registration_id)},
        )

    @staticmethod
    async def _locked_event_delivery(
        db: AsyncSession,
        event: RegisteredWorkerEvent,
    ) -> RegisteredWorkerEventDelivery | None:
        return (
            await db.execute(
                select(RegisteredWorkerEventDelivery)
                .where(
                    RegisteredWorkerEventDelivery.redis_stream
                    == event.redis_stream,
                    RegisteredWorkerEventDelivery.consumer_group
                    == event.consumer_group,
                    RegisteredWorkerEventDelivery.message_id
                    == event.message_id,
                )
                .with_for_update()
            )
        ).scalar_one_or_none()

    @staticmethod
    async def _locked_task_attestation(
        db: AsyncSession,
        event: RegisteredWorkerEvent,
    ) -> WorkerTaskDeliveryAttestation | None:
        return (
            await db.execute(
                select(WorkerTaskDeliveryAttestation)
                .where(
                    WorkerTaskDeliveryAttestation.redis_stream
                    == event.source_task_stream,
                    WorkerTaskDeliveryAttestation.consumer_group
                    == event.source_task_group,
                    WorkerTaskDeliveryAttestation.message_id
                    == event.source_task_message_id,
                    WorkerTaskDeliveryAttestation.dispatch_key
                    == event.source_task_dispatch_key,
                )
                .with_for_update()
            )
        ).scalar_one_or_none()

    @staticmethod
    def _attestation_mismatch_reason(
        attestation: WorkerTaskDeliveryAttestation,
        event: RegisteredWorkerEvent,
    ) -> str | None:
        if (
            attestation.payload_sha256
            != event.source_task_payload_sha256
        ):
            return "source_task_payload_mismatch"
        if (
            attestation.job_id != event.job_id
            or attestation.node_execution_id != event.node_execution_id
        ):
            return "source_task_authority_mismatch"
        if (
            attestation.worker_registration_id
            != event.claim.worker_registration_id
            or attestation.worker_lease_epoch
            != event.claim.worker_lease_epoch
            or attestation.worker_id != event.claim.worker_id
            or _utc(attestation.worker_started_at)
            != _utc(event.claim.started_at)
        ):
            return "worker_claim_mismatch"
        return None

    @staticmethod
    async def _locked_receipt_for_attestation(
        db: AsyncSession,
        attestation_id: uuid.UUID,
    ) -> RegisteredWorkerEventReceipt | None:
        return (
            await db.execute(
                select(RegisteredWorkerEventReceipt)
                .where(
                    RegisteredWorkerEventReceipt.source_task_attestation_id
                    == attestation_id
                )
                .with_for_update()
            )
        ).scalar_one_or_none()

    @staticmethod
    async def _receipt_for_delivery(
        db: AsyncSession,
        delivery: RegisteredWorkerEventDelivery,
    ) -> RegisteredWorkerEventReceipt:
        if delivery.receipt_id is None:
            raise RegisteredWorkerEventError(
                "accepted registered event delivery has no receipt"
            )
        receipt = (
            await db.execute(
                select(RegisteredWorkerEventReceipt)
                .where(
                    RegisteredWorkerEventReceipt.id == delivery.receipt_id
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if receipt is None:
            raise RegisteredWorkerEventError(
                "registered event receipt is missing"
            )
        return receipt

    @staticmethod
    async def _add_event_delivery(
        db: AsyncSession,
        *,
        receipt_id: uuid.UUID | None,
        attestation_id: uuid.UUID,
        event: RegisteredWorkerEvent,
        resolution_state: str = "accepted",
        reason_code: str | None = None,
    ) -> RegisteredWorkerEventDelivery:
        delivery = RegisteredWorkerEventDelivery(
            source_task_attestation_id=attestation_id,
            receipt_id=receipt_id,
            redis_stream=event.redis_stream,
            consumer_group=event.consumer_group,
            message_id=event.message_id,
            payload_sha256=event.payload_sha256,
            resolution_state=resolution_state,
            reason_code=reason_code,
            ack_state="pending",
        )
        db.add(delivery)
        await db.flush()
        return delivery

    @staticmethod
    def _source_receipt_mismatch_reason(
        receipt: RegisteredWorkerEventReceipt,
        event: RegisteredWorkerEvent,
        attestation_id: uuid.UUID,
    ) -> str | None:
        facts = event.receipt_facts(
            source_task_attestation_id=attestation_id,
        )
        for event_identity_field in (
            "redis_stream",
            "consumer_group",
            "message_id",
        ):
            facts.pop(event_identity_field)
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
            return None
        if name in {"payload_sha256", "payload_json", "event_type"}:
            return "event_payload_mismatch"
        return "worker_claim_mismatch"

    @staticmethod
    def _require_matching_source_receipt(
        receipt: RegisteredWorkerEventReceipt,
        event: RegisteredWorkerEvent,
        attestation_id: uuid.UUID,
    ) -> None:
        mismatch_reason = (
            RegisteredWorkerEventReceiptService
            ._source_receipt_mismatch_reason(
                receipt,
                event,
                attestation_id,
            )
        )
        if mismatch_reason is None:
            return
        raise RegisteredWorkerEventError(
            "registered event receipt fact or payload hash mismatch"
        )

    @staticmethod
    def _require_matching_event_delivery_identity(
        delivery: RegisteredWorkerEventDelivery,
        event: RegisteredWorkerEvent,
    ) -> None:
        if (
            delivery.redis_stream != event.redis_stream
            or delivery.consumer_group != event.consumer_group
            or delivery.message_id != event.message_id
            or delivery.payload_sha256 != event.payload_sha256
        ):
            raise RegisteredWorkerEventError(
                "registered event delivery fact or payload hash mismatch"
            )

    @staticmethod
    def _require_matching_delivery(
        delivery: RegisteredWorkerEventDelivery,
        receipt: RegisteredWorkerEventReceipt,
        event: RegisteredWorkerEvent,
    ) -> None:
        RegisteredWorkerEventReceiptService._require_matching_event_delivery_identity(
            delivery,
            event,
        )
        RegisteredWorkerEventReceiptService._require_matching_source_receipt(
            receipt,
            event,
            receipt.source_task_attestation_id,
        )

    @staticmethod
    def _validated_dispatch_payload(
        dispatch: WorkerTaskDispatch,
    ) -> dict[str, str]:
        payload = dispatch.payload_json
        if not isinstance(payload, dict):
            raise RegisteredWorkerEventError(
                "worker task dispatch payload is invalid"
            )
        if payload.get("dispatch_key") != str(dispatch.dispatch_key):
            raise RegisteredWorkerEventError(
                "worker task dispatch key mismatch"
            )
        payload_hash = canonical_redis_payload_sha256(payload)
        if payload_hash != dispatch.payload_sha256:
            raise RegisteredWorkerEventError(
                "worker task dispatch payload hash mismatch"
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


async def _observe_registered_worker_task_delivery(
    db: AsyncSession,
    event: RegisteredWorkerEvent,
) -> uuid.UUID:
    if db.get_bind().dialect.name == "postgresql":
        attestation_id = (
            await db.execute(
                select(WorkerTaskDeliveryAttestation.id).where(
                    WorkerTaskDeliveryAttestation.redis_stream
                    == event.source_task_stream,
                    WorkerTaskDeliveryAttestation.consumer_group
                    == event.source_task_group,
                    WorkerTaskDeliveryAttestation.message_id
                    == event.source_task_message_id,
                    WorkerTaskDeliveryAttestation.dispatch_key
                    == event.source_task_dispatch_key,
                )
            )
        ).scalar_one_or_none()
        if attestation_id is None:
            raise RegisteredWorkerEventError(
                "registered event task delivery attestation is missing"
            )
        return await observe_worker_event_emission(
            db,
            event.claim,
            attestation_id=attestation_id,
            redis_stream=event.redis_stream,
            consumer_group=event.consumer_group,
            message_id=event.message_id,
            payload_sha256=event.payload_sha256,
        )
    return await observe_worker_task_delivery(
        db,
        event.claim,
        redis_stream=event.source_task_stream,
        consumer_group=event.source_task_group,
        message_id=event.source_task_message_id,
        payload_sha256=event.source_task_payload_sha256,
        dispatch_key=event.source_task_dispatch_key,
    )
