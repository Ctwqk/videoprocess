"""Release an original registered claim only within first-retry receipt application."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import TypeVar

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.job import JobStatus, NodeExecution, NodeStatus
from app.models.registered_worker_event_receipt import (
    RegisteredWorkerEventDelivery,
    RegisteredWorkerEventReceipt,
    WorkerEventEmission,
    WorkerTaskDeliveryAttestation,
    WorkerTaskDispatch,
)
from app.models.worker_registration import WorkerAdmissionGrant, WorkerRegistration
from app.services.job_execution_authority import (
    JobExecutionAuthorityBlocked,
    _utc,
    lock_job_execution_authority,
    require_active_execution_authority,
)


_OWNERSHIP = ("worker_id", "worker_registration_id", "worker_lease_epoch", "started_at")
_CLAIM = (
    "job_id",
    "node_execution_id",
    "worker_registration_id",
    "worker_lease_epoch",
    "worker_id",
)
_T = TypeVar("_T")


def _require(condition: bool) -> None:
    if not condition:
        raise JobExecutionAuthorityBlocked(
            "registered retry release authority mismatch"
        )


def _required(value: _T | None) -> _T:
    if value is None:
        raise JobExecutionAuthorityBlocked(
            "registered retry release authority mismatch"
        )
    return value


async def release_registered_retry_claim(
    db: AsyncSession, receipt_id: uuid.UUID
) -> uuid.UUID:
    _require(isinstance(receipt_id, uuid.UUID))
    if db.get_bind().dialect.name == "postgresql":
        node_id = await db.scalar(
            text("SELECT public.vp_release_registered_retry_claim(:receipt_id)"),
            {"receipt_id": receipt_id},
        )
        _require(isinstance(node_id, uuid.UUID))
        node = _required(await db.get(NodeExecution, node_id))
        # Never issue an ownership UPDATE as the restricted orchestrator role.
        await db.refresh(node, attribute_names=list(_OWNERSHIP))
        return node_id

    _require(db.get_bind().dialect.name == "sqlite")
    return await _release_sqlite(db, receipt_id)


async def _release_sqlite(db: AsyncSession, receipt_id: uuid.UUID) -> uuid.UUID:
    receipt = _required(
        await db.get(RegisteredWorkerEventReceipt, receipt_id, populate_existing=True)
    )
    authority = await lock_job_execution_authority(
        db,
        receipt.job_id,
        node_execution_id=receipt.node_execution_id,
    )
    require_active_execution_authority(
        authority,
        job_statuses={JobStatus.RUNNING},
        node_statuses={NodeStatus.QUEUED},
    )
    node = _required(authority.node)
    _require(
        receipt.application_state == "accepted"
        and receipt.applied_at is None
        and receipt.event_type == "node_failed"
        and receipt.ack_state == "pending"
        and receipt.source_task_ack_state == "pending"
        and node.retry_count == 1
        and node.queued_at is not None
        and node.worker_id == receipt.worker_id
        and node.worker_registration_id == receipt.worker_registration_id
        and node.worker_lease_epoch == receipt.worker_lease_epoch
        and node.started_at is not None
        and _utc(node.started_at) == _utc(receipt.worker_started_at)
    )
    attestation = _required(
        await db.get(WorkerTaskDeliveryAttestation, receipt.source_task_attestation_id)
    )
    _require(
        all(getattr(attestation, key) == getattr(receipt, key) for key in _CLAIM)
        and _utc(attestation.worker_started_at) == _utc(receipt.worker_started_at)
        and (
            attestation.redis_stream,
            attestation.consumer_group,
            attestation.message_id,
        )
        == (
            receipt.source_task_stream,
            receipt.source_task_group,
            receipt.source_task_message_id,
        )
    )
    registration = _required(
        await db.get(WorkerRegistration, receipt.worker_registration_id)
    )
    now = _required(await db.scalar(select(func.current_timestamp())))
    _require(
        registration.lease_epoch == receipt.worker_lease_epoch
        and registration.status == "active"
        and _utc(registration.lease_expires_at) > _utc(now)
    )
    grant = await db.get(WorkerAdmissionGrant, registration.grant_id)
    _require(
        grant is not None
        and (grant.redis_stream, grant.redis_group)
        == (attestation.redis_stream, attestation.consumer_group)
    )
    original = (
        await db.scalars(
            select(WorkerTaskDispatch).where(
                WorkerTaskDispatch.dispatch_key == attestation.dispatch_key
            )
        )
    ).one_or_none()
    original = _required(original)
    _require(
        (
            original.job_id,
            original.node_execution_id,
            original.redis_stream,
            original.consumer_group,
            original.redis_message_id,
            original.payload_sha256,
        )
        == (
            attestation.job_id,
            attestation.node_execution_id,
            attestation.redis_stream,
            attestation.consumer_group,
            attestation.message_id,
            attestation.payload_sha256,
        )
        and original.delivery_state == "delivered"
        and original.resolution_state in {"unresolved", "acknowledged"}
    )
    emission = (
        await db.scalars(
            select(WorkerEventEmission).where(
                WorkerEventEmission.source_task_attestation_id == attestation.id
            )
        )
    ).one_or_none()
    emission = _required(emission)
    _require(
        all(
            getattr(emission, key) == getattr(receipt, key)
            for key in (
                *_CLAIM,
                "redis_stream",
                "consumer_group",
                "message_id",
                "payload_sha256",
                "payload_json",
                "event_type",
            )
        )
        and _utc(emission.worker_started_at) == _utc(receipt.worker_started_at)
        and emission.emission_state == "emitted"
        and emission.emitted_at is not None
        and emission.resolved_at is None
    )
    _require(
        all(
            receipt.payload_json.get(key) == value
            for key, value in {
                "event": "node_failed",
                "job_id": str(receipt.job_id),
                "node_execution_id": str(receipt.node_execution_id),
                "worker_id": receipt.worker_id,
                "worker_registration_id": str(receipt.worker_registration_id),
                "worker_lease_epoch": str(receipt.worker_lease_epoch),
                "task_stream": receipt.source_task_stream,
                "task_group": receipt.source_task_group,
                "task_message_id": receipt.source_task_message_id,
                "task_payload_sha256": attestation.payload_sha256,
                "task_dispatch_key": str(attestation.dispatch_key),
            }.items()
        )
    )
    try:
        payload_started_at = datetime.fromisoformat(
            receipt.payload_json.get("started_at", "")
        )
    except (TypeError, ValueError) as exc:
        raise JobExecutionAuthorityBlocked(
            "registered retry release authority mismatch"
        ) from exc
    _require(_utc(payload_started_at) == _utc(receipt.worker_started_at))
    delivery = (
        await db.scalars(
            select(RegisteredWorkerEventDelivery).where(
                RegisteredWorkerEventDelivery.redis_stream == receipt.redis_stream,
                RegisteredWorkerEventDelivery.consumer_group == receipt.consumer_group,
                RegisteredWorkerEventDelivery.message_id == receipt.message_id,
            )
        )
    ).one_or_none()
    _require(
        delivery is not None
        and delivery.receipt_id == receipt.id
        and delivery.source_task_attestation_id == attestation.id
        and delivery.payload_sha256 == receipt.payload_sha256
        and delivery.resolution_state == "accepted"
        and delivery.ack_state == "pending"
        and delivery.reason_code is None
    )
    retries = (
        await db.scalars(
            select(WorkerTaskDispatch).where(
                WorkerTaskDispatch.origin_receipt_id == receipt.id
            )
        )
    ).all()
    _require(len(retries) == 1)
    retry = retries[0]
    _require(
        retry.id != original.id
        and retry.job_id == receipt.job_id
        and retry.node_execution_id == receipt.node_execution_id
        and (retry.redis_stream, retry.consumer_group)
        == (original.redis_stream, original.consumer_group)
        and retry.delivery_state == "pending"
        and retry.resolution_state == "unresolved"
        and all(
            getattr(retry, key) is None
            for key in (
                "delivery_attempted_at",
                "delivery_error",
                "redis_message_id",
                "delivered_at",
                "acknowledged_at",
                "cancelled_at",
            )
        )
        and _utc(retry.created_at) >= _utc(receipt.accepted_at)
        and all(
            retry.payload_json.get(key) == value
            for key, value in {
                "job_id": str(receipt.job_id),
                "node_execution_id": str(receipt.node_execution_id),
                "dispatch_key": str(retry.dispatch_key),
            }.items()
        )
    )
    competing = await db.scalar(
        select(WorkerTaskDispatch.id)
        .where(
            WorkerTaskDispatch.node_execution_id == node.id,
            WorkerTaskDispatch.id.not_in((original.id, retry.id)),
            WorkerTaskDispatch.resolution_state.in_(
                ("unresolved", "cancel_authorized")
            ),
        )
        .limit(1)
    )
    retry_attestation = await db.scalar(
        select(WorkerTaskDeliveryAttestation.id)
        .where(
            WorkerTaskDeliveryAttestation.dispatch_key == retry.dispatch_key,
        )
        .limit(1)
    )
    _require(competing is None and retry_attestation is None)
    for field in _OWNERSHIP:
        setattr(node, field, None)
    await db.flush()
    return node.id
