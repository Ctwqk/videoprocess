from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.channel_agent import ChannelProfile, ProductionTask
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.registered_worker_event_receipt import (
    WorkerTaskDeliveryAttestation,
    WorkerTaskDispatch,
)
from app.models.schedule import RuntimeSchedule
from app.models.worker_registration import WorkerRegistration
from app.services.schedule_service import VideoScheduleState, get_or_create_and_lock_runtime_schedule


class JobExecutionAuthorityBlocked(RuntimeError):
    """The durable job/node authority no longer permits execution."""


@dataclass(frozen=True)
class LockedJobExecutionAuthority:
    channel: ChannelProfile | None
    schedule: RuntimeSchedule
    task: ProductionTask | None
    job: Job
    node: NodeExecution | None


@dataclass(frozen=True)
class NodeExecutionClaim:
    job_id: uuid.UUID
    node_execution_id: uuid.UUID
    worker_id: str
    started_at: datetime
    worker_registration_id: uuid.UUID | None = None
    worker_lease_epoch: int | None = None


async def claim_registered_worker_node(
    db: AsyncSession,
    *,
    job_id: uuid.UUID,
    node_execution_id: uuid.UUID,
    registration_id: uuid.UUID | None,
    lease_epoch: int | None,
    worker_id: str,
    redis_stream: str,
    consumer_group: str,
    message_id: str,
    payload_sha256: str,
    dispatch_key: uuid.UUID,
) -> tuple[NodeExecutionClaim, uuid.UUID]:
    """Atomically claim a node and attest its exact delivered task."""

    registration_id, lease_epoch = _validated_registration_identity(
        registration_id,
        lease_epoch,
    )
    if (
        not isinstance(job_id, uuid.UUID)
        or not isinstance(node_execution_id, uuid.UUID)
        or not isinstance(dispatch_key, uuid.UUID)
        or not all(
            isinstance(value, str) and value.strip()
            for value in (
                worker_id,
                redis_stream,
                consumer_group,
                message_id,
            )
        )
        or len(payload_sha256) != 64
        or any(
            character not in "0123456789abcdef"
            for character in payload_sha256
        )
    ):
        raise JobExecutionAuthorityBlocked(
            "registered worker node claim identity is invalid"
        )

    if db.get_bind().dialect.name == "postgresql":
        result = await db.execute(
            text(
                """
                SELECT worker_started_at, attestation_id
                FROM public.vp_claim_worker_node(
                    :registration_id,
                    :lease_epoch,
                    :worker_id,
                    :job_id,
                    :node_execution_id,
                    :redis_stream,
                    :consumer_group,
                    :message_id,
                    :payload_sha256,
                    :dispatch_key
                )
                """
            ),
            {
                "registration_id": registration_id,
                "lease_epoch": lease_epoch,
                "worker_id": worker_id,
                "job_id": job_id,
                "node_execution_id": node_execution_id,
                "redis_stream": redis_stream,
                "consumer_group": consumer_group,
                "message_id": message_id,
                "payload_sha256": payload_sha256,
                "dispatch_key": dispatch_key,
            },
        )
        row = result.one_or_none()
        if (
            row is None
            or not isinstance(row[0], datetime)
            or not isinstance(row[1], uuid.UUID)
        ):
            raise JobExecutionAuthorityBlocked(
                "registered worker node claim result is invalid"
            )
        claim = NodeExecutionClaim(
            job_id=job_id,
            node_execution_id=node_execution_id,
            worker_id=worker_id,
            started_at=row[0],
            worker_registration_id=registration_id,
            worker_lease_epoch=lease_epoch,
        )
        return claim, row[1]

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
    registration = (
        await db.execute(
            select(WorkerRegistration)
            .where(WorkerRegistration.id == registration_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    database_now = await db.scalar(select(func.current_timestamp()))
    dispatch = (
        await db.execute(
            select(WorkerTaskDispatch)
            .where(
                WorkerTaskDispatch.dispatch_key == dispatch_key,
                WorkerTaskDispatch.job_id == job_id,
                WorkerTaskDispatch.node_execution_id == node_execution_id,
                WorkerTaskDispatch.redis_stream == redis_stream,
                WorkerTaskDispatch.consumer_group == consumer_group,
                WorkerTaskDispatch.redis_message_id == message_id,
                WorkerTaskDispatch.payload_sha256 == payload_sha256,
                WorkerTaskDispatch.delivery_state == "delivered",
                WorkerTaskDispatch.resolution_state == "unresolved",
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if (
        job is None
        or node is None
        or registration is None
        or not isinstance(database_now, datetime)
        or job.status != JobStatus.RUNNING
        or node.status != NodeStatus.QUEUED
        or registration.lease_epoch != lease_epoch
        or registration.status != "active"
        or _utc(registration.lease_expires_at) <= _utc(database_now)
        or dispatch is None
    ):
        raise JobExecutionAuthorityBlocked(
            "registered worker node claim is no longer authoritative"
        )
    claim = NodeExecutionClaim(
        job_id=job_id,
        node_execution_id=node_execution_id,
        worker_id=worker_id,
        started_at=database_now,
        worker_registration_id=registration_id,
        worker_lease_epoch=lease_epoch,
    )
    node.status = NodeStatus.RUNNING
    node.started_at = database_now
    node.worker_id = worker_id
    node.worker_registration_id = registration_id
    node.worker_lease_epoch = lease_epoch
    attestation = WorkerTaskDeliveryAttestation(
        redis_stream=redis_stream,
        consumer_group=consumer_group,
        message_id=message_id,
        payload_sha256=payload_sha256,
        dispatch_key=dispatch_key,
        job_id=job_id,
        node_execution_id=node_execution_id,
        worker_registration_id=registration_id,
        worker_lease_epoch=lease_epoch,
        worker_id=worker_id,
        worker_started_at=database_now,
        attested_at=database_now,
    )
    db.add(attestation)
    await db.flush()
    return claim, attestation.id


async def require_registered_worker_node_claim(
    db: AsyncSession,
    claim: NodeExecutionClaim,
) -> None:
    registration_id, lease_epoch = _validated_registration_claim(claim)
    await _call_worker_authority_function(
        db,
        """
        SELECT public.vp_require_worker_node_claim(
            :registration_id,
            :lease_epoch,
            :worker_id,
            :worker_started_at,
            :job_id,
            :node_execution_id
        )
        """,
        {
            "registration_id": registration_id,
            "lease_epoch": lease_epoch,
            "worker_id": claim.worker_id,
            "worker_started_at": claim.started_at,
            "job_id": claim.job_id,
            "node_execution_id": claim.node_execution_id,
        },
        error_message="registered worker node claim is no longer authoritative",
    )


async def persist_registered_worker_artifact(
    db: AsyncSession,
    claim: NodeExecutionClaim,
    *,
    filename: str,
    mime_type: str,
    file_size: int,
    storage_backend: str,
    storage_path: str,
    media_info: dict | None,
) -> uuid.UUID:
    registration_id, lease_epoch = _validated_registration_claim(claim)
    result = await _call_worker_authority_function(
        db,
        """
        SELECT public.vp_persist_worker_artifact(
            :registration_id,
            :lease_epoch,
            :worker_id,
            :worker_started_at,
            :job_id,
            :node_execution_id,
            :filename,
            :mime_type,
            :file_size,
            :storage_backend,
            :storage_path,
            CAST(:media_info AS jsonb)
        )
        """,
        {
            "registration_id": registration_id,
            "lease_epoch": lease_epoch,
            "worker_id": claim.worker_id,
            "worker_started_at": claim.started_at,
            "job_id": claim.job_id,
            "node_execution_id": claim.node_execution_id,
            "filename": filename,
            "mime_type": mime_type,
            "file_size": file_size,
            "storage_backend": storage_backend,
            "storage_path": storage_path,
            "media_info": json.dumps(media_info),
        },
        error_message="registered worker artifact cannot be persisted",
    )
    if not isinstance(result, uuid.UUID):
        raise JobExecutionAuthorityBlocked(
            "registered worker artifact identity is invalid"
        )
    return result


async def prepare_worker_event_emission(
    db: AsyncSession,
    claim: NodeExecutionClaim,
    *,
    attestation_id: uuid.UUID,
    redis_stream: str,
    consumer_group: str,
    payload_sha256: str,
    payload: dict[str, str],
    event_type: str,
) -> uuid.UUID:
    registration_id, lease_epoch = _validated_registration_claim(claim)
    result = await _call_worker_authority_function(
        db,
        """
        SELECT public.vp_prepare_worker_event_emission(
            :registration_id,
            :lease_epoch,
            :worker_id,
            :worker_started_at,
            :job_id,
            :node_execution_id,
            :attestation_id,
            :redis_stream,
            :consumer_group,
            :payload_sha256,
            CAST(:payload_json AS jsonb),
            :event_type
        )
        """,
        {
            "registration_id": registration_id,
            "lease_epoch": lease_epoch,
            "worker_id": claim.worker_id,
            "worker_started_at": claim.started_at,
            "job_id": claim.job_id,
            "node_execution_id": claim.node_execution_id,
            "attestation_id": attestation_id,
            "redis_stream": redis_stream,
            "consumer_group": consumer_group,
            "payload_sha256": payload_sha256,
            "payload_json": json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
            ),
            "event_type": event_type,
        },
        error_message="worker event emission cannot be prepared",
    )
    if not isinstance(result, uuid.UUID):
        raise JobExecutionAuthorityBlocked(
            "worker event emission identity is invalid"
        )
    return result


async def mark_worker_event_emitted(
    db: AsyncSession,
    claim: NodeExecutionClaim,
    *,
    emission_id: uuid.UUID,
    message_id: str,
) -> None:
    registration_id, lease_epoch = _validated_registration_claim(claim)
    await _call_worker_authority_function(
        db,
        """
        SELECT public.vp_mark_worker_event_emitted(
            :emission_id,
            :registration_id,
            :lease_epoch,
            :message_id
        )
        """,
        {
            "emission_id": emission_id,
            "registration_id": registration_id,
            "lease_epoch": lease_epoch,
            "message_id": message_id,
        },
        error_message="worker event emission cannot be recorded",
    )


async def observe_worker_event_emission(
    db: AsyncSession,
    claim: NodeExecutionClaim,
    *,
    attestation_id: uuid.UUID,
    redis_stream: str,
    consumer_group: str,
    message_id: str,
    payload_sha256: str,
) -> uuid.UUID:
    registration_id, lease_epoch = _validated_registration_claim(claim)
    result = await _call_worker_authority_function(
        db,
        """
        SELECT public.vp_observe_worker_event_emission(
            :registration_id,
            :lease_epoch,
            :worker_id,
            :worker_started_at,
            :job_id,
            :node_execution_id,
            :attestation_id,
            :redis_stream,
            :consumer_group,
            :message_id,
            :payload_sha256
        )
        """,
        {
            "registration_id": registration_id,
            "lease_epoch": lease_epoch,
            "worker_id": claim.worker_id,
            "worker_started_at": claim.started_at,
            "job_id": claim.job_id,
            "node_execution_id": claim.node_execution_id,
            "attestation_id": attestation_id,
            "redis_stream": redis_stream,
            "consumer_group": consumer_group,
            "message_id": message_id,
            "payload_sha256": payload_sha256,
        },
        error_message="worker event emission cannot be observed",
    )
    if not isinstance(result, uuid.UUID):
        raise JobExecutionAuthorityBlocked(
            "worker event emission attestation is invalid"
        )
    return result


async def recover_registered_worker_node(
    db: AsyncSession,
    job_id: uuid.UUID,
    node_execution_id: uuid.UUID,
) -> str:
    """Recover only an expired resolved registered claim under DB authority."""

    if db.get_bind().dialect.name == "postgresql":
        outcome = await _call_worker_authority_function(
            db,
            """
            SELECT public.vp_recover_registered_worker_node(
                :job_id,
                :node_execution_id
            )
            """,
            {
                "job_id": job_id,
                "node_execution_id": node_execution_id,
            },
            error_message="registered worker node recovery failed",
        )
        if outcome not in {
            "live",
            "held_unresolved",
            "recovered",
            "not_registered",
        }:
            raise JobExecutionAuthorityBlocked(
                "registered worker node recovery result is invalid"
            )
        return outcome

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
    if job is None or node is None:
        raise JobExecutionAuthorityBlocked(
            "registered worker recovery authority is missing"
        )
    registration_id = node.worker_registration_id
    lease_epoch = node.worker_lease_epoch
    if registration_id is None or lease_epoch is None:
        return "not_registered"
    registration = (
        await db.execute(
            select(WorkerRegistration)
            .where(WorkerRegistration.id == registration_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    database_now = await db.scalar(select(func.current_timestamp()))
    if not isinstance(database_now, datetime):
        raise JobExecutionAuthorityBlocked(
            "database recovery clock is unavailable"
        )
    if (
        registration is not None
        and registration.lease_epoch == lease_epoch
        and registration.status == "active"
        and _utc(registration.lease_expires_at) > _utc(database_now)
    ):
        return "live"
    unresolved_dispatch = (
        await db.execute(
            select(WorkerTaskDispatch.id)
            .where(
                WorkerTaskDispatch.node_execution_id == node.id,
                WorkerTaskDispatch.resolution_state.in_(
                    ("unresolved", "cancel_authorized")
                ),
            )
            .with_for_update()
        )
    ).scalar_one_or_none()
    if unresolved_dispatch is not None:
        return "held_unresolved"
    node.status = NodeStatus.PENDING
    node.worker_id = None
    node.worker_registration_id = None
    node.worker_lease_epoch = None
    node.queued_at = None
    node.started_at = None
    node.completed_at = None
    node.progress = 0
    node.error_message = None
    node.input_artifact_ids = []
    await db.flush()
    return "recovered"


def require_matching_node_execution_claim(
    authority: LockedJobExecutionAuthority,
    claim: NodeExecutionClaim,
) -> None:
    node = authority.node
    if (
        authority.job.id != claim.job_id
        or node is None
        or node.id != claim.node_execution_id
        or node.worker_id != claim.worker_id
        or not isinstance(node.started_at, datetime)
        or _utc(node.started_at) != _utc(claim.started_at)
        or getattr(node, "worker_registration_id", None)
        != claim.worker_registration_id
        or getattr(node, "worker_lease_epoch", None)
        != claim.worker_lease_epoch
    ):
        raise JobExecutionAuthorityBlocked("node execution claim changed")


async def require_worker_registration_lease(
    db: AsyncSession,
    claim: NodeExecutionClaim,
) -> None:
    await require_worker_registration_identity(
        db,
        claim.worker_registration_id,
        claim.worker_lease_epoch,
    )


async def observe_worker_registration_lease(
    db: AsyncSession,
    claim: NodeExecutionClaim,
) -> None:
    registration_id, lease_epoch = _validated_registration_claim(claim)
    await _call_worker_authority_function(
        db,
        """
        SELECT public.vp_observe_worker_lease(
            :registration_id,
            :lease_epoch
        )
        """,
        {
            "registration_id": registration_id,
            "lease_epoch": lease_epoch,
        },
        error_message="worker registration lease cannot be observed",
    )


async def attest_worker_task_delivery(
    db: AsyncSession,
    claim: NodeExecutionClaim,
    *,
    redis_stream: str,
    consumer_group: str,
    message_id: str,
    payload_sha256: str,
    dispatch_key: uuid.UUID,
) -> uuid.UUID:
    registration_id, lease_epoch = _validated_registration_claim(claim)
    _validate_task_delivery_identity(
        claim,
        redis_stream=redis_stream,
        consumer_group=consumer_group,
        message_id=message_id,
        payload_sha256=payload_sha256,
        dispatch_key=dispatch_key,
    )
    result = await _call_worker_authority_function(
        db,
        """
        SELECT public.vp_attest_worker_task_delivery(
            :registration_id,
            :lease_epoch,
            :worker_id,
            :worker_started_at,
            :job_id,
            :node_execution_id,
            :redis_stream,
            :consumer_group,
            :message_id,
            :payload_sha256,
            :dispatch_key
        )
        """,
        {
            "registration_id": registration_id,
            "lease_epoch": lease_epoch,
            "worker_id": claim.worker_id,
            "worker_started_at": claim.started_at,
            "job_id": claim.job_id,
            "node_execution_id": claim.node_execution_id,
            "redis_stream": redis_stream,
            "consumer_group": consumer_group,
            "message_id": message_id,
            "payload_sha256": payload_sha256,
            "dispatch_key": dispatch_key,
        },
        error_message="worker task delivery cannot be attested",
    )
    if not isinstance(result, uuid.UUID):
        raise JobExecutionAuthorityBlocked(
            "worker task delivery attestation identity is invalid"
        )
    return result


async def observe_worker_task_delivery(
    db: AsyncSession,
    claim: NodeExecutionClaim,
    *,
    redis_stream: str,
    consumer_group: str,
    message_id: str,
    payload_sha256: str,
    dispatch_key: uuid.UUID,
) -> uuid.UUID:
    registration_id, lease_epoch = _validated_registration_claim(claim)
    _validate_task_delivery_identity(
        claim,
        redis_stream=redis_stream,
        consumer_group=consumer_group,
        message_id=message_id,
        payload_sha256=payload_sha256,
        dispatch_key=dispatch_key,
    )
    result = await _call_worker_authority_function(
        db,
        """
        SELECT public.vp_observe_worker_task_delivery(
            :registration_id,
            :lease_epoch,
            :worker_id,
            :worker_started_at,
            :job_id,
            :node_execution_id,
            :redis_stream,
            :consumer_group,
            :message_id,
            :payload_sha256,
            :dispatch_key
        )
        """,
        {
            "registration_id": registration_id,
            "lease_epoch": lease_epoch,
            "worker_id": claim.worker_id,
            "worker_started_at": claim.started_at,
            "job_id": claim.job_id,
            "node_execution_id": claim.node_execution_id,
            "redis_stream": redis_stream,
            "consumer_group": consumer_group,
            "message_id": message_id,
            "payload_sha256": payload_sha256,
            "dispatch_key": dispatch_key,
        },
        error_message="worker task delivery cannot be observed",
    )
    if not isinstance(result, uuid.UUID):
        raise JobExecutionAuthorityBlocked(
            "worker task delivery attestation identity is invalid"
        )
    return result


async def require_worker_registration_margin(
    db: AsyncSession,
    claim: NodeExecutionClaim,
    *,
    minimum_margin_seconds: int,
) -> None:
    registration_id, lease_epoch = _validated_registration_claim(claim)
    if (
        type(minimum_margin_seconds) is not int
        or minimum_margin_seconds <= 0
    ):
        raise JobExecutionAuthorityBlocked(
            "worker registration lease margin is invalid"
        )
    await _call_worker_authority_function(
        db,
        """
        SELECT public.vp_require_worker_lease_margin(
            :registration_id,
            :lease_epoch,
            :minimum_margin_seconds
        )
        """,
        {
            "registration_id": registration_id,
            "lease_epoch": lease_epoch,
            "minimum_margin_seconds": minimum_margin_seconds,
        },
        error_message="worker registration lease margin is insufficient",
    )


async def require_worker_task_ack_receipt(
    db: AsyncSession,
    claim: NodeExecutionClaim,
    *,
    redis_stream: str,
    consumer_group: str,
    message_id: str,
    payload_sha256: str,
    dispatch_key: uuid.UUID,
) -> None:
    registration_id, lease_epoch = _validated_registration_claim(claim)
    if not all(
        isinstance(value, str) and value.strip()
        for value in (
            claim.worker_id,
            redis_stream,
            consumer_group,
            message_id,
        )
    ) or (
        not isinstance(payload_sha256, str)
        or len(payload_sha256) != 64
        or any(
            character not in "0123456789abcdef"
            for character in payload_sha256
        )
        or not isinstance(dispatch_key, uuid.UUID)
    ):
        raise JobExecutionAuthorityBlocked(
            "worker task acknowledgement identity is invalid"
        )
    await _call_worker_authority_function(
        db,
        """
        SELECT public.vp_require_worker_task_ack_receipt(
            :registration_id,
            :lease_epoch,
            :worker_id,
            :worker_started_at,
            :redis_stream,
            :consumer_group,
            :message_id,
            :payload_sha256,
            :dispatch_key
        )
        """,
        {
            "registration_id": registration_id,
            "lease_epoch": lease_epoch,
            "worker_id": claim.worker_id,
            "worker_started_at": claim.started_at,
            "redis_stream": redis_stream,
            "consumer_group": consumer_group,
            "message_id": message_id,
            "payload_sha256": payload_sha256,
            "dispatch_key": dispatch_key,
        },
        error_message=(
            "worker task acknowledgement has no applied event receipt"
        ),
    )


async def acknowledge_worker_task_delivery(
    db: AsyncSession,
    claim: NodeExecutionClaim,
    *,
    attestation_id: uuid.UUID,
    redis_stream: str,
    consumer_group: str,
    message_id: str,
    payload_sha256: str,
    dispatch_key: uuid.UUID,
) -> None:
    registration_id, lease_epoch = _validated_registration_claim(claim)
    if not isinstance(attestation_id, uuid.UUID):
        raise JobExecutionAuthorityBlocked(
            "worker task acknowledgement attestation is invalid"
        )
    _validate_task_delivery_identity(
        claim,
        redis_stream=redis_stream,
        consumer_group=consumer_group,
        message_id=message_id,
        payload_sha256=payload_sha256,
        dispatch_key=dispatch_key,
    )
    await _call_worker_authority_function(
        db,
        """
        SELECT public.vp_acknowledge_worker_task_delivery(
            :attestation_id,
            :registration_id,
            :lease_epoch,
            :worker_id,
            :worker_started_at,
            :redis_stream,
            :consumer_group,
            :message_id,
            :payload_sha256,
            :dispatch_key
        )
        """,
        {
            "attestation_id": attestation_id,
            "registration_id": registration_id,
            "lease_epoch": lease_epoch,
            "worker_id": claim.worker_id,
            "worker_started_at": claim.started_at,
            "redis_stream": redis_stream,
            "consumer_group": consumer_group,
            "message_id": message_id,
            "payload_sha256": payload_sha256,
            "dispatch_key": dispatch_key,
        },
        error_message=(
            "worker task acknowledgement state cannot be persisted"
        ),
    )


async def authorize_worker_task_ack(
    db: AsyncSession,
    claim: NodeExecutionClaim,
    *,
    attestation_id: uuid.UUID,
) -> None:
    registration_id, lease_epoch = _validated_registration_claim(claim)
    if not isinstance(attestation_id, uuid.UUID):
        raise JobExecutionAuthorityBlocked(
            "worker task acknowledgement attestation is invalid"
        )
    await _call_worker_authority_function(
        db,
        """
        SELECT public.vp_authorize_worker_task_ack(
            :attestation_id,
            :registration_id,
            :lease_epoch,
            :worker_id,
            :worker_started_at
        )
        """,
        {
            "attestation_id": attestation_id,
            "registration_id": registration_id,
            "lease_epoch": lease_epoch,
            "worker_id": claim.worker_id,
            "worker_started_at": claim.started_at,
        },
        error_message="worker task acknowledgement cannot be authorized",
    )


async def require_worker_registration_identity(
    db: AsyncSession,
    registration_id: uuid.UUID | None,
    lease_epoch: int | None,
) -> None:
    registration_id, lease_epoch = _validated_registration_identity(
        registration_id,
        lease_epoch,
    )
    await _call_worker_authority_function(
        db,
        """
        SELECT public.vp_require_worker_lease(
            :registration_id,
            :lease_epoch
        )
        """,
        {
            "registration_id": registration_id,
            "lease_epoch": lease_epoch,
        },
        error_message=(
            "worker registration lease is no longer authoritative"
        ),
    )


def _validate_task_delivery_identity(
    claim: NodeExecutionClaim,
    *,
    redis_stream: str,
    consumer_group: str,
    message_id: str,
    payload_sha256: str,
    dispatch_key: uuid.UUID,
) -> None:
    if (
        not all(
            isinstance(value, str) and value.strip()
            for value in (
                claim.worker_id,
                redis_stream,
                consumer_group,
                message_id,
            )
        )
        or not isinstance(payload_sha256, str)
        or len(payload_sha256) != 64
        or any(character not in "0123456789abcdef" for character in payload_sha256)
        or not isinstance(dispatch_key, uuid.UUID)
    ):
        raise JobExecutionAuthorityBlocked(
            "worker task delivery identity is invalid"
        )


def _validated_registration_claim(
    claim: NodeExecutionClaim,
) -> tuple[uuid.UUID, int]:
    return _validated_registration_identity(
        claim.worker_registration_id,
        claim.worker_lease_epoch,
    )


def _validated_registration_identity(
    registration_id: uuid.UUID | None,
    lease_epoch: int | None,
) -> tuple[uuid.UUID, int]:
    if (
        not isinstance(registration_id, uuid.UUID)
        or type(lease_epoch) is not int
        or lease_epoch <= 0
    ):
        raise JobExecutionAuthorityBlocked(
            "node execution has no durable worker registration lease"
        )
    return registration_id, lease_epoch


async def _call_worker_authority_function(
    db: AsyncSession,
    statement: str,
    parameters: dict[str, object],
    *,
    error_message: str,
) -> object:
    try:
        return await db.scalar(text(statement), parameters)
    except Exception as exc:
        raise JobExecutionAuthorityBlocked(error_message) from exc


def require_active_execution_authority(
    authority: LockedJobExecutionAuthority,
    *,
    job_statuses: set[JobStatus],
    node_statuses: set[NodeStatus] | None = None,
) -> None:
    if authority.channel is not None and (
        not authority.channel.enabled or authority.channel.halted_at is not None
    ):
        raise JobExecutionAuthorityBlocked("channel execution is blocked")
    if authority.task is not None and authority.task.state != "producing":
        raise JobExecutionAuthorityBlocked("production task is not producing")
    if (
        authority.schedule.state == VideoScheduleState.DRAINING.value
        and authority.job.status != JobStatus.RUNNING
    ):
        raise JobExecutionAuthorityBlocked("runtime schedule is draining")
    if authority.schedule.state not in {
        VideoScheduleState.OPEN.value,
        VideoScheduleState.DRAINING.value,
    }:
        raise JobExecutionAuthorityBlocked("runtime schedule is not open")
    try:
        guarded_job_id = authority.schedule.guarded_job_id
    except AttributeError as exc:
        raise JobExecutionAuthorityBlocked(
            "runtime schedule guarded authority is unavailable"
        ) from exc
    if guarded_job_id is not None and authority.job.id != guarded_job_id:
        raise JobExecutionAuthorityBlocked("job does not hold guarded schedule authority")
    if authority.job.status not in job_statuses:
        raise JobExecutionAuthorityBlocked("job status no longer permits execution")
    if node_statuses is not None:
        if authority.node is None or authority.node.status not in node_statuses:
            raise JobExecutionAuthorityBlocked("node status no longer permits execution")


async def lock_job_execution_authority(
    db: AsyncSession,
    job_id: uuid.UUID,
    *,
    node_execution_id: uuid.UUID | None = None,
    lock_all_nodes: bool = False,
) -> LockedJobExecutionAuthority:
    """Lock shared execution authority in quarantine-compatible order.

    ChannelOps jobs lock channel -> schedule -> task -> job -> node. Jobs that
    are not linked to ChannelOps lock schedule -> job -> node.
    """

    task_refs = list(
        (
            await db.execute(
                select(ProductionTask.id, ProductionTask.channel_profile_id)
                .where(ProductionTask.job_id == job_id)
                .order_by(ProductionTask.id)
                .limit(2)
            )
        ).all()
    )
    if len(task_refs) > 1:
        raise JobExecutionAuthorityBlocked("job is linked to multiple production tasks")

    channel = None
    task = None
    if task_refs:
        task_id, discovered_channel_id = task_refs[0]
        channel = (
            await db.execute(
                select(ChannelProfile)
                .where(ChannelProfile.id == discovered_channel_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        if channel is None:
            raise JobExecutionAuthorityBlocked("production task channel was not found")

    schedule, _created = await get_or_create_and_lock_runtime_schedule(db)

    if task_refs:
        task_id, discovered_channel_id = task_refs[0]
        task = (
            await db.execute(
                select(ProductionTask)
                .where(ProductionTask.id == task_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        if (
            task is None
            or task.job_id != job_id
            or task.channel_profile_id != discovered_channel_id
            or channel is None
            or task.channel_profile_id != channel.id
        ):
            raise JobExecutionAuthorityBlocked("production task authority changed while locking")

    job = (
        await db.execute(
            select(Job)
            .where(Job.id == job_id)
            .options(selectinload(Job.node_executions))
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if job is None:
        raise JobExecutionAuthorityBlocked("job was not found")

    node = None
    if lock_all_nodes:
        nodes = list(
            (
                await db.execute(
                    select(NodeExecution)
                    .where(NodeExecution.job_id == job.id)
                    .order_by(NodeExecution.id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalars().all()
        )
        if node_execution_id is not None:
            node = next((item for item in nodes if item.id == node_execution_id), None)
            if node is None:
                raise JobExecutionAuthorityBlocked("node execution authority changed while locking")
        await db.refresh(job, attribute_names=["node_executions"])
    elif node_execution_id is not None:
        node = (
            await db.execute(
                select(NodeExecution)
                .where(NodeExecution.id == node_execution_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        if node is None or node.job_id != job.id:
            raise JobExecutionAuthorityBlocked("node execution authority changed while locking")

    return LockedJobExecutionAuthority(
        channel=channel,
        schedule=schedule,
        task=task,
        job=job,
        node=node,
    )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
