from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.channel_agent import ChannelProfile, ProductionTask
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.schedule import RuntimeSchedule
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
