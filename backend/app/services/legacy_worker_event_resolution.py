from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.channel_agent import ChannelProfile, ProductionTask
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.legacy_worker_event_resolution import LegacyWorkerEventResolution
from app.models.schedule import RuntimeSchedule


_MESSAGE_ID_PATTERN = re.compile(r"^[1-9][0-9]*-[0-9]+$")
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
REDIS_STREAM = "vp:events"
CONSUMER_GROUP = "orchestrator"
VIDEO_SCHEDULE_SERVICE = "videoprocess"
_ADVISORY_LOCK_KEY = 8_709_332_781_042_015_777


class LegacyEventResolutionError(RuntimeError):
    pass


@dataclass(frozen=True)
class ExpectedLegacyEvent:
    message_id: str
    payload_sha256: str


@dataclass(frozen=True)
class LegacyEventResolutionRequest:
    expected_events: tuple[ExpectedLegacyEvent, ...]
    operator_id: str
    apply: bool = False


@dataclass(frozen=True)
class LegacyEventCandidate:
    message_id: str
    payload_sha256: str
    job_id: uuid.UUID
    node_execution_id: uuid.UUID


@dataclass(frozen=True)
class LegacyEventResolutionReport:
    applied: bool
    candidates: tuple[LegacyEventCandidate, ...]
    resolution_ids: tuple[uuid.UUID, ...]
    xack_count: int
    final_pending: int


@dataclass(frozen=True)
class _ValidatedLegacyEvent:
    candidate: LegacyEventCandidate
    payload: dict[str, str]
    channel_halted_at: datetime


def parse_expected_events(values: Sequence[str]) -> tuple[ExpectedLegacyEvent, ...]:
    if not values:
        raise LegacyEventResolutionError("at least one expected event is required")

    parsed: list[ExpectedLegacyEvent] = []
    seen_ids: set[str] = set()
    for value in values:
        message_id, separator, payload_sha256 = value.partition("=")
        if (
            separator != "="
            or _MESSAGE_ID_PATTERN.fullmatch(message_id) is None
            or _SHA256_PATTERN.fullmatch(payload_sha256) is None
        ):
            raise LegacyEventResolutionError("expected event manifest is invalid")
        if message_id in seen_ids:
            raise LegacyEventResolutionError("expected event message IDs must be unique")
        seen_ids.add(message_id)
        parsed.append(
            ExpectedLegacyEvent(
                message_id=message_id,
                payload_sha256=payload_sha256,
            )
        )
    return tuple(parsed)


def canonical_payload_sha256(payload: Mapping[str, str]) -> str:
    if any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in payload.items()
    ):
        raise LegacyEventResolutionError("Redis event payload must contain strings")
    encoded = json.dumps(
        dict(payload),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


async def resolve_legacy_worker_events(
    db: AsyncSession,
    redis_client: Any,
    request: LegacyEventResolutionRequest,
) -> LegacyEventResolutionReport:
    operator_id = request.operator_id.strip()
    if not operator_id:
        raise LegacyEventResolutionError("operator identity is required")
    if not request.expected_events:
        raise LegacyEventResolutionError("at least one expected event is required")

    expected_by_id = {
        item.message_id: item.payload_sha256 for item in request.expected_events
    }
    if len(expected_by_id) != len(request.expected_events):
        raise LegacyEventResolutionError("expected event message IDs must be unique")

    pending_ids: tuple[str, ...]
    validated_events: list[_ValidatedLegacyEvent] = []
    resolutions: list[LegacyWorkerEventResolution] = []
    async with db.begin():
        await _acquire_advisory_lock(db)
        await _require_closed_schedule(db)

        existing_rows = list(
            (
                await db.execute(
                    select(LegacyWorkerEventResolution)
                    .where(
                        LegacyWorkerEventResolution.redis_stream == REDIS_STREAM,
                        LegacyWorkerEventResolution.consumer_group == CONSUMER_GROUP,
                        LegacyWorkerEventResolution.message_id.in_(expected_by_id),
                    )
                    .with_for_update()
                )
            ).scalars()
        )
        existing_by_id = {row.message_id: row for row in existing_rows}
        all_events_archived = len(existing_by_id) == len(expected_by_id)

        pending_ids = await _read_pending_ids(
            redis_client,
            detail_limit=len(expected_by_id),
        )
        pending_set = set(pending_ids)
        expected_set = set(expected_by_id)
        if pending_set != expected_set and not (
            request.apply
            and all_events_archived
            and pending_set < expected_set
        ):
            raise LegacyEventResolutionError(
                "expected events do not match the complete pending set"
            )

        for message_id in expected_by_id:
            payload = await _read_exact_payload(redis_client, message_id)
            payload_sha256 = canonical_payload_sha256(payload)
            if payload_sha256 != expected_by_id[message_id]:
                raise LegacyEventResolutionError(
                    f"legacy event payload hash changed for {message_id}"
                )
            candidate = _candidate_from_payload(
                message_id,
                payload_sha256,
                payload,
            )
            channel_halted_at = await _require_terminal_database_state(db, candidate)
            validated = _ValidatedLegacyEvent(
                candidate=candidate,
                payload=payload,
                channel_halted_at=channel_halted_at,
            )
            validated_events.append(validated)

            existing = existing_by_id.get(message_id)
            if existing is not None:
                _require_matching_archive(existing, validated)
                resolutions.append(existing)
            elif request.apply:
                resolution = _new_resolution(validated, operator_id)
                db.add(resolution)
                resolutions.append(resolution)

        if request.apply:
            await db.flush()

    candidates = tuple(item.candidate for item in validated_events)
    if not request.apply:
        return LegacyEventResolutionReport(
            applied=False,
            candidates=candidates,
            resolution_ids=(),
            xack_count=0,
            final_pending=len(pending_ids),
        )

    resolution_ids = tuple(row.id for row in resolutions)
    if not pending_ids:
        await _record_acknowledgement(db, resolution_ids)
        return LegacyEventResolutionReport(
            applied=True,
            candidates=candidates,
            resolution_ids=resolution_ids,
            xack_count=0,
            final_pending=0,
        )

    for validated in validated_events:
        payload = await _read_exact_payload(
            redis_client,
            validated.candidate.message_id,
        )
        if (
            canonical_payload_sha256(payload)
            != validated.candidate.payload_sha256
        ):
            raise LegacyEventResolutionError(
                f"legacy event payload hash changed for "
                f"{validated.candidate.message_id}"
            )

    xack_count = await redis_client.xack(
        REDIS_STREAM,
        CONSUMER_GROUP,
        *pending_ids,
    )
    if type(xack_count) is not int or xack_count != len(pending_ids):
        raise LegacyEventResolutionError(
            "Redis legacy event acknowledgement count is invalid"
        )
    final_pending = await _read_pending_count(redis_client)
    if final_pending != 0:
        raise LegacyEventResolutionError(
            "Redis pending set changed during legacy event resolution"
        )
    await _record_acknowledgement(db, resolution_ids)
    return LegacyEventResolutionReport(
        applied=True,
        candidates=candidates,
        resolution_ids=resolution_ids,
        xack_count=xack_count,
        final_pending=final_pending,
    )


async def _acquire_advisory_lock(db: AsyncSession) -> None:
    bind = db.get_bind()
    if bind.dialect.name == "postgresql":
        await db.execute(
            text("SELECT pg_advisory_xact_lock(:lock_key)"),
            {"lock_key": _ADVISORY_LOCK_KEY},
        )


async def _require_closed_schedule(db: AsyncSession) -> None:
    schedule = (
        await db.execute(
            select(RuntimeSchedule)
            .where(RuntimeSchedule.service_name == VIDEO_SCHEDULE_SERVICE)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if schedule is None or schedule.state != "CLOSED":
        raise LegacyEventResolutionError("video schedule must be CLOSED")
    if schedule.guarded_job_id is not None:
        raise LegacyEventResolutionError("video schedule has guarded job authority")

    active_nodes = int(
        await db.scalar(
            select(func.count())
            .select_from(NodeExecution)
            .where(NodeExecution.status.in_((NodeStatus.QUEUED, NodeStatus.RUNNING)))
        )
        or 0
    )
    if active_nodes:
        raise LegacyEventResolutionError("active node executions block resolution")


async def _read_pending_count(redis_client: Any) -> int:
    summary = await redis_client.xpending(REDIS_STREAM, CONSUMER_GROUP)
    if (
        not isinstance(summary, dict)
        or type(summary.get("pending")) is not int
        or summary["pending"] < 0
    ):
        raise LegacyEventResolutionError("Redis pending summary is invalid")
    return summary["pending"]


async def _read_pending_ids(
    redis_client: Any,
    *,
    detail_limit: int,
) -> tuple[str, ...]:
    pending_count = await _read_pending_count(redis_client)
    if pending_count > detail_limit:
        raise LegacyEventResolutionError(
            "expected events do not match the complete pending set"
        )
    if pending_count == 0:
        return ()
    rows = await redis_client.xpending_range(
        REDIS_STREAM,
        CONSUMER_GROUP,
        "-",
        "+",
        pending_count,
    )
    if not isinstance(rows, list) or len(rows) != pending_count:
        raise LegacyEventResolutionError("Redis pending detail is invalid")

    message_ids: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            raise LegacyEventResolutionError("Redis pending detail is invalid")
        message_id = row.get("message_id")
        if (
            not isinstance(message_id, str)
            or _MESSAGE_ID_PATTERN.fullmatch(message_id) is None
            or message_id in message_ids
        ):
            raise LegacyEventResolutionError("Redis pending detail is invalid")
        message_ids.append(message_id)
    return tuple(message_ids)


async def _read_exact_payload(
    redis_client: Any,
    message_id: str,
) -> dict[str, str]:
    rows = await redis_client.xrange(
        REDIS_STREAM,
        min=message_id,
        max=message_id,
        count=1,
    )
    if (
        not isinstance(rows, list)
        or len(rows) != 1
        or not isinstance(rows[0], (tuple, list))
        or len(rows[0]) != 2
        or rows[0][0] != message_id
        or not isinstance(rows[0][1], dict)
    ):
        raise LegacyEventResolutionError(
            f"legacy event payload is unavailable for {message_id}"
        )
    payload = rows[0][1]
    canonical_payload_sha256(payload)
    return payload


def _candidate_from_payload(
    message_id: str,
    payload_sha256: str,
    payload: Mapping[str, str],
) -> LegacyEventCandidate:
    if payload.get("event") != "node_failed":
        raise LegacyEventResolutionError("legacy event must be node_failed")
    if "worker_id" in payload or "started_at" in payload:
        raise LegacyEventResolutionError("legacy event contains an execution claim")
    try:
        job_id = uuid.UUID(payload["job_id"])
        node_execution_id = uuid.UUID(payload["node_execution_id"])
    except (KeyError, TypeError, ValueError) as exc:
        raise LegacyEventResolutionError(
            "legacy event identifiers are invalid"
        ) from exc
    return LegacyEventCandidate(
        message_id=message_id,
        payload_sha256=payload_sha256,
        job_id=job_id,
        node_execution_id=node_execution_id,
    )


async def _require_terminal_database_state(
    db: AsyncSession,
    candidate: LegacyEventCandidate,
) -> datetime:
    job = (
        await db.execute(
            select(Job).where(Job.id == candidate.job_id).with_for_update()
        )
    ).scalar_one_or_none()
    if job is None or job.status != JobStatus.CANCELLED:
        raise LegacyEventResolutionError("legacy event job is not CANCELLED")

    node = (
        await db.execute(
            select(NodeExecution)
            .where(NodeExecution.id == candidate.node_execution_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if (
        node is None
        or node.job_id != job.id
        or node.status != NodeStatus.CANCELLED
    ):
        raise LegacyEventResolutionError("legacy event node is not CANCELLED")

    tasks = list(
        (
            await db.execute(
                select(ProductionTask)
                .where(ProductionTask.job_id == job.id)
                .with_for_update()
            )
        ).scalars()
    )
    if len(tasks) != 1 or tasks[0].state != "held":
        raise LegacyEventResolutionError(
            "legacy event production task is not uniquely held"
        )

    channel = (
        await db.execute(
            select(ChannelProfile)
            .where(ChannelProfile.id == tasks[0].channel_profile_id)
            .with_for_update()
        )
    ).scalar_one_or_none()
    if channel is None or channel.halted_at is None:
        raise LegacyEventResolutionError("legacy event channel is not halted")
    return channel.halted_at


def _require_matching_archive(
    resolution: LegacyWorkerEventResolution,
    validated: _ValidatedLegacyEvent,
) -> None:
    candidate = validated.candidate
    if (
        resolution.payload_sha256 != candidate.payload_sha256
        or resolution.payload_json != validated.payload
        or resolution.event_type != "node_failed"
        or resolution.job_id != candidate.job_id
        or resolution.node_execution_id != candidate.node_execution_id
        or resolution.resolution_reason != "terminal_cancelled"
        or resolution.observed_job_status != "CANCELLED"
        or resolution.observed_node_status != "CANCELLED"
        or resolution.observed_task_state != "held"
    ):
        raise LegacyEventResolutionError(
            f"legacy event archive mismatch for {candidate.message_id}"
        )


def _new_resolution(
    validated: _ValidatedLegacyEvent,
    operator_id: str,
) -> LegacyWorkerEventResolution:
    candidate = validated.candidate
    return LegacyWorkerEventResolution(
        redis_stream=REDIS_STREAM,
        consumer_group=CONSUMER_GROUP,
        message_id=candidate.message_id,
        payload_sha256=candidate.payload_sha256,
        payload_json=validated.payload,
        event_type="node_failed",
        job_id=candidate.job_id,
        node_execution_id=candidate.node_execution_id,
        resolution_reason="terminal_cancelled",
        operator_id=operator_id,
        observed_job_status="CANCELLED",
        observed_node_status="CANCELLED",
        observed_task_state="held",
        observed_channel_halted_at=validated.channel_halted_at,
    )


async def _record_acknowledgement(
    db: AsyncSession,
    resolution_ids: tuple[uuid.UUID, ...],
) -> None:
    acknowledged_at = datetime.now(timezone.utc)
    async with db.begin():
        rows = list(
            (
                await db.execute(
                    select(LegacyWorkerEventResolution)
                    .where(LegacyWorkerEventResolution.id.in_(resolution_ids))
                    .with_for_update()
                )
            ).scalars()
        )
        if len(rows) != len(resolution_ids):
            raise LegacyEventResolutionError(
                "legacy event archive disappeared before acknowledgement"
            )
        for row in rows:
            row.acknowledged_at = row.acknowledged_at or acknowledged_at
