from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models.channel_agent import ChannelProfile, ProductionTask
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.legacy_worker_event_resolution import LegacyWorkerEventResolution
from app.models.schedule import RuntimeSchedule
from app.services.legacy_worker_event_resolution import (
    LegacyEventResolutionRequest,
    LegacyEventResolutionError,
    canonical_payload_sha256,
    parse_expected_events,
    resolve_legacy_worker_events,
)

NOW = datetime(2026, 7, 26, 3, 16, 51, tzinfo=timezone.utc)
TABLES = (
    ChannelProfile.__table__,
    ProductionTask.__table__,
    Job.__table__,
    NodeExecution.__table__,
    RuntimeSchedule.__table__,
    LegacyWorkerEventResolution.__table__,
)


class FakeRedis:
    def __init__(self, entries: dict[str, dict[str, str]]):
        self.entries = entries
        self.pending_ids = list(entries)
        self.ack_calls: list[tuple[str, str, tuple[str, ...]]] = []
        self.before_xack: Callable[[], Awaitable[None]] | None = None
        self.xack_override: int | None = None
        self.xrange_calls = 0
        self.before_xrange: Callable[[int], None] | None = None

    async def xpending(self, _stream: str, _group: str):
        return {"pending": len(self.pending_ids)}

    async def xpending_range(
        self,
        _stream: str,
        _group: str,
        _minimum: str,
        _maximum: str,
        count: int,
    ):
        return [
            {
                "message_id": message_id,
                "consumer": "orchestrator-api-1",
                "time_since_delivered": 1000,
                "times_delivered": 3,
            }
            for message_id in self.pending_ids[:count]
        ]

    async def xrange(
        self,
        _stream: str,
        min: str,
        max: str,
        count: int,
    ):
        assert min == max
        self.xrange_calls += 1
        if self.before_xrange is not None:
            self.before_xrange(self.xrange_calls)
        payload = self.entries.get(min)
        return [] if payload is None else [(min, dict(payload))][:count]

    async def xack(self, stream: str, group: str, *message_ids: str):
        self.ack_calls.append((stream, group, message_ids))
        if self.before_xack is not None:
            await self.before_xack()
        if self.xack_override is not None:
            return self.xack_override
        acknowledged = 0
        for message_id in message_ids:
            if message_id in self.pending_ids:
                self.pending_ids.remove(message_id)
                acknowledged += 1
        return acknowledged


@pytest.fixture
async def resolution_db():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        for table in TABLES:
            await connection.run_sync(table.create)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        yield session
    await engine.dispose()


async def seed_terminal_event(
    db: AsyncSession,
    *,
    job_status: JobStatus = JobStatus.CANCELLED,
    node_status: NodeStatus = NodeStatus.CANCELLED,
    task_state: str = "held",
    channel_halted: bool = True,
    schedule_state: str = "CLOSED",
    guarded_job: bool = False,
) -> tuple[dict[str, str], str]:
    channel = ChannelProfile(
        name="fifth canary",
        enabled=True,
        dry_run=False,
        halted_at=NOW if channel_halted else None,
    )
    job = Job(
        pipeline_id=uuid.uuid4(),
        pipeline_snapshot={"nodes": [], "edges": []},
        status=job_status,
        completed_at=NOW if job_status == JobStatus.CANCELLED else None,
    )
    node = NodeExecution(
        job=job,
        node_id="smart_trim_1",
        node_type="smart_trim",
        status=node_status,
        started_at=NOW,
        completed_at=NOW if node_status == NodeStatus.CANCELLED else None,
    )
    db.add_all([channel, job, node])
    await db.flush()
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=uuid.uuid4(),
        prompt="fifth canary",
        state=task_state,
        job_id=job.id,
    )
    schedule = RuntimeSchedule(
        service_name="videoprocess",
        state=schedule_state,
        guarded_job_id=job.id if guarded_job else None,
        updated_by="test",
    )
    db.add_all([task, schedule])
    await db.commit()
    payload = {
        "event": "node_failed",
        "job_id": str(job.id),
        "node_execution_id": str(node.id),
        "error": "smart_trim input video has no detectable duration",
    }
    return payload, "1785034608101-0"


def request_for(
    message_id: str,
    payload: dict[str, str],
    *,
    apply: bool = False,
) -> LegacyEventResolutionRequest:
    return LegacyEventResolutionRequest(
        expected_events=parse_expected_events(
            (f"{message_id}={canonical_payload_sha256(payload)}",)
        ),
        operator_id="operator:wenjieliu",
        apply=apply,
    )


def test_parse_expected_events_accepts_exact_unique_manifest():
    first_hash = "a" * 64
    second_hash = "0" * 64

    parsed = parse_expected_events(
        (
            f"1785034608101-0={first_hash}",
            f"1785034608559-0={second_hash}",
        )
    )

    assert [(item.message_id, item.payload_sha256) for item in parsed] == [
        ("1785034608101-0", first_hash),
        ("1785034608559-0", second_hash),
    ]


@pytest.mark.parametrize(
    "values",
    (
        (),
        ("1785034608101-0",),
        ("0-0=" + "a" * 64,),
        ("1785034608101-x=" + "a" * 64,),
        ("1785034608101-0=" + "A" * 64,),
        ("1785034608101-0=" + "a" * 63,),
        ("1785034608101-0=" + "g" * 64,),
        (
            "1785034608101-0=" + "a" * 64,
            "1785034608101-0=" + "a" * 64,
        ),
    ),
)
def test_parse_expected_events_rejects_unsafe_manifest(values: tuple[str, ...]):
    with pytest.raises(LegacyEventResolutionError):
        parse_expected_events(values)


def test_canonical_payload_sha256_is_stable_across_mapping_order():
    payload = {
        "event": "node_failed",
        "job_id": "871ec6e9-1ac0-458c-8870-15e7684cf49f",
        "node_execution_id": "2f387cc9-fd8b-451d-a7eb-9d4df500501a",
        "error": "smart_trim input video has no detectable duration",
    }
    expected = hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()

    assert canonical_payload_sha256(payload) == expected
    assert canonical_payload_sha256(dict(reversed(tuple(payload.items())))) == expected


@pytest.mark.parametrize(
    "payload",
    (
        {"event": 1},
        {1: "node_failed"},
        {"event": True},
    ),
)
def test_canonical_payload_sha256_rejects_non_string_redis_fields(payload: dict):
    with pytest.raises(LegacyEventResolutionError):
        canonical_payload_sha256(payload)


@pytest.mark.anyio
async def test_dry_run_validates_terminal_event_without_mutation(
    resolution_db: AsyncSession,
):
    payload, message_id = await seed_terminal_event(resolution_db)
    redis = FakeRedis({message_id: payload})

    report = await resolve_legacy_worker_events(
        resolution_db,
        redis,
        request_for(message_id, payload),
    )

    assert report.applied is False
    assert report.xack_count == 0
    assert report.final_pending == 1
    assert [
        (
            item.message_id,
            item.payload_sha256,
            item.job_id,
            item.node_execution_id,
        )
        for item in report.candidates
    ] == [
        (
            message_id,
            canonical_payload_sha256(payload),
            uuid.UUID(payload["job_id"]),
            uuid.UUID(payload["node_execution_id"]),
        )
    ]
    assert redis.ack_calls == []
    assert (
        await resolution_db.scalar(
            select(func.count()).select_from(LegacyWorkerEventResolution)
        )
        == 0
    )


@pytest.mark.anyio
async def test_dry_run_rejects_complete_pel_set_drift(resolution_db: AsyncSession):
    payload, message_id = await seed_terminal_event(resolution_db)
    extra_payload = dict(payload, node_execution_id=str(uuid.uuid4()))
    redis = FakeRedis(
        {
            message_id: payload,
            "1785034608559-0": extra_payload,
        }
    )

    with pytest.raises(LegacyEventResolutionError, match="complete pending set"):
        await resolve_legacy_worker_events(
            resolution_db,
            redis,
            request_for(message_id, payload),
        )

    assert redis.ack_calls == []


@pytest.mark.anyio
async def test_dry_run_rejects_payload_hash_drift(resolution_db: AsyncSession):
    payload, message_id = await seed_terminal_event(resolution_db)
    redis = FakeRedis({message_id: dict(payload, error="changed")})

    with pytest.raises(LegacyEventResolutionError, match="payload hash"):
        await resolve_legacy_worker_events(
            resolution_db,
            redis,
            request_for(message_id, payload),
        )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("mutate_payload", "message"),
    (
        (lambda payload: dict(payload, worker_id="vision-worker@legacy:1"), "execution claim"),
        (lambda payload: dict(payload, started_at=NOW.isoformat()), "execution claim"),
        (lambda payload: dict(payload, event="node_completed"), "node_failed"),
        (lambda payload: dict(payload, job_id="not-a-uuid"), "identifiers"),
    ),
)
async def test_dry_run_rejects_ineligible_event_payload(
    resolution_db: AsyncSession,
    mutate_payload: Callable[[dict[str, str]], dict[str, str]],
    message: str,
):
    payload, message_id = await seed_terminal_event(resolution_db)
    ineligible = mutate_payload(payload)
    redis = FakeRedis({message_id: ineligible})

    with pytest.raises(LegacyEventResolutionError, match=message):
        await resolve_legacy_worker_events(
            resolution_db,
            redis,
            request_for(message_id, ineligible),
        )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("seed_options", "message"),
    (
        ({"schedule_state": "OPEN"}, "schedule"),
        ({"guarded_job": True}, "guarded"),
        ({"job_status": JobStatus.RUNNING}, "job"),
        ({"node_status": NodeStatus.RUNNING}, "active node"),
        ({"job_status": JobStatus.FAILED}, "job"),
        ({"node_status": NodeStatus.FAILED}, "node"),
        ({"task_state": "failed"}, "task"),
        ({"channel_halted": False}, "channel"),
    ),
)
async def test_dry_run_rejects_nonterminal_database_authority(
    resolution_db: AsyncSession,
    seed_options: dict,
    message: str,
):
    payload, message_id = await seed_terminal_event(resolution_db, **seed_options)
    redis = FakeRedis({message_id: payload})

    with pytest.raises(LegacyEventResolutionError, match=message):
        await resolve_legacy_worker_events(
            resolution_db,
            redis,
            request_for(message_id, payload),
        )


@pytest.mark.anyio
async def test_apply_commits_archive_before_ack_and_records_acknowledgement(
    resolution_db: AsyncSession,
):
    payload, message_id = await seed_terminal_event(resolution_db)
    redis = FakeRedis({message_id: payload})
    archive_visible_before_ack = False

    async def observe_archive():
        nonlocal archive_visible_before_ack
        assert resolution_db.bind is not None
        async with resolution_db.bind.connect() as connection:
            count = await connection.scalar(
                select(func.count()).select_from(LegacyWorkerEventResolution)
            )
        archive_visible_before_ack = count == 1

    redis.before_xack = observe_archive

    report = await resolve_legacy_worker_events(
        resolution_db,
        redis,
        request_for(message_id, payload, apply=True),
    )

    assert archive_visible_before_ack is True
    assert report.applied is True
    assert report.xack_count == 1
    assert report.final_pending == 0
    assert len(report.resolution_ids) == 1
    assert redis.ack_calls == [
        ("vp:events", "orchestrator", (message_id,)),
    ]
    resolution = (
        await resolution_db.execute(select(LegacyWorkerEventResolution))
    ).scalar_one()
    assert resolution.id == report.resolution_ids[0]
    assert resolution.payload_json == payload
    assert resolution.payload_sha256 == canonical_payload_sha256(payload)
    assert resolution.operator_id == "operator:wenjieliu"
    assert resolution.observed_job_status == "CANCELLED"
    assert resolution.observed_node_status == "CANCELLED"
    assert resolution.observed_task_state == "held"
    assert resolution.acknowledged_at is not None


@pytest.mark.anyio
async def test_apply_rechecks_payload_after_archive_before_ack(
    resolution_db: AsyncSession,
):
    payload, message_id = await seed_terminal_event(resolution_db)
    redis = FakeRedis({message_id: payload})

    def mutate_on_second_read(call_count: int):
        if call_count == 2:
            redis.entries[message_id] = dict(payload, error="changed after archive")

    redis.before_xrange = mutate_on_second_read

    with pytest.raises(LegacyEventResolutionError, match="payload hash"):
        await resolve_legacy_worker_events(
            resolution_db,
            redis,
            request_for(message_id, payload, apply=True),
        )

    assert redis.ack_calls == []
    resolution = (
        await resolution_db.execute(select(LegacyWorkerEventResolution))
    ).scalar_one()
    assert resolution.acknowledged_at is None


@pytest.mark.anyio
async def test_apply_fails_closed_on_partial_ack_and_can_resume(
    resolution_db: AsyncSession,
):
    payload, message_id = await seed_terminal_event(resolution_db)
    redis = FakeRedis({message_id: payload})
    redis.xack_override = 0

    with pytest.raises(LegacyEventResolutionError, match="acknowledgement count"):
        await resolve_legacy_worker_events(
            resolution_db,
            redis,
            request_for(message_id, payload, apply=True),
        )

    resolution = (
        await resolution_db.execute(select(LegacyWorkerEventResolution))
    ).scalar_one()
    assert resolution.acknowledged_at is None
    original_resolution_id = resolution.id
    await resolution_db.rollback()
    redis.xack_override = None

    report = await resolve_legacy_worker_events(
        resolution_db,
        redis,
        request_for(message_id, payload, apply=True),
    )

    assert report.resolution_ids == (original_resolution_id,)
    assert report.xack_count == 1
    assert report.final_pending == 0
    assert (
        await resolution_db.scalar(
            select(func.count()).select_from(LegacyWorkerEventResolution)
        )
        == 1
    )


@pytest.mark.anyio
async def test_apply_recovers_when_exact_archive_exists_and_pel_is_empty(
    resolution_db: AsyncSession,
):
    payload, message_id = await seed_terminal_event(resolution_db)
    redis = FakeRedis({message_id: payload})
    first = await resolve_legacy_worker_events(
        resolution_db,
        redis,
        request_for(message_id, payload, apply=True),
    )
    redis.ack_calls.clear()

    recovered = await resolve_legacy_worker_events(
        resolution_db,
        redis,
        request_for(message_id, payload, apply=True),
    )

    assert recovered.applied is True
    assert recovered.resolution_ids == first.resolution_ids
    assert recovered.xack_count == 0
    assert recovered.final_pending == 0
    assert redis.ack_calls == []


@pytest.mark.anyio
async def test_apply_rejects_existing_archive_with_immutable_drift(
    resolution_db: AsyncSession,
):
    payload, message_id = await seed_terminal_event(resolution_db)
    resolution_db.add(
        LegacyWorkerEventResolution(
            redis_stream="vp:events",
            consumer_group="orchestrator",
            message_id=message_id,
            payload_sha256="f" * 64,
            payload_json=dict(payload, error="different archived event"),
            event_type="node_failed",
            job_id=uuid.UUID(payload["job_id"]),
            node_execution_id=uuid.UUID(payload["node_execution_id"]),
            resolution_reason="terminal_cancelled",
            operator_id="operator:other",
            observed_job_status="CANCELLED",
            observed_node_status="CANCELLED",
            observed_task_state="held",
            observed_channel_halted_at=NOW,
        )
    )
    await resolution_db.commit()
    redis = FakeRedis({message_id: payload})

    with pytest.raises(LegacyEventResolutionError, match="archive mismatch"):
        await resolve_legacy_worker_events(
            resolution_db,
            redis,
            request_for(message_id, payload, apply=True),
        )

    assert redis.ack_calls == []
