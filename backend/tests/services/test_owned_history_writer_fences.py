from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from app.services import job_execution_authority as authority
from app.services.registered_worker_event_receipt import RegisteredWorkerEventReceiptService
from app.models.registered_worker_event_receipt import WorkerTaskDispatch
from test_registered_worker_event_receipt import receipt_session_factory as receipt_session_factory
from test_legacy_worker_event_resolution import (
    FakeRedis, request_for, resolution_db as resolution_db, seed_terminal_event,
)
from app.services import legacy_worker_event_resolution as legacy
from app.models.legacy_worker_event_resolution import LegacyWorkerEventResolution


async def test_receipt_ack_entry_joins_native_channel_schedule_fence_before_lower_locks(monkeypatch):
    calls = []
    async def entry(db, job_id):
        calls.extend(["channel", "schedule"])
        return None, [], SimpleNamespace()
    monkeypatch.setattr(authority, "lock_job_execution_entry", entry, raising=False)
    class DB:
        def get_bind(self):
            return SimpleNamespace(dialect=postgresql.dialect())
        async def execute(self, statement, *args):
            sql = str(statement.compile(dialect=postgresql.dialect()))
            calls.append("job" if "FROM jobs" in sql else "node" if "FROM node_executions" in sql else "registration")
            return SimpleNamespace(scalar_one_or_none=lambda: SimpleNamespace())
    await RegisteredWorkerEventReceiptService._lock_job_node_registration(
        DB(), job_id=uuid.uuid4(), node_execution_id=uuid.uuid4(), registration_id=uuid.uuid4())
    assert calls[:4] == ["channel", "schedule", "job", "node"]


async def test_native_shared_entry_precedes_job_lock_without_schedule_state_override(monkeypatch):
    calls = []
    job_id, channel_id, task_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    class DB:
        async def execute(self, statement):
            sql = str(statement.compile(dialect=postgresql.dialect()))
            calls.append(sql)
            if "production_tasks" in sql:
                return SimpleNamespace(all=lambda: [(task_id, channel_id, job_id)])
            return SimpleNamespace(scalar_one_or_none=lambda: SimpleNamespace(id=channel_id))
    async def schedule(db):
        calls.append("schedule FOR UPDATE")
        return SimpleNamespace(state="CLOSED"), False
    monkeypatch.setattr(authority, "get_or_create_and_lock_runtime_schedule", schedule)
    channel, refs, schedule = await authority.lock_job_execution_entry(DB(), job_id)
    assert channel.id == channel_id and refs == [(task_id, channel_id)] and schedule.state == "CLOSED"
    assert "channel_profiles" in calls[1] and calls[1].endswith("FOR UPDATE")
    assert calls[2] == "schedule FOR UPDATE"


async def test_dispatch_batch_enters_all_channels_before_its_first_row_lock(receipt_session_factory, monkeypatch):
    jobs = [uuid.uuid4(), uuid.uuid4()]
    async with receipt_session_factory() as db:
        for job in jobs:
            db.add(WorkerTaskDispatch(job_id=job, node_execution_id=uuid.uuid4(), dispatch_key=uuid.uuid4(),
                redis_stream="vp:tasks:ffmpeg_go", consumer_group="ffmpeg_go-workers", payload_sha256="a" * 64,
                payload_json={}, delivery_state="pending"))
        await db.commit()
    calls = []
    async def entry(db, job_ids):
        calls.append(set(job_ids))
        assert not any(db.is_modified(row) for row in db.identity_map.values())
    monkeypatch.setattr(authority, "lock_job_execution_entries", entry, raising=False)
    rows = await RegisteredWorkerEventReceiptService(receipt_session_factory)._begin_dispatch_attempts(receipt_id=None, limit=100)
    assert calls == [set(jobs)] and len(rows) == 2


async def test_legacy_archive_and_ack_enter_common_fence_before_lower_locks(resolution_db, monkeypatch):
    payload, message_id = await seed_terminal_event(resolution_db)
    calls = []
    entry = authority.lock_job_execution_entries
    async def record(db, jobs, **kwargs):
        calls.append(set(jobs))
        return await entry(db, jobs, **kwargs)
    monkeypatch.setattr(authority, "lock_job_execution_entries", record)
    report = await legacy.resolve_legacy_worker_events(
        resolution_db, FakeRedis({message_id: payload}), request_for(message_id, payload, apply=True))
    assert report.applied and report.xack_count == 1
    assert calls == [{uuid.UUID(payload["job_id"])}] * 2


async def test_legacy_entry_discovery_drift_cannot_archive_or_ack(resolution_db):
    payload, message_id = await seed_terminal_event(resolution_db)
    redis = FakeRedis({message_id: payload})
    def mutate_after_discovery(count):
        if count == 2:
            redis.entries[message_id] = dict(payload, job_id=str(uuid.uuid4()))
    redis.before_xrange = mutate_after_discovery
    with pytest.raises(legacy.LegacyEventResolutionError, match="payload hash"):
        await legacy.resolve_legacy_worker_events(resolution_db, redis, request_for(message_id, payload, apply=True))
    assert redis.ack_calls == []
    assert not (await resolution_db.scalars(select(LegacyWorkerEventResolution))).all()


async def test_entry_rejects_task_channel_rebinding_while_waiting_for_schedule(monkeypatch):
    job_id, task_id, channel_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    reads = 0
    class DB:
        async def execute(self, statement):
            nonlocal reads
            sql = str(statement.compile(dialect=postgresql.dialect()))
            if "production_tasks" in sql:
                reads += 1
                return SimpleNamespace(all=lambda: [(task_id, channel_id if reads == 1 else uuid.uuid4(), job_id)])
            return SimpleNamespace(scalar_one_or_none=lambda: SimpleNamespace(id=channel_id))
    async def schedule(db):
        return SimpleNamespace(state="CLOSED"), False
    monkeypatch.setattr(authority, "get_or_create_and_lock_runtime_schedule", schedule)
    with pytest.raises(authority.JobExecutionAuthorityBlocked, match="authority changed"):
        await authority.lock_job_execution_entries(DB(), [job_id])
