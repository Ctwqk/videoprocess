"""Parent-only real DB expiry/receipt checks; Redis ACK transport is a test double."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace
import uuid

import asyncpg
import pytest
from sqlalchemy import text

from app.orchestrator.engine import JobEngine
from app.services.job_execution_authority import persist_registered_worker_artifact
from tests.services.test_owned_producer_postgres import (
    d_database as d_database, d_pg as _d_pg, media_paths as media_paths, reserve, transition,
)
from tests.worker.test_registered_retry_postgres import retry_pg as _retry_pg


@pytest.fixture
async def elapsed_pg(d_database, monkeypatch, media_paths, request):
    from tests.channel_agent import test_owned_inventory as admission_fixture

    class HistoricalApprovalClock(datetime):
        @classmethod
        def now(cls, tz=None):
            # Construct a valid seven-day immutable fixture window ending soon.
            # Neither inventory._now nor PostgreSQL clock_timestamp is replaced.
            return datetime.now(tz) - timedelta(days=7) + timedelta(hours=1, seconds=20)

    monkeypatch.setattr(admission_fixture, "datetime", HistoricalApprovalClock)
    fixture = _d_pg.__wrapped__(d_database, monkeypatch, media_paths, request)
    try:
        env = await anext(fixture)
        remaining = await env.native.owner.fetchval(
            "SELECT extract(epoch FROM expires_at-clock_timestamp()) FROM owned_seed_inventories WHERE id=$1", env.inventory_id)
        assert 5 < remaining <= 20, "fixture setup consumed the real expiry window"
        yield env
    finally:
        await fixture.aclose()


async def wait_for_expiry(env):
    for _ in range(250):
        row = await env.native.owner.fetchrow(
            "SELECT state,clock_timestamp()>=expires_at expired FROM owned_seed_inventories WHERE id=$1", env.inventory_id)
        assert row["state"] == "approved"
        if row["expired"]:
            return
        await asyncio.sleep(0.1)
    pytest.fail("real database expiry was not observed within the fixture bound")


@pytest.mark.parametrize("boundary", ["reserve", "attempt", "fence"])
async def test_pg_real_elapsed_expiry_denies_new_effect_without_state_mutation(elapsed_pg, boundary):
    env, operation_id = elapsed_pg, None
    if boundary != "reserve":
        operation_id = (await env.store.claim(env.native.context)).operation.id
    if boundary == "fence":
        await transition(env, operation_id, "attempting")
    await wait_for_expiry(env)
    with pytest.raises(asyncpg.RaiseError, match="owned_inventory_producer_inactive"):
        if boundary == "reserve":
            await reserve(env)
        else:
            await transition(env, operation_id, "attempting" if boundary == "attempt" else "fence")
    rows = await env.native.owner.fetch(
        "SELECT request_attempted_at,manager_task_id FROM youtube_upload_operations WHERE production_task_id=$1", env.task_id)
    assert len(rows) == int(boundary != "reserve")
    assert all(r["manager_task_id"] is None and (r["request_attempted_at"] is not None) == (boundary == "fence") for r in rows)


async def test_pg_registered_receipt_and_ack_settle_after_real_expiry(elapsed_pg, d_database, tmp_path, monkeypatch):
    env = elapsed_pg
    context = env.native.context
    claim = await env.store.claim(context)
    async with env.store.submission_fence(context):
        await env.store.mark_attempting(claim.operation.id, context=context)
        await env.store.mark_submitted(claim.operation.id, str(uuid.uuid4()), context=context)
    await wait_for_expiry(env)
    receipt = {"video_id": "abcdefghijk", "title": context.title, "privacy": "unlisted"}
    await env.store.mark_succeeded(claim.operation.id, "abcdefghijk", receipt, context=context)
    async with env.native.sessions() as db:
        output_id = await persist_registered_worker_artifact(db, context.execution_claim,
            filename="upload-receipt.json", mime_type="application/json", file_size=1,
            storage_backend="local", storage_path="artifacts/upload-receipt.json", media_info=receipt)
        await db.commit()
    # Reuse the actual restricted runtime/receipt fixture, selecting our newly
    # registered receipt artifact instead of its original synthetic input.
    worker = SimpleNamespace(**{**vars(env.native), "artifact_id": output_id})
    fixture = _retry_pg.__wrapped__(d_database, worker, tmp_path, monkeypatch, SimpleNamespace(param="completion"))
    try:
        case = await anext(fixture)
        async def apply(db, receipt_row, event):
            assert await db.scalar(text("SELECT session_user")) == case.role
            await JobEngine().apply_registered_worker_event(db, receipt_row, event)
        receipt_id = await case.service.accept_and_apply(case.event, apply)
        assert receipt_id is not None

        class RedisAckTransport:
            def __init__(self):
                self.calls = []

            async def xack(self, stream, group, message):
                self.calls.append((stream, group, message))
                return 1

        redis = RedisAckTransport()
        await case.service.acknowledge_applied(redis, case.event)
        row = await env.native.owner.fetchrow(
            "SELECT application_state,ack_state,source_task_ack_state FROM registered_worker_event_receipts WHERE id=$1", receipt_id)
        assert dict(row) == {"application_state": "applied", "ack_state": "acknowledged", "source_task_ack_state": "acknowledged"}
        assert set(redis.calls) == {
            (case.event.redis_stream, case.event.consumer_group, case.event.message_id),
            ("vp:tasks:youtube_publisher", "youtube_publisher-workers", env.native.message_id),
        }
        assert await env.native.owner.fetchval("SELECT status::text FROM node_executions WHERE id=$1", env.native.node_id) == "SUCCEEDED"
        operation = await env.native.owner.fetchrow("SELECT status,request_attempted_at,completed_at FROM youtube_upload_operations WHERE id=$1", claim.operation.id)
        assert operation["status"] == "succeeded" and operation["request_attempted_at"] < operation["completed_at"]
    finally:
        await fixture.aclose()
