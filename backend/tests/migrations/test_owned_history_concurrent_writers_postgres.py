"""Remaining A2 cross-writer cases; parent-owned opt-in PostgreSQL only."""
from __future__ import annotations

import asyncio
import hashlib
import uuid
from contextlib import aclosing
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from app.models.channel_agent import PublishingAccount
from app.services import owned_seed_inventory as service
from app.services import owned_seed_inventory_history as history
from app.services.registered_worker_event_receipt import RegisteredWorkerEventReceiptService, parse_registered_worker_event
from tests.migrations.owned_history_postgres import a2_pg as a2_pg, wait_blocked
from tests.migrations.test_owned_history_seal_postgres import a2_env as a2_env, approve, seal
from tests.worker.ack_drill_postgres import ack_drill_runtime as _ack_drill_runtime


def old_event(h, *, new_delivery=False):
    receipt = h.case.rows["registered_worker_event_receipts"][0]
    return parse_registered_worker_event(redis_stream=receipt["redis_stream"], consumer_group=receipt["consumer_group"],
        message_id="9000-0" if new_delivery else receipt["message_id"], payload=receipt["payload_json"])


@pytest.mark.parametrize("seal_first", [True, False])
async def test_actual_new_receipt_delivery_cannot_cross_retirement_seal(a2_env, monkeypatch, seal_first):
    h = a2_env
    receipt_service = RegisteredWorkerEventReceiptService(h.case.registered.session)
    event = old_event(h, new_delivery=True)
    calls = []
    async def callback(*_args):
        calls.append("callback")
        pytest.fail("historical applied event must not execute callback again")
    if not seal_first:
        written, waiting, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
        pids = {}
        add = receipt_service._add_event_delivery
        lock = service._lock_history_scope
        async def pending_delivery(db, **kwargs):
            value = await add(db, **kwargs)
            pids["writer"] = await db.scalar(text("SELECT pg_backend_pid()"))
            written.set()
            await asyncio.wait_for(release.wait(), 8)
            return value
        async def pending_seal(db, *args, **kwargs):
            pids["sealer"] = await db.scalar(text("SELECT pg_backend_pid()"))
            waiting.set()
            return await lock(db, *args, **kwargs)
        monkeypatch.setattr(receipt_service, "_add_event_delivery", pending_delivery)
        monkeypatch.setattr(service, "_lock_history_scope", pending_seal)
        writer = asyncio.create_task(receipt_service.accept_and_apply(event, callback))
        qualifier = None
        try:
            await asyncio.wait_for(written.wait(), 5)
            qualifier = asyncio.create_task(approve(h))
            await asyncio.wait_for(waiting.wait(), 5)
            await wait_blocked(h.case.owner, pids["sealer"], pids["writer"])
            release.set()
            assert await asyncio.wait_for(writer, 5) is not None
            response = await asyncio.wait_for(qualifier, 5)
            assert response.status_code == 409 and response.json()["detail"] != "owned_inventory_v2_activation_disabled"
            assert calls == []
        finally:
            release.set()
            for task in (writer, qualifier):
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(*(t for t in (writer, qualifier) if t is not None), return_exceptions=True)
        return
    entered = asyncio.Event()
    pid = None
    original = receipt_service._authority_locker
    async def signalled(db, *args, **kwargs):
        nonlocal pid
        pid = await db.scalar(text("SELECT pg_backend_pid()"))
        entered.set()
        return await original(db, *args, **kwargs)
    receipt_service._authority_locker = signalled
    task = None
    try:
        async with h.case.sessions() as sealer:
            await seal(h, sealer)
            blocker = await sealer.scalar(text("SELECT pg_backend_pid()"))
            task = asyncio.create_task(receipt_service.accept_and_apply(event, callback))
            await asyncio.wait_for(entered.wait(), 5)
            await wait_blocked(h.case.owner, pid, blocker)
            async with h.case.owner.transaction():
                await h.case.owner.fetch("SELECT id FROM node_executions FOR UPDATE NOWAIT")
            await sealer.commit()
            with pytest.raises(DBAPIError, match="owned_history_sealed"):
                await asyncio.wait_for(task, 5)
        assert calls == []
        assert await h.case.owner.fetchval("SELECT count(*) FROM registered_worker_event_deliveries WHERE message_id='9000-0'") == 0
    finally:
        if task is not None and not task.done():
            task.cancel()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)


async def test_actual_already_terminal_task_event_ack_waits_without_redis_mutation(a2_env, monkeypatch):
    h = a2_env
    entered = asyncio.Event()
    pid = None
    original = RegisteredWorkerEventReceiptService._lock_job_node_registration
    async def signalled(cls, db, **kwargs):
        nonlocal pid
        pid = await db.scalar(text("SELECT pg_backend_pid()"))
        entered.set()
        return await original(db, **kwargs)
    monkeypatch.setattr(RegisteredWorkerEventReceiptService, "_lock_job_node_registration", classmethod(signalled))
    class NoAck:
        async def xack(self, *_args):
            pytest.fail("certified terminal ACK replay must not mutate Redis")
    delivery_id = uuid.UUID(h.case.rows["registered_worker_event_deliveries"][0]["id"])
    receipt_service = RegisteredWorkerEventReceiptService(h.case.registered.session)
    task = None
    try:
        async with h.case.sessions() as sealer:
            await seal(h, sealer)
            blocker = await sealer.scalar(text("SELECT pg_backend_pid()"))
            task = asyncio.create_task(receipt_service._acknowledge_delivery_id(NoAck(), delivery_id))
            await asyncio.wait_for(entered.wait(), 5)
            await wait_blocked(h.case.owner, pid, blocker)
            async with h.case.owner.transaction():
                await h.case.owner.fetch("SELECT id FROM node_executions FOR UPDATE NOWAIT")
            await sealer.commit()
            await asyncio.wait_for(task, 5)
    finally:
        if task is not None and not task.done():
            task.cancel()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)


@pytest.fixture
async def a2_active_worker(a2_env, tmp_path):
    h = a2_env
    path = tmp_path / "synthetic-input.bin"
    path.write_bytes(b"synthetic owned fixture bytes, never uploaded")
    database = SimpleNamespace(owner_url=h.case.target, runtime_url=h.case.urls["worker"],
        operator_url=h.case.urls["operator"], role=h.case.urls["worker"].username,
        service="a2-worker-" + uuid.uuid4().hex, token_hash=hashlib.sha256(uuid.uuid4().bytes).hexdigest())
    await h.case.owner.execute("UPDATE runtime_schedules SET state='OPEN' WHERE service_name='videoprocess'")
    generator = _ack_drill_runtime.__wrapped__(database, [{"input": str(path)}], SimpleNamespace(param=None))
    async with aclosing(generator):
        worker = await anext(generator)
        async with h.case.sessions() as db:
            db.add(PublishingAccount(id=worker.account_id, channel_profile_id=worker.channel_id,
                account_label="unclassified fixture producer", platform_account_id="", default_privacy="unlisted"))
            await db.commit()
        yield h, worker


def reserve_arguments(worker):
    claim = worker.context.execution_claim
    return {"registration": claim.worker_registration_id, "epoch": claim.worker_lease_epoch,
            "worker": claim.worker_id, "started": claim.started_at, "job": worker.job_id,
            "node": worker.node_id, "artifact": worker.artifact_id, "sha": worker.context.content_sha256}


RESERVE = text("""SELECT vp_reserve_worker_youtube_upload(:registration,:epoch,:worker,:started,
    :job,:node,:artifact,:sha,'Synthetic owned fixture','unlisted')""")


@pytest.mark.parametrize("qualifier_first", [True, False])
async def test_actual_restricted_unknown_reserve_serializes_on_the_same_schedule(a2_active_worker, qualifier_first):
    h, worker = a2_active_worker
    task = None
    # These calls exercise the actual final-entry serialization point. A2's
    # public route still rejects OPEN/busy and never grants v2 activation.
    async with h.case.sessions() as observer:
        initial = await history.load_owned_history_evidence(observer, platform_channel_id=h.env.scope["platform_channel_id"])
        sources = service._retirement_sources(initial, requested=True)
        await observer.rollback()
        observations = await service._observe_retirement(sources, observed_at=initial.observed_at)
    async with worker.sessions() as writer, h.case.sessions() as qualifier:
        writer_pid = await writer.scalar(text("SELECT pg_backend_pid()"))
        qualifier_pid = await qualifier.scalar(text("SELECT pg_backend_pid()"))
        locators = service.OwnedHistoryLocators.model_validate(h.data["history_locators"])
        try:
            if qualifier_first:
                await h.case.owner.execute("UPDATE runtime_schedules SET state='CLOSED' WHERE service_name='videoprocess'")
                await service._lock_history_scope(qualifier, h.env.channel_id, locators, sources)
                task = asyncio.create_task(writer.scalar(RESERVE, reserve_arguments(worker)))
                await wait_blocked(h.case.owner, writer_pid, qualifier_pid)
                async with h.case.owner.transaction():
                    await h.case.owner.fetch("SELECT id FROM node_executions WHERE id=$1 FOR UPDATE NOWAIT", worker.node_id)
                await qualifier.rollback()
                with pytest.raises(DBAPIError, match="schedule_authority_changed"):
                    await asyncio.wait_for(task, 5)
                await writer.rollback()
                assert await h.case.owner.fetchval("SELECT count(*) FROM youtube_upload_operations WHERE node_execution_id=$1", worker.node_id) == 0
            else:
                operation_id = await writer.scalar(RESERVE, reserve_arguments(worker))
                assert isinstance(operation_id, uuid.UUID)
                task = asyncio.create_task(service._lock_history_scope(qualifier, h.env.channel_id, locators, sources))
                await wait_blocked(h.case.owner, qualifier_pid, writer_pid)
                await writer.commit()
                await asyncio.wait_for(task, 5)
                fresh = await history.load_owned_history_evidence(qualifier, platform_channel_id=h.env.scope["platform_channel_id"])
                snapshot = history.OwnedHistorySnapshot.from_rows(fresh.rows.as_dict(), platform_channel_id=fresh.platform_channel_id,
                    observed_at=fresh.observed_at, redis_observations=observations[1])
                row = await qualifier.get(service.OwnedSeedInventory, uuid.UUID(h.result["id"]))
                with pytest.raises(service.OwnedInventoryError, match="owned_history_unclassified"):
                    service._assess_qualified_draft(snapshot, row, "fixture", snapshot.observed_at)
                await qualifier.rollback()
                await h.case.owner.execute("UPDATE runtime_schedules SET state='CLOSED' WHERE service_name='videoprocess'")
                response = await approve(h)
                assert response.status_code == 409 and response.json()["detail"] != "owned_inventory_v2_activation_disabled"
        finally:
            if task is not None and not task.done():
                task.cancel()
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)
            await qualifier.rollback()
            await writer.rollback()
