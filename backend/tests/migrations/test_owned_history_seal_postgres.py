"""Parent-run A2 barriers. No HTTP server, real Redis, upload, or activation.

Explicit fixture approval below is isolated database data for trigger tests;
the production v2 approve route is always asserted disabled.
"""
from __future__ import annotations

import asyncio
import uuid
from contextlib import aclosing
from types import SimpleNamespace

import asyncpg
import pytest
from sqlalchemy import text, update
from sqlalchemy.exc import DBAPIError

from app.models.channel_agent import ChannelProfile, ProductionTask
from app.models.job import Job
from app.models.owned_seed_inventory import OwnedSeedInventory
from app.services import owned_seed_inventory as service
from app.services import owned_seed_inventory_history as history
from app.services.job_execution_authority import lock_job_execution_entry
from app.services.registered_worker_event_receipt import (
    RegisteredWorkerEventReceiptService, parse_registered_worker_event,
)
from tests.api.test_owned_seed_inventory import inventory_env as _inventory_env
from tests.migrations.owned_history_postgres import (
    ReadonlyRedis, a2_pg as a2_pg, catalogue, dsn, qualify, wait_blocked,
)


@pytest.fixture
async def a2_env(a2_pg, monkeypatch):
    case = a2_pg
    generator = _inventory_env.__wrapped__(monkeypatch, SimpleNamespace(param=case.target.render_as_string(hide_password=False)))
    async with aclosing(generator):
        env = await anext(generator)
        env.storage.blobs["assets/owned.mp4"] = b"a" * 100
        redis = ReadonlyRedis(case.rows)
        monkeypatch.setattr(service, "_history_redis", lambda: redis)
        data = await env.data("a2-history")
        data.update(version=2, history_locators={"operations": [], "retired_unassigned_preupload": {
            "operation_id": history.RETIRED_TUPLE[0], "legacy_account_id": history.RETIRED_TUPLE[4],
            "legacy_channel_profile_id": history.RETIRED_TUPLE[5]}})
        response = await env.client.post(env.url, json=data)
        assert response.status_code == 200, response.text
        yield SimpleNamespace(case=case, env=env, result=response.json(), data=data, redis=redis)


async def seal(h, db):
    """Only explicit synthetic fixture authority, never the public approval path."""
    row = await db.get(OwnedSeedInventory, uuid.UUID(h.result["id"]))
    await service._requalify_v2_draft(db, row, "a2-fixture")
    await db.execute(update(OwnedSeedInventory).where(OwnedSeedInventory.id == row.id).values(
        approved_at=await db.scalar(text("SELECT clock_timestamp()")), approved_by="a2-fixture",
        approval_reference="fixture:trigger-test-only", state="approved"))


async def approve(h):
    return await h.env.client.post(f"{h.env.url}/{h.result['id']}/approve", json={
        "manifest_sha256": h.result["manifest_sha256"], "approval_reference": "fixture:requalify",
        "tick_interval_minutes": 1})


async def mutate_producer(h, action):
    base = "/api/v1/channel-agent"
    channel, account = history.RETIRED_TUPLE[5], history.RETIRED_TUPLE[4]
    if action == "account_patch":
        return await h.env.client.patch(f"{base}/channels/{channel}/accounts/{account}", json={"account_label": "changed-observed-account"})
    if action == "account_resume":
        return await h.env.client.post(f"{base}/accounts/{account}/resume")
    if action == "source_delete":
        return await h.env.client.delete(f"/api/v1/assets/{h.case.rows['assets'][0]['id']}")
    async with h.case.sessions() as writer:
        await lock_job_execution_entry(writer, uuid.UUID(history.RETIRED_TUPLE[2]))
        await writer.execute(update(ProductionTask).where(ProductionTask.id == uuid.UUID(history.RETIRED_TUPLE[1]))
            .values(channel_profile_id=h.env.channel_id))
        await writer.commit()
    return SimpleNamespace(status_code=200)


async def test_actual_catalog_preserves_all_installed_function_authority(a2_pg):
    case = a2_pg
    expected = case.migration["ENTRY_JOBS"]
    assert len(case.before) == 57 and len(case.after) == 59
    for name, original in case.before.items():
        current = case.after[name]
        assert {k: v for k, v in original.items() if k != "source"} == {k: v for k, v in current.items() if k != "source"}
        if name in expected:
            injection = f"    -- owned_history_entry_040\n    PERFORM public.vp_owned_history_job_entry({expected[name]});\n"
            assert current["source"].count(injection) == 1
            assert current["source"].replace(injection, "") == original["source"]
        else:
            assert current == original
    assert set(case.after) - set(case.before) == {"vp_owned_history_job_entry", "vp_owned_history_seal_guard"}
    worker = await asyncpg.connect(dsn(case.urls["worker"]), timeout=5)
    try:
        for signature in ("vp_owned_history_job_entry(uuid)", "vp_owned_history_seal_guard()"):
            assert not await worker.fetchval("SELECT has_function_privilege(session_user,$1,'EXECUTE')", "public." + signature)
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await worker.execute("SELECT vp_owned_history_job_entry($1)", uuid.UUID(history.RETIRED_TUPLE[2]))
    finally:
        await worker.close()


@pytest.mark.parametrize("a2_pg", [False, True], indirect=True, ids=["emission-linked", "applied-receipt-null-link"])
async def test_actual_complete_sql_graph_and_fresh_readonly_qualification(a2_pg, monkeypatch):
    case = a2_pg
    certificate, redis = await qualify(case, monkeypatch)
    assert {call[0] for call in redis.calls} == {"get", "pending", "close"}
    assert len([call for call in redis.calls if call[0] == "get"]) == 4
    assert len([call for call in redis.calls if call[0] == "pending"]) == 6
    assert certificate["retained_facts"]["operation"]["status"] == "reserved"
    assert certificate["retained_facts"]["operation"]["request_attempted_at"] is None
    assert certificate["retained_facts"]["account"]["platform_account_id"] == ""
    async with case.sessions() as db:
        await db.execute(text("SET TRANSACTION READ ONLY"))
        pending = Job(pipeline_id=uuid.UUID(case.rows["jobs"][0]["pipeline_id"]), pipeline_snapshot={})
        db.add(pending)
        snapshot = await history.load_owned_history_evidence(db, platform_channel_id="UC" + "a" * 22)
        assert pending in db.new
        assert snapshot.observed_at.tzinfo is not None
        assert len(snapshot.rows.as_dict()["jobs"]) == 1
        assert "lease_secret_sha256" not in snapshot.rows.canonical_json
        assert "token_sha256" not in snapshot.rows.canonical_json


async def test_actual_v2_draft_requalifies_but_never_activates(a2_env):
    h = a2_env
    result = await approve(h)
    assert result.status_code == 409 and result.json()["detail"] == "owned_inventory_v2_activation_disabled"
    reread = await h.env.client.get(f"{h.env.url}/{h.result['id']}")
    assert reread.json()["manifest"] == h.result["manifest"]
    assert reread.json()["approved_at"] is None
    async with h.case.sessions() as db:
        assert (await db.get(ChannelProfile, h.env.channel_id)).owned_seed_inventory_id is None


@pytest.mark.parametrize("action", ["account_patch", "account_resume", "source_delete", "task_rebind"])
async def test_actual_qualification_io_is_unlocked_and_detects_committed_drift(a2_env, action):
    h = a2_env
    entered, release = asyncio.Event(), asyncio.Event()
    async def pause_read(_path):
        entered.set()
        await asyncio.wait_for(release.wait(), 5)
    h.env.storage.on_read = pause_read
    task = asyncio.create_task(approve(h))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        response = await asyncio.wait_for(mutate_producer(h, action), 3)
        assert response.status_code in {200, 204}
    finally:
        release.set()
        response = await asyncio.wait_for(task, 10)
    assert response.status_code == 409 and response.json()["detail"] != "owned_inventory_v2_activation_disabled"


@pytest.mark.parametrize("action", ["account_patch", "account_resume", "source_delete", "task_rebind"])
async def test_actual_final_requalification_fence_blocks_native_writers(a2_env, monkeypatch, action):
    h = a2_env
    locked, release = asyncio.Event(), asyncio.Event()
    original = service._lock_assets
    blocker_pid = None
    async def pause_after_entry(db, descriptors):
        nonlocal blocker_pid
        await original(db, descriptors)
        if not locked.is_set():
            blocker_pid = await db.scalar(text("SELECT pg_backend_pid()"))
            locked.set()
            await asyncio.wait_for(release.wait(), 8)
    monkeypatch.setattr(service, "_lock_assets", pause_after_entry)
    qualifier = asyncio.create_task(approve(h))
    writer_task = None
    try:
        await asyncio.wait_for(locked.wait(), 5)
        async with h.case.sessions() as writer:
            writer_pid = await writer.scalar(text("SELECT pg_backend_pid()"))
            async def write():
                if action == "source_delete":
                    await service.assert_asset_deletable(writer, uuid.UUID(h.case.rows["assets"][0]["id"]))
                else:
                    await service.lock_history_channel_mutation(writer, uuid.UUID(history.RETIRED_TUPLE[5]))
                await writer.rollback()
            writer_task = asyncio.create_task(write())
            await wait_blocked(h.case.owner, writer_pid, blocker_pid)
            assert not writer_task.done()
            release.set()
            response = await asyncio.wait_for(qualifier, 5)
            await asyncio.wait_for(writer_task, 5)
        assert response.json()["detail"] == "owned_inventory_v2_activation_disabled"
    finally:
        release.set()
        for task in (qualifier, writer_task):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(*(t for t in (qualifier, writer_task) if t is not None), return_exceptions=True)


def rpc(case, name):
    graph = case.rows
    att = graph["worker_task_delivery_attestations"][0]
    receipt = next(r for r in graph["registered_worker_event_receipts"] if r["event_type"] == "node_failed")
    retry = next(d for d in graph["worker_task_dispatches"] if d["origin_receipt_id"] is not None)
    uid = uuid.UUID
    if name == "recovery":
        return "SELECT vp_recover_registered_worker_node($1,$2)", [uid(history.RETIRED_TUPLE[2]), uid(history.RETIRED_TUPLE[3])]
    if name == "task_ack":
        return "SELECT vp_acknowledge_proven_worker_task_dispatch($1)", [uid(att["id"])]
    if name == "retry_release":
        return "SELECT vp_release_registered_retry_claim($1)", [uid(receipt["id"])]
    if name == "cancel_authorize":
        return "SELECT vp_authorize_cancelled_worker_task_ack($1)", [uid(retry["id"])]
    args = [uid(retry["id"]), retry["redis_stream"], retry["consumer_group"], retry["redis_message_id"],
            retry["payload_sha256"], uid(retry["dispatch_key"])]
    return "SELECT vp_acknowledge_cancelled_worker_task($1,$2,$3,$4,$5,$6)", args


@pytest.mark.parametrize("name", ["recovery", "task_ack", "retry_release", "cancel_authorize", "cancel_ack"])
async def test_actual_seal_first_rejects_restricted_rpc_after_observed_entry_wait(a2_env, name):
    h = a2_env
    sql, args = rpc(h.case, name)
    caller = await asyncpg.connect(dsn(h.case.urls["orchestrator"]), timeout=5, command_timeout=10)
    task = None
    try:
        async with h.case.sessions() as sealer:
            await seal(h, sealer)
            blocker = await sealer.scalar(text("SELECT pg_backend_pid()"))
            task = asyncio.create_task(caller.fetchval(sql, *args))
            await wait_blocked(h.case.owner, caller.get_server_pid(), blocker)
            # A lower-row NOWAIT probe must not be obstructed by the waiting RPC.
            async with h.case.owner.transaction():
                await h.case.owner.fetch("SELECT id FROM node_executions FOR UPDATE NOWAIT")
                await h.case.owner.fetch("SELECT id FROM worker_registrations FOR UPDATE NOWAIT")
            await sealer.commit()
            with pytest.raises(asyncpg.RaiseError, match="owned_history_sealed"):
                await asyncio.wait_for(task, 5)
    finally:
        if task is not None and not task.done():
            task.cancel()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await caller.close()


@pytest.mark.parametrize("name", ["recovery", "task_ack", "cancel_authorize"])
async def test_actual_rpc_first_holds_entry_until_seal_can_reload(a2_env, name):
    h = a2_env
    sql, args = rpc(h.case, name)
    caller = await asyncpg.connect(dsn(h.case.urls["orchestrator"]), timeout=5, command_timeout=10)
    transaction = caller.transaction()
    await transaction.start()
    task = None
    try:
        await caller.fetchval(sql, *args)
        async with h.case.sessions() as sealer:
            pid = await sealer.scalar(text("SELECT pg_backend_pid()"))
            # Use the same entry as requalification; no private helper grant.
            task = asyncio.create_task(lock_job_execution_entry(sealer, uuid.UUID(history.RETIRED_TUPLE[2])))
            await wait_blocked(h.case.owner, pid, caller.get_server_pid())
            await transaction.rollback()
            transaction = None
            await asyncio.wait_for(task, 5)
            await sealer.rollback()
            await seal(h, sealer)
            await sealer.commit()
    finally:
        if transaction is not None:
            await transaction.rollback()
        if task is not None and not task.done():
            task.cancel()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await caller.close()


@pytest.mark.parametrize("mutation", ["insert", "update", "delete"])
async def test_actual_sealed_graph_membership_backstop(a2_env, mutation):
    h = a2_env
    async with h.case.sessions() as db:
        await seal(h, db)
        await db.commit()
    async with h.case.sessions() as writer:
        await lock_job_execution_entry(writer, uuid.UUID(history.RETIRED_TUPLE[2]))
        statements = {
            "insert": "INSERT INTO node_executions(id,job_id,node_id,node_type,node_config,status,progress) VALUES(gen_random_uuid(),:job,'foreign','trim','{}','CANCELLED',0)",
            "update": "UPDATE node_executions SET error_message='changed' WHERE id=:node",
            "delete": "DELETE FROM worker_task_dispatches WHERE id=:dispatch",
        }
        with pytest.raises(DBAPIError, match="owned_history_sealed"):
            await writer.execute(text(statements[mutation]), {"job": uuid.UUID(history.RETIRED_TUPLE[2]),
                "node": uuid.UUID(history.RETIRED_TUPLE[3]), "dispatch": uuid.UUID(h.case.rows["worker_task_dispatches"][0]["id"])})
        await writer.rollback()


async def test_actual_registered_receipt_replay_serializes_without_new_callback_or_ack(a2_env):
    h = a2_env
    receipt = h.case.rows["registered_worker_event_receipts"][0]
    event = parse_registered_worker_event(redis_stream=receipt["redis_stream"], consumer_group=receipt["consumer_group"],
        message_id=receipt["message_id"], payload=receipt["payload_json"])
    calls = []
    async def callback(*_args):
        calls.append("callback")
        raise AssertionError("already-applied receipt must not be reapplied")
    service_under_test = RegisteredWorkerEventReceiptService(h.case.registered.session)
    async with h.case.sessions() as db:
        await seal(h, db)
        await db.commit()
    before = await h.case.owner.fetchval("SELECT count(*) FROM registered_worker_event_deliveries")
    result = await service_under_test.accept_and_apply(event, callback)
    assert result == uuid.UUID(receipt["id"]) and calls == []
    assert await h.case.owner.fetchval("SELECT count(*) FROM registered_worker_event_deliveries") == before
    async with h.case.registered.session() as db:
        with pytest.raises(DBAPIError):
            await db.execute(text("UPDATE node_executions SET worker_id=NULL WHERE id=:id"), {"id": uuid.UUID(history.RETIRED_TUPLE[3])})


async def test_actual_liveness_revocation_and_succession_keep_original_certificate(a2_env):
    h = a2_env
    async with h.case.sessions() as db:
        await seal(h, db)
        await db.commit()
        await db.execute(text("UPDATE worker_registrations SET heartbeat_at=heartbeat_at+interval '1 second', lease_expires_at=lease_expires_at+interval '1 second'"))
        await db.execute(text("UPDATE worker_admission_grants SET revoke_reason='normal-release', updated_at=clock_timestamp()"))
        await db.execute(update(OwnedSeedInventory).where(OwnedSeedInventory.id == uuid.UUID(h.result["id"])).values(
            state="revoked", revoked_at=await db.scalar(text("SELECT clock_timestamp()")), revoked_by="fixture:operator",
            succession_released_at=await db.scalar(text("SELECT clock_timestamp()"))))
        await db.commit()
        row = await db.get(OwnedSeedInventory, uuid.UUID(h.result["id"]))
        assert row.manifest_json == h.result["manifest"] and row.manifest_sha256 == h.result["manifest_sha256"]
        with pytest.raises(service.OwnedInventoryError, match="owned_inventory_historical_producer_pinned"):
            await service.lock_history_channel_mutation(db, uuid.UUID(history.RETIRED_TUPLE[5]))
        await db.rollback()
        snapshot = await history.load_owned_history_evidence(db, platform_channel_id=h.env.scope["platform_channel_id"])
        sources = service._retirement_sources(snapshot, requested=False)
        await db.rollback()
        observed = await service._observe_retirement(sources, observed_at=snapshot.observed_at)
        fresh = await history.load_owned_history_evidence(db, platform_channel_id=h.env.scope["platform_channel_id"])
        cert = service._qualified_retirement(fresh, sources, observed, "new-fixture-operator", "fixture:successor")
        assert cert == h.result["manifest"]["legacy_history"]["retired_unassigned_preupload"]
    assert await catalogue(h.case.owner) == h.case.granted_catalogue
