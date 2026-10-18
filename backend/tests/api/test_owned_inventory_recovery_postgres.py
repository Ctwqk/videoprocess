"""Real SQL recovery authority; only the explicitly configured disposable A2 cluster."""
from __future__ import annotations

import asyncio
import os
import secrets
import sys
import uuid
from contextlib import aclosing
from datetime import timedelta
from types import SimpleNamespace

import asyncpg
import pytest
from sqlalchemy import text

from app.models.channel_agent import ChannelProfile
from app.models.owned_seed_inventory import OwnedSeedInventory
from app.services import owned_inventory_recovery as recovery
from tests.api.test_owned_seed_inventory import approval, inventory_env as _inventory_env
from tests.migrations import owned_history_postgres as native
from tests.migrations.owned_history_postgres import dsn, insert_record, migrate, ReadonlyRedis
from tests.migrations.test_owned_history_lifecycle_postgres import a2_history as a2_history
from tests.services.test_owned_inventory_recovery import redis_rows, registrations
from app.services.worker_role_cli_common import quote_identifier


@pytest.fixture
async def a2_pg(monkeypatch, tmp_path, request):
    # The anchor remains 041. Current native role grants include a 045 helper,
    # so only this suite's disposable child reaches 047 before role setup.
    original = native.migrate
    async def migrate_child(target, revision):
        await original(target, revision)
        if revision == native.HEAD:
            await original(target, "047_owned_inventory_recovery")
    monkeypatch.setattr(native, "migrate", migrate_child)
    generator = native.a2_pg.__wrapped__(monkeypatch, tmp_path, request)
    async with aclosing(generator):
        yield await anext(generator)


@pytest.fixture
async def a2_env(a2_pg, monkeypatch):
    from app.services import owned_seed_inventory as service
    from app.services import owned_seed_inventory_history as history

    case = a2_pg
    monkeypatch.setenv("OWNED_INVENTORY_DISPOSABLE_TEST_CONFIRM", case.target.database)
    generator = _inventory_env.__wrapped__(monkeypatch, SimpleNamespace(
        param=case.target.render_as_string(hide_password=False), expected_migration_head="047_owned_inventory_recovery"))
    async with aclosing(generator):
        env = await anext(generator)
        env.storage.blobs["assets/owned.mp4"] = b"a" * 100
        redis = ReadonlyRedis(case.rows)
        monkeypatch.setattr(service, "_history_redis", lambda: redis)
        data = await env.data("recovery-history")
        data.update(version=2, history_locators={"operations": [], "retired_unassigned_preupload": {
            "operation_id": history.RETIRED_TUPLE[0], "legacy_account_id": history.RETIRED_TUPLE[4],
            "legacy_channel_profile_id": history.RETIRED_TUPLE[5]}})
        response = await env.client.post(env.url, json=data)
        assert response.status_code == 200, response.text
        yield SimpleNamespace(case=case, env=env, result=response.json(), data=data, redis=redis)


@pytest.fixture
async def recovery_pg(a2_history, monkeypatch):
    h = a2_history
    await migrate(h.case.target, "047_owned_inventory_recovery")
    owner = h.case.owner
    now = await owner.fetchval("SELECT clock_timestamp()")
    runtime = registrations(now)
    for table, rows in runtime.items():
        for value in rows:
            # Populate actual constrained native rows. No worker process or credentials are used.
            if table == "worker_admission_grants":
                value.update(token_sha256=uuid.UUID(value["id"]).hex.rjust(64, "0"), issued_at=now.isoformat(),
                             issued_by="recovery-test", created_at=now.isoformat(), updated_at=now.isoformat())
            else:
                value.update(lease_secret_sha256="a" * 64, database_fingerprint="b" * 64,
                             redis_fingerprint="c" * 64, storage_fingerprint="d" * 64)
            await insert_record(owner, table, value)
    assert await owner.fetchval("SELECT guarded_job_id FROM runtime_schedules WHERE service_name='videoprocess'") is None
    await owner.execute("UPDATE runtime_schedules SET state='CLOSED' WHERE service_name='videoprocess'")
    # Settle the fixture's already due normal metric retry without changing its deadline.
    metric = next(m for m in h.history_rows["publication_metric_schedules"] if m["snapshot_stage"] == "24h")
    retry = next(q for q in h.history_rows["channel_ops_queue_items"] if q["payload_json"].get("metrics_poll_count") == 1)
    async with owner.transaction():
        await owner.execute("UPDATE channel_ops_queue_items SET status='succeeded',attempt_count=1 WHERE id=$1", uuid.UUID(retry["id"]))
        await owner.execute("UPDATE publication_metric_schedules SET status='succeeded',attempt_count=2,completed_at=$2,last_attempt_at=$2,last_error_code=NULL WHERE id=$1", uuid.UUID(metric["id"]), now)
        await insert_record(owner, "feedback_snapshots", {**h.history_rows["feedback_snapshots"][0], "id": str(uuid.uuid4()),
            "snapshot_stage": "24h", "collected_at": now.isoformat()})
    data = await h.env.data("recovery-current-window")
    data.update(version=2, history_locators=h.data["history_locators"],
        starts_at=(now - timedelta(hours=1)).isoformat(), expires_at=(now + timedelta(days=7) - timedelta(hours=1)).isoformat())
    from app.schemas.channel_agent import OwnedSeedInventoryCreateV2
    from app.services import owned_seed_inventory as inv
    async with h.case.sessions() as db:
        h.result = await inv.create_inventory(db, h.env.channel_id, OwnedSeedInventoryCreateV2.model_validate(data), "test-operator")
    response = await h.env.client.post(f"{h.env.url}/{h.result['id']}/approve", json=approval(h.result))
    assert response.status_code == 200, response.text
    h.approved = response.json()
    h.inventory_id = uuid.UUID(h.result["id"])
    async with owner.transaction():
        await owner.execute("UPDATE owned_seed_inventories SET state='held',hold_reason='redis_consumer_identity_invalid',updated_at=clock_timestamp() WHERE id=$1", h.inventory_id)
        await owner.execute("UPDATE channel_profiles SET intake_paused_at=clock_timestamp(),intake_pause_reason='redis_consumer_identity_invalid' WHERE id=$1", h.env.channel_id)
    async with h.case.sessions() as db:
        row = await db.get(OwnedSeedInventory, h.inventory_id)
        channel = await db.get(ChannelProfile, h.env.channel_id)
        h.request = dict(client_request_id=str(uuid.uuid4()), manifest_sha256=row.manifest_sha256,
            expected_hold_sha256=recovery.hold_fingerprint(row, channel), recovery_reference="incident:verified-native-repair",
            operator_runtime_evidence={"reference": "deployment:original-guarded-receipt", "sha256": "a" * 64,
                "attestation": "Parent verified deployed native revision, service health, approved 127/150 placement; 126 excluded."})
    async def runtime_redis():
        return [{"stream": stream, "group": group, "consumer": name, "idle_ms": 0, "pending": 0, "lag": 0}
                for stream, (group, name) in redis_rows().items()]
    monkeypatch.setattr(recovery, "observe_runtime_redis", runtime_redis)
    h.url = f"{h.env.url}/{h.inventory_id}/recover-operational-hold"
    return h


async def test_pg_recovery_atomically_preserves_original_authority_and_replay_never_reopens(recovery_pg):
    h = recovery_pg
    before = await h.case.owner.fetchrow("SELECT * FROM owned_seed_inventories WHERE id=$1", h.inventory_id)
    items = await h.case.owner.fetch("SELECT * FROM owned_seed_inventory_items WHERE inventory_id=$1 ORDER BY ordinal", h.inventory_id)
    response = await h.env.client.post(h.url, json=h.request)
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["state"] == "approved" and result["hold_reason"] is None
    assert result["manifest"] == h.approved["manifest"]
    after = await h.case.owner.fetchrow("SELECT * FROM owned_seed_inventories WHERE id=$1", h.inventory_id)
    assert {k: v for k, v in before.items() if k not in {"state", "hold_reason", "updated_at"}} == {
        k: v for k, v in after.items() if k not in {"state", "hold_reason", "updated_at"}}
    assert await h.case.owner.fetch("SELECT * FROM owned_seed_inventory_items WHERE inventory_id=$1 ORDER BY ordinal", h.inventory_id) == items
    assert await h.case.owner.fetchval("SELECT intake_paused_at FROM channel_profiles WHERE id=$1", h.env.channel_id) is None
    assert await h.case.owner.fetchval("SELECT state FROM runtime_schedules WHERE service_name='videoprocess'") == "CLOSED"
    assert await h.case.owner.fetchval("SELECT count(*) FROM owned_inventory_recovery_permits") == 0
    audit = await h.case.owner.fetchrow("SELECT * FROM owned_inventory_recoveries WHERE id=$1", uuid.UUID(h.request["client_request_id"]))
    assert audit is not None
    for statement in (
        "UPDATE owned_inventory_recoveries SET recovered_by='changed'",
        "DELETE FROM owned_inventory_recoveries",
        "TRUNCATE owned_inventory_recoveries CASCADE",
    ):
        with pytest.raises(asyncpg.RaiseError, match="owned_inventory_recovery_audit_immutable"):
            await h.case.owner.execute(statement)
    await h.case.owner.execute("UPDATE owned_seed_inventories SET state='held',hold_reason='service_unhealthy' WHERE id=$1", h.inventory_id)
    await h.case.owner.execute("UPDATE channel_profiles SET intake_paused_at=clock_timestamp(),intake_pause_reason='service_unhealthy' WHERE id=$1", h.env.channel_id)
    replay = await h.env.client.post(h.url, json=h.request)
    assert replay.status_code == 200 and replay.json()["state"] == "held", replay.text
    assert replay.json()["hold_reason"] == "service_unhealthy"
    assert await h.case.owner.fetchrow("SELECT * FROM owned_inventory_recoveries WHERE id=$1", uuid.UUID(h.request["client_request_id"])) == audit
    assert await h.case.owner.fetchval("SELECT intake_paused_at FROM channel_profiles WHERE id=$1", h.env.channel_id) is not None


async def test_pg_raw_reset_and_runtime_forgery_are_denied(recovery_pg):
    h = recovery_pg
    with pytest.raises(asyncpg.RaiseError, match="owned_inventory_approval_immutable"):
        await h.case.owner.execute("UPDATE owned_seed_inventories SET state='approved',hold_reason=NULL WHERE id=$1", h.inventory_id)
    for kind in ("worker", "orchestrator", "operator"):
        connection = await asyncpg.connect(dsn(h.case.urls[kind]))
        try:
            for table in ("owned_inventory_recoveries", "owned_inventory_recovery_permits"):
                assert not await connection.fetchval("SELECT has_table_privilege(session_user,$1,'INSERT')", table)
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await connection.execute(f"INSERT INTO {table} DEFAULT VALUES")
            assert not await connection.fetchval("SELECT has_function_privilege(session_user,'public.vp_recover_owned_inventory(uuid,uuid,uuid,text,text,text,jsonb,jsonb,jsonb)','EXECUTE')")
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute("SELECT public.vp_recover_owned_inventory($1,$2,$3,'forged',$4,$4,'{}','{}','{}')", h.env.channel_id, h.inventory_id, uuid.uuid4(), "a" * 64)
        finally:
            await connection.close()


async def test_pg_identical_recovery_contenders_commit_one_audit(recovery_pg, monkeypatch):
    h = recovery_pg
    original = recovery.observe_runtime_redis
    entered = 0
    barrier = asyncio.Event()
    async def synchronize():
        nonlocal entered
        result = await original()
        entered += 1
        if entered == 2:
            barrier.set()
        await asyncio.wait_for(barrier.wait(), 10)
        return result
    monkeypatch.setattr(recovery, "observe_runtime_redis", synchronize)
    results = await asyncio.wait_for(asyncio.gather(*[h.env.client.post(h.url, json=h.request) for _ in range(2)]), 30)
    assert [r.status_code for r in results] == [200, 200], [r.text for r in results]
    assert await h.case.owner.fetchval("SELECT count(*) FROM owned_inventory_recoveries") == 1


async def test_pg_recovery_commit_failure_keeps_hold_and_no_audit(recovery_pg, monkeypatch):
    from sqlalchemy.ext.asyncio import AsyncSession

    h = recovery_pg
    async def fail(db):
        raise recovery.inv.OwnedInventoryError("synthetic_recovery_commit_failure")
    monkeypatch.setattr(AsyncSession, "commit", fail)
    response = await h.env.client.post(h.url, json=h.request)
    assert response.status_code == 409, response.text
    assert await h.case.owner.fetchval("SELECT state FROM owned_seed_inventories WHERE id=$1", h.inventory_id) == "held"
    assert await h.case.owner.fetchval("SELECT count(*) FROM owned_inventory_recoveries") == 0
    assert await h.case.owner.fetchval("SELECT count(*) FROM owned_inventory_recovery_permits") == 0


async def test_pg_runtime_lock_order_allows_native_worker_reconnect(recovery_pg):
    """Native reconnect holds registration before grant; recovery must agree."""
    h = recovery_pg
    row = await h.case.owner.fetchrow("SELECT id,grant_id FROM worker_registrations WHERE status='active' ORDER BY id LIMIT 1")
    native_connection = await asyncpg.connect(dsn(h.case.target))
    locking = None
    try:
        async with h.case.sessions() as db:
            recovery_pid = await db.scalar(text("SELECT pg_backend_pid()"))
            native_pid = await native_connection.fetchval("SELECT pg_backend_pid()")
            transaction = native_connection.transaction()
            await transaction.start()
            try:
                await native_connection.fetchval("SELECT id FROM worker_registrations WHERE id=$1 FOR UPDATE", row["id"])
                locking = asyncio.create_task(recovery._lock_runtime_registrations(db))
                await native.wait_blocked(h.case.owner, recovery_pid, native_pid)
                await asyncio.wait_for(native_connection.fetchval(
                    "SELECT id FROM worker_admission_grants WHERE id=$1 FOR UPDATE", row["grant_id"]), 5)
            finally:
                await transaction.rollback()
            await asyncio.wait_for(locking, 5)
            await db.rollback()
    finally:
        if locking is not None and not locking.done():
            locking.cancel()
            await asyncio.gather(locking, return_exceptions=True)
        await native_connection.close()


async def test_pg_047_migrates_as_nonsuperuser_owner_without_runtime_authority(a2_pg):
    """Qualify the production owner shape on this fixture's disposable child only."""
    case = a2_pg
    child = await asyncio.create_subprocess_exec(sys.executable, "-m", "alembic", "downgrade",
        "046_owned_retained_terminal", cwd=native.BACKEND,
        env={**os.environ, "DATABASE_URL": case.target.render_as_string(hide_password=False)},
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        await asyncio.wait_for(child.communicate(), 120)
        assert child.returncode == 0, "scratch-only downgrade to 046 failed"
    finally:
        if child.returncode is None:
            child.kill()
            await child.wait()
    role, password = "vp_recovery_owner_" + uuid.uuid4().hex[:20], secrets.token_hex(24)
    quoted = quote_identifier(role)
    await case.owner.execute(f"CREATE ROLE {quoted} LOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS PASSWORD '{password}'")
    try:
        await case.owner.execute(f"ALTER DATABASE {quote_identifier(case.target.database)} OWNER TO {quoted}")
        await case.owner.execute(f"ALTER SCHEMA public OWNER TO {quoted}")
        tables = await case.owner.fetch("SELECT relname FROM pg_class WHERE relnamespace='public'::regnamespace AND relkind IN ('r','p')")
        for table in tables:
            await case.owner.execute(f"ALTER TABLE public.{quote_identifier(table['relname'])} OWNER TO {quoted}")
        await case.owner.execute(f"ALTER FUNCTION public.owned_inventory_immutable() OWNER TO {quoted}")
        # Keep a non-default ACL so replacement must preserve actual grants.
        await case.owner.execute("GRANT EXECUTE ON FUNCTION public.owned_inventory_immutable() TO vp_worker_runtime")
        before = (await native.catalogue(case.owner))["owned_inventory_immutable"]
        login = case.target.set(username=role, password=password)
        connection = await asyncpg.connect(dsn(login))
        try:
            assert await connection.fetchval("SELECT current_user") == role
            assert not await connection.fetchval("SELECT rolsuper OR rolcreaterole OR rolcreatedb OR rolbypassrls FROM pg_roles WHERE rolname=current_user")
        finally:
            await connection.close()
        await migrate(login, "047_owned_inventory_recovery")
        after = await native.catalogue(case.owner)
        assert {k: after["owned_inventory_immutable"][k] for k in ("owner", "acl")} == {
            k: before[k] for k in ("owner", "acl")}
        for name in ("vp_recover_owned_inventory", "vp_owned_inventory_consume_recovery"):
            function = after[name]
            assert function["owner"] == role and function["definer"] is True
            assert function["config"] == ["search_path=pg_catalog"]
            assert not await case.owner.fetchval("""SELECT EXISTS(SELECT 1 FROM pg_proc p,
                LATERAL aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a
                WHERE p.pronamespace='public'::regnamespace AND p.proname=$1 AND a.grantee=0 AND a.privilege_type='EXECUTE')""", name)
            signature = await case.owner.fetchval("SELECT oid::regprocedure::text FROM pg_proc WHERE pronamespace='public'::regnamespace AND proname=$1", name)
            for url in case.urls.values():
                assert not await case.owner.fetchval("SELECT has_function_privilege($1,$2,'EXECUTE')", url.username, signature)
    finally:
        await case.owner.execute(f"REASSIGN OWNED BY {quoted} TO {quote_identifier(case.target.username)}")
        await case.owner.execute(f"DROP OWNED BY {quoted}")
        await case.owner.execute(f"DROP ROLE {quoted}")
