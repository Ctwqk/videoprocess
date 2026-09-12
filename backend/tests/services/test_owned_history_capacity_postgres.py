"""Parent-only capacity qualification; reuse D's confirmed disposable contract.

Each new fixture upgrades its isolated D child from 042 to 043. Historical D
and consumer fixtures remain unchanged. No Redis, Manager or platform calls.
"""
from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import runpy
import sys
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.services import owned_seed_inventory_history as history
from app.services.worker_control_role_cli import role_names_for_generation
from app.services.worker_role_cli_common import create_login_role, quote_identifier, role_database_url
from tests.migrations.owned_history_postgres import dsn, migrate
from tests.migrations.test_registered_consumer_reconcile_postgres import (
    CALL, SIGNATURE, Case, insert_row, seed_rows,
)
from tests.migrations.test_registered_consumer_terminal_postgres import (
    HELPER, corrupt, insert_rows, snapshot_rows, terminal_rows,
)
from tests.services.test_owned_producer_postgres import (
    d_database as d_database, d_pg as d_pg, reserve, transition,
)
from tests.services.test_owned_seed_inventory_history import UC
from tests.services.test_registered_consumer_reconcile import decode
from tests.services.test_registered_consumer_reconcile_runtime import invocation
from tests.worker.test_youtube_upload_handler import media_paths as media_paths


HEAD = "043_owned_history_snapshot_rows"
PREVIOUS = "042_owned_producer_fence"
FUNCTIONS = ["public.vp_owned_producer_rows()", HELPER,
    "public.vp_registered_consumer_terminal_upload(uuid,timestamptz)"]


async def downgrade(config):
    child = await asyncio.create_subprocess_exec(sys.executable, "-m", "alembic", "downgrade", PREVIOUS,
        cwd=Path(__file__).parents[2], env={**os.environ, "DATABASE_URL": config.owner_url.render_as_string(hide_password=False)},
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        await asyncio.wait_for(child.communicate(), 120)
        assert child.returncode == 0, "capacity scratch downgrade failed"
    finally:
        if child.returncode is None:
            child.kill()
            await child.wait()


async def catalog(connection):
    return await connection.fetch("""SELECT oid, proowner, proacl::text, prosecdef,
        proconfig, provolatile, proisstrict, proparallel, prosrc
        FROM pg_proc WHERE oid = ANY(ARRAY(SELECT x::regprocedure::oid FROM unnest($1::text[]) x))
        ORDER BY oid""", FUNCTIONS)


@pytest.fixture
async def capacity_db(d_database):
    owner = await asyncpg.connect(dsn(d_database.owner_url), timeout=5)
    try:
        before = await catalog(owner)
        await migrate(d_database.owner_url, HEAD)
        assert await owner.fetchval("SELECT version_num FROM alembic_version") == HEAD
        yield SimpleNamespace(config=d_database, owner=owner, before=before)
    finally:
        await owner.close()


@pytest.fixture
async def capacity_producer(d_pg):
    await migrate(d_pg.owner_url, HEAD)
    return d_pg


@pytest.fixture
async def capacity_terminal(capacity_db):
    db, owner = capacity_db.config, capacity_db.owner
    generation = "capacity-" + uuid4().hex[:16]
    payload, registrations, grants = seed_rows(await owner.fetchval("SELECT clock_timestamp()"), generation)
    role = role_names_for_generation(generation).versioned["operator"]
    password = uuid4().hex + uuid4().hex
    created, operator = False, None
    stable = "vp_worker_operator_runtime"
    assert await owner.fetchval("SELECT to_regrole($1)", stable) is None
    try:
        async with owner.transaction():
            for row in grants:
                await insert_row(owner, row)
            for row in sorted(registrations, key=lambda row: row.superseded_by is not None):
                await insert_row(owner, row)
            await owner.execute(f"CREATE ROLE {stable} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS")
            await create_login_role(owner, role, password, setting_prefix="capacity_fixture", stable_role=stable)
            await owner.execute(f"GRANT EXECUTE ON FUNCTION {SIGNATURE} TO {stable}")
        created = True
        operator = await asyncpg.connect(role_database_url(dsn(db.owner_url), role, password), timeout=2, command_timeout=2)
        request = replace(invocation(), pins=decode(payload), control_generation=generation)
        case = Case(owner, operator, None, None, request, registrations, grants, [role])
        case.capacity_config = db
        yield case
    finally:
        if operator is not None:
            await operator.close(timeout=2)
        if created:
            await owner.execute(f"REVOKE EXECUTE ON FUNCTION {SIGNATURE} FROM {stable}")
            await owner.execute(f"DROP ROLE {quote_identifier(role)}")
            await owner.execute(f"DROP ROLE {stable}")


async def fill_assets(owner, total):
    existing = await owner.fetchval("SELECT count(*) FROM assets")
    ids = [uuid4() for _ in range(total - existing)]
    await owner.execute("""INSERT INTO assets(id,filename,original_name,storage_backend,storage_path,uploaded_by)
        SELECT id,'capacity','capacity','local','test-only','test-only' FROM unnest($1::uuid[]) id""", ids)
    return ids


async def python_snapshot(config):
    engine = create_async_engine(config.owner_url, poolclass=NullPool)
    try:
        async with async_sessionmaker(engine)() as session:
            return await history.load_owned_history_evidence(session, platform_channel_id=UC)
    finally:
        await engine.dispose()


async def test_pg_capacity_catalog_roundtrip_and_restricted_acl(capacity_db):
    env = capacity_db
    after = await catalog(env.owner)
    assert len(after) == 3
    assert [dict(r, prosrc="") for r in after] == [dict(r, prosrc="") for r in env.before]
    assert all(a["prosrc"] != b["prosrc"] for a, b in zip(after, env.before, strict=True))
    for url in (env.config.runtime_url, env.config.operator_url):
        connection = await asyncpg.connect(dsn(url), timeout=2)
        try:
            for function in FUNCTIONS:
                assert not await connection.fetchval("SELECT has_function_privilege(current_user,$1,'EXECUTE')", function)
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.fetchval("SELECT public.vp_owned_producer_rows()")
        finally:
            await connection.close()
    await downgrade(env.config)
    assert await catalog(env.owner) == env.before
    await migrate(env.config.owner_url, HEAD)
    assert await catalog(env.owner) == after


@pytest.mark.parametrize("signature", FUNCTIONS)
async def test_pg_capacity_unknown_installed_body_refuses(capacity_db, signature):
    env = capacity_db
    await downgrade(env.config)
    migration = runpy.run_path(str(Path(__file__).parents[2] / "alembic/versions/043_owned_history_snapshot_rows.py"))
    before, after = migration["_bodies"]()[signature.replace("timestamptz", "timestamp with time zone")]
    definition = await env.owner.fetchval("SELECT pg_get_functiondef($1::regprocedure)", signature)
    assert definition.count(before) == 1
    await env.owner.execute(definition.replace(before, before + "\n-- synthetic unknown installed body\n", 1))
    drifted = await catalog(env.owner)
    with pytest.raises(asyncpg.RaiseError, match="owned_history_capacity_definition_changed"):
        await env.owner.execute(migration["_replacement"](signature, before, after))
    assert await catalog(env.owner) == drifted
    assert await env.owner.fetchval("SELECT version_num FROM alembic_version") == PREVIOUS


@pytest.mark.parametrize("count", [8192, 8193])
async def test_pg_capacity_snapshot_rows_and_digest(capacity_db, count):
    env = capacity_db
    ids = await fill_assets(env.owner, count)
    if count == 8193:
        with pytest.raises(history.OwnedHistoryError, match="owned_history_read_failed"):
            await python_snapshot(env.config)
        with pytest.raises(asyncpg.RaiseError, match="owned_history_incomplete"):
            await env.owner.fetchval("SELECT public.vp_owned_producer_rows()")
        assert await env.owner.fetchval("SELECT count(*) FROM assets") == 8193
        return
    snapshot = await python_snapshot(env.config)
    actual = json.loads(await env.owner.fetchval("SELECT public.vp_owned_producer_rows()"))
    assert len(snapshot.rows.as_dict()["assets"]) == count
    assert {UUID(row["id"]) for row in actual["assets"]} == set(ids)
    assert set(actual) == set(history.HISTORY_MODELS)
    assert snapshot.rows == history.FrozenJSON.from_value(actual)
    assert history.history_sha256(snapshot.rows) == await env.owner.fetchval("SELECT public.vp_owned_producer_hash(public.vp_owned_producer_rows())")


async def test_pg_capacity_go_python_sql_digest(capacity_db):
    env = capacity_db
    await fill_assets(env.owner, 8192)
    snapshot = await python_snapshot(env.config)
    sql_hash = await env.owner.fetchval("SELECT public.vp_owned_producer_hash(public.vp_owned_producer_rows())")
    assert history.history_sha256(snapshot.rows) == sql_hash
    child_env = {**os.environ, "GOPROXY": "off", "GOSUMDB": "off", "GOTOOLCHAIN": "local",
        "OWNED_D_POSTGRES_TEST_URL": env.config.owner_url.render_as_string(hide_password=False),
        "OWNED_D_POSTGRES_TEST_CONFIRM": env.config.owner_url.database,
        "OWNED_CAPACITY_EXPECTED_SHA256": sql_hash}
    child = await asyncio.create_subprocess_exec("go", "test", "./internal/channelops", "-run", "^TestOwnedPGHistoryCapacityReadOnly$", "-count=1",
        cwd=Path(__file__).parents[3], env=child_env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    try:
        output, _ = await asyncio.wait_for(child.communicate(), timeout=90)
        assert child.returncode == 0, output.decode()
    finally:
        if child.returncode is None:
            child.kill()
            await child.wait()


async def test_pg_capacity_bytes_still_refuse(capacity_db):
    env = capacity_db
    await fill_assets(env.owner, 8192)
    # 16384 bounded scalars already total 16 MiB, before complete-row/JSON overhead.
    await env.owner.execute("""UPDATE assets SET media_info=jsonb_build_object('oversize',
        jsonb_build_array(repeat('x',1024),repeat('y',1024)))""")
    assert await env.owner.fetchval("SELECT count(*) FROM assets") == 8192
    with pytest.raises(history.OwnedHistoryError, match="^owned_history_read_failed$"):
        await python_snapshot(env.config)
    assert await env.owner.fetchval("SELECT 1") == 1
    with pytest.raises(asyncpg.RaiseError, match="^owned_history_too_large$"):
        await env.owner.fetchval("SELECT public.vp_owned_producer_rows()")
    assert await env.owner.fetchval("SELECT 1") == 1


@pytest.mark.parametrize("null_link", [False, True])
async def test_pg_capacity_terminal_paths_and_sentinel(capacity_terminal, null_link):
    case = capacity_terminal
    rows = terminal_rows(case, await case.owner.fetchval("SELECT clock_timestamp()"))
    if null_link:
        for row in rows["worker_task_delivery_attestations"]:
            receipt = next(r for r in rows["registered_worker_event_receipts"] if r["source_task_attestation_id"] == row["id"])
            dispatch = next(d for d in rows["worker_task_dispatches"] if d["dispatch_key"] == row["dispatch_key"])
            acknowledged = (datetime.fromisoformat(receipt["applied_at"]) + timedelta(seconds=1)).isoformat()
            row.update(ack_event_emission_id=None, acknowledged_at=acknowledged)
            dispatch["acknowledged_at"] = receipt["source_task_acknowledged_at"] = acknowledged
    # The unchanged A1 fixture includes all four native terminal paths.
    assert any(n["node_type"] == "source" for n in rows["node_executions"])
    assert any(d["resolution_state"] == "cancelled" for d in rows["worker_task_dispatches"])
    assert any(d["origin_receipt_id"] for d in rows["worker_task_dispatches"])
    assert rows["registered_worker_event_receipts"]
    await insert_rows(case.owner, rows)
    await fill_assets(case.owner, 8192)
    before = await snapshot_rows(case.owner, rows)
    assert len(await case.operator.fetch(CALL, *case.arguments())) == 8
    assert await snapshot_rows(case.owner, rows) == before
    extra = await fill_assets(case.owner, 8193)
    with pytest.raises(asyncpg.RaiseError, match="registered_reconcile_work_active"):
        await case.operator.fetch(CALL, *case.arguments())
    await case.owner.execute("DELETE FROM assets WHERE id=ANY($1::uuid[])", extra)
    await case.owner.execute("""UPDATE assets SET media_info=jsonb_build_object('oversize',
        jsonb_build_array(repeat('x',1024),repeat('y',1024)))""")
    assert await case.owner.fetchval("SELECT count(*) FROM assets") == 8192
    with pytest.raises(asyncpg.RaiseError, match="^registered_reconcile_work_active$"):
        await case.operator.fetch(CALL, *case.arguments())
    assert await case.operator.fetchval("SELECT 1") == 1
    assert await case.owner.fetchval("SELECT 1") == 1
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await case.operator.fetchval(f"SELECT {HELPER}")


@pytest.mark.parametrize("fault", ["attempt", "retry_ack_order", "receipt_pending", "unpaused"])
async def test_pg_capacity_terminal_drift_still_blocks(capacity_terminal, fault):
    case = capacity_terminal
    now = await case.owner.fetchval("SELECT clock_timestamp()")
    rows = terminal_rows(case, now)
    corrupt(rows, fault, now)
    await insert_rows(case.owner, rows)
    await fill_assets(case.owner, 4930)
    with pytest.raises(asyncpg.RaiseError, match="registered_reconcile_work_active"):
        await case.operator.fetch(CALL, *case.arguments())


async def test_pg_capacity_terminal_graph_4097_refuses(capacity_terminal):
    case = capacity_terminal
    await downgrade(case.capacity_config)
    rows = terminal_rows(case, await case.owner.fetchval("SELECT clock_timestamp()"))
    source = next(n for n in rows["node_executions"] if n["node_type"] == "source")
    artifact = next(a for a in rows["artifacts"] if a["id"] == source["output_artifact_id"])
    spec = next(n for n in rows["jobs"][0]["pipeline_snapshot"]["nodes"] if n["id"] == source["node_id"])
    while len(rows["node_executions"]) < 4097:
        node, art, definition = deepcopy(source), deepcopy(artifact), deepcopy(spec)
        node.update(id=str(uuid4()), node_id="capacity_source_" + str(len(rows["node_executions"])), output_artifact_id=str(uuid4()))
        art.update(id=node["output_artifact_id"], node_execution_id=node["id"])
        definition["id"] = node["node_id"]
        rows["node_executions"].append(node)
        rows["artifacts"].append(art)
        rows["jobs"][0]["pipeline_snapshot"]["nodes"].append(definition)
    await insert_rows(case.owner, rows)
    assert await case.owner.fetchval("SELECT count(*) FROM node_executions") == 4097
    operation = UUID(rows["youtube_upload_operations"][0]["id"])
    # The previous per-table resource bound was the only graph-size barrier.
    assert await case.owner.fetchval("SELECT public.vp_registered_consumer_terminal_upload($1,clock_timestamp())", operation) is True
    assert await case.owner.fetchval(f"SELECT {HELPER}") is False
    await migrate(case.capacity_config.owner_url, HEAD)
    assert await case.owner.fetchval("SELECT public.vp_registered_consumer_terminal_upload($1,clock_timestamp())", UUID(rows["youtube_upload_operations"][0]["id"])) is False
    with pytest.raises(asyncpg.RaiseError, match="registered_reconcile_work_active"):
        await case.operator.fetch(CALL, *case.arguments())


@pytest.mark.parametrize("boundary", ["reserve", "attempt", "fence"])
async def test_pg_capacity_restricted_new_effect_tail_drift(capacity_producer, boundary):
    env = capacity_producer
    await fill_assets(env.native.owner, 4930)
    operation = None
    if boundary != "reserve":
        operation = await reserve(env)
    if boundary == "fence":
        await transition(env, operation, "attempting")
    # Completed nodes of an unrelated job do not change current-task authority.
    unrelated = await env.native.owner.fetchval("""INSERT INTO jobs(pipeline_id,pipeline_snapshot,status,orchestrator_owner)
        SELECT pipeline_id,'{"nodes":[],"edges":[]}','SUCCEEDED','python' FROM jobs WHERE id=$1 RETURNING id""", env.native.job_id)
    await env.native.owner.execute("""INSERT INTO node_executions(id,job_id,node_id,node_type,node_label,node_config,status,retry_count,input_artifact_ids)
        SELECT md5('capacity-node-'||i)::uuid,$1,'capacity-'||i,'export','','{}','SUCCEEDED',0,'{}' FROM generate_series(1,4096) i""", unrelated)
    positive = env.native.runtime.transaction()
    await positive.start()
    try:
        if boundary == "reserve":
            await reserve(env)
        else:
            await transition(env, operation, "attempting" if boundary == "attempt" else "fence")
    finally:
        await positive.rollback()
    bad = UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
    await env.native.owner.execute("""INSERT INTO node_executions(id,job_id,node_id,node_type,node_label,node_config,status,retry_count,input_artifact_ids)
        VALUES($1,$2,'capacity-bad','export','','{}','RUNNING',0,'{}')""", bad, env.native.job_id)
    async with env.factory() as db:
        snapshot = await history.load_owned_history_evidence(db, platform_channel_id=env.scope["platform_channel_id"])
    assert snapshot.rows.as_dict()["node_executions"][-1]["id"] == str(bad)
    with pytest.raises(asyncpg.RaiseError, match="owned_(inventory|history)_"):
        if boundary == "reserve":
            await reserve(env)
        else:
            await transition(env, operation, "attempting" if boundary == "attempt" else "fence")
    rows = await env.native.owner.fetch("SELECT request_attempted_at,manager_task_id FROM youtube_upload_operations WHERE production_task_id=$1", env.task_id)
    assert len(rows) == (0 if boundary == "reserve" else 1)
    assert all(row["manager_task_id"] is None and (row["request_attempted_at"] is not None) == (boundary == "fence") for row in rows)


async def test_pg_capacity_restricted_reserve_attempt_fence(capacity_producer):
    env = capacity_producer
    await fill_assets(env.native.owner, 8192)
    operation = await reserve(env)
    await transition(env, operation, "attempting")
    await transition(env, operation, "fence")
    row = await env.native.owner.fetchrow("SELECT status,request_attempted_at,manager_task_id FROM youtube_upload_operations WHERE id=$1", operation)
    assert row["status"] == "reserved" and row["request_attempted_at"] is not None and row["manager_task_id"] is None
