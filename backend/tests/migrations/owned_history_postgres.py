"""Parent-only A2 fixtures: isolated databases, native grants, no platform/Redis I/O.

The confirmed pre-migrated URL is an administrative scratch anchor, not a data
target. Each case creates/migrates/drops only its own random test database so
immutable approved history is never deleted or trigger-bypassed for cleanup.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import runpy
import secrets
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import asyncpg
import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.orchestrator.registered_db import RegisteredDatabase
from app.services import owned_seed_inventory as inventory
from app.services import owned_seed_inventory_history as history
from app.services.worker_control_role_cli import (
    ORCHESTRATOR_AUTHORITY_SELECT_COLUMNS, ORCHESTRATOR_ENTITY_COLUMNS,
    ORCHESTRATOR_INSERT_COLUMNS, ORCHESTRATOR_UPDATE_COLUMNS, ROLE_FUNCTIONS,
    role_names_for_generation,
)
from app.services.worker_role_cli_common import (
    create_login_role, ensure_stable_role, grant_columns, grant_functions,
    quote_identifier, reset_public_privileges,
)
from app.services.worker_runtime_role_cli import _set_runtime_privileges
from tests.services.test_owned_seed_inventory_history import NOW, retired_rows

BACKEND = Path(__file__).resolve().parents[2]
HEAD = "040_owned_history_seal"
MIGRATION = BACKEND / "alembic/versions/040_owned_history_seal.py"


def checked_url(raw, confirmation):
    try:
        url = make_url(raw)
        valid = (url.drivername == "postgresql+asyncpg" and url.host in {"127.0.0.1", "::1"}
                 and url.port is not None and 1024 <= url.port <= 65535 and url.port != 5432
                 and url.username and url.password and not url.query
                 and re.fullmatch(r"vp_owned_inventory_test_a2_[a-z0-9_]+", url.database or "")
                 and confirmation == url.database)
    except Exception:
        valid = False
    if not valid:
        raise ValueError("explicit A2 disposable URL and matching database confirmation required")
    return url


def dsn(url):
    return url.set(drivername="postgresql").render_as_string(hide_password=False)


def seed_document(now, *, null_link=False):
    rows, redis = retired_rows()
    rows["owned_seed_inventories"] = []
    delta = now - NOW

    def shift(value):
        if isinstance(value, dict):
            return {key: shift(item) for key, item in value.items()}
        if isinstance(value, list):
            return [shift(item) for item in value]
        if isinstance(value, str) and re.fullmatch(r"\d{4}-\d\d-\d\dT.*\+00:00", value):
            return (datetime.fromisoformat(value) + delta).isoformat()
        return value

    rows, redis = shift(rows), shift(redis)
    revoked = (now - timedelta(days=3)).isoformat()
    for row in rows["worker_admission_grants"]:
        row.update(image_identity="vp-python-worker:deploy-aaaaaaaaaaaa", revoked_at=revoked,
                   revoke_reason="fixture-retired", issued_by="fixture-operator")
    for row in rows["worker_registrations"]:
        row.update(image_identity="vp-python-worker:deploy-aaaaaaaaaaaa", revoked_at=revoked,
                   revoke_reason="fixture-retired")
    rows["jobs"][0]["orchestrator_owner"] = "python"
    rows["publishing_accounts"][0]["paused_until"] = (now + timedelta(hours=1)).isoformat()
    for artifact in rows["artifacts"]:
        artifact["kind"] = "INTERMEDIATE"
    for emission in rows["worker_event_emissions"]:
        emission["payload_sha256"] = history.history_sha256(emission["payload_json"])
        receipt = next(r for r in rows["registered_worker_event_receipts"]
                       if r["source_task_attestation_id"] == emission["source_task_attestation_id"])
        receipt["payload_sha256"] = emission["payload_sha256"]
        delivery = next(r for r in rows["registered_worker_event_deliveries"] if r["receipt_id"] == receipt["id"])
        delivery["payload_sha256"] = emission["payload_sha256"]
        next(r for r in redis if r["kind"] == "event" and r["message_id"] == emission["message_id"])["payload_sha256"] = emission["payload_sha256"]
        if null_link:
            attestation = next(r for r in rows["worker_task_delivery_attestations"] if r["id"] == emission["source_task_attestation_id"])
            acknowledged = (datetime.fromisoformat(receipt["applied_at"]) + timedelta(seconds=1)).isoformat()
            attestation.update(ack_event_emission_id=None, acknowledged_at=acknowledged)
            receipt["source_task_acknowledged_at"] = acknowledged
            next(d for d in rows["worker_task_dispatches"] if d["dispatch_key"] == attestation["dispatch_key"])["acknowledged_at"] = acknowledged
    return rows, tuple(history.RedisTerminalObservation.parse(r) for r in redis)


async def catalogue(connection):
    return {r["name"]: dict(r) for r in await connection.fetch("""
        SELECT p.proname AS name, pg_get_function_identity_arguments(p.oid) AS arguments,
               pg_get_function_result(p.oid) AS result, pg_get_userbyid(p.proowner) AS owner,
               p.proacl::text AS acl, p.proconfig AS config, p.prosecdef AS definer, p.prosrc AS source
        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public'
    """
    )}


async def migrate(url, target):
    child = await asyncio.create_subprocess_exec(sys.executable, "-m", "alembic", "upgrade", target,
        cwd=BACKEND, env={**os.environ, "DATABASE_URL": url.render_as_string(hide_password=False)},
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        await asyncio.wait_for(child.communicate(), 120)
        assert child.returncode == 0, "A2 scratch migration failed (inspect parent migration qualification)"
    finally:
        if child.returncode is None:
            child.kill()
            await child.wait()


async def insert_record(owner, table, row):
    assert table in history.HISTORY_MODELS
    await owner.execute(f"INSERT INTO public.{table} SELECT * FROM jsonb_populate_record(NULL::public.{table}, $1::jsonb)",
                        json.dumps(row))


async def seed_graph(owner, *, null_link=False):
    now = await owner.fetchval("SELECT clock_timestamp()")
    rows, _ = seed_document(now, null_link=null_link)
    pipeline = rows["jobs"][0]["pipeline_id"]
    await owner.execute("INSERT INTO pipelines(id,name,definition) VALUES($1,'A2 synthetic terminal graph',$2::json)",
                        uuid.UUID(pipeline), json.dumps(rows["jobs"][0]["pipeline_snapshot"]))
    order = ("channel_profiles", "publishing_accounts", "assets", "manual_seeds", "jobs",
             "worker_admission_grants", "worker_registrations", "node_executions", "artifacts",
             "production_tasks", "youtube_upload_operations", "worker_task_delivery_attestations",
             "worker_event_emissions", "registered_worker_event_receipts", "registered_worker_event_deliveries",
             "worker_task_dispatches")
    for table in order:
        for value in rows[table]:
            value = dict(value)
            if table == "worker_admission_grants":
                value["token_sha256"] = hashlib.sha256(value["id"].encode()).hexdigest()
            if table == "worker_registrations":
                value["lease_secret_sha256"] = hashlib.sha256(value["id"].encode()).hexdigest()
            await insert_record(owner, table, value)
    return rows


@pytest.fixture
async def a2_pg(monkeypatch, tmp_path, request):
    raw = os.environ.get("OWNED_HISTORY_A2_POSTGRES_TEST_URL")
    if not raw:
        pytest.skip("parent-only explicit A2 scratch PostgreSQL required")
    anchor = checked_url(raw, os.environ.get("OWNED_HISTORY_A2_POSTGRES_CONFIRM"))
    system = os.environ.get("OWNED_HISTORY_A2_POSTGRES_SYSTEM_ID", "")
    assert re.fullmatch(r"[0-9]{10,20}", system), "explicit scratch cluster identity required"
    admin = await asyncpg.connect(dsn(anchor), timeout=5, command_timeout=10)
    name = "vp_owned_history_a2_" + uuid.uuid4().hex
    target = anchor.set(database=name)
    created, roles, stables, engines = False, [], [], []
    owner = None
    registered = RegisteredDatabase()
    try:
        assert 160000 <= int(await admin.fetchval("SHOW server_version_num")) < 170000
        assert str(await admin.fetchval("SELECT system_identifier FROM pg_control_system()")) == system
        assert await admin.fetchval("SELECT version_num FROM public.alembic_version") == HEAD
        await admin.execute(f"CREATE DATABASE {quote_identifier(name)}")
        created = True
        await migrate(target, "039_registered_consumer_guard")
        owner = await asyncpg.connect(dsn(target), timeout=5, command_timeout=10)
        before = await catalogue(owner)
        await migrate(target, HEAD)
        after = await catalogue(owner)
        await owner.execute("INSERT INTO runtime_schedules(service_name,state,updated_by) VALUES('videoprocess','CLOSED','a2-fixture') ON CONFLICT(service_name) DO NOTHING")
        async with owner.transaction():
            rows = await seed_graph(owner, null_link=getattr(request, "param", False))
        generation = "a2-" + uuid.uuid4().hex[:20]
        orchestration = role_names_for_generation(generation)
        urls = {}
        for kind, stable in (("worker", "vp_worker_runtime"), ("orchestrator", orchestration.stable["orchestrator"]),
                             ("operator", orchestration.stable["operator"])):
            role = "vp_a2_worker_" + uuid.uuid4().hex[:20] if kind == "worker" else orchestration.versioned[kind]
            password = secrets.token_hex(24)
            assert not await owner.fetchval("SELECT to_regrole($1)", stable), "scratch native group must initially be absent"
            async with owner.transaction():
                await ensure_stable_role(owner, stable, setting_prefix="a2_test", authorized_members=(role,))
                if kind == "worker":
                    await _set_runtime_privileges(owner, stable)
                else:
                    await reset_public_privileges(owner, stable)
                    await grant_functions(owner, stable, ROLE_FUNCTIONS[kind])
                    if kind == "orchestrator":
                        for privilege, mapping in (("SELECT", ORCHESTRATOR_AUTHORITY_SELECT_COLUMNS),
                            ("SELECT", ORCHESTRATOR_ENTITY_COLUMNS), ("INSERT", ORCHESTRATOR_INSERT_COLUMNS),
                            ("UPDATE", ORCHESTRATOR_UPDATE_COLUMNS)):
                            for table, columns in mapping.items():
                                await grant_columns(owner, stable, privilege, table, columns)
                await create_login_role(owner, role, password, setting_prefix="a2_test", stable_role=stable)
            stables.append(stable)
            roles.append(role)
            urls[kind] = target.set(username=role, password=password)
        path = tmp_path / "orchestrator-test-url"
        path.write_text(urls["orchestrator"].render_as_string(hide_password=False))
        path.chmod(0o400)
        monkeypatch.setenv("WORKER_ORCHESTRATOR_DATABASE_URL_FILE", str(path))
        monkeypatch.setenv("WORKER_ORCHESTRATOR_CONTROL_GENERATION", generation)
        await registered.start(target.render_as_string(hide_password=False))
        owner_engine = create_async_engine(target, poolclass=NullPool, hide_parameters=True)
        engines.append(owner_engine)
        worker_engine = create_async_engine(urls["worker"], poolclass=NullPool, hide_parameters=True)
        engines.append(worker_engine)
        yield SimpleNamespace(owner=owner, target=target, urls=urls, rows=rows, before=before, after=after,
            sessions=async_sessionmaker(owner_engine, expire_on_commit=False), registered=registered,
            worker_sessions=async_sessionmaker(worker_engine, expire_on_commit=False),
            migration=runpy.run_path(str(MIGRATION)), granted_catalogue=await catalogue(owner))
    finally:
        await registered.close()
        for engine in engines:
            await engine.dispose()
        if owner is not None:
            await owner.close()
        if created:
            await admin.execute(f"DROP DATABASE {quote_identifier(name)}")
        for role in reversed(roles + stables):
            await admin.execute(f"DROP ROLE {quote_identifier(role)}")
        await admin.close()


class ReadonlyRedis:
    def __init__(self, rows):
        self.markers = {"vp:worker-task-dispatch:" + r["dispatch_key"]: r["redis_message_id"]
                        for r in rows["worker_task_dispatches"]}
        self.calls = []

    async def get(self, key):
        self.calls.append(("get", key))
        return self.markers[key]

    async def xpending_range(self, *args):
        self.calls.append(("pending", args))
        return []

    async def aclose(self):
        self.calls.append(("close",))


async def wait_blocked(owner, pid, blocker_pid):
    async with asyncio.timeout(5):
        while not await owner.fetchval("SELECT $2::int = ANY(pg_blocking_pids($1))", pid, blocker_pid):
            await asyncio.sleep(0.01)


async def qualify(case, monkeypatch):
    redis = ReadonlyRedis(case.rows)
    monkeypatch.setattr(inventory, "_history_redis", lambda: redis)
    class Storage:
        async def read_bounded(self, path, limit):
            assert path == "assets/owned.mp4" and limit >= 100
            return b"a" * 100
    monkeypatch.setattr(inventory.storage_manager, "get_storage", lambda *a, **k: Storage())
    async with case.sessions() as db:
        snapshot = await history.load_owned_history_evidence(db, platform_channel_id="UC" + "a" * 22)
        sources = inventory._retirement_sources(snapshot, requested=True)
        await db.rollback()
        observed = await inventory._observe_retirement(sources, observed_at=snapshot.observed_at)
        fresh = await history.load_owned_history_evidence(db, platform_channel_id=snapshot.platform_channel_id)
        certificate = inventory._qualified_retirement(fresh, sources, observed, "a2-fixture", "fixture:only")
    return certificate, redis
