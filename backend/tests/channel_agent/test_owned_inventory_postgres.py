"""Parent-only C qualification. Every case uses a fresh, explicitly confirmed child DB."""
from __future__ import annotations

import asyncio
import os
import re
import uuid
from pathlib import Path
from types import SimpleNamespace

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from tests.api.test_owned_seed_inventory import inventory_env as api_inventory_env
from tests.channel_agent.test_owned_inventory import (  # noqa: F401 - parent-run shared cases
    Policy, claimed_tick, configure_owned_env, state, tick,
    test_exact_lowest_atomic_admission_and_replay as test_pg_atomic_replay,
    test_actual_python_handler_refuses_lost_queue_lease as test_pg_queued_lease_loss,
    test_plan_enqueue_failure_rolls_back_whole_selection as test_pg_atomic_rollback,
    test_policy_failure_durably_holds_despite_runtime_drift as test_pg_failed_policy_hold,
    test_policy_runs_without_database_transaction as test_pg_external_policy,
    test_runner_does_not_retry_or_finish_after_owned_commit_response_loss as test_pg_response_loss,
    test_run_once_respects_owned_atomic_completion_and_lost_authority as test_pg_actual_runner,
)
from tests.migrations.owned_history_postgres import dsn, migrate, seed_graph
from app.channel_agent.service import ChannelAgentService
from app.config import settings
from app.services import owned_seed_inventory as inventory
from app.services import owned_seed_inventory_history as history
from tests.channel_agent.test_owned_inventory import NativeReader


def checked_url(raw, confirmation):
    url = make_url(raw)
    if not (url.drivername == "postgresql+asyncpg" and url.host in {"127.0.0.1", "::1"}
            and url.port is not None and 1024 <= url.port <= 65535 and url.port != 5432
            and url.username and url.password and not url.query and confirmation == url.database
            and re.fullmatch(r"vp_owned_inventory_test_c_[a-z0-9_]+", url.database or "")):
        raise ValueError("explicit C disposable database and matching confirmation required")
    return url


@pytest.fixture
async def owned_env(monkeypatch):
    raw = os.environ.get("OWNED_C_POSTGRES_TEST_URL", "")
    if not raw:
        pytest.skip("parent-only explicit C PostgreSQL qualification")
    anchor = checked_url(raw, os.environ.get("OWNED_C_POSTGRES_TEST_CONFIRM"))
    system = os.environ.get("OWNED_C_POSTGRES_SYSTEM_ID", "")
    assert re.fullmatch(r"[0-9]{10,20}", system), "explicit scratch cluster identity required"
    admin = await asyncpg.connect(dsn(anchor), timeout=5, command_timeout=10)
    name = "vp_owned_inventory_test_c_" + uuid.uuid4().hex
    target = anchor.set(database=name)
    created = False
    generator = None
    try:
        assert 160000 <= int(await admin.fetchval("SHOW server_version_num")) < 170000
        assert str(await admin.fetchval("SELECT system_identifier FROM pg_control_system()")) == system
        assert await admin.fetchval("SELECT version_num FROM alembic_version") == "041_registered_consumer_terminal"
        await admin.execute(f'CREATE DATABASE "{name}"')
        created = True
        await migrate(target, "041_registered_consumer_terminal")
        connection = await asyncpg.connect(dsn(target), timeout=5)
        try:
            await connection.execute("INSERT INTO runtime_schedules(service_name,state,updated_by) VALUES('videoprocess','CLOSED','c-fixture') ON CONFLICT(service_name) DO UPDATE SET state='CLOSED',guarded_job_id=NULL")
        finally:
            await connection.close()
        monkeypatch.setenv("OWNED_INVENTORY_DISPOSABLE_TEST_CONFIRM", name)
        generator = api_inventory_env.__wrapped__(monkeypatch, SimpleNamespace(param=target.render_as_string(hide_password=False)))
        env = await anext(generator)
        env.pg_url = target
        yield await configure_owned_env(env, monkeypatch, sqlite=False)
    finally:
        if generator is not None:
            await generator.aclose()
        if created:
            await admin.execute(f'DROP DATABASE "{name}"')
        await admin.close()


async def test_pg_python_python_queued_contenders(owned_env):
    env = owned_env
    one, two = await claimed_tick(env), await claimed_tick(env)
    entered, release = asyncio.Queue(), asyncio.Event()

    async def barrier():
        await entered.put(True)
        await asyncio.wait_for(release.wait(), 10)

    async def run(item):
        async with env.factory() as db:
            return await ChannelAgentService(pds_client=Policy(barrier)).tick(db, channel_id=env.channel_id, queue_item=item)

    tasks = [asyncio.create_task(run(item)) for item in (one, two)]
    try:
        await asyncio.wait_for(entered.get(), 10)
        await asyncio.wait_for(entered.get(), 10)
    finally:
        release.set()
        await asyncio.wait_for(asyncio.gather(*tasks), 15)
    result = await state(env)
    assert len(result["tasks"]) == sum(i.state == "reserved" for i in result["items"]) == 1


@pytest.mark.parametrize("mode", ["contend", "leader_loss"])
async def test_pg_actual_go_python_contenders(owned_env, tmp_path, mode):
    env = owned_env
    entered, release = asyncio.Event(), asyncio.Event()

    async def barrier():
        entered.set()
        await asyncio.wait_for(release.wait(), 60)

    python = asyncio.create_task(tick(env, Policy(barrier)))
    process = None
    go_dir = tmp_path / "go-contender"
    go_dir.mkdir(mode=0o700)
    try:
        await asyncio.wait_for(entered.wait(), 10)
        process = await asyncio.create_subprocess_exec("go", "test", "./internal/channelops", "-count=1", "-v",
            "-run", "^TestOwnedPythonContenderBridge$", cwd=Path(__file__).resolve().parents[3],
            env={**os.environ, "GOPROXY": "off", "GOSUMDB": "off", "GOTOOLCHAIN": "local",
                 "DATABASE_URL": "invalid-offline", "OWNED_INVENTORY_DISPOSABLE_TEST_URL": dsn(env.pg_url),
                 "OWNED_INVENTORY_DISPOSABLE_TEST_CONFIRM": env.pg_url.database,
                 "OWNED_PYTHON_CONTENDER_CHANNEL": str(env.channel_id), "OWNED_PYTHON_CONTENDER_DIR": str(go_dir),
                 "OWNED_PYTHON_CONTENDER_MODE": mode}, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        async with asyncio.timeout(45):
            while not (go_dir / "ready").exists():
                if process.returncode is not None:
                    pytest.fail("Go contender exited before external PDS barrier")
                await asyncio.sleep(0.02)
        (go_dir / "release").write_text("release")
        release.set()
        output, _ = await asyncio.wait_for(process.communicate(), 30)
        assert process.returncode == 0, output.decode()
        await asyncio.wait_for(python, 15)
    finally:
        release.set()
        if process is not None and process.returncode is None:
            process.kill()
            await process.wait()
        if not python.done():
            python.cancel()
        await asyncio.gather(python, return_exceptions=True)
    result = await state(env)
    assert len(result["tasks"]) == sum(i.state == "reserved" for i in result["items"]) == 1


async def test_pg_complete_a1_reader_and_database_clock(owned_env):
    env = owned_env
    async with env.factory() as db:
        row = await db.scalar(text("SELECT clock_timestamp()"))
        assert row.tzinfo is not None
        result = await history.load_owned_history_evidence(db, platform_channel_id=env.scope["platform_channel_id"])
        assert set(result.rows.as_dict()) == set(history.HISTORY_MODELS)


async def test_pg_new_blank_operation_after_prepare_is_not_filtered_away(owned_env):
    env = owned_env

    async def insert_unclassified():
        owner = await asyncpg.connect(dsn(env.pg_url), timeout=5)
        try:
            async with owner.transaction():
                await seed_graph(owner)
        finally:
            await owner.close()

    policy = Policy(insert_unclassified)
    await tick(env, policy)
    result = await state(env)
    assert len(policy.calls) == 1 and not any(t.channel_profile_id == env.channel_id for t in result["tasks"])
    assert result["inventory"].state == "held"
    assert result["inventory"].hold_reason == "owned_history_unclassified"


@pytest.mark.parametrize("pending", [False, True])
async def test_pg_real_b2_v2_history_loaded_by_python_and_native_proof_reentered(owned_env, monkeypatch, tmp_path, pending):
    env = owned_env
    directory = tmp_path / "retirement-seed"
    directory.mkdir(mode=0o700)
    process = await asyncio.create_subprocess_exec("go", "test", "./internal/channelops", "-count=1", "-v",
        "-run", "^TestOwnedPythonContenderBridge$", cwd=Path(__file__).resolve().parents[3],
        env={**os.environ, "GOPROXY": "off", "GOSUMDB": "off", "GOTOOLCHAIN": "local", "DATABASE_URL": "invalid-offline",
             "OWNED_INVENTORY_DISPOSABLE_TEST_URL": dsn(env.pg_url), "OWNED_INVENTORY_DISPOSABLE_TEST_CONFIRM": env.pg_url.database,
             "OWNED_PYTHON_CONTENDER_CHANNEL": str(env.channel_id), "OWNED_PYTHON_CONTENDER_DIR": str(directory),
             "OWNED_PYTHON_CONTENDER_MODE": "seed_retirement"}, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    try:
        output, _ = await asyncio.wait_for(process.communicate(), 60)
        assert process.returncode == 0, output.decode()
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    async with env.factory() as db:
        snapshot = await history.load_owned_history_evidence(db, platform_channel_id=env.scope["platform_channel_id"])
        assert inventory._retirement_sources(snapshot, requested=False) is not None
    native = NativeReader(snapshot.rows.as_dict())
    monkeypatch.setattr(inventory, "_history_redis", lambda: native)
    monkeypatch.setattr(settings, "redis_url", "redis://history-reader:fixture@127.0.0.1:55464/15")

    async def policy_action():
        native.pending = pending

    policy = Policy(policy_action)
    await tick(env, policy)
    result = await state(env)
    selected = [t for t in result["tasks"] if t.channel_profile_id == env.channel_id]
    assert len(policy.calls) == 1 and native.calls.count("whoami") == 3
    assert len(selected) == (0 if pending else 1)
    if pending:
        assert result["inventory"].state == "held"
    else:
        assert selected[0].agent_approval_evidence_json["owned_inventory"]["retired_source_sha256"]
