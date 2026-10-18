"""Test-only cleanup must not turn immutable recovery receipts into disposable data."""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest
from sqlalchemy import select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models.channel_agent import ChannelProfile, LaneFormatMatrix, PublishingAccount, TopicLane
from app.models.owned_inventory_recovery import OwnedInventoryRecoveryRecord
from app.models.owned_seed_inventory import OwnedSeedInventory
from tests.disposable_postgres_cleanup import checked_target, cleanup_disposable_postgres
from tests.migrations.owned_history_postgres import migrate


LOCAL_URL = "postgresql+asyncpg://postgres:fixture@127.0.0.1:55447/vp_owned_inventory_test_cleanup_unit"
LOCAL_ENV = {
    "CHANNEL_OPS_POSTGRES_TEST_CONFIRM": "vp_owned_inventory_test_cleanup_unit",
    "CHANNEL_OPS_POSTGRES_TEST_SYSTEM_ID": "7697927417582219303",
}
CI_ENV = {"GITHUB_ACTIONS": "true", "GITHUB_REPOSITORY": "Ctwqk/videoprocess"}


def test_explicit_local_and_exact_repository_ci_targets():
    assert checked_target(make_url(LOCAL_URL), LOCAL_ENV) == LOCAL_ENV["CHANNEL_OPS_POSTGRES_TEST_SYSTEM_ID"]
    assert checked_target(make_url("postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/postgres"), CI_ENV) is None
    assert checked_target(make_url(LOCAL_URL.replace(":55447/", ":5432/")), {**LOCAL_ENV, **CI_ENV}) == LOCAL_ENV["CHANNEL_OPS_POSTGRES_TEST_SYSTEM_ID"]


@pytest.mark.parametrize("url,env", [
    (LOCAL_URL, {}),
    (LOCAL_URL, {**LOCAL_ENV, "CHANNEL_OPS_POSTGRES_TEST_SYSTEM_ID": ""}),
    (LOCAL_URL, {**LOCAL_ENV, "CHANNEL_OPS_POSTGRES_TEST_CONFIRM": "other"}),
    (LOCAL_URL.replace("127.0.0.1", "10.0.0.150"), LOCAL_ENV),
    (LOCAL_URL.replace(":55447/", ":5432/"), LOCAL_ENV),
    (LOCAL_URL + "?host=elsewhere", LOCAL_ENV),
    (LOCAL_URL.replace("vp_owned_inventory_test_cleanup_unit", "videoprocess"), LOCAL_ENV),
    ("postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/postgres", {}),
    ("postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/postgres", {**CI_ENV, "GITHUB_REPOSITORY": "other/repository"}),
    ("postgresql+asyncpg://postgres:unexpected@127.0.0.1:5432/postgres", CI_ENV),
])
def test_unqualified_cleanup_target_is_rejected(url, env):
    with pytest.raises(ValueError, match="disposable PostgreSQL"):
        checked_target(make_url(url), env)


@pytest.fixture
async def cleanup_pg(monkeypatch):
    raw = os.environ.get("CHANNEL_OPS_POSTGRES_TEST_URL")
    if not raw:
        pytest.skip("requires explicitly authorized disposable PostgreSQL 16")
    anchor = make_url(raw)
    system_id = checked_target(anchor, os.environ)
    admin = await asyncpg.connect(anchor.set(drivername="postgresql").render_as_string(hide_password=False))
    database = "vp_owned_inventory_test_cleanup_" + uuid.uuid4().hex[:24]
    target = anchor.set(database=database)
    created = False
    engine = None
    try:
        assert await admin.fetchval("SELECT current_database()") == anchor.database
        assert 160000 <= int(await admin.fetchval("SHOW server_version_num")) < 170000
        actual_system = str(await admin.fetchval("SELECT system_identifier FROM pg_control_system()"))
        assert system_id is None or actual_system == system_id
        await admin.execute(f'CREATE DATABASE "{database}"')
        created = True
        await migrate(target, "head")
        # The freshly created child is disposable even on the exact CI cluster.
        monkeypatch.setenv("CHANNEL_OPS_POSTGRES_TEST_CONFIRM", database)
        monkeypatch.setenv("CHANNEL_OPS_POSTGRES_TEST_SYSTEM_ID", actual_system)
        engine = create_async_engine(target)
        yield engine
    finally:
        if engine is not None:
            await engine.dispose()
        if created:
            await admin.execute(f'DROP DATABASE "{database}"')
        await admin.close()


async def seed_scope(engine, *, audit=False):
    async with async_sessionmaker(engine, expire_on_commit=False)() as db:
        channel = ChannelProfile(name="disposable cleanup", enabled=False)
        db.add(channel)
        await db.flush()
        account = PublishingAccount(channel_profile_id=channel.id, account_label="fixture")
        lane = TopicLane(channel_profile_id=channel.id, name="fixture")
        db.add_all([account, lane])
        await db.flush()
        if audit:
            lane_format = LaneFormatMatrix(topic_lane_id=lane.id, format_key="fixture")
            db.add(lane_format)
            await db.flush()
            starts = datetime.now(timezone.utc)
            inventory = OwnedSeedInventory(
                client_request_id=uuid.uuid4(), channel_profile_id=channel.id,
                topic_lane_id=lane.id, lane_format_id=lane_format.id, target_account_id=account.id,
                platform_channel_id="UC" + uuid.uuid4().hex[:22], request_sha256="a" * 64,
                manifest_sha256="b" * 64, manifest_json={}, starts_at=starts,
                expires_at=starts + timedelta(days=7), created_by="disposable-test",
            )
            db.add(inventory)
            await db.flush()
            db.add(OwnedInventoryRecoveryRecord(
                id=uuid.uuid4(), inventory_id=inventory.id, request_sha256="c" * 64,
                expected_hold_sha256="d" * 64, recovered_by="disposable-test",
                recovered_at=starts, transaction_id=1, before_inventory_json={"fixture": True},
                before_channel_json={}, evidence_json={"retain": "exactly"},
            ))
        await db.commit()
        return channel.id, account.id


async def assert_guard_enabled(engine):
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT tgenabled::text FROM pg_trigger WHERE tgrelid='public.owned_inventory_recoveries'::regclass AND tgname='owned_inventory_recovery_audit_no_truncate'")) == "O"
    with pytest.raises(DBAPIError, match="owned_inventory_recovery_audit_immutable"):
        async with engine.begin() as conn:
            await conn.execute(text("TRUNCATE public.owned_inventory_recoveries CASCADE"))


async def install_lock_probe(engine, table, *, fail=False):
    # Fires inside the actual TRUNCATE statement, while the helper's locks and
    # temporary exception are in force. No production function is replaced.
    async with engine.begin() as conn:
        await conn.execute(text("""
CREATE FUNCTION public.cleanup_lock_probe() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid()
        AND relation='public.owned_inventory_recoveries'::regclass
        AND mode='AccessExclusiveLock' AND granted)
        OR NOT EXISTS (SELECT 1 FROM pg_locks WHERE pid=pg_backend_pid()
            AND locktype='advisory' AND mode='ExclusiveLock' AND granted)
        OR (SELECT tgenabled FROM pg_trigger WHERE tgrelid='public.owned_inventory_recoveries'::regclass
            AND tgname='owned_inventory_recovery_audit_no_truncate') <> 'D'
        OR (SELECT tgenabled FROM pg_trigger WHERE tgrelid='public.owned_inventory_recoveries'::regclass
            AND tgname='owned_inventory_recovery_audit_immutable') <> 'O'
    THEN RAISE EXCEPTION 'cleanup_lock_proof_missing'; END IF;
    """ + ("RAISE EXCEPTION 'fixture_cleanup_injected_error';" if fail else "RETURN NULL;") + " END; $$"))
        await conn.execute(text(f"CREATE TRIGGER cleanup_lock_probe BEFORE TRUNCATE ON public.{table} FOR EACH STATEMENT EXECUTE FUNCTION public.cleanup_lock_probe()"))


@pytest.mark.asyncio
@pytest.mark.parametrize("plan,table", [("autoflow", "channel_profiles"), ("quarantine", "publishing_accounts")])
async def test_pg_cleanup_holds_locks_and_restores_guard(cleanup_pg, plan, table):
    engine = cleanup_pg
    await seed_scope(engine)
    await install_lock_probe(engine, table)
    await cleanup_disposable_postgres(engine, plan)
    async with engine.connect() as conn:
        assert await conn.scalar(text(f"SELECT count(*) FROM public.{table}")) == 0
    await assert_guard_enabled(engine)


@pytest.mark.asyncio
async def test_pg_failed_cleanup_rolls_back_trigger_and_rows(cleanup_pg):
    engine = cleanup_pg
    channel_id, _ = await seed_scope(engine)
    await install_lock_probe(engine, "channel_profiles", fail=True)
    with pytest.raises(DBAPIError, match="fixture_cleanup_injected_error"):
        await cleanup_disposable_postgres(engine, "autoflow")
    async with engine.connect() as conn:
        assert await conn.scalar(select(ChannelProfile.id).where(ChannelProfile.id == channel_id)) == channel_id
    await assert_guard_enabled(engine)


@pytest.mark.asyncio
async def test_pg_nonempty_recovery_evidence_is_never_cleaned(cleanup_pg):
    engine = cleanup_pg
    await seed_scope(engine, audit=True)
    async with engine.connect() as conn:
        before = (await conn.execute(text("SELECT to_jsonb(a) FROM public.owned_inventory_recoveries a"))).scalars().all()
    assert len(before) == 1
    for plan in ("autoflow", "quarantine"):
        with pytest.raises(ValueError, match="recovery evidence"):
            await cleanup_disposable_postgres(engine, plan)
        async with engine.connect() as conn:
            assert (await conn.execute(text("SELECT to_jsonb(a) FROM public.owned_inventory_recoveries a"))).scalars().all() == before
            assert await conn.scalar(text("SELECT count(*) FROM public.channel_profiles")) == 1
            assert await conn.scalar(text("SELECT count(*) FROM public.publishing_accounts")) == 1
        await assert_guard_enabled(engine)


@pytest.mark.asyncio
async def test_pg_wrong_actual_cluster_identity_refuses_cleanup(cleanup_pg, monkeypatch):
    engine = cleanup_pg
    await seed_scope(engine)
    monkeypatch.setenv("CHANNEL_OPS_POSTGRES_TEST_SYSTEM_ID", "1000000000000000000")
    with pytest.raises(ValueError, match="actual disposable PostgreSQL identity"):
        await cleanup_disposable_postgres(engine, "autoflow")
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM public.channel_profiles")) == 1
    await assert_guard_enabled(engine)
