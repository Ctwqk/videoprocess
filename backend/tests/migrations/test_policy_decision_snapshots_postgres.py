"""Parent-only PostgreSQL 16 qualification, skipped explicitly by offline authors."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg
import pytest
from sqlalchemy.engine import make_url
from sqlalchemy import CheckConstraint, UniqueConstraint

from app.models import channel_agent as models
from tests.channel_agent.test_policy_snapshot_models import (
    CONTRACTS, DECISION_OPTIONAL, FOREIGN_KEYS, PREVIOUS, REVISION, TICK_OPTIONAL,
)

POSTGRES_URL = os.getenv("CHANNEL_OPS_POSTGRES_TEST_URL", "")
BACKEND = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(
    not POSTGRES_URL,
    reason="parent PostgreSQL 16 qualification required: set CHANNEL_OPS_POSTGRES_TEST_URL; offline only",
)


def migrate(url, direction, revision):
    result = subprocess.run(
        [sys.executable, "-m", "alembic", direction, revision], cwd=BACKEND,
        env={**os.environ, "DATABASE_URL": url.render_as_string(hide_password=False)},
        capture_output=True, text=True, timeout=180,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.fixture
async def scratch():
    anchor = make_url(POSTGRES_URL)
    assert anchor.drivername in {"postgresql", "postgresql+asyncpg"}
    database = f"vp_policy_snapshots_{uuid.uuid4().hex}"
    url = anchor.set(drivername="postgresql+asyncpg", database=database)
    admin = await asyncpg.connect(
        anchor.set(drivername="postgresql", database="postgres").render_as_string(hide_password=False),
        timeout=10,
    )
    created = False
    try:
        assert 160000 <= int(await admin.fetchval("SHOW server_version_num")) < 170000
        await admin.execute(f'CREATE DATABASE "{database}"')
        created = True
        yield url
    finally:
        if created:
            await admin.execute(f'DROP DATABASE "{database}"')
        await admin.close()


async def connect(url):
    return await asyncpg.connect(url.set(drivername="postgresql").render_as_string(hide_password=False))


async def insert(conn, table, values):
    # Identifiers come only from test constants, never an external caller.
    columns = ", ".join(values)
    args = ", ".join(f"${i}" for i in range(1, len(values) + 1))
    return await conn.fetchval(
        f"INSERT INTO {table} ({columns}) VALUES ({args}) RETURNING id", *values.values(),
    )


def channel():
    return dict(
        id=uuid.uuid4(), name="policy fixture", positioning="", language="en",
        default_aspect_ratio="9:16", risk_policy_json="{}", content_mix_policy_json="{}",
        cadence_policy_json="{}", alert_policy_json="{}", enabled=False, dry_run=True,
        config_version=1, tick_interval_minutes=60,
    )


def tick(channel_id):
    return dict(
        id=uuid.uuid4(), channel_profile_id=channel_id, tick_id=uuid.uuid4().hex, dry_run=True,
        ideas_discovered=0, candidates_scored=0, tasks_selected=0, tasks_rejected=0,
        guards_triggered_json="[]", decision_summary_json="{}",
    )


def decision(tick_row):
    return dict(
        id=uuid.uuid4(), tick_audit_id=tick_row["id"], channel_profile_id=tick_row["channel_profile_id"],
        candidate_id="candidate-1", candidate_source="manual_seed", score_json="{}",
        guard_results_json="[]", pds_decision_json="{}", learning_context_json="{}", selected=False,
    )


def policy():
    return dict(
        policy_key="channelops-baseline", version="sha256:" + "a" * 64, status="draft",
        feature_schema_version="channelops-candidate-v1", reward_version="legacy-unversioned",
        formula_json='{"aggregate_score":null}', hard_guard_config_json="{}",
        portfolio_config_json="{}", exploration_config_json='{"enabled":false}',
        code_commit_sha="a" * 40, template_registry_version="legacy-unversioned",
        prompt_bundle_version="legacy-unversioned", config_hash="a" * 64,
        created_by="offline-test", change_reason="passive snapshot",
    )


async def assert_catalogue(conn):
    for model_name, required, optional in CONTRACTS:
        table = getattr(models, model_name).__table__
        rows = await conn.fetch(
            "SELECT column_name, is_nullable, column_default FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=$1", table.name,
        )
        assert {r["column_name"] for r in rows} == required | optional
        assert {r["column_name"] for r in rows if r["is_nullable"] == "NO"} == required
        assert all(r["column_default"] is None for r in rows if r["column_name"] not in {"id", "created_at"})
        constraints = await conn.fetch(
            "SELECT conname, contype::text AS kind, pg_get_constraintdef(oid) AS definition "
            "FROM pg_constraint WHERE conrelid=$1::regclass", table.name,
        )
        assert [r["definition"] for r in constraints if r["kind"] == "p"] == ["PRIMARY KEY (id)"]
        assert {r["conname"] for r in constraints if r["kind"] in {"c", "u"}} == {
            c.name for c in table.constraints if isinstance(c, (CheckConstraint, UniqueConstraint))
        }
    for table, columns in FOREIGN_KEYS.items():
        observed = await conn.fetch(
            "SELECT a.attname, c.confrelid::regclass::text AS target, "
            "c.confdeltype::text AS confdeltype, c.confupdtype::text AS confupdtype "
            "FROM pg_constraint c JOIN pg_attribute a ON a.attrelid=c.conrelid "
            "AND a.attnum=c.conkey[1] WHERE c.conrelid=$1::regclass AND c.contype='f'", table,
        )
        for name, target in columns.items():
            row, = [r for r in observed if r["attname"] == name]
            assert row["target"] == target.split(".")[0]
            assert row["confdeltype"] == "r"
            assert row["confupdtype"] in ("a", "r")


@pytest.mark.asyncio
@pytest.mark.parametrize("forward", [False, True], ids=["fresh", "legacy-forward"])
async def test_policy_snapshot_migration_contract_and_retention(scratch, forward):
    legacy_channel, legacy_tick, legacy_decision = channel(), None, None
    if forward:
        migrate(scratch, "upgrade", PREVIOUS)
        conn = await connect(scratch)
        try:
            await insert(conn, "channel_profiles", legacy_channel)
            legacy_tick = tick(legacy_channel["id"])
            await insert(conn, "agent_tick_audits", legacy_tick)
            legacy_decision = decision(legacy_tick)
            await insert(conn, "decision_audit_entries", legacy_decision)
        finally:
            await conn.close()
    migrate(scratch, "upgrade", REVISION)
    conn = await connect(scratch)
    try:
        assert await conn.fetchval("SELECT version_num FROM alembic_version") == REVISION
        await assert_catalogue(conn)
        assert await conn.fetchval("SELECT count(*) FROM agent_tick_audits") == int(forward)
        for name in ("decision_policy_versions", "policy_activation_history", "candidate_feature_snapshots"):
            assert await conn.fetchval(f"SELECT count(*) FROM {name}") == 0
        if forward:
            row = await conn.fetchrow("SELECT * FROM agent_tick_audits WHERE id=$1", legacy_tick["id"])
            assert row["replay_status"] == "legacy_unreplayable"
            assert all(row[name] is None for name in TICK_OPTIONAL)
            row = await conn.fetchrow("SELECT * FROM decision_audit_entries WHERE id=$1", legacy_decision["id"])
            assert all(row[name] is None for name in DECISION_OPTIONAL)
            assert json.loads(row["score_json"]) == {}

        parent = channel()
        await insert(conn, "channel_profiles", parent)
        unversioned = tick(parent["id"])
        await insert(conn, "agent_tick_audits", unversioned)
        old_decision = decision(unversioned)
        await insert(conn, "decision_audit_entries", old_decision)
        assert await conn.fetchval("SELECT replay_status FROM agent_tick_audits WHERE id=$1", unversioned["id"]) == "legacy_unreplayable"
        await conn.execute("DELETE FROM agent_tick_audits WHERE id=$1", unversioned["id"])
        assert not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM decision_audit_entries WHERE id=$1)", old_decision["id"])

        policy_row = policy()
        policy_id = await insert(conn, "decision_policy_versions", policy_row)
        for status in ("validated", "retired"):
            await insert(conn, "decision_policy_versions", {**policy_row, "version": status, "status": status})
        with pytest.raises(asyncpg.UniqueViolationError):
            await insert(conn, "decision_policy_versions", {**policy_row, "config_hash": "b" * 64})
        with pytest.raises(asyncpg.CheckViolationError):
            await insert(conn, "decision_policy_versions", {**policy_row, "version": "bad", "status": "active"})

        now = datetime.now(timezone.utc)
        activation = dict(
            channel_profile_id=parent["id"], policy_version_id=policy_id, mode="off",
            rollout_percentage=0, deterministic_salt="fixture", effective_from=now,
            request_id=uuid.uuid4().hex, actor="fixture", reason="test", feature_flag_snapshot_json="{}",
        )
        activation_id = await insert(conn, "policy_activation_history", activation)
        account_id = await insert(conn, "publishing_accounts", dict(
            channel_profile_id=parent["id"], platform="youtube", account_label="fixture",
            platform_account_id="", credential_ref="", platform_specific_config_json="{}",
            default_privacy="private", external_asset_auto_publish=False, enabled=False,
        ))
        await insert(conn, "policy_activation_history", {
            **activation, "request_id": uuid.uuid4().hex, "target_account_id": account_id,
        })
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute("DELETE FROM publishing_accounts WHERE id=$1", account_id)
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute("UPDATE publishing_accounts SET id=$1 WHERE id=$2", uuid.uuid4(), account_id)
        with pytest.raises(asyncpg.UniqueViolationError):
            await insert(conn, "policy_activation_history", activation)
        for mode in ("shadow", "canary", "active"):
            await insert(conn, "policy_activation_history", {
                **activation, "request_id": uuid.uuid4().hex, "mode": mode, "rollout_percentage": 100,
                "previous_activation_id": activation_id, "effective_to": now + timedelta(seconds=1),
            })
        for changes in (
            {"mode": "enabled"}, {"rollout_percentage": -1}, {"rollout_percentage": 101},
            {"effective_to": now}, {"effective_to": now - timedelta(seconds=1)},
        ):
            with pytest.raises(asyncpg.CheckViolationError):
                await insert(conn, "policy_activation_history", {**activation, "request_id": uuid.uuid4().hex, **changes})

        tick_row = {**tick(parent["id"]), "policy_version_id": policy_id, "replay_status": "snapshot_pending"}
        await insert(conn, "agent_tick_audits", tick_row)
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute("UPDATE agent_tick_audits SET replay_status='replayable' WHERE id=$1", tick_row["id"])
        snapshot = dict(
            tick_audit_id=tick_row["id"], candidate_id="candidate-1", candidate_source="manual_seed",
            source_kind="manual_seed", policy_version_id=policy_id, feature_schema_version="channelops-candidate-v1",
            feature_as_of=now, raw_features_json="{}", missing_feature_mask_json='{"baseline_score":true}',
            source_record_refs_json="{}", candidate_set_hash="b" * 64, feature_hash="c" * 64,
        )
        snapshot_id = await insert(conn, "candidate_feature_snapshots", snapshot)
        for table, values, required in (
            ("decision_policy_versions", policy_row, CONTRACTS[0][1]),
            ("policy_activation_history", activation, CONTRACTS[1][1]),
            ("candidate_feature_snapshots", snapshot, CONTRACTS[2][1]),
        ):
            for name in required - {"id", "created_at"}:
                with pytest.raises(asyncpg.NotNullViolationError):
                    await insert(conn, table, {**values, name: None})
        with pytest.raises(asyncpg.UniqueViolationError):
            await insert(conn, "candidate_feature_snapshots", snapshot)
        for table, values, column in (
            ("candidate_feature_snapshots", snapshot, "policy_version_id"),
            ("candidate_feature_snapshots", snapshot, "tick_audit_id"),
            ("policy_activation_history", activation, "channel_profile_id"),
            ("policy_activation_history", activation, "target_account_id"),
            ("policy_activation_history", activation, "previous_activation_id"),
            ("policy_activation_history", activation, "policy_version_id"),
        ):
            with pytest.raises(asyncpg.ForeignKeyViolationError):
                await insert(conn, table, {
                    **values, column: uuid.uuid4(),
                    **({"candidate_id": uuid.uuid4().hex} if table == "candidate_feature_snapshots" else {"request_id": uuid.uuid4().hex}),
                })
        linked = {**decision(tick_row), "policy_version_id": policy_id, "feature_snapshot_id": snapshot_id, "decision": "accepted"}
        linked_id = await insert(conn, "decision_audit_entries", linked)
        for column in ("policy_version_id", "feature_snapshot_id"):
            with pytest.raises(asyncpg.ForeignKeyViolationError):
                await insert(conn, "decision_audit_entries", {**linked, "id": uuid.uuid4(), column: uuid.uuid4()})
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await insert(conn, "agent_tick_audits", {**tick(parent["id"]), "policy_version_id": uuid.uuid4()})
        with pytest.raises(asyncpg.CheckViolationError):
            await insert(conn, "decision_audit_entries", {**linked, "id": uuid.uuid4(), "decision": "selected"})
        await insert(conn, "decision_audit_entries", {**linked, "id": uuid.uuid4(), "decision": "rejected"})
        stored = await conn.fetchrow("SELECT * FROM candidate_feature_snapshots WHERE id=$1", snapshot_id)
        assert all(stored[name] is None for name in CONTRACTS[2][2])
        stored = await conn.fetchrow("SELECT * FROM decision_audit_entries WHERE id=$1", linked_id)
        assert all(stored[name] is None for name in ("baseline_score", "final_score", "rank", "shadow_score", "shadow_rank", "shadow_selected", "experiment_id"))

        for table, row_id in (
            ("decision_policy_versions", policy_id), ("policy_activation_history", activation_id),
            ("candidate_feature_snapshots", snapshot_id), ("decision_audit_entries", linked_id),
        ):
            for operation in (f"UPDATE {table} SET id=id", f"DELETE FROM {table}"):
                with pytest.raises(asyncpg.RaiseError, match="immutable_policy_fact"):
                    await conn.execute(operation + " WHERE id=$1", row_id)
        # Snapshot FKs protect even pending ticks; complete empty ticks also need retention.
        with pytest.raises((asyncpg.ForeignKeyViolationError, asyncpg.RaiseError)):
            await conn.execute("DELETE FROM agent_tick_audits WHERE id=$1", tick_row["id"])
        await conn.execute("UPDATE agent_tick_audits SET replay_status='snapshot_complete' WHERE id=$1", tick_row["id"])
        empty_tick = {**tick(parent["id"]), "replay_status": "snapshot_complete", "policy_version_id": policy_id}
        await insert(conn, "agent_tick_audits", empty_tick)
        for row_id in (tick_row["id"], empty_tick["id"]):
            with pytest.raises(asyncpg.RaiseError, match="immutable_policy_fact"):
                await conn.execute("DELETE FROM agent_tick_audits WHERE id=$1", row_id)
            with pytest.raises(asyncpg.RaiseError, match="immutable_policy_fact"):
                await conn.execute("UPDATE agent_tick_audits SET replay_status='legacy_unreplayable' WHERE id=$1", row_id)
        with pytest.raises((asyncpg.ForeignKeyViolationError, asyncpg.RaiseError)):
            await conn.execute("DELETE FROM channel_profiles WHERE id=$1", parent["id"])
        if forward:
            await conn.execute("DELETE FROM channel_profiles WHERE id=$1", legacy_channel["id"])
            assert not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM agent_tick_audits WHERE id=$1)", legacy_tick["id"])
    finally:
        await conn.close()

    migrate(scratch, "downgrade", PREVIOUS)
    conn = await connect(scratch)
    try:
        assert await conn.fetchval("SELECT version_num FROM alembic_version") == PREVIOUS
        for table in ("decision_policy_versions", "policy_activation_history", "candidate_feature_snapshots"):
            assert await conn.fetchval("SELECT to_regclass($1)", table) is None
        assert await conn.fetchval("SELECT to_regprocedure('public.vp_immutable_policy_fact()')") is None
        assert not await conn.fetchval("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname IN ('trg_replayable_decision_immutable','trg_snapshot_complete_tick_retained'))")
        for table, columns in (("agent_tick_audits", TICK_OPTIONAL | {"replay_status"}), ("decision_audit_entries", DECISION_OPTIONAL)):
            observed = await conn.fetch("SELECT column_name FROM information_schema.columns WHERE table_name=$1", table)
            assert not columns & {r["column_name"] for r in observed}
        assert await conn.fetchval("SELECT count(*) FROM decision_audit_entries WHERE tick_audit_id=$1", tick_row["id"]) == 2
    finally:
        await conn.close()
    migrate(scratch, "upgrade", REVISION)
    conn = await connect(scratch)
    try:
        assert await conn.fetchval("SELECT count(*) FROM candidate_feature_snapshots") == 0
        assert await conn.fetchval("SELECT bool_and(replay_status='legacy_unreplayable') FROM agent_tick_audits")
    finally:
        await conn.close()
