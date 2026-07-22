from __future__ import annotations

import os
import subprocess
import sys
import uuid
from pathlib import Path

import asyncpg
import pytest


POSTGRES_URL = os.getenv("CHANNEL_OPS_POSTGRES_TEST_URL", "")
BACKEND_ROOT = Path(__file__).resolve().parents[2]
PREVIOUS_REVISION = "029_channelops_discovery_ingestion_runs"
TARGET_REVISION = "030_channelops_intake_pause"


def _asyncpg_url(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


def _database_url(database: str) -> str:
    return f"{POSTGRES_URL.rsplit('/', 1)[0]}/{database}"


def _run_alembic(database_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=BACKEND_ROOT,
        env={**os.environ, "DATABASE_URL": database_url},
        text=True,
        capture_output=True,
        check=False,
    )


def test_intake_pause_migration_emits_nullable_additive_columns() -> None:
    completed = _run_alembic(
        "postgresql+asyncpg://migration:unused@127.0.0.1:9/videoprocess",
        "upgrade",
        f"{PREVIOUS_REVISION}:{TARGET_REVISION}",
        "--sql",
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    sql = completed.stdout
    assert "ADD COLUMN intake_paused_at TIMESTAMP WITH TIME ZONE" in sql
    assert "ADD COLUMN intake_pause_reason TEXT" in sql
    assert "UPDATE channel_profiles" not in sql
    assert "NOT NULL" not in "\n".join(
        line for line in sql.splitlines() if "intake_pause" in line
    )


@pytest.mark.asyncio
@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for live migration tests",
)
async def test_intake_pause_migration_preserves_channels_and_is_reversible() -> None:
    database = f"vp_intake_pause_{uuid.uuid4().hex}"
    admin = await asyncpg.connect(_asyncpg_url(_database_url("postgres")))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()

    target_url = _database_url(database)
    channel_id = uuid.uuid4()
    try:
        completed = _run_alembic(target_url, "upgrade", PREVIOUS_REVISION)
        assert completed.returncode == 0, completed.stdout + completed.stderr
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            await conn.execute(
                """
                INSERT INTO channel_profiles (
                    id, name, positioning, language, default_aspect_ratio,
                    risk_policy_json, content_mix_policy_json, cadence_policy_json,
                    alert_policy_json, enabled, dry_run, config_version,
                    tick_interval_minutes, created_at, updated_at
                ) VALUES (
                    $1, 'historical channel', '', 'en', '9:16', '{}'::json,
                    '{}'::json, '{}'::json, '{}'::json, TRUE, FALSE, 1, 60,
                    NOW(), NOW()
                )
                """,
                channel_id,
            )
        finally:
            await conn.close()

        completed = _run_alembic(target_url, "upgrade", TARGET_REVISION)
        assert completed.returncode == 0, completed.stdout + completed.stderr
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            row = await conn.fetchrow(
                """
                SELECT name, intake_paused_at, intake_pause_reason
                FROM channel_profiles WHERE id = $1
                """,
                channel_id,
            )
            assert tuple(row.values()) == ("historical channel", None, None)
            await conn.execute(
                """
                UPDATE channel_profiles
                SET intake_paused_at = NOW(), intake_pause_reason = 'guarded canary'
                WHERE id = $1
                """,
                channel_id,
            )
        finally:
            await conn.close()

        completed = _run_alembic(target_url, "downgrade", PREVIOUS_REVISION)
        assert completed.returncode == 0, completed.stdout + completed.stderr
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert await conn.fetchval(
                "SELECT name FROM channel_profiles WHERE id = $1",
                channel_id,
            ) == "historical channel"
            assert not await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'channel_profiles'
                      AND column_name IN ('intake_paused_at', 'intake_pause_reason')
                )
                """
            )
        finally:
            await conn.close()
    finally:
        admin = await asyncpg.connect(_asyncpg_url(_database_url("postgres")))
        try:
            await admin.execute(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')
        finally:
            await admin.close()
