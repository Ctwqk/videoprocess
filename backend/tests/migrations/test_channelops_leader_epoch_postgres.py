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
PREVIOUS_REVISION = "031_guarded_schedule_job_authority"
TARGET_REVISION = "032_channelops_leader_epoch"


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


def test_leader_epoch_migration_emits_required_postgres_ddl() -> None:
    completed = _run_alembic(
        "postgresql+asyncpg://migration:unused@127.0.0.1:9/videoprocess",
        "upgrade",
        f"{PREVIOUS_REVISION}:{TARGET_REVISION}",
        "--sql",
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    sql = completed.stdout
    assert "CREATE TABLE channelops_leader_epochs" in sql
    assert "service_name VARCHAR(64) NOT NULL" in sql
    assert "epoch BIGINT NOT NULL" in sql
    assert "holder_id VARCHAR(255) NOT NULL" in sql
    assert "acquired_at TIMESTAMP WITH TIME ZONE NOT NULL" in sql
    assert "heartbeat_at TIMESTAMP WITH TIME ZONE NOT NULL" in sql
    assert "released_at TIMESTAMP WITH TIME ZONE" in sql
    assert "ck_channelops_leader_epoch_positive" in sql
    assert "ck_channelops_leader_holder_nonempty" in sql
    assert "ck_channelops_leader_heartbeat_order" in sql
    assert "ck_channelops_leader_release_order" in sql


@pytest.mark.asyncio
@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for live migration tests",
)
async def test_leader_epoch_migration_is_durable_and_restrictive() -> None:
    database = f"vp_leader_epoch_{uuid.uuid4().hex}"
    admin = await asyncpg.connect(_asyncpg_url(_database_url("postgres")))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()

    target_url = _database_url(database)
    try:
        completed = _run_alembic(target_url, "upgrade", PREVIOUS_REVISION)
        assert completed.returncode == 0, completed.stdout + completed.stderr
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert await conn.fetchval("SELECT version_num FROM alembic_version") == PREVIOUS_REVISION
            assert await conn.fetchval(
                "SELECT to_regclass('public.channelops_leader_epochs') IS NULL"
            )
        finally:
            await conn.close()

        completed = _run_alembic(target_url, "upgrade", "head")
        assert completed.returncode == 0, completed.stdout + completed.stderr
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            columns = {
                row["column_name"]
                for row in await conn.fetch(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'channelops_leader_epochs'
                    """
                )
            }
            assert columns == {
                "service_name",
                "epoch",
                "holder_id",
                "acquired_at",
                "heartbeat_at",
                "released_at",
            }
            primary_key_columns = {
                row["attname"]
                for row in await conn.fetch(
                    """
                    SELECT attribute.attname
                    FROM pg_index AS index
                    JOIN pg_attribute AS attribute
                      ON attribute.attrelid = index.indrelid
                     AND attribute.attnum = ANY(index.indkey)
                    WHERE index.indrelid = 'public.channelops_leader_epochs'::regclass
                      AND index.indisprimary
                    """
                )
            }
            assert primary_key_columns == {"service_name"}

            acquired_at = "2026-07-23T12:00:00+00:00"
            heartbeat_at = "2026-07-23T12:01:00+00:00"
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(
                    """
                    INSERT INTO channelops_leader_epochs (
                        service_name, epoch, holder_id, acquired_at, heartbeat_at
                    ) VALUES ('epoch-zero', 0, 'holder', $1::timestamptz, $2::timestamptz)
                    """,
                    acquired_at,
                    heartbeat_at,
                )
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(
                    """
                    INSERT INTO channelops_leader_epochs (
                        service_name, epoch, holder_id, acquired_at, heartbeat_at
                    ) VALUES ('blank-holder', 1, '   ', $1::timestamptz, $2::timestamptz)
                    """,
                    acquired_at,
                    heartbeat_at,
                )
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(
                    """
                    INSERT INTO channelops_leader_epochs (
                        service_name, epoch, holder_id, acquired_at, heartbeat_at
                    ) VALUES ('heartbeat-before-acquired', 1, 'holder', $1::timestamptz, $2::timestamptz)
                    """,
                    heartbeat_at,
                    acquired_at,
                )
        finally:
            await conn.close()
    finally:
        admin = await asyncpg.connect(_asyncpg_url(_database_url("postgres")))
        try:
            await admin.execute(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')
        finally:
            await admin.close()
