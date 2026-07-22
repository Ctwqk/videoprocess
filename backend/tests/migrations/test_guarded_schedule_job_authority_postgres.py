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
PREVIOUS_REVISION = "030_channelops_intake_pause"
TARGET_REVISION = "031_guarded_schedule_job_authority"


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


def test_guarded_schedule_job_authority_migration_emits_nullable_restrictive_ddl() -> None:
    completed = _run_alembic(
        "postgresql+asyncpg://migration:unused@127.0.0.1:9/videoprocess",
        "upgrade",
        f"{PREVIOUS_REVISION}:{TARGET_REVISION}",
        "--sql",
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    sql = completed.stdout
    assert "ADD COLUMN guarded_job_id UUID" in sql
    assert "fk_runtime_schedules_guarded_job_id_jobs" in sql
    assert "FOREIGN KEY(guarded_job_id) REFERENCES jobs (id)" in sql
    assert "ON DELETE" not in sql


async def _insert_historical_job(conn: asyncpg.Connection, job_id: uuid.UUID) -> None:
    pipeline_id = uuid.uuid4()
    await conn.execute(
        """
        INSERT INTO pipelines (id, name, definition)
        VALUES ($1, 'guarded schedule migration pipeline', '{"version":"1.0","nodes":[],"edges":[]}'::json)
        """,
        pipeline_id,
    )
    await conn.execute(
        """
        INSERT INTO jobs (id, pipeline_id, pipeline_snapshot, status, orchestrator_owner)
        VALUES ($1, $2, '{"version":"1.0","nodes":[],"edges":[]}'::json, 'PENDING', 'python')
        """,
        job_id,
        pipeline_id,
    )


@pytest.mark.asyncio
@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for live migration tests",
)
async def test_guarded_schedule_job_authority_migration_is_reversible_and_restrictive() -> None:
    database = f"vp_guarded_schedule_authority_{uuid.uuid4().hex}"
    admin_url = _database_url("postgres")
    admin = await asyncpg.connect(_asyncpg_url(admin_url))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()

    target_url = _database_url(database)
    job_id = uuid.uuid4()
    try:
        completed = _run_alembic(target_url, "upgrade", PREVIOUS_REVISION)
        assert completed.returncode == 0, completed.stdout + completed.stderr
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            await conn.execute(
                """
                INSERT INTO runtime_schedules (service_name, state, updated_by)
                VALUES ('historical', 'OPEN', 'migration test')
                """
            )
            await _insert_historical_job(conn, job_id)
        finally:
            await conn.close()

        completed = _run_alembic(target_url, "upgrade", TARGET_REVISION)
        assert completed.returncode == 0, completed.stdout + completed.stderr
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert await conn.fetchval("SELECT version_num FROM alembic_version") == TARGET_REVISION
            assert await conn.fetchval(
                "SELECT guarded_job_id FROM runtime_schedules WHERE service_name = 'historical'"
            ) is None
            await conn.execute(
                """
                UPDATE runtime_schedules
                SET guarded_job_id = $1
                WHERE service_name = 'historical'
                """,
                job_id,
            )
            with pytest.raises(asyncpg.ForeignKeyViolationError):
                await conn.execute("DELETE FROM jobs WHERE id = $1", job_id)
        finally:
            await conn.close()

        completed = _run_alembic(target_url, "downgrade", PREVIOUS_REVISION)
        assert completed.returncode == 0, completed.stdout + completed.stderr
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert not await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'runtime_schedules' AND column_name = 'guarded_job_id'
                )
                """
            )
        finally:
            await conn.close()

        completed = _run_alembic(target_url, "upgrade", TARGET_REVISION)
        assert completed.returncode == 0, completed.stdout + completed.stderr
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert await conn.fetchval("SELECT version_num FROM alembic_version") == TARGET_REVISION
            assert await conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'runtime_schedules' AND column_name = 'guarded_job_id'
                )
                """
            )
        finally:
            await conn.close()
    finally:
        admin = await asyncpg.connect(_asyncpg_url(admin_url))
        try:
            await admin.execute(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')
        finally:
            await admin.close()
