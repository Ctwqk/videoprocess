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
PREVIOUS_REVISION = "028_channelops_metric_schedules"
TARGET_REVISION = "029_channelops_discovery_ingestion_runs"

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(not POSTGRES_URL, reason="set CHANNEL_OPS_POSTGRES_TEST_URL for migration tests"),
]


def _asyncpg_url(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


def _database_url(database: str) -> str:
    base = POSTGRES_URL.rsplit("/", 1)[0]
    return f"{base}/{database}"


def _run_alembic(database_url: str, *args: str) -> None:
    completed = subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=BACKEND_ROOT,
        env={**os.environ, "DATABASE_URL": database_url},
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


async def _insert_channel(conn: asyncpg.Connection, channel_id: uuid.UUID) -> None:
    await conn.execute(
        """
        INSERT INTO channel_profiles (
            id, name, positioning, language, default_aspect_ratio, risk_policy_json,
            content_mix_policy_json, cadence_policy_json, alert_policy_json, enabled,
            dry_run, config_version, tick_interval_minutes, created_at, updated_at
        ) VALUES (
            $1, 'discovery migration channel', '', 'en', '9:16', '{}'::json,
            '{}'::json, '{}'::json, '{}'::json, TRUE, FALSE, 1, 60, NOW(), NOW()
        )
        """,
        channel_id,
    )


async def _insert_queue_item(conn: asyncpg.Connection, queue_item_id: uuid.UUID, channel_id: uuid.UUID) -> None:
    await conn.execute(
        """
        INSERT INTO channel_ops_queue_items (
            id, kind, idempotency_key, channel_profile_id, priority, payload_json,
            status, attempt_count, max_attempts
        ) VALUES ($1, 'ingest_discovery', $2, $3, 80, '{}'::json, 'running', 1, 3)
        """,
        queue_item_id,
        f"discovery-migration:{queue_item_id}",
        channel_id,
    )


async def _insert_run(
    conn: asyncpg.Connection,
    *,
    channel_id: uuid.UUID,
    queue_item_id: uuid.UUID | None,
    bucket: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO discovery_ingestion_runs (
            channel_profile_id, queue_item_id, source, scheduler_bucket, query_version,
            status, attempt_count, query_count, created_count, refreshed_count,
            expired_count, quota_units_estimated, policy_snapshot_json
        ) VALUES (
            $1, $2, 'youtube_search', $3, 'youtube-lane-keyword-v1',
            'running', 1, 0, 0, 0, 0, 0, '{}'::json
        )
        """,
        channel_id,
        queue_item_id,
        bucket,
    )


async def test_discovery_ingestion_run_migration_is_reversible_and_enforces_idempotency() -> None:
    database = f"vp_discovery_ingestion_runs_{uuid.uuid4().hex}"
    admin_url = _database_url("postgres")
    admin = await asyncpg.connect(_asyncpg_url(admin_url))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()

    target_url = _database_url(database)
    try:
        _run_alembic(target_url, "upgrade", "head")
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert await conn.fetchval("SELECT version_num FROM alembic_version") == TARGET_REVISION
            columns = {
                row["column_name"]: (row["data_type"], row["character_maximum_length"])
                for row in await conn.fetch(
                    """
                    SELECT column_name, data_type, character_maximum_length
                    FROM information_schema.columns
                    WHERE table_name = 'discovery_ingestion_runs'
                    """
                )
            }
            assert set(columns) >= {
                "id",
                "channel_profile_id",
                "queue_item_id",
                "source",
                "scheduler_bucket",
                "query_version",
                "status",
                "attempt_count",
                "query_count",
                "created_count",
                "refreshed_count",
                "expired_count",
                "quota_units_estimated",
                "policy_snapshot_json",
                "started_at",
                "finished_at",
                "last_error_code",
            }
            assert columns["policy_snapshot_json"][0] == "json"
            assert columns["started_at"][0] == "timestamp with time zone"
            assert columns["finished_at"][0] == "timestamp with time zone"
            assert columns["last_error_code"] == ("character varying", 64)

            constraints = {
                row["conname"]
                for row in await conn.fetch(
                    "SELECT conname FROM pg_constraint "
                    "WHERE conrelid = 'discovery_ingestion_runs'::regclass"
                )
            }
            assert constraints >= {
                "uq_discovery_ingestion_run_channel_source_bucket",
                "uq_discovery_ingestion_run_queue_item",
                "ck_discovery_ingestion_run_source",
                "ck_discovery_ingestion_run_status",
                "ck_discovery_ingestion_run_attempt_count",
            }

            channel_id = uuid.uuid4()
            other_channel_id = uuid.uuid4()
            queue_item_id = uuid.uuid4()
            other_queue_item_id = uuid.uuid4()
            await _insert_channel(conn, channel_id)
            await _insert_channel(conn, other_channel_id)
            await _insert_queue_item(conn, queue_item_id, channel_id)
            await _insert_queue_item(conn, other_queue_item_id, other_channel_id)
            await _insert_run(
                conn,
                channel_id=channel_id,
                queue_item_id=queue_item_id,
                bucket="2026-07-21-18",
            )

            with pytest.raises(asyncpg.UniqueViolationError):
                await _insert_run(
                    conn,
                    channel_id=channel_id,
                    queue_item_id=other_queue_item_id,
                    bucket="2026-07-21-18",
                )
            with pytest.raises(asyncpg.UniqueViolationError):
                await _insert_run(
                    conn,
                    channel_id=other_channel_id,
                    queue_item_id=queue_item_id,
                    bucket="2026-07-21-19",
                )
        finally:
            await conn.close()

        _run_alembic(target_url, "downgrade", PREVIOUS_REVISION)
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert not await conn.fetchval(
                "SELECT to_regclass('public.discovery_ingestion_runs') IS NOT NULL"
            )
        finally:
            await conn.close()

        _run_alembic(target_url, "upgrade", "head")
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert await conn.fetchval("SELECT version_num FROM alembic_version") == TARGET_REVISION
            assert await conn.fetchval(
                "SELECT to_regclass('public.discovery_ingestion_runs') IS NOT NULL"
            )
        finally:
            await conn.close()
    finally:
        admin = await asyncpg.connect(_asyncpg_url(admin_url))
        try:
            await admin.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1",
                database,
            )
            await admin.execute(f'DROP DATABASE IF EXISTS "{database}"')
        finally:
            await admin.close()
