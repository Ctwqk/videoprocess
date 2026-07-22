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

pytestmark = pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for migration tests",
)

EXPECTED_COLUMN_METADATA = {
    "id": ("uuid", True, "gen_random_uuid()"),
    "channel_profile_id": ("uuid", True, None),
    "queue_item_id": ("uuid", False, None),
    "source": ("character varying(64)", True, "'youtube_search'::character varying"),
    "scheduler_bucket": ("character varying(64)", True, None),
    "query_version": (
        "character varying(64)",
        True,
        "'youtube-lane-keyword-v1'::character varying",
    ),
    "status": ("character varying(16)", True, "'running'::character varying"),
    "attempt_count": ("integer", True, "1"),
    "query_count": ("integer", True, "0"),
    "created_count": ("integer", True, "0"),
    "refreshed_count": ("integer", True, "0"),
    "expired_count": ("integer", True, "0"),
    "quota_units_estimated": ("integer", True, "0"),
    "policy_snapshot_json": ("json", True, "'{}'::json"),
    "started_at": ("timestamp with time zone", True, "now()"),
    "finished_at": ("timestamp with time zone", False, None),
    "last_error_code": ("character varying(64)", False, None),
}
EXPECTED_CHECK_DEFINITIONS = {
    "ck_discovery_ingestion_run_attempt_count": "CHECK (attempt_count >= 1)",
    "ck_discovery_ingestion_run_created_count": "CHECK (created_count >= 0)",
    "ck_discovery_ingestion_run_expired_count": "CHECK (expired_count >= 0)",
    "ck_discovery_ingestion_run_query_count": "CHECK (query_count >= 0)",
    "ck_discovery_ingestion_run_query_version": (
        "CHECK (query_version::text = 'youtube-lane-keyword-v1'::text)"
    ),
    "ck_discovery_ingestion_run_quota_units_estimated": (
        "CHECK (quota_units_estimated >= 0)"
    ),
    "ck_discovery_ingestion_run_refreshed_count": "CHECK (refreshed_count >= 0)",
    "ck_discovery_ingestion_run_source": "CHECK (source::text = 'youtube_search'::text)",
    "ck_discovery_ingestion_run_status": (
        "CHECK (status::text = ANY (ARRAY['running'::character varying, "
        "'succeeded'::character varying, 'failed'::character varying]::text[]))"
    ),
}
EXPECTED_UNIQUE_DEFINITIONS = {
    "uq_discovery_ingestion_run_channel_source_bucket": (
        "UNIQUE (channel_profile_id, source, scheduler_bucket)"
    ),
    "uq_discovery_ingestion_run_queue_item": "UNIQUE (queue_item_id)",
}
EXPECTED_FOREIGN_KEYS = {
    "channel_profile_id": ("channel_profiles", "id", "CASCADE"),
    "queue_item_id": ("channel_ops_queue_items", "id", "SET NULL"),
}


def _assert_exact_column_metadata(
    observed: dict[str, tuple[str, bool, str | None]],
) -> None:
    assert observed == EXPECTED_COLUMN_METADATA


async def _assert_parent_rows_exist(
    conn: asyncpg.Connection,
    *,
    channel_ids: list[uuid.UUID],
    queue_item_ids: list[uuid.UUID],
) -> None:
    assert await conn.fetchval(
        "SELECT count(*) FROM channel_profiles WHERE id = ANY($1::uuid[])",
        channel_ids,
    ) == len(channel_ids)
    assert await conn.fetchval(
        "SELECT count(*) FROM channel_ops_queue_items WHERE id = ANY($1::uuid[])",
        queue_item_ids,
    ) == len(queue_item_ids)


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


@pytest.mark.asyncio
async def test_discovery_ingestion_run_migration_is_reversible_and_enforces_idempotency() -> None:
    database = f"vp_discovery_ingestion_runs_{uuid.uuid4().hex}"
    admin_url = _database_url("postgres")
    admin = await asyncpg.connect(_asyncpg_url(admin_url))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()

    target_url = _database_url(database)
    channel_id = uuid.uuid4()
    other_channel_id = uuid.uuid4()
    queue_item_id = uuid.uuid4()
    other_queue_item_id = uuid.uuid4()
    channel_ids = [channel_id, other_channel_id]
    queue_item_ids = [queue_item_id, other_queue_item_id]
    try:
        _run_alembic(target_url, "upgrade", PREVIOUS_REVISION)
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert await conn.fetchval("SELECT version_num FROM alembic_version") == PREVIOUS_REVISION
            await _insert_channel(conn, channel_id)
            await _insert_channel(conn, other_channel_id)
            await _insert_queue_item(conn, queue_item_id, channel_id)
            await _insert_queue_item(conn, other_queue_item_id, other_channel_id)
            await _assert_parent_rows_exist(
                conn,
                channel_ids=channel_ids,
                queue_item_ids=queue_item_ids,
            )
        finally:
            await conn.close()

        _run_alembic(target_url, "upgrade", TARGET_REVISION)
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert await conn.fetchval("SELECT version_num FROM alembic_version") == TARGET_REVISION
            columns = {
                row["column_name"]: (
                    row["formatted_type"],
                    row["not_null"],
                    row["column_default"],
                )
                for row in await conn.fetch(
                    """
                    SELECT
                        attribute.attname AS column_name,
                        format_type(attribute.atttypid, attribute.atttypmod) AS formatted_type,
                        attribute.attnotnull AS not_null,
                        pg_get_expr(default_value.adbin, default_value.adrelid) AS column_default
                    FROM pg_attribute AS attribute
                    LEFT JOIN pg_attrdef AS default_value
                      ON default_value.adrelid = attribute.attrelid
                     AND default_value.adnum = attribute.attnum
                    WHERE attribute.attrelid = 'discovery_ingestion_runs'::regclass
                      AND attribute.attnum > 0
                      AND NOT attribute.attisdropped
                    ORDER BY attribute.attnum
                    """
                )
            }
            _assert_exact_column_metadata(columns)

            check_definitions = {
                row["constraint_name"]: row["definition"]
                for row in await conn.fetch(
                    """
                    SELECT
                        conname AS constraint_name,
                        pg_get_constraintdef(oid, TRUE) AS definition
                    FROM pg_constraint
                    WHERE conrelid = 'discovery_ingestion_runs'::regclass
                      AND contype = 'c'
                    """
                )
            }
            assert check_definitions == EXPECTED_CHECK_DEFINITIONS

            unique_definitions = {
                row["constraint_name"]: row["definition"]
                for row in await conn.fetch(
                    """
                    SELECT
                        conname AS constraint_name,
                        pg_get_constraintdef(oid, TRUE) AS definition
                    FROM pg_constraint
                    WHERE conrelid = 'discovery_ingestion_runs'::regclass
                      AND contype = 'u'
                    """
                )
            }
            assert unique_definitions == EXPECTED_UNIQUE_DEFINITIONS

            foreign_keys = {
                row["source_column"]: (
                    row["target_table"],
                    row["target_column"],
                    row["delete_action"],
                )
                for row in await conn.fetch(
                    """
                    SELECT
                        source_attribute.attname AS source_column,
                        target_table.relname AS target_table,
                        target_attribute.attname AS target_column,
                        CASE constraint_row.confdeltype
                            WHEN 'c' THEN 'CASCADE'
                            WHEN 'n' THEN 'SET NULL'
                            WHEN 'r' THEN 'RESTRICT'
                            WHEN 'a' THEN 'NO ACTION'
                            WHEN 'd' THEN 'SET DEFAULT'
                        END AS delete_action
                    FROM pg_constraint AS constraint_row
                    JOIN pg_class AS target_table
                      ON target_table.oid = constraint_row.confrelid
                    JOIN pg_attribute AS source_attribute
                      ON source_attribute.attrelid = constraint_row.conrelid
                     AND source_attribute.attnum = constraint_row.conkey[1]
                    JOIN pg_attribute AS target_attribute
                      ON target_attribute.attrelid = constraint_row.confrelid
                     AND target_attribute.attnum = constraint_row.confkey[1]
                    WHERE constraint_row.conrelid = 'discovery_ingestion_runs'::regclass
                      AND constraint_row.contype = 'f'
                    """
                )
            }
            assert foreign_keys == EXPECTED_FOREIGN_KEYS
            await _assert_parent_rows_exist(
                conn,
                channel_ids=channel_ids,
                queue_item_ids=queue_item_ids,
            )

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
            await _assert_parent_rows_exist(
                conn,
                channel_ids=channel_ids,
                queue_item_ids=queue_item_ids,
            )
        finally:
            await conn.close()

        _run_alembic(target_url, "upgrade", TARGET_REVISION)
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert await conn.fetchval("SELECT version_num FROM alembic_version") == TARGET_REVISION
            assert await conn.fetchval(
                "SELECT to_regclass('public.discovery_ingestion_runs') IS NOT NULL"
            )
            assert await conn.fetchval("SELECT count(*) FROM discovery_ingestion_runs") == 0
            await _assert_parent_rows_exist(
                conn,
                channel_ids=channel_ids,
                queue_item_ids=queue_item_ids,
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
