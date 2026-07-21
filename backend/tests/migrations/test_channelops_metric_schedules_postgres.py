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
PREVIOUS_REVISION = "027_publication_promotion_operations"
TARGET_REVISION = "028_channelops_metric_schedules"


def _asyncpg_url(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


def _database_url(database: str) -> str:
    base = POSTGRES_URL.rsplit("/", 1)[0]
    return f"{base}/{database}"


def _run_alembic(database_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=BACKEND_ROOT,
        env={**os.environ, "DATABASE_URL": database_url},
        text=True,
        capture_output=True,
        check=False,
    )


def test_metric_schedule_migration_emits_safe_postgres_ddl_without_backfill() -> None:
    completed = _run_alembic(
        "postgresql+asyncpg://migration:unused@127.0.0.1:9/videoprocess",
        "upgrade",
        f"{PREVIOUS_REVISION}:{TARGET_REVISION}",
        "--sql",
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    sql = completed.stdout
    assert "CREATE TABLE publication_metric_schedules" in sql
    assert "uq_metric_schedule_publication_stage" in sql
    assert "ck_metric_schedule_stage" in sql
    assert "ck_metric_schedule_status" in sql
    assert "ck_metric_schedule_attempt_count" in sql
    assert "ck_metric_schedule_due_order" in sql
    assert "ck_metric_schedule_grace_order" in sql
    assert "ix_metric_schedules_status_due" in sql
    assert "ON DELETE CASCADE" in sql
    assert "INSERT INTO publication_metric_schedules" not in sql
    assert "INSERT INTO channel_ops_queue_items" not in sql


async def _insert_historical_publication(
    conn: asyncpg.Connection,
    *,
    publication_id: uuid.UUID,
) -> None:
    channel_id = uuid.uuid4()
    account_id = uuid.uuid4()
    task_id = uuid.uuid4()
    await conn.execute(
        """
        INSERT INTO channel_profiles (
            id, name, positioning, language, default_aspect_ratio, risk_policy_json,
            content_mix_policy_json, cadence_policy_json, alert_policy_json, enabled,
            dry_run, config_version, tick_interval_minutes, created_at, updated_at
        ) VALUES (
            $1, 'metric migration channel', '', 'en', '9:16', '{}'::json,
            '{}'::json, '{}'::json, '{}'::json, TRUE, FALSE, 1, 60, NOW(), NOW()
        )
        """,
        channel_id,
    )
    await conn.execute(
        """
        INSERT INTO publishing_accounts (
            id, channel_profile_id, platform, account_label, platform_account_id,
            credential_ref, platform_specific_config_json, default_privacy,
            external_asset_auto_publish, enabled, created_at, updated_at
        ) VALUES (
            $1, $2, 'youtube', 'metric migration account', 'migration-account',
            'migration/credential', '{}'::json, 'unlisted', FALSE, TRUE, NOW(), NOW()
        )
        """,
        account_id,
        channel_id,
    )
    await conn.execute(
        """
        INSERT INTO production_tasks (
            id, channel_profile_id, target_account_id, source, title_seed, prompt,
            rationale_json, score_breakdown_json, portfolio_bucket, source_platforms_json,
            material_library_ids_json, uses_external_assets, priority, state, retry_count,
            channel_config_version_snapshot, channel_config_snapshot_json,
            transition_history_json, approval_mode, agent_approval_evidence_json,
            human_review_evidence_json, created_at, updated_at
        ) VALUES (
            $1, $2, $3, 'manual_seed', 'migration title', 'migration prompt',
            '{}'::json, '{}'::json, 'explore', '[]'::json, '[]'::json, FALSE, 1,
            'scheduled', 0, 1, '{}'::json, '[]'::json, 'human', '{}'::json,
            '{}'::json, NOW(), NOW()
        )
        """,
        task_id,
        channel_id,
        account_id,
    )
    await conn.execute(
        """
        INSERT INTO publication_records (
            id, production_task_id, platform, account_id, platform_content_id,
            title, description, tags_json, desired_privacy, current_privacy,
            publish_status, compliance_disposition, quota_units_estimated,
            warnings_json, created_at, updated_at
        ) VALUES (
            $1, $2, 'youtube', $3, 'metric-migration-video', 'migration title', '',
            '[]'::json, 'unlisted', 'unlisted', 'scheduled', 'allowed', 0,
            '[]'::json, NOW(), NOW()
        )
        """,
        publication_id,
        task_id,
        account_id,
    )
    await conn.execute(
        """
        INSERT INTO channel_ops_queue_items (
            id, kind, idempotency_key, priority, payload_json, status,
            attempt_count, max_attempts, channel_profile_id
        ) VALUES (
            $1, 'collect_metrics', $2, 90,
            json_build_object('publication_id', $3::text), 'queued', 0, 3, $4
        )
        """,
        uuid.uuid4(),
        f"collect_metrics:{publication_id}:poll:0",
        publication_id,
        channel_id,
    )


@pytest.mark.asyncio
@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for live migration tests",
)
async def test_metric_schedule_migration_is_reversible_and_does_not_backfill() -> None:
    database = f"vp_metric_schedules_{uuid.uuid4().hex}"
    admin_url = _database_url("postgres")
    admin = await asyncpg.connect(_asyncpg_url(admin_url))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()

    target_url = _database_url(database)
    publication_id = uuid.uuid4()
    try:
        completed = _run_alembic(target_url, "upgrade", PREVIOUS_REVISION)
        assert completed.returncode == 0, completed.stdout + completed.stderr
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            await _insert_historical_publication(conn, publication_id=publication_id)
        finally:
            await conn.close()

        completed = _run_alembic(target_url, "upgrade", TARGET_REVISION)
        assert completed.returncode == 0, completed.stdout + completed.stderr
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert await conn.fetchval("SELECT version_num FROM alembic_version") == TARGET_REVISION
            assert await conn.fetchval("SELECT count(*) FROM publication_metric_schedules") == 0
            assert await conn.fetchval(
                "SELECT count(*) FROM channel_ops_queue_items "
                "WHERE idempotency_key LIKE 'collect_metrics:%:stage:%'"
            ) == 0
            assert await conn.fetchval(
                "SELECT count(*) FROM channel_ops_queue_items "
                "WHERE idempotency_key = $1",
                f"collect_metrics:{publication_id}:poll:0",
            ) == 1

            await conn.execute(
                """
                INSERT INTO publication_metric_schedules (
                    publication_id, snapshot_stage, effective_start_at, due_at, grace_until
                ) VALUES ($1, '24h', NOW(), NOW() + INTERVAL '24 hours', NOW() + INTERVAL '30 hours')
                """,
                publication_id,
            )
            with pytest.raises(asyncpg.UniqueViolationError):
                await conn.execute(
                    """
                    INSERT INTO publication_metric_schedules (
                        publication_id, snapshot_stage, effective_start_at, due_at, grace_until
                    ) VALUES ($1, '24h', NOW(), NOW() + INTERVAL '24 hours', NOW() + INTERVAL '30 hours')
                    """,
                    publication_id,
                )
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(
                    """
                    INSERT INTO publication_metric_schedules (
                        publication_id, snapshot_stage, effective_start_at, due_at, grace_until
                    ) VALUES ($1, 'immediate', NOW(), NOW(), NOW())
                    """,
                    publication_id,
                )
        finally:
            await conn.close()

        completed = _run_alembic(target_url, "downgrade", PREVIOUS_REVISION)
        assert completed.returncode == 0, completed.stdout + completed.stderr
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert not await conn.fetchval(
                "SELECT to_regclass('public.publication_metric_schedules') IS NOT NULL"
            )
            assert await conn.fetchval(
                "SELECT count(*) FROM publication_records WHERE id = $1",
                publication_id,
            ) == 1
        finally:
            await conn.close()

        completed = _run_alembic(target_url, "upgrade", TARGET_REVISION)
        assert completed.returncode == 0, completed.stdout + completed.stderr
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert await conn.fetchval("SELECT count(*) FROM publication_metric_schedules") == 0
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
