from __future__ import annotations

import os
import subprocess
import uuid
from pathlib import Path

import asyncpg
import pytest


POSTGRES_URL = os.getenv("CHANNEL_OPS_POSTGRES_TEST_URL", "")
BACKEND_ROOT = Path(__file__).resolve().parents[2]

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
        [str(BACKEND_ROOT / ".venv/bin/alembic"), *args],
        cwd=BACKEND_ROOT,
        env={**os.environ, "DATABASE_URL": database_url},
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


async def _insert_old_writer_publication(
    conn: asyncpg.Connection,
    *,
    channel_id: uuid.UUID,
    account_id: uuid.UUID,
    task_id: uuid.UUID,
    publication_id: uuid.UUID,
) -> None:
    await conn.execute(
        """
        INSERT INTO channel_profiles (
            id, name, positioning, language, default_aspect_ratio, risk_policy_json,
            content_mix_policy_json, cadence_policy_json, alert_policy_json, enabled,
            dry_run, config_version, tick_interval_minutes, created_at, updated_at
        ) VALUES (
            $1, 'promotion migration channel', '', 'en', '9:16', '{}'::json,
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
            $1, $2, 'youtube', 'migration account', 'migration-account',
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
            created_at, updated_at
        ) VALUES (
            $1, $2, $3, 'manual_seed', 'migration title', 'migration prompt',
            '{}'::json, '{}'::json, 'explore', '[]'::json, '[]'::json, FALSE, 1,
            'produced', 0, 1, '{}'::json, '[]'::json, 'human', '{}'::json, NOW(), NOW()
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
            $1, $2, 'youtube', $3, 'migration-video', 'migration title', '',
            '[]'::json, 'unlisted', 'private', 'uploaded', 'allowed', 0,
            '[]'::json, NOW(), NOW()
        )
        """,
        publication_id,
        task_id,
        account_id,
    )


async def test_postgres_16_promotion_operation_migration_is_rolling_safe_and_reversible() -> None:
    database = f"vp_final_review10_{uuid.uuid4().hex}"
    admin_url = _database_url("postgres")
    admin = await asyncpg.connect(_asyncpg_url(admin_url))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()

    target_url = _database_url(database)
    publication_id = uuid.uuid4()
    try:
        _run_alembic(target_url, "upgrade", "head")
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert await conn.fetchval("SHOW server_version_num") >= "160000"
            assert (
                await conn.fetchval("SELECT version_num FROM alembic_version")
                == "029_channelops_discovery_ingestion_runs"
            )
            constraints = {
                row["conname"]
                for row in await conn.fetch(
                    "SELECT conname FROM pg_constraint "
                    "WHERE conrelid = 'publication_promotion_operations'::regclass"
                )
            }
            assert constraints >= {
                "uq_publication_promotion_operations_publication",
                "uq_publication_promotion_operations_queue_item",
                "uq_publication_promotion_operations_attempt_key",
                "ck_publication_promotion_operations_target_privacy",
                "ck_publication_promotion_operations_status",
            }

            channel_id = uuid.uuid4()
            account_id = uuid.uuid4()
            task_id = uuid.uuid4()
            queue_item_id = uuid.uuid4()
            await _insert_old_writer_publication(
                conn,
                channel_id=channel_id,
                account_id=account_id,
                task_id=task_id,
                publication_id=publication_id,
            )
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM publication_promotion_operations WHERE publication_id = $1",
                    publication_id,
                )
                == 0
            )

            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(
                    """
                    INSERT INTO publication_promotion_operations (
                        publication_id, production_task_id, queue_item_id,
                        platform_video_id, target_privacy, scheduled_at,
                        attempt_key, status
                    ) VALUES ($1, $2, $3, 'migration-video', 'public', NOW(), $4, 'reserved')
                    """,
                    publication_id,
                    task_id,
                    queue_item_id,
                    f"channelops-promotion:{uuid.uuid4()}",
                )

            await conn.execute(
                """
                INSERT INTO publication_promotion_operations (
                    publication_id, production_task_id, queue_item_id,
                    platform_video_id, target_privacy, scheduled_at,
                    attempt_key, status
                ) VALUES ($1, $2, $3, 'migration-video', 'unlisted', NOW(), $4, 'reserved')
                """,
                publication_id,
                task_id,
                queue_item_id,
                f"channelops-promotion:{uuid.uuid4()}",
            )
        finally:
            await conn.close()

        _run_alembic(target_url, "downgrade", "026_autoflow_authority_fence")
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert (
                await conn.fetchval("SELECT version_num FROM alembic_version")
                == "026_autoflow_authority_fence"
            )
            assert not await conn.fetchval(
                "SELECT to_regclass('public.publication_promotion_operations') IS NOT NULL"
            )
            assert await conn.fetchval(
                "SELECT count(*) FROM publication_records WHERE id = $1",
                publication_id,
            ) == 1
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
