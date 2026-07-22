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


async def _insert_channel(conn: asyncpg.Connection, channel_id: uuid.UUID, *, halted: bool) -> None:
    await conn.execute(
        """
        INSERT INTO channel_profiles (
            id, name, positioning, language, default_aspect_ratio, risk_policy_json,
            content_mix_policy_json, cadence_policy_json, alert_policy_json, enabled,
            dry_run, halted_at, halt_reason, config_version, tick_interval_minutes,
            created_at, updated_at
        ) VALUES (
            $1, $2, '', 'en', '9:16', '{}'::json, '{}'::json, '{}'::json,
            '{}'::json, TRUE, FALSE, CASE WHEN $3 THEN NOW() ELSE NULL END,
            CASE WHEN $3 THEN 'migration fixture halt' ELSE NULL END, 1, 60, NOW(), NOW()
        )
        """,
        channel_id,
        f"migration-{channel_id}",
        halted,
    )


async def _insert_account(conn: asyncpg.Connection, account_id: uuid.UUID, channel_id: uuid.UUID) -> None:
    await conn.execute(
        """
        INSERT INTO publishing_accounts (
            id, channel_profile_id, platform, account_label, platform_account_id,
            credential_ref, platform_specific_config_json, default_privacy,
            external_asset_auto_publish, enabled, paused_until, last_token_check_at,
            last_token_check_status, created_at, updated_at
        ) VALUES (
            $1, $2, 'youtube', 'migration account', 'migration-account',
            'migration/credential', '{}'::json, 'unlisted', FALSE, TRUE,
            NULL, NULL, NULL, NOW(), NOW()
        )
        """,
        account_id,
        channel_id,
    )


async def _insert_legacy_task(
    conn: asyncpg.Connection,
    task_id: uuid.UUID,
    channel_id: uuid.UUID,
    account_id: uuid.UUID,
) -> None:
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
            $1, $2, $3, 'manual_seed', 'legacy title', 'legacy prompt', '{}'::json,
            '{}'::json, 'explore', '[]'::json, '[]'::json, FALSE, 1, 'planning', 0,
            1, '{}'::json, '[]'::json, 'human', '{}'::json, NOW(), NOW()
        )
        """,
        task_id,
        channel_id,
        account_id,
    )


async def _insert_queue(
    conn: asyncpg.Connection,
    *,
    item_id: uuid.UUID,
    key: str,
    kind: str,
    payload: str,
    channel_id: uuid.UUID | None,
) -> None:
    await conn.execute(
        """
        INSERT INTO channel_ops_queue_items (
            id, kind, idempotency_key, priority, payload_json, status,
            attempt_count, max_attempts, channel_profile_id
        ) VALUES ($1, $2, $3, 10, $4::json, 'queued', 0, 3, $5)
        """,
        item_id,
        kind,
        key,
        payload,
        channel_id,
    )


async def test_postgres_16_fresh_repair_and_mixed_writer_migrations() -> None:
    database = f"vp_final_review3_{uuid.uuid4().hex}"
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
            assert await conn.fetchval("SELECT version()")
            assert (
                await conn.fetchval("SELECT version_num FROM alembic_version")
                == "031_guarded_schedule_job_authority"
            )
            assert await conn.fetchval(
                "SELECT column_default FROM information_schema.columns "
                "WHERE table_name = 'production_tasks' AND column_name = 'human_review_evidence_json'"
            ) == "'{}'::json"
        finally:
            await conn.close()

        _run_alembic(target_url, "downgrade", "023_youtube_upload_operations")
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            active_channel = uuid.uuid4()
            halted_channel = uuid.uuid4()
            account_id = uuid.uuid4()
            task_id = uuid.uuid4()
            await _insert_channel(conn, active_channel, halted=False)
            await _insert_channel(conn, halted_channel, halted=True)
            await _insert_account(conn, account_id, active_channel)
            await _insert_legacy_task(conn, task_id, active_channel, account_id)

            queue_ids = {name: uuid.uuid4() for name in (
                "repair_null", "mismatch", "unresolved", "legacy_alert", "global_alert", "payload_alert"
            )}
            await _insert_queue(
                conn,
                item_id=queue_ids["repair_null"],
                key="migration:repair-null",
                kind="execute_task",
                payload=f'{{"production_task_id":"{task_id}"}}',
                channel_id=None,
            )
            await _insert_queue(
                conn,
                item_id=queue_ids["mismatch"],
                key="migration:mismatch",
                kind="execute_task",
                payload=f'{{"production_task_id":"{task_id}"}}',
                channel_id=halted_channel,
            )
            await _insert_queue(
                conn,
                item_id=queue_ids["unresolved"],
                key="migration:unresolved",
                kind="execute_task",
                payload=f'{{"production_task_id":"{uuid.uuid4()}"}}',
                channel_id=None,
            )
            await _insert_queue(
                conn,
                item_id=queue_ids["legacy_alert"],
                key="migration:legacy-alert",
                kind="send_alert",
                payload='{}',
                channel_id=active_channel,
            )
            await _insert_queue(
                conn,
                item_id=queue_ids["global_alert"],
                key="migration:global-alert",
                kind="send_alert",
                payload='{}',
                channel_id=None,
            )
            await _insert_queue(
                conn,
                item_id=queue_ids["payload_alert"],
                key="migration:payload-alert",
                kind="send_alert",
                payload=f'{{"channel_id":"{active_channel}"}}',
                channel_id=None,
            )
        finally:
            await conn.close()

        _run_alembic(target_url, "upgrade", "head")
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            repaired = await conn.fetchrow(
                "SELECT status, channel_profile_id FROM channel_ops_queue_items WHERE id = $1",
                queue_ids["repair_null"],
            )
            assert repaired["status"] == "queued"
            assert repaired["channel_profile_id"] == active_channel
            for name in ("mismatch", "unresolved"):
                row = await conn.fetchrow(
                    "SELECT status, locked_by, locked_at, dead_letter_at FROM channel_ops_queue_items WHERE id = $1",
                    queue_ids[name],
                )
                assert row["status"] == "dead_lettered"
                assert row["locked_by"] is None and row["locked_at"] is None and row["dead_letter_at"] is not None
            legacy_alert = await conn.fetchrow(
                "SELECT status, channel_profile_id FROM channel_ops_queue_items WHERE id = $1",
                queue_ids["legacy_alert"],
            )
            assert legacy_alert["status"] == "queued" and legacy_alert["channel_profile_id"] == active_channel
            global_alert = await conn.fetchrow(
                "SELECT status, channel_profile_id FROM channel_ops_queue_items WHERE id = $1",
                queue_ids["global_alert"],
            )
            assert global_alert["status"] == "queued" and global_alert["channel_profile_id"] is None
            payload_alert = await conn.fetchrow(
                "SELECT status, channel_profile_id FROM channel_ops_queue_items WHERE id = $1",
                queue_ids["payload_alert"],
            )
            assert payload_alert["status"] == "queued" and payload_alert["channel_profile_id"] == active_channel

            mixed_task_id = uuid.uuid4()
            await _insert_legacy_task(conn, mixed_task_id, active_channel, account_id)
            assert await conn.fetchval(
                "SELECT human_review_evidence_json::text FROM production_tasks WHERE id = $1",
                mixed_task_id,
            ) == "{}"
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
