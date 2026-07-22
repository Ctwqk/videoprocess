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


async def test_postgres_16_revision_trigger_rejects_025_writers_and_downgrades_cleanly() -> None:
    database = f"vp_final_review4_{uuid.uuid4().hex}"
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
            assert await conn.fetchval("SHOW server_version_num") >= "160000"
            assert (
                await conn.fetchval("SELECT version_num FROM alembic_version")
                == "029_channelops_discovery_ingestion_runs"
            )
            columns = {
                row["column_name"]: row["is_nullable"]
                for row in await conn.fetch(
                    "SELECT column_name, is_nullable FROM information_schema.columns "
                    "WHERE table_name IN ('autoflow_plans', 'autoflow_runs') "
                    "AND column_name IN ('execution_revision', 'approved_revision', 'request_fingerprint')"
                )
            }
            assert columns == {
                "execution_revision": "NO",
                "approved_revision": "YES",
                "request_fingerprint": "YES",
            }

            plan_id = uuid.uuid4()
            approved_hash = "a" * 64
            await conn.execute(
                """
                INSERT INTO autoflow_plans (
                    id, prompt, request_json, intent_json, template_id, pipeline_definition,
                    storyboard_json, candidates_json, metadata_json, rights_json,
                    validation_json, status, review_approved_at, approved_revision_hash,
                    approved_revision, created_at, updated_at
                ) VALUES (
                    $1, 'reviewed prompt', '{"prompt":"reviewed prompt"}'::json,
                    '{"subject":"reviewed"}'::json, 'material_library_remix',
                    '{"nodes":[],"edges":[]}'::json, NULL, '[]'::json, '{}'::json,
                    '{"status":"review_required","review_approved":true}'::json,
                    '{"valid":true}'::json, 'review_approved', NOW(), $2, 1, NOW(), NOW()
                )
                """,
                plan_id,
                approved_hash,
            )
            initial = await conn.fetchrow(
                "SELECT execution_revision, approved_revision, approved_revision_hash, review_approved_at "
                "FROM autoflow_plans WHERE id = $1",
                plan_id,
            )
            assert dict(initial) == {
                "execution_revision": 1,
                "approved_revision": 1,
                "approved_revision_hash": approved_hash,
                "review_approved_at": initial["review_approved_at"],
            }
            assert initial["review_approved_at"] is not None

            await conn.execute(
                """
                UPDATE autoflow_plans
                SET prompt = '025 canonical writer changed prompt',
                    request_json = '{"prompt":"025 canonical writer changed prompt"}'::json
                WHERE id = $1
                """,
                plan_id,
            )
            canonical_change = await conn.fetchrow(
                """
                SELECT execution_revision, approved_revision, approved_revision_hash,
                       review_approved_at, public_approved_at, agent_approved_by,
                       rights_json::jsonb AS rights
                FROM autoflow_plans WHERE id = $1
                """,
                plan_id,
            )
            assert canonical_change["execution_revision"] == 2
            assert canonical_change["approved_revision"] is None
            assert canonical_change["approved_revision_hash"] is None
            assert canonical_change["review_approved_at"] is None
            assert canonical_change["public_approved_at"] is None
            assert canonical_change["agent_approved_by"] is None
            assert "review_approved" not in canonical_change["rights"]

            await conn.execute(
                """
                UPDATE autoflow_plans
                SET status = 'review_approved',
                    review_approved_at = NOW(),
                    approved_revision_hash = $2,
                    rights_json = (rights_json::jsonb || '{"review_approved":true}'::jsonb)::json
                WHERE id = $1
                """,
                plan_id,
                "b" * 64,
            )
            old_approval = await conn.fetchrow(
                """
                SELECT execution_revision, approved_revision, approved_revision_hash,
                       review_approved_at, rights_json::jsonb AS rights
                FROM autoflow_plans WHERE id = $1
                """,
                plan_id,
            )
            assert old_approval["execution_revision"] == 2
            assert old_approval["approved_revision"] is None
            assert old_approval["approved_revision_hash"] is None
            assert old_approval["review_approved_at"] is None
            assert "review_approved" not in old_approval["rights"]

            await conn.execute(
                """
                UPDATE autoflow_plans
                SET review_approved_at = NOW(),
                    approved_revision_hash = $2,
                    approved_revision = execution_revision,
                    rights_json = (rights_json::jsonb || '{"review_approved":true}'::jsonb)::json
                WHERE id = $1
                """,
                plan_id,
                "c" * 64,
            )
            current_approval = await conn.fetchrow(
                "SELECT execution_revision, approved_revision, approved_revision_hash, review_approved_at "
                "FROM autoflow_plans WHERE id = $1",
                plan_id,
            )
            assert current_approval["execution_revision"] == 2
            assert current_approval["approved_revision"] == 2
            assert current_approval["approved_revision_hash"] == "c" * 64
            assert current_approval["review_approved_at"] is not None
        finally:
            await conn.close()

        _run_alembic(target_url, "downgrade", "025_autoflow_revision_idempotency")
        conn = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert await conn.fetchval("SELECT version_num FROM alembic_version") == "025_autoflow_revision_idempotency"
            remaining = await conn.fetchval(
                "SELECT count(*) FROM information_schema.columns "
                "WHERE table_name IN ('autoflow_plans', 'autoflow_runs') "
                "AND column_name IN ('execution_revision', 'approved_revision', 'request_fingerprint')"
            )
            assert remaining == 0
            assert not await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_autoflow_plan_authority_fence')"
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
