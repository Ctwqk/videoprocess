from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import asyncpg
import pytest


POSTGRES_URL = os.getenv("CHANNEL_OPS_POSTGRES_TEST_URL", "")
BACKEND_ROOT = Path(__file__).resolve().parents[2]
PREVIOUS_REVISION = "033_legacy_worker_event_resolutions"
TARGET_REVISION = "034_worker_registrations"
RELEASE_COMMIT = "0123456789abcdef0123456789abcdef01234567"
IMAGE_IDENTITY = "vp-ffmpeg-worker-go:deploy-0123456789ab"
ENDPOINT_BINDINGS = {
    "database": {
        "database": "videoprocess",
        "driver": "postgresql",
        "host": "vp-postgres-swarm",
        "port": 5432,
    },
    "redis": {
        "database": 3,
        "host": "vp-redis-swarm",
        "port": 6379,
        "scheme": "redis",
    },
    "storage": {
        "backend": "not_applicable",
    },
}


def _run_alembic(
    database_url: str,
    *args: str,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=BACKEND_ROOT,
        env={**os.environ, "DATABASE_URL": database_url},
        text=True,
        capture_output=True,
        check=False,
    )


def _database_url(database: str) -> str:
    return f"{POSTGRES_URL.rsplit('/', 1)[0]}/{database}"


def _asyncpg_url(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def test_worker_registration_migration_emits_complete_additive_schema_and_functions() -> None:
    completed = _run_alembic(
        "postgresql+asyncpg://migration:unused@127.0.0.1:9/videoprocess",
        "upgrade",
        f"{PREVIOUS_REVISION}:{TARGET_REVISION}",
        "--sql",
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    sql = completed.stdout
    assert "CREATE TABLE worker_admission_grants" in sql
    assert "CREATE TABLE worker_registrations" in sql
    for column in (
        "generation",
        "release_commit",
        "image_identity",
        "database_principal",
        "redis_stream",
        "redis_group",
        "endpoint_bindings_json",
        "state",
        "issued_at",
        "issued_by",
        "activated_at",
        "worker_slot",
        "lease_secret_sha256",
        "superseded_by",
        "worker_registration_id",
        "worker_lease_epoch",
    ):
        assert column in sql
    for constraint in (
        "uq_worker_admission_grants_service_generation",
        "uq_worker_admission_grants_token_sha256",
        "uq_worker_admission_grants_active_service",
        "uq_worker_registrations_service_epoch",
        "uq_worker_registrations_active_service",
        "ck_worker_admission_grant_release_commit",
        "ck_worker_admission_grant_state",
        "ck_worker_admission_grant_lifecycle",
        "ck_worker_registration_slot_positive",
        "ck_worker_registration_lease_secret_sha256",
        "ck_worker_registration_supersession",
        "ck_node_execution_worker_lease_binding",
    ):
        assert constraint in sql
    for function_name in (
        "vp_worker_register",
        "vp_worker_heartbeat",
        "vp_worker_release",
        "vp_require_worker_lease",
    ):
        assert f"CREATE FUNCTION {function_name}" in sql
        assert f"ALTER FUNCTION {function_name}" not in sql
        assert f"REVOKE ALL ON FUNCTION {function_name}" in sql
    assert sql.count("SECURITY DEFINER") >= 4
    assert sql.count("SET search_path = pg_catalog, public") >= 4


@pytest.mark.asyncio
@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for live migration tests",
)
async def test_postgres_16_schema_functions_and_lease_fencing_are_restrictive() -> None:
    database = f"vp_worker_registration_migration_{uuid.uuid4().hex}"
    runtime_role = f"vp_worker_runtime_{uuid.uuid4().hex[:16]}"
    mismatch_role = f"vp_worker_mismatch_{uuid.uuid4().hex[:16]}"
    direct_role = f"vp_worker_direct_{uuid.uuid4().hex[:16]}"
    owner_role = f"vp_worker_owner_{uuid.uuid4().hex[:16]}"
    runtime_password = uuid.uuid4().hex
    mismatch_password = uuid.uuid4().hex
    direct_password = uuid.uuid4().hex
    owner_password = uuid.uuid4().hex
    admin_url = _database_url("postgres")
    admin = await asyncpg.connect(_asyncpg_url(admin_url))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
        await admin.execute(
            f'CREATE ROLE "{runtime_role}" LOGIN PASSWORD '
            f"'{runtime_password}'"
        )
        await admin.execute(
            f'CREATE ROLE "{mismatch_role}" LOGIN PASSWORD '
            f"'{mismatch_password}'"
        )
        await admin.execute(
            f'CREATE ROLE "{direct_role}" LOGIN PASSWORD '
            f"'{direct_password}'"
        )
        await admin.execute(
            f'CREATE ROLE "{owner_role}" LOGIN PASSWORD '
            f"'{owner_password}'"
        )
    finally:
        await admin.close()

    target_url = _database_url(database)
    try:
        completed = _run_alembic(target_url, "upgrade", "head")
        assert completed.returncode == 0, completed.stdout + completed.stderr
        connection = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert await connection.fetchval("SHOW server_version_num") >= "160000"
            assert (
                await connection.fetchval(
                    "SELECT version_num FROM alembic_version"
                )
                == TARGET_REVISION
            )
            grant_columns = {
                row["column_name"]
                for row in await connection.fetch(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'worker_admission_grants'
                    """
                )
            }
            assert grant_columns == {
                "id",
                "service_name",
                "generation",
                "worker_type",
                "worker_host",
                "capabilities_json",
                "release_commit",
                "image_identity",
                "database_principal",
                "redis_stream",
                "redis_group",
                "endpoint_bindings_json",
                "token_sha256",
                "state",
                "issued_at",
                "issued_by",
                "activated_at",
                "revoked_at",
                "revoke_reason",
                "created_at",
                "updated_at",
            }
            registration_columns = {
                row["column_name"]
                for row in await connection.fetch(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'worker_registrations'
                    """
                )
            }
            assert registration_columns == {
                "id",
                "grant_id",
                "service_name",
                "worker_type",
                "worker_host",
                "capabilities_json",
                "worker_instance_id",
                "worker_slot",
                "redis_consumer_id",
                "image_identity",
                "database_principal",
                "database_fingerprint",
                "redis_fingerprint",
                "storage_fingerprint",
                "lease_epoch",
                "lease_secret_sha256",
                "status",
                "registered_at",
                "heartbeat_at",
                "lease_expires_at",
                "revoked_at",
                "revoke_reason",
                "superseded_by",
            }
            node_binding = {
                row["column_name"]: row["is_nullable"]
                for row in await connection.fetch(
                    """
                    SELECT column_name, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'node_executions'
                      AND column_name IN (
                          'worker_registration_id',
                          'worker_lease_epoch'
                      )
                    """
                )
            }
            assert node_binding == {
                "worker_registration_id": "YES",
                "worker_lease_epoch": "YES",
            }

            functions = await connection.fetch(
                """
                SELECT proname, prosecdef, proconfig,
                       has_function_privilege('public', oid, 'EXECUTE')
                           AS public_execute
                FROM pg_proc
                WHERE pronamespace = 'public'::regnamespace
                  AND proname = ANY($1::text[])
                """,
                [
                    "vp_worker_register",
                    "vp_worker_heartbeat",
                    "vp_worker_release",
                    "vp_require_worker_lease",
                ],
            )
            assert {row["proname"] for row in functions} == {
                "vp_worker_register",
                "vp_worker_heartbeat",
                "vp_worker_release",
                "vp_require_worker_lease",
            }
            assert all(row["prosecdef"] for row in functions)
            assert all(
                "search_path=pg_catalog, public" in (row["proconfig"] or [])
                for row in functions
            )
            assert not any(row["public_execute"] for row in functions)
            function_signatures = (
                "vp_worker_register(text,bigint,text,text,uuid,integer,text,"
                "jsonb,text,text,text,text,jsonb,text,text,text,text,text)",
                "vp_worker_heartbeat(uuid,text,uuid,bigint,text)",
                "vp_worker_release(uuid,text,uuid,bigint,text,text)",
                "vp_require_worker_lease(uuid,bigint)",
            )
            for role_name in (
                runtime_role,
                mismatch_role,
                direct_role,
                owner_role,
            ):
                for signature in function_signatures:
                    await connection.execute(
                        f'GRANT EXECUTE ON FUNCTION {signature} '
                        f'TO "{role_name}"'
                    )
            await connection.execute(
                f'ALTER TABLE worker_admission_grants OWNER TO "{owner_role}"'
            )
            await connection.execute(
                f'ALTER TABLE worker_registrations OWNER TO "{owner_role}"'
            )

            grant_id = uuid.uuid4()
            admission_token_hash = _sha256("admission-token")
            await connection.execute(
                """
                INSERT INTO worker_admission_grants (
                    id, service_name, generation, worker_type, worker_host,
                    capabilities_json, release_commit, image_identity,
                    database_principal, redis_stream, redis_group,
                    endpoint_bindings_json,
                    token_sha256, state, issued_at, issued_by, activated_at
                ) VALUES (
                    $1, 'service-a', 7, 'ffmpeg_go', 'colima-127',
                    '["media_cpu"]'::jsonb, $2, $3, $4,
                    'vp:tasks:ffmpeg_go', 'ffmpeg_go-workers', $5::jsonb,
                    $6, 'active', NOW(), 'migration-test', NOW()
                )
                """,
                grant_id,
                RELEASE_COMMIT,
                IMAGE_IDENTITY,
                runtime_role,
                json.dumps(ENDPOINT_BINDINGS),
                admission_token_hash,
            )
            with pytest.raises(asyncpg.UniqueViolationError):
                await connection.execute(
                    """
                    INSERT INTO worker_admission_grants (
                        service_name, generation, worker_type, worker_host,
                        capabilities_json, release_commit, image_identity,
                        database_principal, redis_stream, redis_group,
                        endpoint_bindings_json,
                        token_sha256, state, issued_at, issued_by, activated_at
                    ) VALUES (
                        'service-a', 8, 'ffmpeg_go', 'colima-127',
                        '["media_cpu"]'::jsonb, $1, $2, $3,
                        'vp:tasks:ffmpeg_go', 'ffmpeg_go-workers', $4::jsonb,
                        $5, 'active', NOW(), 'migration-test', NOW()
                    )
                    """,
                    RELEASE_COMMIT,
                    IMAGE_IDENTITY,
                    runtime_role,
                    json.dumps(ENDPOINT_BINDINGS),
                    _sha256("second-token"),
                )

            first_instance = uuid.uuid4()
            first_lease_hash = _sha256("lease-one")
            runtime_url = (
                f"postgresql://{runtime_role}:{runtime_password}"
                f"@{target_url.split('@', 1)[1]}"
            )
            mismatch_url = (
                f"postgresql://{mismatch_role}:{mismatch_password}"
                f"@{target_url.split('@', 1)[1]}"
            )
            direct_url = (
                f"postgresql://{direct_role}:{direct_password}"
                f"@{target_url.split('@', 1)[1]}"
            )
            owner_url = (
                f"postgresql://{owner_role}:{owner_password}"
                f"@{target_url.split('@', 1)[1]}"
            )
            runtime_connection = await asyncpg.connect(runtime_url)
            mismatch_connection = await asyncpg.connect(mismatch_url)
            direct_connection = await asyncpg.connect(direct_url)
            owner_connection = await asyncpg.connect(owner_url)
            register_sql = """
                SELECT * FROM vp_worker_register(
                    $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10,
                    $11, $12, $13::jsonb, $14, $15, $16, $17, $18
                )
            """
            register_args = (
                "service-a",
                7,
                "ffmpeg_go",
                "colima-127",
                first_instance,
                1,
                "consumer-one",
                '["media_cpu"]',
                RELEASE_COMMIT,
                IMAGE_IDENTITY,
                "vp:tasks:ffmpeg_go",
                "ffmpeg_go-workers",
                json.dumps(ENDPOINT_BINDINGS),
                "a" * 64,
                "b" * 64,
                "c" * 64,
                admission_token_hash,
                first_lease_hash,
            )
            try:
                with pytest.raises(
                    asyncpg.RaiseError,
                    match="database_principal_privileged",
                ):
                    await connection.fetchrow(register_sql, *register_args)
                with pytest.raises(
                    asyncpg.RaiseError,
                    match="database_principal_mismatch",
                ):
                    await mismatch_connection.fetchrow(
                        register_sql,
                        *register_args,
                    )
                with pytest.raises(
                    asyncpg.RaiseError,
                    match="database_principal_privileged",
                ):
                    await owner_connection.fetchrow(
                        register_sql,
                        *register_args,
                    )
                for table_name in (
                    "worker_admission_grants",
                    "worker_registrations",
                ):
                    for privilege in (
                        "SELECT",
                        "INSERT",
                        "UPDATE",
                        "DELETE",
                        "TRUNCATE",
                    ):
                        await connection.execute(
                            f"GRANT {privilege} ON {table_name} "
                            f'TO "{direct_role}"'
                        )
                        with pytest.raises(
                            asyncpg.RaiseError,
                            match="database_principal_privileged",
                        ):
                            await direct_connection.fetchrow(
                                register_sql,
                                *register_args,
                            )
                        await connection.execute(
                            f"REVOKE {privilege} ON {table_name} "
                            f'FROM "{direct_role}"'
                        )

                first = await runtime_connection.fetchrow(
                    register_sql,
                    *register_args,
                )
            finally:
                await mismatch_connection.close()
                await direct_connection.close()
                await owner_connection.close()
            assert first["grant_id"] == grant_id
            assert first["lease_epoch"] == 1
            assert 179 <= (
                first["lease_expires_at"]
                - await connection.fetchval("SELECT clock_timestamp()")
            ).total_seconds() <= 180

            second_instance = uuid.uuid4()
            second_lease_hash = _sha256("lease-two")
            second = await runtime_connection.fetchrow(
                """
                SELECT * FROM vp_worker_register(
                    $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10,
                    $11, $12, $13::jsonb, $14, $15, $16, $17, $18
                )
                """,
                "service-a",
                7,
                "ffmpeg_go",
                "colima-127",
                second_instance,
                2,
                "consumer-two",
                '["media_cpu"]',
                RELEASE_COMMIT,
                IMAGE_IDENTITY,
                "vp:tasks:ffmpeg_go",
                "ffmpeg_go-workers",
                json.dumps(ENDPOINT_BINDINGS),
                "a" * 64,
                "b" * 64,
                "c" * 64,
                admission_token_hash,
                second_lease_hash,
            )
            assert second["lease_epoch"] == 2
            old = await connection.fetchrow(
                """
                SELECT status, superseded_by
                FROM worker_registrations
                WHERE id = $1
                """,
                first["registration_id"],
            )
            assert dict(old) == {
                "status": "revoked",
                "superseded_by": second["registration_id"],
            }
            assert await connection.fetchval(
                """
                SELECT database_principal
                FROM worker_registrations
                WHERE id = $1
                """,
                second["registration_id"],
            ) == runtime_role
            assert (
                await connection.fetchval(
                    """
                    SELECT count(*)
                    FROM worker_registrations
                    WHERE service_name = 'service-a' AND status = 'active'
                    """
                )
                == 1
            )
            with pytest.raises(
                asyncpg.RaiseError,
                match="lease_fenced",
            ):
                await runtime_connection.fetchval(
                    """
                    SELECT vp_worker_heartbeat($1, $2, $3, $4, $5)
                    """,
                    first["registration_id"],
                    "service-a",
                    first_instance,
                    1,
                    first_lease_hash,
                )

            renewed = await runtime_connection.fetchval(
                "SELECT vp_worker_heartbeat($1, $2, $3, $4, $5)",
                second["registration_id"],
                "service-a",
                second_instance,
                2,
                second_lease_hash,
            )
            assert 179 <= (
                renewed
                - await connection.fetchval("SELECT clock_timestamp()")
            ).total_seconds() <= 180
            await runtime_connection.execute(
                "SELECT vp_require_worker_lease($1, $2)",
                second["registration_id"],
                2,
            )
            assert await runtime_connection.fetchval(
                "SELECT vp_worker_release($1, $2, $3, $4, $5, $6)",
                second["registration_id"],
                "service-a",
                second_instance,
                2,
                second_lease_hash,
                "shutdown",
            )
            assert await runtime_connection.fetchval(
                "SELECT vp_worker_release($1, $2, $3, $4, $5, $6)",
                second["registration_id"],
                "service-a",
                second_instance,
                2,
                second_lease_hash,
                "retry",
            )
            with pytest.raises(asyncpg.RaiseError, match="lease_fenced"):
                await runtime_connection.execute(
                    "SELECT vp_require_worker_lease($1, $2)",
                    second["registration_id"],
                    2,
                )
            await runtime_connection.close()
        finally:
            await connection.close()

        completed = _run_alembic(target_url, "downgrade", PREVIOUS_REVISION)
        assert completed.returncode == 0, completed.stdout + completed.stderr
        connection = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert not await connection.fetchval(
                "SELECT to_regclass('public.worker_registrations') IS NOT NULL"
            )
            assert not await connection.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1 FROM pg_proc
                    WHERE pronamespace = 'public'::regnamespace
                      AND proname = 'vp_worker_register'
                )
                """
            )
            assert not await connection.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_name = 'node_executions'
                      AND column_name = 'worker_registration_id'
                )
                """
            )
        finally:
            await connection.close()
    finally:
        admin = await asyncpg.connect(_asyncpg_url(admin_url))
        try:
            await admin.execute(
                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'
            )
            await admin.execute(f'DROP ROLE IF EXISTS "{runtime_role}"')
            await admin.execute(f'DROP ROLE IF EXISTS "{mismatch_role}"')
            await admin.execute(f'DROP ROLE IF EXISTS "{direct_role}"')
            await admin.execute(f'DROP ROLE IF EXISTS "{owner_role}"')
        finally:
            await admin.close()
