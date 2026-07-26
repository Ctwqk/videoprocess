from __future__ import annotations

import asyncio
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
FINGERPRINT_FIXTURE = (
    BACKEND_ROOT.parent
    / "tests/fixtures/worker_registration/fingerprints-v1.json"
)
PREVIOUS_REVISION = "033_legacy_worker_event_resolutions"
TARGET_REVISION = "034_worker_registrations"
RELEASE_COMMIT = "0123456789abcdef0123456789abcdef01234567"
IMAGE_IDENTITY = "vp-ffmpeg-worker-go:deploy-0123456789ab"
FINGERPRINT_CASES = json.loads(FINGERPRINT_FIXTURE.read_text())["cases"]
ENDPOINT_VALIDATION_CASES = json.loads(FINGERPRINT_FIXTURE.read_text())[
    "endpoint_validation_cases"
]
NOT_APPLICABLE_FINGERPRINT_CASE = next(
    case for case in FINGERPRINT_CASES
    if case["name"] == "not_applicable"
)
ENDPOINT_BINDINGS = {
    name: json.loads(canonical)
    for name, canonical in NOT_APPLICABLE_FINGERPRINT_CASE[
        "canonical"
    ].items()
}
ENDPOINT_FINGERPRINTS = NOT_APPLICABLE_FINGERPRINT_CASE["sha256"]


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


def _endpoint_validation_payload(case: dict[str, object]) -> str:
    if "bindings_json" in case:
        return case["bindings_json"]
    if "bindings" in case:
        bindings = case["bindings"]
    else:
        bindings = json.loads(json.dumps(ENDPOINT_BINDINGS))
        operation = case.get("replace") or case.get("extra")
        dependency, field_name, value = operation
        bindings[dependency][field_name] = value
    return json.dumps(bindings)


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
    assert "CREATE TABLE registered_worker_event_receipts" in sql
    assert "CREATE TABLE worker_event_dispatches" in sql
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
        "vp_observe_worker_lease",
        "vp_require_worker_lease_margin",
        "vp_require_worker_task_ack_receipt",
        "vp_worker_grant_upsert",
        "vp_worker_grant_activate",
        "vp_worker_grant_revoke",
        "vp_worker_registration_revoke",
        "vp_worker_registration_expire",
        "vp_worker_endpoint_fingerprints",
    ):
        assert f"CREATE FUNCTION public.{function_name}" in sql
        assert f"ALTER FUNCTION public.{function_name}" not in sql
        assert f"REVOKE ALL ON FUNCTION public.{function_name}" in sql
    assert sql.count("SECURITY DEFINER") >= 13
    assert sql.count("SET search_path = pg_catalog") >= 13
    assert "SET search_path = pg_catalog, public" not in sql
    assert "pg_advisory_xact_lock_shared" in sql
    assert "pg_advisory_xact_lock" in sql
    register_function_sql = sql[
        sql.index("CREATE FUNCTION public.vp_worker_register") :
        sql.index("CREATE FUNCTION public.vp_worker_heartbeat")
    ]
    assert register_function_sql.count("p_worker_slot integer") == 1
    assert register_function_sql.count(
        "public.vp_worker_endpoint_fingerprints"
    ) == 1
    for legacy_endpoint_parser_name in (
        "v_database_binding",
        "v_redis_binding",
        "v_storage_binding",
        "v_database_canonical",
        "v_redis_canonical",
        "v_storage_canonical",
    ):
        assert legacy_endpoint_parser_name not in register_function_sql


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
    set_role_login = f"vp_worker_member_{uuid.uuid4().hex[:16]}"
    bridge_role = f"vp_worker_bridge_{uuid.uuid4().hex[:16]}"
    writer_role = f"vp_worker_writer_{uuid.uuid4().hex[:16]}"
    owner_role = f"vp_worker_owner_{uuid.uuid4().hex[:16]}"
    operator_role = f"vp_worker_operator_{uuid.uuid4().hex[:16]}"
    runtime_password = uuid.uuid4().hex
    mismatch_password = uuid.uuid4().hex
    direct_password = uuid.uuid4().hex
    set_role_password = uuid.uuid4().hex
    owner_password = uuid.uuid4().hex
    operator_password = uuid.uuid4().hex
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
            f'CREATE ROLE "{direct_role}" LOGIN NOINHERIT PASSWORD '
            f"'{direct_password}'"
        )
        await admin.execute(
            f'CREATE ROLE "{set_role_login}" LOGIN NOINHERIT PASSWORD '
            f"'{set_role_password}'"
        )
        await admin.execute(f'CREATE ROLE "{bridge_role}" NOLOGIN')
        await admin.execute(f'CREATE ROLE "{writer_role}" NOLOGIN')
        await admin.execute(
            f'CREATE ROLE "{owner_role}" LOGIN PASSWORD '
            f"'{owner_password}'"
        )
        await admin.execute(
            f'CREATE ROLE "{operator_role}" LOGIN PASSWORD '
            f"'{operator_password}'"
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
            null_safe_checks = await connection.fetch(
                """
                SELECT conname, pg_get_constraintdef(oid) AS definition
                FROM pg_constraint
                WHERE contype = 'c'
                  AND (
                      conname LIKE 'ck_worker_%'
                      OR conname = 'ck_node_execution_worker_lease_binding'
                  )
                """
            )
            assert null_safe_checks
            assert all(
                "IS TRUE" in row["definition"]
                for row in null_safe_checks
            )

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
                    "vp_worker_grant_upsert",
                    "vp_worker_grant_activate",
                    "vp_worker_grant_revoke",
                    "vp_worker_registration_revoke",
                    "vp_worker_registration_expire",
                    "vp_worker_endpoint_fingerprints",
                ],
            )
            assert {row["proname"] for row in functions} == {
                "vp_worker_register",
                "vp_worker_heartbeat",
                "vp_worker_release",
                "vp_require_worker_lease",
                "vp_worker_grant_upsert",
                "vp_worker_grant_activate",
                "vp_worker_grant_revoke",
                "vp_worker_registration_revoke",
                "vp_worker_registration_expire",
                "vp_worker_endpoint_fingerprints",
            }
            assert all(row["prosecdef"] for row in functions)
            assert all(
                "search_path=pg_catalog" in (row["proconfig"] or [])
                for row in functions
            )
            assert not any(row["public_execute"] for row in functions)
            for case in ENDPOINT_VALIDATION_CASES:
                bindings_json = _endpoint_validation_payload(case)
                if case["accepted"]:
                    fingerprints = await connection.fetchrow(
                        """
                        SELECT *
                        FROM public.vp_worker_endpoint_fingerprints($1::jsonb)
                        """,
                        bindings_json,
                    )
                    assert {
                        name: fingerprints[f"{name}_fingerprint"]
                        for name in ("database", "redis", "storage")
                    } == case["sha256"]
                else:
                    error_type = (
                        asyncpg.InvalidTextRepresentationError
                        if case.get("invalid_json")
                        else asyncpg.RaiseError
                    )
                    error_match = (
                        "invalid input syntax for type json"
                        if case.get("invalid_json")
                        else "claim_mismatch"
                    )
                    with pytest.raises(error_type, match=error_match):
                        await connection.fetchrow(
                            """
                            SELECT *
                            FROM public.vp_worker_endpoint_fingerprints(
                                $1::jsonb
                            )
                            """,
                            bindings_json,
                        )
            function_signatures = (
                "vp_worker_register(text,bigint,text,text,uuid,integer,text,"
                "jsonb,text,text,text,text,jsonb,text,text,text,text,text)",
                "vp_worker_heartbeat(uuid,text,uuid,bigint,text)",
                "vp_worker_release(uuid,text,uuid,bigint,text,text)",
                "vp_require_worker_lease(uuid,bigint)",
            )
            operator_function_signatures = (
                "vp_worker_grant_upsert(text,bigint,text,text,jsonb,text,text,"
                "text,text,text,jsonb,text,text)",
                "vp_worker_grant_activate(text,bigint)",
                "vp_worker_grant_revoke(text,bigint,text)",
                "vp_worker_registration_revoke(text,uuid,text)",
                "vp_worker_registration_expire(text,uuid)",
            )
            for role_name in (
                runtime_role,
                mismatch_role,
                direct_role,
                set_role_login,
                owner_role,
            ):
                for signature in function_signatures:
                    await connection.execute(
                        f'GRANT EXECUTE ON FUNCTION public.{signature} '
                        f'TO "{role_name}"'
                    )
            for signature in operator_function_signatures:
                await connection.execute(
                    f'GRANT EXECUTE ON FUNCTION public.{signature} '
                    f'TO "{operator_role}"'
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
            with pytest.raises(asyncpg.CheckViolationError):
                await connection.execute(
                    """
                    INSERT INTO worker_admission_grants (
                        service_name, generation, worker_type, worker_host,
                        capabilities_json, release_commit, image_identity,
                        database_principal, redis_stream, redis_group,
                        endpoint_bindings_json, token_sha256, state,
                        issued_at, issued_by, revoked_at, revoke_reason
                    ) VALUES (
                        'invalid-revoked-service', 1, 'ffmpeg_go',
                        'colima-127', '["media_cpu"]'::jsonb, $1, $2, $3,
                        'vp:tasks:ffmpeg_go', 'ffmpeg_go-workers', $4::jsonb,
                        $5, 'revoked', NOW(), 'migration-test', NOW(), NULL
                    )
                    """,
                    RELEASE_COMMIT,
                    IMAGE_IDENTITY,
                    runtime_role,
                    json.dumps(ENDPOINT_BINDINGS),
                    _sha256("invalid-revoked-token"),
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
            set_role_url = (
                f"postgresql://{set_role_login}:{set_role_password}"
                f"@{target_url.split('@', 1)[1]}"
            )
            owner_url = (
                f"postgresql://{owner_role}:{owner_password}"
                f"@{target_url.split('@', 1)[1]}"
            )
            operator_url = (
                f"postgresql://{operator_role}:{operator_password}"
                f"@{target_url.split('@', 1)[1]}"
            )
            runtime_connection = await asyncpg.connect(runtime_url)
            mismatch_connection = await asyncpg.connect(mismatch_url)
            direct_connection = await asyncpg.connect(direct_url)
            set_role_connection = await asyncpg.connect(set_role_url)
            owner_connection = await asyncpg.connect(owner_url)
            operator_connection = await asyncpg.connect(operator_url)
            register_sql = """
                SELECT * FROM public.vp_worker_register(
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
                ENDPOINT_FINGERPRINTS["database"],
                ENDPOINT_FINGERPRINTS["redis"],
                ENDPOINT_FINGERPRINTS["storage"],
                admission_token_hash,
                first_lease_hash,
            )

            async def assert_register_rejected(
                args: tuple[object, ...],
                code: str,
            ) -> None:
                transaction = runtime_connection.transaction()
                await transaction.start()
                try:
                    with pytest.raises(asyncpg.RaiseError, match=code):
                        await runtime_connection.fetchrow(
                            register_sql,
                            *args,
                        )
                finally:
                    await transaction.rollback()

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
                for mutation in (
                    "SELECT * FROM public.worker_admission_grants",
                    "INSERT INTO public.worker_admission_grants DEFAULT VALUES",
                    "UPDATE public.worker_admission_grants SET state = 'revoked'",
                    "DELETE FROM public.worker_admission_grants",
                    "TRUNCATE public.worker_admission_grants",
                    "SELECT * FROM public.worker_registrations",
                    "INSERT INTO public.worker_registrations DEFAULT VALUES",
                    "UPDATE public.worker_registrations SET status = 'revoked'",
                    "DELETE FROM public.worker_registrations",
                    "TRUNCATE public.worker_registrations",
                ):
                    with pytest.raises(asyncpg.InsufficientPrivilegeError):
                        await operator_connection.execute(mutation)
                null_safe_operator_calls = (
                    (
                        """
                        SELECT public.vp_worker_grant_upsert(
                            $1, $2, $3, $4, $5::jsonb, $6, $7, $8,
                            $9, $10, $11::jsonb, $12, $13
                        )
                        """,
                        (
                            "null-service",
                            1,
                            "ffmpeg_go",
                            "colima-127",
                            '["media_cpu"]',
                            RELEASE_COMMIT,
                            IMAGE_IDENTITY,
                            runtime_role,
                            "vp:tasks:ffmpeg_go",
                            "ffmpeg_go-workers",
                            json.dumps(ENDPOINT_BINDINGS),
                            _sha256("null-test-token"),
                            "migration-test",
                        ),
                    ),
                    (
                        "SELECT public.vp_worker_grant_activate($1, $2)",
                        ("null-service", 1),
                    ),
                    (
                        "SELECT public.vp_worker_grant_revoke($1, $2, $3)",
                        ("null-service", 1, "operator-stop"),
                    ),
                    (
                        """
                        SELECT public.vp_worker_registration_revoke(
                            $1, $2, $3
                        )
                        """,
                        ("null-service", uuid.uuid4(), "operator-stop"),
                    ),
                    (
                        """
                        SELECT public.vp_worker_registration_expire($1, $2)
                        """,
                        ("null-service", uuid.uuid4()),
                    ),
                )
                for operator_sql, operator_args in null_safe_operator_calls:
                    for index in range(len(operator_args)):
                        null_args = list(operator_args)
                        null_args[index] = None
                        with pytest.raises(
                            asyncpg.RaiseError,
                            match="claim_mismatch",
                        ):
                            await operator_connection.fetchval(
                                operator_sql,
                                *null_args,
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

                for table_name, column_name, privilege in (
                    (
                        "worker_admission_grants",
                        "token_sha256",
                        "SELECT",
                    ),
                    (
                        "worker_admission_grants",
                        "service_name",
                        "INSERT",
                    ),
                    (
                        "worker_admission_grants",
                        "state",
                        "UPDATE",
                    ),
                    (
                        "worker_registrations",
                        "lease_secret_sha256",
                        "SELECT",
                    ),
                    (
                        "worker_registrations",
                        "service_name",
                        "INSERT",
                    ),
                    (
                        "worker_registrations",
                        "status",
                        "UPDATE",
                    ),
                ):
                    await connection.execute(
                        f"GRANT {privilege} ({column_name}) "
                        f"ON {table_name} TO \"{direct_role}\""
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
                        f"REVOKE {privilege} ({column_name}) "
                        f"ON {table_name} FROM \"{direct_role}\""
                    )

                await connection.execute(
                    f'GRANT "{owner_role}" TO "{direct_role}"'
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
                    f'REVOKE "{owner_role}" FROM "{direct_role}"'
                )

                await connection.execute(
                    "GRANT UPDATE (status) ON worker_registrations "
                    f'TO "{writer_role}"'
                )
                await connection.execute(
                    f'GRANT "{writer_role}" TO "{bridge_role}"'
                )
                await connection.execute(
                    f'GRANT "{bridge_role}" TO "{set_role_login}"'
                )
                with pytest.raises(
                    asyncpg.RaiseError,
                    match="database_principal_privileged",
                ):
                    await set_role_connection.fetchrow(
                        register_sql,
                        *register_args,
                    )

                for index in range(len(register_args)):
                    null_args = list(register_args)
                    null_args[index] = None
                    await assert_register_rejected(
                        tuple(null_args),
                        "token_invalid" if index == 16 else "claim_mismatch",
                    )

                for fingerprint_index in (13, 14, 15):
                    mismatched_args = list(register_args)
                    mismatched_args[fingerprint_index] = "0" * 64
                    await assert_register_rejected(
                        tuple(mismatched_args),
                        "claim_mismatch",
                    )

                secret_endpoint_args = list(register_args)
                secret_endpoint_args[12] = json.dumps(
                    {
                        **ENDPOINT_BINDINGS,
                        "database": {
                            **ENDPOINT_BINDINGS["database"],
                            "password": "must-not-be-admitted",
                        },
                    }
                )
                await assert_register_rejected(
                    tuple(secret_endpoint_args),
                    "claim_mismatch",
                )
                for malformed_endpoints in (
                    [],
                    {
                        **ENDPOINT_BINDINGS,
                        "database": "not-an-object",
                    },
                    {
                        **ENDPOINT_BINDINGS,
                        "database": {
                            **ENDPOINT_BINDINGS["database"],
                            "port": "5432",
                        },
                    },
                    {
                        **ENDPOINT_BINDINGS,
                        "redis": {
                            **ENDPOINT_BINDINGS["redis"],
                            "database": 1.5,
                        },
                    },
                    {
                        **ENDPOINT_BINDINGS,
                        "storage": {
                            "backend": "minio",
                            "bucket": "videoprocess",
                            "host": "vp-minio-swarm",
                            "port": "9000",
                        },
                    },
                ):
                    malformed_endpoint_args = list(register_args)
                    malformed_endpoint_args[12] = json.dumps(
                        malformed_endpoints
                    )
                    await assert_register_rejected(
                        tuple(malformed_endpoint_args),
                        "claim_mismatch",
                    )

                first = await runtime_connection.fetchrow(
                    register_sql,
                    *register_args,
                )
            finally:
                await mismatch_connection.close()
                await direct_connection.close()
                await set_role_connection.close()
                await owner_connection.close()
                await operator_connection.close()
            assert first["grant_id"] == grant_id
            assert first["lease_epoch"] == 1
            assert 179 <= (
                first["lease_expires_at"]
                - await connection.fetchval("SELECT clock_timestamp()")
            ).total_seconds() <= 180
            with pytest.raises(asyncpg.CheckViolationError):
                await connection.execute(
                    """
                    INSERT INTO worker_registrations (
                        grant_id, service_name, worker_type, worker_host,
                        capabilities_json, worker_instance_id, worker_slot,
                        redis_consumer_id, image_identity, database_principal,
                        database_fingerprint, redis_fingerprint,
                        storage_fingerprint, lease_epoch, lease_secret_sha256,
                        status, registered_at, heartbeat_at, lease_expires_at,
                        revoked_at, revoke_reason
                    ) VALUES (
                        $1, 'invalid-revoked-registration', 'ffmpeg_go',
                        'colima-127', '["media_cpu"]'::jsonb, $2, 9,
                        'invalid-consumer', $3, $4, $5, $6, $7, 1, $8,
                        'revoked', NOW(), NOW(), NOW() + INTERVAL '180 seconds',
                        NOW(), NULL
                    )
                    """,
                    grant_id,
                    uuid.uuid4(),
                    IMAGE_IDENTITY,
                    runtime_role,
                    ENDPOINT_FINGERPRINTS["database"],
                    ENDPOINT_FINGERPRINTS["redis"],
                    ENDPOINT_FINGERPRINTS["storage"],
                    _sha256("invalid-registration-secret"),
                )

            pipeline_id = await connection.fetchval(
                """
                INSERT INTO pipelines (name, definition)
                VALUES ('worker-binding-check', '{}'::json)
                RETURNING id
                """
            )
            job_id = await connection.fetchval(
                """
                INSERT INTO jobs (pipeline_id, pipeline_snapshot)
                VALUES ($1, '{}'::json)
                RETURNING id
                """,
                pipeline_id,
            )
            node_execution_id = await connection.fetchval(
                """
                INSERT INTO node_executions (job_id, node_id, node_type)
                VALUES ($1, 'node-a', 'youtube_upload')
                RETURNING id
                """,
                job_id,
            )
            for registration_id, epoch in (
                (first["registration_id"], None),
                (None, 1),
            ):
                transaction = connection.transaction()
                await transaction.start()
                try:
                    with pytest.raises(asyncpg.CheckViolationError):
                        await connection.execute(
                            """
                            UPDATE node_executions
                            SET worker_registration_id = $1,
                                worker_lease_epoch = $2
                            WHERE id = $3
                            """,
                            registration_id,
                            epoch,
                            node_execution_id,
                        )
                finally:
                    await transaction.rollback()

            second_instance = uuid.uuid4()
            second_lease_hash = _sha256("lease-two")
            second = await runtime_connection.fetchrow(
                """
                SELECT * FROM public.vp_worker_register(
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
                ENDPOINT_FINGERPRINTS["database"],
                ENDPOINT_FINGERPRINTS["redis"],
                ENDPOINT_FINGERPRINTS["storage"],
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
                    SELECT public.vp_worker_heartbeat($1, $2, $3, $4, $5)
                    """,
                    first["registration_id"],
                    "service-a",
                    first_instance,
                    1,
                    first_lease_hash,
                )

            heartbeat_args = (
                second["registration_id"],
                "service-a",
                second_instance,
                2,
                second_lease_hash,
            )
            for index in range(len(heartbeat_args)):
                null_args = list(heartbeat_args)
                null_args[index] = None
                transaction = runtime_connection.transaction()
                await transaction.start()
                try:
                    with pytest.raises(
                        asyncpg.RaiseError,
                        match="lease_fenced",
                    ):
                        await runtime_connection.fetchval(
                            """
                            SELECT public.vp_worker_heartbeat(
                                $1, $2, $3, $4, $5
                            )
                            """,
                            *null_args,
                        )
                finally:
                    await transaction.rollback()

            for require_args in (
                (None, 2),
                (second["registration_id"], None),
            ):
                with pytest.raises(
                    asyncpg.RaiseError,
                    match="lease_fenced",
                ):
                    await runtime_connection.execute(
                        "SELECT public.vp_require_worker_lease($1, $2)",
                        *require_args,
                    )

            release_args = (
                second["registration_id"],
                "service-a",
                second_instance,
                2,
                second_lease_hash,
                "shutdown",
            )
            for index in range(len(release_args)):
                null_args = list(release_args)
                null_args[index] = None
                transaction = runtime_connection.transaction()
                await transaction.start()
                try:
                    with pytest.raises(
                        asyncpg.RaiseError,
                        match=(
                            "claim_mismatch"
                            if index == 5
                            else "lease_fenced"
                        ),
                    ):
                        await runtime_connection.fetchval(
                            """
                            SELECT public.vp_worker_release(
                                $1, $2, $3, $4, $5, $6
                            )
                            """,
                            *null_args,
                        )
                finally:
                    await transaction.rollback()

            renewed = await runtime_connection.fetchval(
                """
                SELECT public.vp_worker_heartbeat($1, $2, $3, $4, $5)
                """,
                *heartbeat_args,
            )
            assert 179 <= (
                renewed
                - await connection.fetchval("SELECT clock_timestamp()")
            ).total_seconds() <= 180

            fence_connection = await asyncpg.connect(runtime_url)
            heartbeat_connection = await asyncpg.connect(runtime_url)
            takeover_connection = await asyncpg.connect(runtime_url)
            third_instance = uuid.uuid4()
            third_lease_hash = _sha256("lease-three")
            third_args = list(register_args)
            third_args[4] = third_instance
            third_args[5] = 3
            third_args[6] = "consumer-three"
            third_args[17] = third_lease_hash
            fence_transaction = fence_connection.transaction()
            heartbeat_task: asyncio.Task[object] | None = None
            takeover_task: asyncio.Task[object] | None = None
            fence_committed = False
            try:
                await fence_transaction.start()
                await fence_connection.execute(
                    "SELECT public.vp_require_worker_lease($1, $2)",
                    second["registration_id"],
                    2,
                )
                heartbeat_task = asyncio.create_task(
                    heartbeat_connection.fetchval(
                        """
                        SELECT public.vp_worker_heartbeat(
                            $1, $2, $3, $4, $5
                        )
                        """,
                        *heartbeat_args,
                    )
                )
                heartbeat_done, _ = await asyncio.wait(
                    {heartbeat_task},
                    timeout=0.5,
                )
                assert heartbeat_task in heartbeat_done, (
                    "heartbeat blocked behind a held shared lease fence"
                )
                await heartbeat_task

                takeover_task = asyncio.create_task(
                    takeover_connection.fetchrow(
                        register_sql,
                        *third_args,
                    )
                )
                takeover_done, _ = await asyncio.wait(
                    {takeover_task},
                    timeout=0.3,
                )
                assert takeover_task not in takeover_done, (
                    "takeover crossed a held shared lease fence"
                )
                await fence_transaction.commit()
                fence_committed = True
                third = await takeover_task
            finally:
                if not fence_committed:
                    await fence_transaction.rollback()
                for task in (heartbeat_task, takeover_task):
                    if task is not None and not task.done():
                        task.cancel()
                await asyncio.gather(
                    *(
                        task
                        for task in (heartbeat_task, takeover_task)
                        if task is not None
                    ),
                    return_exceptions=True,
                )
                await fence_connection.close()
                await heartbeat_connection.close()
                await takeover_connection.close()

            assert third["lease_epoch"] == 3
            release_fence_connection = await asyncpg.connect(runtime_url)
            release_connection = await asyncpg.connect(runtime_url)
            release_transaction = release_fence_connection.transaction()
            release_task: asyncio.Task[object] | None = None
            release_fence_committed = False
            try:
                await release_transaction.start()
                await release_fence_connection.execute(
                    "SELECT public.vp_require_worker_lease($1, $2)",
                    third["registration_id"],
                    3,
                )
                release_task = asyncio.create_task(
                    release_connection.fetchval(
                        """
                        SELECT public.vp_worker_release(
                            $1, $2, $3, $4, $5, $6
                        )
                        """,
                        third["registration_id"],
                        "service-a",
                        third_instance,
                        3,
                        third_lease_hash,
                        "shutdown",
                    )
                )
                release_done, _ = await asyncio.wait(
                    {release_task},
                    timeout=0.3,
                )
                assert release_task not in release_done, (
                    "release crossed a held shared lease fence"
                )
                await release_transaction.commit()
                release_fence_committed = True
                assert await release_task
            finally:
                if not release_fence_committed:
                    await release_transaction.rollback()
                if release_task is not None and not release_task.done():
                    release_task.cancel()
                if release_task is not None:
                    await asyncio.gather(
                        release_task,
                        return_exceptions=True,
                    )
                await release_fence_connection.close()
                await release_connection.close()

            assert await runtime_connection.fetchval(
                """
                SELECT public.vp_worker_release($1, $2, $3, $4, $5, $6)
                """,
                third["registration_id"],
                "service-a",
                third_instance,
                3,
                third_lease_hash,
                "retry",
            )

            fourth_instance = uuid.uuid4()
            fourth_args = list(register_args)
            fourth_args[4] = fourth_instance
            fourth_args[5] = 4
            fourth_args[6] = "consumer-four"
            fourth_args[17] = _sha256("lease-four")
            fourth = await runtime_connection.fetchrow(
                register_sql,
                *fourth_args,
            )
            operator_mutation_connection = await asyncpg.connect(operator_url)
            order_fence_connection = await asyncpg.connect(runtime_url)
            order_takeover_connection = await asyncpg.connect(runtime_url)
            order_transaction = order_fence_connection.transaction()
            order_committed = False
            takeover_task: asyncio.Task[object] | None = None
            upsert_task: asyncio.Task[object] | None = None
            fifth_args = list(register_args)
            fifth_args[4] = uuid.uuid4()
            fifth_args[5] = 5
            fifth_args[6] = "consumer-five"
            fifth_args[17] = _sha256("lease-five")
            upsert_args = (
                "service-a",
                8,
                "ffmpeg_go",
                "colima-127",
                '["media_cpu"]',
                RELEASE_COMMIT,
                IMAGE_IDENTITY,
                runtime_role,
                "vp:tasks:ffmpeg_go",
                "ffmpeg_go-workers",
                json.dumps(ENDPOINT_BINDINGS),
                _sha256("admission-token-eight"),
                "migration-test",
            )
            try:
                await order_transaction.start()
                await order_fence_connection.execute(
                    "SELECT public.vp_require_worker_lease($1, $2)",
                    fourth["registration_id"],
                    4,
                )
                takeover_task = asyncio.create_task(
                    order_takeover_connection.fetchrow(
                        register_sql,
                        *fifth_args,
                    )
                )
                takeover_done, _ = await asyncio.wait(
                    {takeover_task},
                    timeout=0.25,
                )
                assert takeover_task not in takeover_done

                grant_probe = connection.transaction()
                await grant_probe.start()
                try:
                    assert await connection.fetchval(
                        """
                        SELECT id
                        FROM public.worker_admission_grants
                        WHERE service_name = 'service-a'
                          AND generation = 7
                        FOR UPDATE NOWAIT
                        """
                    ) == grant_id
                finally:
                    await grant_probe.rollback()

                upsert_task = asyncio.create_task(
                    operator_mutation_connection.fetchval(
                        """
                        SELECT public.vp_worker_grant_upsert(
                            $1, $2, $3, $4, $5::jsonb, $6, $7, $8,
                            $9, $10, $11::jsonb, $12, $13
                        )
                        """,
                        *upsert_args,
                    )
                )
                upsert_done, _ = await asyncio.wait(
                    {upsert_task},
                    timeout=0.25,
                )
                assert upsert_task not in upsert_done, (
                    "grant upsert crossed a held shared lease fence"
                )
                await order_transaction.commit()
                order_committed = True
                fifth = await asyncio.wait_for(takeover_task, timeout=3)
                pending_grant_id = await asyncio.wait_for(
                    upsert_task,
                    timeout=3,
                )
            finally:
                if not order_committed:
                    await order_transaction.rollback()
                for task in (takeover_task, upsert_task):
                    if task is not None and not task.done():
                        task.cancel()
                await asyncio.gather(
                    *(
                        task
                        for task in (takeover_task, upsert_task)
                        if task is not None
                    ),
                    return_exceptions=True,
                )
                await order_fence_connection.close()
                await order_takeover_connection.close()

            assert fifth["lease_epoch"] == 5
            assert pending_grant_id is not None

            async def assert_operator_waits_for_fence(
                sql: str,
                *args: object,
            ) -> object:
                fence = await asyncpg.connect(runtime_url)
                operator = await asyncpg.connect(operator_url)
                transaction = fence.transaction()
                task: asyncio.Task[object] | None = None
                committed = False
                try:
                    await transaction.start()
                    await fence.execute(
                        "SELECT public.vp_require_worker_lease($1, $2)",
                        fifth["registration_id"],
                        5,
                    )
                    task = asyncio.create_task(operator.fetchval(sql, *args))
                    done, _ = await asyncio.wait({task}, timeout=0.25)
                    assert task not in done, (
                        "operator mutation crossed a held shared lease fence"
                    )
                    await transaction.commit()
                    committed = True
                    return await asyncio.wait_for(task, timeout=3)
                finally:
                    if not committed:
                        await transaction.rollback()
                    if task is not None and not task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                    await fence.close()
                    await operator.close()

            assert (
                await assert_operator_waits_for_fence(
                    "SELECT public.vp_worker_grant_activate($1, $2)",
                    "service-a",
                    8,
                )
                == pending_grant_id
            )
            assert await assert_operator_waits_for_fence(
                "SELECT public.vp_worker_grant_revoke($1, $2, $3)",
                "service-a",
                8,
                "replacement-retired",
            )
            assert await assert_operator_waits_for_fence(
                "SELECT public.vp_worker_registration_revoke($1, $2, $3)",
                "service-a",
                fifth["registration_id"],
                "operator-stop",
            )

            generation_nine_token_hash = _sha256("admission-token-nine")
            generation_nine_id = await operator_mutation_connection.fetchval(
                """
                SELECT public.vp_worker_grant_upsert(
                    $1, $2, $3, $4, $5::jsonb, $6, $7, $8,
                    $9, $10, $11::jsonb, $12, $13
                )
                """,
                "service-a",
                9,
                "ffmpeg_go",
                "colima-127",
                '["media_cpu"]',
                RELEASE_COMMIT,
                IMAGE_IDENTITY,
                runtime_role,
                "vp:tasks:ffmpeg_go",
                "ffmpeg_go-workers",
                json.dumps(ENDPOINT_BINDINGS),
                generation_nine_token_hash,
                "migration-test",
            )
            assert (
                await operator_mutation_connection.fetchval(
                    "SELECT public.vp_worker_grant_activate($1, $2)",
                    "service-a",
                    9,
                )
                == generation_nine_id
            )
            expiry_args = list(register_args)
            expiry_args[1] = 9
            expiry_args[4] = uuid.uuid4()
            expiry_args[5] = 6
            expiry_args[6] = "consumer-six"
            expiry_args[16] = generation_nine_token_hash
            expiry_args[17] = _sha256("lease-six")
            expiry_registration = await runtime_connection.fetchrow(
                register_sql,
                *expiry_args,
            )
            await connection.execute(
                """
                UPDATE worker_registrations
                SET lease_expires_at =
                        clock_timestamp() + INTERVAL '0.5 seconds'
                WHERE id = $1
                """,
                expiry_registration["registration_id"],
            )
            expiry_fence = await asyncpg.connect(runtime_url)
            expiry_operator = await asyncpg.connect(operator_url)
            expiry_transaction = expiry_fence.transaction()
            expiry_task: asyncio.Task[object] | None = None
            expiry_committed = False
            try:
                await expiry_transaction.start()
                await expiry_fence.execute(
                    "SELECT public.vp_require_worker_lease($1, $2)",
                    expiry_registration["registration_id"],
                    6,
                )
                await asyncio.sleep(0.6)
                expiry_task = asyncio.create_task(
                    expiry_operator.fetchval(
                        """
                        SELECT public.vp_worker_registration_expire($1, $2)
                        """,
                        "service-a",
                        expiry_registration["registration_id"],
                    )
                )
                expiry_done, _ = await asyncio.wait(
                    {expiry_task},
                    timeout=0.25,
                )
                assert expiry_task not in expiry_done, (
                    "expiry mutation crossed a held shared lease fence"
                )
                await expiry_transaction.commit()
                expiry_committed = True
                assert await asyncio.wait_for(expiry_task, timeout=3)
            finally:
                if not expiry_committed:
                    await expiry_transaction.rollback()
                if expiry_task is not None and not expiry_task.done():
                    expiry_task.cancel()
                    await asyncio.gather(
                        expiry_task,
                        return_exceptions=True,
                    )
                await expiry_fence.close()
                await expiry_operator.close()
                await operator_mutation_connection.close()

            time_wait_operator = await asyncpg.connect(operator_url)
            generation_ten_token_hash = _sha256("admission-token-ten")
            try:
                await time_wait_operator.fetchval(
                    """
                    SELECT public.vp_worker_grant_upsert(
                        $1, $2, $3, $4, $5::jsonb, $6, $7, $8,
                        $9, $10, $11::jsonb, $12, $13
                    )
                    """,
                    "service-a",
                    10,
                    "ffmpeg_go",
                    "colima-127",
                    '["media_cpu"]',
                    RELEASE_COMMIT,
                    IMAGE_IDENTITY,
                    runtime_role,
                    "vp:tasks:ffmpeg_go",
                    "ffmpeg_go-workers",
                    json.dumps(ENDPOINT_BINDINGS),
                    generation_ten_token_hash,
                    "migration-test",
                )
                await time_wait_operator.fetchval(
                    "SELECT public.vp_worker_grant_activate($1, $2)",
                    "service-a",
                    10,
                )
            finally:
                await time_wait_operator.close()
            time_wait_args = list(register_args)
            time_wait_args[1] = 10
            time_wait_args[4] = uuid.uuid4()
            time_wait_args[5] = 7
            time_wait_args[6] = "consumer-seven"
            time_wait_args[16] = generation_ten_token_hash
            time_wait_args[17] = _sha256("lease-seven")
            time_wait_registration = await runtime_connection.fetchrow(
                register_sql,
                *time_wait_args,
            )
            await connection.execute(
                """
                UPDATE public.worker_registrations
                SET lease_expires_at = pg_catalog.clock_timestamp()
                    + INTERVAL '0.5 seconds'
                WHERE id = $1
                """,
                time_wait_registration["registration_id"],
            )
            expiry_lock = connection.transaction()
            await expiry_lock.start()
            expiry_lock_committed = False
            expiry_require_connection = await asyncpg.connect(runtime_url)
            expiry_require_task: asyncio.Task[object] | None = None
            try:
                await connection.execute(
                    """
                    SELECT pg_advisory_xact_lock(
                        hashtextextended(
                            'vp-worker-registration:' || $1::uuid::text,
                            0
                        )
                    )
                    """,
                    time_wait_registration["registration_id"],
                )
                expiry_require_task = asyncio.create_task(
                    expiry_require_connection.execute(
                        "SELECT public.vp_require_worker_lease($1, $2)",
                        time_wait_registration["registration_id"],
                        7,
                    )
                )
                expiry_done, _ = await asyncio.wait(
                    {expiry_require_task},
                    timeout=0.2,
                )
                assert expiry_require_task not in expiry_done, (
                    "require did not acquire the registration advisory fence"
                )
                await asyncio.sleep(0.5)
                await expiry_lock.commit()
                expiry_lock_committed = True
                with pytest.raises(
                    asyncpg.RaiseError,
                    match="lease_expired",
                ):
                    await expiry_require_task
            finally:
                if not expiry_lock_committed:
                    await expiry_lock.rollback()
                if (
                    expiry_require_task is not None
                    and not expiry_require_task.done()
                ):
                    expiry_require_task.cancel()
                if expiry_require_task is not None:
                    await asyncio.gather(
                        expiry_require_task,
                        return_exceptions=True,
                    )
                await expiry_require_connection.close()

            with pytest.raises(asyncpg.RaiseError, match="lease_fenced"):
                await runtime_connection.execute(
                    "SELECT public.vp_require_worker_lease($1, $2)",
                    third["registration_id"],
                    3,
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
            await admin.execute(f'DROP ROLE IF EXISTS "{set_role_login}"')
            await admin.execute(f'DROP ROLE IF EXISTS "{bridge_role}"')
            await admin.execute(f'DROP ROLE IF EXISTS "{writer_role}"')
            await admin.execute(f'DROP ROLE IF EXISTS "{owner_role}"')
            await admin.execute(f'DROP ROLE IF EXISTS "{operator_role}"')
        finally:
            await admin.close()
