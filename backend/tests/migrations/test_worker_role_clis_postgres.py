from __future__ import annotations

import asyncio
import hashlib
import json
import os
import stat
import subprocess
import sys
import threading
import uuid
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import asyncpg  # type: ignore[import-untyped]
import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.orchestrator.engine import JobEngine
from app.services import worker_control_role_cli as control_cli
from app.services import worker_registration_operator_cli as operator_cli
from app.services import worker_role_cli_common as role_common
from app.services import worker_runtime_role_cli as runtime_cli
from app.services.registered_worker_event_receipt import (
    RegisteredWorkerEventReceiptService,
    parse_registered_worker_event,
)
from app.services.youtube_upload_operations import (
    UploadOperationContext,
    YouTubeUploadOperationStore,
)
from worker import main as worker_main


POSTGRES_URL = os.getenv("CHANNEL_OPS_POSTGRES_TEST_URL", "")
BACKEND_ROOT = Path(__file__).resolve().parents[2]


def _database_url(database: str) -> str:
    return f"{POSTGRES_URL.rsplit('/', 1)[0]}/{database}"


def _asyncpg_url(database_url: str) -> str:
    return database_url.replace(
        "postgresql+asyncpg://",
        "postgresql://",
        1,
    )


def _alembic_url(database_url: str) -> str:
    return database_url.replace(
        "postgresql://",
        "postgresql+asyncpg://",
        1,
    )


def _run_alembic(database_url: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=BACKEND_ROOT,
        env={**os.environ, "DATABASE_URL": database_url},
        text=True,
        capture_output=True,
        check=False,
    )


def _write_secret(path: Path, value: str, mode: int) -> None:
    if path.exists():
        path.chmod(0o600)
    path.write_text(f"{value}\n", encoding="utf-8")
    path.chmod(mode)


async def _upsert_worker_grant(
    connection: asyncpg.Connection,
    *,
    service_name: str,
    generation: int,
    database_principal: str,
    admission_token: str,
) -> uuid.UUID:
    grant_id = await connection.fetchval(
        """
        SELECT public.vp_worker_grant_upsert(
            $1, $2, $3, $4, $5::jsonb, $6, $7, $8,
            $9, $10, $11::jsonb, $12, $13
        )
        """,
        service_name,
        generation,
        "ffmpeg_go",
        "colima-127",
        '["media_cpu"]',
        "0123456789abcdef0123456789abcdef01234567",
        "vp-ffmpeg-worker-go:deploy-0123456789ab",
        database_principal,
        "vp:tasks:ffmpeg_go",
        "ffmpeg_go-workers",
        json.dumps(
            {
                "database": {
                    "database": "videoprocess",
                    "driver": "postgresql",
                    "host": "vp-postgres",
                    "port": 55439,
                },
                "redis": {
                    "database": 14,
                    "host": "vp-redis",
                    "port": 56379,
                    "scheme": "redis",
                },
                "storage": {
                    "backend": "minio",
                    "bucket": "videoprocess",
                    "host": "10.0.0.150",
                    "port": 9000,
                },
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        hashlib.sha256(admission_token.encode()).hexdigest(),
        "task4a-authority-race",
    )
    assert isinstance(grant_id, uuid.UUID)
    return grant_id


async def _explicit_function_grants(
    connection: asyncpg.Connection,
    role_name: str,
) -> set[str]:
    rows = await connection.fetch(
        """
        SELECT
            routine.proname || '(' ||
            replace(
                pg_catalog.oidvectortypes(routine.proargtypes),
                ', ',
                ','
            ) || ')' AS signature
        FROM pg_catalog.pg_proc AS routine
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = routine.pronamespace
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(
                routine.proacl,
                pg_catalog.acldefault('f', routine.proowner)
            )
        ) AS privilege
        JOIN pg_catalog.pg_roles AS grantee
          ON grantee.oid = privilege.grantee
        WHERE namespace.nspname = 'public'
          AND grantee.rolname = $1
          AND privilege.privilege_type = 'EXECUTE'
        """,
        role_name,
    )
    return {row["signature"] for row in rows}


async def _effective_security_definer_functions(
    connection: asyncpg.Connection,
    role_name: str,
) -> set[str]:
    rows = await connection.fetch(
        """
        SELECT
            routine.proname || '(' ||
            replace(
                pg_catalog.oidvectortypes(routine.proargtypes),
                ', ',
                ','
            ) || ')' AS signature
        FROM pg_catalog.pg_proc AS routine
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = routine.pronamespace
        WHERE namespace.nspname = 'public'
          AND routine.prosecdef
          AND pg_catalog.has_function_privilege(
              $1,
              routine.oid,
              'EXECUTE'
          )
        """,
        role_name,
    )
    return {row["signature"] for row in rows}


async def _explicit_table_grants(
    connection: asyncpg.Connection,
    role_name: str,
) -> set[tuple[str, str]]:
    rows = await connection.fetch(
        """
        SELECT relation.relname, privilege.privilege_type
        FROM pg_catalog.pg_class AS relation
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            relation.relacl
        ) AS privilege
        JOIN pg_catalog.pg_roles AS grantee
          ON grantee.oid = privilege.grantee
        WHERE namespace.nspname = 'public'
          AND grantee.rolname = $1
        """,
        role_name,
    )
    return {
        (row["relname"], row["privilege_type"])
        for row in rows
    }


async def _explicit_column_grants(
    connection: asyncpg.Connection,
    role_name: str,
) -> set[tuple[str, str, str]]:
    rows = await connection.fetch(
        """
        SELECT
            relation.relname,
            attribute.attname,
            privilege.privilege_type
        FROM pg_catalog.pg_class AS relation
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        JOIN pg_catalog.pg_attribute AS attribute
          ON attribute.attrelid = relation.oid
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            attribute.attacl
        ) AS privilege
        JOIN pg_catalog.pg_roles AS grantee
          ON grantee.oid = privilege.grantee
        WHERE namespace.nspname = 'public'
          AND attribute.attnum > 0
          AND NOT attribute.attisdropped
          AND grantee.rolname = $1
        """,
        role_name,
    )
    return {
        (
            row["relname"],
            row["attname"],
            row["privilege_type"],
        )
        for row in rows
    }


def _pipeline_snapshot() -> dict[str, object]:
    return {
        "nodes": [
            {
                "id": "publish",
                "type": "youtube_upload",
                "position": {"x": 0, "y": 0},
                "data": {
                    "label": "Publish",
                    "config": {},
                },
            }
        ],
        "edges": [],
    }


@pytest.mark.asyncio
@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for live role tests",
)
async def test_real_role_clis_enforce_exact_cross_role_boundaries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database = f"vp_worker_roles_{uuid.uuid4().hex[:24]}"
    service_name = "vp-ffmpeg-worker-go-swarm"
    worker_generation = int(uuid.uuid4().hex[:12], 16)
    control_generation = f"t-{uuid.uuid4().hex[:20]}"
    rogue_principal = f"vp_task4a_rogue_{uuid.uuid4().hex[:16]}"
    leak_caller = f"vp_task4a_leak_{uuid.uuid4().hex[:16]}"
    leak_target = f"vp_task4a_target_{uuid.uuid4().hex[:16]}"
    migration_drift_owner = (
        f"vp_task4a_migration_owner_{uuid.uuid4().hex[:10]}"
    )
    migration_acl_probe = (
        f"vp_task4a_migration_probe_{uuid.uuid4().hex[:10]}"
    )
    runtime_names = runtime_cli.role_names_for_generation(
        service_name,
        worker_generation,
    )
    control_names = control_cli.role_names_for_generation(
        control_generation
    )
    admin_url = _database_url("postgres")
    target_url = _database_url(database)

    admin = await asyncpg.connect(_asyncpg_url(admin_url))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()

    try:
        migration_seed = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            await migration_seed.execute(
                f'CREATE ROLE "{migration_drift_owner}" NOLOGIN'
            )
            await migration_seed.execute(
                f'CREATE ROLE "{migration_acl_probe}" NOLOGIN'
            )
            await migration_seed.execute(
                f'ALTER DEFAULT PRIVILEGES FOR ROLE '
                f'"{migration_drift_owner}" '
                "GRANT SELECT ON TABLES TO PUBLIC"
            )
            await migration_seed.execute(
                f'ALTER DEFAULT PRIVILEGES FOR ROLE '
                f'"{migration_drift_owner}" '
                "GRANT EXECUTE ON FUNCTIONS TO PUBLIC"
            )
        finally:
            await migration_seed.close()
        migrated = _run_alembic(_alembic_url(target_url))
        assert migrated.returncode == 0, migrated.stdout + migrated.stderr
        migration_proof = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert not await migration_proof.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_default_acl AS defaults
                    CROSS JOIN LATERAL pg_catalog.aclexplode(
                        defaults.defaclacl
                    ) AS privilege
                    JOIN pg_catalog.pg_roles AS owner
                      ON owner.oid = defaults.defaclrole
                    WHERE owner.rolname = $1
                      AND defaults.defaclnamespace = 0
                      AND privilege.grantee = 0
                )
                """,
                migration_drift_owner,
            )
            await migration_proof.execute(
                f'GRANT CREATE ON SCHEMA public '
                f'TO "{migration_drift_owner}"'
            )
            await migration_proof.execute(
                f'SET ROLE "{migration_drift_owner}"'
            )
            await migration_proof.execute(
                "CREATE TABLE public.task4a_migration_future_table "
                "(id bigint)"
            )
            await migration_proof.execute(
                """
                CREATE FUNCTION public.task4a_migration_future_definer()
                RETURNS boolean
                LANGUAGE sql
                SECURITY DEFINER
                SET search_path = pg_catalog
                AS 'SELECT TRUE'
                """
            )
            await migration_proof.execute("RESET ROLE")
            assert not await migration_proof.fetchval(
                "SELECT has_table_privilege("
                "$1, 'public.task4a_migration_future_table', 'SELECT')",
                migration_acl_probe,
            )
            assert not await migration_proof.fetchval(
                "SELECT has_function_privilege("
                "$1, 'public.task4a_migration_future_definer()', "
                "'EXECUTE')",
                migration_acl_probe,
            )
        finally:
            await migration_proof.close()
        owner_file = tmp_path / "owner-url"
        _write_secret(owner_file, target_url, 0o400)

        runtime_state = tmp_path / "runtime-state"
        monkeypatch.setenv(
            runtime_cli.OWNER_URL_FILE_ENV,
            str(owner_file),
        )
        assert await runtime_cli.run(
            [
                "provision",
                "--service-name",
                service_name,
                "--generation",
                str(worker_generation),
                "--state-dir",
                str(runtime_state),
            ]
        ) == 0
        capsys.readouterr()

        control_state = tmp_path / "control-state"
        monkeypatch.setenv(
            control_cli.OWNER_URL_FILE_ENV,
            str(owner_file),
        )
        assert await control_cli.run(
            [
                "provision",
                "--generation",
                control_generation,
                "--state-dir",
                str(control_state),
            ]
        ) == 0
        capsys.readouterr()

        runtime_paths = runtime_cli.credential_paths(
            runtime_state,
            service_name,
            worker_generation,
        )
        runtime_url = runtime_paths["database_url"].read_text().strip()
        admission_token = (
            runtime_paths["admission_token"].read_text().strip()
        )
        control_paths = control_cli.credential_paths(
            control_state,
            control_generation,
        )
        role_urls = {
            purpose: path.read_text().strip()
            for purpose, path in control_paths.items()
        }
        assert {
            purpose: stat.S_IMODE(path.stat().st_mode)
            for purpose, path in runtime_paths.items()
        } == {
            "database_url": 0o400,
            "admission_token": 0o400,
            "state": 0o600,
        }
        assert all(
            stat.S_IMODE(path.stat().st_mode) == 0o400
            for path in control_paths.values()
        )
        assert all(
            stat.S_IMODE(path.stat().st_mode) == 0o700
            for path in (
                runtime_state,
                runtime_paths["state"].parent.parent,
                runtime_paths["state"].parent,
                control_state,
                next(iter(control_paths.values())).parent,
            )
        )
        parsed_role_urls = tuple(
            make_url(database_url)
            for database_url in (runtime_url, *role_urls.values())
        )
        database_credentials = {
            str(database_url.username): str(database_url.password)
            for database_url in parsed_role_urls
        }
        database_passwords = set(database_credentials.values())
        assert len(database_passwords) == 4
        assert all(
            password and password != "None"
            for password in database_passwords
        )
        assert all(
            password not in os.environ.values()
            and password not in sys.argv
            for password in database_passwords
        )
        original_runtime_state = {
            purpose: path.read_bytes()
            for purpose, path in runtime_paths.items()
        }
        original_control_state = {
            purpose: path.read_bytes()
            for purpose, path in control_paths.items()
        }
        drift_owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            sentinel_password = "TASK4A-REVIEW-SENTINEL-PASSWORD"
            await drift_owner.execute(
                f'CREATE ROLE "{leak_caller}" LOGIN PASSWORD '
                "'task4a-caller-password'"
            )
            leak_url = make_url(target_url).set(
                username=leak_caller,
                password="task4a-caller-password",
            ).render_as_string(hide_password=False)
            leak_connection = await asyncpg.connect(_asyncpg_url(leak_url))
            try:
                with pytest.raises(
                    asyncpg.InsufficientPrivilegeError
                ) as caught:
                    async with leak_connection.transaction():
                        await role_common.create_login_role(
                            leak_connection,
                            leak_target,
                            sentinel_password,
                            setting_prefix="task4a_leak_probe",
                        )
                diagnostics = "\n".join(
                    str(value)
                    for value in (
                        caught.value,
                        caught.value.detail,
                        caught.value.hint,
                        caught.value.context,
                        getattr(caught.value, "query", None),
                    )
                    if value is not None
                )
                assert sentinel_password not in diagnostics
            finally:
                await leak_connection.close()

            await drift_owner.execute(
                f'ALTER ROLE "{runtime_names.versioned}" '
                "CONNECTION LIMIT 0"
            )
            assert await runtime_cli.run(
                [
                    "provision",
                    "--service-name",
                    service_name,
                    "--generation",
                    str(worker_generation),
                    "--state-dir",
                    str(runtime_state),
                ]
            ) == 4
            capsys.readouterr()
            await drift_owner.execute(
                f'ALTER ROLE "{runtime_names.versioned}" '
                "CONNECTION LIMIT -1"
            )
            await drift_owner.execute(
                f'ALTER ROLE "{runtime_names.versioned}" '
                "VALID UNTIL '2000-01-01 00:00:00+00'"
            )
            assert await runtime_cli.run(
                [
                    "provision",
                    "--service-name",
                    service_name,
                    "--generation",
                    str(worker_generation),
                    "--state-dir",
                    str(runtime_state),
                ]
            ) == 4
            capsys.readouterr()
            await drift_owner.execute(
                f'ALTER ROLE "{runtime_names.versioned}" '
                "VALID UNTIL 'infinity'"
            )

            parsed_runtime_url = make_url(runtime_url)
            wrong_password_url = parsed_runtime_url.set(
                password="wrong-task4a-password"
            ).render_as_string(hide_password=False)
            _write_secret(
                runtime_paths["database_url"],
                wrong_password_url,
                0o400,
            )
            assert await runtime_cli.run(
                [
                    "provision",
                    "--service-name",
                    service_name,
                    "--generation",
                    str(worker_generation),
                    "--state-dir",
                    str(runtime_state),
                ]
            ) == 4
            capsys.readouterr()
            wrong_endpoint_url = parsed_runtime_url.set(
                host="localhost"
            ).render_as_string(hide_password=False)
            _write_secret(
                runtime_paths["database_url"],
                wrong_endpoint_url,
                0o400,
            )
            assert await runtime_cli.run(
                [
                    "provision",
                    "--service-name",
                    service_name,
                    "--generation",
                    str(worker_generation),
                    "--state-dir",
                    str(runtime_state),
                ]
            ) == 4
            capsys.readouterr()
            _write_secret(
                runtime_paths["database_url"],
                runtime_url,
                0o400,
            )

            await drift_owner.execute(
                f'ALTER ROLE "{runtime_names.stable}" '
                "LOGIN INHERIT SUPERUSER CREATEDB CREATEROLE "
                "REPLICATION BYPASSRLS"
            )
            await drift_owner.execute(
                f'GRANT pg_write_all_data TO "{runtime_names.stable}"'
            )
            await drift_owner.execute(
                f'ALTER ROLE "{runtime_names.versioned}" '
                "LOGIN INHERIT SUPERUSER CREATEDB CREATEROLE "
                "REPLICATION BYPASSRLS"
            )
            await drift_owner.execute(
                f'GRANT pg_read_all_data TO "{runtime_names.versioned}"'
            )
            await drift_owner.execute(
                "GRANT SELECT ON public.worker_registrations "
                f'TO "{runtime_names.versioned}"'
            )
            await drift_owner.execute(
                f'GRANT CREATE, TEMPORARY ON DATABASE "{database}" '
                f'TO "{runtime_names.versioned}"'
            )

            orchestrator_stable = control_names.stable["orchestrator"]
            orchestrator_login = control_names.versioned["orchestrator"]
            await drift_owner.execute(
                f'ALTER ROLE "{orchestrator_stable}" '
                "LOGIN INHERIT SUPERUSER CREATEDB CREATEROLE "
                "REPLICATION BYPASSRLS"
            )
            await drift_owner.execute(
                f'GRANT pg_write_all_data TO "{orchestrator_stable}"'
            )
            await drift_owner.execute(
                f'ALTER ROLE "{orchestrator_login}" '
                "LOGIN INHERIT SUPERUSER CREATEDB CREATEROLE "
                "REPLICATION BYPASSRLS"
            )
            await drift_owner.execute(
                f'GRANT pg_read_all_data TO "{orchestrator_login}"'
            )
            await drift_owner.execute(
                "GRANT SELECT ON public.worker_registrations "
                f'TO "{orchestrator_login}"'
            )
            await drift_owner.execute(
                f'GRANT CREATE, TEMPORARY ON DATABASE "{database}" '
                f'TO "{orchestrator_login}"'
            )
            await drift_owner.execute(
                f'ALTER ROLE "{runtime_names.versioned}" '
                "CONNECTION LIMIT 3 VALID UNTIL '2099-01-01 00:00:00+00'"
            )
            await drift_owner.execute(
                f'CREATE ROLE "{rogue_principal}" LOGIN PASSWORD '
                "'task4a-rogue-password'"
            )
            await drift_owner.execute(
                "GRANT UPDATE (kind) ON TABLE public.artifacts TO PUBLIC"
            )
            await drift_owner.execute(
                "GRANT SELECT ON TABLE public.worker_registrations TO PUBLIC"
            )
            await drift_owner.execute(
                "CREATE SEQUENCE public.task4a_public_sequence"
            )
            await drift_owner.execute(
                "GRANT USAGE ON SEQUENCE "
                "public.task4a_public_sequence TO PUBLIC"
            )
            await drift_owner.execute(
                "GRANT CREATE ON SCHEMA public TO PUBLIC"
            )
            await drift_owner.execute(
                f'GRANT CREATE, TEMPORARY ON DATABASE "{database}" TO PUBLIC'
            )
            await drift_owner.execute(
                "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
                "GRANT SELECT ON TABLES TO PUBLIC"
            )
            await drift_owner.execute(
                "ALTER DEFAULT PRIVILEGES "
                "GRANT SELECT ON TABLES TO PUBLIC"
            )
            await drift_owner.execute(
                "ALTER DEFAULT PRIVILEGES "
                "GRANT USAGE ON SEQUENCES TO PUBLIC"
            )
            await drift_owner.execute(
                "ALTER DEFAULT PRIVILEGES "
                "GRANT EXECUTE ON FUNCTIONS TO PUBLIC"
            )
            await drift_owner.execute(
                "ALTER DEFAULT PRIVILEGES "
                "GRANT USAGE ON TYPES TO PUBLIC"
            )
            await drift_owner.execute(
                "ALTER DEFAULT PRIVILEGES "
                "GRANT USAGE ON SCHEMAS TO PUBLIC"
            )
            await drift_owner.execute(
                f'ALTER DEFAULT PRIVILEGES FOR ROLE "{rogue_principal}" '
                "IN SCHEMA public GRANT SELECT ON TABLES TO PUBLIC"
            )
            await drift_owner.execute(
                f'ALTER DEFAULT PRIVILEGES FOR ROLE "{rogue_principal}" '
                "GRANT SELECT ON TABLES TO PUBLIC"
            )
            await drift_owner.execute(
                f'ALTER DEFAULT PRIVILEGES FOR ROLE "{rogue_principal}" '
                "GRANT USAGE ON SEQUENCES TO PUBLIC"
            )
            await drift_owner.execute(
                f'ALTER DEFAULT PRIVILEGES FOR ROLE "{rogue_principal}" '
                "GRANT EXECUTE ON FUNCTIONS TO PUBLIC"
            )
            await drift_owner.execute(
                f'ALTER DEFAULT PRIVILEGES FOR ROLE "{rogue_principal}" '
                "GRANT USAGE ON TYPES TO PUBLIC"
            )
            await drift_owner.execute(
                f'ALTER DEFAULT PRIVILEGES FOR ROLE "{rogue_principal}" '
                "GRANT USAGE ON SCHEMAS TO PUBLIC"
            )
            await drift_owner.execute(
                "GRANT EXECUTE ON FUNCTION "
                "public.vp_worker_grant_activate(text,bigint) TO PUBLIC"
            )
            await drift_owner.execute(
                "GRANT EXECUTE ON FUNCTION "
                "public.vp_worker_grant_activate(text,bigint) "
                f'TO "{rogue_principal}"'
            )
            await drift_owner.execute(
                f'GRANT "{runtime_names.versioned}" '
                f'TO "{rogue_principal}"'
            )
            await drift_owner.execute(
                f'GRANT "{control_names.stable["operator"]}" '
                f'TO "{rogue_principal}"'
            )
        finally:
            await drift_owner.close()

        assert await runtime_cli.run(
            [
                "provision",
                "--service-name",
                service_name,
                "--generation",
                str(worker_generation),
                "--state-dir",
                str(runtime_state),
            ]
        ) == 0
        assert await control_cli.run(
            [
                "provision",
                "--generation",
                control_generation,
                "--state-dir",
                str(control_state),
            ]
        ) == 0
        capsys.readouterr()
        assert {
            purpose: path.read_bytes()
            for purpose, path in runtime_paths.items()
        } == original_runtime_state
        assert {
            purpose: path.read_bytes()
            for purpose, path in control_paths.items()
        } == original_control_state

        converged_owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            converged_attributes = await converged_owner.fetchrow(
                """
                SELECT
                    rolconnlimit,
                    rolvaliduntil = 'infinity'::timestamptz
                        AS validity_unbounded
                FROM pg_catalog.pg_roles
                WHERE rolname = $1
                """,
                runtime_names.versioned,
            )
            assert converged_attributes is not None
            assert converged_attributes["rolconnlimit"] == -1
            assert converged_attributes["validity_unbounded"]
            assert not await converged_owner.fetchval(
                "SELECT has_column_privilege("
                "$1, 'public.artifacts', 'kind', 'UPDATE')",
                rogue_principal,
            )
            assert not await converged_owner.fetchval(
                "SELECT has_table_privilege("
                "$1, 'public.worker_registrations', 'SELECT')",
                rogue_principal,
            )
            assert not await converged_owner.fetchval(
                "SELECT has_sequence_privilege("
                "$1, 'public.task4a_public_sequence', 'USAGE')",
                rogue_principal,
            )
            assert not await converged_owner.fetchval(
                "SELECT has_schema_privilege($1, 'public', 'CREATE')",
                rogue_principal,
            )
            assert not await converged_owner.fetchval(
                "SELECT has_database_privilege("
                "$1, current_database(), 'CREATE')",
                rogue_principal,
            )
            assert not await converged_owner.fetchval(
                "SELECT has_database_privilege("
                "$1, current_database(), 'TEMPORARY')",
                rogue_principal,
            )
            assert not await converged_owner.fetchval(
                """
                SELECT pg_catalog.pg_has_role($1, $2, 'MEMBER')
                """,
                rogue_principal,
                runtime_names.versioned,
            )
            assert not await converged_owner.fetchval(
                """
                SELECT pg_catalog.pg_has_role($1, $2, 'MEMBER')
                """,
                rogue_principal,
                control_names.stable["operator"],
            )
            assert not await converged_owner.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_default_acl AS defaults
                    CROSS JOIN LATERAL pg_catalog.aclexplode(
                        defaults.defaclacl
                    ) AS privilege
                    LEFT JOIN pg_catalog.pg_namespace AS namespace
                      ON namespace.oid = defaults.defaclnamespace
                    WHERE (
                        defaults.defaclnamespace = 0
                        OR namespace.nspname = 'public'
                    )
                      AND privilege.grantee = 0
                )
                """
            )
            await converged_owner.execute(
                f'GRANT CREATE ON SCHEMA public TO "{rogue_principal}"'
            )
            await converged_owner.execute(
                f'GRANT CREATE ON DATABASE "{database}" '
                f'TO "{rogue_principal}"'
            )
            await converged_owner.execute(
                f'SET ROLE "{rogue_principal}"'
            )
            await converged_owner.execute(
                "CREATE TABLE public.task4a_future_table (id bigint)"
            )
            await converged_owner.execute(
                "CREATE SEQUENCE public.task4a_future_sequence"
            )
            await converged_owner.execute(
                """
                CREATE FUNCTION public.task4a_future_security_definer()
                RETURNS boolean
                LANGUAGE sql
                SECURITY DEFINER
                SET search_path = pg_catalog
                AS 'SELECT TRUE'
                """
            )
            await converged_owner.execute(
                "CREATE TYPE public.task4a_future_type "
                "AS ENUM ('reviewed')"
            )
            await converged_owner.execute(
                "CREATE SCHEMA task4a_future_schema"
            )
            await converged_owner.execute("RESET ROLE")
            assert not await converged_owner.fetchval(
                "SELECT has_table_privilege("
                "$1, 'public.task4a_future_table', 'SELECT')",
                leak_caller,
            )
            assert not await converged_owner.fetchval(
                "SELECT has_sequence_privilege("
                "$1, 'public.task4a_future_sequence', 'USAGE')",
                leak_caller,
            )
            assert not await converged_owner.fetchval(
                "SELECT has_function_privilege("
                "$1, 'public.task4a_future_security_definer()', "
                "'EXECUTE')",
                leak_caller,
            )
            assert not await converged_owner.fetchval(
                "SELECT has_type_privilege("
                "$1, 'public.task4a_future_type', 'USAGE')",
                leak_caller,
            )
            assert not await converged_owner.fetchval(
                "SELECT has_schema_privilege("
                "$1, 'task4a_future_schema', 'USAGE')",
                leak_caller,
            )
        finally:
            await converged_owner.close()

        rogue_url = make_url(target_url).set(
            username=rogue_principal,
            password="task4a-rogue-password",
        ).render_as_string(hide_password=False)
        rogue = await asyncpg.connect(_asyncpg_url(rogue_url))
        try:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await rogue.fetchval(
                    "SELECT public.vp_worker_grant_activate($1, $2)",
                    service_name,
                    worker_generation,
                )
        finally:
            await rogue.close()

        request = {
            "version": 1,
            "service_name": service_name,
            "generation": worker_generation,
            "worker_type": "ffmpeg_go",
            "worker_host": "colima-127",
            "capabilities": ["media_cpu"],
            "release_commit": (
                "0123456789abcdef0123456789abcdef01234567"
            ),
            "image_identity": (
                "vp-ffmpeg-worker-go:deploy-0123456789ab"
            ),
            "database_principal": runtime_names.versioned,
            "redis_stream": "vp:tasks:ffmpeg_go",
            "redis_group": "ffmpeg_go-workers",
            "endpoint_bindings": {
                "database": {
                    "driver": "postgresql",
                    "host": "vp-postgres",
                    "port": 55439,
                    "database": database,
                },
                "redis": {
                    "scheme": "redis",
                    "host": "vp-redis",
                    "port": 56379,
                    "database": 14,
                },
                "storage": {
                    "backend": "minio",
                    "host": "10.0.0.150",
                    "port": 9000,
                    "bucket": "videoprocess",
                },
            },
            "token_sha256": hashlib.sha256(
                admission_token.encode()
            ).hexdigest(),
            "issued_by": "vp-deploy-controller",
        }
        request_file = tmp_path / "upsert.json"
        request_file.write_text(json.dumps(request), encoding="utf-8")
        request_file.chmod(0o600)
        operator_url_file = tmp_path / "operator-url"
        _write_secret(
            operator_url_file,
            role_urls["operator"],
            0o400,
        )
        monkeypatch.setenv(
            operator_cli.DATABASE_URL_FILE_ENV,
            str(operator_url_file),
        )
        assert await operator_cli.run(
            ["upsert", "--request-file", str(request_file)]
        ) == 0
        assert await operator_cli.run(
            [
                "activate",
                "--service-name",
                service_name,
                "--generation",
                str(worker_generation),
            ]
        ) == 0
        capsys.readouterr()

        owner = await asyncpg.connect(_asyncpg_url(target_url))
        runtime = await asyncpg.connect(_asyncpg_url(runtime_url))
        operator = await asyncpg.connect(
            _asyncpg_url(role_urls["operator"])
        )
        orchestrator = await asyncpg.connect(
            _asyncpg_url(role_urls["orchestrator"])
        )
        janitor = await asyncpg.connect(
            _asyncpg_url(role_urls["staging_janitor"])
        )
        try:
            for principal, password in database_credentials.items():
                stored_password = await owner.fetchval(
                    """
                    SELECT rolpassword
                    FROM pg_catalog.pg_authid
                    WHERE rolname = $1
                    """,
                    principal,
                )
                assert stored_password
                assert password not in stored_password
            for secret in (*database_passwords, admission_token):
                assert not await owner.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM public.worker_admission_grants AS grant_record
                        WHERE pg_catalog.strpos(
                            pg_catalog.to_jsonb(grant_record)::text,
                            $1
                        ) > 0
                        UNION ALL
                        SELECT 1
                        FROM public.worker_registrations AS registration
                        WHERE pg_catalog.strpos(
                            pg_catalog.to_jsonb(registration)::text,
                            $1
                        ) > 0
                        UNION ALL
                        SELECT 1
                        FROM pg_catalog.pg_stat_activity AS activity
                        WHERE pg_catalog.strpos(activity.query, $1) > 0
                    )
                    """,
                    secret,
                )

            login_principals = {
                runtime_names.versioned,
                *control_names.versioned.values(),
            }
            stable_principals = {
                runtime_names.stable,
                *control_names.stable.values(),
            }
            all_principals = login_principals | stable_principals
            rows = await owner.fetch(
                """
                SELECT rolname, rolcanlogin, rolsuper, rolcreatedb,
                       rolcreaterole, rolreplication, rolbypassrls,
                       rolinherit
                FROM pg_catalog.pg_roles
                WHERE rolname = ANY($1::text[])
                """,
                list(all_principals),
            )
            roles = {row["rolname"]: row for row in rows}
            assert set(roles) == all_principals
            for role_name in login_principals:
                role = roles[role_name]
                assert role["rolcanlogin"]
                assert role["rolinherit"]
            for role_name in stable_principals:
                role = roles[role_name]
                assert not role["rolcanlogin"]
                assert not role["rolinherit"]
            assert all(
                not role["rolsuper"]
                and not role["rolcreatedb"]
                and not role["rolcreaterole"]
                and not role["rolreplication"]
                and not role["rolbypassrls"]
                for role in roles.values()
            )
            expected_memberships = dict(
                (
                    (runtime_names.versioned, runtime_names.stable),
                    *(
                        (
                            control_names.versioned[purpose],
                            control_names.stable[purpose],
                        )
                        for purpose in control_names.stable
                    ),
                )
            )
            for principal in all_principals:
                memberships = await owner.fetchval(
                    """
                    SELECT COALESCE(
                        array_agg(
                            granted.rolname
                            ORDER BY granted.rolname
                        ),
                        ARRAY[]::text[]
                    )
                    FROM pg_catalog.pg_auth_members AS membership
                    JOIN pg_catalog.pg_roles AS member
                      ON member.oid = membership.member
                    JOIN pg_catalog.pg_roles AS granted
                      ON granted.oid = membership.roleid
                    WHERE member.rolname = $1
                    """,
                    principal,
                )
                expected_stable = expected_memberships.get(principal)
                assert memberships == (
                    [expected_stable] if expected_stable else []
                )
                assert not await owner.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_catalog.pg_class AS object
                        WHERE object.relowner = (
                            SELECT oid FROM pg_catalog.pg_roles
                            WHERE rolname = $1
                        )
                        UNION ALL
                        SELECT 1
                        FROM pg_catalog.pg_proc AS object
                        WHERE object.proowner = (
                            SELECT oid FROM pg_catalog.pg_roles
                            WHERE rolname = $1
                        )
                        UNION ALL
                        SELECT 1
                        FROM pg_catalog.pg_type AS object
                        WHERE object.typowner = (
                            SELECT oid FROM pg_catalog.pg_roles
                            WHERE rolname = $1
                        )
                        UNION ALL
                        SELECT 1
                        FROM pg_catalog.pg_namespace AS object
                        WHERE object.nspowner = (
                            SELECT oid FROM pg_catalog.pg_roles
                            WHERE rolname = $1
                        )
                        UNION ALL
                        SELECT 1
                        FROM pg_catalog.pg_database AS object
                        WHERE object.datdba = (
                            SELECT oid FROM pg_catalog.pg_roles
                            WHERE rolname = $1
                        )
                    )
                    """,
                    principal,
                )
                assert await owner.fetchval(
                    "SELECT has_schema_privilege($1, 'public', 'USAGE')",
                    principal,
                )
                assert not await owner.fetchval(
                    "SELECT has_schema_privilege($1, 'public', 'CREATE')",
                    principal,
                )
                assert not await owner.fetchval(
                    "SELECT has_database_privilege("
                    "$1, current_database(), 'TEMPORARY')",
                    principal,
                )
                assert not await owner.fetchval(
                    "SELECT has_database_privilege("
                    "$1, current_database(), 'CREATE')",
                    principal,
                )

            for signature in runtime_cli.WORKER_FUNCTIONS:
                assert await owner.fetchval(
                    "SELECT has_function_privilege($1, $2, 'EXECUTE')",
                    runtime_names.versioned,
                    f"public.{signature}",
                )
            assert await _explicit_function_grants(
                owner,
                runtime_names.stable,
            ) == set(runtime_cli.WORKER_FUNCTIONS)
            assert await _effective_security_definer_functions(
                owner,
                runtime_names.stable,
            ) == set(runtime_cli.WORKER_FUNCTIONS)
            assert await _effective_security_definer_functions(
                owner,
                runtime_names.versioned,
            ) == set(runtime_cli.WORKER_FUNCTIONS)
            for purpose, signatures in control_cli.ROLE_FUNCTIONS.items():
                assert await _explicit_function_grants(
                    owner,
                    control_names.stable[purpose],
                ) == set(signatures)
                assert await _effective_security_definer_functions(
                    owner,
                    control_names.stable[purpose],
                ) == set(signatures)
                assert await _effective_security_definer_functions(
                    owner,
                    control_names.versioned[purpose],
                ) == set(signatures)
            for login_principal in login_principals:
                assert await _explicit_function_grants(
                    owner,
                    login_principal,
                ) == set()
                assert await _explicit_table_grants(
                    owner,
                    login_principal,
                ) == set()
                assert await _explicit_column_grants(
                    owner,
                    login_principal,
                ) == set()

            runtime_column_grants = {
                (table_name, column, "SELECT")
                for table_name, columns in (
                    runtime_cli.WORKER_SELECT_COLUMNS.items()
                )
                for column in columns
            }
            assert await _explicit_table_grants(
                owner,
                runtime_names.stable,
            ) == set()
            assert await _explicit_column_grants(
                owner,
                runtime_names.stable,
            ) == runtime_column_grants

            operator_role = control_names.stable["operator"]
            assert await _explicit_table_grants(
                owner,
                operator_role,
            ) == set()
            assert await _explicit_column_grants(
                owner,
                operator_role,
            ) == set()

            janitor_role = control_names.stable["staging_janitor"]
            assert await _explicit_table_grants(
                owner,
                janitor_role,
            ) == set()
            assert await _explicit_column_grants(
                owner,
                janitor_role,
            ) == {
                ("artifacts", "storage_path", "SELECT"),
                (
                    "intermediate_artifact_cache",
                    "storage_path",
                    "SELECT",
                ),
            }

            orchestrator_role = control_names.stable["orchestrator"]
            assert await _explicit_table_grants(
                owner,
                orchestrator_role,
            ) == set()
            expected_orchestrator_columns: set[
                tuple[str, str, str]
            ] = set()
            for privilege, grants in (
                (
                    "SELECT",
                    control_cli.ORCHESTRATOR_AUTHORITY_SELECT_COLUMNS,
                ),
                ("SELECT", control_cli.ORCHESTRATOR_ENTITY_COLUMNS),
                ("INSERT", control_cli.ORCHESTRATOR_INSERT_COLUMNS),
                ("UPDATE", control_cli.ORCHESTRATOR_UPDATE_COLUMNS),
            ):
                expected_orchestrator_columns.update(
                    (table_name, column, privilege)
                    for table_name, columns in grants.items()
                    for column in columns
                )
            assert await _explicit_column_grants(
                owner,
                orchestrator_role,
            ) == expected_orchestrator_columns

            assert not await owner.fetchval(
                "SELECT has_function_privilege("
                "$1, 'public.vp_worker_grant_activate(text,bigint)', "
                "'EXECUTE')",
                runtime_names.versioned,
            )
            assert not await owner.fetchval(
                "SELECT has_function_privilege("
                "$1, 'public.vp_worker_register("
                "text,bigint,text,text,uuid,integer,text,jsonb,text,text,"
                "text,text,jsonb,text,text,text,text,text)', 'EXECUTE')",
                control_names.versioned["orchestrator"],
            )
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await orchestrator.fetchval(
                    "SELECT public.vp_require_worker_lease("
                    "$1::uuid, $2::bigint)",
                    uuid.uuid4(),
                    1,
                )

            for connection, query, arguments in (
                (
                    operator,
                    "SELECT public.vp_observe_worker_lease("
                    "$1::uuid, $2::bigint)",
                    (uuid.uuid4(), 1),
                ),
                (
                    janitor,
                    "SELECT public.vp_staging_janitor_readiness($1, $2)",
                    (900, 600),
                ),
                (
                    runtime,
                    "SELECT public.vp_begin_staging_janitor_run("
                    "$1::uuid, $2, $3)",
                    (uuid.uuid4(), "runtime-denied", 600),
                ),
            ):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await connection.fetchval(query, *arguments)
            for connection in (
                runtime,
                operator,
                orchestrator,
                janitor,
            ):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await connection.execute(
                        "CREATE TEMP TABLE worker_role_temp_escape "
                        "(id integer)"
                    )

            await runtime.fetch("SELECT id, status FROM public.jobs LIMIT 0")
            for statement in (
                "SELECT * FROM public.worker_admission_grants",
                "SELECT * FROM public.worker_registrations",
                "SELECT * FROM public.worker_task_dispatches",
                "SELECT * FROM public.runtime_schedules",
                "INSERT INTO public.artifacts DEFAULT VALUES",
                "UPDATE public.artifacts SET kind = kind WHERE false",
                "DELETE FROM public.artifacts WHERE false",
                "TRUNCATE public.artifacts",
                "INSERT INTO public.runtime_schedules DEFAULT VALUES",
                "UPDATE public.runtime_schedules "
                "SET state = state WHERE false",
                "INSERT INTO public.youtube_upload_operations "
                "DEFAULT VALUES",
                "UPDATE public.youtube_upload_operations "
                "SET status = status WHERE false",
                "UPDATE public.node_executions "
                "SET status = status WHERE false",
                "INSERT INTO public.worker_event_emissions DEFAULT VALUES",
                "UPDATE public.worker_task_dispatches "
                "SET delivery_state = delivery_state WHERE false",
                "SELECT * FROM public.alembic_version",
                "CREATE TABLE public.worker_runtime_escape (id integer)",
            ):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await runtime.execute(statement)
            await owner.execute(
                "CREATE SEQUENCE public.worker_role_probe_sequence"
            )
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await runtime.fetchval(
                    "SELECT pg_catalog.nextval($1::regclass)",
                    "public.worker_role_probe_sequence",
                )

            for table_name in (
                "worker_admission_grants",
                "worker_registrations",
            ):
                for statement in (
                    f"SELECT * FROM public.{table_name}",
                    f"INSERT INTO public.{table_name} DEFAULT VALUES",
                    f"UPDATE public.{table_name} "
                    "SET service_name = service_name WHERE false",
                    f"DELETE FROM public.{table_name} WHERE false",
                    f"TRUNCATE public.{table_name}",
                ):
                    with pytest.raises(
                        asyncpg.InsufficientPrivilegeError
                    ):
                        await operator.execute(statement)

            await janitor.fetch(
                "SELECT storage_path FROM public.artifacts LIMIT 0"
            )
            await janitor.fetch(
                "SELECT storage_path "
                "FROM public.intermediate_artifact_cache LIMIT 0"
            )
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await janitor.fetch(
                    "SELECT id FROM public.artifacts LIMIT 0"
                )
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await janitor.fetch(
                    "SELECT * FROM public.staging_janitor_status"
                )
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await janitor.execute(
                    "UPDATE public.artifacts "
                    "SET storage_path = storage_path WHERE false"
                )

            await orchestrator.fetch(
                "SELECT * FROM public.worker_task_dispatches LIMIT 0"
            )
            await orchestrator.execute(
                "UPDATE public.jobs SET status = 'RUNNING' WHERE false"
            )
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await orchestrator.execute(
                    "UPDATE public.jobs "
                    "SET pipeline_snapshot = pipeline_snapshot WHERE false"
                )
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await orchestrator.execute(
                    "DELETE FROM public.worker_task_dispatches WHERE false"
                )
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await orchestrator.execute(
                    "TRUNCATE public.worker_task_dispatches"
                )
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await orchestrator.fetch(
                    "SELECT * FROM public.worker_registrations"
                )
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await orchestrator.execute(
                    f'SET ROLE "{runtime_names.stable}"'
                )

            assert await runtime_cli.run(
                [
                    "revoke",
                    "--service-name",
                    service_name,
                    "--generation",
                    str(worker_generation),
                    "--state-dir",
                    str(runtime_state),
                ]
            ) == 4
            premature_revoke_output = capsys.readouterr().out
            assert json.loads(premature_revoke_output) == {
                "code": "worker_runtime_role_operation_failed",
                "status": "error",
            }
            assert admission_token not in premature_revoke_output
            assert runtime_url not in premature_revoke_output
            assert all(path.exists() for path in runtime_paths.values())
            assert await owner.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_roles
                    WHERE rolname = $1
                )
                """,
                runtime_names.versioned,
            )

            assert int(await owner.fetchval("SHOW server_version_num")) >= 160000
            fingerprints = await owner.fetchrow(
                "SELECT * FROM public.vp_worker_endpoint_fingerprints("
                "$1::jsonb)",
                json.dumps(request["endpoint_bindings"]),
            )
            assert fingerprints is not None
            worker_instance_id = uuid.uuid4()
            redis_consumer_id = "ffmpeg-go-worker:role-proof"
            lease_secret_sha256 = hashlib.sha256(
                b"role-proof-lease-secret"
            ).hexdigest()
            registration = await runtime.fetchrow(
                """
                SELECT *
                FROM public.vp_worker_register(
                    $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10,
                    $11, $12, $13::jsonb, $14, $15, $16, $17, $18
                )
                """,
                service_name,
                worker_generation,
                request["worker_type"],
                request["worker_host"],
                worker_instance_id,
                1,
                redis_consumer_id,
                json.dumps(request["capabilities"]),
                request["release_commit"],
                request["image_identity"],
                request["redis_stream"],
                request["redis_group"],
                json.dumps(request["endpoint_bindings"]),
                fingerprints["database_fingerprint"],
                fingerprints["redis_fingerprint"],
                fingerprints["storage_fingerprint"],
                request["token_sha256"],
                lease_secret_sha256,
            )
            assert registration is not None

            pipeline_id = uuid.uuid4()
            job_id = uuid.uuid4()
            node_execution_id = uuid.uuid4()
            channel_id = uuid.uuid4()
            production_task_id = uuid.uuid4()
            target_account_id = uuid.uuid4()
            dispatch_key = uuid.uuid4()
            task_message_id = "1710000000000-41"
            pipeline_snapshot = _pipeline_snapshot()
            task_payload = {
                "config": "{}",
                "dispatch_key": str(dispatch_key),
                "input_artifacts": "{}",
                "job_id": str(job_id),
                "node_execution_id": str(node_execution_id),
                "node_id": "publish",
                "node_type": "youtube_upload",
            }
            task_payload_sha256 = hashlib.sha256(
                json.dumps(
                    task_payload,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            await owner.execute(
                """
                INSERT INTO public.channel_profiles (
                    id, name, positioning, language, default_aspect_ratio,
                    risk_policy_json, content_mix_policy_json,
                    cadence_policy_json, alert_policy_json, enabled, dry_run,
                    config_version, tick_interval_minutes, created_at,
                    updated_at
                ) VALUES (
                    $1, 'role proof channel', '', 'en', '9:16',
                    '{}'::json, '{}'::json, '{}'::json, '{}'::json,
                    TRUE, TRUE, 1, 60, clock_timestamp(), clock_timestamp()
                )
                """,
                channel_id,
            )
            await owner.execute(
                """
                INSERT INTO public.pipelines (id, name, definition)
                VALUES ($1, 'role-proof-pipeline', $2::json)
                """,
                pipeline_id,
                json.dumps(pipeline_snapshot),
            )
            await owner.execute(
                """
                INSERT INTO public.jobs (
                    id, pipeline_id, pipeline_snapshot, status,
                    execution_plan, submitted_by, orchestrator_owner
                ) VALUES (
                    $1, $2, $3::json, 'RUNNING'::job_status,
                    $4::json, 'role-proof', 'python'
                )
                """,
                job_id,
                pipeline_id,
                json.dumps(pipeline_snapshot),
                json.dumps({"dependencies": {"publish": []}}),
            )
            await owner.execute(
                """
                INSERT INTO public.production_tasks (
                    id, channel_profile_id, target_account_id, source,
                    title_seed, prompt, rationale_json, score_breakdown_json,
                    portfolio_bucket, source_platforms_json,
                    material_library_ids_json, uses_external_assets,
                    approval_mode, agent_approval_evidence_json,
                    human_review_evidence_json, pipeline_id, job_id, priority,
                    state, retry_count, channel_config_version_snapshot,
                    channel_config_snapshot_json, transition_history_json,
                    created_at, updated_at
                ) VALUES (
                    $1, $2, $3, 'manual_seed', 'role proof',
                    'restricted role proof', '{}'::json, '{}'::json,
                    'explore', '[]'::json, '[]'::json, FALSE, 'human',
                    '{}'::json, '{"approved":true}'::json, $4, $5, 1,
                    'producing', 0, 1, '{}'::json, '[]'::json,
                    clock_timestamp(), clock_timestamp()
                )
                """,
                production_task_id,
                channel_id,
                target_account_id,
                pipeline_id,
                job_id,
            )
            await owner.execute(
                """
                INSERT INTO public.node_executions (
                    id, job_id, node_id, node_type, node_label, node_config,
                    status, input_artifact_ids
                ) VALUES (
                    $1, $2, 'publish', 'youtube_upload', 'Publish',
                    '{}'::json, 'QUEUED'::node_status, ARRAY[]::uuid[]
                )
                """,
                node_execution_id,
                job_id,
            )
            await owner.execute(
                """
                INSERT INTO public.worker_task_dispatches (
                    dispatch_key, job_id, node_execution_id, redis_stream,
                    consumer_group, payload_sha256, payload_json,
                    delivery_state, delivery_attempted_at, redis_message_id,
                    delivered_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7::jsonb, 'delivered',
                    clock_timestamp(), $8, clock_timestamp()
                )
                """,
                dispatch_key,
                job_id,
                node_execution_id,
                request["redis_stream"],
                request["redis_group"],
                task_payload_sha256,
                json.dumps(task_payload),
                task_message_id,
            )

            worker_engine = create_async_engine(
                runtime_url.replace(
                    "postgresql://",
                    "postgresql+asyncpg://",
                    1,
                )
            )
            worker_sessions = async_sessionmaker(
                worker_engine,
                expire_on_commit=False,
            )
            orchestrator_engine = create_async_engine(
                role_urls["orchestrator"].replace(
                    "postgresql://",
                    "postgresql+asyncpg://",
                    1,
                )
            )
            orchestrator_sessions = async_sessionmaker(
                orchestrator_engine,
                expire_on_commit=False,
            )
            delivery = worker_main.WorkerTaskDelivery(
                redis_stream=str(request["redis_stream"]),
                consumer_group=str(request["redis_group"]),
                message_id=task_message_id,
                payload_sha256=task_payload_sha256,
                dispatch_key=dispatch_key,
            )
            delivery_token = worker_main._current_task_delivery.set(delivery)
            monkeypatch.setattr(
                worker_main,
                "get_worker_session",
                lambda: worker_sessions,
            )

            class RedisProof:
                def __init__(self) -> None:
                    self.eval_calls: list[tuple[object, ...]] = []
                    self.xack_calls: list[tuple[str, str, str]] = []

                async def eval(self, *arguments: object) -> str:
                    self.eval_calls.append(arguments)
                    return "1710000001000-41"

                async def xack(
                    self,
                    stream: str,
                    group: str,
                    message_id: str,
                ) -> int:
                    self.xack_calls.append((stream, group, message_id))
                    return 1

            redis_proof = RedisProof()
            try:
                claim = await worker_main._claim_node_execution(
                    str(job_id),
                    str(node_execution_id),
                    worker_lease=SimpleNamespace(
                        registration_id=registration["registration_id"],
                        lease_epoch=registration["lease_epoch"],
                        redis_consumer_id=redis_consumer_id,
                    ),
                    session_factory=worker_sessions,
                )
                assert claim is not None
                assert isinstance(claim.started_at, datetime)
                assert isinstance(delivery.attestation_id, uuid.UUID)

                artifact_id = uuid.UUID(
                    await worker_main._persist_artifact_for_current_claim(
                        claim,
                        filename="role-proof.mp4",
                        mime_type="video/mp4",
                        file_size=128,
                        storage_backend="minio",
                        storage_path=(
                            f"staging/artifacts/{job_id}/"
                            f"{node_execution_id}-0123456789abcdef.mp4"
                        ),
                        media_info={"duration": 1.0},
                        session_factory=worker_sessions,
                    )
                )
                await owner.execute(
                    """
                    UPDATE public.node_executions
                    SET input_artifact_ids = ARRAY[$2]::uuid[]
                    WHERE id = $1
                    """,
                    node_execution_id,
                    artifact_id,
                )

                upload_context = UploadOperationContext(
                    job_id=job_id,
                    node_execution_id=node_execution_id,
                    execution_claim=claim,
                    input_artifact_id=artifact_id,
                    content_sha256="a" * 64,
                    title="Restricted role proof",
                    privacy="unlisted",
                )
                upload_store = YouTubeUploadOperationStore(worker_sessions)
                upload_claim = await upload_store.claim(upload_context)
                assert upload_claim.action == "submit"
                manager_task_id = str(uuid.uuid4())
                async with upload_store.submission_fence(upload_context):
                    attempting = await upload_store.mark_attempting(
                        upload_claim.operation.id,
                        context=upload_context,
                    )
                    assert attempting.request_attempted_at is not None
                    submitted = await upload_store.mark_submitted(
                        upload_claim.operation.id,
                        manager_task_id,
                        context=upload_context,
                    )
                    assert submitted.status == "submitted"
                succeeded = await upload_store.mark_succeeded(
                    upload_claim.operation.id,
                    "role-proof-video",
                    {
                        "title": "Restricted role proof",
                        "privacy": "unlisted",
                    },
                    context=upload_context,
                )
                assert succeeded.status == "succeeded"

                event_payload = {
                    "event": "node_completed",
                    "job_id": str(job_id),
                    "node_execution_id": str(node_execution_id),
                    "output_artifact_id": str(artifact_id),
                    "worker_id": claim.worker_id,
                    "started_at": worker_main._claim_started_at_utc(claim),
                    "worker_registration_id": str(
                        claim.worker_registration_id
                    ),
                    "worker_lease_epoch": str(claim.worker_lease_epoch),
                }
                worker_main._bind_registered_event_to_task_delivery(
                    event_payload
                )
                emission_id = await worker_main._xadd_event_for_claim(
                    redis_proof,
                    event_payload,
                    claim,
                )
                assert isinstance(emission_id, uuid.UUID)
                assert len(redis_proof.eval_calls) == 1

                event = parse_registered_worker_event(
                    redis_stream="vp:events",
                    consumer_group="orchestrator",
                    message_id="1710000001000-41",
                    payload=event_payload,
                )
                receipt_service = RegisteredWorkerEventReceiptService(
                    orchestrator_sessions
                )
                receipt_id = await receipt_service.accept_and_apply(
                    event,
                    JobEngine().apply_registered_worker_event,
                )
                assert isinstance(receipt_id, uuid.UUID)
                await receipt_service.acknowledge_applied(
                    redis_proof,
                    event,
                )
            finally:
                worker_main._current_task_delivery.reset(delivery_token)
                await worker_engine.dispose()
                await orchestrator_engine.dispose()

            applied = await owner.fetchrow(
                """
                SELECT
                    job.status::text AS job_status,
                    node.status::text AS node_status,
                    artifact.kind::text AS artifact_kind,
                    operation.status AS upload_status,
                    emission.emission_state,
                    attestation.ack_state AS task_ack_state,
                    receipt.application_state,
                    receipt.ack_state AS event_ack_state,
                    delivery.ack_state AS delivery_ack_state,
                    dispatch.resolution_state
                FROM public.jobs AS job
                JOIN public.node_executions AS node
                  ON node.job_id = job.id
                JOIN public.artifacts AS artifact
                  ON artifact.id = node.output_artifact_id
                JOIN public.youtube_upload_operations AS operation
                  ON operation.node_execution_id = node.id
                JOIN public.worker_task_delivery_attestations AS attestation
                  ON attestation.node_execution_id = node.id
                JOIN public.worker_event_emissions AS emission
                  ON emission.source_task_attestation_id = attestation.id
                JOIN public.registered_worker_event_receipts AS receipt
                  ON receipt.source_task_attestation_id = attestation.id
                JOIN public.registered_worker_event_deliveries AS delivery
                  ON delivery.receipt_id = receipt.id
                JOIN public.worker_task_dispatches AS dispatch
                  ON dispatch.dispatch_key = attestation.dispatch_key
                WHERE job.id = $1
                """,
                job_id,
            )
            assert applied is not None
            assert dict(applied) == {
                "job_status": "SUCCEEDED",
                "node_status": "SUCCEEDED",
                "artifact_kind": "FINAL",
                "upload_status": "succeeded",
                "emission_state": "resolved",
                "task_ack_state": "acknowledged",
                "application_state": "applied",
                "event_ack_state": "acknowledged",
                "delivery_ack_state": "acknowledged",
                "resolution_state": "acknowledged",
            }

            await orchestrator.execute(
                "SELECT public.vp_observe_worker_lease($1, $2)",
                registration["registration_id"],
                registration["lease_epoch"],
            )
            observer_transaction = orchestrator.transaction()
            await observer_transaction.start()
            await orchestrator.execute(
                "SELECT public.vp_observe_worker_lease($1, $2)",
                registration["registration_id"],
                registration["lease_epoch"],
            )
            revoke_task = asyncio.create_task(
                operator_cli.run(
                    [
                        "revoke-grant",
                        "--service-name",
                        service_name,
                        "--generation",
                        str(worker_generation),
                        "--reason",
                        "role-proof-retirement",
                    ]
                )
            )
            done, _pending = await asyncio.wait(
                {revoke_task},
                timeout=0.2,
            )
            assert revoke_task not in done
            await observer_transaction.commit()
            assert await asyncio.wait_for(revoke_task, timeout=3) == 0
            revoke_output = capsys.readouterr().out
            assert admission_token not in revoke_output
            assert runtime_url not in revoke_output

            retirement_fence = operator.transaction()
            await retirement_fence.start()
            await operator.execute(
                """
                SELECT pg_catalog.pg_advisory_xact_lock(
                    pg_catalog.hashtextextended(
                        'vp-worker-service:' || $1,
                        0
                    )
                )
                """,
                service_name,
            )
            role_retirement = asyncio.create_task(
                runtime_cli.run(
                    [
                        "revoke",
                        "--service-name",
                        service_name,
                        "--generation",
                        str(worker_generation),
                        "--state-dir",
                        str(runtime_state),
                    ]
                )
            )
            done, _pending = await asyncio.wait(
                {role_retirement},
                timeout=0.2,
            )
            assert role_retirement not in done
            await retirement_fence.commit()
            assert await asyncio.wait_for(role_retirement, timeout=3) == 0
            retirement_output = capsys.readouterr().out
            assert admission_token not in retirement_output
            assert runtime_url not in retirement_output
        finally:
            await runtime.close()
            await operator.close()
            await orchestrator.close()
            await janitor.close()
            await owner.close()

        assert await operator_cli.run(
            [
                "revoke-grant",
                "--service-name",
                service_name,
                "--generation",
                str(worker_generation),
                "--reason",
                "test_cleanup",
            ]
        ) == 0
        capsys.readouterr()
        assert await runtime_cli.run(
            [
                "revoke",
                "--service-name",
                service_name,
                "--generation",
                str(worker_generation),
                "--state-dir",
                str(runtime_state),
            ]
        ) == 0
        assert await control_cli.run(
            [
                "revoke",
                "--generation",
                control_generation,
                "--state-dir",
                str(control_state),
            ]
        ) == 0
        assert await control_cli.run(
            [
                "revoke",
                "--generation",
                control_generation,
                "--state-dir",
                str(control_state),
            ]
        ) == 0
        capsys.readouterr()
    finally:
        admin = await asyncpg.connect(_asyncpg_url(admin_url))
        try:
            await admin.execute(
                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'
            )
            for role_name in (
                rogue_principal,
                leak_target,
                leak_caller,
                migration_drift_owner,
                migration_acl_probe,
                runtime_names.versioned,
                *control_names.versioned.values(),
                runtime_names.stable,
                *control_names.stable.values(),
            ):
                await admin.execute(
                    f'DROP ROLE IF EXISTS "{role_name}"'
                )
        finally:
            await admin.close()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for live role tests",
)
async def test_role_lifecycle_serializes_database_and_state_race_orders(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = f"vp_worker_role_races_{uuid.uuid4().hex[:20]}"
    service_name = f"task4a-race-{uuid.uuid4().hex[:10]}"
    worker_generation = int(uuid.uuid4().hex[:12], 16)
    control_generation = f"race-{uuid.uuid4().hex[:16]}"
    runtime_names = runtime_cli.role_names_for_generation(
        service_name,
        worker_generation,
    )
    control_names = control_cli.role_names_for_generation(
        control_generation
    )
    admin_url = _database_url("postgres")
    target_url = _database_url(database)
    runtime_state = tmp_path / "runtime-state"
    control_state = tmp_path / "control-state"
    publish_release = threading.Event()
    removal_release = threading.Event()

    admin = await asyncpg.connect(_asyncpg_url(admin_url))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()

    try:
        migrated = _run_alembic(_alembic_url(target_url))
        assert migrated.returncode == 0, migrated.stdout + migrated.stderr

        publish_entered = threading.Event()
        real_runtime_write = runtime_cli.write_generation_state

        def paused_runtime_write(*args: object, **kwargs: object) -> None:
            publish_entered.set()
            if not publish_release.wait(timeout=10):
                raise AssertionError("runtime state publish was not released")
            real_runtime_write(*args, **kwargs)

        monkeypatch.setattr(
            runtime_cli,
            "write_generation_state",
            paused_runtime_write,
        )
        provision_first = asyncio.create_task(
            asyncio.to_thread(
                lambda: asyncio.run(
                    runtime_cli._provision(
                        target_url,
                        service_name,
                        worker_generation,
                        runtime_state,
                        runtime_names,
                    )
                )
            )
        )
        assert await asyncio.to_thread(publish_entered.wait, 10)
        revoke_second = asyncio.create_task(
            runtime_cli._revoke(
                target_url,
                service_name,
                worker_generation,
                runtime_state,
                runtime_names,
            )
        )
        await asyncio.sleep(0.2)
        revoke_waited_for_publish = not revoke_second.done()
        publish_release.set()
        runtime_results = await asyncio.gather(
            provision_first,
            revoke_second,
            return_exceptions=True,
        )
        assert revoke_waited_for_publish
        assert not any(
            isinstance(result, BaseException)
            for result in runtime_results
        )
        runtime_owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert not await runtime_owner.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = $1
                )
                """,
                runtime_names.versioned,
            )
        finally:
            await runtime_owner.close()
        assert not any(
            path.exists()
            for path in runtime_cli.credential_paths(
                runtime_state,
                service_name,
                worker_generation,
            ).values()
        )

        monkeypatch.setattr(
            runtime_cli,
            "write_generation_state",
            real_runtime_write,
        )
        orphan_owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            async with orphan_owner.transaction():
                await role_common.create_login_role(
                    orphan_owner,
                    runtime_names.versioned,
                    "interrupted-task4a-password",
                    setting_prefix="task4a_interrupted",
                    stable_role=runtime_names.stable,
                )
                await orphan_owner.execute(
                    f'GRANT "{runtime_names.stable}" '
                    f'TO "{runtime_names.versioned}"'
                )
        finally:
            await orphan_owner.close()
        partial_paths = runtime_cli.credential_paths(
            runtime_state,
            service_name,
            worker_generation,
        )
        role_common.write_secure_files(
            runtime_state,
            (service_name, str(worker_generation)),
            {
                runtime_cli.STATE_FILENAMES["database_url"]: (
                    "postgresql://interrupted:state@invalid/partial\n"
                )
            },
            file_mode=0o400,
        )
        await runtime_cli._provision(
            target_url,
            service_name,
            worker_generation,
            runtime_state,
            runtime_names,
        )
        assert all(path.exists() for path in partial_paths.values())
        recovered_url = partial_paths["database_url"].read_text().strip()
        recovered = await asyncpg.connect(_asyncpg_url(recovered_url))
        try:
            assert await recovered.fetchval(
                "SELECT session_user"
            ) == runtime_names.versioned
        finally:
            await recovered.close()
        await runtime_cli._revoke(
            target_url,
            service_name,
            worker_generation,
            runtime_state,
            runtime_names,
        )

        await control_cli._provision(
            target_url,
            control_generation,
            control_state,
            control_names,
        )
        removal_entered = threading.Event()
        real_control_remove = control_cli.remove_secure_files

        def paused_control_remove(*args: object, **kwargs: object) -> None:
            removal_entered.set()
            if not removal_release.wait(timeout=10):
                raise AssertionError("control state removal was not released")
            real_control_remove(*args, **kwargs)

        monkeypatch.setattr(
            control_cli,
            "remove_secure_files",
            paused_control_remove,
        )
        revoke_first = asyncio.create_task(
            asyncio.to_thread(
                lambda: asyncio.run(
                    control_cli._revoke(
                        target_url,
                        control_generation,
                        control_state,
                        control_names,
                    )
                )
            )
        )
        assert await asyncio.to_thread(removal_entered.wait, 10)
        provision_second = asyncio.create_task(
            control_cli._provision(
                target_url,
                control_generation,
                control_state,
                control_names,
            )
        )
        await asyncio.sleep(0.2)
        provision_waited_for_removal = not provision_second.done()
        removal_release.set()
        control_results = await asyncio.gather(
            revoke_first,
            provision_second,
            return_exceptions=True,
        )
        assert provision_waited_for_removal
        assert not any(
            isinstance(result, BaseException)
            for result in control_results
        )
        control_owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert await control_owner.fetchval(
                """
                SELECT pg_catalog.count(*) = $2
                FROM pg_catalog.pg_roles
                WHERE rolname = ANY($1::text[])
                """,
                list(control_names.versioned.values()),
                len(control_names.versioned),
            )
        finally:
            await control_owner.close()
        assert all(
            path.exists()
            for path in control_cli.credential_paths(
                control_state,
                control_generation,
            ).values()
        )
    finally:
        publish_release.set()
        removal_release.set()
        admin = await asyncpg.connect(_asyncpg_url(admin_url))
        try:
            await admin.execute(
                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'
            )
            for role_name in (
                runtime_names.versioned,
                *control_names.versioned.values(),
                runtime_names.stable,
                *control_names.stable.values(),
            ):
                await admin.execute(f'DROP ROLE IF EXISTS "{role_name}"')
        finally:
            await admin.close()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for live role tests",
)
async def test_runtime_recovery_serializes_with_operator_authority_both_orders(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = f"vp_worker_authority_race_{uuid.uuid4().hex[:16]}"
    service_name = f"task4a-authority-{uuid.uuid4().hex[:8]}"
    generations = (
        int(uuid.uuid4().hex[:12], 16),
        int(uuid.uuid4().hex[:12], 16),
    )
    runtime_names = tuple(
        runtime_cli.role_names_for_generation(service_name, generation)
        for generation in generations
    )
    control_generation = f"authority-{uuid.uuid4().hex[:12]}"
    control_names = control_cli.role_names_for_generation(
        control_generation
    )
    admin_url = _database_url("postgres")
    target_url = _database_url(database)
    runtime_state = tmp_path / "runtime-state"
    control_state = tmp_path / "control-state"
    publish_release = threading.Event()

    admin = await asyncpg.connect(_asyncpg_url(admin_url))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()

    try:
        migrated = _run_alembic(_alembic_url(target_url))
        assert migrated.returncode == 0, migrated.stdout + migrated.stderr
        await control_cli._provision(
            target_url,
            control_generation,
            control_state,
            control_names,
        )
        operator_url = control_cli.credential_paths(
            control_state,
            control_generation,
        )["operator"].read_text().strip()

        first_generation = generations[0]
        first_names = runtime_names[0]
        await runtime_cli._provision(
            target_url,
            service_name,
            first_generation,
            runtime_state,
            first_names,
        )
        first_paths = runtime_cli.credential_paths(
            runtime_state,
            service_name,
            first_generation,
        )
        first_token = first_paths["admission_token"].read_text().strip()
        first_paths["state"].unlink()

        publish_entered = threading.Event()
        real_write = runtime_cli.write_generation_state

        def paused_write(*args: object, **kwargs: object) -> None:
            publish_entered.set()
            if not publish_release.wait(timeout=10):
                raise AssertionError("runtime recovery was not released")
            real_write(*args, **kwargs)

        monkeypatch.setattr(
            runtime_cli,
            "write_generation_state",
            paused_write,
        )
        recovery_first = asyncio.create_task(
            asyncio.to_thread(
                lambda: asyncio.run(
                    runtime_cli._provision(
                        target_url,
                        service_name,
                        first_generation,
                        runtime_state,
                        first_names,
                    )
                )
            )
        )
        assert await asyncio.to_thread(publish_entered.wait, 10)

        operator = await asyncpg.connect(_asyncpg_url(operator_url))
        try:
            async def activate_first_token() -> None:
                async with operator.transaction():
                    await _upsert_worker_grant(
                        operator,
                        service_name=service_name,
                        generation=first_generation,
                        database_principal=first_names.versioned,
                        admission_token=first_token,
                    )
                    await operator.fetchval(
                        "SELECT public.vp_worker_grant_activate($1, $2)",
                        service_name,
                        first_generation,
                    )

            activation_second = asyncio.create_task(
                activate_first_token()
            )
            await asyncio.sleep(0.2)
            assert not activation_second.done()
            publish_release.set()
            await asyncio.wait_for(recovery_first, timeout=5)
            await asyncio.wait_for(activation_second, timeout=5)
            assert (
                first_paths["admission_token"].read_text().strip()
                == first_token
            )

            monkeypatch.setattr(
                runtime_cli,
                "write_generation_state",
                real_write,
            )
            second_generation = generations[1]
            second_names = runtime_names[1]
            await runtime_cli._provision(
                target_url,
                service_name,
                second_generation,
                runtime_state,
                second_names,
            )
            second_paths = runtime_cli.credential_paths(
                runtime_state,
                service_name,
                second_generation,
            )
            second_token = (
                second_paths["admission_token"].read_text().strip()
            )
            second_paths["state"].unlink()

            operator_first = operator.transaction()
            await operator_first.start()
            await _upsert_worker_grant(
                operator,
                service_name=service_name,
                generation=second_generation,
                database_principal=second_names.versioned,
                admission_token=second_token,
            )
            recovery_second = asyncio.create_task(
                runtime_cli._provision(
                    target_url,
                    service_name,
                    second_generation,
                    runtime_state,
                    second_names,
                )
            )
            await asyncio.sleep(0.2)
            assert not recovery_second.done()
            await operator.fetchval(
                "SELECT public.vp_worker_grant_activate($1, $2)",
                service_name,
                second_generation,
            )
            await operator_first.commit()
            await asyncio.wait_for(recovery_second, timeout=5)

            owner = await asyncpg.connect(_asyncpg_url(target_url))
            try:
                active = await owner.fetchrow(
                    """
                    SELECT generation, token_sha256
                    FROM public.worker_admission_grants
                    WHERE service_name = $1 AND state = 'active'
                    """,
                    service_name,
                )
                assert active is not None
                assert active["generation"] == second_generation
                assert active["token_sha256"] == hashlib.sha256(
                    second_paths["admission_token"]
                    .read_text()
                    .strip()
                    .encode()
                ).hexdigest()
            finally:
                await owner.close()
        finally:
            await operator.close()
    finally:
        publish_release.set()
        admin = await asyncpg.connect(_asyncpg_url(admin_url))
        try:
            await admin.execute(
                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'
            )
            for role_name in (
                *(names.versioned for names in runtime_names),
                *control_names.versioned.values(),
                runtime_names[0].stable,
                *control_names.stable.values(),
            ):
                await admin.execute(f'DROP ROLE IF EXISTS "{role_name}"')
        finally:
            await admin.close()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for live role tests",
)
async def test_generation_overlap_requires_valid_immutable_state_proof(
    tmp_path: Path,
) -> None:
    database = f"vp_worker_overlap_{uuid.uuid4().hex[:20]}"
    service_name = f"task4a-overlap-{uuid.uuid4().hex[:8]}"
    runtime_generations = (
        int(uuid.uuid4().hex[:12], 16),
        int(uuid.uuid4().hex[:12], 16),
        int(uuid.uuid4().hex[:12], 16),
    )
    runtime_names = tuple(
        runtime_cli.role_names_for_generation(service_name, generation)
        for generation in runtime_generations
    )
    control_generations = (
        f"overlap-a-{uuid.uuid4().hex[:8]}",
        f"overlap-b-{uuid.uuid4().hex[:8]}",
    )
    control_names = tuple(
        control_cli.role_names_for_generation(generation)
        for generation in control_generations
    )
    forged_runtime = f"vp_forged_{uuid.uuid4().hex[:16]}"
    forged_control = f"vp_forged_{uuid.uuid4().hex[:16]}"
    admin_url = _database_url("postgres")
    target_url = _database_url(database)
    runtime_state = tmp_path / "runtime-state"
    control_state = tmp_path / "control-state"

    admin = await asyncpg.connect(_asyncpg_url(admin_url))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()

    try:
        migrated = _run_alembic(_alembic_url(target_url))
        assert migrated.returncode == 0, migrated.stdout + migrated.stderr
        for generation, names in zip(
            runtime_generations[:2],
            runtime_names[:2],
            strict=True,
        ):
            await runtime_cli._provision(
                target_url,
                service_name,
                generation,
                runtime_state,
                names,
            )
        for generation, names in zip(
            control_generations,
            control_names,
            strict=True,
        ):
            await control_cli._provision(
                target_url,
                generation,
                control_state,
                names,
            )

        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            stale_names = runtime_names[2]
            async with owner.transaction():
                await role_common.create_login_role(
                    owner,
                    stale_names.versioned,
                    "task4a-stale-actual-password",
                    setting_prefix="task4a_stale",
                    stable_role=stale_names.stable,
                )
                await owner.execute(
                    f'GRANT "{stale_names.stable}" '
                    f'TO "{stale_names.versioned}"'
                )
                await role_common.create_login_role(
                    owner,
                    forged_runtime,
                    "task4a-forged-runtime-password",
                    setting_prefix="task4a_forged_runtime",
                    stable_role=runtime_names[0].stable,
                )
                await owner.execute(
                    f'GRANT "{runtime_names[0].stable}" '
                    f'TO "{forged_runtime}"'
                )
                await role_common.create_login_role(
                    owner,
                    forged_control,
                    "task4a-forged-control-password",
                    setting_prefix="task4a_forged_control",
                    stable_role=control_names[0].stable["operator"],
                )
                await owner.execute(
                    f'GRANT "{control_names[0].stable["operator"]}" '
                    f'TO "{forged_control}"'
                )
            stale_url = role_common.role_database_url(
                target_url,
                stale_names.versioned,
                "task4a-wrong-stale-password",
            )
            runtime_cli.write_generation_state(
                runtime_state,
                service_name,
                runtime_generations[2],
                stale_names,
                database_url=stale_url,
                admission_token="task4a-stale-admission-token",
            )
        finally:
            await owner.close()

        await runtime_cli._provision(
            target_url,
            service_name,
            runtime_generations[1],
            runtime_state,
            runtime_names[1],
        )
        await control_cli._provision(
            target_url,
            control_generations[1],
            control_state,
            control_names[1],
        )

        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            for names in runtime_names[:2]:
                assert await owner.fetchval(
                    "SELECT pg_catalog.pg_has_role($1, $2, 'MEMBER')",
                    names.versioned,
                    names.stable,
                )
            for names in control_names:
                for purpose, versioned_role in names.versioned.items():
                    assert await owner.fetchval(
                        "SELECT pg_catalog.pg_has_role($1, $2, 'MEMBER')",
                        versioned_role,
                        names.stable[purpose],
                    )
            for role_name, stable_role in (
                (runtime_names[2].versioned, runtime_names[2].stable),
                (forged_runtime, runtime_names[0].stable),
                (
                    forged_control,
                    control_names[0].stable["operator"],
                ),
            ):
                assert not await owner.fetchval(
                    "SELECT pg_catalog.pg_has_role($1, $2, 'MEMBER')",
                    role_name,
                    stable_role,
                )
        finally:
            await owner.close()
    finally:
        admin = await asyncpg.connect(_asyncpg_url(admin_url))
        try:
            await admin.execute(
                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'
            )
            for role_name in (
                *(names.versioned for names in runtime_names),
                *(
                    role_name
                    for names in control_names
                    for role_name in names.versioned.values()
                ),
                forged_runtime,
                forged_control,
                runtime_names[0].stable,
                *control_names[0].stable.values(),
            ):
                await admin.execute(f'DROP ROLE IF EXISTS "{role_name}"')
        finally:
            await admin.close()
