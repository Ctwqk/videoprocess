from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
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


def _run_alembic(
    database_url: str,
    revision: str = "head",
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", revision],
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


async def _role_membership_edges(
    connection: asyncpg.Connection,
    role_name: str,
) -> set[tuple[str, str]]:
    rows = await connection.fetch(
        """
        SELECT granted.rolname AS granted_role, member.rolname AS member_role
        FROM pg_catalog.pg_auth_members AS membership
        JOIN pg_catalog.pg_roles AS granted
          ON granted.oid = membership.roleid
        JOIN pg_catalog.pg_roles AS member
          ON member.oid = membership.member
        WHERE granted.rolname = $1 OR member.rolname = $1
        """,
        role_name,
    )
    return {
        (row["granted_role"], row["member_role"])
        for row in rows
    }


async def _role_membership_rows(
    connection: asyncpg.Connection,
    role_names: tuple[str, ...],
) -> list[dict[str, object]]:
    rows = await connection.fetch(
        """
        SELECT
            granted.rolname AS granted_role,
            member.rolname AS member_role,
            grantor.rolname AS grantor_role,
            membership.admin_option,
            membership.inherit_option,
            membership.set_option
        FROM pg_catalog.pg_auth_members AS membership
        JOIN pg_catalog.pg_roles AS granted
          ON granted.oid = membership.roleid
        JOIN pg_catalog.pg_roles AS member
          ON member.oid = membership.member
        JOIN pg_catalog.pg_roles AS grantor
          ON grantor.oid = membership.grantor
        WHERE granted.rolname = ANY($1::text[])
           OR member.rolname = ANY($1::text[])
           OR grantor.rolname = ANY($1::text[])
        ORDER BY
            granted.rolname,
            member.rolname,
            grantor.rolname
        """,
        list(role_names),
    )
    return [dict(row) for row in rows]


async def _drop_test_roles(
    connection: asyncpg.Connection,
    role_names: tuple[str, ...],
) -> None:
    existing = {
        row["rolname"]
        for row in await connection.fetch(
            """
            SELECT rolname
            FROM pg_catalog.pg_roles
            WHERE rolname = ANY($1::text[])
            """,
            list(role_names),
        )
    }
    memberships = await connection.fetch(
        """
        SELECT
            granted.rolname AS granted_role,
            member.rolname AS member_role,
            grantor.rolname AS grantor_role
        FROM pg_catalog.pg_auth_members AS membership
        JOIN pg_catalog.pg_roles AS granted
          ON granted.oid = membership.roleid
        JOIN pg_catalog.pg_roles AS member
          ON member.oid = membership.member
        JOIN pg_catalog.pg_roles AS grantor
          ON grantor.oid = membership.grantor
        WHERE granted.rolname = ANY($1::text[])
           OR member.rolname = ANY($1::text[])
           OR grantor.rolname = ANY($1::text[])
        ORDER BY
            (grantor.rolname = ANY($1::text[])) DESC,
            granted.rolname,
            member.rolname,
            grantor.rolname
        """,
        list(role_names),
    )
    for membership in memberships:
        await connection.execute(
            f"REVOKE "
            f"{role_common.quote_identifier(membership['granted_role'])} "
            f"FROM "
            f"{role_common.quote_identifier(membership['member_role'])} "
            f"GRANTED BY "
            f"{role_common.quote_identifier(membership['grantor_role'])} "
            "CASCADE"
        )
    for role_name in reversed(role_names):
        if role_name in existing:
            await connection.execute(
                f"DROP ROLE {role_common.quote_identifier(role_name)}"
            )


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
async def test_invalid_control_generations_quarantine_every_principal(
    tmp_path: Path,
) -> None:
    database = f"vp_control_quarantine_{uuid.uuid4().hex[:16]}"
    generations = (
        f"wrong-password-{uuid.uuid4().hex[:8]}",
        f"wrong-endpoint-{uuid.uuid4().hex[:8]}",
        f"wrong-mode-{uuid.uuid4().hex[:8]}",
        f"partial-{uuid.uuid4().hex[:8]}",
    )
    names = tuple(
        control_cli.role_names_for_generation(generation)
        for generation in generations
    )
    admin_url = _database_url("postgres")
    target_url = _database_url(database)
    control_state = tmp_path / "control-state"
    admin = await asyncpg.connect(_asyncpg_url(admin_url))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()

    original_urls: list[dict[str, str]] = []
    old_operator: asyncpg.Connection | None = None
    try:
        migrated = _run_alembic(_alembic_url(target_url))
        assert migrated.returncode == 0, migrated.stdout + migrated.stderr
        for generation, control_names in zip(
            generations,
            names,
            strict=True,
        ):
            await control_cli._provision(
                target_url,
                generation,
                control_state,
                control_names,
            )
            original_urls.append(
                {
                    purpose: path.read_text().strip()
                    for purpose, path in control_cli.credential_paths(
                        control_state,
                        generation,
                    ).items()
                }
            )

        old_operator = await asyncpg.connect(
            _asyncpg_url(original_urls[0]["operator"])
        )
        paths = tuple(
            control_cli.credential_paths(control_state, generation)
            for generation in generations
        )
        wrong_password = make_url(
            paths[0]["operator"].read_text().strip()
        ).set(password="task4a-round5-wrong-password")
        _write_secret(
            paths[0]["operator"],
            wrong_password.render_as_string(hide_password=False),
            0o400,
        )
        wrong_endpoint = make_url(
            paths[1]["orchestrator"].read_text().strip()
        ).set(host="127.0.0.2")
        _write_secret(
            paths[1]["orchestrator"],
            wrong_endpoint.render_as_string(hide_password=False),
            0o400,
        )
        paths[2]["staging_janitor"].chmod(0o600)
        paths[3]["orchestrator"].unlink()
        forensic_snapshots = tuple(
            {
                purpose: (
                    None
                    if not path.exists()
                    else (
                        path.read_bytes(),
                        stat.S_IMODE(path.stat().st_mode),
                    )
                )
                for purpose, path in generation_paths.items()
            }
            for generation_paths in paths
        )

        for generation, control_names, generation_paths, snapshot in zip(
            generations,
            names,
            paths,
            forensic_snapshots,
            strict=True,
        ):
            with pytest.raises(
                (
                    asyncpg.PostgresError,
                    OSError,
                    control_cli.ControlRoleError,
                    role_common.WorkerRoleCommonError,
                )
            ):
                await control_cli._provision(
                    target_url,
                    generation,
                    control_state,
                    control_names,
                )
            assert {
                purpose: (
                    None
                    if not path.exists()
                    else (
                        path.read_bytes(),
                        stat.S_IMODE(path.stat().st_mode),
                    )
                )
                for purpose, path in generation_paths.items()
            } == snapshot

            owner = await asyncpg.connect(_asyncpg_url(target_url))
            try:
                for versioned_role in control_names.versioned.values():
                    role = await owner.fetchrow(
                        """
                        SELECT rolcanlogin
                        FROM pg_catalog.pg_roles
                        WHERE rolname = $1
                        """,
                        versioned_role,
                    )
                    assert role is not None
                    assert not role["rolcanlogin"]
                    assert not await _role_membership_edges(
                        owner,
                        versioned_role,
                    )
            finally:
                await owner.close()

            for role_url in original_urls[
                generations.index(generation)
            ].values():
                with pytest.raises(asyncpg.PostgresError):
                    await asyncpg.connect(_asyncpg_url(role_url))

        assert old_operator is not None
        with pytest.raises(
            (
                asyncpg.PostgresError,
                asyncpg.InterfaceError,
                ConnectionError,
            )
        ):
            await old_operator.fetchval(
                "SELECT public.vp_worker_grant_activate($1, $2)",
                "task4a-round5-old-operator",
                1,
            )
    finally:
        if old_operator is not None and not old_operator.is_closed():
            await old_operator.close()
        admin = await asyncpg.connect(_asyncpg_url(admin_url))
        try:
            await admin.execute(
                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'
            )
            await _drop_test_roles(
                admin,
                (
                    *(
                        versioned
                        for control_names in names
                        for versioned in control_names.versioned.values()
                    ),
                    *names[0].stable.values(),
                ),
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
        with pytest.raises(runtime_cli.RuntimeRoleError):
            await runtime_cli._provision(
                target_url,
                service_name,
                worker_generation,
                runtime_state,
                runtime_names,
            )
        assert not partial_paths["state"].exists()
        await runtime_cli._revoke(
            target_url,
            service_name,
            worker_generation,
            runtime_state,
            runtime_names,
        )
        await runtime_cli._provision(
            target_url,
            service_name,
            worker_generation,
            runtime_state,
            runtime_names,
        )
        assert all(path.exists() for path in partial_paths.values())
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
        pending_operator = await asyncpg.connect(
            _asyncpg_url(operator_url)
        )
        try:
            await _upsert_worker_grant(
                pending_operator,
                service_name=service_name,
                generation=first_generation,
                database_principal=first_names.versioned,
                admission_token=first_token,
            )
        finally:
            await pending_operator.close()
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


@pytest.mark.asyncio
@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for live role tests",
)
async def test_shared_stable_role_authority_serializes_cross_scope_provision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = f"vp_worker_shared_lock_{uuid.uuid4().hex[:18]}"
    services = (
        f"task4a-shared-a-{uuid.uuid4().hex[:6]}",
        f"task4a-shared-b-{uuid.uuid4().hex[:6]}",
    )
    runtime_generations = (
        (int(uuid.uuid4().hex[:12], 16), int(uuid.uuid4().hex[:12], 16)),
        (int(uuid.uuid4().hex[:12], 16), int(uuid.uuid4().hex[:12], 16)),
    )
    runtime_names = tuple(
        tuple(
            runtime_cli.role_names_for_generation(service_name, generation)
            for generation in generations
        )
        for service_name, generations in zip(
            services,
            runtime_generations,
            strict=True,
        )
    )
    control_generations = (
        f"shared-a-{uuid.uuid4().hex[:10]}",
        f"shared-b-{uuid.uuid4().hex[:10]}",
    )
    control_names = tuple(
        control_cli.role_names_for_generation(generation)
        for generation in control_generations
    )
    admin_url = _database_url("postgres")
    target_url = _database_url(database)
    runtime_state = tmp_path / "runtime-state"
    control_state = tmp_path / "control-state"

    admin = await asyncpg.connect(_asyncpg_url(admin_url))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()

    runtime_release = asyncio.Event()
    control_release = asyncio.Event()
    try:
        migrated = _run_alembic(_alembic_url(target_url))
        assert migrated.returncode == 0, migrated.stdout + migrated.stderr

        for service_name, generations, names_by_generation in zip(
            services,
            runtime_generations,
            runtime_names,
            strict=True,
        ):
            await runtime_cli._provision(
                target_url,
                service_name,
                generations[0],
                runtime_state,
                names_by_generation[0],
            )
        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            for names_by_generation in runtime_names:
                assert await owner.fetchval(
                    "SELECT pg_catalog.pg_has_role($1, $2, 'MEMBER')",
                    names_by_generation[0].versioned,
                    names_by_generation[0].stable,
                )
        finally:
            await owner.close()

        runtime_first_scanned = asyncio.Event()
        runtime_second_scanned = asyncio.Event()
        real_runtime_authorized = (
            runtime_cli._authorized_runtime_members
        )

        async def paused_runtime_authorized(
            connection: asyncpg.Connection,
            owner_url: str,
            state_dir: Path,
            service_name: str,
        ) -> set[str]:
            result = await real_runtime_authorized(
                connection,
                owner_url,
                state_dir,
                service_name,
            )
            if service_name == services[0]:
                runtime_first_scanned.set()
                await runtime_release.wait()
            else:
                runtime_second_scanned.set()
            return result

        monkeypatch.setattr(
            runtime_cli,
            "_authorized_runtime_members",
            paused_runtime_authorized,
        )
        runtime_first = asyncio.create_task(
            runtime_cli._provision(
                target_url,
                services[0],
                runtime_generations[0][1],
                runtime_state,
                runtime_names[0][1],
            )
        )
        await asyncio.wait_for(runtime_first_scanned.wait(), timeout=5)
        runtime_second = asyncio.create_task(
            runtime_cli._provision(
                target_url,
                services[1],
                runtime_generations[1][1],
                runtime_state,
                runtime_names[1][1],
            )
        )
        await asyncio.sleep(0.2)
        runtime_crossed_shared_fence = runtime_second_scanned.is_set()
        runtime_release.set()
        runtime_results = await asyncio.gather(
            runtime_first,
            runtime_second,
            return_exceptions=True,
        )
        assert not runtime_crossed_shared_fence
        assert not any(
            isinstance(result, BaseException)
            for result in runtime_results
        )

        monkeypatch.setattr(
            runtime_cli,
            "_authorized_runtime_members",
            real_runtime_authorized,
        )
        control_first_scanned = asyncio.Event()
        control_second_scanned = asyncio.Event()
        control_calls = 0
        real_control_authorized = control_cli._authorized_control_members

        async def paused_control_authorized(
            connection: asyncpg.Connection,
            owner_url: str,
            state_dir: Path,
        ) -> dict[str, set[str]]:
            nonlocal control_calls
            result = await real_control_authorized(
                connection,
                owner_url,
                state_dir,
            )
            control_calls += 1
            if control_calls == 1:
                control_first_scanned.set()
                await control_release.wait()
            else:
                control_second_scanned.set()
            return result

        monkeypatch.setattr(
            control_cli,
            "_authorized_control_members",
            paused_control_authorized,
        )
        control_first = asyncio.create_task(
            control_cli._provision(
                target_url,
                control_generations[0],
                control_state,
                control_names[0],
            )
        )
        await asyncio.wait_for(control_first_scanned.wait(), timeout=5)
        control_second = asyncio.create_task(
            control_cli._provision(
                target_url,
                control_generations[1],
                control_state,
                control_names[1],
            )
        )
        await asyncio.sleep(0.2)
        control_crossed_shared_fence = control_second_scanned.is_set()
        control_release.set()
        control_results = await asyncio.gather(
            control_first,
            control_second,
            return_exceptions=True,
        )
        assert not control_crossed_shared_fence
        assert not any(
            isinstance(result, BaseException)
            for result in control_results
        )

        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            for names_by_generation in runtime_names:
                for names in names_by_generation:
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
        finally:
            await owner.close()
        for generation in control_generations:
            assert all(
                path.exists()
                for path in control_cli.credential_paths(
                    control_state,
                    generation,
                ).values()
            )
    finally:
        runtime_release.set()
        control_release.set()
        admin = await asyncpg.connect(_asyncpg_url(admin_url))
        try:
            await admin.execute(
                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'
            )
            for role_name in (
                *(
                    names.versioned
                    for names_by_generation in runtime_names
                    for names in names_by_generation
                ),
                *(
                    role_name
                    for names in control_names
                    for role_name in names.versioned.values()
                ),
                runtime_names[0][0].stable,
                *control_names[0].stable.values(),
            ):
                await admin.execute(f'DROP ROLE IF EXISTS "{role_name}"')
        finally:
            await admin.close()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for live role tests",
)
async def test_runtime_recovery_requires_exact_nonrevoked_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = f"vp_worker_recovery_auth_{uuid.uuid4().hex[:16]}"
    service_name = f"task4a-recovery-{uuid.uuid4().hex[:8]}"
    generations = tuple(
        int(uuid.uuid4().hex[:12], 16) for _index in range(4)
    )
    runtime_names = tuple(
        runtime_cli.role_names_for_generation(service_name, generation)
        for generation in generations
    )
    control_generation = f"recovery-{uuid.uuid4().hex[:12]}"
    control_names = control_cli.role_names_for_generation(
        control_generation
    )
    admin_url = _database_url("postgres")
    target_url = _database_url(database)
    runtime_state = tmp_path / "runtime-state"
    control_state = tmp_path / "control-state"
    recovery_release = threading.Event()

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
        operator = await asyncpg.connect(_asyncpg_url(operator_url))
        try:
            pending_generation = generations[0]
            pending_names = runtime_names[0]
            await runtime_cli._provision(
                target_url,
                service_name,
                pending_generation,
                runtime_state,
                pending_names,
            )
            pending_paths = runtime_cli.credential_paths(
                runtime_state,
                service_name,
                pending_generation,
            )
            pending_token = (
                pending_paths["admission_token"].read_text().strip()
            )
            await _upsert_worker_grant(
                operator,
                service_name=service_name,
                generation=pending_generation,
                database_principal=pending_names.versioned,
                admission_token=pending_token,
            )
            pending_paths["state"].unlink()
            await runtime_cli._provision(
                target_url,
                service_name,
                pending_generation,
                runtime_state,
                pending_names,
            )
            assert pending_paths["state"].exists()
            await operator.fetchval(
                "SELECT public.vp_worker_grant_activate($1, $2)",
                service_name,
                pending_generation,
            )

            pending_paths["state"].unlink()
            revoke_first = operator.transaction()
            await revoke_first.start()
            await operator.fetchval(
                "SELECT public.vp_worker_grant_revoke($1, $2, $3)",
                service_name,
                pending_generation,
                "round3-revoke-first",
            )
            revoked_recovery = asyncio.create_task(
                runtime_cli._provision(
                    target_url,
                    service_name,
                    pending_generation,
                    runtime_state,
                    pending_names,
                )
            )
            await asyncio.sleep(0.2)
            assert not revoked_recovery.done()
            await revoke_first.commit()
            revoked_result = await asyncio.gather(
                revoked_recovery,
                return_exceptions=True,
            )
            assert isinstance(
                revoked_result[0],
                runtime_cli.RuntimeRoleError,
            )
            assert not any(path.exists() for path in pending_paths.values())

            owner = await asyncpg.connect(_asyncpg_url(target_url))
            try:
                assert not await owner.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_catalog.pg_roles
                        WHERE rolname = $1
                    )
                    """,
                    pending_names.versioned,
                )
            finally:
                await owner.close()

            mismatch_generation = generations[1]
            mismatch_names = runtime_names[1]
            await runtime_cli._provision(
                target_url,
                service_name,
                mismatch_generation,
                runtime_state,
                mismatch_names,
            )
            mismatch_paths = runtime_cli.credential_paths(
                runtime_state,
                service_name,
                mismatch_generation,
            )
            await _upsert_worker_grant(
                operator,
                service_name=service_name,
                generation=mismatch_generation,
                database_principal=mismatch_names.versioned,
                admission_token="round3-deliberately-different-token",
            )
            await operator.fetchval(
                "SELECT public.vp_worker_grant_activate($1, $2)",
                service_name,
                mismatch_generation,
            )
            with pytest.raises(runtime_cli.RuntimeRoleError):
                await runtime_cli._provision(
                    target_url,
                    service_name,
                    mismatch_generation,
                    runtime_state,
                    mismatch_names,
                )
            owner = await asyncpg.connect(_asyncpg_url(target_url))
            try:
                assert not await owner.fetchval(
                    "SELECT pg_catalog.pg_has_role($1, $2, 'MEMBER')",
                    mismatch_names.versioned,
                    mismatch_names.stable,
                )
            finally:
                await owner.close()
            assert all(path.exists() for path in mismatch_paths.values())

            no_authority_generation = generations[2]
            no_authority_names = runtime_names[2]
            await runtime_cli._provision(
                target_url,
                service_name,
                no_authority_generation,
                runtime_state,
                no_authority_names,
            )
            no_authority_paths = runtime_cli.credential_paths(
                runtime_state,
                service_name,
                no_authority_generation,
            )
            no_authority_paths["state"].unlink()
            with pytest.raises(runtime_cli.RuntimeRoleError):
                await runtime_cli._provision(
                    target_url,
                    service_name,
                    no_authority_generation,
                    runtime_state,
                    no_authority_names,
                )
            assert not no_authority_paths["state"].exists()

            recovery_generation = generations[3]
            recovery_names = runtime_names[3]
            await runtime_cli._provision(
                target_url,
                service_name,
                recovery_generation,
                runtime_state,
                recovery_names,
            )
            recovery_paths = runtime_cli.credential_paths(
                runtime_state,
                service_name,
                recovery_generation,
            )
            recovery_token = (
                recovery_paths["admission_token"].read_text().strip()
            )
            await _upsert_worker_grant(
                operator,
                service_name=service_name,
                generation=recovery_generation,
                database_principal=recovery_names.versioned,
                admission_token=recovery_token,
            )
            await operator.fetchval(
                "SELECT public.vp_worker_grant_activate($1, $2)",
                service_name,
                recovery_generation,
            )
            recovery_paths["state"].unlink()
            recovery_entered = threading.Event()
            real_write = runtime_cli.write_generation_state

            def paused_recovery_write(
                *args: object,
                **kwargs: object,
            ) -> None:
                recovery_entered.set()
                if not recovery_release.wait(timeout=10):
                    raise AssertionError("recovery was not released")
                real_write(*args, **kwargs)

            monkeypatch.setattr(
                runtime_cli,
                "write_generation_state",
                paused_recovery_write,
            )
            recovery_first = asyncio.create_task(
                asyncio.to_thread(
                    lambda: asyncio.run(
                        runtime_cli._provision(
                            target_url,
                            service_name,
                            recovery_generation,
                            runtime_state,
                            recovery_names,
                        )
                    )
                )
            )
            assert await asyncio.to_thread(recovery_entered.wait, 10)
            revoke_second = asyncio.create_task(
                operator.fetchval(
                    "SELECT public.vp_worker_grant_revoke($1, $2, $3)",
                    service_name,
                    recovery_generation,
                    "round3-recovery-first",
                )
            )
            await asyncio.sleep(0.2)
            assert not revoke_second.done()
            recovery_release.set()
            await asyncio.wait_for(recovery_first, timeout=5)
            await asyncio.wait_for(revoke_second, timeout=5)
            monkeypatch.setattr(
                runtime_cli,
                "write_generation_state",
                real_write,
            )
            await runtime_cli._revoke(
                target_url,
                service_name,
                recovery_generation,
                runtime_state,
                recovery_names,
            )
            assert not any(
                path.exists() for path in recovery_paths.values()
            )
        finally:
            await operator.close()
    finally:
        recovery_release.set()
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
async def test_default_acl_convergence_includes_objectless_effective_creators(
    tmp_path: Path,
) -> None:
    database = f"vp_worker_acl_creators_{uuid.uuid4().hex[:16]}"
    migration_group = f"vp_acl_group_{uuid.uuid4().hex[:12]}"
    migration_creator = f"vp_acl_creator_{uuid.uuid4().hex[:12]}"
    migration_probe = f"vp_acl_probe_{uuid.uuid4().hex[:12]}"
    runtime_group = f"vp_acl_group_{uuid.uuid4().hex[:12]}"
    runtime_creator = f"vp_acl_creator_{uuid.uuid4().hex[:12]}"
    runtime_probe = f"vp_acl_probe_{uuid.uuid4().hex[:12]}"
    service_name = f"task4a-acl-{uuid.uuid4().hex[:8]}"
    generation = int(uuid.uuid4().hex[:12], 16)
    names = runtime_cli.role_names_for_generation(
        service_name,
        generation,
    )
    admin_url = _database_url("postgres")
    target_url = _database_url(database)
    runtime_state = tmp_path / "runtime-state"

    admin = await asyncpg.connect(_asyncpg_url(admin_url))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()

    system_defaults_query = """
        SELECT
            owner.rolname,
            defaults.defaclnamespace,
            defaults.defaclobjtype::text,
            defaults.defaclacl::text
        FROM pg_catalog.pg_default_acl AS defaults
        JOIN pg_catalog.pg_roles AS owner
          ON owner.oid = defaults.defaclrole
        WHERE owner.rolname LIKE 'pg\\_%' ESCAPE '\\'
        ORDER BY 1, 2, 3, 4
    """
    try:
        migrated_to_previous = _run_alembic(
            _alembic_url(target_url),
            "033_legacy_worker_event_resolutions",
        )
        assert migrated_to_previous.returncode == 0, (
            migrated_to_previous.stdout + migrated_to_previous.stderr
        )
        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            for role_name in (
                migration_group,
                migration_creator,
                migration_probe,
            ):
                await owner.execute(f'CREATE ROLE "{role_name}" NOLOGIN')
            await owner.execute(
                f'GRANT CREATE ON SCHEMA public TO "{migration_group}"'
            )
            await owner.execute(
                f'GRANT CREATE ON DATABASE "{database}" '
                f'TO "{migration_group}"'
            )
            await owner.execute(
                f'GRANT "{migration_group}" TO "{migration_creator}"'
            )
            assert await owner.fetchval(
                "SELECT has_schema_privilege($1, 'public', 'CREATE')",
                migration_creator,
            )
            assert not await owner.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_default_acl AS defaults
                    JOIN pg_catalog.pg_roles AS role
                      ON role.oid = defaults.defaclrole
                    WHERE role.rolname = $1
                )
                """,
                migration_creator,
            )
            assert not await owner.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_class AS relation
                    WHERE relation.relowner = (
                        SELECT oid
                        FROM pg_catalog.pg_roles
                        WHERE rolname = $1
                    )
                )
                """,
                migration_creator,
            )
            system_defaults_before = tuple(
                tuple(row) for row in await owner.fetch(system_defaults_query)
            )
        finally:
            await owner.close()

        migrated = _run_alembic(_alembic_url(target_url))
        assert migrated.returncode == 0, migrated.stdout + migrated.stderr
        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert tuple(
                tuple(row) for row in await owner.fetch(system_defaults_query)
            ) == system_defaults_before
            await owner.execute(f'SET ROLE "{migration_creator}"')
            await owner.execute(
                "CREATE TABLE public.task4a_objectless_migration_table "
                "(id bigint)"
            )
            await owner.execute(
                """
                CREATE FUNCTION public.task4a_objectless_migration_definer()
                RETURNS boolean
                LANGUAGE sql
                SECURITY DEFINER
                SET search_path = pg_catalog
                AS 'SELECT TRUE'
                """
            )
            await owner.execute("RESET ROLE")
            assert not await owner.fetchval(
                "SELECT has_table_privilege("
                "$1, 'public.task4a_objectless_migration_table', 'SELECT')",
                migration_probe,
            )
            assert not await owner.fetchval(
                "SELECT has_function_privilege("
                "$1, 'public.task4a_objectless_migration_definer()', "
                "'EXECUTE')",
                migration_probe,
            )

            for role_name in (
                runtime_group,
                runtime_creator,
                runtime_probe,
            ):
                await owner.execute(f'CREATE ROLE "{role_name}" NOLOGIN')
            await owner.execute(
                f'GRANT CREATE ON SCHEMA public TO "{runtime_group}"'
            )
            await owner.execute(
                f'GRANT CREATE ON DATABASE "{database}" '
                f'TO "{runtime_group}"'
            )
            await owner.execute(
                f'GRANT "{runtime_group}" TO "{runtime_creator}"'
            )
            assert not await owner.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_default_acl AS defaults
                    JOIN pg_catalog.pg_roles AS role
                      ON role.oid = defaults.defaclrole
                    WHERE role.rolname = $1
                )
                """,
                runtime_creator,
            )
        finally:
            await owner.close()

        await runtime_cli._provision(
            target_url,
            service_name,
            generation,
            runtime_state,
            names,
        )
        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert tuple(
                tuple(row) for row in await owner.fetch(system_defaults_query)
            ) == system_defaults_before
            await owner.execute(f'SET ROLE "{runtime_creator}"')
            await owner.execute(
                "CREATE TABLE public.task4a_objectless_runtime_table "
                "(id bigint)"
            )
            await owner.execute(
                """
                CREATE FUNCTION public.task4a_objectless_runtime_definer()
                RETURNS boolean
                LANGUAGE sql
                SECURITY DEFINER
                SET search_path = pg_catalog
                AS 'SELECT TRUE'
                """
            )
            await owner.execute("RESET ROLE")
            assert not await owner.fetchval(
                "SELECT has_table_privilege("
                "$1, 'public.task4a_objectless_runtime_table', 'SELECT')",
                runtime_probe,
            )
            assert not await owner.fetchval(
                "SELECT has_function_privilege("
                "$1, 'public.task4a_objectless_runtime_definer()', "
                "'EXECUTE')",
                runtime_probe,
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
                migration_creator,
                migration_probe,
                migration_group,
                runtime_creator,
                runtime_probe,
                runtime_group,
                names.versioned,
                names.stable,
            ):
                await admin.execute(f'DROP ROLE IF EXISTS "{role_name}"')
        finally:
            await admin.close()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for live role tests",
)
async def test_runtime_authority_snapshot_fences_every_managed_service(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = f"vp_worker_snapshot_{uuid.uuid4().hex[:18]}"
    services = tuple(
        f"task4a-snapshot-{index}-{uuid.uuid4().hex[:6]}"
        for index in range(6)
    )
    generations = tuple(
        int(uuid.uuid4().hex[:12], 16) for _service in services
    )
    names = tuple(
        runtime_cli.role_names_for_generation(service_name, generation)
        for service_name, generation in zip(
            services,
            generations,
            strict=True,
        )
    )
    replacement_generation = int(uuid.uuid4().hex[:12], 16)
    replacement_names = runtime_cli.role_names_for_generation(
        services[1],
        replacement_generation,
    )
    control_generation = f"snapshot-{uuid.uuid4().hex[:12]}"
    control_names = control_cli.role_names_for_generation(
        control_generation
    )
    admin_url = _database_url("postgres")
    target_url = _database_url(database)
    runtime_state = tmp_path / "runtime-state"
    control_state = tmp_path / "control-state"
    scan_ready = asyncio.Event()
    scan_release = asyncio.Event()

    admin = await asyncpg.connect(_asyncpg_url(admin_url))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()

    scan_task: asyncio.Task[None] | None = None
    revoke_task: asyncio.Task[object] | None = None
    activate_task: asyncio.Task[object] | None = None
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

        operator = await asyncpg.connect(_asyncpg_url(operator_url))
        try:
            for (
                service_name,
                generation,
                runtime_names,
            ) in zip(services, generations, names, strict=True):
                await runtime_cli._provision(
                    target_url,
                    service_name,
                    generation,
                    runtime_state,
                    runtime_names,
                )
                token = runtime_cli.credential_paths(
                    runtime_state,
                    service_name,
                    generation,
                )["admission_token"].read_text().strip()
                await _upsert_worker_grant(
                    operator,
                    service_name=service_name,
                    generation=generation,
                    database_principal=runtime_names.versioned,
                    admission_token=token,
                )
                await operator.fetchval(
                    "SELECT public.vp_worker_grant_activate($1, $2)",
                    service_name,
                    generation,
                )
            await runtime_cli._provision(
                target_url,
                services[1],
                replacement_generation,
                runtime_state,
                replacement_names,
            )
            replacement_token = runtime_cli.credential_paths(
                runtime_state,
                services[1],
                replacement_generation,
            )["admission_token"].read_text().strip()
            await _upsert_worker_grant(
                operator,
                service_name=services[1],
                generation=replacement_generation,
                database_principal=replacement_names.versioned,
                admission_token=replacement_token,
            )
        finally:
            await operator.close()

        acquired_services: list[str] = []
        real_acquire_service_lock = (
            runtime_cli.acquire_worker_service_authority_lock
        )

        async def recording_service_lock(
            connection: asyncpg.Connection,
            service_name: str,
        ) -> None:
            acquired_services.append(service_name)
            await real_acquire_service_lock(connection, service_name)

        real_authorized_members = runtime_cli._authorized_runtime_members

        async def paused_authorized_members(
            connection: asyncpg.Connection,
            owner_url: str,
            state_dir: Path,
            service_name: str,
        ) -> set[str]:
            result = await real_authorized_members(
                connection,
                owner_url,
                state_dir,
                service_name,
            )
            scan_ready.set()
            await scan_release.wait()
            return result

        monkeypatch.setattr(
            runtime_cli,
            "acquire_worker_service_authority_lock",
            recording_service_lock,
        )
        monkeypatch.setattr(
            runtime_cli,
            "_authorized_runtime_members",
            paused_authorized_members,
        )
        scan_task = asyncio.create_task(
            runtime_cli._provision(
                target_url,
                services[2],
                generations[2],
                runtime_state,
                names[2],
            )
        )
        await asyncio.wait_for(scan_ready.wait(), timeout=10)

        async def revoke_victim() -> object:
            connection = await asyncpg.connect(_asyncpg_url(operator_url))
            try:
                return await connection.fetchval(
                    "SELECT public.vp_worker_grant_revoke($1, $2, $3)",
                    services[0],
                    generations[0],
                    "round4-snapshot-race",
                )
            finally:
                await connection.close()

        async def activate_pending() -> object:
            connection = await asyncpg.connect(_asyncpg_url(operator_url))
            try:
                return await connection.fetchval(
                    "SELECT public.vp_worker_grant_activate($1, $2)",
                    services[1],
                    replacement_generation,
                )
            finally:
                await connection.close()

        revoke_task = asyncio.create_task(revoke_victim())
        activate_task = asyncio.create_task(activate_pending())
        await asyncio.sleep(0.2)
        mutations_waited_for_snapshot = (
            not revoke_task.done() and not activate_task.done()
        )
        scan_release.set()
        await asyncio.wait_for(scan_task, timeout=10)
        await asyncio.wait_for(
            asyncio.gather(revoke_task, activate_task),
            timeout=10,
        )

        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            grant_states = {
                (row["service_name"], row["generation"]): row["state"]
                for row in await owner.fetch(
                    """
                    SELECT service_name, generation, state
                    FROM public.worker_admission_grants
                    WHERE (
                        service_name = $1
                        AND generation = $2
                    )
                       OR service_name = $3
                    """,
                    services[0],
                    generations[0],
                    services[1],
                )
            }
            assert grant_states == {
                (services[0], generations[0]): "revoked",
                (services[1], generations[1]): "revoked",
                (services[1], replacement_generation): "active",
            }
            assert not await owner.fetchval(
                "SELECT pg_catalog.pg_has_role($1, $2, 'MEMBER')",
                names[0].versioned,
                names[0].stable,
            )
            assert not await owner.fetchval(
                "SELECT pg_catalog.pg_has_role($1, $2, 'MEMBER')",
                names[1].versioned,
                names[1].stable,
            )
            assert await owner.fetchval(
                "SELECT pg_catalog.pg_has_role($1, $2, 'MEMBER')",
                replacement_names.versioned,
                replacement_names.stable,
            )
        finally:
            await owner.close()

        assert acquired_services == sorted(services)
        assert mutations_waited_for_snapshot
        for service_name, generation in zip(
            services,
            generations,
            strict=True,
        ):
            assert all(
                path.exists()
                for path in runtime_cli.credential_paths(
                    runtime_state,
                    service_name,
                    generation,
                ).values()
            )
        assert all(
            path.exists()
            for path in runtime_cli.credential_paths(
                runtime_state,
                services[1],
                replacement_generation,
            ).values()
        )
    finally:
        scan_release.set()
        for task in (scan_task, revoke_task, activate_task):
            if task is not None and not task.done():
                task.cancel()
        admin = await asyncpg.connect(_asyncpg_url(admin_url))
        try:
            await admin.execute(
                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'
            )
            for role_name in (
                *(runtime_names.versioned for runtime_names in names),
                replacement_names.versioned,
                *control_names.versioned.values(),
                names[0].stable,
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
async def test_runtime_revoke_preserves_every_owned_catalog_object(
    tmp_path: Path,
) -> None:
    database = f"vp_worker_owned_{uuid.uuid4().hex[:20]}"
    service_name = f"task4a-owned-{uuid.uuid4().hex[:8]}"
    generation = int(uuid.uuid4().hex[:12], 16)
    names = runtime_cli.role_names_for_generation(
        service_name,
        generation,
    )
    collation_name = f"task4a_collation_{uuid.uuid4().hex[:12]}"
    configuration_name = f"task4a_config_{uuid.uuid4().hex[:12]}"
    admin_url = _database_url("postgres")
    target_url = _database_url(database)
    runtime_state = tmp_path / "runtime-state"

    admin = await asyncpg.connect(_asyncpg_url(admin_url))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()

    try:
        migrated = _run_alembic(_alembic_url(target_url))
        assert migrated.returncode == 0, migrated.stdout + migrated.stderr
        await runtime_cli._provision(
            target_url,
            service_name,
            generation,
            runtime_state,
            names,
        )
        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            await owner.execute(
                f'CREATE COLLATION public."{collation_name}" '
                'FROM pg_catalog."C"'
            )
            await owner.execute(
                "CREATE TEXT SEARCH CONFIGURATION "
                f'public."{configuration_name}" '
                "(COPY = pg_catalog.simple)"
            )
            await owner.execute(
                f'ALTER COLLATION public."{collation_name}" '
                f'OWNER TO "{names.versioned}"'
            )
            await owner.execute(
                "ALTER TEXT SEARCH CONFIGURATION "
                f'public."{configuration_name}" '
                f'OWNER TO "{names.versioned}"'
            )
        finally:
            await owner.close()

        with pytest.raises(
            runtime_cli.RuntimeRoleError,
            match="generation state removal failed",
        ):
            await runtime_cli._revoke(
                target_url,
                service_name,
                generation,
                runtime_state,
                names,
            )

        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert await owner.fetchval(
                "SELECT pg_catalog.to_regcollation($1) IS NOT NULL",
                f"public.{collation_name}",
            )
            assert await owner.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_ts_config AS configuration
                    JOIN pg_catalog.pg_namespace AS namespace
                      ON namespace.oid = configuration.cfgnamespace
                    WHERE namespace.nspname = 'public'
                      AND configuration.cfgname = $1
                )
                """,
                configuration_name,
            )
            assert await owner.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_roles
                    WHERE rolname = $1
                )
                """,
                names.versioned,
            )
            await owner.execute(
                "DROP TEXT SEARCH CONFIGURATION "
                f'public."{configuration_name}"'
            )
            await owner.execute(
                f'DROP COLLATION public."{collation_name}"'
            )
        finally:
            await owner.close()

        await runtime_cli._revoke(
            target_url,
            service_name,
            generation,
            runtime_state,
            names,
        )
        await runtime_cli._revoke(
            target_url,
            service_name,
            generation,
            runtime_state,
            names,
        )
        assert not any(
            path.exists()
            for path in runtime_cli.credential_paths(
                runtime_state,
                service_name,
                generation,
            ).values()
        )
    finally:
        admin = await asyncpg.connect(_asyncpg_url(admin_url))
        try:
            await admin.execute(
                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'
            )
            for role_name in (names.versioned, names.stable):
                await admin.execute(f'DROP ROLE IF EXISTS "{role_name}"')
        finally:
            await admin.close()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for live role tests",
)
async def test_invalid_complete_runtime_generations_are_quarantined(
    tmp_path: Path,
) -> None:
    database = f"vp_worker_quarantine_{uuid.uuid4().hex[:16]}"
    service_name = f"task4a-quarantine-{uuid.uuid4().hex[:8]}"
    generations = tuple(
        int(uuid.uuid4().hex[:12], 16) for _index in range(3)
    )
    names = tuple(
        runtime_cli.role_names_for_generation(service_name, generation)
        for generation in generations
    )
    admin_url = _database_url("postgres")
    target_url = _database_url(database)
    runtime_state = tmp_path / "runtime-state"

    admin = await asyncpg.connect(_asyncpg_url(admin_url))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()

    try:
        migrated = _run_alembic(_alembic_url(target_url))
        assert migrated.returncode == 0, migrated.stdout + migrated.stderr
        for generation, runtime_names in zip(
            generations,
            names,
            strict=True,
        ):
            await runtime_cli._provision(
                target_url,
                service_name,
                generation,
                runtime_state,
                runtime_names,
            )

        paths_by_generation = tuple(
            runtime_cli.credential_paths(
                runtime_state,
                service_name,
                generation,
            )
            for generation in generations
        )
        first_url = make_url(
            paths_by_generation[0]["database_url"].read_text().strip()
        )
        _write_secret(
            paths_by_generation[0]["database_url"],
            first_url.set(
                password="round4-deliberately-wrong-password"
            ).render_as_string(hide_password=False),
            0o400,
        )
        _write_secret(
            paths_by_generation[1]["state"],
            "{not-valid-json",
            0o600,
        )
        third_url = make_url(
            paths_by_generation[2]["database_url"].read_text().strip()
        )
        _write_secret(
            paths_by_generation[2]["database_url"],
            third_url.set(host="127.0.0.2").render_as_string(
                hide_password=False
            ),
            0o400,
        )
        forensic_snapshots = tuple(
            {
                purpose: (
                    path.read_bytes(),
                    stat.S_IMODE(path.stat().st_mode),
                )
                for purpose, path in paths.items()
            }
            for paths in paths_by_generation
        )

        for generation, runtime_names, paths, snapshot in zip(
            generations,
            names,
            paths_by_generation,
            forensic_snapshots,
            strict=True,
        ):
            with pytest.raises(runtime_cli.RuntimeRoleError):
                await runtime_cli._provision(
                    target_url,
                    service_name,
                    generation,
                    runtime_state,
                    runtime_names,
                )
            assert {
                purpose: (
                    path.read_bytes(),
                    stat.S_IMODE(path.stat().st_mode),
                )
                for purpose, path in paths.items()
            } == snapshot

            owner = await asyncpg.connect(_asyncpg_url(target_url))
            try:
                role = await owner.fetchrow(
                    """
                    SELECT rolcanlogin
                    FROM pg_catalog.pg_roles
                    WHERE rolname = $1
                    """,
                    runtime_names.versioned,
                )
                assert role is not None
                assert not role["rolcanlogin"]
                assert not await owner.fetchval(
                    "SELECT pg_catalog.pg_has_role($1, $2, 'MEMBER')",
                    runtime_names.versioned,
                    runtime_names.stable,
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
                *(runtime_names.versioned for runtime_names in names),
                names[0].stable,
            ):
                await admin.execute(f'DROP ROLE IF EXISTS "{role_name}"')
        finally:
            await admin.close()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for live role tests",
)
async def test_invalid_generation_quarantine_serializes_with_revoke(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = f"vp_worker_quarantine_race_{uuid.uuid4().hex[:12]}"
    services = (
        f"task4a-quarantine-first-{uuid.uuid4().hex[:6]}",
        f"task4a-revoke-first-{uuid.uuid4().hex[:6]}",
    )
    generations = tuple(
        int(uuid.uuid4().hex[:12], 16) for _service in services
    )
    names = tuple(
        runtime_cli.role_names_for_generation(service_name, generation)
        for service_name, generation in zip(
            services,
            generations,
            strict=True,
        )
    )
    control_generation = f"quarantine-race-{uuid.uuid4().hex[:8]}"
    control_names = control_cli.role_names_for_generation(
        control_generation
    )
    admin_url = _database_url("postgres")
    target_url = _database_url(database)
    runtime_state = tmp_path / "runtime-state"
    control_state = tmp_path / "control-state"
    read_entered = threading.Event()
    read_release = threading.Event()
    revoke_transaction: asyncpg.Transaction | None = None

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
        operator = await asyncpg.connect(_asyncpg_url(operator_url))
        try:
            for service_name, generation, runtime_names in zip(
                services,
                generations,
                names,
                strict=True,
            ):
                await runtime_cli._provision(
                    target_url,
                    service_name,
                    generation,
                    runtime_state,
                    runtime_names,
                )
                token = runtime_cli.credential_paths(
                    runtime_state,
                    service_name,
                    generation,
                )["admission_token"].read_text().strip()
                await _upsert_worker_grant(
                    operator,
                    service_name=service_name,
                    generation=generation,
                    database_principal=runtime_names.versioned,
                    admission_token=token,
                )
                await operator.fetchval(
                    "SELECT public.vp_worker_grant_activate($1, $2)",
                    service_name,
                    generation,
                )
        finally:
            await operator.close()

        paths = tuple(
            runtime_cli.credential_paths(
                runtime_state,
                service_name,
                generation,
            )
            for service_name, generation in zip(
                services,
                generations,
                strict=True,
            )
        )
        for generation_paths in paths:
            _write_secret(
                generation_paths["state"],
                "{corrupted-for-round4",
                0o600,
            )
        forensic_snapshots = tuple(
            {
                purpose: (
                    path.read_bytes(),
                    stat.S_IMODE(path.stat().st_mode),
                )
                for purpose, path in generation_paths.items()
            }
            for generation_paths in paths
        )

        real_read_secure_file = runtime_cli.read_secure_file
        paused_once = False

        def paused_state_read(
            path: Path,
            *,
            required_mode: int,
        ) -> str:
            nonlocal paused_once
            value = real_read_secure_file(
                path,
                required_mode=required_mode,
            )
            if path == paths[0]["state"] and not paused_once:
                paused_once = True
                read_entered.set()
                if not read_release.wait(timeout=10):
                    raise AssertionError(
                        "invalid generation read was not released"
                    )
            return value

        monkeypatch.setattr(
            runtime_cli,
            "read_secure_file",
            paused_state_read,
        )
        quarantine_first = asyncio.create_task(
            asyncio.to_thread(
                lambda: asyncio.run(
                    runtime_cli._provision(
                        target_url,
                        services[0],
                        generations[0],
                        runtime_state,
                        names[0],
                    )
                )
            )
        )
        assert await asyncio.to_thread(read_entered.wait, 10)

        async def revoke_generation(index: int, reason: str) -> object:
            connection = await asyncpg.connect(_asyncpg_url(operator_url))
            try:
                return await connection.fetchval(
                    "SELECT public.vp_worker_grant_revoke($1, $2, $3)",
                    services[index],
                    generations[index],
                    reason,
                )
            finally:
                await connection.close()

        revoke_second = asyncio.create_task(
            revoke_generation(0, "round4-quarantine-first")
        )
        await asyncio.sleep(0.2)
        assert not revoke_second.done()
        read_release.set()
        quarantine_result = await asyncio.gather(
            quarantine_first,
            return_exceptions=True,
        )
        assert isinstance(
            quarantine_result[0],
            runtime_cli.RuntimeRoleError,
        )
        await asyncio.wait_for(revoke_second, timeout=10)

        monkeypatch.setattr(
            runtime_cli,
            "read_secure_file",
            real_read_secure_file,
        )
        operator = await asyncpg.connect(_asyncpg_url(operator_url))
        try:
            revoke_transaction = operator.transaction()
            await revoke_transaction.start()
            await operator.fetchval(
                "SELECT public.vp_worker_grant_revoke($1, $2, $3)",
                services[1],
                generations[1],
                "round4-revoke-first",
            )
            revoke_first = asyncio.create_task(
                runtime_cli._provision(
                    target_url,
                    services[1],
                    generations[1],
                    runtime_state,
                    names[1],
                )
            )
            await asyncio.sleep(0.2)
            assert not revoke_first.done()
            await revoke_transaction.commit()
            revoke_transaction = None
            revoke_first_result = await asyncio.gather(
                revoke_first,
                return_exceptions=True,
            )
            assert isinstance(
                revoke_first_result[0],
                runtime_cli.RuntimeRoleError,
            )
        finally:
            if revoke_transaction is not None:
                await revoke_transaction.rollback()
            await operator.close()

        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            for index, runtime_names in enumerate(names):
                role = await owner.fetchrow(
                    """
                    SELECT rolcanlogin
                    FROM pg_catalog.pg_roles
                    WHERE rolname = $1
                    """,
                    runtime_names.versioned,
                )
                assert role is not None
                assert not role["rolcanlogin"]
                assert not await owner.fetchval(
                    "SELECT pg_catalog.pg_has_role($1, $2, 'MEMBER')",
                    runtime_names.versioned,
                    runtime_names.stable,
                )
                assert await owner.fetchval(
                    """
                    SELECT state = 'revoked'
                    FROM public.worker_admission_grants
                    WHERE service_name = $1 AND generation = $2
                    """,
                    services[index],
                    generations[index],
                )
                assert {
                    purpose: (
                        path.read_bytes(),
                        stat.S_IMODE(path.stat().st_mode),
                    )
                    for purpose, path in paths[index].items()
                } == forensic_snapshots[index]
        finally:
            await owner.close()
    finally:
        read_release.set()
        admin = await asyncpg.connect(_asyncpg_url(admin_url))
        try:
            await admin.execute(
                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'
            )
            for role_name in (
                *(runtime_names.versioned for runtime_names in names),
                *control_names.versioned.values(),
                names[0].stable,
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
async def test_role_graph_converges_and_operator_revoke_isolates_multihop(
    tmp_path: Path,
) -> None:
    database = f"vp_role_graph_{uuid.uuid4().hex[:20]}"
    service_name = f"task4a-graph-{uuid.uuid4().hex[:8]}"
    generation = int(uuid.uuid4().hex[:12], 16)
    control_generation = f"graph-{uuid.uuid4().hex[:12]}"
    runtime_names = runtime_cli.role_names_for_generation(
        service_name,
        generation,
    )
    control_names = control_cli.role_names_for_generation(
        control_generation
    )
    role_pairs = (
        (
            "runtime",
            runtime_names.versioned,
            runtime_names.stable,
        ),
        *(
            (
                purpose,
                control_names.versioned[purpose],
                control_names.stable[purpose],
            )
            for purpose in control_names.versioned
        ),
    )
    bridges = {
        purpose: f"vp_t4a_bridge_{index}_{uuid.uuid4().hex[:8]}"
        for index, (purpose, _versioned, _stable) in enumerate(role_pairs)
    }
    outsiders = {
        purpose: f"vp_t4a_outsider_{index}_{uuid.uuid4().hex[:8]}"
        for index, (purpose, _versioned, _stable) in enumerate(role_pairs)
    }
    set_role_session: asyncpg.Connection | None = None
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
        await runtime_cli._provision(
            target_url,
            service_name,
            generation,
            runtime_state,
            runtime_names,
        )
        await control_cli._provision(
            target_url,
            control_generation,
            control_state,
            control_names,
        )

        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            for purpose, versioned_role, stable_role in role_pairs:
                bridge = bridges[purpose]
                outsider = outsiders[purpose]
                await owner.execute(
                    f"CREATE ROLE {role_common.quote_identifier(bridge)} "
                    "NOLOGIN"
                )
                await owner.execute(
                    f"CREATE ROLE {role_common.quote_identifier(outsider)} "
                    "LOGIN"
                )
                await owner.execute(
                    f"REVOKE {role_common.quote_identifier(stable_role)} "
                    f"FROM {role_common.quote_identifier(versioned_role)}"
                )
                await owner.execute(
                    f"GRANT {role_common.quote_identifier(stable_role)} "
                    f"TO {role_common.quote_identifier(bridge)}"
                )
                await owner.execute(
                    f"GRANT {role_common.quote_identifier(bridge)} "
                    f"TO {role_common.quote_identifier(versioned_role)}"
                )
                await owner.execute(
                    f"GRANT {role_common.quote_identifier(versioned_role)} "
                    f"TO {role_common.quote_identifier(outsider)}"
                )
                assert await owner.fetchval(
                    "SELECT pg_catalog.pg_has_role($1, $2, 'USAGE')",
                    outsider,
                    stable_role,
                )
        finally:
            await owner.close()

        await runtime_cli._provision(
            target_url,
            service_name,
            generation,
            runtime_state,
            runtime_names,
        )
        await control_cli._provision(
            target_url,
            control_generation,
            control_state,
            control_names,
        )

        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            for purpose, versioned_role, stable_role in role_pairs:
                assert await _role_membership_edges(
                    owner,
                    versioned_role,
                ) == {(stable_role, versioned_role)}
                assert not await owner.fetchval(
                    "SELECT pg_catalog.pg_has_role($1, $2, 'USAGE')",
                    outsiders[purpose],
                    stable_role,
                )
                assert not await owner.fetchval(
                    "SELECT pg_catalog.pg_has_role($1, $2, 'USAGE')",
                    bridges[purpose],
                    stable_role,
                )
                direct_members = {
                    row["member_role"]
                    for row in await owner.fetch(
                        """
                        SELECT member.rolname AS member_role
                        FROM pg_catalog.pg_auth_members AS membership
                        JOIN pg_catalog.pg_roles AS granted
                          ON granted.oid = membership.roleid
                        JOIN pg_catalog.pg_roles AS member
                          ON member.oid = membership.member
                        WHERE granted.rolname = $1
                        """,
                        stable_role,
                    )
                }
                assert direct_members == {versioned_role}
        finally:
            await owner.close()

        runtime_paths = runtime_cli.credential_paths(
            runtime_state,
            service_name,
            generation,
        )
        token = runtime_paths["admission_token"].read_text().strip()
        operator_url = control_cli.credential_paths(
            control_state,
            control_generation,
        )["operator"].read_text().strip()
        operator = await asyncpg.connect(_asyncpg_url(operator_url))
        try:
            await _upsert_worker_grant(
                operator,
                service_name=service_name,
                generation=generation,
                database_principal=runtime_names.versioned,
                admission_token=token,
            )
            await operator.fetchval(
                "SELECT public.vp_worker_grant_activate($1, $2)",
                service_name,
                generation,
            )
        finally:
            await operator.close()

        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            bridge = bridges["runtime"]
            outsider = outsiders["runtime"]
            await owner.execute(
                f"REVOKE {role_common.quote_identifier(runtime_names.stable)} "
                f"FROM "
                f"{role_common.quote_identifier(runtime_names.versioned)}"
            )
            await owner.execute(
                f"GRANT {role_common.quote_identifier(runtime_names.stable)} "
                f"TO {role_common.quote_identifier(bridge)}"
            )
            await owner.execute(
                f"GRANT {role_common.quote_identifier(bridge)} TO "
                f"{role_common.quote_identifier(runtime_names.versioned)}"
            )
            await owner.execute(
                f"GRANT "
                f"{role_common.quote_identifier(runtime_names.versioned)} "
                f"TO {role_common.quote_identifier(outsider)}"
            )
        finally:
            await owner.close()

        set_role_session = await asyncpg.connect(
            _asyncpg_url(
                runtime_paths["database_url"].read_text().strip()
            )
        )
        await set_role_session.execute(
            f"SET ROLE "
            f"{role_common.quote_identifier(runtime_names.stable)}"
        )
        operator = await asyncpg.connect(_asyncpg_url(operator_url))
        try:
            await operator.fetchval(
                "SELECT public.vp_worker_grant_revoke($1, $2, $3)",
                service_name,
                generation,
                "task4a-round5-role-graph",
            )
        finally:
            await operator.close()
        with pytest.raises(
            (
                asyncpg.PostgresError,
                asyncpg.InterfaceError,
                ConnectionError,
            )
        ):
            await set_role_session.fetchval("SELECT current_user")

        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            runtime_role = await owner.fetchrow(
                """
                SELECT rolcanlogin
                FROM pg_catalog.pg_roles
                WHERE rolname = $1
                """,
                runtime_names.versioned,
            )
            assert runtime_role is not None
            assert not runtime_role["rolcanlogin"]
            assert not await _role_membership_edges(
                owner,
                runtime_names.versioned,
            )
            assert not await owner.fetchval(
                "SELECT pg_catalog.pg_has_role($1, $2, 'USAGE')",
                outsiders["runtime"],
                runtime_names.stable,
            )
            assert not await owner.fetchval(
                "SELECT pg_catalog.pg_has_role($1, $2, 'USAGE')",
                bridges["runtime"],
                runtime_names.stable,
            )
        finally:
            await owner.close()
    finally:
        if set_role_session is not None and not set_role_session.is_closed():
            await set_role_session.close()
        admin = await asyncpg.connect(_asyncpg_url(admin_url))
        try:
            await admin.execute(
                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'
            )
            await _drop_test_roles(
                admin,
                (
                    *(bridges.values()),
                    *(outsiders.values()),
                    runtime_names.versioned,
                    *control_names.versioned.values(),
                    runtime_names.stable,
                    *control_names.stable.values(),
                ),
            )
        finally:
            await admin.close()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for live role tests",
)
async def test_overlap_provision_hardens_every_authorized_generation(
    tmp_path: Path,
) -> None:
    database = f"vp_role_overlap_harden_{uuid.uuid4().hex[:16]}"
    service_name = f"task4a-harden-{uuid.uuid4().hex[:8]}"
    generations = (
        int(uuid.uuid4().hex[:12], 16),
        int(uuid.uuid4().hex[:12], 16),
    )
    runtime_names = tuple(
        runtime_cli.role_names_for_generation(service_name, generation)
        for generation in generations
    )
    control_generations = (
        f"harden-a-{uuid.uuid4().hex[:8]}",
        f"harden-b-{uuid.uuid4().hex[:8]}",
    )
    control_names = tuple(
        control_cli.role_names_for_generation(generation)
        for generation in control_generations
    )
    old_generation_roles = (
        runtime_names[0].versioned,
        *control_names[0].versioned.values(),
    )
    intended_stable_roles = {
        runtime_names[0].versioned: runtime_names[0].stable,
        **{
            control_names[0].versioned[purpose]:
            control_names[0].stable[purpose]
            for purpose in control_names[0].versioned
        },
    }
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
        await runtime_cli._provision(
            target_url,
            service_name,
            generations[0],
            runtime_state,
            runtime_names[0],
        )
        await control_cli._provision(
            target_url,
            control_generations[0],
            control_state,
            control_names[0],
        )

        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            for role_name in old_generation_roles:
                quoted_role = role_common.quote_identifier(role_name)
                await owner.execute(
                    f"ALTER ROLE {quoted_role} SUPERUSER CREATEDB "
                    "CREATEROLE REPLICATION BYPASSRLS"
                )
                await owner.execute(
                    "GRANT SELECT ON TABLE public.worker_admission_grants "
                    f"TO {quoted_role}"
                )
        finally:
            await owner.close()

        await runtime_cli._provision(
            target_url,
            service_name,
            generations[1],
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
            for role_name in old_generation_roles:
                role = await owner.fetchrow(
                    """
                    SELECT
                        rolcanlogin,
                        rolinherit,
                        rolsuper,
                        rolcreatedb,
                        rolcreaterole,
                        rolreplication,
                        rolbypassrls
                    FROM pg_catalog.pg_roles
                    WHERE rolname = $1
                    """,
                    role_name,
                )
                assert role is not None
                assert dict(role) == {
                    "rolcanlogin": True,
                    "rolinherit": True,
                    "rolsuper": False,
                    "rolcreatedb": False,
                    "rolcreaterole": False,
                    "rolreplication": False,
                    "rolbypassrls": False,
                }
                assert await _role_membership_edges(
                    owner,
                    role_name,
                ) == {(intended_stable_roles[role_name], role_name)}
                assert not await owner.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_catalog.pg_class AS relation
                        CROSS JOIN LATERAL pg_catalog.aclexplode(
                            relation.relacl
                        ) AS privilege
                        JOIN pg_catalog.pg_roles AS grantee
                          ON grantee.oid = privilege.grantee
                        JOIN pg_catalog.pg_namespace AS namespace
                          ON namespace.oid = relation.relnamespace
                        WHERE namespace.nspname = 'public'
                          AND relation.relname = 'worker_admission_grants'
                          AND grantee.rolname = $1
                          AND privilege.privilege_type = 'SELECT'
                    )
                    """,
                    role_name,
                )
        finally:
            await owner.close()
    finally:
        admin = await asyncpg.connect(_asyncpg_url(admin_url))
        try:
            await admin.execute(
                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'
            )
            await _drop_test_roles(
                admin,
                (
                    *(names.versioned for names in runtime_names),
                    *(
                        role
                        for names in control_names
                        for role in names.versioned.values()
                    ),
                    runtime_names[0].stable,
                    *control_names[0].stable.values(),
                ),
            )
        finally:
            await admin.close()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for live role tests",
)
async def test_role_retirement_never_deletes_concurrently_owned_objects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database = f"vp_role_retire_race_{uuid.uuid4().hex[:16]}"
    service_name = f"task4a-retire-{uuid.uuid4().hex[:8]}"
    generation = int(uuid.uuid4().hex[:12], 16)
    names = runtime_cli.role_names_for_generation(
        service_name,
        generation,
    )
    collation_name = f"task4a_race_collation_{uuid.uuid4().hex[:10]}"
    configuration_name = f"task4a_race_config_{uuid.uuid4().hex[:10]}"
    admin_url = _database_url("postgres")
    target_url = _database_url(database)
    runtime_state = tmp_path / "runtime-state"
    pause_entered = asyncio.Event()
    pause_release = asyncio.Event()
    paused = False

    admin = await asyncpg.connect(_asyncpg_url(admin_url))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()

    real_owner_probe = role_common._role_owns_objects
    real_quarantine = getattr(
        role_common,
        "quarantine_login_roles",
        None,
    )

    async def pause_once() -> None:
        nonlocal paused
        if paused:
            return
        paused = True
        pause_entered.set()
        await asyncio.wait_for(pause_release.wait(), timeout=10)

    async def paused_owner_probe(
        connection: asyncpg.Connection,
        role_name: str,
    ) -> bool:
        result = await real_owner_probe(connection, role_name)
        if role_name == names.versioned and not result:
            await pause_once()
        return result

    async def paused_quarantine(
        connection: asyncpg.Connection,
        role_names: tuple[str, ...],
    ) -> None:
        if real_quarantine is None:
            return
        await real_quarantine(connection, role_names)
        if names.versioned in role_names:
            await pause_once()

    monkeypatch.setattr(
        role_common,
        "_role_owns_objects",
        paused_owner_probe,
    )
    monkeypatch.setattr(
        role_common,
        "quarantine_login_roles",
        paused_quarantine,
        raising=False,
    )

    retire_task: asyncio.Task[None] | None = None
    try:
        migrated = _run_alembic(_alembic_url(target_url))
        assert migrated.returncode == 0, migrated.stdout + migrated.stderr
        await runtime_cli._provision(
            target_url,
            service_name,
            generation,
            runtime_state,
            names,
        )
        paths = runtime_cli.credential_paths(
            runtime_state,
            service_name,
            generation,
        )
        forensic_snapshot = {
            purpose: (
                path.read_bytes(),
                stat.S_IMODE(path.stat().st_mode),
            )
            for purpose, path in paths.items()
        }
        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            await owner.execute(
                f'CREATE COLLATION public."{collation_name}" '
                'FROM pg_catalog."C"'
            )
            await owner.execute(
                "CREATE TEXT SEARCH CONFIGURATION "
                f'public."{configuration_name}" '
                "(COPY = pg_catalog.simple)"
            )
        finally:
            await owner.close()

        retire_task = asyncio.create_task(
            runtime_cli._revoke(
                target_url,
                service_name,
                generation,
                runtime_state,
                names,
            )
        )
        await asyncio.wait_for(pause_entered.wait(), timeout=10)
        concurrent_owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            await concurrent_owner.execute(
                f'ALTER COLLATION public."{collation_name}" '
                f'OWNER TO {role_common.quote_identifier(names.versioned)}'
            )
            await concurrent_owner.execute(
                "ALTER TEXT SEARCH CONFIGURATION "
                f'public."{configuration_name}" OWNER TO '
                f"{role_common.quote_identifier(names.versioned)}"
            )
        finally:
            await concurrent_owner.close()
        pause_release.set()
        result = await asyncio.gather(
            retire_task,
            return_exceptions=True,
        )
        assert isinstance(
            result[0],
            (asyncpg.PostgresError, runtime_cli.RuntimeRoleError),
        )
        owner_file = tmp_path / "owner-url"
        _write_secret(owner_file, target_url, 0o400)
        monkeypatch.setenv(
            runtime_cli.OWNER_URL_FILE_ENV,
            str(owner_file),
        )
        assert await runtime_cli.run(
            [
                "revoke",
                "--service-name",
                service_name,
                "--generation",
                str(generation),
                "--state-dir",
                str(runtime_state),
            ]
        ) == 4
        assert json.loads(capsys.readouterr().out) == {
            "code": "worker_runtime_role_operation_failed",
            "status": "error",
        }

        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert await owner.fetchval(
                "SELECT pg_catalog.to_regcollation($1) IS NOT NULL",
                f"public.{collation_name}",
            )
            assert await owner.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_ts_config AS configuration
                    JOIN pg_catalog.pg_namespace AS namespace
                      ON namespace.oid = configuration.cfgnamespace
                    WHERE namespace.nspname = 'public'
                      AND configuration.cfgname = $1
                )
                """,
                configuration_name,
            )
            role = await owner.fetchrow(
                """
                SELECT rolcanlogin
                FROM pg_catalog.pg_roles
                WHERE rolname = $1
                """,
                names.versioned,
            )
            assert role is not None
            assert not role["rolcanlogin"]
            assert not await _role_membership_edges(
                owner,
                names.versioned,
            )
        finally:
            await owner.close()
        assert {
            purpose: (
                path.read_bytes(),
                stat.S_IMODE(path.stat().st_mode),
            )
            for purpose, path in paths.items()
        } == forensic_snapshot
        assert "DROP OWNED" not in Path(
            role_common.__file__
        ).read_text(encoding="utf-8")
    finally:
        pause_release.set()
        if retire_task is not None and not retire_task.done():
            retire_task.cancel()
        admin = await asyncpg.connect(_asyncpg_url(admin_url))
        try:
            await admin.execute(
                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'
            )
            await _drop_test_roles(
                admin,
                (names.versioned, names.stable),
            )
        finally:
            await admin.close()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for live role tests",
)
async def test_runtime_and_control_share_database_dcl_lock_and_stress(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = f"vp_dcl_lock_{uuid.uuid4().hex[:20]}"
    runtime_services = tuple(
        f"task4a-dcl-{index}-{uuid.uuid4().hex[:6]}"
        for index in range(4)
    )
    runtime_generations = tuple(
        int(uuid.uuid4().hex[:12], 16)
        for _service_name in runtime_services
    )
    runtime_names = tuple(
        runtime_cli.role_names_for_generation(service_name, generation)
        for service_name, generation in zip(
            runtime_services,
            runtime_generations,
            strict=True,
        )
    )
    control_generations = tuple(
        f"dcl-{index}-{uuid.uuid4().hex[:8]}"
        for index in range(3)
    )
    control_names = tuple(
        control_cli.role_names_for_generation(generation)
        for generation in control_generations
    )
    admin_url = _database_url("postgres")
    target_url = _database_url(database)
    runtime_state = tmp_path / "runtime-state"
    control_state = tmp_path / "control-state"
    runtime_stable_entered = asyncio.Event()
    runtime_stable_release = asyncio.Event()
    control_stable_entered = asyncio.Event()
    paused = False

    admin = await asyncpg.connect(_asyncpg_url(admin_url))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()

    real_runtime_stable = runtime_cli.ensure_stable_role
    real_control_stable = control_cli.ensure_stable_role

    async def paused_runtime_stable(
        connection: asyncpg.Connection,
        role_name: str,
        *,
        setting_prefix: str,
        authorized_members: tuple[str, ...],
    ) -> None:
        nonlocal paused
        if not paused:
            paused = True
            runtime_stable_entered.set()
            await asyncio.wait_for(
                runtime_stable_release.wait(),
                timeout=10,
            )
        await real_runtime_stable(
            connection,
            role_name,
            setting_prefix=setting_prefix,
            authorized_members=authorized_members,
        )

    async def observed_control_stable(
        connection: asyncpg.Connection,
        role_name: str,
        *,
        setting_prefix: str,
        authorized_members: tuple[str, ...],
    ) -> None:
        control_stable_entered.set()
        await real_control_stable(
            connection,
            role_name,
            setting_prefix=setting_prefix,
            authorized_members=authorized_members,
        )

    monkeypatch.setattr(
        runtime_cli,
        "ensure_stable_role",
        paused_runtime_stable,
    )
    monkeypatch.setattr(
        control_cli,
        "ensure_stable_role",
        observed_control_stable,
    )

    first_runtime: asyncio.Task[None] | None = None
    first_control: asyncio.Task[None] | None = None
    try:
        migrated = _run_alembic(_alembic_url(target_url))
        assert migrated.returncode == 0, migrated.stdout + migrated.stderr
        first_runtime = asyncio.create_task(
            runtime_cli._provision(
                target_url,
                runtime_services[0],
                runtime_generations[0],
                runtime_state,
                runtime_names[0],
            )
        )
        await asyncio.wait_for(runtime_stable_entered.wait(), timeout=10)
        first_control = asyncio.create_task(
            control_cli._provision(
                target_url,
                control_generations[0],
                control_state,
                control_names[0],
            )
        )
        control_crossed_shared_boundary = False
        try:
            await asyncio.wait_for(
                control_stable_entered.wait(),
                timeout=0.4,
            )
            control_crossed_shared_boundary = True
        except TimeoutError:
            pass
        runtime_stable_release.set()
        first_results = await asyncio.wait_for(
            asyncio.gather(
                first_runtime,
                first_control,
                return_exceptions=True,
            ),
            timeout=20,
        )
        assert not control_crossed_shared_boundary
        assert not any(
            isinstance(result, BaseException)
            for result in first_results
        )

        monkeypatch.setattr(
            runtime_cli,
            "ensure_stable_role",
            real_runtime_stable,
        )
        monkeypatch.setattr(
            control_cli,
            "ensure_stable_role",
            real_control_stable,
        )
        for _round in range(3):
            provision_results = await asyncio.wait_for(
                asyncio.gather(
                    *(
                        runtime_cli._provision(
                            target_url,
                            service_name,
                            generation,
                            runtime_state,
                            names,
                        )
                        for service_name, generation, names in zip(
                            runtime_services,
                            runtime_generations,
                            runtime_names,
                            strict=True,
                        )
                    ),
                    *(
                        control_cli._provision(
                            target_url,
                            generation,
                            control_state,
                            names,
                        )
                        for generation, names in zip(
                            control_generations,
                            control_names,
                            strict=True,
                        )
                    ),
                    return_exceptions=True,
                ),
                timeout=60,
            )
            assert not any(
                isinstance(result, BaseException)
                for result in provision_results
            )

        revoke_results = await asyncio.wait_for(
            asyncio.gather(
                *(
                    runtime_cli._revoke(
                        target_url,
                        service_name,
                        generation,
                        runtime_state,
                        names,
                    )
                    for service_name, generation, names in zip(
                        runtime_services,
                        runtime_generations,
                        runtime_names,
                        strict=True,
                    )
                ),
                *(
                    control_cli._revoke(
                        target_url,
                        generation,
                        control_state,
                        names,
                    )
                    for generation, names in zip(
                        control_generations,
                        control_names,
                        strict=True,
                    )
                ),
                return_exceptions=True,
            ),
            timeout=60,
        )
        assert not any(
            isinstance(result, BaseException)
            for result in revoke_results
        )
    finally:
        runtime_stable_release.set()
        for task in (first_runtime, first_control):
            if task is not None and not task.done():
                task.cancel()
        admin = await asyncpg.connect(_asyncpg_url(admin_url))
        try:
            await admin.execute(
                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'
            )
            await _drop_test_roles(
                admin,
                (
                    *(names.versioned for names in runtime_names),
                    *(
                        versioned
                        for names in control_names
                        for versioned in names.versioned.values()
                    ),
                    runtime_names[0].stable,
                    *control_names[0].stable.values(),
                ),
            )
        finally:
            await admin.close()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for live role tests",
)
async def test_pg16_membership_metadata_and_delegated_grants_converge(
    tmp_path: Path,
) -> None:
    database = f"vp_role_grantor_{uuid.uuid4().hex[:20]}"
    service_name = f"task4a-grantor-{uuid.uuid4().hex[:8]}"
    generation = int(uuid.uuid4().hex[:12], 16)
    control_generation = f"grantor-{uuid.uuid4().hex[:12]}"
    runtime_names = runtime_cli.role_names_for_generation(
        service_name,
        generation,
    )
    control_names = control_cli.role_names_for_generation(control_generation)
    runtime_outsider = f"vp_t4a_runtime_out_{uuid.uuid4().hex[:10]}"
    control_outsider = f"vp_t4a_control_out_{uuid.uuid4().hex[:10]}"
    runtime_unrelated = f"vp_t4a_runtime_other_{uuid.uuid4().hex[:8]}"
    control_unrelated = f"vp_t4a_control_other_{uuid.uuid4().hex[:8]}"
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
        await control_cli._provision(
            target_url,
            control_generation,
            control_state,
            control_names,
        )
        await runtime_cli._provision(
            target_url,
            service_name,
            generation,
            runtime_state,
            runtime_names,
        )

        runtime_paths = runtime_cli.credential_paths(
            runtime_state,
            service_name,
            generation,
        )
        control_paths = control_cli.credential_paths(
            control_state,
            control_generation,
        )
        runtime_url = runtime_paths["database_url"].read_text().strip()
        admission_token = (
            runtime_paths["admission_token"].read_text().strip()
        )
        operator_url = control_paths["operator"].read_text().strip()

        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            expected_grantor = await owner.fetchval("SELECT current_user")
            await owner.execute(
                f"CREATE ROLE {role_common.quote_identifier(runtime_outsider)} "
                "LOGIN"
            )
            await owner.execute(
                f"CREATE ROLE {role_common.quote_identifier(control_outsider)} "
                "LOGIN"
            )
            await owner.execute(
                f"CREATE ROLE {role_common.quote_identifier(runtime_unrelated)} "
                "NOLOGIN"
            )
            await owner.execute(
                f"CREATE ROLE {role_common.quote_identifier(control_unrelated)} "
                "NOLOGIN"
            )
            await owner.execute(
                f"GRANT {role_common.quote_identifier(runtime_unrelated)} "
                f"TO {role_common.quote_identifier(runtime_outsider)}"
            )
            await owner.execute(
                f"GRANT {role_common.quote_identifier(control_unrelated)} "
                f"TO {role_common.quote_identifier(control_outsider)}"
            )
            await owner.execute(
                f"GRANT {role_common.quote_identifier(runtime_names.stable)} "
                f"TO {role_common.quote_identifier(runtime_names.versioned)} "
                "WITH ADMIN TRUE"
            )
            await owner.execute(
                f"GRANT {role_common.quote_identifier(runtime_names.stable)} "
                f"TO {role_common.quote_identifier(runtime_names.versioned)} "
                "WITH INHERIT FALSE"
            )
            await owner.execute(
                f"GRANT {role_common.quote_identifier(runtime_names.stable)} "
                f"TO {role_common.quote_identifier(runtime_names.versioned)} "
                "WITH SET FALSE"
            )
        finally:
            await owner.close()

        runtime = await asyncpg.connect(_asyncpg_url(runtime_url))
        try:
            await runtime.execute(
                f"GRANT {role_common.quote_identifier(runtime_names.stable)} "
                f"TO {role_common.quote_identifier(runtime_outsider)}"
            )
        finally:
            await runtime.close()

        await runtime_cli._provision(
            target_url,
            service_name,
            generation,
            runtime_state,
            runtime_names,
        )
        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert await _role_membership_rows(
                owner,
                (runtime_names.stable, runtime_names.versioned),
            ) == [
                {
                    "granted_role": runtime_names.stable,
                    "member_role": runtime_names.versioned,
                    "grantor_role": expected_grantor,
                    "admin_option": False,
                    "inherit_option": True,
                    "set_option": True,
                }
            ]
            assert not await owner.fetchval(
                "SELECT pg_catalog.pg_has_role($1, $2, 'MEMBER')",
                runtime_outsider,
                runtime_names.stable,
            )
            assert await owner.fetchval(
                "SELECT pg_catalog.pg_has_role($1, $2, 'MEMBER')",
                runtime_outsider,
                runtime_unrelated,
            )
            await owner.execute(
                f"GRANT {role_common.quote_identifier(runtime_names.stable)} "
                f"TO {role_common.quote_identifier(runtime_names.versioned)} "
                "WITH ADMIN TRUE"
            )
            await owner.execute(
                f"GRANT {role_common.quote_identifier(runtime_names.stable)} "
                f"TO {role_common.quote_identifier(runtime_names.versioned)} "
                "WITH INHERIT FALSE"
            )
            await owner.execute(
                f"GRANT {role_common.quote_identifier(runtime_names.stable)} "
                f"TO {role_common.quote_identifier(runtime_names.versioned)} "
                "WITH SET FALSE"
            )
        finally:
            await owner.close()
        runtime = await asyncpg.connect(_asyncpg_url(runtime_url))
        try:
            await runtime.execute(
                f"GRANT {role_common.quote_identifier(runtime_names.stable)} "
                f"TO {role_common.quote_identifier(runtime_outsider)}"
            )
        finally:
            await runtime.close()

        operator = await asyncpg.connect(_asyncpg_url(operator_url))
        try:
            await _upsert_worker_grant(
                operator,
                service_name=service_name,
                generation=generation,
                database_principal=runtime_names.versioned,
                admission_token=admission_token,
            )
            await operator.fetchval(
                "SELECT public.vp_worker_grant_activate($1, $2)",
                service_name,
                generation,
            )
        finally:
            await operator.close()

        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            runtime_rows = await _role_membership_rows(
                owner,
                (
                    runtime_names.stable,
                    runtime_names.versioned,
                ),
            )
            assert runtime_rows == [
                {
                    "granted_role": runtime_names.stable,
                    "member_role": runtime_names.versioned,
                    "grantor_role": expected_grantor,
                    "admin_option": False,
                    "inherit_option": True,
                    "set_option": True,
                }
            ]
            assert not await owner.fetchval(
                "SELECT pg_catalog.pg_has_role($1, $2, 'MEMBER')",
                runtime_outsider,
                runtime_names.stable,
            )
            assert await owner.fetchval(
                "SELECT pg_catalog.pg_has_role($1, $2, 'MEMBER')",
                runtime_outsider,
                runtime_unrelated,
            )

            for purpose, stable_role in control_names.stable.items():
                versioned_role = control_names.versioned[purpose]
                await owner.execute(
                    f"GRANT {role_common.quote_identifier(stable_role)} "
                    f"TO {role_common.quote_identifier(versioned_role)} "
                    "WITH ADMIN TRUE"
                )
                await owner.execute(
                    f"GRANT {role_common.quote_identifier(stable_role)} "
                    f"TO {role_common.quote_identifier(versioned_role)} "
                    "WITH INHERIT FALSE"
                )
                await owner.execute(
                    f"GRANT {role_common.quote_identifier(stable_role)} "
                    f"TO {role_common.quote_identifier(versioned_role)} "
                    "WITH SET FALSE"
                )
        finally:
            await owner.close()

        old_operator = await asyncpg.connect(_asyncpg_url(operator_url))
        try:
            await old_operator.execute(
                "GRANT "
                f"{role_common.quote_identifier(control_names.stable['operator'])} "
                f"TO {role_common.quote_identifier(control_outsider)}"
            )
        finally:
            await old_operator.close()

        await control_cli._provision(
            target_url,
            control_generation,
            control_state,
            control_names,
        )
        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            for purpose, stable_role in control_names.stable.items():
                versioned_role = control_names.versioned[purpose]
                rows = await _role_membership_rows(
                    owner,
                    (stable_role, versioned_role),
                )
                assert rows == [
                    {
                        "granted_role": stable_role,
                        "member_role": versioned_role,
                        "grantor_role": expected_grantor,
                        "admin_option": False,
                        "inherit_option": True,
                        "set_option": True,
                    }
                ]
            assert not await owner.fetchval(
                "SELECT pg_catalog.pg_has_role($1, $2, 'MEMBER')",
                control_outsider,
                control_names.stable["operator"],
            )
            assert await owner.fetchval(
                "SELECT pg_catalog.pg_has_role($1, $2, 'MEMBER')",
                control_outsider,
                control_unrelated,
            )
            await owner.execute(
                f"GRANT {role_common.quote_identifier(runtime_names.stable)} "
                f"TO {role_common.quote_identifier(runtime_names.versioned)} "
                "WITH ADMIN TRUE"
            )
        finally:
            await owner.close()

        runtime = await asyncpg.connect(_asyncpg_url(runtime_url))
        try:
            await runtime.execute(
                f"GRANT {role_common.quote_identifier(runtime_names.stable)} "
                f"TO {role_common.quote_identifier(runtime_outsider)}"
            )
        finally:
            await runtime.close()

        operator = await asyncpg.connect(_asyncpg_url(operator_url))
        try:
            assert await operator.fetchval(
                "SELECT public.vp_worker_grant_revoke($1, $2, $3)",
                service_name,
                generation,
                "task4a-round6-delegated-grant",
            )
        finally:
            await operator.close()

        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert await owner.fetchval(
                """
                SELECT state = 'revoked' AND revoked_at IS NOT NULL
                FROM public.worker_admission_grants
                WHERE service_name = $1 AND generation = $2
                """,
                service_name,
                generation,
            )
            revoked_role = await owner.fetchrow(
                """
                SELECT rolcanlogin
                FROM pg_catalog.pg_roles
                WHERE rolname = $1
                """,
                runtime_names.versioned,
            )
            assert revoked_role is not None
            assert not revoked_role["rolcanlogin"]
            assert not await _role_membership_rows(
                owner,
                (runtime_names.versioned,),
            )
            assert not await owner.fetchval(
                "SELECT pg_catalog.pg_has_role($1, $2, 'MEMBER')",
                runtime_outsider,
                runtime_names.stable,
            )
        finally:
            await owner.close()
        with pytest.raises(asyncpg.PostgresError):
            await asyncpg.connect(_asyncpg_url(runtime_url))
    finally:
        admin = await asyncpg.connect(_asyncpg_url(admin_url))
        try:
            await admin.execute(
                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'
            )
            await _drop_test_roles(
                admin,
                (
                    runtime_unrelated,
                    control_unrelated,
                    runtime_outsider,
                    control_outsider,
                    runtime_names.versioned,
                    *control_names.versioned.values(),
                    runtime_names.stable,
                    *control_names.stable.values(),
                ),
            )
        finally:
            await admin.close()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for live role tests",
)
async def test_invalid_state_tree_boundaries_quarantine_current_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = f"vp_role_root_{uuid.uuid4().hex[:20]}"
    services = tuple(
        f"task4a-root-{index}-{uuid.uuid4().hex[:6]}"
        for index in range(4)
    )
    generations = tuple(
        int(uuid.uuid4().hex[:12], 16) for _service in services
    )
    runtime_names = tuple(
        runtime_cli.role_names_for_generation(service, generation)
        for service, generation in zip(
            services,
            generations,
            strict=True,
        )
    )
    control_generation = f"root-{uuid.uuid4().hex[:12]}"
    control_names = control_cli.role_names_for_generation(control_generation)
    admin_url = _database_url("postgres")
    target_url = _database_url(database)
    runtime_state = tmp_path / "runtime-state"
    control_state = tmp_path / "control-state"
    old_operator: asyncpg.Connection | None = None

    admin = await asyncpg.connect(_asyncpg_url(admin_url))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
    finally:
        await admin.close()

    try:
        migrated = _run_alembic(_alembic_url(target_url))
        assert migrated.returncode == 0, migrated.stdout + migrated.stderr
        await runtime_cli._provision(
            target_url,
            services[0],
            generations[0],
            runtime_state,
            runtime_names[0],
        )
        await control_cli._provision(
            target_url,
            control_generation,
            control_state,
            control_names,
        )
        runtime_paths = runtime_cli.credential_paths(
            runtime_state,
            services[0],
            generations[0],
        )
        control_paths = control_cli.credential_paths(
            control_state,
            control_generation,
        )
        runtime_urls = [
            runtime_paths["database_url"].read_text().strip()
        ]
        operator_url = control_paths["operator"].read_text().strip()
        runtime_root_snapshot = {
            purpose: path.read_bytes()
            for purpose, path in runtime_paths.items()
        }
        control_root_snapshot = {
            purpose: path.read_bytes()
            for purpose, path in control_paths.items()
        }
        old_operator = await asyncpg.connect(_asyncpg_url(operator_url))

        runtime_state.chmod(0o755)
        with pytest.raises(
            (
                asyncpg.PostgresError,
                OSError,
                runtime_cli.RuntimeRoleError,
                role_common.WorkerRoleCommonError,
            )
        ):
            await runtime_cli._provision(
                target_url,
                services[0],
                generations[0],
                runtime_state,
                runtime_names[0],
            )
        control_state.chmod(0o755)
        with pytest.raises(
            (
                asyncpg.PostgresError,
                OSError,
                control_cli.ControlRoleError,
                role_common.WorkerRoleCommonError,
            )
        ):
            await control_cli._provision(
                target_url,
                control_generation,
                control_state,
                control_names,
            )

        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            for role_name in (
                runtime_names[0].versioned,
                *control_names.versioned.values(),
            ):
                quarantined_role = await owner.fetchrow(
                    """
                    SELECT rolcanlogin
                    FROM pg_catalog.pg_roles
                    WHERE rolname = $1
                    """,
                    role_name,
                )
                assert quarantined_role is not None
                assert not quarantined_role["rolcanlogin"]
                assert not await _role_membership_rows(owner, (role_name,))
        finally:
            await owner.close()
        assert {
            purpose: path.read_bytes()
            for purpose, path in runtime_paths.items()
        } == runtime_root_snapshot
        assert {
            purpose: path.read_bytes()
            for purpose, path in control_paths.items()
        } == control_root_snapshot
        with pytest.raises(asyncpg.PostgresError):
            await asyncpg.connect(_asyncpg_url(runtime_urls[0]))
        with pytest.raises(asyncpg.PostgresError):
            await asyncpg.connect(_asyncpg_url(operator_url))
        with pytest.raises(
            (
                asyncpg.PostgresError,
                asyncpg.InterfaceError,
                ConnectionError,
            )
        ):
            await old_operator.fetchval(
                "SELECT public.vp_worker_grant_activate($1, $2)",
                services[0],
                generations[0],
            )

        runtime_state.chmod(0o700)
        control_state.chmod(0o700)
        await runtime_cli._provision(
            target_url,
            services[1],
            generations[1],
            runtime_state,
            runtime_names[1],
        )
        service_paths = runtime_cli.credential_paths(
            runtime_state,
            services[1],
            generations[1],
        )
        runtime_urls.append(
            service_paths["database_url"].read_text().strip()
        )
        service_snapshot = {
            purpose: path.read_bytes()
            for purpose, path in service_paths.items()
        }
        service_dir = runtime_state / services[1]
        service_dir.chmod(0o755)
        with pytest.raises(
            (
                asyncpg.PostgresError,
                OSError,
                runtime_cli.RuntimeRoleError,
                role_common.WorkerRoleCommonError,
            )
        ):
            await runtime_cli._provision(
                target_url,
                services[1],
                generations[1],
                runtime_state,
                runtime_names[1],
            )
        service_dir.chmod(0o700)

        await runtime_cli._provision(
            target_url,
            services[2],
            generations[2],
            runtime_state,
            runtime_names[2],
        )
        symlink_paths = runtime_cli.credential_paths(
            runtime_state,
            services[2],
            generations[2],
        )
        runtime_urls.append(
            symlink_paths["database_url"].read_text().strip()
        )
        symlink_snapshot = {
            purpose: path.read_bytes()
            for purpose, path in symlink_paths.items()
        }
        symlink_service_dir = runtime_state / services[2]
        evidence_service_dir = runtime_state / f"{services[2]}.evidence"
        symlink_service_dir.rename(evidence_service_dir)
        symlink_service_dir.symlink_to(
            evidence_service_dir,
            target_is_directory=True,
        )
        with pytest.raises(
            (
                asyncpg.PostgresError,
                OSError,
                runtime_cli.RuntimeRoleError,
                role_common.WorkerRoleCommonError,
            )
        ):
            await runtime_cli._provision(
                target_url,
                services[2],
                generations[2],
                runtime_state,
                runtime_names[2],
            )
        symlink_service_dir.unlink()
        evidence_service_dir.rename(symlink_service_dir)

        await runtime_cli._provision(
            target_url,
            services[3],
            generations[3],
            runtime_state,
            runtime_names[3],
        )
        owner_paths = runtime_cli.credential_paths(
            runtime_state,
            services[3],
            generations[3],
        )
        runtime_urls.append(owner_paths["database_url"].read_text().strip())
        owner_snapshot = {
            purpose: path.read_bytes()
            for purpose, path in owner_paths.items()
        }
        actual_euid = os.geteuid()
        with monkeypatch.context() as local_patch:
            local_patch.setattr(
                runtime_cli.os,
                "geteuid",
                lambda: actual_euid + 1,
            )
            with pytest.raises(
                (
                    asyncpg.PostgresError,
                    OSError,
                    runtime_cli.RuntimeRoleError,
                    role_common.WorkerRoleCommonError,
                )
            ):
                await runtime_cli._provision(
                    target_url,
                    services[3],
                    generations[3],
                    runtime_state,
                    runtime_names[3],
                )

        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            for names in runtime_names:
                quarantined_role = await owner.fetchrow(
                    """
                    SELECT rolcanlogin
                    FROM pg_catalog.pg_roles
                    WHERE rolname = $1
                    """,
                    names.versioned,
                )
                assert quarantined_role is not None
                assert not quarantined_role["rolcanlogin"]
                assert not await _role_membership_rows(
                    owner,
                    (names.versioned,),
                )
        finally:
            await owner.close()
        assert {
            purpose: path.read_bytes()
            for purpose, path in service_paths.items()
        } == service_snapshot
        assert {
            purpose: path.read_bytes()
            for purpose, path in symlink_paths.items()
        } == symlink_snapshot
        assert {
            purpose: path.read_bytes()
            for purpose, path in owner_paths.items()
        } == owner_snapshot
        for runtime_url in runtime_urls:
            with pytest.raises(asyncpg.PostgresError):
                await asyncpg.connect(_asyncpg_url(runtime_url))
    finally:
        if old_operator is not None and not old_operator.is_closed():
            await old_operator.close()
        for path in (runtime_state, control_state):
            if path.exists() and not path.is_symlink():
                path.chmod(0o700)
        admin = await asyncpg.connect(_asyncpg_url(admin_url))
        try:
            await admin.execute(
                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'
            )
            await _drop_test_roles(
                admin,
                (
                    *(names.versioned for names in runtime_names),
                    *control_names.versioned.values(),
                    runtime_names[0].stable,
                    *control_names.stable.values(),
                ),
            )
        finally:
            await admin.close()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for live role tests",
)
async def test_state_root_replacement_and_revoke_race_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = f"vp_role_root_race_{uuid.uuid4().hex[:16]}"
    replacement_service = f"task4a-replace-{uuid.uuid4().hex[:8]}"
    replacement_generation = int(uuid.uuid4().hex[:12], 16)
    replacement_names = runtime_cli.role_names_for_generation(
        replacement_service,
        replacement_generation,
    )
    race_service = f"task4a-root-race-{uuid.uuid4().hex[:8]}"
    race_generation = int(uuid.uuid4().hex[:12], 16)
    race_names = runtime_cli.role_names_for_generation(
        race_service,
        race_generation,
    )
    control_generation = f"root-race-{uuid.uuid4().hex[:10]}"
    control_names = control_cli.role_names_for_generation(control_generation)
    delegated_outsider = f"vp_t4a_race_out_{uuid.uuid4().hex[:10]}"
    admin_url = _database_url("postgres")
    target_url = _database_url(database)
    replacement_state = tmp_path / "replacement-state"
    replacement_evidence = tmp_path / "replacement-state-evidence"
    race_state = tmp_path / "race-state"
    control_state = tmp_path / "control-state"

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
        await runtime_cli._provision(
            target_url,
            replacement_service,
            replacement_generation,
            replacement_state,
            replacement_names,
        )
        replacement_paths = runtime_cli.credential_paths(
            replacement_state,
            replacement_service,
            replacement_generation,
        )
        replacement_url = (
            replacement_paths["database_url"].read_text().strip()
        )
        replacement_snapshot = {
            purpose: path.read_bytes()
            for purpose, path in replacement_paths.items()
        }
        real_managed_services = runtime_cli._managed_runtime_service_names
        real_authorized_members = runtime_cli._authorized_runtime_members
        replaced = False
        authorized_scan_after_replacement = False

        def replace_after_initial_scan(
            state_dir: Path,
            current_service_name: str,
        ) -> tuple[str, ...]:
            nonlocal replaced
            result = real_managed_services(state_dir, current_service_name)
            if not replaced:
                replaced = True
                state_dir.rename(replacement_evidence)
                shutil.copytree(replacement_evidence, state_dir)
            return result

        async def reject_replacement_authority_scan(
            connection: asyncpg.Connection,
            owner_url: str,
            state_dir: Path,
            service_name: str,
        ) -> set[str]:
            nonlocal authorized_scan_after_replacement
            authorized_scan_after_replacement = True
            raise AssertionError("replacement root must not authorize roles")

        monkeypatch.setattr(
            runtime_cli,
            "_managed_runtime_service_names",
            replace_after_initial_scan,
        )
        monkeypatch.setattr(
            runtime_cli,
            "_authorized_runtime_members",
            reject_replacement_authority_scan,
        )
        with pytest.raises(
            (
                asyncpg.PostgresError,
                OSError,
                runtime_cli.RuntimeRoleError,
                role_common.WorkerRoleCommonError,
            )
        ):
            await runtime_cli._provision(
                target_url,
                replacement_service,
                replacement_generation,
                replacement_state,
                replacement_names,
            )
        assert not authorized_scan_after_replacement
        monkeypatch.setattr(
            runtime_cli,
            "_managed_runtime_service_names",
            real_managed_services,
        )
        monkeypatch.setattr(
            runtime_cli,
            "_authorized_runtime_members",
            real_authorized_members,
        )

        original_evidence_paths = runtime_cli.credential_paths(
            replacement_evidence,
            replacement_service,
            replacement_generation,
        )
        assert {
            purpose: path.read_bytes()
            for purpose, path in original_evidence_paths.items()
        } == replacement_snapshot
        assert {
            purpose: path.read_bytes()
            for purpose, path in replacement_paths.items()
        } == replacement_snapshot

        await runtime_cli._provision(
            target_url,
            race_service,
            race_generation,
            race_state,
            race_names,
        )
        race_paths = runtime_cli.credential_paths(
            race_state,
            race_service,
            race_generation,
        )
        race_url = race_paths["database_url"].read_text().strip()
        race_token = race_paths["admission_token"].read_text().strip()
        race_snapshot = {
            purpose: path.read_bytes()
            for purpose, path in race_paths.items()
        }
        operator = await asyncpg.connect(_asyncpg_url(operator_url))
        try:
            await _upsert_worker_grant(
                operator,
                service_name=race_service,
                generation=race_generation,
                database_principal=race_names.versioned,
                admission_token=race_token,
            )
            await operator.fetchval(
                "SELECT public.vp_worker_grant_activate($1, $2)",
                race_service,
                race_generation,
            )
        finally:
            await operator.close()

        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            await owner.execute(
                f"CREATE ROLE "
                f"{role_common.quote_identifier(delegated_outsider)} LOGIN"
            )
            await owner.execute(
                f"GRANT {role_common.quote_identifier(race_names.stable)} "
                f"TO {role_common.quote_identifier(race_names.versioned)} "
                "WITH ADMIN TRUE"
            )
        finally:
            await owner.close()
        runtime = await asyncpg.connect(_asyncpg_url(race_url))
        try:
            await runtime.execute(
                f"GRANT {role_common.quote_identifier(race_names.stable)} "
                f"TO {role_common.quote_identifier(delegated_outsider)}"
            )
        finally:
            await runtime.close()

        (race_state / race_service).chmod(0o755)
        operator = await asyncpg.connect(_asyncpg_url(operator_url))
        try:
            provision_result, revoke_result = await asyncio.wait_for(
                asyncio.gather(
                    runtime_cli._provision(
                        target_url,
                        race_service,
                        race_generation,
                        race_state,
                        race_names,
                    ),
                    operator.fetchval(
                        "SELECT public.vp_worker_grant_revoke($1, $2, $3)",
                        race_service,
                        race_generation,
                        "task4a-round6-invalid-root-race",
                    ),
                    return_exceptions=True,
                ),
                timeout=20,
            )
        finally:
            await operator.close()
        assert isinstance(
            provision_result,
            (
                asyncpg.PostgresError,
                OSError,
                runtime_cli.RuntimeRoleError,
                role_common.WorkerRoleCommonError,
            ),
        )
        assert revoke_result is True

        owner = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            for role_name in (
                replacement_names.versioned,
                race_names.versioned,
            ):
                quarantined_role = await owner.fetchrow(
                    """
                    SELECT rolcanlogin
                    FROM pg_catalog.pg_roles
                    WHERE rolname = $1
                    """,
                    role_name,
                )
                assert quarantined_role is not None
                assert not quarantined_role["rolcanlogin"]
                assert not await _role_membership_rows(owner, (role_name,))
            assert await owner.fetchval(
                """
                SELECT state = 'revoked' AND revoked_at IS NOT NULL
                FROM public.worker_admission_grants
                WHERE service_name = $1 AND generation = $2
                """,
                race_service,
                race_generation,
            )
            assert not await owner.fetchval(
                "SELECT pg_catalog.pg_has_role($1, $2, 'MEMBER')",
                delegated_outsider,
                race_names.stable,
            )
        finally:
            await owner.close()
        assert {
            purpose: path.read_bytes()
            for purpose, path in race_paths.items()
        } == race_snapshot
        with pytest.raises(asyncpg.PostgresError):
            await asyncpg.connect(_asyncpg_url(replacement_url))
        with pytest.raises(asyncpg.PostgresError):
            await asyncpg.connect(_asyncpg_url(race_url))
    finally:
        for path in (replacement_state, replacement_evidence, race_state):
            if path.exists() and not path.is_symlink():
                path.chmod(0o700)
                for child in path.iterdir():
                    if child.is_dir() and not child.is_symlink():
                        child.chmod(0o700)
        admin = await asyncpg.connect(_asyncpg_url(admin_url))
        try:
            await admin.execute(
                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'
            )
            await _drop_test_roles(
                admin,
                (
                    delegated_outsider,
                    replacement_names.versioned,
                    race_names.versioned,
                    *control_names.versioned.values(),
                    replacement_names.stable,
                    *control_names.stable.values(),
                ),
            )
        finally:
            await admin.close()
