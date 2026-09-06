from __future__ import annotations

import asyncio
import os
import secrets
import sys
import uuid
from pathlib import Path

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from app.services import worker_control_role_cli as control
from app.services import worker_role_cli_common as common
from app.services import worker_runtime_role_cli as runtime


POSTGRES_URL = os.environ.get("WORKER_ROLE_LIFECYCLE_POSTGRES_TEST_URL") or os.environ.get(
    "CHANNEL_OPS_POSTGRES_TEST_URL", "",
)
pytestmark = pytest.mark.skipif(not POSTGRES_URL, reason="isolated PG16 URL required")


async def test_non_superuser_provision_retry_and_revoke(tmp_path, monkeypatch):
    suffix = uuid.uuid4().hex[:16]
    database = f"vp_lifecycle_{suffix}"
    deploy = f"vp_deploy_{suffix}"
    outsider = f"vp_outside_{suffix}"
    password = secrets.token_urlsafe(32)
    monkeypatch.setattr(runtime, "STABLE_ROLE", f"vp_runtime_{suffix}")
    monkeypatch.setattr(control, "STABLE_ROLES", {
        purpose: f"vp_{purpose}_{suffix}" for purpose in control.STABLE_ROLES
    })
    service = f"lifecycle-{suffix}"
    generation = f"t-{suffix}"
    runtime_names = runtime.role_names_for_generation(service, 1)
    control_names = control.role_names_for_generation(generation)
    stable_roles = [runtime_names.stable, *control_names.stable.values()]
    login_roles = [runtime_names.versioned, *control_names.versioned.values()]
    roles = [deploy, outsider, *stable_roles, *login_roles]
    admin_url = make_url(POSTGRES_URL).set(drivername="postgresql")
    target_url = admin_url.set(database=database).render_as_string(hide_password=False)
    deploy_url = admin_url.set(
        database=database, username=deploy, password=password,
    ).render_as_string(hide_password=False)
    admin = await asyncpg.connect(admin_url.render_as_string(hide_password=False))
    owner = None
    principal = None
    try:
        assert 160000 <= int(await admin.fetchval("SHOW server_version_num")) < 170000
        assert await admin.fetchval("SELECT oid FROM pg_roles WHERE rolname = current_user") == 10
        await admin.execute(f'CREATE DATABASE "{database}"')
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "alembic", "upgrade", "head",
            cwd=Path(__file__).resolve().parents[2],
            env={**os.environ, "DATABASE_URL": target_url.replace(
                "postgresql://", "postgresql+asyncpg://", 1,
            )},
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        assert process.returncode == 0, (stdout + stderr).decode()
        owner = await asyncpg.connect(target_url)
        await owner.execute(
            f'CREATE ROLE "{deploy}" LOGIN INHERIT CREATEROLE PASSWORD \'{password}\''
        )
        await owner.execute(f'CREATE ROLE "{outsider}" NOLOGIN')
        await owner.execute(f'ALTER DATABASE "{database}" OWNER TO "{deploy}"')
        await owner.execute(f'ALTER SCHEMA public OWNER TO "{deploy}"')
        for relation in await owner.fetch("""
            SELECT relname, relkind FROM pg_class
            WHERE relnamespace = 'public'::regnamespace
              AND relkind IN ('r', 'p', 'S', 'v', 'm', 'f')
            ORDER BY (relkind = 'S'), relname
        """):
            kind = {"S": "SEQUENCE", "v": "VIEW", "m": "MATERIALIZED VIEW",
                    "f": "FOREIGN TABLE"}.get(relation["relkind"], "TABLE")
            await owner.execute(
                f'ALTER {kind} public.{common.quote_identifier(relation["relname"])} '
                f'OWNER TO "{deploy}"'
            )
        for routine in await owner.fetch("""
            SELECT oid::regprocedure::text AS signature FROM pg_proc
            WHERE pronamespace = 'public'::regnamespace
        """):
            await owner.execute(f'ALTER ROUTINE {routine["signature"]} OWNER TO "{deploy}"')
        for datatype in await owner.fetch("""
            SELECT typname, typtype FROM pg_type
            WHERE typnamespace = 'public'::regnamespace AND typtype IN ('d', 'e')
        """):
            kind = "DOMAIN" if datatype["typtype"] == "d" else "TYPE"
            await owner.execute(
                f'ALTER {kind} public.{common.quote_identifier(datatype["typname"])} '
                f'OWNER TO "{deploy}"'
            )
        # Existing control roles get the same bootstrap admin-only edge as CREATE ROLE.
        for role in control_names.stable.values():
            await owner.execute(f'CREATE ROLE "{role}" NOLOGIN NOINHERIT')
            await owner.execute(
                f'GRANT "{role}" TO "{deploy}" WITH ADMIN TRUE, INHERIT FALSE, SET FALSE'
            )
        principal = await asyncpg.connect(deploy_url)
        assert not await principal.fetchval("""
            SELECT EXISTS (SELECT 1 FROM pg_auth_members
            WHERE member = current_user::regrole AND (inherit_option OR set_option))
        """)
        runtime_state = tmp_path / "runtime"
        control_state = tmp_path / "control"
        await runtime._provision(deploy_url, service, 1, runtime_state, runtime_names)
        await control._provision(deploy_url, generation, control_state, control_names)
        paths = [*runtime.credential_paths(runtime_state, service, 1).values(),
                 *control.credential_paths(control_state, generation).values()]
        credentials = {path: path.read_bytes() for path in paths}
        await principal.execute(
            f'GRANT UPDATE (storage_path) ON public.artifacts TO "{runtime_names.stable}"'
        )
        await principal.execute(
            f'GRANT UPDATE (storage_path) ON public.artifacts TO "{control_names.stable["staging_janitor"]}"'
        )
        await principal.execute("GRANT SELECT (storage_path) ON public.artifacts TO PUBLIC")
        await runtime._provision(deploy_url, service, 1, runtime_state, runtime_names)
        await control._provision(deploy_url, generation, control_state, control_names)
        assert credentials == {path: path.read_bytes() for path in paths}
        for role in [runtime_names.versioned, control_names.versioned["staging_janitor"]]:
            assert not await principal.fetchval(
                "SELECT has_column_privilege($1, 'public.artifacts', 'storage_path', 'UPDATE')", role,
            )
        assert await principal.fetchval(
            "SELECT has_column_privilege($1, 'public.artifacts', 'storage_path', 'SELECT')",
            control_names.versioned["staging_janitor"],
        )
        for purpose, role in control_names.versioned.items():
            for signature in control.ROLE_FUNCTIONS[purpose]:
                assert await principal.fetchval(
                    "SELECT has_function_privilege($1, $2, 'EXECUTE')", role, f"public.{signature}",
                )
        for role in [*stable_roles, *login_roles]:
            assert dict(await principal.fetchrow("""
                SELECT admin_option, inherit_option, set_option, grantor::int
                FROM pg_auth_members WHERE roleid = $1::regrole
                  AND member = current_user::regrole
            """, role)) == {
                "admin_option": True, "inherit_option": False,
                "set_option": False, "grantor": 10,
            }
            assert not await principal.fetchval("SELECT pg_has_role($1, 'SET')", role)
            assert not await principal.fetchval("SELECT pg_has_role($1, 'USAGE')", role)

        # An inaccessible owner's explicit defaults must fail closed, not be altered.
        await owner.execute(
            f'ALTER DEFAULT PRIVILEGES FOR ROLE "{outsider}" GRANT SELECT ON TABLES TO PUBLIC'
        )
        with pytest.raises(common.WorkerRoleCommonError, match="PUBLIC privileges remain"):
            async with principal.transaction():
                await common.reset_public_privileges(principal, runtime_names.stable)
        await owner.execute(
            f'ALTER DEFAULT PRIVILEGES FOR ROLE "{outsider}" REVOKE SELECT ON TABLES FROM PUBLIC'
        )
        for option in ("INHERIT", "SET"):
            await owner.execute(f'GRANT "{runtime_names.stable}" TO "{deploy}" WITH {option} TRUE')
            with pytest.raises(common.WorkerRoleCommonError, match="creator membership invalid"):
                async with principal.transaction():
                    await common.revoke_role_membership_authority(principal, (runtime_names.stable,))
            await owner.execute(f'GRANT "{runtime_names.stable}" TO "{deploy}" WITH {option} FALSE')

        await runtime._revoke(deploy_url, service, 1, runtime_state, runtime_names)
        await control._revoke(deploy_url, generation, control_state, control_names)
        assert not await owner.fetchval("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = ANY($1::text[]))", login_roles)
        assert not any(path.exists() for path in paths)
        assert await owner.fetchval("SELECT count(*) FROM public.alembic_version") == 1
    finally:
        if principal is not None:
            await principal.close()
        if owner is not None:
            await owner.close()
        await admin.execute(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')
        for role in reversed(roles):
            await admin.execute(f'DROP ROLE IF EXISTS "{role}"')
        await admin.close()
