from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
import sys
import uuid
from contextlib import AsyncExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from app.services.worker_runtime_role_cli import role_names_for_generation
from app.services.worker_session_signal_sql import bootstrap_sql


POSTGRES_URL = os.getenv("CHANNEL_OPS_POSTGRES_TEST_URL", "")
BACKEND_ROOT = Path(__file__).resolve().parents[2]
PREVIOUS_REVISION = "034_worker_registrations"
TARGET_REVISION = "036_worker_session_signal"
SERVICE = "vp-ffmpeg-worker-go-swarm"
RUNTIME_ROLE = "vp_worker_runtime"
ENDPOINT_BINDINGS = next(
    case["canonical"]
    for case in json.loads(
        (BACKEND_ROOT.parent / "tests/fixtures/worker_registration/fingerprints-v1.json")
        .read_text()
    )["cases"]
    if case["name"] == "not_applicable"
)
OPERATOR_SIGNATURES = (
    "vp_worker_grant_upsert(text,bigint,text,text,jsonb,text,text,text,text,text,"
    "jsonb,text,text)",
    "vp_worker_grant_activate(text,bigint)",
    "vp_worker_grant_revoke(text,bigint,text)",
)
pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not POSTGRES_URL,
        reason="set CHANNEL_OPS_POSTGRES_TEST_URL to an isolated local PG16 server",
    ),
]


def _url(database: str, *, user: str | None = None, password: str = "") -> str:
    url = make_url(POSTGRES_URL).set(drivername="postgresql", database=database)
    if user is not None:
        url = url.set(username=user, password=password)
    return url.render_as_string(hide_password=False)


async def _connect(url: str) -> asyncpg.Connection:
    return await asyncpg.connect(
        url,
        timeout=10,
        command_timeout=15,
        server_settings={"statement_timeout": "10000", "lock_timeout": "5000"},
    )


def _migrate(url: str, revision: str) -> None:
    completed = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", revision],
        cwd=BACKEND_ROOT,
        env={
            **os.environ,
            "DATABASE_URL": url.replace("postgresql://", "postgresql+asyncpg://", 1),
        },
        text=True,
        capture_output=True,
        check=False,
        timeout=60,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


async def _functions(connection: asyncpg.Connection) -> list[dict[str, object]]:
    return [
        dict(row)
        for row in await connection.fetch(
            """
            SELECT p.oid, p.proname, p.proowner, p.proacl::text,
                   p.prosecdef, p.proconfig, p.proargtypes::text,
                   p.prorettype, p.prosrc
            FROM pg_catalog.pg_proc AS p
            JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
            WHERE n.nspname = 'public'
            ORDER BY p.oid
            """
        )
    ]


@dataclass
class OperatorDatabase:
    admin: asyncpg.Connection
    owner: asyncpg.Connection
    operator: asyncpg.Connection
    owner_name: str
    operator_name: str
    bootstrap_name: str
    workers: tuple[str, str]
    other_name: str
    functions_before: list[dict[str, object]]
    password: str

    async def upsert(self, generation: int = 1, service: str = SERVICE) -> uuid.UUID:
        grant_id = await self.operator.fetchval(
            """
            SELECT public.vp_worker_grant_upsert(
                $1, $2, 'ffmpeg_go', 'local-pg16-fixture', '["media_cpu"]',
                '0123456789abcdef0123456789abcdef01234567',
                'vp-ffmpeg-worker-go:deploy-0123456789ab',
                $3, 'vp:tasks:ffmpeg_go', 'ffmpeg_go-workers', $4::jsonb, $5,
                'creator-edge-regression'
            )
            """,
            service,
            generation,
            self.workers[generation - 1],
            json.dumps({key: json.loads(value) for key, value in ENDPOINT_BINDINGS.items()}),
            hashlib.sha256(f"{service}:{generation}".encode()).hexdigest(),
        )
        assert isinstance(grant_id, uuid.UUID)
        return grant_id

    async def activate(self, generation: int = 1) -> uuid.UUID:
        return await self.operator.fetchval(
            "SELECT public.vp_worker_grant_activate($1, $2)", SERVICE, generation
        )

    async def revoke(self, generation: int = 1) -> bool:
        return await self.operator.fetchval(
            "SELECT public.vp_worker_grant_revoke($1, $2, 'fixture-stop')",
            SERVICE, generation,
        )

    async def edges(self) -> list[tuple[object, ...]]:
        return [
            tuple(row)
            for row in await self.admin.fetch(
                """
                SELECT m.oid, granted.rolname, member.rolname, grantor.rolname,
                       grantor.oid, grantor.rolsuper,
                       m.admin_option, m.inherit_option, m.set_option
                FROM pg_catalog.pg_auth_members AS m
                JOIN pg_catalog.pg_roles AS granted ON granted.oid = m.roleid
                JOIN pg_catalog.pg_roles AS member ON member.oid = m.member
                JOIN pg_catalog.pg_roles AS grantor ON grantor.oid = m.grantor
                WHERE granted.rolname = ANY($1::text[])
                   OR member.rolname = ANY($1::text[])
                   OR grantor.rolname = ANY($1::text[])
                ORDER BY m.oid
                """,
                [RUNTIME_ROLE, *self.workers],
            )
        ]

    async def state(self) -> list[tuple[object, ...]]:
        rows = await self.admin.fetch(
            """
            SELECT g.id, g.state, g.activated_at, g.revoked_at, g.revoke_reason,
                   g.updated_at, r.rolcanlogin
            FROM public.worker_admission_grants AS g
            JOIN pg_catalog.pg_roles AS r ON r.rolname = g.database_principal
            ORDER BY g.generation
            """
        )
        return [tuple(row) for row in rows]


@pytest.fixture
async def operator_database(request: pytest.FixtureRequest) -> AsyncIterator[OperatorDatabase]:
    # Cluster-wide role DDL is deliberately restricted to disposable local PG16.
    assert make_url(POSTGRES_URL).host in {"127.0.0.1", "localhost", "::1"}
    suffix = uuid.uuid4().hex[:12]
    database = f"vp_operator_edges_{suffix}"
    owner_name = f"vp_deploy_migrator_{suffix}"
    operator_name = f"vp_operator_{suffix}"
    workers = tuple(role_names_for_generation(SERVICE, gen).versioned for gen in (1, 2))
    other_name = f"vp_other_{suffix}"
    password = uuid.uuid4().hex
    role_names: list[str] = []
    async with AsyncExitStack() as stack:
        admin = await _connect(_url("postgres"))
        stack.push_async_callback(admin.close)
        version = int(await admin.fetchval("SHOW server_version_num"))
        if not 160000 <= version < 170000:
            pytest.skip("this regression requires PostgreSQL 16")
        assert await admin.fetchval(
            "SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user"
        )
        if await admin.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = $1)",
            RUNTIME_ROLE,
        ):
            pytest.skip("requires a fresh isolated cluster without vp_worker_runtime")
        bootstrap_name = await admin.fetchval(
            "SELECT rolname FROM pg_catalog.pg_roles WHERE oid = 10 AND rolsuper"
        )
        assert bootstrap_name
        try:
            for name, attributes in (
                (owner_name, "LOGIN INHERIT CREATEROLE"),
                (operator_name, "LOGIN INHERIT NOCREATEROLE"),
                (other_name, "NOLOGIN INHERIT NOCREATEROLE"),
            ):
                await admin.execute(
                    f'CREATE ROLE "{name}" {attributes} NOSUPERUSER NOCREATEDB '
                    f"NOREPLICATION NOBYPASSRLS PASSWORD '{password}'"
                )
                role_names.append(name)
            await admin.execute(f'CREATE DATABASE "{database}" OWNER "{owner_name}"')
            owner_url = _url(database, user=owner_name, password=password)
            await asyncio.to_thread(_migrate, owner_url, PREVIOUS_REVISION)
            owner = await _connect(owner_url)
            async with AsyncExitStack() as connections:
                connections.push_async_callback(owner.close)
                for name, attributes in (
                    (RUNTIME_ROLE, "NOLOGIN NOINHERIT"),
                    *((worker, "LOGIN INHERIT") for worker in workers),
                ):
                    # Let PG16 create the real OID-10, admin-only creator grant.
                    await owner.execute(
                        f'CREATE ROLE "{name}" {attributes} NOSUPERUSER NOCREATEDB '
                        f"NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '{password}'"
                    )
                    role_names.append(name)
                for signature in OPERATOR_SIGNATURES:
                    await owner.execute(
                        f'GRANT EXECUTE ON FUNCTION public.{signature} TO "{operator_name}"'
                    )
                functions_before = await _functions(owner)
                revision = getattr(request, "param", TARGET_REVISION)
                target_admin = await _connect(_url(database))
                connections.push_async_callback(target_admin.close)
                if revision == TARGET_REVISION:
                    await target_admin.execute(bootstrap_sql())
                if revision != PREVIOUS_REVISION:
                    await asyncio.to_thread(_migrate, owner_url, revision)
                operator = await _connect(
                    _url(database, user=operator_name, password=password)
                )
                connections.push_async_callback(operator.close)
                yield OperatorDatabase(
                    target_admin, owner, operator, owner_name, operator_name,
                    bootstrap_name, workers, other_name, functions_before, password,
                )
        finally:
            await admin.execute(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')
            # PG16 refuses DROP ROLE while that role remains a membership grantor.
            for edge in await admin.fetch(
                "SELECT granted.rolname AS role, member.rolname AS member, "
                "grantor.rolname AS grantor FROM pg_catalog.pg_auth_members AS m "
                "JOIN pg_catalog.pg_roles AS granted ON granted.oid = m.roleid "
                "JOIN pg_catalog.pg_roles AS member ON member.oid = m.member "
                "JOIN pg_catalog.pg_roles AS grantor ON grantor.oid = m.grantor "
                "WHERE grantor.rolname = ANY($1::text[])",
                role_names,
            ):
                await admin.execute(
                    f'REVOKE "{edge["role"]}" FROM "{edge["member"]}" '
                    f'GRANTED BY "{edge["grantor"]}" CASCADE'
                )
            for name in reversed(role_names):
                await admin.execute(f'DROP ROLE IF EXISTS "{name}"')


async def _assert_creator_edges(fixture: OperatorDatabase) -> list[tuple[object, ...]]:
    creator_edges = [edge for edge in await fixture.edges() if edge[2] == fixture.owner_name]
    assert {edge[1] for edge in creator_edges} == {RUNTIME_ROLE, *fixture.workers}
    assert len(creator_edges) == 3
    for edge in creator_edges:
        assert edge[3:] == (fixture.bootstrap_name, 10, True, True, False, False)
        assert not await fixture.admin.fetchval(
            "SELECT pg_catalog.pg_has_role($1, $2, 'USAGE') "
            "OR pg_catalog.pg_has_role($1, $2, 'SET')",
            fixture.owner_name,
            edge[1],
        )
    return creator_edges


@pytest.mark.parametrize("operator_database", [PREVIOUS_REVISION], indirect=True)
async def test_034_reproduces_bootstrap_creator_revoke_42501(
    operator_database: OperatorDatabase,
) -> None:
    fixture = operator_database
    await _assert_creator_edges(fixture)
    await fixture.upsert()
    before_state, before_edges = await fixture.state(), await fixture.edges()
    with pytest.raises(asyncpg.InsufficientPrivilegeError) as error:
        await fixture.activate()
    assert error.value.sqlstate == "42501"
    assert await fixture.state() == before_state
    assert await fixture.edges() == before_edges


async def test_035_distinct_operator_lifecycle_preserves_only_safe_creator_edges(
    operator_database: OperatorDatabase,
) -> None:
    fixture = operator_database
    assert tuple(await fixture.operator.fetchrow("SELECT current_user, session_user")) == (
        fixture.operator_name, fixture.operator_name,
    )
    owner = await fixture.admin.fetchrow(
        "SELECT oid, rolcanlogin, rolinherit, rolcreaterole, rolsuper, rolcreatedb, "
        "rolreplication, rolbypassrls FROM pg_catalog.pg_roles WHERE rolname = $1",
        fixture.owner_name,
    )
    assert tuple(owner)[1:] == (True, True, True, False, False, False, False)
    assert await fixture.admin.fetchval(
        "SELECT datdba FROM pg_catalog.pg_database WHERE datname = current_database()"
    ) == owner["oid"]
    functions_after = await _functions(fixture.owner)
    assert len(functions_after) == len(fixture.functions_before)
    changed = set()
    for before, after in zip(fixture.functions_before, functions_after, strict=True):
        if before["prosrc"] != after["prosrc"]:
            changed.add(before["proname"])
        assert {k: v for k, v in before.items() if k != "prosrc"} == {
            k: v for k, v in after.items() if k != "prosrc"
        }
        if after["proname"] in {signature.split("(")[0] for signature in OPERATOR_SIGNATURES}:
            assert after["proowner"] == owner["oid"]
            assert after["prosecdef"] is True
            assert after["proconfig"] == ["search_path=pg_catalog"]
    assert changed == {"vp_worker_grant_activate", "vp_worker_grant_revoke"}
    creators = await _assert_creator_edges(fixture)
    first = await fixture.upsert()
    assert await fixture.activate() == first
    assert await fixture.activate() == first
    canonical = [edge for edge in await fixture.edges() if edge[2] != fixture.owner_name]
    assert [edge[1:4] + edge[6:] for edge in canonical] == [
        (RUNTIME_ROLE, fixture.workers[0], fixture.owner_name, False, True, True)
    ]
    second = await fixture.upsert(2)
    assert await fixture.activate(2) == second
    states = await fixture.state()
    assert [(state[1], state[4], state[6]) for state in states] == [
        ("revoked", "superseded", False), ("active", None, True),
    ]
    assert await _assert_creator_edges(fixture) == creators
    assert await fixture.revoke(2) is True
    assert await fixture.revoke(2) is True
    assert [(state[1], state[6]) for state in await fixture.state()] == [
        ("revoked", False), ("revoked", False),
    ]
    assert await fixture.edges() == creators


async def test_worker_replacement_retires_an_open_old_generation_session(
    operator_database: OperatorDatabase,
) -> None:
    fixture = operator_database
    await fixture.upsert()
    await fixture.activate()
    database = await fixture.owner.fetchval("SELECT current_database()")
    old_worker = await _connect(
        _url(database, user=fixture.workers[0], password=fixture.password)
    )
    try:
        old_pid = old_worker.get_server_pid()
        replacement = await fixture.upsert(2)
        assert await fixture.activate(2) == replacement
        for _ in range(50):
            if old_worker.is_closed():
                break
            await asyncio.sleep(0.02)
        assert old_worker.is_closed()
        assert not await fixture.admin.fetchval(
            "SELECT EXISTS (SELECT FROM pg_stat_activity WHERE pid = $1)", old_pid
        )
        assert await fixture.operator.fetchval("SELECT 1") == 1
    finally:
        await old_worker.close()


@pytest.mark.parametrize("operation", ["activate", "revoke"])
@pytest.mark.parametrize(
    "unsafe",
    [
        "group_inherit", "group_set", "worker_inherit", "worker_set",
        "worker_no_admin", "different_member", "different_superuser_grantor",
        "owner_createdb", "owner_replication", "owner_bypassrls",
        "owner_noinherit", "owner_nologin", "owner_nocreaterole",
        "owner_inherit_parent", "owner_set_parent",
    ],
)
async def test_035_unsafe_creator_edges_fail_closed_without_partial_mutation(
    operator_database: OperatorDatabase, unsafe: str, operation: str,
) -> None:
    fixture = operator_database
    await fixture.upsert()
    if operation == "revoke":
        await fixture.activate()
    target = RUNTIME_ROLE if unsafe.startswith("group_") else fixture.workers[0]
    owner = fixture.owner_name
    bootstrap = fixture.bootstrap_name
    if unsafe in {"group_inherit", "worker_inherit", "group_set", "worker_set"}:
        option = "INHERIT" if unsafe.endswith("inherit") else "SET"
        await fixture.admin.execute(
            f'GRANT "{target}" TO "{owner}" WITH {option} TRUE GRANTED BY "{bootstrap}"'
        )
    elif unsafe == "worker_no_admin":
        await fixture.admin.execute(
            f'REVOKE ADMIN OPTION FOR "{target}" FROM "{owner}" '
            f'GRANTED BY "{bootstrap}" CASCADE'
        )
    elif unsafe == "different_member":
        await fixture.admin.execute(
            f'GRANT "{target}" TO "{fixture.operator_name}" '
            f'WITH ADMIN TRUE, INHERIT FALSE, SET FALSE GRANTED BY "{bootstrap}"'
        )
    elif unsafe == "different_superuser_grantor":
        await fixture.admin.execute(f'ALTER ROLE "{fixture.other_name}" SUPERUSER')
        await fixture.admin.execute(
            f'GRANT "{target}" TO "{fixture.other_name}" '
            "WITH ADMIN TRUE, INHERIT FALSE, SET FALSE"
        )
        await fixture.admin.execute(
            f'GRANT "{target}" TO "{owner}" WITH ADMIN TRUE, INHERIT FALSE, SET FALSE '
            f'GRANTED BY "{fixture.other_name}"'
        )
    elif unsafe in {"owner_inherit_parent", "owner_set_parent"}:
        options = (
            "INHERIT TRUE, SET FALSE" if unsafe == "owner_inherit_parent"
            else "INHERIT FALSE, SET TRUE"
        )
        await fixture.admin.execute(
            f'GRANT "{fixture.other_name}" TO "{owner}" WITH ADMIN FALSE, {options}'
        )
    else:
        attribute = unsafe.removeprefix("owner_").upper()
        await fixture.admin.execute(f'ALTER ROLE "{owner}" {attribute}')
    before_state, before_edges = await fixture.state(), await fixture.edges()
    with pytest.raises(asyncpg.PostgresError) as error:
        await getattr(fixture, operation)()
    assert error.value.sqlstate in {"42501", "P0001"}
    if error.value.sqlstate == "P0001":
        assert "worker_role_not_isolated" in str(error.value)
    assert await fixture.state() == before_state
    assert await fixture.edges() == before_edges


@pytest.mark.parametrize(
    "attribute", ["NOLOGIN", "NOINHERIT", "CREATEROLE", "CREATEDB", "REPLICATION", "BYPASSRLS"],
)
async def test_035_preserves_runtime_principal_attribute_checks(
    operator_database: OperatorDatabase, attribute: str,
) -> None:
    fixture = operator_database
    await fixture.upsert()
    await fixture.admin.execute(f'ALTER ROLE "{fixture.workers[0]}" {attribute}')
    before_state, before_edges = await fixture.state(), await fixture.edges()
    with pytest.raises(asyncpg.RaiseError, match="worker_role_not_isolated"):
        await fixture.activate()
    assert await fixture.state() == before_state
    assert await fixture.edges() == before_edges


async def test_035_preserves_operator_privilege_and_input_guards(
    operator_database: OperatorDatabase,
) -> None:
    fixture = operator_database
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await fixture.operator.execute("DELETE FROM public.worker_admission_grants")
    with pytest.raises(asyncpg.RaiseError, match="claim_mismatch"):
        await fixture.operator.fetchval(
            "SELECT public.vp_worker_grant_activate('creator-edge', NULL)"
        )
    with pytest.raises(asyncpg.RaiseError, match="grant_missing"):
        await fixture.activate()
    await fixture.upsert()
    with pytest.raises(asyncpg.RaiseError, match="database_principal_conflict"):
        await fixture.upsert(service="different-service")
    await fixture.owner.execute(
        f'GRANT SELECT ON public.worker_admission_grants TO "{fixture.operator_name}"'
    )
    with pytest.raises(asyncpg.RaiseError, match="database_principal_privileged"):
        await fixture.activate()
    assert (await fixture.state())[0][1] == "pending"
