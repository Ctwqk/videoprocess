from __future__ import annotations

import asyncio
import uuid

import asyncpg
import pytest

from app.services import worker_registration_operator_cli as operator_cli
from app.services import worker_runtime_role_cli as runtime_cli
from app.services import worker_role_cli_common as role_common
from app.services import worker_control_role_cli as control_cli
from app.services import worker_marker_control_role_cli as marker_cli
from app.services.worker_control_role_cli import ROLE_FUNCTIONS, STABLE_ROLES
from app.services.worker_runtime_role_cli import role_names_for_generation
from app.services.worker_session_signal_sql import SCHEMA, VERIFY_SQL, bootstrap_sql
from test_worker_operator_creator_edges_postgres import (
    SERVICE,
    TARGET_REVISION,
    OperatorDatabase,
    _connect,
    _migrate,
    _url,
    operator_database as operator_database,
    pytestmark as pytestmark,
)


async def _worker(fixture: OperatorDatabase, generation: int = 1, *, database=None):
    database = database or await fixture.owner.fetchval("SELECT current_database()")
    return await _connect(
        _url(database, user=fixture.workers[generation - 1], password=fixture.password)
    )


async def _closed(connection):
    for _ in range(100):
        if connection.is_closed():
            return
        await asyncio.sleep(0.01)
    assert connection.is_closed()


async def _retire(fixture, generation=1, service=SERVICE):
    return await fixture.owner.fetchval(
        f"SELECT {SCHEMA}.retire($1, $2)", service, generation
    )


async def test_signal_preserves_replacement_unrelated_and_other_database(
    operator_database,
):
    f = operator_database
    await f.upsert()
    await f.activate()
    old = await _worker(f)
    replacement = await _worker(f, 2)
    other_database = await _worker(f, database="postgres")
    await f.admin.execute(f'ALTER ROLE "{f.other_name}" LOGIN')
    database = await f.owner.fetchval("SELECT current_database()")
    unrelated = await _connect(_url(database, user=f.other_name, password=f.password))
    try:
        await f.upsert(2)
        await f.activate(2)
        await _closed(old)
        for connection in (replacement, other_database, unrelated, f.operator):
            assert await connection.fetchval("SELECT 1") == 1
        assert await _retire(f) == 0
    finally:
        for connection in (old, replacement, other_database, unrelated):
            await connection.close()


async def test_direct_runtime_operator_and_public_denied(operator_database):
    f = operator_database
    await f.upsert()
    await f.activate()
    worker = await _worker(f)
    await f.admin.execute(f'ALTER ROLE "{f.other_name}" LOGIN')
    database = await f.owner.fetchval("SELECT current_database()")
    public = await _connect(_url(database, user=f.other_name, password=f.password))
    try:
        for connection in (worker, f.operator, public):
            for function in ("validate_target", "retire"):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await connection.fetchval(
                        f"SELECT {SCHEMA}.{function}($1, $2)", SERVICE, 1
                    )
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute(
                    f"CREATE FUNCTION {SCHEMA}.escape() RETURNS int LANGUAGE sql AS 'SELECT 1'"
                )
        await f.owner.execute(VERIFY_SQL)
        assert not await f.owner.fetchval(
            "SELECT pg_has_role(current_user, 'pg_signal_backend', 'USAGE')"
        )
    finally:
        await worker.close()
        await public.close()


@pytest.mark.parametrize(
    "attribute",
    [
        "LOGIN",
        "NOINHERIT",
        "SUPERUSER",
        "CREATEROLE",
        "CREATEDB",
        "REPLICATION",
        "BYPASSRLS",
    ],
)
async def test_private_helper_rejects_target_attributes(operator_database, attribute):
    f = operator_database
    await f.upsert()
    await f.revoke()
    await f.admin.execute(f'ALTER ROLE "{f.workers[0]}" {attribute}')
    with pytest.raises(asyncpg.RaiseError, match="worker_signal_target_invalid"):
        await _retire(f)


@pytest.mark.parametrize(
    "drift",
    [
        "inherit",
        "set",
        "no_admin",
        "missing",
        "foreign_member",
        "parent",
        "grantor",
        "foreign_creator",
    ],
)
async def test_private_helper_requires_exact_creator_and_no_other_edges(
    operator_database, drift
):
    f = operator_database
    await f.upsert()
    await f.revoke()
    worker, owner, other, bootstrap = (
        f.workers[0],
        f.owner_name,
        f.other_name,
        f.bootstrap_name,
    )
    if drift in {"inherit", "set"}:
        sql = f'GRANT "{worker}" TO "{owner}" WITH {drift.upper()} TRUE GRANTED BY "{bootstrap}"'
    elif drift == "no_admin":
        sql = f'REVOKE ADMIN OPTION FOR "{worker}" FROM "{owner}" CASCADE'
    elif drift == "missing":
        sql = f'REVOKE "{worker}" FROM "{owner}" CASCADE'
    elif drift == "foreign_member":
        sql = (
            f'GRANT "{worker}" TO "{other}" WITH ADMIN FALSE, INHERIT FALSE, SET FALSE'
        )
    elif drift == "parent":
        sql = (
            f'GRANT "{other}" TO "{worker}" WITH ADMIN FALSE, INHERIT FALSE, SET FALSE'
        )
    elif drift == "grantor":
        await f.admin.execute(
            f'GRANT "{other}" TO "{worker}" WITH ADMIN TRUE, INHERIT FALSE, SET FALSE'
        )
        sql = f'GRANT "{other}" TO "{f.operator_name}" GRANTED BY "{worker}"'
    else:
        await f.admin.execute(f'ALTER ROLE "{other}" SUPERUSER')
        await f.admin.execute(
            f'GRANT "{worker}" TO "{other}" WITH ADMIN TRUE, INHERIT FALSE, SET FALSE'
        )
        sql = f'GRANT "{worker}" TO "{owner}" WITH ADMIN TRUE, INHERIT FALSE, SET FALSE GRANTED BY "{other}"'
    await f.admin.execute(sql)
    with pytest.raises(
        asyncpg.RaiseError, match="worker_signal_(creator|owner)_invalid"
    ):
        await _retire(f)


@pytest.mark.parametrize(
    "drift",
    [
        "owner",
        "CREATEDB",
        "REPLICATION",
        "BYPASSRLS",
        "NOLOGIN",
        "NOINHERIT",
        "NOCREATEROLE",
        "parent",
    ],
)
async def test_private_helper_pins_trusted_database_owner(operator_database, drift):
    f = operator_database
    await f.upsert()
    await f.revoke()
    if drift == "owner":
        database = await f.owner.fetchval("SELECT current_database()")
        await f.admin.execute(f'ALTER DATABASE "{database}" OWNER TO "{f.other_name}"')
    elif drift == "parent":
        await f.admin.execute(
            f'GRANT "{f.other_name}" TO "{f.owner_name}" WITH INHERIT FALSE, SET TRUE'
        )
    else:
        await f.admin.execute(f'ALTER ROLE "{f.owner_name}" {drift}')
    with pytest.raises(asyncpg.RaiseError, match="worker_signal_owner_invalid"):
        await _retire(f)


@pytest.mark.parametrize(
    "service,generation",
    [("unmanaged", 1), (SERVICE, 0), (None, 1), (SERVICE, None), (SERVICE, 99)],
)
async def test_helper_rejects_noncanonical_target_inputs(
    operator_database, service, generation
):
    with pytest.raises(
        asyncpg.RaiseError, match="worker_signal_(identity|target)_invalid"
    ):
        await _retire(operator_database, generation, service)


@pytest.mark.parametrize("invalid", ["NOLOGIN", "CREATEDB", "creator", "canonical"])
async def test_invalid_replacement_preserves_open_old_connection(
    operator_database, invalid
):
    f = operator_database
    await f.upsert()
    await f.activate()
    old = await _worker(f)
    try:
        await f.upsert(2)
        if invalid == "creator":
            await f.admin.execute(
                f'GRANT "{f.workers[1]}" TO "{f.owner_name}" WITH SET TRUE'
            )
        elif invalid == "canonical":
            await f.owner.execute(
                "ALTER TABLE public.worker_admission_grants DISABLE TRIGGER USER"
            )
            await f.owner.execute(
                "UPDATE public.worker_admission_grants SET database_principal = $1 WHERE generation = 2",
                f.other_name,
            )
            await f.owner.execute(
                "ALTER TABLE public.worker_admission_grants ENABLE TRIGGER USER"
            )
        else:
            await f.admin.execute(f'ALTER ROLE "{f.workers[1]}" {invalid}')
        before_state, before_edges = await f.state(), await f.edges()
        with pytest.raises(asyncpg.PostgresError):
            await f.activate(2)
        assert await f.state() == before_state
        assert await f.edges() == before_edges
        assert await old.fetchval("SELECT 1") == 1
    finally:
        await old.close()


async def test_post_commit_drain_retires_precommit_reconnect_and_is_idempotent(
    operator_database,
):
    f = operator_database
    await f.upsert()
    await f.activate()
    old = await _worker(f)
    late = None
    try:
        await f.upsert(2)
        async with f.operator.transaction():
            await f.activate(2)
            await _closed(old)
            late = await _worker(f)
        assert await late.fetchval("SELECT 1") == 1
        await f.activate(2)
        await _closed(late)
        assert await _retire(f) == 0
        assert await _retire(f) == 0
        with pytest.raises(asyncpg.InvalidAuthorizationSpecificationError):
            await _worker(f)
    finally:
        await old.close()
        if late:
            await late.close()


@pytest.mark.parametrize("change", ["login", "rename", "drop", "membership", "owner"])
async def test_catalog_identity_is_pinned_until_transaction_end(
    operator_database, change
):
    f = operator_database
    await f.upsert()
    await f.revoke()
    database = await f.owner.fetchval("SELECT current_database()")
    sql = {
        "login": f'ALTER ROLE "{f.workers[0]}" LOGIN',
        "rename": f'ALTER ROLE "{f.workers[0]}" RENAME TO "{f.workers[0]}_renamed"',
        "drop": f'DROP ROLE "{f.workers[0]}"',
        "membership": f'GRANT "{f.workers[0]}" TO "{f.other_name}"',
        "owner": f'ALTER DATABASE "{database}" OWNER TO "{f.other_name}"',
    }[change]
    await f.admin.execute("SET lock_timeout = '100ms'")
    async with f.owner.transaction():
        await f.owner.fetchval(f"SELECT {SCHEMA}.validate_target($1, $2)", SERVICE, 1)
        with pytest.raises(asyncpg.LockNotAvailableError):
            await f.admin.execute(sql)
        assert await _retire(f) == 0


async def test_bootstrap_never_calls_substituted_application_objects_or_temp_types(
    operator_database,
):
    f = operator_database
    await f.upsert()
    await f.revoke()
    await f.owner.execute("""
        CREATE TABLE public.signal_callback_audit (principal text);
        CREATE FUNCTION public.signal_callback() RETURNS boolean LANGUAGE plpgsql AS $$
        BEGIN
            INSERT INTO public.signal_callback_audit VALUES (current_user);
            RETURN true;
        END $$;
        ALTER TABLE public.worker_admission_grants RENAME TO hidden_grants;
        CREATE VIEW public.worker_admission_grants AS
            SELECT * FROM public.hidden_grants WHERE public.signal_callback();
        CREATE TEMP TABLE initialize_temp (id int);
        CREATE DOMAIN pg_temp.oid AS pg_catalog.oid CHECK (public.signal_callback());
        CREATE DOMAIN pg_temp.text AS pg_catalog.text CHECK (public.signal_callback());
    """)
    assert await _retire(f) == 0
    assert (
        await f.owner.fetchval("SELECT count(*) FROM public.signal_callback_audit") == 0
    )


@pytest.mark.parametrize(
    "drift",
    [
        "body",
        "function_acl",
        "schema_acl",
        "owner",
        "search_path",
        "missing",
        "lock_timeout",
        "validation_lock_timeout",
    ],
)
async def test_ordinary_migration_rejects_bootstrap_drift(operator_database, drift):
    f = operator_database
    sql = {
        "body": f"CREATE OR REPLACE FUNCTION {SCHEMA}.retire(p_service_name text, p_generation bigint) RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS 'BEGIN RETURN 0; END'",
        "function_acl": f'GRANT EXECUTE ON FUNCTION {SCHEMA}.retire(text,bigint) TO "{f.operator_name}"',
        "schema_acl": f"GRANT USAGE ON SCHEMA {SCHEMA} TO PUBLIC",
        "owner": f'ALTER FUNCTION {SCHEMA}.retire(text,bigint) OWNER TO "{f.owner_name}"',
        "search_path": f"ALTER FUNCTION {SCHEMA}.retire(text,bigint) SET search_path = public, pg_catalog",
        "missing": f"DROP SCHEMA {SCHEMA} CASCADE",
        "lock_timeout": f"ALTER FUNCTION {SCHEMA}.retire(text,bigint) SET lock_timeout = 0",
        "validation_lock_timeout": f"ALTER FUNCTION {SCHEMA}.validate_target(text,bigint) SET lock_timeout = 0",
    }[drift]
    await f.admin.execute(sql)
    await f.owner.execute(
        "UPDATE public.alembic_version SET version_num = '035_worker_creator_edges'"
    )
    database = await f.owner.fetchval("SELECT current_database()")
    with pytest.raises(
        AssertionError, match="worker_signal_bootstrap_missing_or_drifted"
    ):
        await asyncio.to_thread(
            _migrate,
            _url(database, user=f.owner_name, password=f.password),
            TARGET_REVISION,
        )
    assert (
        await f.owner.fetchval("SELECT version_num FROM public.alembic_version")
        == "035_worker_creator_edges"
    )


async def test_bootstrap_install_is_idempotent_and_deploy_cannot_install(
    operator_database,
):
    f = operator_database
    await f.admin.execute(bootstrap_sql())
    await f.owner.execute(VERIFY_SQL)
    with pytest.raises(
        asyncpg.RaiseError, match="worker_signal_original_bootstrap_required"
    ):
        await f.owner.execute(bootstrap_sql())
    await f.owner.execute("ROLLBACK")


@pytest.mark.parametrize("isolation", ["repeatable_read", "serializable"])
async def test_helper_rejects_stale_transaction_snapshots(operator_database, isolation):
    f = operator_database
    await f.upsert()
    await f.revoke()
    async with f.owner.transaction(isolation=isolation):
        with pytest.raises(asyncpg.RaiseError, match="worker_signal_isolation_invalid"):
            await _retire(f)


async def test_activation_allows_already_removed_revoked_role(operator_database):
    f = operator_database
    await f.upsert()
    await f.activate()
    await f.revoke()
    await f.owner.execute(f'DROP ROLE "{f.workers[0]}"')
    replacement = await f.upsert(2)
    assert await f.activate(2) == replacement


@pytest.mark.parametrize("open_session", [False, True])
async def test_generic_public_lifecycle_keeps_only_ordinary_owner_authority(
    operator_database, open_session
):
    f = operator_database
    service = "generic-go-fixture"
    first = await f.upsert(service=service)
    assert (
        await f.operator.fetchval(
            "SELECT public.vp_worker_grant_activate($1, 1)", service
        )
        == first
    )
    old = await _worker(f) if open_session else None
    try:
        second = await f.upsert(2, service=service)
        if open_session:
            before = await f.state()
            with pytest.raises(
                asyncpg.InsufficientPrivilegeError, match="terminate process"
            ):
                await f.operator.fetchval(
                    "SELECT public.vp_worker_grant_activate($1, 2)", service
                )
            assert await f.state() == before
            assert await old.fetchval("SELECT 1") == 1
        else:
            assert (
                await f.operator.fetchval(
                    "SELECT public.vp_worker_grant_activate($1, 2)", service
                )
                == second
            )
            assert await f.operator.fetchval(
                "SELECT public.vp_worker_grant_revoke($1, 2, 'test')", service
            )
        with pytest.raises(asyncpg.RaiseError, match="worker_signal_identity_invalid"):
            await _retire(f, 1, service)
    finally:
        if old:
            await old.close()


async def test_managed_service_never_falls_back_when_helper_is_missing(
    operator_database,
):
    f = operator_database
    await f.upsert()
    await f.activate()
    old = await _worker(f)
    try:
        await f.upsert(2)
        await f.admin.execute(f"DROP SCHEMA {SCHEMA} CASCADE")
        before = await f.state()
        with pytest.raises(asyncpg.InvalidSchemaNameError):
            await f.activate(2)
        assert await f.state() == before
        assert await old.fetchval("SELECT 1") == 1
    finally:
        await old.close()


async def test_operator_cli_commits_activation_before_drain(
    operator_database, monkeypatch, capsys
):
    f = operator_database
    await f.upsert()
    await f.activate()
    await f.upsert(2)
    database = await f.owner.fetchval("SELECT current_database()")
    original_connect = operator_cli.asyncpg.connect
    observations = []

    class ObserveConnection:
        def __init__(self, connection):
            self.connection = connection

        async def fetchval(self, query, *args):
            observations.append(
                await f.owner.fetchval(
                    "SELECT state FROM public.worker_admission_grants WHERE generation = 2"
                )
            )
            return await self.connection.fetchval(query, *args)

        async def close(self):
            await self.connection.close()

    async def connect(url, **options):
        connection = await original_connect(url, **options)
        assert await connection.fetchval("SHOW lock_timeout") == "2s"
        assert await connection.fetchval("SHOW statement_timeout") == "1min"
        return ObserveConnection(connection)

    monkeypatch.setattr(
        operator_cli,
        "load_database_url_file",
        lambda _: _url(database, user=f.operator_name, password=f.password),
    )
    monkeypatch.setattr(operator_cli.asyncpg, "connect", connect)
    assert (
        await operator_cli.run(
            ["activate", "--service-name", SERVICE, "--generation", "2"]
        )
        == 0
    )
    assert observations == ["pending", "active"]
    assert '"status":"ok"' in capsys.readouterr().out


async def test_real_operator_stable_allowlist_supports_repeated_drain(
    operator_database,
):
    f = operator_database
    stable = STABLE_ROLES["operator"]
    await f.owner.execute(f'CREATE ROLE "{stable}" NOLOGIN NOINHERIT')
    try:
        for signature in ROLE_FUNCTIONS["operator"]:
            await f.owner.execute(
                f'REVOKE ALL ON FUNCTION public.{signature} FROM "{f.operator_name}"'
            )
            await f.owner.execute(
                f'GRANT EXECUTE ON FUNCTION public.{signature} TO "{stable}"'
            )
        await f.owner.execute(f'GRANT "{stable}" TO "{f.operator_name}"')
        await f.upsert()
        await f.activate()
        old = await _worker(f)
        try:
            await f.upsert(2)
            await f.activate(2)
            await f.activate(2)
            await _closed(old)
            assert await f.revoke(2)
            assert await f.revoke(2)
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await f.operator.fetchval(f"SELECT {SCHEMA}.retire($1, $2)", SERVICE, 1)
        finally:
            await old.close()
    finally:
        await f.admin.execute(f'REVOKE "{stable}" FROM "{f.operator_name}"')
        await f.admin.execute(f'DROP OWNED BY "{stable}"')
        await f.admin.execute(f'DROP ROLE "{stable}"')


async def test_fresh_original_bootstrap_database_installs_and_signals():
    admin = await _connect(_url("postgres"))
    database = "vp_signal_bootstrap_" + uuid.uuid4().hex[:12]
    generation = uuid.uuid4().int % (2**63 - 1) + 1
    name = role_names_for_generation(SERVICE, generation).versioned
    password = uuid.uuid4().hex
    target = worker = None
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
        await asyncio.to_thread(_migrate, _url(database), TARGET_REVISION)
        target = await _connect(_url(database))
        await target.execute(VERIFY_SQL)
        await target.execute(f"CREATE ROLE \"{name}\" LOGIN PASSWORD '{password}'")
        worker = await _connect(_url(database, user=name, password=password))
        await target.execute(f'ALTER ROLE "{name}" NOLOGIN')
        assert (
            await target.fetchval(f"SELECT {SCHEMA}.retire($1,$2)", SERVICE, generation)
            == 1
        )
        await _closed(worker)
    finally:
        if worker:
            await worker.close()
        if target:
            await target.close()
        await admin.execute(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')
        await admin.execute(f'DROP ROLE IF EXISTS "{name}"')
        await admin.close()


async def _runtime_cleanup(fixture, entrypoint, state_dir, names):
    database = await fixture.owner.fetchval("SELECT current_database()")
    if entrypoint == "deauthorize":
        await runtime_cli._deauthorize_generation(fixture.owner, SERVICE, 1, names)
    elif entrypoint == "retire_local":
        await runtime_cli._retire_local_generation(
            fixture.owner, state_dir, SERVICE, 1, names
        )
    else:
        await runtime_cli._revoke(
            _url(database, user=fixture.owner_name, password=fixture.password),
            SERVICE,
            1,
            state_dir,
            names,
        )


@pytest.mark.parametrize("entrypoint", ["deauthorize", "retire_local", "revoke"])
@pytest.mark.parametrize("authority", ["pre_grant", "revoked"])
async def test_runtime_cleanup_retires_open_canonical_session(
    operator_database, tmp_path, entrypoint, authority
):
    f = operator_database
    names = role_names_for_generation(SERVICE, 1)
    if authority == "revoked":
        await f.upsert()
        await f.activate()
        # Durable revocation may precede the separate role-cleanup transaction.
        await f.owner.execute("""
            UPDATE public.worker_admission_grants
            SET state = 'revoked', revoked_at = clock_timestamp(),
                revoke_reason = 'cleanup-test', updated_at = clock_timestamp()
            WHERE generation = 1
        """)
    state_dir = tmp_path / "runtime-state"
    database = await f.owner.fetchval("SELECT current_database()")
    runtime_cli.write_generation_state(
        state_dir,
        SERVICE,
        1,
        names,
        database_url=_url(database, user=names.versioned, password=f.password),
        admission_token="local-cleanup-test-token",
    )
    old = await _worker(f)
    replacement = await _worker(f, 2)
    other_database = await _worker(f, database="postgres")
    try:
        await _runtime_cleanup(f, entrypoint, state_dir, names)
        await _closed(old)
        assert await replacement.fetchval("SELECT 1") == 1
        assert await other_database.fetchval("SELECT 1") == 1
        exists = await f.owner.fetchval(
            "SELECT rolcanlogin FROM pg_catalog.pg_roles WHERE rolname = $1",
            names.versioned,
        )
        assert exists is False if entrypoint == "deauthorize" else exists is None
        if entrypoint != "deauthorize":
            assert not (state_dir / SERVICE / "1").exists()
        await _runtime_cleanup(f, entrypoint, state_dir, names)
    finally:
        await old.close()
        await replacement.close()
        await other_database.close()


@pytest.mark.parametrize("entrypoint", ["deauthorize", "retire_local", "revoke"])
async def test_runtime_cleanup_missing_helper_keeps_quarantine_and_never_drops(
    operator_database, tmp_path, entrypoint
):
    f = operator_database
    names = role_names_for_generation(SERVICE, 1)
    state_dir = tmp_path / "runtime-state"
    database = await f.owner.fetchval("SELECT current_database()")
    runtime_cli.write_generation_state(
        state_dir,
        SERVICE,
        1,
        names,
        database_url=_url(database, user=names.versioned, password=f.password),
        admission_token="local-cleanup-test-token",
    )
    paths = runtime_cli.credential_paths(state_dir, SERVICE, 1)
    files_before = {name: path.read_bytes() for name, path in paths.items()}
    old = await _worker(f)
    try:
        await f.admin.execute(f"DROP SCHEMA {SCHEMA} CASCADE")
        with pytest.raises(
            (asyncpg.InvalidSchemaNameError, runtime_cli.RuntimeRoleError)
        ):
            await _runtime_cleanup(f, entrypoint, state_dir, names)
        assert await old.fetchval("SELECT 1") == 1
        # Quarantine was committed even though signaling failed, without DROP.
        assert (
            await f.admin.fetchval(
                "SELECT rolcanlogin FROM pg_catalog.pg_roles WHERE rolname = $1",
                names.versioned,
            )
            is False
        )
        assert {name: path.read_bytes() for name, path in paths.items()} == files_before
    finally:
        await old.close()


@pytest.mark.parametrize("entrypoint", ["deauthorize", "retire_local", "revoke"])
async def test_runtime_cleanup_rejects_mismatched_generation_before_quarantine(
    operator_database, tmp_path, entrypoint
):
    f = operator_database
    wrong_names = role_names_for_generation(SERVICE, 2)
    old = await _worker(f, 2)
    try:
        with pytest.raises(
            runtime_cli.RuntimeRoleError, match="generation role identity invalid"
        ):
            await _runtime_cleanup(
                f, entrypoint, tmp_path / "absent-state", wrong_names
            )
        assert await old.fetchval("SELECT 1") == 1
        assert (
            await f.admin.fetchval(
                "SELECT rolcanlogin FROM pg_catalog.pg_roles WHERE rolname = $1",
                wrong_names.versioned,
            )
            is True
        )
    finally:
        await old.close()


async def test_worker_cleanup_callback_runs_after_quarantine_commit(operator_database):
    f = operator_database
    names = role_names_for_generation(SERVICE, 1)
    await f.upsert()
    await f.activate()
    old = await _worker(f)
    retire = runtime_cli._generation_session_retirement(f.owner, SERVICE, 1, names)
    assert retire is not None
    observations = []

    async def observed_retire():
        assert not f.owner.is_in_transaction()
        assert (
            await f.admin.fetchval(
                "SELECT rolcanlogin FROM pg_catalog.pg_roles WHERE rolname = $1",
                names.versioned,
            )
            is False
        )
        edges = [edge for edge in await f.edges() if names.versioned in edge[1:4]]
        assert len(edges) == 1
        assert edges[0][2] == f.owner_name
        assert edges[0][4:] == (10, True, True, False, False)
        observations.append("committed")
        await retire()

    try:
        await role_common.quarantine_login_roles(
            f.owner,
            (names.versioned,),
            retire_sessions=observed_retire,
        )
        await _closed(old)
        assert observations == ["committed"]
    finally:
        await old.close()


@pytest.mark.parametrize("purpose", ["generic", "control", "marker"])
async def test_non_worker_cleanup_keeps_native_signal_authority(
    operator_database, purpose
):
    f = operator_database
    service = "generic-cleanup-fixture"
    generic_names = role_names_for_generation(service, 1)
    generation = uuid.uuid4().hex
    name = {
        "generic": generic_names.versioned,
        "control": control_cli.role_names_for_generation(generation).versioned[
            "operator"
        ],
        "marker": marker_cli.role_names_for_generation(generation).versioned["janitor"],
    }[purpose]
    database = await f.owner.fetchval("SELECT current_database()")
    old = None
    await f.owner.execute(f"CREATE ROLE \"{name}\" LOGIN PASSWORD '{f.password}'")
    try:
        old = await _connect(_url(database, user=name, password=f.password))
        await f.admin.execute(f"DROP SCHEMA {SCHEMA} CASCADE")
        with pytest.raises(
            asyncpg.InsufficientPrivilegeError, match="terminate process"
        ):
            if purpose == "generic":
                await runtime_cli._deauthorize_generation(
                    f.owner, service, 1, generic_names
                )
            else:
                await role_common.quarantine_login_roles(f.owner, (name,))
        assert await old.fetchval("SELECT 1") == 1
        assert (
            await f.admin.fetchval(
                "SELECT rolcanlogin FROM pg_catalog.pg_roles WHERE rolname = $1",
                name,
            )
            is False
        )
        await old.close()
        await role_common.drop_login_roles(f.owner, (name,))
        assert not await f.admin.fetchval(
            "SELECT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = $1)",
            name,
        )
    finally:
        if old:
            await old.close()
        await f.admin.execute(f'DROP ROLE IF EXISTS "{name}"')


@pytest.mark.parametrize("operation", ["helper", "activate", "revoke"])
async def test_signal_catalog_contention_is_bounded_and_rolls_back(
    operator_database, operation
):
    f = operator_database
    await f.upsert()
    await f.activate()
    await f.upsert(2)
    old = await _worker(f)
    if operation == "helper":
        await f.owner.execute(f'ALTER ROLE "{f.workers[0]}" NOLOGIN')
        await role_common.revoke_role_membership_authority(f.owner, (f.workers[0],))
    caller = f.owner if operation == "helper" else f.operator
    await caller.execute("SET lock_timeout = 0; SET statement_timeout = 0")
    assert await caller.fetchval("SHOW lock_timeout") == "0"
    before_state, before_edges = await f.state(), await f.edges()
    query, arguments = {
        "helper": (f"SELECT {SCHEMA}.retire($1, $2)", (SERVICE, 1)),
        "activate": ("SELECT public.vp_worker_grant_activate($1, $2)", (SERVICE, 2)),
        "revoke": (
            "SELECT public.vp_worker_grant_revoke($1, $2, $3)",
            (SERVICE, 1, "test"),
        ),
    }[operation]
    task = None
    try:
        async with f.admin.transaction():
            # Compatible with ordinary role writes, incompatible with helper SHARE.
            await f.admin.execute(
                "LOCK TABLE pg_catalog.pg_authid IN ROW EXCLUSIVE MODE"
            )
            task = asyncio.create_task(caller.fetchval(query, *arguments, timeout=4))
            for _ in range(100):
                if await f.admin.fetchval(
                    """
                    SELECT EXISTS (SELECT FROM pg_catalog.pg_locks
                    WHERE pid = $1 AND relation = 'pg_catalog.pg_authid'::regclass
                      AND mode = 'ShareLock' AND NOT granted)
                """,
                    caller.get_server_pid(),
                ):
                    break
                await asyncio.sleep(0.01)
            assert await f.admin.fetchval(
                """
                SELECT EXISTS (SELECT FROM pg_catalog.pg_locks
                WHERE pid = $1 AND relation = 'pg_catalog.pg_database'::regclass
                  AND mode = 'ShareLock' AND granted)
            """,
                caller.get_server_pid(),
            )
            # The four-second client deadline is only a RED-test safety net.
            # Success requires PostgreSQL's helper-local two-second lock timeout.
            with pytest.raises(asyncpg.LockNotAvailableError):
                await task
            assert not await f.admin.fetchval(
                """
                SELECT EXISTS (SELECT FROM pg_catalog.pg_locks
                WHERE pid = $1 AND relation IN (
                    'pg_catalog.pg_database'::regclass,
                    'pg_catalog.pg_authid'::regclass,
                    'pg_catalog.pg_auth_members'::regclass))
            """,
                caller.get_server_pid(),
            )
            assert await f.state() == before_state
            assert await f.edges() == before_edges
            assert await old.fetchval("SELECT 1") == 1
            assert await caller.fetchval("SHOW lock_timeout") == "0"
    finally:
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        await old.close()
