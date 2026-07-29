from __future__ import annotations

import base64
import errno
import hashlib
import hmac
import os
import secrets
import stat
from collections.abc import Mapping, Sequence
from pathlib import Path

import asyncpg  # type: ignore[import-untyped]
from sqlalchemy.engine import URL, make_url


MAX_DATABASE_URL_BYTES = 4096
MANAGED_LOGIN_COMMENT_PREFIX = "videoprocess-worker-role:v1:"
DATABASE_ACL_DCL_LOCK_SCOPE = "vp-worker-database-acl-dcl"


class WorkerRoleCommonError(RuntimeError):
    pass


def load_database_url_file(
    environment_name: str,
    *,
    required_mode: int = 0o400,
) -> str:
    raw_path = os.environ.get(environment_name, "")
    if not raw_path:
        raise WorkerRoleCommonError("database URL file missing")
    path = Path(raw_path)
    if not path.is_absolute():
        raise WorkerRoleCommonError("database URL file invalid")
    raw = _read_bounded_secure_file(
        path,
        required_mode=required_mode,
        maximum_bytes=MAX_DATABASE_URL_BYTES,
        invalid_message="database URL file invalid",
        changed_message="database URL file changed",
    )

    try:
        decoded = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise WorkerRoleCommonError("database URL file invalid") from exc
    if (
        len(raw) > MAX_DATABASE_URL_BYTES
        or "\x00" in decoded
        or len(decoded.splitlines()) != 1
    ):
        raise WorkerRoleCommonError("database URL file invalid")
    database_url = decoded.strip()
    try:
        parsed = make_url(database_url)
    except Exception as exc:
        raise WorkerRoleCommonError("database URL file invalid") from exc
    if (
        parsed.drivername not in {"postgresql", "postgresql+asyncpg"}
        or not parsed.username
        or not parsed.host
        or not parsed.database
    ):
        raise WorkerRoleCommonError("database URL file invalid")
    return database_url


def asyncpg_url(database_url: str) -> str:
    return database_url.replace(
        "postgresql+asyncpg://",
        "postgresql://",
        1,
    )


def role_database_url(
    owner_url: str,
    role_name: str,
    password: str,
) -> str:
    return make_url(owner_url).set(
        username=role_name,
        password=password,
    ).render_as_string(hide_password=False)


def quote_identifier(identifier: str) -> str:
    if (
        not identifier
        or len(identifier.encode("utf-8")) > 63
        or "\x00" in identifier
    ):
        raise WorkerRoleCommonError("invalid PostgreSQL identifier")
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


async def acquire_role_lifecycle_lock(
    connection: asyncpg.Connection,
    scope: str,
) -> None:
    if not scope or "\x00" in scope or len(scope.encode("utf-8")) > 512:
        raise WorkerRoleCommonError("role lifecycle scope invalid")
    await connection.execute(
        """
        SELECT pg_catalog.pg_advisory_lock(
            pg_catalog.hashtextextended(
                'vp-worker-role-lifecycle:' || $1,
                0
            )
        )
        """,
        scope,
    )


async def acquire_database_acl_dcl_lock(
    connection: asyncpg.Connection,
) -> None:
    # Global order: database DCL, stable roles, services, generation, rows.
    await connection.execute(
        """
        SELECT pg_catalog.pg_advisory_lock(
            pg_catalog.hashtextextended($1, 0)
        )
        """,
        DATABASE_ACL_DCL_LOCK_SCOPE,
    )


async def acquire_stable_role_authority_locks(
    connection: asyncpg.Connection,
    role_names: Sequence[str],
) -> None:
    # The caller already owns the database DCL lock.
    ordered_role_names = sorted(set(role_names))
    if not ordered_role_names or len(ordered_role_names) != len(role_names):
        raise WorkerRoleCommonError("stable role lock scope invalid")
    for role_name in ordered_role_names:
        quote_identifier(role_name)
        await connection.execute(
            """
            SELECT pg_catalog.pg_advisory_lock(
                pg_catalog.hashtextextended(
                    'vp-worker-stable-role:' || $1,
                    0
                )
            )
            """,
            role_name,
        )


async def acquire_worker_service_authority_lock(
    connection: asyncpg.Connection,
    service_name: str,
) -> None:
    if (
        not service_name
        or "\x00" in service_name
        or len(service_name.encode("utf-8")) > 255
    ):
        raise WorkerRoleCommonError("worker service scope invalid")
    await connection.execute(
        """
        SELECT pg_catalog.pg_advisory_lock(
            pg_catalog.hashtextextended(
                'vp-worker-service:' || $1,
                0
            )
        )
        """,
        service_name,
    )


async def ensure_stable_role(
    connection: asyncpg.Connection,
    role_name: str,
    *,
    setting_prefix: str,
    authorized_members: Sequence[str],
) -> None:
    setting_name = f"vp.{setting_prefix}.role_name"
    await connection.execute(
        "SELECT pg_catalog.set_config($1, $2, true)",
        setting_name,
        role_name,
    )
    await connection.execute(
        f"""
        DO $block$
        DECLARE
            v_role_name text := pg_catalog.current_setting(
                '{setting_name}'
            );
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_roles
                WHERE rolname = v_role_name
            ) THEN
                EXECUTE pg_catalog.format(
                    'CREATE ROLE %I NOLOGIN NOINHERIT NOSUPERUSER '
                    'NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS',
                    v_role_name
                );
            ELSE
                EXECUTE pg_catalog.format(
                    'ALTER ROLE %I NOLOGIN NOINHERIT NOSUPERUSER '
                    'NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS',
                    v_role_name
                );
            END IF;
        END
        $block$
        """
    )
    if await _role_owns_objects(connection, role_name):
        raise WorkerRoleCommonError("database role owns objects")
    await connection.execute(
        f"ALTER ROLE {quote_identifier(role_name)} RESET ALL"
    )
    await _revoke_database_ddl(connection, role_name)
    memberships = await connection.fetch(
        """
        SELECT granted.rolname
        FROM pg_catalog.pg_auth_members AS membership
        JOIN pg_catalog.pg_roles AS member
          ON member.oid = membership.member
        JOIN pg_catalog.pg_roles AS granted
          ON granted.oid = membership.roleid
        WHERE member.rolname = $1
        """,
        role_name,
    )
    for membership in memberships:
        await connection.execute(
            f"REVOKE {quote_identifier(membership['rolname'])} "
            f"FROM {quote_identifier(role_name)}"
        )
    await _converge_reverse_memberships(
        connection,
        role_name,
        authorized_members=authorized_members,
    )
    for authorized_member in sorted(set(authorized_members)):
        if await connection.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_roles
                WHERE rolname = $1
            )
            """,
            authorized_member,
        ):
            await harden_existing_login_role(
                connection,
                authorized_member,
                role_name,
            )


async def harden_existing_login_role(
    connection: asyncpg.Connection,
    role_name: str,
    stable_role: str,
) -> None:
    if not await connection.fetchval(
        """
        SELECT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_roles
            WHERE rolname = $1
        )
        """,
        role_name,
    ):
        raise WorkerRoleCommonError("database login role missing")
    if await _role_owns_objects(connection, role_name):
        raise WorkerRoleCommonError("database login role owns objects")

    quoted_role = quote_identifier(role_name)
    await connection.execute(
        f"ALTER ROLE {quoted_role} "
        "LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE "
        "NOREPLICATION NOBYPASSRLS "
        "CONNECTION LIMIT -1 VALID UNTIL 'infinity'"
    )
    await connection.execute(f"ALTER ROLE {quoted_role} RESET ALL")
    await _revoke_database_ddl(connection, role_name)
    await _converge_login_role_memberships(
        connection,
        role_name,
        stable_role=stable_role,
    )

    await connection.execute(
        f"REVOKE ALL ON SCHEMA public FROM {quoted_role}"
    )
    await connection.execute(
        "REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public "
        f"FROM {quoted_role}"
    )
    await connection.execute(
        "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public "
        f"FROM {quoted_role}"
    )
    await connection.execute(
        "REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public "
        f"FROM {quoted_role}"
    )
    column_privileges = await connection.fetch(
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
    for privilege in column_privileges:
        await connection.execute(
            f"REVOKE {privilege['privilege_type']} "
            f"({quote_identifier(privilege['attname'])}) "
            f"ON TABLE public.{quote_identifier(privilege['relname'])} "
            f"FROM {quoted_role}"
        )
    await mark_managed_login_role(connection, role_name, stable_role)


async def create_login_role(
    connection: asyncpg.Connection,
    role_name: str,
    password: str,
    *,
    setting_prefix: str,
    stable_role: str | None = None,
) -> None:
    role_setting = f"vp.{setting_prefix}.role_name"
    verifier_setting = f"vp.{setting_prefix}.password_verifier"
    password_verifier = _scram_sha_256_verifier(password)
    await connection.execute(
        "SELECT pg_catalog.set_config($1, $2, true), "
        "pg_catalog.set_config($3, $4, true)",
        role_setting,
        role_name,
        verifier_setting,
        password_verifier,
    )
    await connection.execute(
        f"""
        DO $block$
        DECLARE
            v_role_name text := pg_catalog.current_setting(
                '{role_setting}'
            );
            v_password_verifier text := pg_catalog.current_setting(
                '{verifier_setting}'
            );
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM pg_catalog.pg_roles
                WHERE rolname = v_role_name
            ) THEN
                RAISE EXCEPTION USING
                    MESSAGE = 'worker_role_generation_exists',
                    ERRCODE = 'P0001';
            END IF;
            EXECUTE pg_catalog.format(
                'CREATE ROLE %I LOGIN INHERIT NOSUPERUSER '
                'NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS '
                'CONNECTION LIMIT -1 VALID UNTIL %L PASSWORD %L',
                v_role_name,
                'infinity',
                v_password_verifier
            );
        END
        $block$
        """
    )
    if stable_role is not None:
        await mark_managed_login_role(connection, role_name, stable_role)


async def mark_managed_login_role(
    connection: asyncpg.Connection,
    role_name: str,
    stable_role: str,
) -> None:
    marker = f"{MANAGED_LOGIN_COMMENT_PREFIX}{stable_role}"
    quoted_marker = f"'{marker.replace(chr(39), chr(39) * 2)}'"
    await connection.execute(
        f"COMMENT ON ROLE {quote_identifier(role_name)} IS {quoted_marker}"
    )


def _scram_sha_256_verifier(password: str) -> str:
    if not password or "\x00" in password:
        raise WorkerRoleCommonError("database password invalid")
    salt = secrets.token_bytes(16)
    iterations = 4096
    salted_password = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        iterations,
    )
    client_key = hmac.new(
        salted_password,
        b"Client Key",
        hashlib.sha256,
    ).digest()
    stored_key = hashlib.sha256(client_key).digest()
    server_key = hmac.new(
        salted_password,
        b"Server Key",
        hashlib.sha256,
    ).digest()
    encoded_salt = base64.b64encode(salt).decode("ascii")
    encoded_stored_key = base64.b64encode(stored_key).decode("ascii")
    encoded_server_key = base64.b64encode(server_key).decode("ascii")
    return (
        f"SCRAM-SHA-256${iterations}:{encoded_salt}"
        f"${encoded_stored_key}:{encoded_server_key}"
    )


async def verify_role_database_url(
    owner_url: str,
    role_url: str,
    expected_role: str,
) -> None:
    try:
        owner = make_url(owner_url)
        candidate = make_url(role_url)
    except Exception as exc:
        raise WorkerRoleCommonError("database role URL invalid") from exc
    if (
        candidate.username != expected_role
        or candidate.password is None
        or _database_endpoint(candidate) != _database_endpoint(owner)
    ):
        raise WorkerRoleCommonError("database role URL invalid")

    connection = await asyncpg.connect(asyncpg_url(role_url))
    try:
        identity = await connection.fetchrow(
            """
            SELECT
                session_user AS session_user,
                current_user AS current_user,
                pg_catalog.current_database() AS database_name
            """
        )
        if (
            identity is None
            or identity["session_user"] != expected_role
            or identity["current_user"] != expected_role
            or identity["database_name"] != owner.database
        ):
            raise WorkerRoleCommonError("database role identity invalid")
    finally:
        await connection.close()


def _database_endpoint(url: URL) -> tuple[object, ...]:
    return (
        url.drivername.split("+", 1)[0],
        url.host,
        url.port,
        url.database,
        tuple(
            sorted(
                (key, tuple(values))
                for key, values in url.normalized_query.items()
            )
        ),
    )


async def reset_public_privileges(
    connection: asyncpg.Connection,
    role_name: str,
) -> None:
    await _converge_public_privileges(connection)
    quoted = quote_identifier(role_name)
    await connection.execute(
        f"REVOKE ALL ON SCHEMA public FROM {quoted}"
    )
    await connection.execute(f"GRANT USAGE ON SCHEMA public TO {quoted}")
    await connection.execute(
        "REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public "
        f"FROM {quoted}"
    )
    await connection.execute(
        "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public "
        f"FROM {quoted}"
    )
    await connection.execute(
        "REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public "
        f"FROM {quoted}"
    )


async def grant_functions(
    connection: asyncpg.Connection,
    role_name: str,
    signatures: Sequence[str],
) -> None:
    quoted = quote_identifier(role_name)
    for signature in signatures:
        await _converge_function_execute_grantees(
            connection,
            signature,
            authorized_role=role_name,
        )
        await connection.execute(
            f"GRANT EXECUTE ON FUNCTION public.{signature} TO {quoted}"
        )


async def grant_columns(
    connection: asyncpg.Connection,
    role_name: str,
    privilege: str,
    table_name: str,
    columns: Sequence[str],
) -> None:
    if privilege not in {"SELECT", "INSERT", "UPDATE"} or not columns:
        raise WorkerRoleCommonError("invalid column grant")
    quoted_columns = ", ".join(quote_identifier(column) for column in columns)
    await connection.execute(
        f"GRANT {privilege} ({quoted_columns}) "
        f"ON TABLE public.{quote_identifier(table_name)} "
        f"TO {quote_identifier(role_name)}"
    )


async def _role_owns_objects(
    connection: asyncpg.Connection,
    role_name: str,
) -> bool:
    return bool(
        await connection.fetchval(
            """
            WITH target_role AS (
                SELECT role.oid
                FROM pg_catalog.pg_roles AS role
                WHERE role.rolname = $1
            )
            -- Default ACL metadata is role-scoped, not a business object.
            SELECT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_shdepend AS dependency
                JOIN target_role
                  ON target_role.oid = dependency.refobjid
                WHERE dependency.refclassid
                      = 'pg_catalog.pg_authid'::pg_catalog.regclass
                  AND dependency.deptype = 'o'
                  AND dependency.classid
                      <> 'pg_catalog.pg_default_acl'::pg_catalog.regclass
            )
            """,
            role_name,
        )
    )


async def _converge_reverse_memberships(
    connection: asyncpg.Connection,
    granted_role: str,
    *,
    authorized_members: Sequence[str],
) -> None:
    rows = await connection.fetch(
        """
        SELECT member.rolname
        FROM pg_catalog.pg_auth_members AS membership
        JOIN pg_catalog.pg_roles AS member
          ON member.oid = membership.member
        JOIN pg_catalog.pg_roles AS granted
          ON granted.oid = membership.roleid
        WHERE granted.rolname = $1
        """,
        granted_role,
    )
    explicitly_authorized = set(authorized_members)
    for row in rows:
        member_name = row["rolname"]
        await connection.execute(
            f"REVOKE {quote_identifier(granted_role)} "
            f"FROM {quote_identifier(member_name)}"
        )
        if member_name in explicitly_authorized:
            await connection.execute(
                f"GRANT {quote_identifier(granted_role)} "
                f"TO {quote_identifier(member_name)}"
            )


async def _converge_login_role_memberships(
    connection: asyncpg.Connection,
    role_name: str,
    *,
    stable_role: str,
) -> None:
    if role_name == stable_role:
        raise WorkerRoleCommonError("database role membership invalid")
    rows = await connection.fetch(
        """
        SELECT
            granted.rolname AS granted_role,
            member.rolname AS member_role
        FROM pg_catalog.pg_auth_members AS membership
        JOIN pg_catalog.pg_roles AS granted
          ON granted.oid = membership.roleid
        JOIN pg_catalog.pg_roles AS member
          ON member.oid = membership.member
        WHERE member.rolname = $1 OR granted.rolname = $1
        ORDER BY granted.rolname, member.rolname
        """,
        role_name,
    )
    for row in rows:
        if (
            row["member_role"] == role_name
            and row["granted_role"] == stable_role
        ):
            continue
        await connection.execute(
            f"REVOKE {quote_identifier(row['granted_role'])} "
            f"FROM {quote_identifier(row['member_role'])}"
        )
    if not await connection.fetchval(
        """
        SELECT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_auth_members AS membership
            JOIN pg_catalog.pg_roles AS granted
              ON granted.oid = membership.roleid
            JOIN pg_catalog.pg_roles AS member
              ON member.oid = membership.member
            WHERE granted.rolname = $1
              AND member.rolname = $2
        )
        """,
        stable_role,
        role_name,
    ):
        await connection.execute(
            f"GRANT {quote_identifier(stable_role)} "
            f"TO {quote_identifier(role_name)}"
        )


async def _converge_public_privileges(
    connection: asyncpg.Connection,
) -> None:
    database_name = await connection.fetchval(
        "SELECT pg_catalog.current_database()"
    )
    if not isinstance(database_name, str):
        raise WorkerRoleCommonError("database identity invalid")
    await connection.execute(
        f"REVOKE CREATE, TEMPORARY ON DATABASE "
        f"{quote_identifier(database_name)} FROM PUBLIC"
    )
    await connection.execute(
        "REVOKE CREATE ON SCHEMA public FROM PUBLIC"
    )
    await connection.execute(
        "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM PUBLIC"
    )
    await connection.execute(
        "REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM PUBLIC"
    )
    public_columns = await connection.fetch(
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
        WHERE namespace.nspname = 'public'
          AND attribute.attnum > 0
          AND NOT attribute.attisdropped
          AND privilege.grantee = 0
        """
    )
    for privilege in public_columns:
        await connection.execute(
            f"REVOKE {privilege['privilege_type']} "
            f"({quote_identifier(privilege['attname'])}) "
            f"ON TABLE public.{quote_identifier(privilege['relname'])} "
            "FROM PUBLIC"
        )
    public_security_definers = await connection.fetch(
        """
        SELECT pg_catalog.format(
            '%I.%I(%s)',
            namespace.nspname,
            routine.proname,
            pg_catalog.pg_get_function_identity_arguments(routine.oid)
        ) AS signature
        FROM pg_catalog.pg_proc AS routine
        JOIN pg_catalog.pg_namespace AS namespace
          ON namespace.oid = routine.pronamespace
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(
                routine.proacl,
                pg_catalog.acldefault('f', routine.proowner)
            )
        ) AS privilege
        WHERE namespace.nspname = 'public'
          AND routine.prosecdef
          AND privilege.grantee = 0
          AND privilege.privilege_type = 'EXECUTE'
        """
    )
    for routine in public_security_definers:
        await connection.execute(
            f"REVOKE EXECUTE ON FUNCTION {routine['signature']} FROM PUBLIC"
        )
    managed_login_roles = await connection.fetch(
        """
        SELECT DISTINCT member.rolname
        FROM pg_catalog.pg_auth_members AS membership
        JOIN pg_catalog.pg_roles AS granted
          ON granted.oid = membership.roleid
        JOIN pg_catalog.pg_roles AS member
          ON member.oid = membership.member
        WHERE granted.rolname IN (
            'vp_worker_runtime',
            'vp_worker_operator_runtime',
            'vp_orchestrator_control_runtime',
            'vp_staging_janitor_runtime'
        )
        ORDER BY member.rolname
        """
    )
    for managed_login in managed_login_roles:
        await _revoke_database_ddl(
            connection,
            managed_login["rolname"],
        )
        await connection.execute(
            "REVOKE CREATE ON SCHEMA public FROM "
            f"{quote_identifier(managed_login['rolname'])}"
        )
    relevant_owners = await connection.fetch(
        """
        WITH relevant_owner_oids AS (
            SELECT role.oid
            FROM pg_catalog.pg_roles AS role
            WHERE role.rolname = current_user
            UNION
            SELECT relation.relowner
            FROM pg_catalog.pg_class AS relation
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'public'
            UNION
            SELECT routine.proowner
            FROM pg_catalog.pg_proc AS routine
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = routine.pronamespace
            WHERE namespace.nspname = 'public'
            UNION
            SELECT type_record.typowner
            FROM pg_catalog.pg_type AS type_record
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = type_record.typnamespace
            WHERE namespace.nspname = 'public'
            UNION
            SELECT namespace.nspowner
            FROM pg_catalog.pg_namespace AS namespace
            WHERE namespace.nspname = 'public'
            UNION
            SELECT defaults.defaclrole
            FROM pg_catalog.pg_default_acl AS defaults
            LEFT JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = defaults.defaclnamespace
            WHERE defaults.defaclnamespace = 0
               OR namespace.nspname = 'public'
            UNION
            SELECT role.oid
            FROM pg_catalog.pg_roles AS role
            WHERE pg_catalog.has_database_privilege(
                      role.oid,
                      pg_catalog.current_database(),
                      'CREATE'
                  )
               OR pg_catalog.has_schema_privilege(
                      role.oid,
                      'public',
                      'CREATE'
                  )
        )
        SELECT owner.rolname AS owner_name
        FROM relevant_owner_oids
        JOIN pg_catalog.pg_roles AS owner
          ON owner.oid = relevant_owner_oids.oid
        WHERE owner.rolname = current_user
           OR (
               owner.rolname !~ '^pg_'
               AND NOT EXISTS (
                   SELECT 1
                   FROM pg_catalog.pg_auth_members AS membership
                   JOIN pg_catalog.pg_roles AS stable
                     ON stable.oid = membership.roleid
                   WHERE membership.member = owner.oid
                     AND stable.rolname IN (
                         'vp_worker_runtime',
                         'vp_worker_operator_runtime',
                         'vp_orchestrator_control_runtime',
                         'vp_staging_janitor_runtime'
                     )
               )
           )
        ORDER BY owner.rolname
        """
    )
    for owner in relevant_owners:
        owner_clause = (
            "ALTER DEFAULT PRIVILEGES FOR ROLE "
            f"{quote_identifier(owner['owner_name'])}"
        )
        for object_kind in (
            "TABLES",
            "SEQUENCES",
            "FUNCTIONS",
            "TYPES",
            "SCHEMAS",
        ):
            await connection.execute(
                f"{owner_clause} REVOKE ALL PRIVILEGES "
                f"ON {object_kind} FROM PUBLIC"
            )
        for object_kind in (
            "TABLES",
            "SEQUENCES",
            "FUNCTIONS",
            "TYPES",
        ):
            await connection.execute(
                f"{owner_clause} IN SCHEMA public "
                "REVOKE ALL PRIVILEGES "
                f"ON {object_kind} FROM PUBLIC"
            )


async def _converge_function_execute_grantees(
    connection: asyncpg.Connection,
    signature: str,
    *,
    authorized_role: str,
) -> None:
    grantees = await connection.fetch(
        """
        SELECT
            privilege.grantee,
            grantee.rolname
        FROM pg_catalog.pg_proc AS routine
        CROSS JOIN LATERAL pg_catalog.aclexplode(
            COALESCE(
                routine.proacl,
                pg_catalog.acldefault('f', routine.proowner)
            )
        ) AS privilege
        LEFT JOIN pg_catalog.pg_roles AS grantee
          ON grantee.oid = privilege.grantee
        WHERE routine.oid = pg_catalog.to_regprocedure($1)
          AND privilege.privilege_type = 'EXECUTE'
          AND privilege.grantee <> routine.proowner
          AND (
              privilege.grantee = 0
              OR grantee.rolname <> $2
          )
        """,
        f"public.{signature}",
        authorized_role,
    )
    for grantee in grantees:
        target = (
            "PUBLIC"
            if grantee["grantee"] == 0
            else quote_identifier(grantee["rolname"])
        )
        await connection.execute(
            f"REVOKE EXECUTE ON FUNCTION public.{signature} FROM {target}"
        )


async def _revoke_database_ddl(
    connection: asyncpg.Connection,
    role_name: str,
) -> None:
    database_name = await connection.fetchval(
        "SELECT pg_catalog.current_database()"
    )
    if not isinstance(database_name, str):
        raise WorkerRoleCommonError("database identity invalid")
    await connection.execute(
        "REVOKE CREATE, TEMPORARY ON DATABASE "
        f"{quote_identifier(database_name)} "
        f"FROM {quote_identifier(role_name)}"
    )


async def drop_login_roles(
    connection: asyncpg.Connection,
    role_names: Sequence[str],
) -> None:
    if connection.is_in_transaction():
        raise WorkerRoleCommonError(
            "database role retirement transaction invalid"
        )
    ordered_roles = tuple(sorted(set(role_names)))
    if not ordered_roles or len(ordered_roles) != len(role_names):
        raise WorkerRoleCommonError("database role retirement invalid")
    for role_name in ordered_roles:
        quote_identifier(role_name)
    await quarantine_login_roles(connection, ordered_roles)
    async with connection.transaction():
        for role_name in ordered_roles:
            if await connection.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_roles
                    WHERE rolname = $1
                )
                """,
                role_name,
            ):
                await connection.execute(
                    f"DROP ROLE {quote_identifier(role_name)}"
                )


async def quarantine_login_roles(
    connection: asyncpg.Connection,
    role_names: Sequence[str],
) -> None:
    if connection.is_in_transaction():
        raise WorkerRoleCommonError(
            "database role quarantine transaction invalid"
        )
    ordered_roles = tuple(sorted(set(role_names)))
    if not ordered_roles or len(ordered_roles) != len(role_names):
        raise WorkerRoleCommonError("database role quarantine invalid")
    for role_name in ordered_roles:
        quote_identifier(role_name)

    existing_roles = {
        row["rolname"]
        for row in await connection.fetch(
            """
            SELECT rolname
            FROM pg_catalog.pg_roles
            WHERE rolname = ANY($1::text[])
            """,
            list(ordered_roles),
        )
    }
    if not existing_roles:
        return
    async with connection.transaction():
        for role_name in ordered_roles:
            if role_name in existing_roles:
                await connection.execute(
                    f"ALTER ROLE {quote_identifier(role_name)} NOLOGIN"
                )
        memberships = await connection.fetch(
            """
            SELECT
                granted.rolname AS granted_role,
                member.rolname AS member_role
            FROM pg_catalog.pg_auth_members AS membership
            JOIN pg_catalog.pg_roles AS granted
              ON granted.oid = membership.roleid
            JOIN pg_catalog.pg_roles AS member
              ON member.oid = membership.member
            WHERE granted.rolname = ANY($1::text[])
               OR member.rolname = ANY($1::text[])
            ORDER BY granted.rolname, member.rolname
            """,
            list(ordered_roles),
        )
        for membership in memberships:
            await connection.execute(
                f"REVOKE "
                f"{quote_identifier(membership['granted_role'])} "
                f"FROM {quote_identifier(membership['member_role'])}"
            )
    await connection.execute(
        """
        SELECT pg_catalog.pg_terminate_backend(activity.pid)
        FROM pg_catalog.pg_stat_activity AS activity
        WHERE activity.usename = ANY($1::text[])
          AND activity.pid <> pg_catalog.pg_backend_pid()
        """,
        list(ordered_roles),
    )


def write_secure_files(
    state_dir: Path,
    path_parts: Sequence[str],
    files: Mapping[str, str],
    *,
    file_mode: int | None = None,
    file_modes: Mapping[str, int] | None = None,
) -> None:
    if not state_dir.is_absolute() or not path_parts or not files:
        raise WorkerRoleCommonError("state path invalid")
    if (file_mode is None) == (file_modes is None):
        raise WorkerRoleCommonError("state file mode invalid")
    if file_mode is not None:
        resolved_file_modes: Mapping[str, int] = {
            filename: file_mode for filename in files
        }
    elif file_modes is not None and set(file_modes) == set(files):
        resolved_file_modes = file_modes
    else:
        raise WorkerRoleCommonError("state file mode invalid")
    if any(
        isinstance(mode, bool)
        or not isinstance(mode, int)
        or mode not in {0o400, 0o600}
        for mode in resolved_file_modes.values()
    ):
        raise WorkerRoleCommonError("state file mode invalid")
    for part in path_parts:
        if (
            not part
            or part in {".", ".."}
            or "/" in part
            or "\x00" in part
        ):
            raise WorkerRoleCommonError("state path invalid")

    state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptors: list[int] = []
    try:
        descriptor = os.open(state_dir, _directory_open_flags())
        descriptors.append(descriptor)
        _require_directory(descriptor)
        os.fchmod(descriptor, 0o700)
        for part in path_parts:
            try:
                os.mkdir(part, mode=0o700, dir_fd=descriptor)
            except FileExistsError:
                pass
            child = os.open(
                part,
                _directory_open_flags(),
                dir_fd=descriptor,
            )
            descriptors.append(child)
            _require_directory(child)
            os.fchmod(child, 0o700)
            descriptor = child
        for filename, value in files.items():
            if (
                not filename
                or "/" in filename
                or "\x00" in filename
                or not isinstance(value, str)
            ):
                raise WorkerRoleCommonError("state file invalid")
            _atomic_write(
                descriptor,
                filename,
                value,
                file_mode=resolved_file_modes[filename],
            )
        for opened in reversed(descriptors):
            os.fsync(opened)
    except OSError as exc:
        raise WorkerRoleCommonError("state write failed") from exc
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def remove_secure_files(
    state_dir: Path,
    path_parts: Sequence[str],
    filenames: Sequence[str],
) -> None:
    if not state_dir.is_absolute():
        raise WorkerRoleCommonError("state path invalid")
    descriptors: list[int] = []
    try:
        try:
            descriptor = os.open(state_dir, _directory_open_flags())
        except FileNotFoundError:
            return
        descriptors.append(descriptor)
        _require_directory(descriptor)
        for part in path_parts:
            try:
                child = os.open(
                    part,
                    _directory_open_flags(),
                    dir_fd=descriptor,
                )
            except FileNotFoundError:
                return
            descriptors.append(child)
            _require_directory(child)
            descriptor = child
        for filename in filenames:
            try:
                os.unlink(filename, dir_fd=descriptor)
            except FileNotFoundError:
                pass
        os.fsync(descriptor)
    except OSError as exc:
        raise WorkerRoleCommonError("state removal failed") from exc
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)

    current = state_dir.joinpath(*path_parts)
    for _part in reversed(path_parts):
        try:
            current.rmdir()
        except OSError as exc:
            if exc.errno not in {
                errno.ENOTEMPTY,
                errno.EEXIST,
                errno.ENOENT,
            }:
                raise WorkerRoleCommonError(
                    "state directory removal failed"
                ) from exc
        current = current.parent


def read_secure_file(path: Path, *, required_mode: int) -> str:
    if required_mode not in {0o400, 0o600}:
        raise WorkerRoleCommonError("state file mode invalid")
    raw = _read_bounded_secure_file(
        path,
        required_mode=required_mode,
        maximum_bytes=MAX_DATABASE_URL_BYTES * 4,
        invalid_message="state file invalid",
        changed_message="state file changed",
    )
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise WorkerRoleCommonError("state file invalid") from exc


def _read_bounded_secure_file(
    path: Path,
    *,
    required_mode: int,
    maximum_bytes: int,
    invalid_message: str,
    changed_message: str,
) -> bytes:
    try:
        before = path.lstat()
    except OSError as exc:
        raise WorkerRoleCommonError(invalid_message) from exc
    if (
        not stat.S_ISREG(before.st_mode)
        or stat.S_IMODE(before.st_mode) != required_mode
        or before.st_size < 1
        or before.st_size > maximum_bytes
    ):
        raise WorkerRoleCommonError(invalid_message)
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NONBLOCK"):
        flags |= os.O_NONBLOCK
    descriptor = -1
    try:
        descriptor = os.open(path, flags)
        opened = os.fstat(descriptor)
        if (
            opened.st_dev != before.st_dev
            or opened.st_ino != before.st_ino
            or not stat.S_ISREG(opened.st_mode)
            or stat.S_IMODE(opened.st_mode) != required_mode
            or opened.st_size < 1
            or opened.st_size > maximum_bytes
            or opened.st_uid != before.st_uid
            or opened.st_gid != before.st_gid
            or opened.st_size != before.st_size
            or opened.st_mtime_ns != before.st_mtime_ns
            or opened.st_ctime_ns != before.st_ctime_ns
        ):
            raise WorkerRoleCommonError(changed_message)
        chunks: list[bytes] = []
        length = 0
        while length < opened.st_size:
            chunk = os.read(descriptor, opened.st_size - length)
            if not chunk:
                raise WorkerRoleCommonError(changed_message)
            chunks.append(chunk)
            length += len(chunk)
        if os.read(descriptor, 1):
            raise WorkerRoleCommonError(changed_message)
        final = os.fstat(descriptor)
        if (
            final.st_dev != opened.st_dev
            or final.st_ino != opened.st_ino
            or not stat.S_ISREG(final.st_mode)
            or stat.S_IMODE(final.st_mode)
            != stat.S_IMODE(opened.st_mode)
            or final.st_uid != opened.st_uid
            or final.st_gid != opened.st_gid
            or final.st_size != opened.st_size
            or final.st_mtime_ns != opened.st_mtime_ns
            or final.st_ctime_ns != opened.st_ctime_ns
        ):
            raise WorkerRoleCommonError(changed_message)
        raw = b"".join(chunks)
    except OSError as exc:
        raise WorkerRoleCommonError(invalid_message) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    return raw


def _directory_open_flags() -> int:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    return flags


def _require_directory(descriptor: int) -> None:
    if not stat.S_ISDIR(os.fstat(descriptor).st_mode):
        raise WorkerRoleCommonError("state directory invalid")


def _atomic_write(
    directory_descriptor: int,
    filename: str,
    value: str,
    *,
    file_mode: int,
) -> None:
    temporary = f".{filename}.{secrets.token_hex(12)}"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    descriptor = -1
    try:
        descriptor = os.open(
            temporary,
            flags,
            file_mode,
            dir_fd=directory_descriptor,
        )
        os.fchmod(descriptor, file_mode)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.rename(
            temporary,
            filename,
            src_dir_fd=directory_descriptor,
            dst_dir_fd=directory_descriptor,
        )
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            os.unlink(temporary, dir_fd=directory_descriptor)
        except FileNotFoundError:
            pass
        raise
