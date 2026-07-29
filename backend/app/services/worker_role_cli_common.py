from __future__ import annotations

import errno
import os
import secrets
import stat
from collections.abc import Mapping, Sequence
from pathlib import Path

import asyncpg  # type: ignore[import-untyped]
from sqlalchemy.engine import make_url


MAX_DATABASE_URL_BYTES = 4096


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
    try:
        before = path.lstat()
    except OSError as exc:
        raise WorkerRoleCommonError("database URL file invalid") from exc
    if (
        not stat.S_ISREG(before.st_mode)
        or stat.S_IMODE(before.st_mode) != required_mode
        or before.st_size < 1
        or before.st_size > MAX_DATABASE_URL_BYTES
    ):
        raise WorkerRoleCommonError("database URL file invalid")

    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NONBLOCK"):
        flags |= os.O_NONBLOCK
    try:
        descriptor = os.open(path, flags)
        try:
            opened = os.fstat(descriptor)
            if (
                opened.st_dev != before.st_dev
                or opened.st_ino != before.st_ino
                or not stat.S_ISREG(opened.st_mode)
                or stat.S_IMODE(opened.st_mode) != required_mode
                or opened.st_size < 1
                or opened.st_size > MAX_DATABASE_URL_BYTES
            ):
                raise WorkerRoleCommonError(
                    "database URL file changed"
                )
            raw = os.read(descriptor, MAX_DATABASE_URL_BYTES + 1)
        finally:
            os.close(descriptor)
    except OSError as exc:
        raise WorkerRoleCommonError("database URL file invalid") from exc

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


async def ensure_stable_role(
    connection: asyncpg.Connection,
    role_name: str,
    *,
    setting_prefix: str,
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
        "NOREPLICATION NOBYPASSRLS"
    )
    await connection.execute(f"ALTER ROLE {quoted_role} RESET ALL")
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
            f"FROM {quoted_role}"
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
    await connection.execute(
        f"GRANT {quote_identifier(stable_role)} TO {quoted_role}"
    )


async def create_login_role(
    connection: asyncpg.Connection,
    role_name: str,
    password: str,
    *,
    setting_prefix: str,
) -> None:
    role_setting = f"vp.{setting_prefix}.role_name"
    password_setting = f"vp.{setting_prefix}.password"
    await connection.execute(
        "SELECT pg_catalog.set_config($1, $2, true), "
        "pg_catalog.set_config($3, $4, true)",
        role_setting,
        role_name,
        password_setting,
        password,
    )
    await connection.execute(
        f"""
        DO $block$
        DECLARE
            v_role_name text := pg_catalog.current_setting(
                '{role_setting}'
            );
            v_password text := pg_catalog.current_setting(
                '{password_setting}'
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
                'PASSWORD %L',
                v_role_name,
                v_password
            );
        END
        $block$
        """
    )


async def reset_public_privileges(
    connection: asyncpg.Connection,
    role_name: str,
) -> None:
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
            SELECT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_class AS object
                WHERE object.relowner = (
                    SELECT oid
                    FROM pg_catalog.pg_roles
                    WHERE rolname = $1
                )
                UNION ALL
                SELECT 1
                FROM pg_catalog.pg_proc AS object
                WHERE object.proowner = (
                    SELECT oid
                    FROM pg_catalog.pg_roles
                    WHERE rolname = $1
                )
                UNION ALL
                SELECT 1
                FROM pg_catalog.pg_type AS object
                WHERE object.typowner = (
                    SELECT oid
                    FROM pg_catalog.pg_roles
                    WHERE rolname = $1
                )
                UNION ALL
                SELECT 1
                FROM pg_catalog.pg_namespace AS object
                WHERE object.nspowner = (
                    SELECT oid
                    FROM pg_catalog.pg_roles
                    WHERE rolname = $1
                )
                UNION ALL
                SELECT 1
                FROM pg_catalog.pg_database AS object
                WHERE object.datdba = (
                    SELECT oid
                    FROM pg_catalog.pg_roles
                    WHERE rolname = $1
                )
            )
            """,
            role_name,
        )
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
    await connection.execute(
        """
        SELECT pg_catalog.pg_terminate_backend(activity.pid)
        FROM pg_catalog.pg_stat_activity AS activity
        WHERE activity.usename = ANY($1::text[])
          AND activity.pid <> pg_catalog.pg_backend_pid()
        """,
        list(role_names),
    )
    for role_name in role_names:
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
            continue
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
        await connection.execute(
            f"DROP ROLE {quote_identifier(role_name)}"
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
    try:
        before = path.lstat()
    except OSError as exc:
        raise WorkerRoleCommonError("state file invalid") from exc
    if (
        not stat.S_ISREG(before.st_mode)
        or stat.S_IMODE(before.st_mode) != required_mode
        or before.st_size < 1
        or before.st_size > MAX_DATABASE_URL_BYTES * 4
    ):
        raise WorkerRoleCommonError("state file invalid")
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
            or opened.st_size > MAX_DATABASE_URL_BYTES * 4
        ):
            raise WorkerRoleCommonError("state file changed")
        raw = os.read(descriptor, MAX_DATABASE_URL_BYTES * 4 + 1)
        final = os.fstat(descriptor)
        if (
            final.st_dev != opened.st_dev
            or final.st_ino != opened.st_ino
            or final.st_mode != opened.st_mode
            or final.st_size != opened.st_size
            or final.st_mtime_ns != opened.st_mtime_ns
            or final.st_ctime_ns != opened.st_ctime_ns
            or len(raw) != opened.st_size
        ):
            raise WorkerRoleCommonError("state file changed")
    except OSError as exc:
        raise WorkerRoleCommonError("state file invalid") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(raw) > MAX_DATABASE_URL_BYTES * 4:
        raise WorkerRoleCommonError("state file invalid")
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise WorkerRoleCommonError("state file invalid") from exc


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
