from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import secrets
import stat
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Never

import asyncpg  # type: ignore[import-untyped]
from sqlalchemy.engine import make_url


OWNER_URL_FILE_ENV = "WORKER_MARKER_CONTROL_OWNER_DATABASE_URL_FILE"
GENERATION_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
MAX_OWNER_URL_BYTES = 4096
STABLE_ROLES = {
    "readiness": "vp_marker_readiness_runtime",
    "janitor": "vp_marker_janitor_runtime",
    "repair": "vp_marker_repair_runtime",
}
ROLE_FUNCTIONS = {
    "readiness": (
        "vp_list_worker_redis_marker_expectations(text,integer)",
        "vp_begin_worker_redis_continuity_check(uuid,integer)",
        "vp_finish_worker_redis_continuity_check("
        "uuid,text,text,text,bigint,bigint)",
    ),
    "janitor": (
        "vp_claim_worker_redis_marker_cleanup(uuid,integer,integer)",
        "vp_finish_worker_redis_marker_cleanup(uuid,uuid,text,text)",
    ),
    "repair": (
        "vp_load_worker_redis_marker_repair(text,uuid)",
        "vp_promote_observed_worker_event_emission(uuid,text,text)",
    ),
}
MARKER_TABLES = (
    "worker_redis_marker_cleanup_authorizations",
    "worker_redis_continuity_status",
    "worker_redis_marker_repair_audits",
)
CREDENTIAL_FILENAMES = {
    "readiness": "worker-marker-readiness-database-url",
    "janitor": "worker-marker-janitor-database-url",
    "repair": "worker-marker-repair-database-url",
}


class MarkerControlRoleError(RuntimeError):
    pass


class MarkerControlArgumentError(MarkerControlRoleError):
    pass


class MarkerControlOwnerURLFileError(MarkerControlRoleError):
    pass


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise MarkerControlArgumentError(message)


@dataclass(frozen=True)
class MarkerControlRoleNames:
    stable: Mapping[str, str]
    versioned: Mapping[str, str]


def role_names_for_generation(generation: str) -> MarkerControlRoleNames:
    if not GENERATION_PATTERN.fullmatch(generation):
        raise MarkerControlArgumentError("invalid generation")
    suffix = hashlib.sha256(generation.encode("utf-8")).hexdigest()[:16]
    versioned = {
        purpose: f"vp_marker_{purpose}_{suffix}"
        for purpose in STABLE_ROLES
    }
    if any(len(name.encode("utf-8")) > 63 for name in versioned.values()):
        raise MarkerControlArgumentError("invalid generation")
    return MarkerControlRoleNames(
        stable=dict(STABLE_ROLES),
        versioned=versioned,
    )


def credential_paths(
    state_dir: Path,
    generation: str,
) -> dict[str, Path]:
    role_names_for_generation(generation)
    generation_dir = state_dir / generation
    return {
        purpose: generation_dir / filename
        for purpose, filename in CREDENTIAL_FILENAMES.items()
    }


async def run(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        state_dir = Path(args.state_dir)
        if not state_dir.is_absolute():
            raise MarkerControlArgumentError("state dir must be absolute")
        names = role_names_for_generation(args.generation)
    except (argparse.ArgumentError, MarkerControlArgumentError):
        _emit("error", "marker_control_invalid_arguments")
        return 2

    try:
        owner_url = _load_owner_database_url()
    except MarkerControlOwnerURLFileError:
        _emit("error", "marker_control_owner_url_file_invalid")
        return 3

    try:
        if args.command == "provision":
            await _provision(
                owner_url,
                args.generation,
                state_dir,
                names,
            )
            reason_code = "marker_control_roles_provisioned"
        else:
            await _revoke(
                owner_url,
                args.generation,
                state_dir,
                names,
            )
            reason_code = "marker_control_roles_revoked"
    except (asyncpg.PostgresError, OSError, MarkerControlRoleError):
        _emit("error", "marker_control_operation_failed")
        return 4

    _emit(
        "ok",
        reason_code,
        roles=dict(names.versioned),
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(run(argv))


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        prog="worker-marker-control-role",
        exit_on_error=False,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("provision", "revoke"):
        command_parser = subparsers.add_parser(
            command,
            exit_on_error=False,
        )
        command_parser.add_argument("--generation", required=True)
        command_parser.add_argument("--state-dir", required=True)
    return parser


def _load_owner_database_url() -> str:
    raw_path = os.environ.get(OWNER_URL_FILE_ENV, "")
    if not raw_path:
        raise MarkerControlOwnerURLFileError("owner URL file missing")
    path = Path(raw_path)
    if not path.is_absolute():
        raise MarkerControlOwnerURLFileError("owner URL file invalid")
    try:
        file_stat = path.lstat()
    except OSError as exc:
        raise MarkerControlOwnerURLFileError(
            "owner URL file invalid"
        ) from exc
    if (
        not stat.S_ISREG(file_stat.st_mode)
        or stat.S_IMODE(file_stat.st_mode) != 0o400
        or file_stat.st_size < 1
        or file_stat.st_size > MAX_OWNER_URL_BYTES
    ):
        raise MarkerControlOwnerURLFileError("owner URL file invalid")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
        try:
            opened_stat = os.fstat(descriptor)
            if (
                opened_stat.st_dev != file_stat.st_dev
                or opened_stat.st_ino != file_stat.st_ino
                or not stat.S_ISREG(opened_stat.st_mode)
            ):
                raise MarkerControlOwnerURLFileError(
                    "owner URL file changed"
                )
            raw = os.read(descriptor, MAX_OWNER_URL_BYTES + 1)
        finally:
            os.close(descriptor)
    except OSError as exc:
        raise MarkerControlOwnerURLFileError(
            "owner URL file invalid"
        ) from exc
    try:
        value = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise MarkerControlOwnerURLFileError(
            "owner URL file invalid"
        ) from exc
    if (
        len(raw) > MAX_OWNER_URL_BYTES
        or "\x00" in value
        or len(value.splitlines()) != 1
    ):
        raise MarkerControlOwnerURLFileError("owner URL file invalid")
    owner_url = value.strip()
    try:
        parsed = make_url(owner_url)
    except Exception as exc:
        raise MarkerControlOwnerURLFileError(
            "owner URL file invalid"
        ) from exc
    if (
        parsed.drivername not in {"postgresql", "postgresql+asyncpg"}
        or not parsed.username
        or not parsed.host
        or not parsed.database
    ):
        raise MarkerControlOwnerURLFileError("owner URL file invalid")
    return owner_url


async def _provision(
    owner_url: str,
    generation: str,
    state_dir: Path,
    names: MarkerControlRoleNames,
) -> None:
    paths = credential_paths(state_dir, generation)
    generation_dir = state_dir / generation
    if generation_dir.exists():
        raise MarkerControlRoleError("generation already exists")
    passwords = {
        purpose: secrets.token_urlsafe(48) for purpose in STABLE_ROLES
    }
    if len(set(passwords.values())) != len(passwords):
        raise MarkerControlRoleError("password generation failed")

    connection = await asyncpg.connect(_asyncpg_url(owner_url))
    roles_created = False
    try:
        async with connection.transaction():
            for stable_role in names.stable.values():
                await _ensure_stable_role(connection, stable_role)
            for purpose, versioned_role in names.versioned.items():
                await _create_login_role(
                    connection,
                    versioned_role,
                    passwords[purpose],
                )
                await connection.execute(
                    f"GRANT {_quote_identifier(names.stable[purpose])} "
                    f"TO {_quote_identifier(versioned_role)}"
                )
            for purpose, stable_role in names.stable.items():
                await _set_stable_role_privileges(
                    connection,
                    stable_role,
                    ROLE_FUNCTIONS[purpose],
                )
        roles_created = True

        state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        generation_dir.mkdir(mode=0o700)
        os.chmod(generation_dir, 0o700)
        for purpose, path in paths.items():
            role_url = _role_database_url(
                owner_url,
                names.versioned[purpose],
                passwords[purpose],
            )
            _atomic_write_secret(path, f"{role_url}\n")
        _fsync_directory(generation_dir)
    except BaseException:
        _remove_generation_credentials(state_dir, generation)
        if roles_created:
            try:
                await _revoke_roles(connection, names)
            except (asyncpg.PostgresError, OSError):
                pass
        raise
    finally:
        await connection.close()


async def _revoke(
    owner_url: str,
    generation: str,
    state_dir: Path,
    names: MarkerControlRoleNames,
) -> None:
    connection = await asyncpg.connect(_asyncpg_url(owner_url))
    try:
        await _revoke_roles(connection, names)
    finally:
        await connection.close()
    _remove_generation_credentials(state_dir, generation)


async def _ensure_stable_role(
    connection: asyncpg.Connection,
    role_name: str,
) -> None:
    await connection.execute(
        "SELECT pg_catalog.set_config("
        "'vp.marker_control.role_name', $1, true)",
        role_name,
    )
    await connection.execute(
        """
        DO $block$
        DECLARE
            v_role_name text := pg_catalog.current_setting(
                'vp.marker_control.role_name'
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
    inherited_roles = await connection.fetch(
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
    for inherited_role in inherited_roles:
        await connection.execute(
            f"REVOKE {_quote_identifier(inherited_role['rolname'])} "
            f"FROM {_quote_identifier(role_name)}"
        )


async def _create_login_role(
    connection: asyncpg.Connection,
    role_name: str,
    password: str,
) -> None:
    await connection.execute(
        """
        SELECT
            pg_catalog.set_config(
                'vp.marker_control.role_name', $1, true
            ),
            pg_catalog.set_config(
                'vp.marker_control.password', $2, true
            )
        """,
        role_name,
        password,
    )
    await connection.execute(
        """
        DO $block$
        DECLARE
            v_role_name text := pg_catalog.current_setting(
                'vp.marker_control.role_name'
            );
            v_password text := pg_catalog.current_setting(
                'vp.marker_control.password'
            );
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM pg_catalog.pg_roles
                WHERE rolname = v_role_name
            ) THEN
                RAISE EXCEPTION USING
                    MESSAGE = 'marker_control_generation_exists',
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


async def _set_stable_role_privileges(
    connection: asyncpg.Connection,
    role_name: str,
    functions: Sequence[str],
) -> None:
    quoted_role = _quote_identifier(role_name)
    await connection.execute(
        f"REVOKE ALL ON SCHEMA public FROM {quoted_role}"
    )
    await connection.execute(
        f"GRANT USAGE ON SCHEMA public TO {quoted_role}"
    )
    await connection.execute(
        "REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA public "
        f"FROM {quoted_role}"
    )
    await connection.execute(
        "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public "
        f"FROM {quoted_role}"
    )
    for table_name in MARKER_TABLES:
        await connection.execute(
            f"REVOKE ALL ON TABLE public.{table_name} FROM {quoted_role}"
        )
    for signature in functions:
        await connection.execute(
            f"GRANT EXECUTE ON FUNCTION public.{signature} TO {quoted_role}"
        )


async def _revoke_roles(
    connection: asyncpg.Connection,
    names: MarkerControlRoleNames,
) -> None:
    async with connection.transaction():
        await connection.execute(
            """
            SELECT pg_catalog.pg_terminate_backend(activity.pid)
            FROM pg_catalog.pg_stat_activity AS activity
            WHERE activity.usename = ANY($1::text[])
              AND activity.pid <> pg_catalog.pg_backend_pid()
            """,
            list(names.versioned.values()),
        )
        for role_name in names.versioned.values():
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
                    f"REVOKE "
                    f"{_quote_identifier(membership['rolname'])} "
                    f"FROM {_quote_identifier(role_name)}"
                )
            await connection.execute(
                f"DROP ROLE {_quote_identifier(role_name)}"
            )


def _role_database_url(
    owner_url: str,
    role_name: str,
    password: str,
) -> str:
    parsed = make_url(owner_url)
    return parsed.set(
        username=role_name,
        password=password,
    ).render_as_string(hide_password=False)


def _asyncpg_url(database_url: str) -> str:
    return database_url.replace(
        "postgresql+asyncpg://",
        "postgresql://",
        1,
    )


def _atomic_write_secret(path: Path, value: str) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        dir=path.parent,
    )
    temporary_path = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o400)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        temporary_path.unlink(missing_ok=True)
        raise


def _remove_generation_credentials(
    state_dir: Path,
    generation: str,
) -> None:
    generation_dir = state_dir / generation
    for path in credential_paths(state_dir, generation).values():
        path.unlink(missing_ok=True)
    if generation_dir.is_dir():
        for path in generation_dir.iterdir():
            if path.name.startswith(".worker-marker-"):
                path.unlink(missing_ok=True)
        try:
            generation_dir.rmdir()
        except OSError:
            pass


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _quote_identifier(identifier: str) -> str:
    if len(identifier.encode("utf-8")) > 63 or "\x00" in identifier:
        raise MarkerControlRoleError("invalid role identifier")
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _emit(
    status: str,
    reason_code: str,
    *,
    roles: Mapping[str, str] | None = None,
) -> None:
    payload: dict[str, object] = {
        "reason_code": reason_code,
        "status": status,
    }
    if roles is not None:
        payload["roles"] = dict(roles)
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(main())
