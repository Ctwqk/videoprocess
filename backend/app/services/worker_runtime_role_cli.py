from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import secrets
import stat
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Never

import asyncpg  # type: ignore[import-untyped]

from app.services.worker_role_cli_common import (
    WorkerRoleCommonError,
    acquire_role_lifecycle_lock,
    acquire_stable_role_authority_locks,
    acquire_worker_service_authority_lock,
    asyncpg_url,
    create_login_role,
    drop_login_roles,
    ensure_stable_role,
    grant_columns,
    grant_functions,
    harden_existing_login_role,
    load_database_url_file,
    quote_identifier,
    read_secure_file,
    remove_secure_files,
    reset_public_privileges,
    role_database_url,
    verify_role_database_url,
    write_secure_files,
)


OWNER_URL_FILE_ENV = "WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE"
SERVICE_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
MAX_GENERATION = 9_223_372_036_854_775_807
STABLE_ROLE = "vp_worker_runtime"
STATE_VERSION = 1
STATE_FILENAMES = {
    "database_url": "worker-database-url",
    "admission_token": "worker-admission-token",
    "state": "state.json",
}
WORKER_FUNCTIONS = (
    "vp_worker_register(text,bigint,text,text,uuid,integer,text,jsonb,"
    "text,text,text,text,jsonb,text,text,text,text,text)",
    "vp_worker_heartbeat(uuid,text,uuid,bigint,text)",
    "vp_worker_release(uuid,text,uuid,bigint,text,text)",
    "vp_require_worker_lease(uuid,bigint)",
    "vp_claim_worker_node("
    "uuid,bigint,text,uuid,uuid,text,text,text,text,uuid)",
    "vp_require_worker_node_claim("
    "uuid,bigint,text,timestamp with time zone,uuid,uuid)",
    "vp_persist_worker_artifact("
    "uuid,bigint,text,timestamp with time zone,uuid,uuid,"
    "text,text,bigint,text,text,jsonb)",
    "vp_prepare_worker_event_emission("
    "uuid,bigint,text,timestamp with time zone,uuid,uuid,uuid,"
    "text,text,text,jsonb,text)",
    "vp_mark_worker_event_emitted(uuid,uuid,bigint,text)",
    "vp_list_worker_prepared_event_emissions(uuid,bigint,integer)",
    "vp_load_worker_prepared_event_emission(uuid,uuid,bigint)",
    "vp_require_worker_lease_margin(uuid,bigint,integer)",
    "vp_require_worker_task_ack_receipt("
    "uuid,bigint,text,timestamp with time zone,text,text,text,text,uuid)",
    "vp_authorize_worker_task_ack("
    "uuid,uuid,bigint,text,timestamp with time zone)",
    "vp_acknowledge_worker_task_delivery("
    "uuid,uuid,bigint,text,timestamp with time zone,"
    "text,text,text,text,uuid)",
    "vp_reserve_worker_youtube_upload("
    "uuid,bigint,text,timestamp with time zone,uuid,uuid,uuid,"
    "text,text,text)",
    "vp_transition_worker_youtube_upload("
    "uuid,bigint,text,timestamp with time zone,uuid,text,text,"
    "text,text,jsonb,text)",
    "vp_staging_janitor_readiness(integer,integer)",
    "vp_require_worker_redis_continuity(integer)",
)
WORKER_SELECT_COLUMNS: Mapping[str, tuple[str, ...]] = {
    "jobs": (
        "id",
        "pipeline_id",
        "pipeline_snapshot",
        "status",
        "execution_plan",
        "submitted_at",
        "started_at",
        "completed_at",
        "error_message",
        "submitted_by",
        "parent_job_id",
        "retry_count",
        "orchestrator_owner",
    ),
    "node_executions": (
        "id",
        "job_id",
        "node_id",
        "node_type",
        "node_label",
        "node_config",
        "status",
        "progress",
        "worker_id",
        "queued_at",
        "started_at",
        "completed_at",
        "error_message",
        "error_trace",
        "retry_count",
        "input_artifact_ids",
        "output_artifact_id",
        "worker_registration_id",
        "worker_lease_epoch",
    ),
    "artifacts": (
        "id",
        "job_id",
        "node_execution_id",
        "kind",
        "filename",
        "mime_type",
        "file_size",
        "storage_backend",
        "storage_path",
        "media_info",
        "created_at",
    ),
    "youtube_upload_operations": (
        "id",
        "production_task_id",
        "job_id",
        "node_execution_id",
        "input_artifact_id",
        "content_sha256",
        "title",
        "privacy",
        "status",
        "manager_task_id",
        "platform_video_id",
        "receipt_json",
        "error_message",
        "request_attempted_at",
        "completed_at",
        "created_at",
        "updated_at",
    ),
}


class RuntimeRoleError(RuntimeError):
    pass


class RuntimeRoleArgumentError(RuntimeRoleError):
    pass


class RuntimeRoleOwnerURLError(RuntimeRoleError):
    pass


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise RuntimeRoleArgumentError(message)


@dataclass(frozen=True)
class RuntimeRoleNames:
    stable: str
    versioned: str


@dataclass(frozen=True)
class RuntimeGenerationCredentials:
    database_url: str
    admission_token: str


@dataclass(frozen=True)
class RuntimeGrantAuthority:
    state: str


def role_names_for_generation(
    service_name: str,
    generation: int,
) -> RuntimeRoleNames:
    if (
        not isinstance(service_name, str)
        or not SERVICE_PATTERN.fullmatch(service_name)
        or isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 1
        or generation > MAX_GENERATION
    ):
        raise RuntimeRoleArgumentError("invalid worker generation")
    identity = f"{service_name}\0{generation}".encode()
    suffix = hashlib.sha256(identity).hexdigest()[:20]
    return RuntimeRoleNames(
        stable=STABLE_ROLE,
        versioned=f"vp_worker_{suffix}",
    )


def credential_paths(
    state_dir: Path,
    service_name: str,
    generation: int,
) -> dict[str, Path]:
    role_names_for_generation(service_name, generation)
    generation_dir = state_dir / service_name / str(generation)
    return {
        purpose: generation_dir / filename
        for purpose, filename in STATE_FILENAMES.items()
    }


def write_generation_state(
    state_dir: Path,
    service_name: str,
    generation: int,
    names: RuntimeRoleNames,
    *,
    database_url: str,
    admission_token: str,
) -> None:
    expected_names = role_names_for_generation(service_name, generation)
    if names != expected_names or not database_url or not admission_token:
        raise RuntimeRoleError("generation state invalid")
    payload = {
        "database_principal": names.versioned,
        "generation": generation,
        "service_name": service_name,
        "token_sha256": hashlib.sha256(
            admission_token.encode("utf-8")
        ).hexdigest(),
        "version": STATE_VERSION,
    }
    try:
        write_secure_files(
            state_dir,
            (service_name, str(generation)),
            {
                STATE_FILENAMES["database_url"]: f"{database_url}\n",
                STATE_FILENAMES["admission_token"]: (
                    f"{admission_token}\n"
                ),
                STATE_FILENAMES["state"]: (
                    json.dumps(
                        payload,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    + "\n"
                ),
            },
            file_modes={
                STATE_FILENAMES["database_url"]: 0o400,
                STATE_FILENAMES["admission_token"]: 0o400,
                STATE_FILENAMES["state"]: 0o600,
            },
        )
    except WorkerRoleCommonError as exc:
        raise RuntimeRoleError("generation state write failed") from exc


async def run(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        state_dir = Path(args.state_dir)
        if not state_dir.is_absolute():
            raise RuntimeRoleArgumentError("state dir must be absolute")
        generation = _parse_generation(args.generation)
        names = role_names_for_generation(args.service_name, generation)
    except (argparse.ArgumentError, RuntimeRoleArgumentError):
        _emit("error", "worker_runtime_invalid_arguments")
        return 2

    try:
        owner_url = load_database_url_file(OWNER_URL_FILE_ENV)
    except WorkerRoleCommonError:
        _emit("error", "worker_runtime_owner_url_invalid")
        return 3

    try:
        if args.command == "provision":
            await _provision(
                owner_url,
                args.service_name,
                generation,
                state_dir,
                names,
            )
            code = "worker_runtime_role_provisioned"
        else:
            await _revoke(
                owner_url,
                args.service_name,
                generation,
                state_dir,
                names,
            )
            code = "worker_runtime_role_revoked"
    except (
        asyncpg.PostgresError,
        OSError,
        RuntimeRoleError,
        WorkerRoleCommonError,
    ):
        _emit("error", "worker_runtime_role_operation_failed")
        return 4

    _emit(
        "ok",
        code,
        service_name=args.service_name,
        generation=generation,
        database_principal=names.versioned,
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(run(argv))


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        prog="worker-runtime-role",
        exit_on_error=False,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("provision", "revoke"):
        command_parser = subparsers.add_parser(
            command,
            exit_on_error=False,
        )
        command_parser.add_argument("--service-name", required=True)
        command_parser.add_argument("--generation", required=True)
        command_parser.add_argument("--state-dir", required=True)
    return parser


def _parse_generation(raw: str) -> int:
    if not raw.isascii() or not raw.isdigit() or raw.startswith("0"):
        raise RuntimeRoleArgumentError("invalid worker generation")
    try:
        generation = int(raw)
    except ValueError as exc:
        raise RuntimeRoleArgumentError("invalid worker generation") from exc
    if generation > MAX_GENERATION:
        raise RuntimeRoleArgumentError("invalid worker generation")
    return generation


async def _provision(
    owner_url: str,
    service_name: str,
    generation: int,
    state_dir: Path,
    names: RuntimeRoleNames,
) -> None:
    paths = credential_paths(state_dir, service_name, generation)
    connection = await asyncpg.connect(asyncpg_url(owner_url))
    created = False
    try:
        await acquire_stable_role_authority_locks(
            connection,
            (names.stable,),
        )
        for managed_service_name in _managed_runtime_service_names(
            state_dir,
            service_name,
        ):
            await acquire_worker_service_authority_lock(
                connection,
                managed_service_name,
            )
        await acquire_role_lifecycle_lock(
            connection,
            f"runtime:{service_name}:{generation}",
        )
        state_presence = {
            purpose: path.exists()
            for purpose, path in paths.items()
        }
        role_exists = bool(
            await connection.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_roles
                    WHERE rolname = $1
                )
                """,
                names.versioned,
            )
        )
        if all(state_presence.values()):
            try:
                credentials = await _validate_existing_generation(
                    connection,
                    owner_url,
                    service_name,
                    generation,
                    paths,
                    names,
                )
            except (
                asyncpg.PostgresError,
                OSError,
                RuntimeRoleError,
                WorkerRoleCommonError,
            ) as exc:
                await _deauthorize_generation(connection, names)
                raise RuntimeRoleError("generation state invalid") from exc
            authority = await _classify_generation_authority(
                connection,
                service_name,
                generation,
                names,
                credentials.admission_token,
            )
            if authority.state == "revoked":
                await _retire_local_generation(
                    connection,
                    state_dir,
                    service_name,
                    generation,
                    names,
                )
                raise RuntimeRoleError("generation authority revoked")
            if authority.state == "mismatch":
                await _deauthorize_generation(connection, names)
                raise RuntimeRoleError("generation authority mismatch")
            await _converge_existing_generation(
                connection,
                owner_url,
                state_dir,
                service_name,
                names,
                credentials,
            )
            return
        if any(state_presence.values()) or role_exists:
            reconstructed = await _reconstruct_generation_state(
                connection,
                owner_url,
                state_dir,
                service_name,
                generation,
                paths,
                names,
            )
            if reconstructed is not None:
                await _converge_existing_generation(
                    connection,
                    owner_url,
                    state_dir,
                    service_name,
                    names,
                    reconstructed,
                )
                return
            await _deauthorize_generation(connection, names)
            raise RuntimeRoleError("interrupted generation unauthorized")

        if await _generation_has_any_authority(
            connection,
            service_name,
            generation,
            names,
        ):
            raise RuntimeRoleError("fresh generation authority conflict")

        database_password = secrets.token_urlsafe(32)
        admission_token = secrets.token_urlsafe(32)
        authorized_members = await _authorized_runtime_members(
            connection,
            owner_url,
            state_dir,
            service_name,
        )
        async with connection.transaction():
            await ensure_stable_role(
                connection,
                names.stable,
                setting_prefix="worker_runtime",
                authorized_members=tuple(
                    sorted(
                        {*authorized_members, names.versioned}
                    )
                ),
            )
            await _set_runtime_privileges(connection, names.stable)
            await create_login_role(
                connection,
                names.versioned,
                database_password,
                setting_prefix="worker_runtime",
                stable_role=names.stable,
            )
            await connection.execute(
                f"GRANT {quote_identifier(names.stable)} "
                f"TO {quote_identifier(names.versioned)}"
            )
        created = True
        database_url = role_database_url(
            owner_url,
            names.versioned,
            database_password,
        )
        await verify_role_database_url(
            owner_url,
            database_url,
            names.versioned,
        )
        write_generation_state(
            state_dir,
            service_name,
            generation,
            names,
            database_url=database_url,
            admission_token=admission_token,
        )
    except BaseException:
        if created:
            try:
                remove_secure_files(
                    state_dir,
                    (service_name, str(generation)),
                    tuple(STATE_FILENAMES.values()),
                )
            except WorkerRoleCommonError:
                pass
            try:
                async with connection.transaction():
                    await drop_login_roles(
                        connection,
                        (names.versioned,),
                    )
            except (asyncpg.PostgresError, OSError, WorkerRoleCommonError):
                pass
        raise
    finally:
        await connection.close()


async def _classify_generation_authority(
    connection: asyncpg.Connection,
    service_name: str,
    generation: int,
    names: RuntimeRoleNames,
    admission_token: str,
) -> RuntimeGrantAuthority:
    rows = await connection.fetch(
        """
        SELECT
            grant_record.service_name,
            grant_record.generation,
            grant_record.database_principal,
            grant_record.token_sha256,
            grant_record.state,
            grant_record.revoked_at
        FROM public.worker_admission_grants AS grant_record
        WHERE (
            grant_record.service_name = $1
            AND grant_record.generation = $2
        )
           OR grant_record.database_principal = $3
        ORDER BY grant_record.id
        """,
        service_name,
        generation,
        names.versioned,
    )
    if not rows:
        return RuntimeGrantAuthority(state="none")
    if len(rows) != 1:
        return RuntimeGrantAuthority(state="mismatch")
    row = rows[0]
    identity_matches = (
        row["service_name"] == service_name
        and row["generation"] == generation
        and row["database_principal"] == names.versioned
    )
    if not identity_matches:
        return RuntimeGrantAuthority(state="mismatch")
    if row["state"] == "revoked":
        return RuntimeGrantAuthority(state="revoked")
    token_sha256 = hashlib.sha256(
        admission_token.encode("utf-8")
    ).hexdigest()
    if (
        row["token_sha256"] != token_sha256
        or row["revoked_at"] is not None
        or row["state"] not in {"pending", "active"}
    ):
        return RuntimeGrantAuthority(state="mismatch")
    return RuntimeGrantAuthority(state=row["state"])


async def _generation_has_any_authority(
    connection: asyncpg.Connection,
    service_name: str,
    generation: int,
    names: RuntimeRoleNames,
) -> bool:
    return bool(
        await connection.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                FROM public.worker_admission_grants AS grant_record
                WHERE (
                    grant_record.service_name = $1
                    AND grant_record.generation = $2
                )
                   OR grant_record.database_principal = $3
            )
            """,
            service_name,
            generation,
            names.versioned,
        )
    )


async def _deauthorize_generation(
    connection: asyncpg.Connection,
    names: RuntimeRoleNames,
) -> None:
    if not await connection.fetchval(
        """
        SELECT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_roles
            WHERE rolname = $1
        )
        """,
        names.versioned,
    ):
        return
    async with connection.transaction():
        quoted_versioned = quote_identifier(names.versioned)
        await connection.execute(
            f"ALTER ROLE {quoted_versioned} NOLOGIN"
        )
        if await connection.fetchval(
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
            names.stable,
            names.versioned,
        ):
            await connection.execute(
                f"REVOKE {quote_identifier(names.stable)} "
                f"FROM {quoted_versioned}"
            )
    await connection.execute(
        """
        SELECT pg_catalog.pg_terminate_backend(activity.pid)
        FROM pg_catalog.pg_stat_activity AS activity
        WHERE activity.usename = $1
          AND activity.pid <> pg_catalog.pg_backend_pid()
        """,
        names.versioned,
    )


async def _retire_local_generation(
    connection: asyncpg.Connection,
    state_dir: Path,
    service_name: str,
    generation: int,
    names: RuntimeRoleNames,
) -> None:
    async with connection.transaction():
        await drop_login_roles(connection, (names.versioned,))
    try:
        remove_secure_files(
            state_dir,
            (service_name, str(generation)),
            tuple(STATE_FILENAMES.values()),
        )
    except WorkerRoleCommonError as exc:
        raise RuntimeRoleError(
            "revoked generation cleanup failed"
        ) from exc


async def _set_runtime_privileges(
    connection: asyncpg.Connection,
    role_name: str,
) -> None:
    await reset_public_privileges(connection, role_name)
    await grant_functions(connection, role_name, WORKER_FUNCTIONS)
    for table_name, columns in WORKER_SELECT_COLUMNS.items():
        await grant_columns(
            connection,
            role_name,
            "SELECT",
            table_name,
            columns,
        )


async def _validate_existing_generation(
    connection: asyncpg.Connection,
    owner_url: str,
    service_name: str,
    generation: int,
    paths: Mapping[str, Path],
    names: RuntimeRoleNames,
) -> RuntimeGenerationCredentials:
    if not all(path.exists() for path in paths.values()):
        raise RuntimeRoleError("generation state incomplete")
    try:
        state = json.loads(
            read_secure_file(paths["state"], required_mode=0o600)
        )
        database_url = read_secure_file(
            paths["database_url"],
            required_mode=0o400,
        ).strip()
        admission_token = read_secure_file(
            paths["admission_token"],
            required_mode=0o400,
        ).strip()
    except (WorkerRoleCommonError, ValueError, TypeError) as exc:
        raise RuntimeRoleError("generation state invalid") from exc
    expected = {
        "database_principal": names.versioned,
        "generation": generation,
        "service_name": service_name,
        "token_sha256": hashlib.sha256(
            admission_token.encode("utf-8")
        ).hexdigest(),
        "version": STATE_VERSION,
    }
    if (
        state != expected
        or not admission_token
        or not await connection.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_roles
                WHERE rolname = $1
            )
            """,
            names.versioned,
        )
    ):
        raise RuntimeRoleError("generation state invalid")
    await verify_role_database_url(
        owner_url,
        database_url,
        names.versioned,
    )
    return RuntimeGenerationCredentials(
        database_url=database_url,
        admission_token=admission_token,
    )


async def _reconstruct_generation_state(
    connection: asyncpg.Connection,
    owner_url: str,
    state_dir: Path,
    service_name: str,
    generation: int,
    paths: Mapping[str, Path],
    names: RuntimeRoleNames,
) -> RuntimeGenerationCredentials | None:
    if (
        paths["state"].exists()
        or not paths["database_url"].exists()
        or not paths["admission_token"].exists()
    ):
        return None
    try:
        database_url = read_secure_file(
            paths["database_url"],
            required_mode=0o400,
        ).strip()
        admission_token = read_secure_file(
            paths["admission_token"],
            required_mode=0o400,
        ).strip()
    except WorkerRoleCommonError as exc:
        raise RuntimeRoleError(
            "interrupted generation credential invalid"
        ) from exc
    if not database_url or not admission_token:
        raise RuntimeRoleError(
            "interrupted generation credential invalid"
        )
    if not await connection.fetchval(
        """
        SELECT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_roles
            WHERE rolname = $1
        )
        """,
        names.versioned,
    ):
        return None
    await verify_role_database_url(
        owner_url,
        database_url,
        names.versioned,
    )
    authority = await _classify_generation_authority(
        connection,
        service_name,
        generation,
        names,
        admission_token,
    )
    if authority.state == "revoked":
        await _retire_local_generation(
            connection,
            state_dir,
            service_name,
            generation,
            names,
        )
        raise RuntimeRoleError("generation authority revoked")
    if authority.state not in {"pending", "active"}:
        await _deauthorize_generation(connection, names)
        raise RuntimeRoleError("interrupted generation unauthorized")
    write_generation_state(
        state_dir,
        service_name,
        generation,
        names,
        database_url=database_url,
        admission_token=admission_token,
    )
    return RuntimeGenerationCredentials(
        database_url=database_url,
        admission_token=admission_token,
    )


async def _converge_existing_generation(
    connection: asyncpg.Connection,
    owner_url: str,
    state_dir: Path,
    service_name: str,
    names: RuntimeRoleNames,
    credentials: RuntimeGenerationCredentials,
) -> None:
    authorized_members = await _authorized_runtime_members(
        connection,
        owner_url,
        state_dir,
        service_name,
    )
    async with connection.transaction():
        await ensure_stable_role(
            connection,
            names.stable,
            setting_prefix="worker_runtime",
            authorized_members=tuple(
                sorted({*authorized_members, names.versioned})
            ),
        )
        await harden_existing_login_role(
            connection,
            names.versioned,
            names.stable,
        )
        await _set_runtime_privileges(connection, names.stable)
    await verify_role_database_url(
        owner_url,
        credentials.database_url,
        names.versioned,
    )


async def _authorized_runtime_members(
    connection: asyncpg.Connection,
    owner_url: str,
    state_dir: Path,
    service_name: str,
) -> set[str]:
    if not SERVICE_PATTERN.fullmatch(service_name):
        raise RuntimeRoleError("generation service invalid")
    if not state_dir.exists():
        return set()
    _require_private_state_directory(state_dir)
    authorized: set[str] = set()
    with os.scandir(state_dir) as entries:
        service_names = sorted(
            entry.name
            for entry in entries
            if entry.is_dir(follow_symlinks=False)
        )
    for managed_service_name in service_names:
        if not SERVICE_PATTERN.fullmatch(managed_service_name):
            continue
        service_dir = state_dir / managed_service_name
        _require_private_state_directory(service_dir)
        with os.scandir(service_dir) as entries:
            generation_names = sorted(
                entry.name
                for entry in entries
                if entry.is_dir(follow_symlinks=False)
            )
        for generation_name in generation_names:
            if (
                not generation_name.isascii()
                or not generation_name.isdigit()
                or generation_name.startswith("0")
            ):
                continue
            generation = int(generation_name)
            if generation > MAX_GENERATION:
                continue
            generation_dir = service_dir / generation_name
            names = role_names_for_generation(
                managed_service_name,
                generation,
            )
            try:
                _require_private_state_directory(generation_dir)
                credentials = await _validate_existing_generation(
                    connection,
                    owner_url,
                    managed_service_name,
                    generation,
                    credential_paths(
                        state_dir,
                        managed_service_name,
                        generation,
                    ),
                    names,
                )
                authority = await _classify_generation_authority(
                    connection,
                    managed_service_name,
                    generation,
                    names,
                    credentials.admission_token,
                )
            except (
                asyncpg.PostgresError,
                OSError,
                RuntimeRoleError,
                WorkerRoleCommonError,
            ):
                await _deauthorize_generation(connection, names)
                continue
            if authority.state in {"none", "pending", "active"}:
                authorized.add(names.versioned)
            else:
                await _deauthorize_generation(connection, names)
    return authorized


def _managed_runtime_service_names(
    state_dir: Path,
    current_service_name: str,
) -> tuple[str, ...]:
    if not SERVICE_PATTERN.fullmatch(current_service_name):
        raise RuntimeRoleError("generation service invalid")
    service_names = {current_service_name}
    if not state_dir.exists():
        return tuple(service_names)
    _require_private_state_directory(state_dir)
    with os.scandir(state_dir) as entries:
        service_names.update(
            entry.name
            for entry in entries
            if entry.is_dir(follow_symlinks=False)
            and SERVICE_PATTERN.fullmatch(entry.name)
        )
    return tuple(sorted(service_names))


def _require_private_state_directory(path: Path) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise RuntimeRoleError("generation state directory invalid") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or metadata.st_uid != os.geteuid()
    ):
        raise RuntimeRoleError("generation state directory invalid")


async def _revoke(
    owner_url: str,
    service_name: str,
    generation: int,
    state_dir: Path,
    names: RuntimeRoleNames,
) -> None:
    connection = await asyncpg.connect(asyncpg_url(owner_url))
    try:
        await acquire_stable_role_authority_locks(
            connection,
            (names.stable,),
        )
        await acquire_worker_service_authority_lock(
            connection,
            service_name,
        )
        await acquire_role_lifecycle_lock(
            connection,
            f"runtime:{service_name}:{generation}",
        )
        async with connection.transaction():
            if await connection.fetchval(
                """
                SELECT
                    EXISTS (
                        SELECT 1
                        FROM public.worker_admission_grants
                        WHERE database_principal = $1
                          AND revoked_at IS NULL
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM public.worker_registrations AS registration
                        JOIN public.worker_admission_grants AS grant_record
                          ON grant_record.id = registration.grant_id
                        WHERE grant_record.database_principal = $1
                          AND registration.status = 'active'
                    )
                """,
                names.versioned,
            ):
                raise RuntimeRoleError("worker role is still admitted")
            await drop_login_roles(connection, (names.versioned,))
        remove_secure_files(
            state_dir,
            (service_name, str(generation)),
            tuple(STATE_FILENAMES.values()),
        )
    except WorkerRoleCommonError as exc:
        raise RuntimeRoleError("generation state removal failed") from exc
    finally:
        await connection.close()


def _emit(status: str, code: str, **fields: object) -> None:
    payload = {"code": code, "status": status, **fields}
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(main())
