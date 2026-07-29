from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
import secrets
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Never

import asyncpg  # type: ignore[import-untyped]

from app.services.worker_role_cli_common import (
    WorkerRoleCommonError,
    acquire_role_lifecycle_lock,
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
            await _validate_existing_generation(
                connection,
                owner_url,
                service_name,
                generation,
                paths,
                names,
            )
            async with connection.transaction():
                await ensure_stable_role(
                    connection,
                    names.stable,
                    setting_prefix="worker_runtime",
                    authorized_members=(names.versioned,),
                )
                await harden_existing_login_role(
                    connection,
                    names.versioned,
                    names.stable,
                )
                await _set_runtime_privileges(connection, names.stable)
            database_url = read_secure_file(
                paths["database_url"],
                required_mode=0o400,
            ).strip()
            await verify_role_database_url(
                owner_url,
                database_url,
                names.versioned,
            )
            return
        if any(state_presence.values()) or role_exists:
            await _recover_interrupted_generation(
                connection,
                state_dir,
                service_name,
                generation,
                names,
            )

        database_password = secrets.token_urlsafe(32)
        admission_token = secrets.token_urlsafe(32)
        async with connection.transaction():
            await ensure_stable_role(
                connection,
                names.stable,
                setting_prefix="worker_runtime",
                authorized_members=(names.versioned,),
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


async def _recover_interrupted_generation(
    connection: asyncpg.Connection,
    state_dir: Path,
    service_name: str,
    generation: int,
    names: RuntimeRoleNames,
) -> None:
    if await connection.fetchval(
        """
        SELECT EXISTS (
            SELECT 1
            FROM public.worker_admission_grants AS grant_record
            WHERE grant_record.database_principal = $1
            UNION ALL
            SELECT 1
            FROM public.worker_registrations AS registration
            JOIN public.worker_admission_grants AS grant_record
              ON grant_record.id = registration.grant_id
            WHERE grant_record.database_principal = $1
        )
        """,
        names.versioned,
    ):
        raise RuntimeRoleError("interrupted generation is durable")
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
            "interrupted generation cleanup failed"
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
) -> None:
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


async def _revoke(
    owner_url: str,
    service_name: str,
    generation: int,
    state_dir: Path,
    names: RuntimeRoleNames,
) -> None:
    connection = await asyncpg.connect(asyncpg_url(owner_url))
    try:
        await acquire_role_lifecycle_lock(
            connection,
            f"runtime:{service_name}:{generation}",
        )
        async with connection.transaction():
            await connection.execute(
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
