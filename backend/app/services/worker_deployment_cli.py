from __future__ import annotations

import argparse
import asyncio
import contextlib
import io
import json
import os
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Never

import asyncpg  # type: ignore[import-untyped]
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import make_url

from app.services import worker_registration_operator_cli as operator_cli
from app.services import worker_runtime_role_cli as runtime_cli
from app.services.worker_role_cli_common import (
    WorkerRoleCommonError,
    asyncpg_url,
    load_database_url_file,
    read_secure_file,
    remove_secure_files,
    write_secure_files,
)


DEPLOY_MIGRATOR_URL_FILE_ENV = "WORKER_DEPLOY_MIGRATOR_DATABASE_URL_FILE"
DEPLOY_READ_URL_FILE_ENV = "WORKER_DEPLOY_READ_DATABASE_URL_FILE"
EXPECTED_MIGRATION_HEAD = "034_worker_registrations"
MAX_PORT = 65535
MAX_REDIS_DATABASE = 15


class WorkerDeploymentError(RuntimeError):
    pass


class WorkerDeploymentArgumentError(WorkerDeploymentError):
    pass


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise WorkerDeploymentArgumentError(message)


async def run(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
    except (
        argparse.ArgumentError,
        WorkerDeploymentArgumentError,
    ):
        _emit("error", "worker_deployment_invalid_arguments")
        return 2

    if args.command == "migrate":
        if os.environ.get("DATABASE_URL", "").strip():
            _emit("error", "worker_deployment_migrator_url_invalid")
            return 3
        try:
            database_url = load_database_url_file(
                DEPLOY_MIGRATOR_URL_FILE_ENV
            )
        except WorkerRoleCommonError:
            _emit("error", "worker_deployment_migrator_url_invalid")
            return 3
        try:
            _upgrade_database(database_url)
        except Exception:
            _emit("error", "worker_deployment_migration_failed")
            return 4
        _emit("ok", "worker_deployment_migrated")
        return 0

    if args.command == "verify-head":
        return await _verify_migration_head()

    try:
        service_name = _service_name(args.service_name)
        generation = runtime_cli._parse_generation(args.generation)
    except (
        WorkerDeploymentArgumentError,
        runtime_cli.RuntimeRoleArgumentError,
    ):
        _emit("error", "worker_deployment_invalid_arguments")
        return 2

    if args.command == "render-request":
        try:
            _render_request(args, service_name, generation)
        except (
            OSError,
            ValueError,
            TypeError,
            WorkerDeploymentError,
            WorkerRoleCommonError,
            operator_cli.OperatorRequestError,
        ):
            _emit("error", "worker_deployment_request_failed")
            return 3
        _emit(
            "ok",
            "worker_deployment_request_rendered",
            service_name=service_name,
            generation=generation,
        )
        return 0

    try:
        database_url = load_database_url_file(
            DEPLOY_READ_URL_FILE_ENV
        )
    except WorkerRoleCommonError:
        _emit("error", "worker_deployment_read_url_invalid")
        return 3
    connection: asyncpg.Connection | None = None
    try:
        connection = await asyncpg.connect(asyncpg_url(database_url))
        if args.command == "retirement-candidates":
            rows = await connection.fetch(
                """
                SELECT registration.id AS registration_id
                FROM public.worker_registrations AS registration
                JOIN public.worker_admission_grants AS grant_record
                  ON grant_record.id = registration.grant_id
                WHERE grant_record.service_name = $1
                  AND grant_record.generation = $2
                  AND registration.status = 'active'
                ORDER BY registration.id
                """,
                service_name,
                generation,
            )
            registration_ids = [
                _canonical_uuid(row["registration_id"])
                for row in rows
            ]
            ready = False
            grant_state = ""
        elif args.command == "generation-state":
            raw_grant_state = await connection.fetchval(
                """
                SELECT grant_record.state::text
                FROM public.worker_admission_grants AS grant_record
                WHERE grant_record.service_name = $1
                  AND grant_record.generation = $2
                """,
                service_name,
                generation,
            )
            if raw_grant_state is None:
                grant_state = "absent"
            elif raw_grant_state in {"pending", "active", "revoked"}:
                grant_state = raw_grant_state
            else:
                raise WorkerDeploymentError(
                    "worker grant state is invalid"
                )
            registration_ids = []
            ready = False
        else:
            registration_ids = []
            grant_state = ""
            ready = bool(
                await connection.fetchval(
                    """
                    SELECT count(*) = 1
                    FROM public.worker_registrations AS registration
                    JOIN public.worker_admission_grants AS grant_record
                      ON grant_record.id = registration.grant_id
                    CROSS JOIN LATERAL
                      public.vp_worker_endpoint_fingerprints(
                        grant_record.endpoint_bindings_json
                      ) AS expected_fingerprints
                    WHERE grant_record.service_name = $1
                      AND grant_record.generation = $2
                      AND grant_record.state = 'active'
                      AND grant_record.revoked_at IS NULL
                      AND registration.status = 'active'
                      AND registration.service_name
                          = grant_record.service_name
                      AND registration.worker_type
                          = grant_record.worker_type
                      AND registration.worker_host
                          = grant_record.worker_host
                      AND registration.capabilities_json
                          = grant_record.capabilities_json
                      AND registration.image_identity
                          = grant_record.image_identity
                      AND registration.database_principal
                          = grant_record.database_principal
                      AND registration.worker_slot = 1
                      AND registration.redis_consumer_id =
                          registration.worker_type
                          || '-worker@'
                          || registration.worker_host
                          || ':'
                          || registration.worker_slot::text
                          || ':'
                          || registration.worker_instance_id::text
                      AND registration.database_fingerprint
                          = expected_fingerprints.database_fingerprint
                      AND registration.redis_fingerprint
                          = expected_fingerprints.redis_fingerprint
                      AND registration.storage_fingerprint
                          = expected_fingerprints.storage_fingerprint
                      AND registration.lease_expires_at
                          > pg_catalog.clock_timestamp()
                            + interval '60 seconds'
                    """,
                    service_name,
                    generation,
                )
            )
    except (asyncpg.PostgresError, OSError, WorkerDeploymentError):
        _emit("error", "worker_deployment_readiness_failed")
        return 4
    finally:
        if connection is not None:
            await connection.close()
    if args.command == "retirement-candidates":
        _emit(
            "ok",
            "worker_deployment_retirement_candidates",
            service_name=service_name,
            generation=generation,
            registration_ids=registration_ids,
        )
        return 0
    if args.command == "generation-state":
        _emit(
            "ok",
            "worker_deployment_generation_state",
            service_name=service_name,
            generation=generation,
            grant_state=grant_state,
        )
        return 0
    if not ready:
        _emit(
            "unready",
            "worker_deployment_unready",
            service_name=service_name,
            generation=generation,
        )
        return 5
    _emit(
        "ok",
        "worker_deployment_ready",
        service_name=service_name,
        generation=generation,
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(run(argv))


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        prog="worker-deployment",
        exit_on_error=False,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser(
        "migrate",
        exit_on_error=False,
    )
    subparsers.add_parser(
        "verify-head",
        exit_on_error=False,
    )
    render = subparsers.add_parser(
        "render-request",
        exit_on_error=False,
    )
    render.add_argument("--service-name", required=True)
    render.add_argument("--generation", required=True)
    render.add_argument("--release-commit", required=True)
    render.add_argument("--image-identity", required=True)
    render.add_argument("--state-dir", required=True)
    render.add_argument("--request-file", required=True)
    render.add_argument("--redis-host", required=True)
    render.add_argument("--redis-port", required=True)
    render.add_argument("--redis-database", required=True)
    render.add_argument("--storage-host", required=True)
    render.add_argument("--storage-port", required=True)
    render.add_argument("--storage-bucket", required=True)
    readiness = subparsers.add_parser(
        "readiness",
        exit_on_error=False,
    )
    readiness.add_argument("--service-name", required=True)
    readiness.add_argument("--generation", required=True)
    retirement = subparsers.add_parser(
        "retirement-candidates",
        exit_on_error=False,
    )
    retirement.add_argument("--service-name", required=True)
    retirement.add_argument("--generation", required=True)
    generation_state = subparsers.add_parser(
        "generation-state",
        exit_on_error=False,
    )
    generation_state.add_argument("--service-name", required=True)
    generation_state.add_argument("--generation", required=True)
    return parser


async def _verify_migration_head() -> int:
    try:
        database_url = load_database_url_file(
            DEPLOY_READ_URL_FILE_ENV
        )
    except WorkerRoleCommonError:
        _emit("error", "worker_deployment_read_url_invalid")
        return 3
    connection: asyncpg.Connection | None = None
    try:
        connection = await asyncpg.connect(asyncpg_url(database_url))
        rows = await connection.fetch(
            "SELECT version_num FROM public.alembic_version "
            "ORDER BY version_num"
        )
        revisions = [
            row["version_num"]
            for row in rows
            if isinstance(row["version_num"], str)
        ]
    except (asyncpg.PostgresError, OSError, KeyError, TypeError):
        _emit("error", "worker_deployment_migration_head_failed")
        return 4
    finally:
        if connection is not None:
            await connection.close()
    if revisions != [EXPECTED_MIGRATION_HEAD]:
        _emit("unready", "worker_deployment_migration_head_unready")
        return 5
    _emit("ok", "worker_deployment_migration_head_verified")
    return 0


def _upgrade_database(database_url: str) -> None:
    backend_root = Path(__file__).resolve().parents[2]
    config = Config(str(backend_root / "alembic.ini"))
    config.set_main_option(
        "script_location",
        str(backend_root / "alembic"),
    )
    config.set_main_option(
        "sqlalchemy.url",
        database_url.replace("%", "%%"),
    )
    with (
        contextlib.redirect_stdout(io.StringIO()),
        contextlib.redirect_stderr(io.StringIO()),
    ):
        command.upgrade(config, EXPECTED_MIGRATION_HEAD)


def _render_request(
    args: argparse.Namespace,
    service_name: str,
    generation: int,
) -> None:
    topology = operator_cli.TOPOLOGY[service_name]
    state_dir = Path(args.state_dir)
    request_file = Path(args.request_file)
    if (
        not state_dir.is_absolute()
        or not request_file.is_absolute()
        or request_file.name != "upsert.json"
        or request_file.parent.name != str(generation)
    ):
        raise WorkerDeploymentError("deployment path invalid")

    paths = runtime_cli.credential_paths(
        state_dir,
        service_name,
        generation,
    )
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
    names = runtime_cli.role_names_for_generation(
        service_name,
        generation,
    )
    expected_state = {
        "database_principal": names.versioned,
        "generation": generation,
        "service_name": service_name,
        "token_sha256": runtime_cli.hashlib.sha256(
            admission_token.encode("utf-8")
        ).hexdigest(),
        "version": runtime_cli.STATE_VERSION,
    }
    if state != expected_state:
        raise WorkerDeploymentError("runtime state invalid")
    parsed_database = make_url(database_url)
    if (
        parsed_database.username != names.versioned
        or parsed_database.host is None
        or parsed_database.database is None
    ):
        raise WorkerDeploymentError("runtime database identity invalid")

    request = {
        "version": 1,
        "service_name": service_name,
        "generation": generation,
        "worker_type": topology["worker_type"],
        "worker_host": topology["worker_host"],
        "capabilities": list(topology["capabilities"]),
        "release_commit": args.release_commit,
        "image_identity": args.image_identity,
        "database_principal": names.versioned,
        "redis_stream": topology["redis_stream"],
        "redis_group": topology["redis_group"],
        "endpoint_bindings": {
            "database": {
                "driver": "postgresql",
                "host": parsed_database.host,
                "port": parsed_database.port or 5432,
                "database": parsed_database.database,
            },
            "redis": {
                "scheme": "redis",
                "host": args.redis_host,
                "port": _bounded_integer(
                    args.redis_port,
                    minimum=1,
                    maximum=MAX_PORT,
                ),
                "database": _bounded_integer(
                    args.redis_database,
                    minimum=0,
                    maximum=MAX_REDIS_DATABASE,
                ),
            },
            "storage": {
                "backend": "minio",
                "host": args.storage_host,
                "port": _bounded_integer(
                    args.storage_port,
                    minimum=1,
                    maximum=MAX_PORT,
                ),
                "bucket": args.storage_bucket,
            },
        },
        "token_sha256": state["token_sha256"],
        "issued_by": "vp-deploy-controller",
    }
    encoded = json.dumps(
        request,
        sort_keys=True,
        separators=(",", ":"),
    )
    write_secure_files(
        request_file.parent.parent,
        (request_file.parent.name,),
        {request_file.name: f"{encoded}\n"},
        file_mode=0o600,
    )
    try:
        operator_cli.load_upsert_request(request_file)
    except operator_cli.OperatorRequestError:
        remove_secure_files(
            request_file.parent.parent,
            (request_file.parent.name,),
            (request_file.name,),
        )
        raise


def _service_name(value: str) -> str:
    if value not in operator_cli.TOPOLOGY:
        raise WorkerDeploymentArgumentError("service name invalid")
    return value


def _bounded_integer(raw: str, *, minimum: int, maximum: int) -> int:
    if not raw.isascii() or not raw.isdigit():
        raise WorkerDeploymentArgumentError("integer invalid")
    value = int(raw)
    if not minimum <= value <= maximum:
        raise WorkerDeploymentArgumentError("integer invalid")
    return value


def _canonical_uuid(value: object) -> str:
    if not isinstance(value, uuid.UUID):
        raise WorkerDeploymentError("registration identity invalid")
    return str(value)


def _emit(status: str, code: str, **fields: object) -> None:
    print(
        json.dumps(
            {"code": code, "status": status, **fields},
            sort_keys=True,
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
