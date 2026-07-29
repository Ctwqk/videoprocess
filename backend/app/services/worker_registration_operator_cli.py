from __future__ import annotations

import argparse
import asyncio
import json
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Never

import asyncpg  # type: ignore[import-untyped]

from app.services.worker_role_cli_common import (
    WorkerRoleCommonError,
    asyncpg_url,
    load_database_url_file,
    read_secure_file,
)


DATABASE_URL_FILE_ENV = "WORKER_REGISTRATION_OPERATOR_DATABASE_URL_FILE"
SERVICE_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
WORKER_VALUE_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
REDIS_VALUE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9:_-]{0,254}$")
PRINCIPAL_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
RELEASE_PATTERN = re.compile(r"^[0-9a-f]{40}$")
IMAGE_PATTERN = re.compile(
    r"^[A-Za-z0-9][-A-Za-z0-9._/]*:deploy-[0-9a-f]{12}$"
)
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
HOST_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.-]{0,254}$")
MAX_GENERATION = 9_223_372_036_854_775_807
REQUEST_FIELDS = {
    "version",
    "service_name",
    "generation",
    "worker_type",
    "worker_host",
    "capabilities",
    "release_commit",
    "image_identity",
    "database_principal",
    "redis_stream",
    "redis_group",
    "endpoint_bindings",
    "token_sha256",
    "issued_by",
}
TOPOLOGY = {
    "vp-ffmpeg-worker-go-swarm": {
        "worker_type": "ffmpeg_go",
        "worker_host": "colima-127",
        "capabilities": ("media_cpu",),
        "redis_stream": "vp:tasks:ffmpeg_go",
        "redis_group": "ffmpeg_go-workers",
    },
    "vp-ffmpeg-worker-gpu-swarm": {
        "worker_type": "ffmpeg",
        "worker_host": "150-gpu",
        "capabilities": ("media_gpu",),
        "redis_stream": "vp:tasks:ffmpeg",
        "redis_group": "ffmpeg-workers",
    },
    "vp-vision-worker-swarm": {
        "worker_type": "vision",
        "worker_host": "150-vision",
        "capabilities": ("vision_gpu",),
        "redis_stream": "vp:tasks:vision",
        "redis_group": "vision-workers",
    },
    "vp-youtube-publisher-swarm": {
        "worker_type": "youtube_publisher",
        "worker_host": "150-publisher",
        "capabilities": ("youtube_publisher",),
        "redis_stream": "vp:tasks:youtube_publisher",
        "redis_group": "youtube_publisher-workers",
    },
}


class OperatorError(RuntimeError):
    pass


class OperatorRequestError(OperatorError):
    pass


class OperatorArgumentError(OperatorError):
    pass


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise OperatorArgumentError(message)


@dataclass(frozen=True)
class WorkerGrantUpsertRequest:
    version: int
    service_name: str
    generation: int
    worker_type: str
    worker_host: str
    capabilities: tuple[str, ...]
    release_commit: str
    image_identity: str
    database_principal: str
    redis_stream: str
    redis_group: str
    endpoint_bindings: Mapping[str, object]
    token_sha256: str
    issued_by: str


def load_upsert_request(path: Path) -> WorkerGrantUpsertRequest:
    if not path.is_absolute():
        raise OperatorRequestError("request path must be absolute")
    try:
        raw = read_secure_file(path, required_mode=0o600)
        payload = json.loads(
            raw,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
    except (
        WorkerRoleCommonError,
        UnicodeError,
        json.JSONDecodeError,
        OperatorRequestError,
    ) as exc:
        raise OperatorRequestError("request file invalid") from exc
    if not isinstance(payload, dict) or set(payload) != REQUEST_FIELDS:
        raise OperatorRequestError("request schema invalid")

    version = _exact_int(payload["version"], minimum=1, maximum=1)
    generation = _exact_int(
        payload["generation"],
        minimum=1,
        maximum=MAX_GENERATION,
    )
    service_name = _matched_string(
        payload["service_name"],
        SERVICE_PATTERN,
    )
    worker_type = _matched_string(
        payload["worker_type"],
        WORKER_VALUE_PATTERN,
    )
    worker_host = _matched_string(payload["worker_host"], HOST_PATTERN)
    capabilities_value = payload["capabilities"]
    if (
        not isinstance(capabilities_value, list)
        or not capabilities_value
        or any(
            not isinstance(capability, str)
            or not WORKER_VALUE_PATTERN.fullmatch(capability)
            for capability in capabilities_value
        )
        or capabilities_value != sorted(set(capabilities_value))
    ):
        raise OperatorRequestError("capabilities are not canonical")
    capabilities = tuple(capabilities_value)
    release_commit = _matched_string(
        payload["release_commit"],
        RELEASE_PATTERN,
    )
    image_identity = _matched_string(
        payload["image_identity"],
        IMAGE_PATTERN,
    )
    database_principal = _matched_string(
        payload["database_principal"],
        PRINCIPAL_PATTERN,
    )
    redis_stream = _matched_string(
        payload["redis_stream"],
        REDIS_VALUE_PATTERN,
    )
    redis_group = _matched_string(
        payload["redis_group"],
        REDIS_VALUE_PATTERN,
    )
    endpoint_bindings = _validate_endpoint_bindings(
        payload["endpoint_bindings"]
    )
    token_sha256 = _matched_string(
        payload["token_sha256"],
        SHA256_PATTERN,
    )
    issued_by = _canonical_string(payload["issued_by"], maximum=255)
    if issued_by != "vp-deploy-controller":
        raise OperatorRequestError("issued_by is invalid")

    topology = TOPOLOGY.get(service_name)
    if topology is None or any(
        actual != topology[field]
        for field, actual in (
            ("worker_type", worker_type),
            ("worker_host", worker_host),
            ("capabilities", capabilities),
            ("redis_stream", redis_stream),
            ("redis_group", redis_group),
        )
    ):
        raise OperatorRequestError("worker topology is invalid")
    if _contains_host_126(endpoint_bindings):
        raise OperatorRequestError("host 126 is forbidden")

    return WorkerGrantUpsertRequest(
        version=version,
        service_name=service_name,
        generation=generation,
        worker_type=worker_type,
        worker_host=worker_host,
        capabilities=capabilities,
        release_commit=release_commit,
        image_identity=image_identity,
        database_principal=database_principal,
        redis_stream=redis_stream,
        redis_group=redis_group,
        endpoint_bindings=endpoint_bindings,
        token_sha256=token_sha256,
        issued_by=issued_by,
    )


async def run(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        operation = _operation_from_arguments(args)
    except (argparse.ArgumentError, OperatorArgumentError, OperatorRequestError):
        _emit("error", "worker_operator_invalid_arguments")
        return 2

    try:
        database_url = load_database_url_file(DATABASE_URL_FILE_ENV)
    except WorkerRoleCommonError:
        _emit("error", "worker_operator_database_url_invalid")
        return 3

    connection: asyncpg.Connection | None = None
    try:
        connection = await asyncpg.connect(asyncpg_url(database_url))
        result = await connection.fetchval(
            operation.query,
            *operation.arguments,
        )
    except (asyncpg.PostgresError, OSError, OperatorError):
        _emit("error", "worker_operator_operation_failed")
        return 4
    finally:
        if connection is not None:
            await connection.close()

    fields: dict[str, object] = {}
    if operation.result_field is not None:
        fields[operation.result_field] = str(result)
    _emit("ok", operation.success_code, **fields)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(run(argv))


@dataclass(frozen=True)
class _Operation:
    query: str
    arguments: tuple[object, ...]
    success_code: str
    result_field: str | None = None


def _operation_from_arguments(args: argparse.Namespace) -> _Operation:
    if args.command == "upsert":
        request = load_upsert_request(Path(args.request_file))
        return _Operation(
            query=(
                "SELECT public.vp_worker_grant_upsert("
                "$1, $2, $3, $4, $5::jsonb, $6, $7, $8, "
                "$9, $10, $11::jsonb, $12, $13)"
            ),
            arguments=(
                request.service_name,
                request.generation,
                request.worker_type,
                request.worker_host,
                json.dumps(
                    request.capabilities,
                    separators=(",", ":"),
                ),
                request.release_commit,
                request.image_identity,
                request.database_principal,
                request.redis_stream,
                request.redis_group,
                json.dumps(
                    request.endpoint_bindings,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                request.token_sha256,
                request.issued_by,
            ),
            success_code="worker_grant_upserted",
            result_field="grant_id",
        )

    service_name = _matched_string(args.service_name, SERVICE_PATTERN)
    if service_name not in TOPOLOGY:
        raise OperatorArgumentError("service is not production admitted")
    if args.command == "activate":
        generation = _parse_generation(args.generation)
        return _Operation(
            query="SELECT public.vp_worker_grant_activate($1, $2)",
            arguments=(service_name, generation),
            success_code="worker_grant_activated",
            result_field="grant_id",
        )
    if args.command == "revoke-grant":
        generation = _parse_generation(args.generation)
        reason = _canonical_string(args.reason, maximum=255)
        return _Operation(
            query="SELECT public.vp_worker_grant_revoke($1, $2, $3)",
            arguments=(service_name, generation, reason),
            success_code="worker_grant_revoked",
        )

    registration_id = _parse_uuid(args.registration_id)
    if args.command == "revoke-registration":
        reason = _canonical_string(args.reason, maximum=255)
        return _Operation(
            query=(
                "SELECT public.vp_worker_registration_revoke("
                "$1, $2::uuid, $3)"
            ),
            arguments=(service_name, registration_id, reason),
            success_code="worker_registration_revoked",
        )
    return _Operation(
        query=(
            "SELECT public.vp_worker_registration_expire("
            "$1, $2::uuid)"
        ),
        arguments=(service_name, registration_id),
        success_code="worker_registration_expired",
    )


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        prog="worker-registration-operator",
        exit_on_error=False,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    upsert = subparsers.add_parser("upsert", exit_on_error=False)
    upsert.add_argument("--request-file", required=True)
    activate = subparsers.add_parser("activate", exit_on_error=False)
    activate.add_argument("--service-name", required=True)
    activate.add_argument("--generation", required=True)
    revoke_grant = subparsers.add_parser(
        "revoke-grant",
        exit_on_error=False,
    )
    revoke_grant.add_argument("--service-name", required=True)
    revoke_grant.add_argument("--generation", required=True)
    revoke_grant.add_argument("--reason", required=True)
    revoke_registration = subparsers.add_parser(
        "revoke-registration",
        exit_on_error=False,
    )
    revoke_registration.add_argument("--service-name", required=True)
    revoke_registration.add_argument("--registration-id", required=True)
    revoke_registration.add_argument("--reason", required=True)
    expire_registration = subparsers.add_parser(
        "expire-registration",
        exit_on_error=False,
    )
    expire_registration.add_argument("--service-name", required=True)
    expire_registration.add_argument("--registration-id", required=True)
    return parser


def _unique_object(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise OperatorRequestError("duplicate JSON key")
        result[key] = value
    return result


def _reject_json_constant(value: str) -> Never:
    raise OperatorRequestError(f"invalid JSON constant: {value}")


def _matched_string(value: object, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise OperatorRequestError("request string is invalid")
    return value


def _canonical_string(value: object, *, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= maximum
        or value != value.strip()
        or "\x00" in value
    ):
        raise OperatorArgumentError("argument string is invalid")
    return value


def _exact_int(value: object, *, minimum: int, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
        or value > maximum
    ):
        raise OperatorRequestError("request integer is invalid")
    return value


def _parse_generation(value: str) -> int:
    if (
        not isinstance(value, str)
        or not value.isascii()
        or not value.isdigit()
        or value.startswith("0")
    ):
        raise OperatorArgumentError("generation is invalid")
    generation = int(value)
    if generation > MAX_GENERATION:
        raise OperatorArgumentError("generation is invalid")
    return generation


def _parse_uuid(value: str) -> uuid.UUID:
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as exc:
        raise OperatorArgumentError("registration id is invalid") from exc
    if str(parsed) != value:
        raise OperatorArgumentError("registration id is not canonical")
    return parsed


def _validate_endpoint_bindings(value: object) -> Mapping[str, object]:
    if not isinstance(value, dict) or set(value) != {
        "database",
        "redis",
        "storage",
    }:
        raise OperatorRequestError("endpoint bindings are invalid")
    database = _validate_endpoint(
        value["database"],
        {
            "driver": "postgresql",
            "host": None,
            "port": None,
            "database": None,
        },
    )
    redis = _validate_endpoint(
        value["redis"],
        {
            "scheme": "redis",
            "host": None,
            "port": None,
            "database": None,
        },
    )
    storage = _validate_endpoint(
        value["storage"],
        {
            "backend": "minio",
            "host": None,
            "port": None,
            "bucket": None,
        },
    )
    if (
        not HOST_PATTERN.fullmatch(str(database["host"]))
        or not HOST_PATTERN.fullmatch(str(redis["host"]))
        or not HOST_PATTERN.fullmatch(str(storage["host"]))
    ):
        raise OperatorRequestError("endpoint host is invalid")
    for endpoint in (database, redis, storage):
        _exact_int(endpoint["port"], minimum=1, maximum=65535)
    _exact_int(redis["database"], minimum=0, maximum=15)
    _canonical_string(database["database"], maximum=63)
    _canonical_string(storage["bucket"], maximum=63)
    return {
        "database": database,
        "redis": redis,
        "storage": storage,
    }


def _validate_endpoint(
    value: object,
    schema: Mapping[str, str | None],
) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != set(schema):
        raise OperatorRequestError("endpoint is invalid")
    result = dict(value)
    for field, fixed_value in schema.items():
        if fixed_value is not None and result[field] != fixed_value:
            raise OperatorRequestError("endpoint is invalid")
    return result


def _contains_host_126(value: Mapping[str, object]) -> bool:
    forbidden = {
        "126",
        "10.0.0.126",
        "colima-126",
        "ccttww-126",
        "colima-swarmbridged",
    }
    return any(
        str(endpoint["host"]).lower() in forbidden
        for endpoint in value.values()
        if isinstance(endpoint, dict) and "host" in endpoint
    )


def _emit(status: str, code: str, **fields: object) -> None:
    payload = {"code": code, **fields, "status": status}
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    raise SystemExit(main())
