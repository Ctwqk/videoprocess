"""Credential-bound Unit 2 runtime, deliberately without a CLI or call site.

Unit 3 must validate exact managed secret IDs/mount descriptors and hold the sync
and admission locks until this coroutine (including cleanup) settles. Its durable
authority callbacks fence attempts across processes. Nothing here manufactures
that authority or retries an uncertain EVAL.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from collections.abc import Awaitable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, Protocol, TypeVar
from urllib.parse import unquote, urlsplit
from uuid import UUID

import asyncpg  # type: ignore[import-untyped]
from redis.asyncio import Redis
from redis.backoff import NoBackoff
from redis.asyncio.retry import Retry
from sqlalchemy.engine import make_url

from app.models.worker_registration import WorkerAdmissionGrant, WorkerRegistration
from app.services.registered_consumer_reconcile import (
    EvalCommand,
    PinDocument,
    assess,
    decode_pins,
    validate_database,
    validate_lua_result,
)
from app.services.worker_control_role_cli import role_names_for_generation
from app.services.worker_registration import (
    _validated_database_binding,
    _validated_redis_binding,
)
from app.services.worker_role_cli_common import asyncpg_url, read_secure_file


TOTAL_SECONDS = 200.0
LOCKED_SECONDS = 8.0
IO_SECONDS = 2.0
ROLLBACK_SECONDS = 2.0
CLOSE_SECONDS = 2.0
POLL_SECONDS = 1.0
MOUNT_UID = MOUNT_GID = 10001
MOUNTS = {
    "database": Path("/run/secrets/registered-reconcile-database-url"),
    "redis": Path("/run/secrets/registered-reconcile-redis-url"),
    "pins": Path("/run/secrets/registered-reconcile-pins"),
}
FORBIDDEN_ENV = frozenset(
    {
        "DATABASE_URL",
        "DATABASE_URL_FILE",
        "REDIS_URL",
        "REDIS_URL_FILE",
        "WORKER_DATABASE_URL_FILE",
        "WORKER_REGISTRATION_OPERATOR_DATABASE_URL_FILE",
        "WORKER_CONTROL_ROLE_OWNER_DATABASE_URL_FILE",
        "WORKER_RUNTIME_ROLE_OWNER_DATABASE_URL_FILE",
        "VISION_CUTOVER_DATABASE_URL_FILE",
        "VISION_CUTOVER_REDIS_URL_FILE",
        "PGHOST",
        "PGPORT",
        "PGDATABASE",
        "PGUSER",
        "PGPASSWORD",
        "PGPASSFILE",
        "PGSERVICE",
        "PGSERVICEFILE",
        "PGOPTIONS",
        "PGSSLMODE",
        "PGSSLROOTCERT",
        "PGSSLCERT",
        "PGSSLKEY",
        "PGSSLCRL",
        "PGSSLNEGOTIATION",
        "PGSSLMINPROTOCOLVERSION",
        "PGSSLMAXPROTOCOLVERSION",
        "PGTARGETSESSIONATTRS",
        "PGKRBSRVNAME",
        "PGGSSLIB",
    }
)
GROUPS = (
    ("vp:tasks:ffmpeg_go", "ffmpeg_go-workers", "vp-ffmpeg-worker-go-swarm"),
    ("vp:tasks:ffmpeg", "ffmpeg-workers", "vp-ffmpeg-worker-gpu-swarm"),
    (
        "vp:tasks:youtube_publisher",
        "youtube_publisher-workers",
        "vp-youtube-publisher-swarm",
    ),
    ("vp:tasks:vision", "vision-workers", None),
    ("vp:events", "orchestrator", None),
)
_REGISTRATION_FIELDS = frozenset(
    {
        "service_name",
        "worker_type",
        "worker_host",
        "capabilities_json",
        "image_identity",
        "database_principal",
        "worker_slot",
        "redis_consumer_id",
        "lease_epoch",
        "database_fingerprint",
        "redis_fingerprint",
        "storage_fingerprint",
        "status",
        "revoke_reason",
    }
)
_GRANT_FIELDS = frozenset(
    {
        "service_name",
        "worker_type",
        "worker_host",
        "capabilities_json",
        "image_identity",
        "database_principal",
        "generation",
        "release_commit",
        "redis_stream",
        "redis_group",
        "endpoint_bindings_json",
        "state",
        "revoke_reason",
    }
)
_ROW_FIELDS = frozenset(
    {
        "observed_at",
        "registration_id",
        "grant_id",
        "worker_instance_id",
        "superseded_by",
        "registered_at",
        "lease_expires_at",
        "registration_revoked_at",
        "grant_activated_at",
        "grant_revoked_at",
        "registration_facts",
        "grant_facts",
    }
)


class ReconcileRuntimeError(RuntimeError):
    """Static error only; callers must not print chained driver exceptions."""


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise ReconcileRuntimeError(code)


@dataclass(frozen=True)
class Invocation:
    pins: PinDocument
    attempt_id: UUID
    replay_only: bool
    control_generation: str
    redis_generation: str
    redis_username: str
    database_secret_id: str
    redis_secret_id: str
    database_secret_sha256: str
    redis_secret_sha256: str

    def __post_init__(self) -> None:
        _require(
            type(self.pins) is PinDocument
            and type(self.attempt_id) is UUID
            and self.attempt_id.int != 0,
            "invocation_invalid",
        )
        _require(type(self.replay_only) is bool, "invocation_invalid")
        for generation in (self.control_generation, self.redis_generation):
            _require(
                type(generation) is str
                and re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", generation) is not None,
                "generation_invalid",
            )
        _require(
            type(self.redis_username) is str
            and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", self.redis_username)
            is not None
            and self.redis_username != "default",
            "redis_principal_invalid",
        )
        for value in (self.database_secret_id, self.redis_secret_id):
            _require(
                type(value) is str and re.fullmatch(r"[a-z0-9]{25}", value) is not None,
                "secret_identity_invalid",
            )
        for value in (self.database_secret_sha256, self.redis_secret_sha256):
            _require(
                type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value) is not None,
                "secret_digest_invalid",
            )

    @property
    def database_secret_name(self) -> str:
        return f"vp-wc-operator-{self.control_generation}"

    @property
    def redis_secret_name(self) -> str:
        return f"vp-control-redis-{self.redis_generation}"


class AttemptAuthority(Protocol):
    """Required Unit 3 adapter; no default, in-memory or replay implementation.

    revalidate proves exact owned FORWARD_APPLYING transaction/revision, mounted
    secret descriptors and pin/plan binding with locks retained. before_eval must
    durably reserve this exact attempt+stream before returning, refusing consumed
    or ambiguous attempts. after_eval persists the exact result or unknown state.
    All callbacks must be cancellation-cooperative and retain caller ownership.
    """

    async def revalidate(self, request: Invocation) -> None: ...
    async def before_eval(self, request: Invocation, command: EvalCommand) -> None: ...
    async def after_eval(
        self, request: Invocation, command: EvalCommand, outcome: str
    ) -> None: ...


@dataclass(frozen=True)
class Credentials:
    database_url: str = field(repr=False)
    redis_url: str = field(repr=False)
    database_name: str
    database_principal: str


def _read_mount(name: str) -> str:
    path = MOUNTS[name]
    before = path.lstat()
    _require(
        (before.st_uid, before.st_gid) == (MOUNT_UID, MOUNT_GID), "secret_owner_invalid"
    )
    value = read_secure_file(path, required_mode=0o400)
    after = path.lstat()
    stable = (
        "st_dev",
        "st_ino",
        "st_uid",
        "st_gid",
        "st_mode",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    _require(
        all(getattr(before, name) == getattr(after, name) for name in stable),
        "secret_changed",
    )
    return value


def _line(value: str) -> str:
    value = value.removesuffix("\n")
    _require(
        bool(value)
        and value == value.strip()
        and not any(char in value for char in "\r\n\x00"),
        "credential_invalid",
    )
    return value


def _fingerprint(value: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()


def load_credentials(request: Invocation) -> Credentials:
    try:
        _require(
            not any(os.environ.get(name) for name in FORBIDDEN_ENV),
            "credential_environment_forbidden",
        )
        database_raw, redis_raw, pin_raw = (
            _read_mount(name) for name in ("database", "redis", "pins")
        )
        _require(
            hashlib.sha256(database_raw.encode()).hexdigest()
            == request.database_secret_sha256,
            "credential_digest_changed",
        )
        _require(
            hashlib.sha256(redis_raw.encode()).hexdigest()
            == request.redis_secret_sha256,
            "credential_digest_changed",
        )
        _require(decode_pins(pin_raw) == request.pins, "mounted_pins_changed")
        database_url, redis_url = _line(database_raw), _line(redis_raw)
        database, redis = make_url(database_url), urlsplit(redis_url)
        principal = role_names_for_generation(request.control_generation).versioned[
            "operator"
        ]
        _require(
            database.drivername in {"postgresql", "postgresql+asyncpg"}
            and database.username == principal
            and bool(database.password)
            and not database.query
            and database.port is not None,
            "database_credential_invalid",
        )
        _require(
            redis.scheme in {"redis", "rediss"}
            and unquote(redis.username or "") == request.redis_username
            and bool(redis.password)
            and redis.port is not None
            and not redis.query
            and not redis.fragment
            and re.fullmatch(r"/(0|[1-9][0-9]*)", redis.path) is not None,
            "redis_credential_invalid",
        )
        database_binding = _validated_database_binding(
            {
                "driver": "postgresql",
                "host": database.host,
                "port": database.port,
                "database": database.database,
            }
        )
        redis_binding = _validated_redis_binding(
            {
                "scheme": redis.scheme,
                "host": redis.hostname,
                "port": redis.port,
                "database": int(redis.path[1:]),
            }
        )
        for worker in request.pins.workers:
            for pin in (worker.current, worker.predecessor):
                if pin is not None:
                    _require(
                        pin.database_fingerprint == _fingerprint(database_binding)
                        and pin.redis_fingerprint == _fingerprint(redis_binding),
                        "credential_endpoint_changed",
                    )
        return Credentials(database_url, redis_url, str(database.database), principal)
    except ReconcileRuntimeError:
        raise
    except Exception:
        raise ReconcileRuntimeError("credential_invalid") from None


async def connect_database(credentials: Credentials) -> Any:
    return await asyncpg.connect(
        asyncpg_url(credentials.database_url),
        timeout=IO_SECONDS,
        command_timeout=IO_SECONDS,
        server_settings={
            "application_name": "vp-registered-consumer-reconcile",
            "statement_timeout": "2000",
            "lock_timeout": "2000",
            "idle_in_transaction_session_timeout": "8000",
        },
    )


def create_redis(credentials: Credentials) -> Redis:
    return Redis.from_url(
        credentials.redis_url,
        decode_responses=True,
        single_connection_client=True,
        socket_timeout=IO_SECONDS,
        socket_connect_timeout=IO_SECONDS,
        retry=Retry(NoBackoff(), 0),
        retry_on_timeout=False,
        retry_on_error=[],
        health_check_interval=0,
        max_connections=1,
        protocol=2,
        lib_name=None,
        lib_version=None,
    )


@dataclass(frozen=True)
class GuardFacts:
    now: datetime
    registrations: Sequence[WorkerRegistration]
    grants: Sequence[WorkerAdmissionGrant]


def _json_object(value: object, expected: frozenset[str]) -> dict:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict) or set(value) != expected:
        raise ReconcileRuntimeError("guard_facts_invalid")
    return value


def decode_guard(rows: object, pins: PinDocument) -> GuardFacts:
    try:
        if (
            not isinstance(rows, Sequence)
            or isinstance(rows, (str, bytes))
            or not 4 <= len(rows) <= 8
        ):
            raise ReconcileRuntimeError("guard_facts_invalid")
        registrations, grants = [], []
        now = None
        for row in rows:
            if isinstance(row, asyncpg.Record):
                row = dict(row)
            _require(
                isinstance(row, Mapping) and set(row) == _ROW_FIELDS,
                "guard_facts_invalid",
            )
            for name in ("observed_at", "registered_at", "lease_expires_at"):
                _require(
                    isinstance(row[name], datetime)
                    and row[name].utcoffset() is not None,
                    "guard_facts_invalid",
                )
            for name in ("registration_id", "grant_id", "worker_instance_id"):
                _require(isinstance(row[name], UUID), "guard_facts_invalid")
            _require(
                row["superseded_by"] is None or isinstance(row["superseded_by"], UUID),
                "guard_facts_invalid",
            )
            if now is None:
                now = row["observed_at"]
            _require(now == row["observed_at"], "guard_clock_changed")
            registrations.append(
                WorkerRegistration(
                    id=row["registration_id"],
                    grant_id=row["grant_id"],
                    worker_instance_id=row["worker_instance_id"],
                    superseded_by=row["superseded_by"],
                    registered_at=row["registered_at"],
                    lease_expires_at=row["lease_expires_at"],
                    revoked_at=row["registration_revoked_at"],
                    **_json_object(row["registration_facts"], _REGISTRATION_FIELDS),
                )
            )
            grants.append(
                WorkerAdmissionGrant(
                    id=row["grant_id"],
                    activated_at=row["grant_activated_at"],
                    revoked_at=row["grant_revoked_at"],
                    **_json_object(row["grant_facts"], _GRANT_FIELDS),
                )
            )
        if not isinstance(now, datetime):
            raise ReconcileRuntimeError("guard_clock_invalid")
        validate_database(pins, registrations, grants, now=now)
        return GuardFacts(now, registrations, grants)
    except ReconcileRuntimeError:
        raise
    except Exception:
        raise ReconcileRuntimeError("guard_facts_invalid") from None


async def read_guard(connection: Any, request: Invocation) -> GuardFacts:
    rows = await connection.fetch(
        "SELECT * FROM public.vp_registered_consumer_reconcile_guard($1::text,$2::uuid[],$3::uuid[])",
        request.control_generation,
        [worker.current.registration_id for worker in request.pins.workers],
        [
            worker.predecessor.registration_id
            for worker in request.pins.workers
            if worker.predecessor is not None
        ],
    )
    return decode_guard(rows, request.pins)


@dataclass(frozen=True)
class Observation:
    inventories: Mapping[str, object]
    passive_members: tuple[tuple[str, ...], ...]


T = TypeVar("T")


async def _io(operation: Awaitable[T]) -> T:
    async with asyncio.timeout(IO_SECONDS):
        return await operation


async def observe_redis(client: Any, pins: PinDocument) -> Observation:
    inventories, passive = {}, []
    for stream, group, service in GROUPS:
        pending = await _io(client.xpending(stream, group))
        _require(
            isinstance(pending, Mapping)
            and type(pending.get("pending")) is int
            and pending["pending"] == 0,
            "redis_pending",
        )
        groups = await _io(client.xinfo_groups(stream))
        _require(
            isinstance(groups, list)
            and all(isinstance(item, Mapping) for item in groups),
            "redis_group_invalid",
        )
        matches = [item for item in groups if item.get("name") == group]
        _require(len(matches) == 1, "redis_group_invalid")
        consumers = await _io(client.xinfo_consumers(stream, group))
        if service is not None:
            inventories[service] = {
                "stream": stream,
                "group": group,
                "pending": pending["pending"],
                "lag": matches[0].get("lag"),
                "consumers": consumers,
            }
        else:
            _require(
                isinstance(consumers, list) and 1 <= len(consumers) <= 256,
                "passive_inventory_invalid",
            )
            names = []
            for consumer in consumers:
                _require(
                    isinstance(consumer, Mapping)
                    and type(consumer.get("name")) is str
                    and 0 < len(consumer["name"]) <= 255
                    and type(consumer.get("pending")) is int
                    and consumer["pending"] == 0
                    and type(consumer.get("idle")) is int
                    and consumer["idle"] >= 0,
                    "passive_inventory_invalid",
                )
                names.append(consumer["name"])
            _require(len(names) == len(set(names)), "passive_inventory_invalid")
            if stream == "vp:tasks:vision":
                vision = next(
                    worker.current
                    for worker in pins.workers
                    if worker.current.worker_type == "vision"
                )
                _require(
                    names == [vision.redis_consumer_id]
                    and consumers[0]["idle"] <= 120000,
                    "vision_inventory_changed",
                )
            passive.append(tuple(sorted(names)))
    return Observation(inventories, tuple(passive))


async def wait_for_aging(delay: float) -> None:
    await asyncio.sleep(delay)


async def _owned_cleanup(operation: Awaitable[None], seconds: float) -> None:
    """Shield only bounded cleanup, never the forward controller or later EVALs."""

    async def bounded() -> None:
        async with asyncio.timeout(seconds):
            await operation

    child = asyncio.create_task(bounded())
    cancelled = False
    while not child.done():
        try:
            await asyncio.shield(child)
        except asyncio.CancelledError:
            cancelled = True
    child.result()
    if cancelled:
        raise asyncio.CancelledError


async def _rollback(connection: Any, transaction: Any) -> None:
    try:
        await _owned_cleanup(transaction.rollback(), ROLLBACK_SECONDS)
    except asyncio.CancelledError:
        raise
    except BaseException:
        connection.terminate()
        raise ReconcileRuntimeError("rollback_failed") from None


async def _close(connection: Any, client: Any) -> None:
    failed = False
    try:
        if client is not None:
            transport = getattr(client, "connection", None)
            try:
                async with asyncio.timeout(CLOSE_SECONDS):
                    if transport is not None:
                        await transport.disconnect(nowait=True)
                    await client.aclose(close_connection_pool=True)
            except BaseException:
                if transport is not None:
                    # redis-py's synchronous destructor primitive: no late socket
                    # waiter or reconnect if bounded graceful close fails.
                    transport._close()
                failed = True
    finally:
        if connection is not None:
            try:
                async with asyncio.timeout(CLOSE_SECONDS):
                    await connection.close(timeout=CLOSE_SECONDS)
            except BaseException:
                connection.terminate()
                failed = True
    if failed:
        raise ReconcileRuntimeError("resource_cleanup_failed")


@dataclass(frozen=True)
class RunResult:
    outcome: Literal["reconciled", "already_absent"]
    pin_sha256: str
    attempted_streams: tuple[str, ...]


async def reconcile_registered_consumers(
    request: Invocation, authority: AttemptAuthority
) -> RunResult:
    """Fully awaited 200s maximum, including reserved rollback/close budgets.

    The in-memory attempted set is only a local duplicate defense. The required
    authority adapter provides the durable cross-process fence. No retries or
    disconnected background controller are provided here.
    """
    deadline = asyncio.get_running_loop().time() + TOTAL_SECONDS
    work_deadline = deadline - ROLLBACK_SECONDS - IO_SECONDS - 2 * CLOSE_SECONDS
    connection = client = None
    attempted: set[str] = set()
    order: list[str] = []
    passive = None
    uncertain: EvalCommand | None = None
    try:
        async with asyncio.timeout_at(work_deadline):
            await _io(authority.revalidate(request))
            credentials = load_credentials(request)
            connection = await _io(connect_database(credentials))
            identity = await _io(
                connection.fetchrow(
                    "SELECT session_user, current_user, pg_catalog.current_database() AS database_name"
                )
            )
            if isinstance(identity, asyncpg.Record):
                identity = dict(identity)
            _require(
                isinstance(identity, Mapping)
                and identity.get("session_user") == credentials.database_principal
                and identity.get("current_user") == credentials.database_principal
                and identity.get("database_name") == credentials.database_name,
                "database_principal_changed",
            )
            client = create_redis(credentials)
            _require(
                await _io(client.acl_whoami()) == request.redis_username,
                "redis_principal_changed",
            )
            server = await _io(client.info("server"))
            _require(
                isinstance(server, Mapping)
                and type(server.get("redis_version")) is str,
                "redis_version_invalid",
            )
            version = re.fullmatch(
                r"([0-9]+)\.([0-9]+)\.[0-9]+", server["redis_version"]
            )
            _require(
                version is not None and (int(version[1]), int(version[2])) >= (7, 2),
                "redis_version_invalid",
            )
            while True:
                await _io(authority.revalidate(request))
                transaction = connection.transaction()
                try:
                    # Reserve rollback inside the eight-second row-lock bound.
                    async with asyncio.timeout(LOCKED_SECONDS - ROLLBACK_SECONDS):
                        await _io(transaction.start())
                        state = await _io(read_guard(connection, request))
                        observation = await observe_redis(client, request.pins)
                        if passive is None:
                            passive = observation.passive_members
                        _require(
                            observation.passive_members == passive,
                            "passive_members_changed",
                        )
                        decision = assess(
                            request.pins,
                            state.registrations,
                            state.grants,
                            observation.inventories,
                            now=state.now,
                            replay_only=request.replay_only,
                        )
                        if decision.outcome != "wait":
                            must_wait = False
                            for command in decision.commands:
                                _require(
                                    not request.replay_only
                                    and command.stream not in attempted,
                                    "attempt_already_consumed",
                                )
                                await _io(authority.revalidate(request))
                                before = await observe_redis(client, request.pins)
                                _require(
                                    before.passive_members == passive,
                                    "passive_members_changed",
                                )
                                state = await _io(read_guard(connection, request))
                                latest = assess(
                                    request.pins,
                                    state.registrations,
                                    state.grants,
                                    before.inventories,
                                    now=state.now,
                                )
                                if latest.outcome == "wait":
                                    must_wait = True
                                    break
                                if not any(
                                    item.stream == command.stream
                                    for item in latest.commands
                                ):
                                    continue
                                await _io(authority.before_eval(request, command))
                                attempted.add(command.stream)
                                order.append(command.stream)
                                uncertain = command
                                state = await _io(read_guard(connection, request))
                                _require(
                                    not validate_database(
                                        request.pins,
                                        state.registrations,
                                        state.grants,
                                        now=state.now,
                                    ),
                                    "authority_changed_after_intent",
                                )
                                result = await _io(
                                    client.execute_command(*command.arguments)
                                )
                                outcome = validate_lua_result(command, result)
                                await _io(
                                    authority.after_eval(request, command, outcome)
                                )
                                uncertain = None
                            if not must_wait:
                                await _io(authority.revalidate(request))
                                final = await observe_redis(client, request.pins)
                                _require(
                                    final.passive_members == passive,
                                    "passive_members_changed",
                                )
                                final_state = await _io(read_guard(connection, request))
                                final_decision = assess(
                                    request.pins,
                                    final_state.registrations,
                                    final_state.grants,
                                    final.inventories,
                                    now=final_state.now,
                                    replay_only=True,
                                )
                                _require(
                                    final_decision.outcome == "already_absent",
                                    "final_not_settled",
                                )
                                return RunResult(
                                    "reconciled" if attempted else "already_absent",
                                    request.pins.sha256,
                                    tuple(order),
                                )
                finally:
                    await _rollback(connection, transaction)
                await wait_for_aging(POLL_SECONDS)
    except asyncio.CancelledError:
        raise
    except ReconcileRuntimeError:
        raise
    except Exception:
        raise ReconcileRuntimeError("registered_reconcile_failed") from None
    finally:

        async def finalize() -> None:
            try:
                if uncertain is not None:
                    await _io(authority.after_eval(request, uncertain, "unknown"))
            finally:
                await _close(connection, client)

        try:
            await _owned_cleanup(finalize(), IO_SECONDS + 2 * CLOSE_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise ReconcileRuntimeError("resource_cleanup_failed") from None
