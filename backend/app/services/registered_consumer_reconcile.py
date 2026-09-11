"""Inert Unit 1 contracts, not an executable deployment gate.

The later transaction integration must construct pins from captured baseline and
verified replacement facts, journal their digest and each attempt, and supply
fresh locked DB facts and Redis observations. This module neither acquires that
authority nor performs I/O. An EVAL response lost in transport is never retried;
the caller may only request a read-only assessment of the same pins.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from typing import Literal, TypeGuard
from uuid import UUID

from app.models.worker_registration import WorkerAdmissionGrant, WorkerRegistration
from app.services.worker_registration import (
    WorkerRegistrationError,
    _normalized_endpoint_bindings,
)


# The admission operator and deploy-sync service contracts, not consumer patterns.
_CONTRACTS = MappingProxyType(
    {
        "vp-ffmpeg-worker-go-swarm": ("ffmpeg_go", "colima-127", "media_cpu"),
        "vp-ffmpeg-worker-gpu-swarm": ("ffmpeg", "150-gpu", "media_gpu"),
        "vp-vision-worker-swarm": ("vision", "150-vision", "vision_gpu"),
        "vp-youtube-publisher-swarm": (
            "youtube_publisher",
            "150-publisher",
            "youtube_publisher",
        ),
    }
)
_RETIRING_SERVICES = tuple(
    name for name in _CONTRACTS if name != "vp-vision-worker-swarm"
)
_MAX_BIGINT = 9_223_372_036_854_775_807
_SHA256 = r"[0-9a-f]{64}"
_RELEASE = r"[0-9a-f]{40}"
_IMAGE = r"[A-Za-z0-9][A-Za-z0-9._/-]*:deploy-[0-9a-f]{12}"


class ReconcileRefused(ValueError):
    """Static, nonsecret refusal. No refusal is permission to mutate or retry."""


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise ReconcileRefused(code)


def _integer(value: object, *, minimum: int = 0) -> TypeGuard[int]:
    return type(value) is int and minimum <= value <= _MAX_BIGINT


def _matches(value: object, pattern: str) -> bool:
    return type(value) is str and re.fullmatch(pattern, value, re.ASCII) is not None


def _aware(value: object) -> TypeGuard[datetime]:
    return (
        isinstance(value, datetime)
        and value.tzinfo is not None
        and value.utcoffset() is not None
    )


@dataclass(frozen=True)
class IdentityPin:
    registration_id: UUID
    grant_id: UUID
    generation: int
    service_name: str
    worker_type: str
    worker_host: str
    capabilities: tuple[str, ...]
    release_commit: str
    image_identity: str
    database_principal: str
    worker_instance_id: UUID
    worker_slot: int
    redis_consumer_id: str
    lease_epoch: int
    registered_at: datetime
    database_fingerprint: str
    redis_fingerprint: str
    storage_fingerprint: str

    def __post_init__(self) -> None:
        _require(
            all(
                type(value) is UUID and value.int != 0
                for value in (
                    self.registration_id,
                    self.grant_id,
                    self.worker_instance_id,
                )
            ),
            "pin_uuid_invalid",
        )
        _require(
            _integer(self.generation, minimum=1)
            and _integer(self.lease_epoch, minimum=1)
            and type(self.worker_slot) is int
            and self.worker_slot == 1,
            "pin_integer_invalid",
        )
        _require(
            type(self.service_name) is str and self.service_name in _CONTRACTS,
            "pin_service_invalid",
        )
        worker_type, host, capability = _CONTRACTS[self.service_name]
        _require(
            self.worker_type == worker_type
            and self.worker_host == host
            and type(self.capabilities) is tuple
            and self.capabilities == (capability,),
            "pin_topology_invalid",
        )
        _require(
            self.redis_consumer_id
            == f"{worker_type}-worker@{host}:1:{self.worker_instance_id}",
            "pin_consumer_invalid",
        )
        _require(
            _matches(self.release_commit, _RELEASE)
            and _matches(self.image_identity, _IMAGE)
            and self.image_identity.endswith(f":deploy-{self.release_commit[:12]}")
            and len(self.image_identity) <= 255,
            "pin_release_invalid",
        )
        _require(
            _matches(self.database_principal, r"[A-Za-z_][A-Za-z0-9_]{0,62}"),
            "pin_principal_invalid",
        )
        _require(_aware(self.registered_at), "pin_timestamp_invalid")
        _require(
            all(
                _matches(value, _SHA256)
                for value in (
                    self.database_fingerprint,
                    self.redis_fingerprint,
                    self.storage_fingerprint,
                )
            ),
            "pin_fingerprint_invalid",
        )


@dataclass(frozen=True)
class WorkerPin:
    current: IdentityPin
    # None is a captured absence, never an omitted field or a wildcard.
    predecessor: IdentityPin | None

    def __post_init__(self) -> None:
        _require(type(self.current) is IdentityPin, "pin_current_invalid")
        old = self.predecessor
        if old is None:
            return
        _require(type(old) is IdentityPin, "pin_predecessor_invalid")
        _require(
            old.service_name == self.current.service_name
            and old.lease_epoch < self.current.lease_epoch
            and old.generation < self.current.generation
            and old.registration_id != self.current.registration_id
            and old.grant_id != self.current.grant_id
            and old.worker_instance_id != self.current.worker_instance_id,
            "pin_successor_invalid",
        )


def _json_scalar(value: object) -> str:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    raise ReconcileRefused("pin_serialization_invalid")


@dataclass(frozen=True)
class PinDocument:
    version: int
    transaction_id: str
    revision: int
    release_commit: str
    workers: tuple[WorkerPin, ...]

    def __post_init__(self) -> None:
        _require(type(self.version) is int and self.version == 1, "pin_version_invalid")
        _require(
            _matches(self.transaction_id, r"tx-[0-9a-f]{32}"), "pin_transaction_invalid"
        )
        _require(_integer(self.revision), "pin_revision_invalid")
        _require(_matches(self.release_commit, _RELEASE), "pin_release_invalid")
        _require(
            type(self.workers) is tuple
            and all(type(item) is WorkerPin for item in self.workers),
            "pin_workers_invalid",
        )
        _require(
            tuple(item.current.service_name for item in self.workers)
            == tuple(_CONTRACTS),
            "pin_workers_invalid",
        )
        _require(
            all(
                item.current.release_commit == self.release_commit
                for item in self.workers
            ),
            "pin_deployment_mismatch",
        )
        identities = [
            pin
            for worker in self.workers
            for pin in (worker.current, worker.predecessor)
            if pin is not None
        ]
        for field in (
            "registration_id",
            "grant_id",
            "worker_instance_id",
            "redis_consumer_id",
        ):
            _require(
                len({getattr(pin, field) for pin in identities}) == len(identities),
                "pin_duplicate_identity",
            )

    @property
    def canonical_json(self) -> str:
        return json.dumps(
            asdict(self), sort_keys=True, separators=(",", ":"), default=_json_scalar
        )

    @property
    def sha256(self) -> str:
        """Hash of canonical nonsecret pins, not a claim of transport authority."""
        return hashlib.sha256(self.canonical_json.encode("ascii")).hexdigest()


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        _require(key not in result, "pin_duplicate_field")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ReconcileRefused("pin_json_invalid")


def _exact_fields(value: object, expected: set[str]) -> dict:
    if type(value) is not dict or set(value) != expected:
        raise ReconcileRefused("pin_schema_invalid")
    return value


def _decode_identity(value: object) -> IdentityPin:
    payload = dict(_exact_fields(value, {field.name for field in fields(IdentityPin)}))
    for name in ("registration_id", "grant_id", "worker_instance_id"):
        raw = payload[name]
        _require(type(raw) is str, "pin_uuid_invalid")
        parsed = UUID(raw)
        _require(str(parsed) == raw, "pin_uuid_invalid")
        payload[name] = parsed
    raw_date = payload["registered_at"]
    _require(type(raw_date) is str, "pin_timestamp_invalid")
    payload["registered_at"] = datetime.fromisoformat(raw_date)
    _require(type(payload["capabilities"]) is list, "pin_capabilities_invalid")
    payload["capabilities"] = tuple(payload["capabilities"])
    return IdentityPin(**payload)


def decode_pins(raw: str) -> PinDocument:
    """Strict decoding only; the later owned transaction must bind the digest."""
    try:
        _require(type(raw) is str, "pin_json_invalid")
        payload = _exact_fields(
            json.loads(
                raw, object_pairs_hook=_unique_object, parse_constant=_reject_constant
            ),
            {field.name for field in fields(PinDocument)},
        )
        _require(type(payload["workers"]) is list, "pin_workers_invalid")
        workers = []
        for value in payload["workers"]:
            worker = _exact_fields(value, {"current", "predecessor"})
            workers.append(
                WorkerPin(
                    current=_decode_identity(worker["current"]),
                    predecessor=None
                    if worker["predecessor"] is None
                    else _decode_identity(worker["predecessor"]),
                )
            )
        return PinDocument(**{**payload, "workers": tuple(workers)})
    except ReconcileRefused:
        raise
    except (ValueError, TypeError, KeyError, OverflowError, RecursionError):
        raise ReconcileRefused("pin_document_invalid") from None


def validate_deployment(
    pins: PinDocument,
    *,
    transaction_id: str,
    revision: int,
    release_commit: str,
    targets: Mapping[str, tuple[int, str]],
    pin_sha256: str,
) -> None:
    """Compare with independently loaded owned journal/forward-plan bindings."""
    _require(
        type(pins) is PinDocument
        and _integer(revision)
        and transaction_id == pins.transaction_id
        and revision == pins.revision
        and release_commit == pins.release_commit
        and pin_sha256 == pins.sha256,
        "deployment_binding_mismatch",
    )
    _require(
        isinstance(targets, Mapping) and set(targets) == set(_CONTRACTS),
        "deployment_targets_mismatch",
    )
    for worker in pins.workers:
        target = targets[worker.current.service_name]
        _require(
            type(target) is tuple
            and len(target) == 2
            and _integer(target[0], minimum=1)
            and target == (worker.current.generation, worker.current.image_identity),
            "deployment_targets_mismatch",
        )


def _same(actual: object, expected: object) -> bool:
    # bool == 1 must not make malformed facts into a valid epoch/generation.
    return type(actual) is type(expected) and actual == expected


def _identity_matches(
    pin: IdentityPin, row: WorkerRegistration, grant: WorkerAdmissionGrant
) -> None:
    for name in (
        "service_name",
        "worker_type",
        "worker_host",
        "image_identity",
        "database_principal",
        "worker_instance_id",
        "worker_slot",
        "redis_consumer_id",
        "lease_epoch",
        "registered_at",
        "database_fingerprint",
        "redis_fingerprint",
        "storage_fingerprint",
    ):
        _require(
            _same(getattr(row, name), getattr(pin, name)),
            "registration_identity_changed",
        )
    _require(_same(row.grant_id, pin.grant_id), "registration_grant_changed")
    for name in (
        "service_name",
        "worker_type",
        "worker_host",
        "image_identity",
        "database_principal",
        "generation",
        "release_commit",
    ):
        _require(
            _same(getattr(grant, name), getattr(pin, name)), "grant_identity_changed"
        )
    for value in (row.capabilities_json, grant.capabilities_json):
        _require(
            type(value) is list and value == list(pin.capabilities),
            "capabilities_changed",
        )
    _require(
        grant.redis_stream == f"vp:tasks:{pin.worker_type}"
        and grant.redis_group == f"{pin.worker_type}-workers",
        "grant_stream_changed",
    )
    try:
        _, _, fingerprints = _normalized_endpoint_bindings(grant.endpoint_bindings_json)
    except (WorkerRegistrationError, ValueError, TypeError):
        raise ReconcileRefused("grant_endpoints_invalid") from None
    _require(
        all(
            fingerprints[name] == getattr(pin, f"{name}_fingerprint")
            for name in ("database", "redis", "storage")
        ),
        "grant_endpoints_changed",
    )


def _revoked(value: WorkerRegistration | WorkerAdmissionGrant, now: datetime) -> bool:
    return (
        _aware(value.revoked_at)
        and value.revoked_at <= now
        and type(value.revoke_reason) is str
        and bool(value.revoke_reason.strip())
    )


def validate_database(
    pins: PinDocument,
    registrations: Sequence[WorkerRegistration],
    grants: Sequence[WorkerAdmissionGrant],
    *,
    now: datetime,
) -> tuple[str, ...]:
    """Pure checks over complete selected-service facts. Return only old lease waits.

    Unit 2 must supply a fresh DB clock and lock all pinned rows, and prove global
    CLOSED/no-work/claim safety and all five groups' PEL before executing commands.
    """
    _require(type(pins) is PinDocument and _aware(now), "database_facts_invalid")
    _require(
        isinstance(registrations, Sequence) and isinstance(grants, Sequence),
        "database_facts_invalid",
    )
    _require(
        all(isinstance(row, WorkerRegistration) for row in registrations),
        "database_facts_invalid",
    )
    _require(
        all(isinstance(row, WorkerAdmissionGrant) for row in grants),
        "database_facts_invalid",
    )
    all_rows: tuple[WorkerRegistration | WorkerAdmissionGrant, ...] = (
        *registrations,
        *grants,
    )
    _require(
        all(
            type(row.id) is UUID and row.service_name in _CONTRACTS for row in all_rows
        ),
        "database_scope_invalid",
    )
    by_registration = {row.id: row for row in registrations}
    by_grant = {row.id: row for row in grants}
    _require(
        len(by_registration) == len(registrations) and len(by_grant) == len(grants),
        "database_duplicate_facts",
    )
    current_ids = {worker.current.registration_id for worker in pins.workers}
    current_grants = {worker.current.grant_id for worker in pins.workers}
    _require(
        {row.id for row in registrations if row.status == "active"} == current_ids,
        "active_registrations_changed",
    )
    _require(
        {row.id for row in grants if row.state == "active"} == current_grants,
        "active_grants_changed",
    )
    waiting = []
    for worker in pins.workers:
        for pin in (worker.current, worker.predecessor):
            if pin is None:
                continue
            row, grant = (
                by_registration.get(pin.registration_id),
                by_grant.get(pin.grant_id),
            )
            if row is None or grant is None:
                raise ReconcileRefused("pinned_fact_missing")
            _identity_matches(pin, row, grant)
            _require(_aware(row.lease_expires_at), "lease_timestamp_invalid")
            if pin is worker.current:
                _require(
                    row.status == "active"
                    and row.revoked_at is None
                    and row.revoke_reason is None
                    and row.superseded_by is None
                    and grant.state == "active"
                    and grant.revoked_at is None
                    and grant.revoke_reason is None
                    and _aware(grant.activated_at)
                    and grant.activated_at <= now
                    and row.lease_expires_at > now + timedelta(seconds=60),
                    "current_not_ready",
                )
            else:
                _require(
                    row.status == "revoked"
                    and grant.state == "revoked"
                    and _revoked(row, now)
                    and _revoked(grant, now)
                    and row.superseded_by == worker.current.registration_id,
                    "predecessor_not_superseded",
                )
                if row.lease_expires_at > now:
                    waiting.append(pin.service_name)
    return tuple(waiting)


def _inventory_outcome(
    worker: WorkerPin, value: object
) -> Literal["ready", "wait", "already_absent"]:
    if not isinstance(value, Mapping) or set(value) != {
        "stream",
        "group",
        "pending",
        "lag",
        "consumers",
    }:
        raise ReconcileRefused("inventory_schema_invalid")
    current = worker.current
    _require(
        value["stream"] == f"vp:tasks:{current.worker_type}"
        and value["group"] == f"{current.worker_type}-workers",
        "inventory_scope_invalid",
    )
    for name in ("pending", "lag"):
        _require(_integer(value[name]) and value[name] == 0, "inventory_backlog")
    consumers = value["consumers"]
    _require(
        isinstance(consumers, Sequence) and not isinstance(consumers, (str, bytes)),
        "inventory_consumers_invalid",
    )
    by_name = {}
    old_name = worker.predecessor.redis_consumer_id if worker.predecessor else None
    for consumer in consumers:
        _require(isinstance(consumer, Mapping), "inventory_consumer_invalid")
        _require(
            {"name", "pending", "idle"}
            <= set(consumer)
            <= {"name", "pending", "idle", "inactive"},
            "inventory_consumer_invalid",
        )
        name = consumer["name"]
        _require(
            type(name) is str
            and name not in by_name
            and name in (current.redis_consumer_id, old_name),
            "inventory_identity_changed",
        )
        _require(
            _integer(consumer["pending"]) and consumer["pending"] == 0,
            "inventory_pending",
        )
        _require(_integer(consumer["idle"]), "inventory_idle_invalid")
        if "inactive" in consumer:
            _require(
                _integer(consumer["inactive"], minimum=-1), "inventory_inactive_invalid"
            )
        by_name[name] = consumer
    _require(current.redis_consumer_id in by_name, "inventory_current_missing")
    _require(
        by_name[current.redis_consumer_id]["idle"] <= 120000,
        "inventory_current_inactive",
    )
    if old_name not in by_name:
        return "already_absent"
    return "ready" if by_name[old_name]["idle"] > 120000 else "wait"


# Fixed keys/groups, no caller-provided group, idle threshold or name regex.
# EVAL is atomic but not rollback-capable: a lost/error response after DELCONSUMER
# is uncertain and must only be reconciled read-only by the owning transaction.
ATOMIC_RECONCILE_LUA = r"""
if #KEYS ~= 1 or #ARGV ~= 2 then
    return redis.error_reply("registered_reconcile_arguments_invalid")
end
local groups = {
    ["vp:tasks:ffmpeg"] = "ffmpeg-workers",
    ["vp:tasks:ffmpeg_go"] = "ffmpeg_go-workers",
    ["vp:tasks:youtube_publisher"] = "youtube_publisher-workers"
}
local group = groups[KEYS[1]]
if not group or ARGV[1] == "" or ARGV[2] == "" or ARGV[1] == ARGV[2] then
    return redis.error_reply("registered_reconcile_scope_invalid")
end
local function integer(value)
    return type(value) == "number" and value >= 0 and value % 1 == 0
end
local function record(values)
    if type(values) ~= "table" or #values % 2 ~= 0 then return nil end
    local result = {}
    for index = 1, #values, 2 do
        if type(values[index]) ~= "string" or result[values[index]] ~= nil then
            return nil
        end
        result[values[index]] = values[index + 1]
    end
    return result
end
local function backlog_clear()
    local pending = redis.call("XPENDING", KEYS[1], group)
    if type(pending) ~= "table" or not integer(pending[1]) or pending[1] ~= 0 then
        return false
    end
    local matches = 0
    for _, values in ipairs(redis.call("XINFO", "GROUPS", KEYS[1])) do
        local item = record(values)
        if not item then return false end
        if item.name == group then
            matches = matches + 1
            if not integer(item.lag) or item.lag ~= 0 then return false end
        end
    end
    return matches == 1
end
local function inventory()
    local current, old = nil, nil
    for _, values in ipairs(redis.call("XINFO", "CONSUMERS", KEYS[1], group)) do
        local item = record(values)
        if not item or not integer(item.pending) or item.pending ~= 0
            or not integer(item.idle) then return nil, nil, false end
        if item.name == ARGV[1] then
            if current or item.idle > 120000 then return nil, nil, false end
            current = item
        elseif item.name == ARGV[2] then
            if old or item.idle <= 120000 then return nil, nil, false end
            old = item
        else
            return nil, nil, false
        end
    end
    return current, old, current ~= nil
end
if not backlog_clear() then
    return redis.error_reply("registered_reconcile_backlog")
end
local current, old, valid = inventory()
if not valid then
    return redis.error_reply("registered_reconcile_inventory_changed")
end
if not old then
    return {"already_absent", current.name}
end
local deleted = redis.call("XGROUP", "DELCONSUMER", KEYS[1], group, old.name)
if not integer(deleted) or deleted ~= 0 then
    return redis.error_reply("registered_reconcile_delete_uncertain")
end
local final_current, final_old, final_valid = inventory()
if not final_valid or final_old or not backlog_clear() then
    return redis.error_reply("registered_reconcile_final_uncertain")
end
return {"retired", final_current.name, old.name}
"""


@dataclass(frozen=True)
class EvalCommand:
    """Transport data only. Unit 2/3 must fence and journal its single attempt."""

    pins: PinDocument
    service_name: str

    def __post_init__(self) -> None:
        _require(type(self.pins) is PinDocument, "command_pins_invalid")
        _require(
            type(self.service_name) is str and self.service_name in _RETIRING_SERVICES,
            "command_service_invalid",
        )
        _require(self.worker.predecessor is not None, "command_predecessor_absent")

    @property
    def worker(self) -> WorkerPin:
        return next(
            worker
            for worker in self.pins.workers
            if worker.current.service_name == self.service_name
        )

    @property
    def stream(self) -> str:
        return f"vp:tasks:{self.worker.current.worker_type}"

    @property
    def current(self) -> str:
        return self.worker.current.redis_consumer_id

    @property
    def predecessor(self) -> str:
        old = self.worker.predecessor
        if old is None:
            raise ReconcileRefused("command_predecessor_absent")
        return old.redis_consumer_id

    @property
    def arguments(self) -> tuple[str | int, ...]:
        return (
            "EVAL",
            ATOMIC_RECONCILE_LUA,
            1,
            self.stream,
            self.current,
            self.predecessor,
        )


@dataclass(frozen=True)
class Assessment:
    outcome: Literal["ready", "wait", "already_absent"]
    commands: tuple[EvalCommand, ...] = ()


def assess(
    pins: PinDocument,
    registrations: Sequence[WorkerRegistration],
    grants: Sequence[WorkerAdmissionGrant],
    inventories: Mapping[str, object],
    *,
    now: datetime,
    replay_only: bool = False,
) -> Assessment:
    """Assess all facts before returning any commands; never retry an attempt.

    `replay_only` must be true after any uncertain/previous execution. Keeping the
    attempt ledger, locks, all-five-group safety and final observation is deferred
    to Unit 2/3. A command here alone is deliberately not execution authority.
    """
    _require(type(replay_only) is bool, "replay_mode_invalid")
    lease_waits = validate_database(pins, registrations, grants, now=now)
    _require(
        isinstance(inventories, Mapping)
        and set(inventories) == set(_RETIRING_SERVICES),
        "inventory_set_invalid",
    )
    selected = [
        worker
        for worker in pins.workers
        if worker.current.service_name in _RETIRING_SERVICES
    ]
    outcomes = [
        _inventory_outcome(worker, inventories[worker.current.service_name])
        for worker in selected
    ]
    if replay_only:
        _require(
            all(outcome == "already_absent" for outcome in outcomes),
            "replay_incomplete",
        )
    if lease_waits or "wait" in outcomes:
        return Assessment("wait")
    commands = tuple(
        EvalCommand(pins, worker.current.service_name)
        for worker, outcome in zip(selected, outcomes, strict=True)
        if outcome == "ready" and worker.predecessor is not None
    )
    return Assessment("ready", commands) if commands else Assessment("already_absent")


def validate_lua_result(
    command: EvalCommand, result: object
) -> Literal["retired", "already_absent"]:
    if result == ["retired", command.current, command.predecessor]:
        return "retired"
    if result == ["already_absent", command.current]:
        return "already_absent"
    raise ReconcileRefused("lua_result_uncertain")
