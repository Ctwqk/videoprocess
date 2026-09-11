from __future__ import annotations

import copy
import hashlib
import json
from contextlib import asynccontextmanager, nullcontext
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest

from app.models.worker_registration import WorkerAdmissionGrant, WorkerRegistration
from app.services import registered_consumer_reconcile as reconcile


NOW = datetime(2026, 9, 11, 3, tzinfo=timezone.utc)
RELEASE = "b9a68b76f037cae6e176df8cf326b00e4ca1e1e0"
OLD_RELEASE = "a" * 40
TOPOLOGY = (
    ("vp-ffmpeg-worker-go-swarm", "ffmpeg_go", "colima-127", "media_cpu"),
    ("vp-ffmpeg-worker-gpu-swarm", "ffmpeg", "150-gpu", "media_gpu"),
    ("vp-vision-worker-swarm", "vision", "150-vision", "vision_gpu"),
    (
        "vp-youtube-publisher-swarm",
        "youtube_publisher",
        "150-publisher",
        "youtube_publisher",
    ),
)
ENDPOINTS = {
    "database": {
        "driver": "postgresql",
        "host": "10.0.0.150",
        "port": 5435,
        "database": "videoprocess",
    },
    "redis": {
        "scheme": "redis",
        "host": "10.0.0.150",
        "port": 6380,
        "database": 0,
    },
    "storage": {
        "backend": "minio",
        "host": "10.0.0.150",
        "port": 9000,
        "bucket": "videoprocess",
    },
}


def identity(index, *, old=False):
    service, worker_type, host, capability = TOPOLOGY[index]
    number = index * 10 + (1 if old else 4)
    instance = str(UUID(int=number + 2))
    release = OLD_RELEASE if old else RELEASE
    return {
        "registration_id": str(UUID(int=number)),
        "grant_id": str(UUID(int=number + 1)),
        "generation": 8 if old else 9,
        "service_name": service,
        "worker_type": worker_type,
        "worker_host": host,
        "capabilities": [capability],
        "release_commit": release,
        "image_identity": f"vp-{worker_type}:deploy-{release[:12]}",
        "database_principal": f"vp_worker_{number}",
        "worker_instance_id": instance,
        "worker_slot": 1,
        "redis_consumer_id": f"{worker_type}-worker@{host}:1:{instance}",
        "lease_epoch": 20 if old else 21,
        "registered_at": (NOW - timedelta(hours=2 if old else 1)).isoformat(),
        **{
            f"{name}_fingerprint": hashlib.sha256(
                json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            for name, value in ENDPOINTS.items()
        },
    }


def document():
    return {
        "version": 1,
        "transaction_id": "tx-" + "a" * 32,
        "revision": 12,
        "release_commit": RELEASE,
        "workers": [
            {"current": identity(i), "predecessor": identity(i, old=True)}
            for i in range(4)
        ],
    }


def decode(payload=None):
    return reconcile.decode_pins(json.dumps(document() if payload is None else payload))


def facts(payload):
    registrations, grants = [], []
    for worker in payload["workers"]:
        for kind in ("current", "predecessor"):
            pin = worker[kind]
            if pin is None:
                continue
            old = kind == "predecessor"
            shared = {
                field: pin[field]
                for field in (
                    "service_name",
                    "worker_type",
                    "worker_host",
                    "image_identity",
                    "database_principal",
                )
            }
            shared["capabilities_json"] = list(pin["capabilities"])
            grants.append(
                WorkerAdmissionGrant(
                    id=UUID(pin["grant_id"]),
                    generation=pin["generation"],
                    release_commit=pin["release_commit"],
                    redis_stream=f"vp:tasks:{pin['worker_type']}",
                    redis_group=f"{pin['worker_type']}-workers",
                    endpoint_bindings_json=copy.deepcopy(ENDPOINTS),
                    state="revoked" if old else "active",
                    revoked_at=NOW - timedelta(minutes=5) if old else None,
                    revoke_reason="superseded" if old else None,
                    activated_at=NOW - timedelta(hours=2),
                    **shared,
                )
            )
            registrations.append(
                WorkerRegistration(
                    id=UUID(pin["registration_id"]),
                    grant_id=UUID(pin["grant_id"]),
                    worker_instance_id=UUID(pin["worker_instance_id"]),
                    worker_slot=pin["worker_slot"],
                    redis_consumer_id=pin["redis_consumer_id"],
                    lease_epoch=pin["lease_epoch"],
                    registered_at=datetime.fromisoformat(pin["registered_at"]),
                    heartbeat_at=NOW - timedelta(seconds=10),
                    lease_expires_at=NOW + timedelta(seconds=-1 if old else 120),
                    status="revoked" if old else "active",
                    revoked_at=NOW - timedelta(minutes=5) if old else None,
                    revoke_reason="superseded" if old else None,
                    superseded_by=UUID(worker["current"]["registration_id"])
                    if old
                    else None,
                    **{
                        f"{key}_fingerprint": pin[f"{key}_fingerprint"]
                        for key in ENDPOINTS
                    },
                    **shared,
                )
            )
    return registrations, grants


def inventories(payload):
    result = {}
    for index in (0, 1, 3):
        worker = payload["workers"][index]
        current = worker["current"]
        consumers = [{"name": current["redis_consumer_id"], "pending": 0, "idle": 100}]
        if worker["predecessor"] is not None:
            consumers.append(
                {
                    "name": worker["predecessor"]["redis_consumer_id"],
                    "pending": 0,
                    "idle": 120001,
                }
            )
        result[current["service_name"]] = {
            "stream": f"vp:tasks:{current['worker_type']}",
            "group": f"{current['worker_type']}-workers",
            "pending": 0,
            "lag": 0,
            "consumers": consumers,
        }
    return result


def assess(payload=None, *, rows=None, inventory=None, replay_only=False):
    payload = document() if payload is None else payload
    registrations, grants = facts(payload) if rows is None else rows
    return reconcile.assess(
        decode(payload),
        registrations,
        grants,
        inventories(payload) if inventory is None else inventory,
        now=NOW,
        replay_only=replay_only,
    )


def test_fixed_three_stream_commands_and_immutable_pins():
    pins = decode()
    with pytest.raises(FrozenInstanceError):
        pins.revision = 13
    assert isinstance(pins.workers, tuple)
    assert isinstance(pins.workers[0].current.capabilities, tuple)
    with pytest.raises(FrozenInstanceError):
        pins.workers[0].current.generation = 10
    assert hash(pins)
    result = assess()
    assert result.outcome == "ready"
    assert len(result.commands) == 3
    for command, index in zip(result.commands, (0, 1, 3), strict=True):
        pin = document()["workers"][index]
        assert command.arguments == (
            "EVAL",
            reconcile.ATOMIC_RECONCILE_LUA,
            1,
            f"vp:tasks:{pin['current']['worker_type']}",
            pin["current"]["redis_consumer_id"],
            pin["predecessor"]["redis_consumer_id"],
        )


@pytest.mark.parametrize(
    "path,value",
    [
        (("version",), True),
        (("revision",), True),
        (("revision",), -1),
        (("transaction_id",), str(UUID(int=1))),
        (("release_commit",), "b9a68b7"),
        (("workers", 0, "current", "registration_id"), "bad"),
        (("workers", 0, "current", "registration_id"), "A" * 32),
        (("workers", 0, "current", "generation"), True),
        (("workers", 0, "current", "lease_epoch"), True),
        (("workers", 0, "current", "worker_slot"), True),
        (("workers", 0, "current", "worker_slot"), 2),
        (("workers", 0, "current", "registered_at"), "2026-09-11T00:00:00"),
        (("workers", 0, "current", "database_principal"), "has spaces"),
        (("workers", 0, "current", "capabilities"), ["media_cpu", "media_cpu"]),
        (("workers", 0, "current", "database_fingerprint"), "x" * 64),
        (("workers", 0, "current", "image_identity"), "vp:latest"),
        (("workers", 0, "current", "release_commit"), OLD_RELEASE),
        (("workers", 0, "current", "redis_consumer_id"), "caller-picked-old"),
        (("workers", 0, "predecessor", "lease_epoch"), 21),
        (("workers", 0, "predecessor", "generation"), 9),
        (("workers", 0, "predecessor", "service_name"), TOPOLOGY[1][0]),
    ],
)
def test_strict_pin_contract_rejects_malformed_or_inconsistent_fields(path, value):
    payload = document()
    target = payload
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    with pytest.raises(reconcile.ReconcileRefused):
        decode(payload)


@pytest.mark.parametrize("location", [(), ("workers", 0), ("workers", 0, "current")])
def test_unknown_fields_are_not_silently_dropped(location):
    payload = document()
    target = payload
    for key in location:
        target = target[key]
    target["unknown"] = "not-authority"
    with pytest.raises(reconcile.ReconcileRefused):
        decode(payload)


@pytest.mark.parametrize(
    "variant", ["missing_old", "missing_worker", "extra_worker", "duplicate_worker"]
)
def test_four_readiness_workers_and_explicit_predecessor_absence_required(variant):
    payload = document()
    if variant == "missing_old":
        del payload["workers"][0]["predecessor"]
    elif variant == "missing_worker":
        payload["workers"].pop(2)
    elif variant == "extra_worker":
        payload["workers"].append(copy.deepcopy(payload["workers"][0]))
    else:
        payload["workers"][2] = copy.deepcopy(payload["workers"][0])
    with pytest.raises(reconcile.ReconcileRefused):
        decode(payload)


def test_duplicate_json_keys_and_constants_are_refused():
    raw = json.dumps(document())
    for changed in (
        raw.replace('"version": 1', '"version": 1, "version": 1'),
        raw.replace('"revision": 12', '"revision": NaN'),
    ):
        with pytest.raises(reconcile.ReconcileRefused):
            reconcile.decode_pins(changed)


def test_pin_digest_and_external_deployment_binding_are_exact():
    pins = decode()
    targets = {
        worker.current.service_name: (9, worker.current.image_identity)
        for worker in pins.workers
    }
    expected = dict(
        transaction_id="tx-" + "a" * 32,
        revision=12,
        release_commit=RELEASE,
        targets=targets,
        pin_sha256=pins.sha256,
    )
    reconcile.validate_deployment(pins, **expected)
    assert decode(json.loads(pins.canonical_json)).sha256 == pins.sha256
    for field, bad in (
        ("transaction_id", "tx-" + "b" * 32),
        ("revision", 13),
        ("release_commit", OLD_RELEASE),
        ("pin_sha256", "0" * 64),
        ("targets", {}),
        ("revision", True),
        ("targets", {**targets, TOPOLOGY[0][0]: (10, targets[TOPOLOGY[0][0]][1])}),
        ("targets", {**targets, TOPOLOGY[0][0]: (9, "other:deploy-" + RELEASE[:12])}),
    ):
        with pytest.raises(reconcile.ReconcileRefused):
            reconcile.validate_deployment(pins, **{**expected, field: bad})


REGISTRATION_CHANGES = {
    "id": UUID(int=999),
    "grant_id": UUID(int=999),
    "service_name": "another-service",
    "worker_type": "another-type",
    "worker_host": "another-host",
    "capabilities_json": ["vision_gpu"],
    "image_identity": "other:deploy-" + RELEASE[:12],
    "database_principal": "another_principal",
    "worker_instance_id": UUID(int=999),
    "worker_slot": 2,
    "redis_consumer_id": "unknown-consumer",
    "lease_epoch": 42,
    "registered_at": NOW,
    "database_fingerprint": "0" * 64,
    "redis_fingerprint": "0" * 64,
    "storage_fingerprint": "0" * 64,
}
GRANT_CHANGES = {
    "id": UUID(int=999),
    "generation": 42,
    "service_name": "another-service",
    "worker_type": "vision",
    "worker_host": "another-host",
    "capabilities_json": ["vision_gpu"],
    "release_commit": "c" * 40,
    "image_identity": "other:deploy-" + RELEASE[:12],
    "database_principal": "another_principal",
    "redis_stream": "vp:events",
    "redis_group": "orchestrator",
    "endpoint_bindings_json": {},
}


@pytest.mark.parametrize("old", [False, True])
@pytest.mark.parametrize("field,value", REGISTRATION_CHANGES.items())
def test_every_registration_identity_field_is_pinned(old, field, value):
    rows = facts(document())
    setattr(rows[0][int(old)], field, value)
    with pytest.raises(reconcile.ReconcileRefused):
        assess(rows=rows)


@pytest.mark.parametrize("old", [False, True])
@pytest.mark.parametrize("field,value", GRANT_CHANGES.items())
def test_every_grant_binding_is_pinned(old, field, value):
    rows = facts(document())
    setattr(rows[1][int(old)], field, value)
    with pytest.raises(reconcile.ReconcileRefused):
        assess(rows=rows)


@pytest.mark.parametrize("old", [False, True])
@pytest.mark.parametrize(
    "table,field", [(0, "id"), (1, "id"), (0, "grant_id"), (0, "worker_instance_id")]
)
def test_same_value_native_asyncpg_uuid_facts_remain_eligible(old, table, field):
    from asyncpg.pgproto.pgproto import UUID as NativeUUID

    rows = facts(document())
    row = rows[table][int(old)]
    setattr(row, field, NativeUUID(str(getattr(row, field))))
    assert assess(rows=rows).outcome == "ready"


@pytest.mark.parametrize("old", [False, True])
@pytest.mark.parametrize(
    "table,field", [(0, "id"), (1, "id"), (0, "grant_id"), (0, "worker_instance_id")]
)
@pytest.mark.parametrize("variant", ["changed_native", "string", "boolean"])
def test_uuid_fact_compatibility_does_not_accept_changed_or_coerced_values(
    old,
    table,
    field,
    variant,
):
    from asyncpg.pgproto.pgproto import UUID as NativeUUID

    rows = facts(document())
    row = rows[table][int(old)]
    value = {
        "changed_native": NativeUUID(str(UUID(int=999))),
        "string": str(getattr(row, field)),
        "boolean": True,
    }[variant]
    setattr(row, field, value)
    with pytest.raises(reconcile.ReconcileRefused):
        assess(rows=rows)


def test_complete_native_uuid_snapshot_preserves_exact_supersession():
    from asyncpg.pgproto.pgproto import UUID as NativeUUID

    rows = facts(document())
    for registration in rows[0]:
        for field in ("id", "grant_id", "worker_instance_id", "superseded_by"):
            value = getattr(registration, field)
            if value is not None:
                setattr(registration, field, NativeUUID(str(value)))
    for grant in rows[1]:
        grant.id = NativeUUID(str(grant.id))
    assert assess(rows=rows).outcome == "ready"
    for value in (NativeUUID(str(UUID(int=999))), str(rows[0][0].id), True):
        rows[0][1].superseded_by = value
        with pytest.raises(reconcile.ReconcileRefused):
            assess(rows=rows)


@pytest.mark.parametrize(
    "old,table,field,value",
    [
        (False, 0, "status", "revoked"),
        (False, 0, "revoked_at", NOW),
        (False, 0, "revoke_reason", "revoked"),
        (False, 0, "superseded_by", UUID(int=999)),
        (False, 1, "state", "revoked"),
        (False, 1, "revoked_at", NOW),
        (False, 1, "activated_at", None),
        (False, 1, "revoke_reason", "revoked"),
        (True, 0, "status", "active"),
        (True, 0, "revoked_at", None),
        (True, 0, "revoke_reason", None),
        (True, 0, "superseded_by", None),
        (True, 0, "superseded_by", UUID(int=999)),
        (True, 1, "state", "active"),
        (True, 1, "revoked_at", None),
        (True, 1, "revoke_reason", None),
    ],
)
def test_current_and_predecessor_lifecycle_is_fenced(old, table, field, value):
    rows = facts(document())
    setattr(rows[table][int(old)], field, value)
    with pytest.raises(reconcile.ReconcileRefused):
        assess(rows=rows)


@pytest.mark.parametrize("seconds", [-1, 0, 60])
def test_current_lease_without_native_margin_is_refusal_not_wait(seconds):
    rows = facts(document())
    rows[0][0].lease_expires_at = NOW + timedelta(seconds=seconds)
    with pytest.raises(reconcile.ReconcileRefused):
        assess(rows=rows)


def test_natural_old_lease_aging_waits_without_preparing_any_mutation():
    rows = facts(document())
    rows[0][1].lease_expires_at = NOW + timedelta(seconds=1)
    result = assess(rows=rows)
    assert result.outcome == "wait"
    assert result.commands == ()
    rows[0][1].lease_expires_at = NOW
    assert assess(rows=rows).outcome == "ready"


def test_current_heartbeat_renewal_is_not_frozen_identity_drift():
    rows = facts(document())
    for row in rows[0]:
        if row.status == "active":
            row.heartbeat_at = NOW
            row.lease_expires_at = NOW + timedelta(seconds=61)
    assert assess(rows=rows).outcome == "ready"


@pytest.mark.parametrize("old", [False, True])
@pytest.mark.parametrize("endpoint", ["database", "redis", "storage"])
def test_valid_but_changed_grant_endpoint_is_not_expected_fingerprint(old, endpoint):
    rows = facts(document())
    rows[1][int(old)].endpoint_bindings_json[endpoint]["host"] = "10.0.0.151"
    with pytest.raises(reconcile.ReconcileRefused):
        assess(rows=rows)


def test_explicit_absence_cannot_allow_an_unpinned_old_redis_name():
    payload = document()
    payload["workers"][0]["predecessor"] = None
    with pytest.raises(reconcile.ReconcileRefused):
        assess(payload, inventory=inventories(document()))


def test_extra_active_grant_is_not_hidden_by_exact_current_registration():
    rows = facts(document())
    extra = WorkerAdmissionGrant(
        id=UUID(int=999),
        service_name=TOPOLOGY[0][0],
        state="active",
    )
    rows[1].append(extra)
    with pytest.raises(reconcile.ReconcileRefused):
        assess(rows=rows)


@pytest.mark.parametrize(
    "variant", ["missing", "extra", "duplicate", "vision_not_ready"]
)
def test_complete_current_readiness_not_just_retiring_streams(variant):
    rows = facts(document())
    if variant == "missing":
        rows[0].pop(0)
    elif variant == "extra":
        extra = copy.copy(rows[0][0])
        extra.id = UUID(int=999)
        rows[0].append(extra)
    elif variant == "duplicate":
        rows[0].append(rows[0][0])
    else:
        rows[0][4].lease_expires_at = NOW
    with pytest.raises(reconcile.ReconcileRefused):
        assess(rows=rows)


@pytest.mark.parametrize(
    "old_idle,expected", [(0, "wait"), (120000, "wait"), (120001, "ready")]
)
def test_old_idle_strict_production_boundary(old_idle, expected):
    inventory = inventories(document())
    inventory[TOPOLOGY[0][0]]["consumers"][1]["idle"] = old_idle
    result = assess(inventory=inventory)
    assert result.outcome == expected
    if expected == "wait":
        assert result.commands == ()


@pytest.mark.parametrize("idle", [0, 120000])
def test_current_idle_accepts_exact_active_boundary(idle):
    inventory = inventories(document())
    inventory[TOPOLOGY[0][0]]["consumers"][0]["idle"] = idle
    assert assess(inventory=inventory).outcome == "ready"


@pytest.mark.parametrize(
    "variant",
    [
        "no_current",
        "extra_current",
        "duplicate",
        "unknown_old",
        "stale_current",
        "group_pending",
        "consumer_pending",
        "lag",
        "unknown_lag",
        "wrong_stream",
        "wrong_group",
        "missing_stream",
        "extra_stream",
        "boolean",
        "negative",
        "string_idle",
        "missing_idle",
        "malformed",
        "old_pending",
    ],
)
def test_bad_inventory_is_refused_even_when_old_lease_would_wait(variant):
    inventory = inventories(document())
    target = inventory[TOPOLOGY[0][0]]
    consumers = target["consumers"]
    if variant == "no_current":
        consumers.pop(0)
    elif variant in {"extra_current", "duplicate"}:
        consumers.append(
            dict(consumers[0], name="another-current")
            if variant == "extra_current"
            else dict(consumers[0])
        )
    elif variant == "unknown_old":
        consumers[1]["name"] = "registered-looking-but-not-pinned"
    elif variant == "stale_current":
        consumers[0]["idle"] = 120001
    elif variant == "group_pending":
        target["pending"] = 1
    elif variant == "consumer_pending":
        consumers[0]["pending"] = 1
    elif variant == "old_pending":
        consumers[1]["pending"] = 1
    elif variant in {"lag", "unknown_lag"}:
        target["lag"] = 1 if variant == "lag" else None
    elif variant == "wrong_stream":
        target["stream"] = "vp:events"
    elif variant == "wrong_group":
        target["group"] = "orchestrator"
    elif variant == "missing_stream":
        inventory.pop(TOPOLOGY[0][0])
    elif variant == "extra_stream":
        inventory[TOPOLOGY[2][0]] = copy.deepcopy(target)
    elif variant == "boolean":
        consumers[0]["pending"] = False
    elif variant == "negative":
        consumers[1]["idle"] = -1
    elif variant == "string_idle":
        consumers[1]["idle"] = "120001"
    elif variant == "missing_idle":
        del consumers[1]["idle"]
    else:
        target["consumers"] = "not-an-inventory"
    rows = facts(document())
    rows[0][1].lease_expires_at = NOW + timedelta(seconds=10)
    with pytest.raises(reconcile.ReconcileRefused):
        assess(rows=rows, inventory=inventory)


@pytest.mark.parametrize("fresh", [False, True])
def test_absent_predecessors_are_read_only_including_replay(fresh):
    payload = document()
    if fresh:
        for worker in payload["workers"]:
            worker["predecessor"] = None
    inventory = inventories(payload)
    for item in inventory.values():
        item["consumers"] = item["consumers"][:1]
    for replay in (False, True):
        result = assess(payload, inventory=inventory, replay_only=replay)
        assert result.outcome == "already_absent"
        assert result.commands == ()


def test_replay_of_partial_attempt_cannot_prepare_remaining_deletions():
    inventory = inventories(document())
    inventory[TOPOLOGY[0][0]]["consumers"].pop()
    with pytest.raises(reconcile.ReconcileRefused):
        assess(inventory=inventory, replay_only=True)
    assert len(assess(inventory=inventory).commands) == 2


def test_lua_result_must_match_exact_command_and_known_outcome():
    command = assess().commands[0]
    current = identity(0)["redis_consumer_id"]
    old = identity(0, old=True)["redis_consumer_id"]
    assert (
        reconcile.validate_lua_result(command, ["retired", current, old]) == "retired"
    )
    assert (
        reconcile.validate_lua_result(command, ["already_absent", current])
        == "already_absent"
    )
    for bad in (
        None,
        [],
        ["retired", current],
        ["retired", current, "other"],
        ["already_absent", "other"],
        ["ok", current],
    ):
        with pytest.raises(reconcile.ReconcileRefused):
            reconcile.validate_lua_result(command, bad)


def test_eval_command_cannot_select_vision_events_or_caller_supplied_names():
    pins = decode()
    for service in (TOPOLOGY[2][0], "vp:events", "caller-selected-worker"):
        with pytest.raises(reconcile.ReconcileRefused):
            reconcile.EvalCommand(pins=pins, service_name=service)
    payload = document()
    payload["workers"][0]["predecessor"] = None
    with pytest.raises(reconcile.ReconcileRefused):
        reconcile.EvalCommand(pins=decode(payload), service_name=TOPOLOGY[0][0])
    command = assess().commands[0]
    assert command.pins is not None
    assert command.pins.sha256 == pins.sha256


@pytest.mark.parametrize("lag", [True, False, -1, 1, 0.0, "0", None])
def test_lag_must_be_strict_integer_zero_not_merely_zero_pending(lag):
    inventory = inventories(document())
    inventory[TOPOLOGY[0][0]]["lag"] = lag
    with pytest.raises(reconcile.ReconcileRefused):
        assess(inventory=inventory)


def test_missing_lag_is_refusal_not_natural_wait():
    inventory = inventories(document())
    del inventory[TOPOLOGY[0][0]]["lag"]
    with pytest.raises(reconcile.ReconcileRefused):
        assess(inventory=inventory)


@pytest.mark.parametrize("old,field", [(False, "generation"), (True, "generation")])
def test_boolean_db_generation_is_not_integer_equality(old, field):
    payload = document()
    # A valid generation 1 pin must still not accept the boolean True DB fact.
    payload["workers"][0]["predecessor"]["generation"] = 1
    if not old:
        payload["workers"][0]["current"]["generation"] = 1
        payload["workers"][0]["predecessor"] = None
    rows = facts(payload)
    setattr(rows[1][int(old)], field, True)
    with pytest.raises(reconcile.ReconcileRefused):
        assess(payload, rows=rows)


def test_lua_has_only_fixed_metadata_commands_and_no_vision_or_event_key():
    # Structural qualification only: real Redis race/aging tests belong to Unit 2/3.
    import re

    script = reconcile.ATOMIC_RECONCILE_LUA
    calls = re.findall(r'redis.call\(\s*"([A-Z]+)"(?:,\s*"([A-Z]+)")?', script)
    assert set(calls) == {
        ("XPENDING", ""),
        ("XINFO", "CONSUMERS"),
        ("XINFO", "GROUPS"),
        ("XGROUP", "DELCONSUMER"),
    }
    assert "vp:tasks:vision" not in script
    assert "vp:events" not in script
    assert "120000" in script


@pytest.fixture
def loop_bound_redis_boundary(monkeypatch):
    """Fake only Redis I/O and the long wait; exercise the actual fixture runner."""
    import asyncio

    from redis.asyncio import Redis

    events = []
    url = "redis://127.0.0.1:1/15"

    class LoopBoundRedis:
        def __init__(self):
            self.loop = asyncio.get_running_loop()

        def record(self, event):
            assert asyncio.get_running_loop() is self.loop, "Redis client crossed loops"
            events.append(event)

        async def exists(self, *keys):
            self.record("setup")
            assert keys == (
                "vp:tasks:ffmpeg_go",
                "vp:tasks:ffmpeg",
                "vp:tasks:youtube_publisher",
            )
            return 0

        async def info(self, section):
            assert section == "server"
            return {"redis_version": "7.4.7"}

        async def xadd(self, stream, fields):
            assert (stream, fields) == ("vp:tasks:ffmpeg_go", {"fixture": "seed"})
            return "1-0"

        async def xgroup_create(self, stream, group, *, id):
            assert (stream, group, id) == (
                "vp:tasks:ffmpeg_go",
                "ffmpeg_go-workers",
                "$",
            )

        async def xgroup_createconsumer(self, stream, group, consumer):
            self.record("consumer")
            assert (stream, group) == ("vp:tasks:ffmpeg_go", "ffmpeg_go-workers")
            assert consumer in {
                identity(0)["redis_consumer_id"],
                identity(0, old=True)["redis_consumer_id"],
            }

        async def xinfo_consumers(self, stream, group):
            self.record("body")
            assert (stream, group) == ("vp:tasks:ffmpeg_go", "ffmpeg_go-workers")
            return []

        async def delete(self, stream):
            self.record("cleanup")
            assert stream == "vp:tasks:ffmpeg_go"

        async def aclose(self):
            self.record("closed")

    def from_url(actual_url, *, decode_responses):
        assert actual_url == url and decode_responses is True
        return LoopBoundRedis()

    async def skip_natural_wait(delay):
        assert delay == 120.1
        events.append("wait")

    monkeypatch.setenv("REGISTERED_RECONCILE_REDIS_TEST_URL", url)
    monkeypatch.setenv(
        "REGISTERED_RECONCILE_REDIS_TEST_CONFIRM", "disposable-fixed-stream-db15"
    )
    monkeypatch.setattr(Redis, "from_url", from_url)
    monkeypatch.setattr(asyncio, "sleep", skip_natural_wait)
    return events


@pytest.mark.anyio
@pytest.mark.parametrize("body_fails", [False, True])
async def test_isolated_redis_fixture_keeps_setup_body_cleanup_on_one_loop(
    loop_bound_redis_boundary,
    isolated_redis_lag_race,
    body_fails,
):
    expected = (
        pytest.raises(RuntimeError, match="test body failure")
        if body_fails
        else nullcontext()
    )
    with expected:
        async with isolated_redis_lag_race() as (client, stream, group):
            await client.xinfo_consumers(stream, group)
            if body_fails:
                raise RuntimeError("test body failure")
    assert loop_bound_redis_boundary == [
        "setup",
        "consumer",
        "wait",
        "consumer",
        "body",
        "cleanup",
        "closed",
    ]


@pytest.fixture
def isolated_redis_lag_race():
    # A sync factory keeps creation through cleanup in the test's own loop,
    # regardless of pytest-asyncio auto mode and AnyIO's independent runner.
    return _isolated_redis_lag_race


@asynccontextmanager
async def _isolated_redis_lag_race():
    """Parent-only opt-in. Fixed production keys need an empty disposable DB.

    No default URL, flush, server setup or threshold override. Unit 1 delivery
    runs exclude this fixture; its skip/pass is reported separately from offline
    validation. Use Redis >=7.2 on an explicit nondefault loopback port, DB 15.
    """
    import asyncio
    import os
    from urllib.parse import urlsplit

    from redis.asyncio import Redis

    url = os.environ.get("REGISTERED_RECONCILE_REDIS_TEST_URL")
    if not url:
        pytest.skip("explicit disposable Redis qualification not configured")
    parsed = urlsplit(url)
    if not (
        os.environ.get("REGISTERED_RECONCILE_REDIS_TEST_CONFIRM")
        == "disposable-fixed-stream-db15"
        and parsed.scheme == "redis"
        and parsed.hostname == "127.0.0.1"
        and parsed.port is not None
        and parsed.port != 6379
        and parsed.path == "/15"
        and not parsed.query
        and not parsed.fragment
    ):
        pytest.fail("disposable Redis qualification guard refused")
    client = Redis.from_url(url, decode_responses=True)
    stream, group = "vp:tasks:ffmpeg_go", "ffmpeg_go-workers"
    created = False
    try:
        assert not await client.exists(
            "vp:tasks:ffmpeg_go",
            "vp:tasks:ffmpeg",
            "vp:tasks:youtube_publisher",
        ), "fixed test keys must not already exist"
        version = (await client.info("server"))["redis_version"]
        assert tuple(int(part) for part in version.split(".")[:2]) >= (7, 2)
        await client.xadd(stream, {"fixture": "seed"})
        created = True
        await client.xgroup_create(stream, group, id="$")
        await client.xgroup_createconsumer(
            stream, group, identity(0, old=True)["redis_consumer_id"]
        )
        await asyncio.sleep(120.1)
        await client.xgroup_createconsumer(
            stream, group, identity(0)["redis_consumer_id"]
        )
        yield client, stream, group
    finally:
        if created:
            await client.delete(stream)
        await client.aclose()


@pytest.mark.anyio
async def test_real_redis_lag_only_xadd_after_observation_is_not_zero_backlog(
    isolated_redis_lag_race,
):
    from redis.exceptions import ResponseError

    async with isolated_redis_lag_race() as (client, stream, group):
        inventory = inventories(document())
        before_consumers = await client.xinfo_consumers(stream, group)
        before_group = next(
            item for item in await client.xinfo_groups(stream) if item["name"] == group
        )
        assert before_group["lag"] == 0
        inventory[TOPOLOGY[0][0]] = {
            "stream": stream,
            "group": group,
            "pending": (await client.xpending(stream, group))["pending"],
            "lag": before_group["lag"],
            "consumers": before_consumers,
        }
        command = assess(inventory=inventory).commands[0]
        # Distinct from the PEL race: no XREADGROUP occurs after this append.
        await client.xadd(stream, {"fixture": "lag-only-race"})
        assert (await client.xpending(stream, group))["pending"] == 0
        assert (
            next(
                item
                for item in await client.xinfo_groups(stream)
                if item["name"] == group
            )["lag"]
            == 1
        )
        with pytest.raises(ResponseError, match="registered_reconcile_backlog"):
            await client.execute_command(*command.arguments)
        assert {
            item["name"] for item in await client.xinfo_consumers(stream, group)
        } == {
            identity(0)["redis_consumer_id"],
            identity(0, old=True)["redis_consumer_id"],
        }
