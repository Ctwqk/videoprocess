from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import FrozenInstanceError, replace
from datetime import timedelta
from uuid import UUID

import pytest

from app.services import registered_consumer_reconcile as reconcile
from tests.services.registered_consumer_history_fixtures import (
    history_assess,
    history_document,
    history_facts,
    history_inventories,
)
from tests.services.test_registered_consumer_reconcile import (
    GRANT_CHANGES,
    NOW,
    REGISTRATION_CHANGES,
    TOPOLOGY,
    assess,
    decode,
    document,
)


def test_v1_canonical_lua_and_command_bytes_remain_compatible():
    pins = decode()
    assert (
        pins.sha256
        == "0af620dbfed1aa91a567efabb21b2c2166c27721b2df6d37850c13aeccfc7b81"
    )
    assert pins.canonical_json == json.dumps(
        document(), sort_keys=True, separators=(",", ":")
    )
    assert hashlib.sha256(reconcile.ATOMIC_RECONCILE_LUA.encode()).hexdigest() == (
        "9d6b685e4b55ef3a155a009fbf6fbef2bf73569fd46098e25911fbc6a3dd79fe"
    )
    assert [
        hashlib.sha256(
            json.dumps(c.arguments, separators=(",", ":")).encode()
        ).hexdigest()
        for c in assess().commands
    ] == [
        "426fcfa58642d9a4a0e4a25d80754802e7be237d4fbe20ab0299cb2dff625685",
        "a3dcc2b85ea12ad7ca6a0e6d4c6aa99909be07ce899cbb9423ace9508f56f117",
        "2f0df80496cc57cf80a89979d9e91805581ef9b439c404a81c2fa87338c545e0",
    ]


def test_v2_empty_ancestry_round_trip():
    payload = document()
    payload["version"] = 2
    for worker in payload["workers"]:
        worker["ancestors"] = []
    pins = decode(payload)
    assert pins.version == 2
    assert all(worker.retiring == (worker.predecessor,) for worker in pins.workers)
    assert json.loads(pins.canonical_json) == payload
    assert reconcile.decode_pins(pins.canonical_json) == pins


def test_v2_three_old_generations_assess_as_one_command_per_stream():
    pins = decode(history_document())
    worker = pins.workers[0]
    assert worker.retiring == (worker.predecessor, *worker.ancestors)
    assert isinstance(worker.ancestors, tuple)
    with pytest.raises(FrozenInstanceError):
        worker.ancestors = ()
    result = history_assess()
    assert result.outcome == "ready"
    assert len(result.commands) == len({c.stream for c in result.commands}) == 3
    for command in result.commands:
        assert command.predecessors == tuple(
            pin.redis_consumer_id for pin in command.worker.retiring
        )
        assert command.predecessor == command.predecessors[0]
        assert command.arguments == (
            "EVAL",
            reconcile.ATOMIC_RECONCILE_HISTORY_LUA,
            1,
            command.stream,
            command.current,
            *command.predecessors,
        )
    assert reconcile.decode_pins(pins.canonical_json) == pins


def test_v1_default_ancestors_and_direct_construction_rejects_nonempty_history():
    legacy = decode()
    assert all(worker.ancestors == () for worker in legacy.workers)
    history = decode(history_document())
    with pytest.raises(reconcile.ReconcileRefused, match="pin_version"):
        replace(history, version=1)


@pytest.mark.parametrize("version", [1, 2])
def test_versions_have_exact_worker_schema(version):
    payload = history_document() if version == 1 else document()
    payload["version"] = version
    with pytest.raises(reconcile.ReconcileRefused, match="pin_schema"):
        decode(payload)


@pytest.mark.parametrize("bad", [None, {}, "", [None], [True]])
def test_ancestry_requires_explicit_identity_list(bad):
    payload = history_document()
    payload["workers"][0]["ancestors"] = bad
    with pytest.raises(reconcile.ReconcileRefused):
        decode(payload)


def test_direct_worker_requires_tuple_history_and_a_predecessor():
    worker = decode(history_document()).workers[0]
    for changes in ({"ancestors": list(worker.ancestors)}, {"predecessor": None}):
        with pytest.raises(reconcile.ReconcileRefused):
            replace(worker, **changes)
    absent = replace(worker, predecessor=None, ancestors=())
    assert absent.retiring == ()


def test_no_predecessor_means_no_commands_when_ancestry_is_empty():
    payload = history_document(0)
    result = history_assess(payload)
    assert result.outcome == "already_absent"
    assert result.commands == ()


def test_vision_cannot_gain_ancestors():
    worker = decode(history_document()).workers[2]
    ancestor = replace(
        worker.predecessor,
        generation=7,
        lease_epoch=19,
        registration_id=UUID(int=999),
        grant_id=UUID(int=998),
        worker_instance_id=UUID(int=997),
        redis_consumer_id=f"vision-worker@150-vision:1:{UUID(int=997)}",
    )
    with pytest.raises(reconcile.ReconcileRefused):
        replace(worker, ancestors=(ancestor,))


def test_retiring_bound_includes_predecessor_and_is_shared_with_consumers():
    payload = history_document(64)
    result = history_assess(payload)
    assert len(result.commands) == 3
    assert all(
        len(c.predecessors) == reconcile.MAX_RETIRING_PER_SERVICE
        for c in result.commands
    )
    assert all(len(c.arguments) == 69 for c in result.commands)
    assert sum(1 + len(w.retiring) for w in decode(payload).workers) == 197
    with pytest.raises(reconcile.ReconcileRefused, match="pin_history_limit"):
        decode(history_document(65))


@pytest.mark.parametrize("field", ["generation", "lease_epoch"])
@pytest.mark.parametrize("depth", [0, 1])
def test_every_adjacent_generation_and_epoch_must_strictly_increase(field, depth):
    payload = history_document()
    worker = payload["workers"][0]
    successor = worker["predecessor"] if depth == 0 else worker["ancestors"][0]
    worker["ancestors"][depth][field] = successor[field]
    with pytest.raises(reconcile.ReconcileRefused, match="pin_successor"):
        decode(payload)


@pytest.mark.parametrize("variant", ["foreign", "duplicate", "reversed", "cycle"])
def test_ancestry_cannot_change_service_duplicate_reorder_or_cycle(variant):
    payload = history_document()
    worker = payload["workers"][0]
    if variant == "foreign":
        worker["ancestors"][0] = payload["workers"][1]["ancestors"][0]
    elif variant == "duplicate":
        worker["ancestors"].append(copy.deepcopy(worker["ancestors"][-1]))
    elif variant == "reversed":
        worker["ancestors"].reverse()
    else:
        worker["ancestors"][-1] = copy.deepcopy(worker["current"])
    with pytest.raises(reconcile.ReconcileRefused):
        decode(payload)


@pytest.mark.parametrize("field", ["registration_id", "grant_id", "worker_instance_id"])
def test_ancestor_ids_are_globally_unique_not_only_adjacent(field):
    payload = history_document()
    oldest = payload["workers"][0]["ancestors"][-1]
    oldest[field] = payload["workers"][3]["current"][field]
    if field == "worker_instance_id":
        oldest["redis_consumer_id"] = f"ffmpeg_go-worker@colima-127:1:{oldest[field]}"
    with pytest.raises(reconcile.ReconcileRefused, match="pin_duplicate_identity"):
        decode(payload)


@pytest.mark.parametrize("table", [0, 1])
@pytest.mark.parametrize("variant", ["missing", "duplicate", "foreign"])
def test_history_requires_exact_complete_unique_fact_sets(table, variant):
    payload = history_document()
    rows = history_facts(payload)
    if variant == "missing":
        rows[table].pop()
    elif variant == "duplicate":
        rows[table].append(rows[table][-1])
    else:
        extra = copy.copy(rows[table][-1])
        extra.id = UUID(int=999999)
        rows[table].append(extra)
    with pytest.raises(reconcile.ReconcileRefused):
        reconcile.validate_database(decode(payload), *rows, now=NOW)


@pytest.mark.parametrize("field,value", REGISTRATION_CHANGES.items())
def test_every_historical_registration_identity_is_checked(field, value):
    payload = history_document()
    rows = history_facts(payload)
    setattr(rows[0][-1], field, value)
    with pytest.raises(reconcile.ReconcileRefused):
        reconcile.validate_database(decode(payload), *rows, now=NOW)


@pytest.mark.parametrize("field,value", GRANT_CHANGES.items())
def test_every_historical_grant_identity_is_checked(field, value):
    payload = history_document()
    rows = history_facts(payload)
    setattr(rows[1][-1], field, value)
    with pytest.raises(reconcile.ReconcileRefused):
        reconcile.validate_database(decode(payload), *rows, now=NOW)


@pytest.mark.parametrize(
    "variant", ["fork", "skip", "foreign", "missing", "cycle", "string"]
)
def test_each_old_fact_must_link_to_its_exact_next_newer_pin(variant):
    payload = history_document()
    rows = history_facts(payload)
    oldest = rows[0][-1]
    successors = {
        "fork": rows[0][-2].superseded_by,
        "skip": UUID(payload["workers"][3]["current"]["registration_id"]),
        "foreign": UUID(payload["workers"][0]["current"]["registration_id"]),
        "missing": None,
        "cycle": oldest.id,
        "string": str(oldest.superseded_by),
    }
    oldest.superseded_by = successors[variant]
    with pytest.raises(reconcile.ReconcileRefused, match="predecessor_not_superseded"):
        history_assess(payload, rows=rows)


@pytest.mark.parametrize(
    "table,field,value",
    [
        (0, "status", "active"),
        (1, "state", "active"),
        (0, "revoked_at", None),
        (1, "revoked_at", None),
        (0, "revoked_at", NOW + timedelta(seconds=1)),
        (1, "revoked_at", NOW + timedelta(seconds=1)),
        (0, "revoke_reason", " "),
        (1, "revoke_reason", ""),
    ],
)
def test_ancestors_require_revoked_registration_and_grant(table, field, value):
    rows = history_facts(history_document())
    setattr(rows[table][-1], field, value)
    with pytest.raises(reconcile.ReconcileRefused):
        history_assess(rows=rows)


@pytest.mark.parametrize("endpoint", ["database", "redis", "storage"])
def test_historical_endpoints_are_bound(endpoint):
    rows = history_facts(history_document())
    rows[1][-1].endpoint_bindings_json[endpoint]["host"] = "10.0.0.151"
    with pytest.raises(reconcile.ReconcileRefused, match="grant_endpoints_changed"):
        history_assess(rows=rows)


def test_absent_historical_consumer_still_requires_expired_database_lease():
    payload = history_document()
    rows = history_facts(payload)
    inventory = history_inventories(payload)
    for value in inventory.values():
        value["consumers"] = value["consumers"][:1]
    rows[0][-1].lease_expires_at = NOW + timedelta(seconds=1)
    result = history_assess(rows=rows, inventory=inventory)
    assert (result.outcome, result.commands) == ("wait", ())
    rows[0][-1].lease_expires_at = NOW
    assert history_assess(rows=rows, inventory=inventory).outcome == "already_absent"


@pytest.mark.parametrize("index", range(4))
def test_all_current_leases_still_require_readiness_margin(index):
    rows = history_facts(history_document())
    rows[0][index * 2].lease_expires_at = NOW + timedelta(seconds=60)
    with pytest.raises(reconcile.ReconcileRefused, match="current_not_ready"):
        history_assess(rows=rows)


@pytest.mark.parametrize("present", range(8))
def test_any_subset_of_pinned_old_names_can_be_absent(present):
    payload = history_document()
    inventory = history_inventories(payload)
    for value in inventory.values():
        value["consumers"] = value["consumers"][:1] + [
            item
            for index, item in enumerate(value["consumers"][1:])
            if present & (1 << index)
        ]
    result = history_assess(payload, inventory=inventory)
    assert result.outcome == ("ready" if present else "already_absent")
    assert len(result.commands) == (3 if present else 0)
    assert all(len(command.predecessors) == 3 for command in result.commands)


@pytest.mark.parametrize("index", [1, 2, 3])
@pytest.mark.parametrize("idle", [0, 120000, 120001])
def test_every_present_old_name_must_age_before_any_command(index, idle):
    inventory = history_inventories(history_document())
    inventory[TOPOLOGY[3][0]]["consumers"][index]["idle"] = idle
    result = history_assess(inventory=inventory)
    assert result.outcome == ("ready" if idle > 120000 else "wait")
    if idle <= 120000:
        assert result.commands == ()


@pytest.mark.parametrize(
    "variant",
    [
        "unknown",
        "duplicate",
        "no_current",
        "stale_current",
        "pending",
        "lag",
        "unknown_lag",
        "old_pending",
    ],
)
def test_history_does_not_weaken_complete_inventory_guards(variant):
    inventory = history_inventories(history_document())
    value = inventory[TOPOLOGY[3][0]]
    if variant == "unknown":
        value["consumers"].append({"name": "foreign", "idle": 120001, "pending": 0})
    elif variant == "duplicate":
        value["consumers"].append(dict(value["consumers"][-1]))
    elif variant == "no_current":
        value["consumers"].pop(0)
    elif variant == "stale_current":
        value["consumers"][0]["idle"] = 120001
    elif variant == "old_pending":
        value["consumers"][-1]["pending"] = 1
    elif variant == "unknown_lag":
        value["lag"] = None
    else:
        value[variant] = 1
    with pytest.raises(reconcile.ReconcileRefused):
        history_assess(inventory=inventory)


@pytest.mark.parametrize("remaining", [0, 1, 2, 3])
def test_uncertain_attempt_allows_only_all_absent_read_only_assessment(remaining):
    inventory = history_inventories(history_document())
    for value in inventory.values():
        value["consumers"] = value["consumers"][:1]
    if remaining:
        complete = history_inventories(history_document())
        service = TOPOLOGY[3][0]
        inventory[service]["consumers"].append(
            complete[service]["consumers"][remaining]
        )
        with pytest.raises(reconcile.ReconcileRefused, match="replay_incomplete"):
            history_assess(inventory=inventory, replay_only=True)
    else:
        result = history_assess(inventory=inventory, replay_only=True)
        assert (result.outcome, result.commands) == ("already_absent", ())


def test_history_lua_reply_attests_all_requested_names_not_delete_count():
    command = history_assess().commands[0]
    assert (
        reconcile.validate_lua_result(
            command, ["retired", command.current, *command.predecessors]
        )
        == "retired"
    )
    assert (
        reconcile.validate_lua_result(command, ["already_absent", command.current])
        == "already_absent"
    )
    for reply in (
        ["retired", command.current, command.predecessor],
        ["retired", command.current, *reversed(command.predecessors)],
        ["retired", command.current, *command.predecessors, "foreign"],
        ["retired", command.current, len(command.predecessors)],
        ["already_absent", command.current, *command.predecessors],
    ):
        with pytest.raises(reconcile.ReconcileRefused, match="lua_result_uncertain"):
            reconcile.validate_lua_result(command, reply)
