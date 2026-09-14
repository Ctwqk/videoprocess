"""The observed same-grant continuity restart is an exact, non-mutating edge."""
import copy
import json
from datetime import datetime, timedelta
from uuid import UUID

import pytest

from app.services import registered_consumer_reconcile as core
from app.services import registered_consumer_reconcile_runtime as runtime
from tests.services.registered_consumer_history_fixtures import history_document, history_facts
from tests.services.test_registered_consumer_history_runtime import history_rows
from tests.services.test_registered_consumer_reconcile import NOW, decode


def restart_document():
    payload = history_document()
    worker = payload["workers"][0]
    old, newer = worker["ancestors"][0], worker["predecessor"]
    registration_fields = {"registration_id", "worker_instance_id", "redis_consumer_id", "lease_epoch", "registered_at"}
    old.update({key: copy.deepcopy(value) for key, value in newer.items() if key not in registration_fields})
    return payload


def restart_facts(payload):
    registrations, grants = history_facts(payload)
    old = next(row for row in registrations if str(row.id) == payload["workers"][0]["ancestors"][0]["registration_id"])
    old.superseded_by = None
    old.revoke_reason = "worker_redis_continuity_unready"
    old.revoked_at = datetime.fromisoformat(payload["workers"][0]["predecessor"]["registered_at"]) - timedelta(seconds=1)
    unique = {}
    for grant in grants:
        unique.setdefault(grant.id, grant)
    return registrations, list(unique.values()), old


def test_v2_exact_restart_bridge_reaches_baseline_without_repairing_rows():
    payload = restart_document()
    pins = decode(payload)
    registrations, grants, old = restart_facts(payload)
    assert core.validate_database(pins, registrations, grants, now=NOW) == ()
    assert old.superseded_by is None
    assert pins.workers[0].ancestors[0].grant_id == pins.workers[0].predecessor.grant_id


@pytest.mark.parametrize("version", [1, 2])
def test_current_to_predecessor_never_reuses_the_active_grant(version):
    payload = restart_document()
    payload["version"] = version
    if version == 1:
        for worker in payload["workers"]:
            del worker["ancestors"]
    old, current = payload["workers"][0]["predecessor"], payload["workers"][0]["current"]
    distinct = {"registration_id", "worker_instance_id", "redis_consumer_id", "lease_epoch", "registered_at"}
    old.update({key: copy.deepcopy(value) for key, value in current.items() if key not in distinct})
    with pytest.raises(core.ReconcileRefused):
        decode(payload)


@pytest.mark.parametrize("fault", ["wrong_reason", "nonnull_wrong_link", "early_revocation", "late_revocation", "active_shared_grant"])
def test_restart_bridge_never_substitutes_for_unproven_revocation(fault):
    payload = restart_document()
    pins = decode(payload)
    registrations, grants, old = restart_facts(payload)
    if fault == "wrong_reason":
        old.revoke_reason = "operator_revoked"
    elif fault == "nonnull_wrong_link":
        old.superseded_by = UUID(int=999999)
    elif fault == "early_revocation":
        old.revoked_at = old.registered_at - timedelta(seconds=1)
    elif fault == "late_revocation":
        old.revoked_at = pins.workers[0].predecessor.registered_at + timedelta(seconds=1)
    else:
        next(grant for grant in grants if grant.id == old.grant_id).state = "active"
    with pytest.raises(core.ReconcileRefused):
        core.validate_database(pins, registrations, grants, now=NOW)


@pytest.mark.parametrize("field,value", [("lease_epoch", 1), ("generation", 1), ("database_fingerprint", "e" * 64)])
def test_restart_pins_require_adjacent_epoch_and_identical_grant_identity(field, value):
    payload = restart_document()
    payload["workers"][0]["ancestors"][0][field] = value
    with pytest.raises(core.ReconcileRefused):
        decode(payload)


def test_repeated_grant_rows_collapse_only_exact_pin_multiplicity_and_facts():
    pins = decode(restart_document())
    _, grants, _ = restart_facts(restart_document())
    by_id = {grant.id: grant for grant in grants}
    expanded = [copy.deepcopy(by_id[pin.grant_id]) for worker in pins.workers for pin in (worker.current, *worker.retiring)]
    collapsed = core.collapse_grant_facts(pins, expanded)
    assert len(collapsed) == len(grants)
    with pytest.raises(core.ReconcileRefused):
        core.collapse_grant_facts(pins, [*expanded, expanded[0]])
    shared = pins.workers[0].predecessor.grant_id
    next(grant for grant in expanded if grant.id == shared).revoke_reason = "changed"
    with pytest.raises(core.ReconcileRefused):
        core.collapse_grant_facts(pins, expanded)


def test_runtime_decodes_repeated_shared_grant_with_exact_row_count():
    payload = restart_document()
    pins = decode(payload)
    rows = history_rows(payload)
    older = next(row for row in rows if str(row["registration_id"]) == payload["workers"][0]["ancestors"][0]["registration_id"])
    older["superseded_by"] = None
    registration_facts = json.loads(older["registration_facts"])
    registration_facts["revoke_reason"] = "worker_redis_continuity_unready"
    older["registration_facts"] = json.dumps(registration_facts)
    older["registration_revoked_at"] = pins.workers[0].predecessor.registered_at - timedelta(seconds=1)
    facts = runtime.decode_guard(rows, pins)
    assert len(facts.registrations) == len(rows)
    assert len(facts.grants) == len(rows) - 1
    corrupted = copy.deepcopy(rows)
    corrupt_row = next(row for row in corrupted if row["grant_id"] == pins.workers[0].predecessor.grant_id)
    grant_facts = json.loads(corrupt_row["grant_facts"])
    grant_facts["revoke_reason"] = "changed"
    corrupt_row["grant_facts"] = json.dumps(grant_facts)
    with pytest.raises(runtime.ReconcileRuntimeError):
        runtime.decode_guard(corrupted, pins)
