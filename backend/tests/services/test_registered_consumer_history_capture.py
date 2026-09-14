from __future__ import annotations

import copy
import importlib
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from uuid import UUID

import pytest

from app.services import registered_consumer_reconcile_job as job
from tests.services.registered_consumer_history_fixtures import (
    history_document,
    history_facts,
    history_inventories,
)
from tests.services.test_registered_consumer_reconcile import NOW, decode


def capture_module():
    name = "app.services.registered_consumer_history_capture"
    assert importlib.util.find_spec(name) is not None, "history capture is missing"
    return importlib.import_module(name)


def snapshots(payload):
    return tuple(
        {"observed_at": NOW.isoformat(), "workers": [w[key] for w in payload["workers"]]}
        for key in ("current", "predecessor")
    )


class RedisInventory:
    def __init__(self, payload):
        self.inventory = history_inventories(payload)
        self.calls = []

    async def xinfo_consumers(self, stream, group):
        service = next(name for name, fixed in job.STREAMS.items() if fixed == stream)
        expected = self.inventory[service]
        assert group == expected["group"]
        self.calls.append((stream, group))
        return copy.deepcopy(expected["consumers"])


class HistoryConnection:
    """Offline DB boundary; the parent qualifies the emitted SQL on actual PG."""

    def __init__(self, payload):
        self.registrations, self.grants = history_facts(payload)
        self.queries = []
        self.selected = []
        self.transactions = 0
        self.peak_transactions = 0
        self.corrupt_result = lambda rows: rows

    @asynccontextmanager
    async def transaction(self, **kwargs):
        assert kwargs == {"isolation": "repeatable_read", "readonly": True}
        self.transactions += 1
        self.peak_transactions = max(self.peak_transactions, self.transactions)
        try:
            yield
        finally:
            self.transactions -= 1

    async def fetchval(self, sql):
        assert self.transactions and sql == "SELECT transaction_timestamp()"
        return NOW

    def incoming(self, newer):
        return [row for row in self.registrations if row.superseded_by == newer.id]

    async def fetch(self, sql, *args):
        assert self.transactions
        assert "token_sha256" not in sql and "lease_secret" not in sql
        assert "FOR UPDATE" not in sql and "FOR SHARE" not in sql
        self.queries.append((sql, args))
        if not sql.lstrip().startswith("WITH RECURSIVE"):
            fields = sql.split("SELECT ", 1)[1].split(" FROM ", 1)[0].split(",")
            grants = "public.worker_admission_grants" in sql
            source = self.grants if grants else self.registrations
            state = "state" if grants else "status"
            return [
                {field: getattr(row, field) for field in fields}
                for row in source if getattr(row, state) == "active"
            ]
        current_id, required, sentinel = args
        assert type(current_id) is UUID and sentinel == 65
        remaining = set(required)
        row = next((r for r in self.registrations if r.id == current_id), None)
        result, depth, fork = [], 0, False
        while row is not None:
            grant = next((g for g in self.grants if g.id == row.grant_id), None)
            values = {
                "depth": depth, "fork": fork,
                **{"r_" + k: v for k, v in vars(row).items() if not k.startswith("_")},
                **({"g_" + k: v for k, v in vars(grant).items() if not k.startswith("_")}
                   if grant is not None else {"g_id": None}),
            }
            result.append(values)
            remaining.discard(row.redis_consumer_id)
            if not remaining or depth == sentinel or fork:
                break
            children = sorted(
                self.incoming(row), key=lambda r: r.id
            )[:2]
            row = children[0] if children else None
            fork = len(children) > 1
            depth += 1
        self.selected.append(len(result))
        return self.corrupt_result(result)


@pytest.mark.asyncio
async def test_capture_reaches_old_redis_identity_through_absent_intermediates():
    capture = capture_module()
    payload = history_document()
    connection, client = HistoryConnection(payload), RedisInventory(payload)
    for inventory in client.inventory.values():
        inventory["consumers"] = [inventory["consumers"][0], inventory["consumers"][-1]]
    snapshot, baseline = snapshots(payload)
    result = await capture.capture_history(connection, client, snapshot, baseline)
    assert result == {w.current.service_name: w.retiring for w in decode(payload).workers}
    assert len(client.calls) == 6  # Recheck the same three inventories after DB capture.
    assert connection.selected == [4, 4, 2, 4]
    assert connection.peak_transactions == 1


@pytest.mark.asyncio
async def test_capture_stops_at_oldest_required_not_all_250_generations():
    capture = capture_module()
    payload = history_document(250)
    connection, client = HistoryConnection(payload), RedisInventory(payload)
    for inventory in client.inventory.values():
        inventory["consumers"] = inventory["consumers"][:4]
    result = await capture.capture_history(connection, client, *snapshots(payload))
    assert [len(pins) for pins in result.values()] == [3, 3, 1, 3]
    assert connection.selected == [4, 4, 2, 4]


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [
    "broken", "fork", "cycle", "missing_current", "missing_grant", "current_changed",
    "baseline_changed", "baseline_not_direct", "grant_changed", "grant_endpoint",
    "foreign", "epoch", "generation", "duplicate", "unknown", "missing_current_consumer",
    "duplicate_consumer", "malformed_consumer", "inventory_overflow", "chain_overflow",
])
async def test_capture_refuses_unproven_history(fault):
    capture = capture_module()
    payload = history_document(65 if fault == "chain_overflow" else 3)
    connection, client = HistoryConnection(payload), RedisInventory(payload)
    snapshot, baseline = snapshots(payload)
    old_id = UUID(payload["workers"][0]["ancestors"][0]["registration_id"])
    old = next(r for r in connection.registrations if r.id == old_id)
    grant = next(g for g in connection.grants if g.id == old.grant_id)
    consumers = client.inventory[payload["workers"][0]["current"]["service_name"]]["consumers"]
    if fault == "broken":
        old.superseded_by = None
    elif fault == "fork":
        duplicate = copy.copy(old)
        duplicate.id = UUID(int=999999)
        connection.registrations.append(duplicate)
    elif fault == "cycle":
        old.superseded_by = old.id
    elif fault == "missing_current":
        connection.registrations.pop(0)
    elif fault == "missing_grant":
        connection.grants.remove(grant)
    elif fault == "current_changed":
        connection.registrations[0].worker_instance_id = UUID(int=998)
    elif fault == "baseline_changed":
        baseline["workers"][0]["database_fingerprint"] = "e" * 64
    elif fault == "baseline_not_direct":
        baseline["workers"][0] = payload["workers"][0]["ancestors"][0]
    elif fault == "grant_changed":
        grant.release_commit = "e" * 40
    elif fault == "grant_endpoint":
        grant.endpoint_bindings_json = {}
    elif fault == "foreign":
        old.worker_host = "foreign"
    elif fault == "epoch":
        old.lease_epoch = 1000
    elif fault == "generation":
        grant.generation = 1000
    elif fault == "duplicate":
        connection.corrupt_result = lambda rows: rows + rows[-1:]
    elif fault == "unknown":
        consumers.append({"name": "unknown", "idle": 200000, "pending": 0})
    elif fault == "missing_current_consumer":
        consumers.pop(0)
    elif fault == "duplicate_consumer":
        consumers.append(consumers[-1])
    elif fault == "malformed_consumer":
        consumers[-1]["name"] = 1
    elif fault == "inventory_overflow":
        consumers.extend({"name": f"unknown-{i}"} for i in range(66))
    elif fault == "chain_overflow":
        for inventory in client.inventory.values():
            inventory["consumers"] = [inventory["consumers"][0], inventory["consumers"][-1]]
    with pytest.raises(job.ProtocolError, match="^registered_reconcile_protocol_failed$"):
        await capture.capture_history(connection, client, snapshot, baseline)
    assert connection.transactions == 0


@pytest.mark.asyncio
async def test_capture_includes_absent_baseline_and_allows_captured_absence():
    capture = capture_module()
    payload = history_document(0)
    connection, client = HistoryConnection(payload), RedisInventory(payload)
    result = await capture.capture_history(connection, client, *snapshots(payload))
    assert [len(pins) for pins in result.values()] == [0, 0, 1, 0]
    assert connection.selected == [1, 1, 2, 1]


def test_build_capture_pins_keeps_exact_baseline_and_complete_v2_history():
    payload = history_document()
    pins = decode(payload)
    result = job.build_capture_pins(
        *snapshots(payload), transaction_id=pins.transaction_id, revision=pins.revision,
        release_commit=pins.release_commit,
        history={w.current.service_name: w.retiring for w in pins.workers},
    )
    assert result["pin_json"] == pins.canonical_json
    assert result["pin_sha256"] == pins.sha256


@pytest.mark.parametrize("fault", ["missing_service", "extra_service", "not_direct", "missing_old", "foreign"])
def test_build_capture_pins_rejects_history_outside_baseline(fault):
    payload = history_document()
    pins = decode(payload)
    history = {w.current.service_name: w.retiring for w in pins.workers}
    service = pins.workers[0].current.service_name
    if fault == "missing_service":
        history.pop(service)
    elif fault == "extra_service":
        history["other"] = ()
    elif fault == "not_direct":
        history[service] = history[service][1:]
    elif fault == "missing_old":
        history[service] = ()
    else:
        history[service] = history[pins.workers[1].current.service_name]
    with pytest.raises(job.ProtocolError):
        job.build_capture_pins(
            *snapshots(payload), transaction_id=pins.transaction_id, revision=pins.revision,
            release_commit=pins.release_commit, history=history,
        )


@pytest.mark.asyncio
async def test_capture_refuses_inventory_identity_change_during_db_read():
    capture = capture_module()
    payload = history_document()
    connection, client = HistoryConnection(payload), RedisInventory(payload)
    original = client.xinfo_consumers

    async def changed(stream, group):
        rows = await original(stream, group)
        if len(client.calls) > 3:
            rows[-1]["name"] = "changed"
        return rows

    client.xinfo_consumers = changed
    with pytest.raises(job.ProtocolError):
        await capture.capture_history(connection, client, *snapshots(payload))


@pytest.mark.asyncio
async def test_capture_accepts_exact_64_boundary_without_truncation():
    capture = capture_module()
    payload = history_document(64)
    connection, client = HistoryConnection(payload), RedisInventory(payload)
    history = await capture.capture_history(connection, client, *snapshots(payload))
    assert history == {w.current.service_name: w.retiring for w in decode(payload).workers}
    assert connection.selected == [65, 65, 2, 65]


def restart_document():
    payload = history_document()
    newer = payload["workers"][0]["predecessor"]
    older = payload["workers"][0]["ancestors"][0]
    for field in (
        "grant_id", "generation", "service_name", "worker_type", "worker_host", "worker_slot",
        "capabilities", "release_commit", "image_identity", "database_principal",
        "database_fingerprint", "redis_fingerprint", "storage_fingerprint",
    ):
        older[field] = copy.deepcopy(newer[field])
    older["registered_at"] = (
        datetime.fromisoformat(newer["registered_at"]) - timedelta(seconds=6)
    ).isoformat()
    return payload


class RestartConnection(HistoryConnection):
    """Model the specified DB edge predicate; parent tests execute the real SQL."""

    def __init__(self, payload, *, explicit=False):
        super().__init__(payload)
        old_id = UUID(payload["workers"][0]["ancestors"][0]["registration_id"])
        self.older = next(row for row in self.registrations if row.id == old_id)
        self.newer = next(row for row in self.registrations if row.id == self.older.superseded_by)
        if not explicit:
            self.older.superseded_by = None
        self.older.revoke_reason = "worker_redis_continuity_unready"
        self.older.revoked_at = self.newer.registered_at - timedelta(seconds=1)

    def incoming(self, newer):
        return [
            older for older in self.registrations
            if older.superseded_by == newer.id or (
                older.superseded_by is None
                and older.status == "revoked"
                and older.revoke_reason == "worker_redis_continuity_unready"
                and older.grant_id == newer.grant_id
                and older.service_name == newer.service_name
                and older.lease_epoch + 1 == newer.lease_epoch
                and older.registered_at < newer.registered_at
                and older.revoked_at is not None
                and older.registered_at <= older.revoked_at <= newer.registered_at
            )
        ]


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit", [False, True])
async def test_capture_proves_retiring_restart_and_collapses_identical_grant_facts(explicit):
    payload = restart_document()
    connection, client = RestartConnection(payload, explicit=explicit), RedisInventory(payload)
    inventory = client.inventory[payload["workers"][0]["current"]["service_name"]]
    inventory["consumers"] = [inventory["consumers"][0], inventory["consumers"][-1]]
    history = await capture_module().capture_history(connection, client, *snapshots(payload))
    pins = decode(payload)
    assert history == {worker.current.service_name: worker.retiring for worker in pins.workers}
    assert connection.selected == [4, 4, 2, 4]
    result = job.build_capture_pins(
        *snapshots(payload), transaction_id=pins.transaction_id,
        revision=pins.revision, release_commit=pins.release_commit, history=history,
    )
    assert result["pin_json"] == pins.canonical_json


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [
    "reason", "nonnull_wrong_link", "epoch_gap", "different_grant", "registered_equal",
    "revoked_before", "revoked_after", "revoked_missing", "changed_identity",
    "fork", "different_repeated_fact",
])
async def test_capture_restart_bridge_remains_fail_closed(fault):
    payload = restart_document()
    connection, client = RestartConnection(payload), RedisInventory(payload)
    older, newer = connection.older, connection.newer
    if fault == "reason":
        older.revoke_reason = "superseded"
    elif fault == "nonnull_wrong_link":
        older.superseded_by = UUID(payload["workers"][0]["current"]["registration_id"])
    elif fault == "epoch_gap":
        older.lease_epoch -= 1
    elif fault == "different_grant":
        older.grant_id = UUID(int=999999)
    elif fault == "registered_equal":
        older.registered_at = newer.registered_at
    elif fault == "revoked_before":
        older.revoked_at = older.registered_at - timedelta(microseconds=1)
    elif fault == "revoked_after":
        older.revoked_at = newer.registered_at + timedelta(microseconds=1)
    elif fault == "revoked_missing":
        older.revoked_at = None
    elif fault == "changed_identity":
        older.storage_fingerprint = "f" * 64
    elif fault == "fork":
        fork = copy.copy(older)
        fork.id = UUID(int=999999)
        fork.superseded_by = newer.id
        connection.registrations.append(fork)
    else:
        def changed(rows):
            for row in rows:
                if row["r_id"] == older.id:
                    row["g_activated_at"] += timedelta(seconds=1)
            return rows

        connection.corrupt_result = changed
    with pytest.raises(job.ProtocolError):
        await capture_module().capture_history(connection, client, *snapshots(payload))
