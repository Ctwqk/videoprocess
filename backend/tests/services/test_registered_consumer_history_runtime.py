"""Versioned SQL routing must retain every pinned historical identity."""
from types import SimpleNamespace
from uuid import UUID
from dataclasses import replace
import json

import pytest

from app.services import registered_consumer_reconcile_runtime as runtime
from tests.services.registered_consumer_history_fixtures import history_document, history_facts
from tests.services.test_registered_consumer_reconcile import NOW, decode
from tests.services.test_registered_consumer_reconcile_runtime import mounted_credentials as mounted_credentials


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [1, 2])
async def test_read_guard_routes_version_and_keeps_all_retiring_ids(monkeypatch, version):
    current = SimpleNamespace(registration_id=UUID(int=1))
    predecessor = SimpleNamespace(registration_id=UUID(int=2))
    ancestor = SimpleNamespace(registration_id=UUID(int=3))
    retiring = (predecessor, ancestor) if version == 2 else (predecessor,)
    worker = SimpleNamespace(current=current, predecessor=predecessor, retiring=retiring)
    pins = SimpleNamespace(version=version, workers=(worker,))
    request = SimpleNamespace(control_generation="history-test", pins=pins)
    seen = []
    rows = object()

    class Connection:
        async def fetch(self, query, *arguments):
            seen.append((query, arguments))
            return rows

    sentinel = object()

    def decode(actual, actual_pins):
        assert actual is rows and actual_pins is pins
        return sentinel

    monkeypatch.setattr(runtime, "decode_guard", decode)
    assert await runtime.read_guard(Connection(), request) is sentinel
    name = "vp_registered_consumer_reconcile_history_guard" if version == 2 else "vp_registered_consumer_reconcile_guard"
    assert seen == [(
        f"SELECT * FROM public.{name}($1::text,$2::uuid[],$3::uuid[])",
        ("history-test", [current.registration_id], [pin.registration_id for pin in retiring]),
    )]


def history_rows(payload):
    registrations, grants = history_facts(payload)
    by_grant = {row.id: row for row in grants}
    return [
        {
            "observed_at": NOW,
            "registration_id": row.id,
            "grant_id": row.grant_id,
            "worker_instance_id": row.worker_instance_id,
            "superseded_by": row.superseded_by,
            "registered_at": row.registered_at,
            "lease_expires_at": row.lease_expires_at,
            "registration_revoked_at": row.revoked_at,
            "grant_activated_at": by_grant[row.grant_id].activated_at,
            "grant_revoked_at": by_grant[row.grant_id].revoked_at,
            "registration_facts": json.dumps({name: getattr(row, name) for name in runtime._REGISTRATION_FIELDS}),
            "grant_facts": json.dumps({name: getattr(by_grant[row.grant_id], name) for name in runtime._GRANT_FIELDS}),
        }
        for row in registrations
    ]


def test_v2_decoder_accepts_complete_history_and_refuses_truncation_or_padding():
    payload = history_document()
    pins = decode(payload)
    rows = history_rows(payload)
    assert len(rows) > 8
    assert len(runtime.decode_guard(rows, pins).registrations) == len(rows)
    for changed in (rows[:-1], rows + rows[:1]):
        with pytest.raises(runtime.ReconcileRuntimeError, match="guard_facts_invalid"):
            runtime.decode_guard(changed, pins)


@pytest.mark.parametrize("endpoint", ["database", "redis"])
def test_mounted_v2_credentials_validate_oldest_ancestor_endpoint(mounted_credentials, endpoint):
    request, paths = mounted_credentials
    payload = history_document()
    payload["workers"][0]["ancestors"][-1][f"{endpoint}_fingerprint"] = "f" * 64
    request = replace(request, pins=decode(payload))
    paths["pins"].chmod(0o600)
    paths["pins"].write_text(request.pins.canonical_json)
    paths["pins"].chmod(0o400)
    with pytest.raises(runtime.ReconcileRuntimeError, match="credential_endpoint_changed"):
        runtime.load_credentials(request)
