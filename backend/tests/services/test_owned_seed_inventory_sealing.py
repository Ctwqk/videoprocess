"""Actual qualifier code with explicit offline storage/Redis transports only."""
from __future__ import annotations

import copy
import hashlib
from datetime import timedelta
from types import SimpleNamespace

import pytest
from redis.asyncio.retry import Retry as AsyncRetry
from redis.exceptions import ConnectionError as RedisConnectionError

from app.services import owned_seed_inventory as service
from app.services import owned_seed_inventory_history as history
from test_owned_seed_inventory_history import NOW, retired_rows, snap


@pytest.fixture
def sealing(monkeypatch):
    rows, observations = retired_rows()
    rows["owned_seed_inventories"] = []
    calls = []
    controls = SimpleNamespace(marker_bad=False, pending=False, read_error=False)
    markers = {"vp:worker-task-dispatch:" + r["dispatch_key"]: r["marker_message_id"]
               for r in observations if r["kind"] == "task"}

    class Redis:
        async def get(self, key):
            calls.append(("get", key))
            if controls.read_error:
                raise RuntimeError("private-sentinel")
            return "9999-0" if controls.marker_bad else markers[key]

        async def xpending_range(self, stream, group, min, max, count):
            calls.append(("pending", stream, group, min, max, count))
            return [{"message_id": min}] if controls.pending else []

        async def aclose(self):
            calls.append(("close",))

    class Storage:
        async def read_bounded(self, path, limit):
            calls.append(("read", path, limit))
            return b"a" * 100

    monkeypatch.setattr(service, "_history_redis", lambda: Redis(), raising=False)
    monkeypatch.setattr(service.storage_manager, "get_storage", lambda *args, **kw: Storage())
    return SimpleNamespace(rows=rows, observations=observations, calls=calls, controls=controls)


async def test_retirement_uses_native_full_graph_storage_and_exact_readonly_redis(sealing):
    h = sealing
    before = copy.deepcopy(h.rows)
    sources = service._retirement_sources(snap(h.rows), requested=True)
    observed = await service._observe_retirement(sources, observed_at=NOW)
    certificate = service._qualified_retirement(snap(h.rows), sources, observed, "operator", "draft:one")
    parsed = history.RetiredPreuploadCertificate.parse(certificate)
    assert parsed.retained_facts.source_assets[0].content_sha256 == hashlib.sha256(b"a" * 100).hexdigest()
    assert parsed.retained_facts.account.as_dict()["platform_account_id"] == ""
    assert "canonical_platform_channel_id" not in certificate
    retry = next(d for d in certificate["terminal_graph"]["worker_task_dispatches"] if d["id"].startswith("3546"))
    assert retry["cancelled_at"] is None and retry["delivered_at"]
    assert len([c for c in h.calls if c[0] == "get"]) == 4
    assert len([c for c in h.calls if c[0] == "pending"]) == 6
    assert h.calls[-1] == ("close",) and h.rows == before
    assert {c[0] for c in h.calls} == {"read", "get", "pending", "close"}


@pytest.mark.parametrize("bad", ["marker_bad", "pending", "read_error"])
async def test_retirement_redis_disagreement_is_static_no_retry_and_closes(sealing, bad):
    h = sealing
    setattr(h.controls, bad, True)
    sources = service._retirement_sources(snap(h.rows), requested=True)
    with pytest.raises(service.OwnedInventoryError, match="^owned_inventory_retirement_") as error:
        await service._observe_retirement(sources, observed_at=NOW)
    assert "private-sentinel" not in str(error.value)
    assert h.calls[-1] == ("close",)
    assert len([c for c in h.calls if c[0] == "get"]) == 1


@pytest.mark.parametrize("bad", ["operation", "new_member", "stale", "future"])
async def test_retirement_reload_and_time_drift_cannot_be_sealed(sealing, bad):
    h = sealing
    sources = service._retirement_sources(snap(h.rows), requested=True)
    observed = await service._observe_retirement(sources, observed_at=NOW)
    at = NOW
    if bad == "operation":
        h.rows["youtube_upload_operations"][0]["manager_task_id"] = "changed"
    elif bad == "new_member":
        row = copy.deepcopy(h.rows["worker_task_dispatches"][0])
        row["id"] = "aaaaaaaa-0000-0000-0000-000000000001"
        h.rows["worker_task_dispatches"].append(row)
    else:
        at += timedelta(seconds=61 if bad == "stale" else -1)
    with pytest.raises((service.OwnedInventoryError, history.OwnedHistoryError)):
        service._qualified_retirement(snap(h.rows, observed_at=at), sources, observed, "operator", "draft:one")


async def test_existing_revoked_certificate_is_reobserved_not_replaced(sealing):
    h = sealing
    sources = service._retirement_sources(snap(h.rows), requested=True)
    observed = await service._observe_retirement(sources, observed_at=NOW)
    certificate = service._qualified_retirement(snap(h.rows), sources, observed, "operator", "draft:one")
    original_rows, _ = retired_rows()
    row = original_rows["owned_seed_inventories"][0]
    row["state"] = "revoked"
    row["manifest_json"]["legacy_history"]["retired_unassigned_preupload"] = certificate
    row["manifest_sha256"] = history.history_sha256(row["manifest_json"])
    h.rows["owned_seed_inventories"] = [row]
    later = NOW + timedelta(seconds=1)
    sources = service._retirement_sources(snap(h.rows, observed_at=later), requested=False)
    observed = await service._observe_retirement(sources, observed_at=later)
    result = service._qualified_retirement(snap(h.rows, observed_at=later), sources, observed, "new-operator", "draft:two")
    assert result == certificate


@pytest.mark.parametrize("table, field, value", [
    ("worker_registrations", "heartbeat_at", (NOW + timedelta(seconds=1)).isoformat()),
    ("worker_registrations", "lease_expires_at", (NOW + timedelta(minutes=1)).isoformat()),
    ("worker_registrations", "status", "expired"),
    ("worker_registrations", "revoked_at", NOW.isoformat()),
    ("worker_registrations", "revoke_reason", "release"),
    ("worker_registrations", "superseded_by", "aaaaaaaa-0000-0000-0000-000000000099"),
    ("worker_admission_grants", "state", "active"),
    ("worker_admission_grants", "revoked_at", NOW.isoformat()),
    ("worker_admission_grants", "revoke_reason", "release"),
    ("worker_admission_grants", "updated_at", (NOW + timedelta(seconds=1)).isoformat()),
])
async def test_retirement_reload_allows_only_a1_liveness_projection(sealing, table, field, value):
    h = sealing
    sources = service._retirement_sources(snap(h.rows), requested=True)
    observed = await service._observe_retirement(sources, observed_at=NOW)
    original = service._qualified_retirement(snap(h.rows), sources, observed, "operator", "draft:one")
    assert h.rows[table][0][field] != value
    h.rows[table][0][field] = value
    result = service._qualified_retirement(snap(h.rows), sources, observed, "operator", "draft:one")
    assert result == original


@pytest.mark.parametrize("table, field, value", [
    ("worker_registrations", "worker_id", "foreign-worker"),
    ("worker_registrations", "lease_epoch", 999),
    ("worker_registrations", "grant_id", "aaaaaaaa-0000-0000-0000-000000000099"),
    ("worker_admission_grants", "release_id", "foreign-release"),
    ("worker_task_dispatches", "payload_sha256", "f" * 64),
    ("registered_worker_event_receipts", "payload_sha256", "f" * 64),
    ("node_executions", "worker_id", "foreign-worker"),
    ("assets", "storage_path", "different-source.mp4"),
])
async def test_retirement_reload_still_rejects_identity_claim_effect_and_source_drift(sealing, table, field, value):
    h = sealing
    sources = service._retirement_sources(snap(h.rows), requested=True)
    observed = await service._observe_retirement(sources, observed_at=NOW)
    h.rows[table][0][field] = value
    with pytest.raises((service.OwnedInventoryError, history.OwnedHistoryError)):
        service._qualified_retirement(snap(h.rows), sources, observed, "operator", "draft:one")


async def test_no_retirement_does_not_build_external_clients(monkeypatch):
    def forbidden():
        pytest.fail("no external client is required")
    monkeypatch.setattr(service.channel_clients, "build_youtube_manager_client", forbidden)
    monkeypatch.setattr(service, "_history_redis", forbidden, raising=False)
    assert await service._observe_history_uploads({}, "UC" + "a" * 22) == ("", {})
    assert await service._observe_retirement(None, observed_at=NOW) is None


async def test_configured_redis_reader_uses_native_async_zero_retry_without_connecting(monkeypatch):
    monkeypatch.setattr(service.settings, "redis_url", "redis://synthetic-redis.invalid:6379/0")
    client = service._history_redis()
    try:
        options = client.connection_pool.connection_kwargs
        retry = options["retry"]
        assert isinstance(retry, AsyncRetry)
        assert options["socket_timeout"] == options["socket_connect_timeout"] == 5
        assert options["retry_on_timeout"] is False and options["health_check_interval"] == 0
        calls = []
        async def fail_once():
            calls.append("attempt")
            raise RedisConnectionError("synthetic transport failure")
        async def failed(error):
            calls.append("failure")
        with pytest.raises(RedisConnectionError):
            await retry.call_with_retry(fail_once, failed)
        assert calls == ["attempt", "failure"]
    finally:
        await client.aclose()
