from __future__ import annotations

import copy
import uuid
from datetime import timedelta

import pytest
from pydantic import ValidationError

from app.services import owned_seed_inventory as inv


def redis_rows():
    return {
        "vp:tasks:ffmpeg_go": ("ffmpeg_go-workers", "ffmpeg_go-worker@colima-127:1:10000000-0000-0000-0000-000000000001"),
        "vp:tasks:ffmpeg": ("ffmpeg-workers", "ffmpeg-worker@150-gpu:1:10000000-0000-0000-0000-000000000002"),
        "vp:tasks:vision": ("vision-workers", "vision-worker@150-vision:1:10000000-0000-0000-0000-000000000003"),
        "vp:tasks:youtube_publisher": ("youtube_publisher-workers", "youtube_publisher-worker@150-publisher:1:10000000-0000-0000-0000-000000000004"),
        "vp:events": ("orchestrator", "orchestrator-api-12"),
    }


def registrations(now):
    rows = {"worker_admission_grants": [], "worker_registrations": []}
    for index, (stream, (group, consumer)) in enumerate(redis_rows().items()):
        if stream == "vp:events":
            continue
        worker_type = stream.removeprefix("vp:tasks:")
        host = consumer.split("@", 1)[1].split(":", 1)[0]
        grant_id, registration_id = str(uuid.UUID(int=9000 + index)), str(uuid.UUID(int=9500 + index))
        common = dict(service_name="live-" + worker_type, worker_type=worker_type, worker_host=host,
            image_identity="vp-python-worker:deploy-aaaaaaaaaaaa", database_principal="qualified-" + worker_type, capabilities_json=[worker_type])
        rows["worker_admission_grants"].append(dict(common, id=grant_id, state="active", revoked_at=None, revoke_reason=None,
            generation=1, release_commit="a" * 40, activated_at=(now - timedelta(minutes=1)).isoformat(),
            redis_stream=stream, redis_group=group, endpoint_bindings_json={}))
        rows["worker_registrations"].append(dict(common, id=registration_id, grant_id=grant_id, status="active", revoked_at=None,
            revoke_reason=None, superseded_by=None, worker_slot=1, worker_instance_id=consumer.rsplit(":", 1)[1],
            redis_consumer_id=consumer, lease_epoch=1, registered_at=(now - timedelta(minutes=1)).isoformat(),
            heartbeat_at=now.isoformat(), lease_expires_at=(now + timedelta(minutes=5)).isoformat()))
    return rows


class Redis:
    def __init__(self):
        self.groups = {stream: [{"name": group, "pending": 0, "lag": 0}] for stream, (group, _) in redis_rows().items()}
        self.consumers = {stream: [{"name": consumer, "pending": 0, "idle": 120000}] for stream, (_, consumer) in redis_rows().items()}
        self.principal = "qualified-control"
        self.closed = False

    async def acl_whoami(self):
        return self.principal

    async def xinfo_groups(self, stream):
        return copy.deepcopy(self.groups[stream])

    async def xinfo_consumers(self, stream, group):
        assert redis_rows()[stream][0] == group
        return copy.deepcopy(self.consumers[stream])

    async def aclose(self):
        self.closed = True


@pytest.mark.asyncio
async def test_recovery_reads_all_normal_groups_with_named_identity(monkeypatch):
    from app.services import owned_inventory_recovery as recovery

    client = Redis()
    monkeypatch.setattr(inv, "_history_redis_url", lambda: "redis://qualified-control:secret@redis.invalid:6380/0")
    monkeypatch.setattr(inv, "_history_redis", lambda url: client)
    evidence = await recovery.observe_runtime_redis()
    assert {entry["stream"] for entry in evidence} == set(redis_rows())
    assert all(entry["pending"] == 0 and entry["lag"] == 0 for entry in evidence)
    assert client.closed
    assert "secret" not in str(evidence)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing", "pending", "lag", "lag_unknown", "duplicate_group", "none", "two", "stale", "wrong_slot", "zero_uuid", "pending_consumer", "bool_idle", "identity", "default"])
async def test_recovery_refuses_any_unqualified_consumer_or_backlog(monkeypatch, fault):
    from app.services import owned_inventory_recovery as recovery

    client = Redis()
    stream = "vp:tasks:youtube_publisher"
    group, consumer = client.groups[stream][0], client.consumers[stream][0]
    if fault == "missing":
        client.groups[stream] = []
    elif fault in {"pending", "lag"}:
        group[fault] = 1
    elif fault == "lag_unknown":
        group["lag"] = None
    elif fault == "duplicate_group":
        client.groups[stream].append(copy.deepcopy(group))
    elif fault == "none":
        client.consumers[stream] = []
    elif fault == "two":
        client.consumers[stream].append(copy.deepcopy(consumer))
    elif fault == "stale":
        consumer["idle"] = 120001
    elif fault == "wrong_slot":
        consumer["name"] = consumer["name"].replace("150-publisher", "126-publisher")
    elif fault == "zero_uuid":
        consumer["name"] = "youtube_publisher-worker@150-publisher:1:00000000-0000-0000-0000-000000000000"
    elif fault == "pending_consumer":
        consumer["pending"] = 1
    elif fault == "bool_idle":
        consumer["idle"] = False
    elif fault == "identity":
        client.principal = "different-control"
    monkeypatch.setattr(inv, "_history_redis_url", lambda: "redis://" + ("default" if fault == "default" else "qualified-control") + ":secret@redis.invalid:6380/0")
    monkeypatch.setattr(inv, "_history_redis", lambda url: client)
    with pytest.raises(inv.OwnedInventoryError):
        await recovery.observe_runtime_redis()


def test_recovery_request_rejects_authority_expansion():
    from app.schemas.owned_inventory_recovery import OwnedInventoryRecovery

    request = dict(client_request_id="10000000-0000-0000-0000-000000000001", manifest_sha256="a" * 64,
        expected_hold_sha256="b" * 64, recovery_reference="incident:diagnosed-redis-registration",
        operator_runtime_evidence={"reference": "native-deployment:verified", "sha256": "c" * 64,
            "attestation": "Native deployment, services and approved 127/150 placement verified; host 126 excluded."})
    assert OwnedInventoryRecovery.model_validate(request).operator_runtime_evidence.sha256 == "c" * 64
    for key, value in {"approval_reference": "replacement", "expires_at": "2099-01-01T00:00:00Z", "privacy": "public", "force": True}.items():
        with pytest.raises(ValidationError):
            OwnedInventoryRecovery.model_validate({**request, key: value})


def test_original_window_is_required_at_the_recovery_boundary():
    from app.services import owned_inventory_recovery as recovery
    from tests.services.test_owned_seed_inventory_history import NOW

    inventory = dict(state="held", hold_reason="redis_consumer_identity_invalid", approved_at=NOW - timedelta(hours=1),
        approved_by="original", approval_reference="standing", revoked_at=None, revoked_by=None,
        succession_released_at=None, starts_at=NOW - timedelta(hours=2), expires_at=NOW + timedelta(days=7) - timedelta(hours=2),
        manifest_json={"version": 2}, privacy="unlisted", max_admissions=7, minimum_interval_seconds=86400)
    recovery.require_recoverable_state(inventory, NOW)
    for change in ({"state": "approved"}, {"hold_reason": "owned_inventory_pds_denied"}, {"revoked_at": NOW},
                   {"succession_released_at": NOW}, {"approved_by": ""}, {"privacy": "public"},
                   {"expires_at": NOW}, {"starts_at": NOW + timedelta(seconds=1)}, {"minimum_interval_seconds": 1}):
        with pytest.raises(inv.OwnedInventoryError):
            recovery.require_recoverable_state({**inventory, **change}, NOW)


@pytest.mark.parametrize("fault", [None, "consumer", "expired", "revoked", "grant", "instance", "two", "wrong_image"])
def test_runtime_observation_requires_exact_live_registration_and_grant(fault):
    from app.services import owned_inventory_recovery as recovery
    from tests.services.test_owned_seed_inventory_history import NOW

    rows = registrations(NOW)
    evidence = [{"stream": stream, "group": group, "consumer": name, "idle_ms": 0, "pending": 0, "lag": 0}
                for stream, (group, name) in redis_rows().items()]
    row = rows["worker_registrations"][0]
    if fault == "consumer":
        row["redis_consumer_id"] = row["redis_consumer_id"].replace("000000000001", "000000000009")
    elif fault == "expired":
        row["lease_expires_at"] = NOW.isoformat()
    elif fault == "revoked":
        row["revoked_at"] = NOW.isoformat()
    elif fault == "grant":
        rows["worker_admission_grants"][0]["state"] = "revoked"
    elif fault == "instance":
        row["worker_instance_id"] = str(uuid.UUID(int=33))
    elif fault == "two":
        rows["worker_registrations"].append(copy.deepcopy(row))
    elif fault == "wrong_image":
        row["image_identity"] = "different-worker"
    if fault is None:
        result = recovery.bind_runtime_registrations(rows, evidence, NOW)
        assert len(result) == 4
        assert result[0]["registration_id"] == str(uuid.UUID(int=9500))
    else:
        with pytest.raises(inv.OwnedInventoryError):
            recovery.bind_runtime_registrations(rows, evidence, NOW)
