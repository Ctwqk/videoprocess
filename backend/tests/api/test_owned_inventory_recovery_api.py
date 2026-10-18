from __future__ import annotations

import copy
import uuid
from datetime import timedelta

import pytest
from sqlalchemy import select

from app.models.channel_agent import ChannelProfile, ManualSeed, ProductionTask
from app.models.owned_seed_inventory import OwnedSeedInventory, OwnedSeedInventoryItem
from app.services import owned_seed_inventory as inv
from app.services import owned_seed_inventory_history as history
from tests.api.test_owned_seed_inventory import inventory_env as inventory_env, approval
from tests.api.test_owned_seed_inventory_history_api import history_env as history_env


@pytest.fixture
async def recovery_env(history_env, monkeypatch):
    from app.services import owned_inventory_recovery as recovery

    h = history_env
    from tests.services.test_owned_inventory_recovery import registrations
    runtime = registrations(h.controls.observed_at)
    for table, values in runtime.items():
        # Snapshot transport is the only database double in this SQLite suite.
        h.rows[table].extend(values)
    h.data["starts_at"] = (h.controls.observed_at - timedelta(hours=1)).isoformat()
    h.data["expires_at"] = (h.controls.observed_at + timedelta(days=7) - timedelta(hours=1)).isoformat()
    created = await h.env.client.post(h.env.url, json=h.data)
    assert created.status_code == 200, created.text
    h.result = created.json()
    approved = await h.env.client.post(f"{h.env.url}/{h.result['id']}/approve", json=approval(h.result))
    assert approved.status_code == 200, approved.text
    h.approved = approved.json()
    async with h.env.factory() as db:
        row = await db.get(OwnedSeedInventory, uuid.UUID(h.result["id"]))
        channel = await db.get(ChannelProfile, h.env.channel_id)
        row.state, row.hold_reason = "held", "redis_consumer_identity_invalid"
        channel.intake_pause_reason = "redis_consumer_identity_invalid"
        await db.commit()
        await db.refresh(row)
        await db.refresh(channel)
        h.request = dict(client_request_id=str(uuid.uuid4()), manifest_sha256=row.manifest_sha256,
            expected_hold_sha256=recovery.hold_fingerprint(row, channel), recovery_reference="incident:fixed-consumer",
            operator_runtime_evidence={"reference": "parent-native-deploy-verified", "sha256": "a" * 64,
                "attestation": "Existing native deployment, services and 127/150 placement verified; 126 excluded."})
    async def observe():
        from tests.services.test_owned_inventory_recovery import redis_rows
        return [{"stream": stream, "group": group, "consumer": name, "idle_ms": 0, "pending": 0, "lag": 0}
                for stream, (group, name) in redis_rows().items()]
    monkeypatch.setattr(recovery, "observe_runtime_redis", observe)
    return h


async def test_recovery_preparation_rechecks_sources_and_history_without_changing_approval(recovery_env):
    from app.schemas.owned_inventory_recovery import OwnedInventoryRecovery
    from app.services import owned_inventory_recovery as recovery

    h = recovery_env
    reads, manager_reads = len(h.env.storage.reads), len(h.requests)
    async with h.env.factory() as db:
        prepared = await recovery.prepare_recovery(db, h.env.channel_id, uuid.UUID(h.result["id"]),
            OwnedInventoryRecovery.model_validate(h.request), "test-operator")
        assert prepared.evidence["operator_runtime_evidence"]["verification"] == "authenticated_operator_attestation"
        assert prepared.inventory.state == "held"
        assert prepared.inventory.approval_reference == h.approved["approval_reference"]
        assert prepared.inventory.manifest_json == h.approved["manifest"]
        assert prepared.channel.intake_paused_at is not None
        assert db.in_transaction()
        await db.rollback()
    assert len(h.env.storage.reads) == reads + 7
    assert len(h.requests) > manager_reads


@pytest.mark.parametrize("fault", ["manifest", "source", "seed", "halted", "pause", "item", "task", "unknown_upload", "registration_changed", "changed_during_io", "stale"])
async def test_recovery_preparation_refuses_drift_and_preserves_hold(recovery_env, fault):
    from app.schemas.owned_inventory_recovery import OwnedInventoryRecovery
    from app.services import owned_inventory_recovery as recovery

    h = recovery_env
    request = copy.deepcopy(h.request)
    if fault == "manifest":
        request["manifest_sha256"] = "f" * 64
    elif fault == "source":
        key = next(iter(h.env.storage.blobs))
        h.env.storage.blobs[key] = b"unapproved-source"
    elif fault == "unknown_upload":
        operation = copy.deepcopy(h.rows["youtube_upload_operations"][0])
        operation.update(id=str(uuid.uuid4()), production_task_id=None, status="uncertain")
        h.rows["youtube_upload_operations"].append(operation)
    elif fault == "registration_changed":
        async def replace_registration(_path):
            h.env.storage.on_read = None
            h.rows["worker_registrations"][-1]["lease_epoch"] += 1
        h.env.storage.on_read = replace_registration
    elif fault in {"seed", "halted", "pause", "item", "task"}:
        async with h.env.factory() as db:
            channel = await db.get(ChannelProfile, h.env.channel_id)
            item = (await db.scalars(select(OwnedSeedInventoryItem))).first()
            if fault == "seed":
                seed = await db.get(ManualSeed, item.manual_seed_id)
                seed.status = "inventory_revoked"
            elif fault == "halted":
                channel.halted_at = h.controls.observed_at
            elif fault == "pause":
                channel.intake_pause_reason = "operator_stop"
            elif fault == "item":
                item.completed_at = h.controls.observed_at
            else:
                db.add(ProductionTask(channel_profile_id=h.env.channel_id, target_account_id=uuid.UUID(h.env.scope["target_account_id"]),
                    manual_seed_id=item.manual_seed_id, prompt="unexpected work", state="selected"))
            await db.commit()
    elif fault == "changed_during_io":
        async def change(_path):
            h.env.storage.on_read = None
            async with h.env.factory() as writer:
                row = await writer.get(OwnedSeedInventory, uuid.UUID(h.result["id"]))
                row.hold_reason = "different_incident"
                await writer.commit()
        h.env.storage.on_read = change
    else:
        async def drift_time(_path):
            h.env.storage.on_read = None
            h.controls.observed_at += timedelta(seconds=61)
        h.env.storage.on_read = drift_time
    async with h.env.factory() as db:
        with pytest.raises((inv.OwnedInventoryError, history.OwnedHistoryError)):
            await recovery.prepare_recovery(db, h.env.channel_id, uuid.UUID(h.result["id"]),
                OwnedInventoryRecovery.model_validate(request), "test-operator")
        await db.rollback()
        row = await db.get(OwnedSeedInventory, uuid.UUID(h.result["id"]))
        assert row.state == "held"
        assert row.manifest_json == h.approved["manifest"]


async def test_recovery_endpoint_requires_operator_before_work(inventory_env):
    env = inventory_env
    response = await env.client.post(f"{env.url}/{uuid.uuid4()}/recover-operational-hold", headers={"Authorization": ""}, json={})
    assert response.status_code == 403
