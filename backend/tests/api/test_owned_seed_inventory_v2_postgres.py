"""Parent-only actual V2 approval on 042; A2's confirmed 041 anchor owns child DBs.

Uses the existing complete constrained retired+succeeded graph and real loader.
Storage, Manager GET and Redis observation are test doubles, never live services.
"""
import asyncio
import copy
import uuid

import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.channel_agent import ChannelProfile, ManualSeed
from app.models.owned_seed_inventory import OwnedSeedInventory
from app.services import owned_seed_inventory as service
from tests.api.test_owned_seed_inventory import approval
from tests.migrations.owned_history_postgres import a2_pg as a2_pg, migrate, wait_blocked
from tests.migrations.test_owned_history_seal_postgres import a2_env as a2_env
from tests.migrations.test_owned_history_lifecycle_postgres import a2_history as a2_history


HISTORY_DRIFT_FIELD = "credential_ref"


def signal_account_writer(monkeypatch, pids, entering):
    original = AsyncSession.scalars
    async def writer_entry(db, statement, *args, **kwargs):
        # Account PATCH takes the channel row lock before its history helper.
        if getattr(statement, "_for_update_arg", None) is not None and any(
            d.get("entity") is ChannelProfile for d in statement.column_descriptions
        ):
            pids["writer"] = await db.scalar(text("SELECT pg_backend_pid()"))
            entering.set()
        return await original(db, statement, *args, **kwargs)
    monkeypatch.setattr(AsyncSession, "scalars", writer_entry)


@pytest.fixture
async def v2_pg(a2_history):
    await migrate(a2_history.case.target, "042_owned_producer_fence")
    assert await a2_history.case.owner.fetchval("SELECT version_num FROM alembic_version") == "042_owned_producer_fence"
    owner = a2_history.case.owner
    schedule = await owner.fetchrow(
        "SELECT state, guarded_job_id FROM runtime_schedules WHERE service_name='videoprocess'")
    assert schedule is not None and schedule["guarded_job_id"] is None
    await owner.execute(
        "UPDATE runtime_schedules SET state='CLOSED', updated_by='v2-fixture' "
        "WHERE service_name='videoprocess' AND guarded_job_id IS NULL")
    assert await owner.fetchval("SELECT state FROM runtime_schedules WHERE service_name='videoprocess'") == "CLOSED"
    return a2_history


async def approve(h, data=None):
    return await h.env.client.post(f"{h.env.url}/{h.result['id']}/approve", json=data or approval(h.result))


async def assert_draft(h):
    async with h.case.sessions() as db:
        row = await db.get(OwnedSeedInventory, uuid.UUID(h.result["id"]))
        assert row.state == "draft" and row.approved_at is None
        assert row.manifest_json == h.result["manifest"]
        channel = await db.get(ChannelProfile, h.env.channel_id)
        assert channel.owned_seed_inventory_id is None and channel.tick_interval_minutes == 60
        seeds = await db.scalars(select(ManualSeed).where(ManualSeed.id.in_(
            [uuid.UUID(item["manual_seed_id"]) for item in h.result["items"]])))
        assert {seed.status for seed in seeds} == {"inventory_pending"}


async def test_pg_v2_real_approval_seals_history_and_replay_never_reactivates(v2_pg):
    h = v2_pg
    old_accounts = await h.case.owner.fetch("SELECT * FROM publishing_accounts ORDER BY id")
    old_operations = await h.case.owner.fetch("SELECT * FROM youtube_upload_operations ORDER BY id")
    response = await approve(h)
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["state"] == "approved" and result["manifest"] == h.result["manifest"]
    assert result["approved_by"] == "test-operator"
    assert {b["use"] for b in result["manifest"]["legacy_history"]["bindings"]} == {"history_only"}
    assert result["manifest"]["legacy_history"]["retired_unassigned_preupload"] is not None
    assert await h.case.owner.fetch("SELECT * FROM publishing_accounts ORDER BY id") == old_accounts
    assert await h.case.owner.fetch("SELECT * FROM youtube_upload_operations ORDER BY id") == old_operations
    channel = await h.case.owner.fetchrow("SELECT * FROM channel_profiles WHERE id=$1", h.env.channel_id)
    assert channel["owned_seed_inventory_id"] == uuid.UUID(result["id"])
    assert channel["tick_interval_minutes"] == 1 and channel["intake_paused_at"] is not None
    assert await h.case.owner.fetchval("SELECT state FROM runtime_schedules WHERE service_name='videoprocess'") == "CLOSED"
    revoked = await h.env.client.post(f"{h.env.url}/{result['id']}/revoke", json={
        "manifest_sha256": result["manifest_sha256"], "reason": "operator stop"})
    assert revoked.status_code == 200, revoked.text
    before = len(h.redis.calls), len(h.manager_calls), len(h.env.storage.reads)
    replay = await approve(h)
    assert replay.status_code == 200 and replay.json()["state"] == "revoked", replay.text
    assert replay.json()["approved_at"] == result["approved_at"]
    assert before == (len(h.redis.calls), len(h.manager_calls), len(h.env.storage.reads))


@pytest.mark.parametrize("same", [True, False])
async def test_pg_v2_two_initial_draft_readers_serialize_one_approval(v2_pg, monkeypatch, same):
    h = v2_pg
    original = service._observe_history_uploads
    both = asyncio.Event()
    count = 0
    async def synchronize(*args, **kwargs):
        nonlocal count
        result = await original(*args, **kwargs)
        count += 1
        if count == 2:
            both.set()
        await asyncio.wait_for(both.wait(), 8)
        return result
    monkeypatch.setattr(service, "_observe_history_uploads", synchronize)
    data = approval(h.result)
    if not same:
        data["approval_reference"] = "other reviewed request"
    responses = await asyncio.wait_for(asyncio.gather(approve(h), approve(h, data)), 20)
    assert count == 2
    assert sorted(r.status_code for r in responses) == ([200, 200] if same else [200, 409])
    if same:
        assert responses[0].json()["approved_at"] == responses[1].json()["approved_at"]
    else:
        assert next(r for r in responses if r.status_code == 409).json()["detail"] == "owned_inventory_approval_conflict"
    assert await h.case.owner.fetchval("SELECT count(*) FROM owned_seed_inventories WHERE approved_at IS NOT NULL") == 1


async def test_pg_v2_final_commit_failure_rolls_back_seal_pointer_and_seeds(v2_pg, monkeypatch):
    h = v2_pg
    original = AsyncSession.commit
    reached = []
    async def fail(db):
        if any(isinstance(row, OwnedSeedInventory) and row.approved_at is not None for row in db.dirty):
            await db.flush()
            reached.append(True)
            raise service.OwnedInventoryError("synthetic_commit_failure")
        await original(db)
    monkeypatch.setattr(AsyncSession, "commit", fail)
    response = await approve(h)
    assert response.status_code == 409 and reached == [True], response.text
    await assert_draft(h)


async def test_pg_v2_committed_history_drift_during_io_denies_approval(v2_pg):
    h = v2_pg
    before = copy.deepcopy(h.result)
    changed = []
    async def change(_path):
        h.env.storage.on_read = None
        old = uuid.UUID(h.data["history_locators"]["operations"][0]["legacy_account_id"])
        await h.case.owner.execute(f"UPDATE publishing_accounts SET {HISTORY_DRIFT_FIELD}=$2 WHERE id=$1",
                                   old, "changed during unlocked IO")
        changed.append(True)
    h.env.storage.on_read = change
    result = await approve(h)
    assert result.status_code == 409 and changed == [True], result.text
    await assert_draft(h)
    assert h.result == before


async def test_pg_v2_approval_holds_final_fence_until_history_is_sealed(v2_pg, monkeypatch):
    h = v2_pg
    locked, release, entering = asyncio.Event(), asyncio.Event(), asyncio.Event()
    pids = {}
    original = service._lock_assets
    old = h.data["history_locators"]["operations"][0]
    async def pause(db, descriptors):
        value = await original(db, descriptors)
        if not locked.is_set():
            pids["approval"] = await db.scalar(text("SELECT pg_backend_pid()"))
            locked.set()
            await asyncio.wait_for(release.wait(), 8)
        return value
    monkeypatch.setattr(service, "_lock_assets", pause)
    signal_account_writer(monkeypatch, pids, entering)
    activation = asyncio.create_task(approve(h))
    writer = None
    try:
        await asyncio.wait_for(locked.wait(), 5)
        writer = asyncio.create_task(h.env.client.patch(
            f"/api/v1/channel-agent/channels/{old['legacy_channel_profile_id']}/accounts/{old['legacy_account_id']}",
            json={"account_label": "must not commit"}))
        await asyncio.wait_for(entering.wait(), 5)
        await wait_blocked(h.case.owner, pids["writer"], pids["approval"])
        release.set()
        accepted, refused = await asyncio.wait_for(asyncio.gather(activation, writer), 10)
        assert accepted.status_code == 200, accepted.text
        assert refused.status_code == 409 and refused.json()["detail"] == "owned_inventory_historical_producer_pinned"
    finally:
        release.set()
        tasks = [task for task in (activation, writer) if task is not None]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("successor_fields", [False, True])
async def test_pg_v2_occupied_slot_is_never_released_for_another_scope(v2_pg, successor_fields):
    h = v2_pg
    result = await approve(h)
    assert result.status_code == 200, result.text
    data = await h.env.data("other-v2")
    data.update(version=2, history_locators=h.data["history_locators"])
    created = await h.env.client.post(h.env.url, json=data)
    assert created.status_code == 200, created.text
    other = created.json()
    request = approval(other)
    if successor_fields:
        request.update(predecessor_inventory_id=h.result["id"], predecessor_closeout_sha256="a" * 64)
    response = await h.env.client.post(f"{h.env.url}/{other['id']}/approve", json=request)
    assert response.status_code == 409, response.text
    assert response.json()["detail"] == ("owned_inventory_v2_successor_unsupported" if successor_fields
                                        else "owned_inventory_platform_slot_occupied")
    assert await h.case.owner.fetchval("SELECT succession_released_at FROM owned_seed_inventories WHERE id=$1",
                                      uuid.UUID(h.result["id"])) is None
