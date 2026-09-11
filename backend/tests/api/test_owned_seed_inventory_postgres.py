"""Parent-run only: an explicitly named disposable database already at migration 037."""

import asyncio
import os
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError

from app.models.channel_agent import ProductionTask
from app.models.owned_seed_inventory import OwnedSeedInventory, OwnedSeedInventoryItem
from test_owned_seed_inventory import approval, draft, inventory_env as inventory_env


DISPOSABLE_URL = os.environ.get("OWNED_INVENTORY_DISPOSABLE_TEST_URL", "")
pytestmark = [
    pytest.mark.skipif(not DISPOSABLE_URL, reason="explicit disposable inventory database required"),
    pytest.mark.parametrize("inventory_env", [pytest.param(DISPOSABLE_URL, id="disposable-pg")], indirect=True),
]


async def test_pg_concurrent_approval_is_one_immutable_result(inventory_env):
    env = inventory_env
    _, row = await draft(env)
    url = f"{env.url}/{row['id']}/approve"
    results = await asyncio.wait_for(asyncio.gather(
        env.client.post(url, json=approval(row)), env.client.post(url, json=approval(row)),
    ), timeout=15)
    assert [result.status_code for result in results] == [200, 200]
    assert results[0].json()["approved_at"] == results[1].json()["approved_at"]


async def test_pg_approval_and_asset_delete_preserve_blob(inventory_env):
    env = inventory_env
    data, row = await draft(env)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def pause_read(_path):
        entered.set()
        await asyncio.wait_for(release.wait(), timeout=5)

    env.storage.on_read = pause_read
    task = asyncio.create_task(env.client.post(f"{env.url}/{row['id']}/approve", json=approval(row)))
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        deletion = await env.client.delete(f"/api/v1/assets/{data['entries'][0]['asset_id']}")
        assert deletion.status_code == 409
        assert env.storage.deletions == []
    finally:
        release.set()
        result = await asyncio.wait_for(task, timeout=10)
    assert result.status_code == 200


async def test_pg_successor_contenders_cannot_share_predecessor(inventory_env):
    env = inventory_env
    _, old = await draft(env)
    old_url = f"{env.url}/{old['id']}"
    assert (await env.client.post(old_url + "/approve", json=approval(old))).status_code == 200
    revoked = await env.client.post(old_url + "/revoke", json={"manifest_sha256": old["manifest_sha256"], "reason": "unused closeout"})
    closeout_hash = revoked.json()["closeout"]["sha256"]
    _, first = await draft(env, "successor-one")
    _, second = await draft(env, "successor-two")
    results = await asyncio.wait_for(asyncio.gather(*[
        env.client.post(f"{env.url}/{row['id']}/approve", json=approval(row,
                        predecessor_inventory_id=old["id"], predecessor_closeout_sha256=closeout_hash))
        for row in (first, second)
    ]), timeout=15)
    assert sorted(result.status_code for result in results) == [200, 409]
    async with env.factory() as db:
        occupied = list((await db.scalars(select(OwnedSeedInventory).where(
            OwnedSeedInventory.platform_channel_id == env.scope["platform_channel_id"],
            OwnedSeedInventory.approved_at.is_not(None), OwnedSeedInventory.succession_released_at.is_(None),
        ))).all())
        assert len(occupied) == 1


@pytest.mark.parametrize("state", ["approved", "held", "expired", "exhausted", "revoked"])
async def test_pg_occupied_slot_includes_all_unreleased_approved_states(inventory_env, state):
    env = inventory_env
    _, old = await draft(env)
    assert (await env.client.post(f"{env.url}/{old['id']}/approve", json=approval(old))).status_code == 200
    _, candidate = await draft(env, "candidate")
    async with env.factory() as db:
        current = await db.get(OwnedSeedInventory, uuid.UUID(old["id"]))
        current.state = state
        await db.commit()
    async with env.factory() as db:
        new = await db.get(OwnedSeedInventory, uuid.UUID(candidate["id"]))
        new.state = "approved"
        new.approved_at = datetime.now(timezone.utc)
        new.approved_by = "test"
        new.approval_reference = "test:conflicting-approval"
        with pytest.raises(DBAPIError):
            await db.commit()
        await db.rollback()


async def test_pg_manifest_item_and_consumption_history_are_immutable(inventory_env):
    env = inventory_env
    _, row = await draft(env)
    assert (await env.client.post(f"{env.url}/{row['id']}/approve", json=approval(row))).status_code == 200
    async with env.factory() as db:
        item = (await db.scalars(select(OwnedSeedInventoryItem).where(
            OwnedSeedInventoryItem.inventory_id == uuid.UUID(row["id"]),
        ).order_by(OwnedSeedInventoryItem.ordinal))).first()
        item_id = item.id
        task = ProductionTask(channel_profile_id=env.channel_id,
                              target_account_id=uuid.UUID(env.scope["target_account_id"]),
                              manual_seed_id=item.manual_seed_id, prompt="consumed", state="selected")
        db.add(task)
        await db.flush()
        item.state = "reserved"
        item.production_task_id = task.id
        item.consumed_at = datetime.now(timezone.utc)
        await db.commit()
    statements = [
        ("UPDATE owned_seed_inventories SET manifest_sha256 = :value WHERE id = :id", row["id"]),
        ("UPDATE owned_seed_inventory_items SET content_sha256 = :value WHERE id = :id", str(item_id)),
        ("UPDATE owned_seed_inventory_items SET state = 'unused', production_task_id = NULL, consumed_at = NULL WHERE id = :id", str(item_id)),
        ("DELETE FROM owned_seed_inventory_items WHERE id = :id", str(item_id)),
    ]
    for statement, target_id in statements:
        async with env.factory() as db:
            with pytest.raises(DBAPIError):
                await db.execute(text(statement), {"id": uuid.UUID(target_id), "value": "c" * 64})
                await db.commit()
            await db.rollback()


async def test_pg_one_reserved_item_constraint(inventory_env):
    env = inventory_env
    _, row = await draft(env)
    async with env.factory() as db:
        items = list((await db.scalars(select(OwnedSeedInventoryItem).where(
            OwnedSeedInventoryItem.inventory_id == uuid.UUID(row["id"]),
        ).order_by(OwnedSeedInventoryItem.ordinal))).all())
        for item in items[:2]:
            task = ProductionTask(channel_profile_id=env.channel_id,
                                  target_account_id=uuid.UUID(env.scope["target_account_id"]),
                                  manual_seed_id=item.manual_seed_id, prompt="reservation", state="selected")
            db.add(task)
            await db.flush()
            item.state = "reserved"
            item.production_task_id = task.id
            item.consumed_at = datetime.now(timezone.utc)
            if item is items[0]:
                await db.commit()
        with pytest.raises(DBAPIError):
            await db.commit()
        await db.rollback()
