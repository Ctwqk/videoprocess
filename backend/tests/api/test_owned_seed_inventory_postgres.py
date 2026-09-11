"""Parent-run only: an explicitly named disposable database already at migration 037."""

import asyncio
import os
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.channel_agent import ChannelOpsQueueItem, ChannelProfile, ProductionTask, PublishingAccount
from app.models.owned_seed_inventory import OwnedSeedInventory, OwnedSeedInventoryItem
from app.services import owned_seed_inventory as inventory_service
from test_owned_seed_inventory import approval, draft, legacy_alias, inventory_env as inventory_env


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


@pytest.mark.parametrize("platform", ["", "youtube"])
async def test_pg_successor_rejects_alias_producer_before_unsafe_absence_read(inventory_env, monkeypatch, platform):
    env = inventory_env
    _, old = await draft(env)
    old_url = f"{env.url}/{old['id']}"
    assert (await env.client.post(old_url + "/approve", json=approval(old))).status_code == 200
    revoked = await env.client.post(old_url + "/revoke", json={"manifest_sha256": old["manifest_sha256"], "reason": "unused closeout"})
    old_digest = revoked.json()["closeout"]["sha256"]
    assert old_digest
    _, candidate = await draft(env, "barrier-successor")
    # Seed the already-approved legacy state that previously allowed alias producers.
    alias_channel, alias_account = await legacy_alias(env, platform)
    observed = asyncio.Event()
    committed = asyncio.Event()
    absence_reads = []
    original_scalars = AsyncSession.scalars

    async def scalars_with_producer_barrier(db, statement, *args, **kwargs):
        result = await original_scalars(db, statement, *args, **kwargs)
        descriptions = getattr(statement, "column_descriptions", [])
        accounts = any(item.get("entity") is PublishingAccount and getattr(item.get("expr"), "key", None) == "id"
                       for item in descriptions)
        tasks = any(item.get("entity") is ProductionTask for item in descriptions)
        if tasks:
            absence_reads.append(True)
        # Old code reaches the empty task read. Fixed code rejects at the earlier binding read.
        if not observed.is_set() and (accounts or tasks):
            observed.set()
            await asyncio.wait_for(committed.wait(), timeout=5)
        return result

    async def ordinary_producer():
        await asyncio.wait_for(observed.wait(), timeout=5)
        async with env.factory() as db:
            await db.execute(select(ChannelProfile).where(ChannelProfile.id == alias_channel).with_for_update())
            task = ProductionTask(channel_profile_id=alias_channel, target_account_id=alias_account,
                                  prompt="ordinary producer", state="selected")
            db.add(task)
            await db.flush()
            db.add(ChannelOpsQueueItem(kind="plan_task", channel_profile_id=alias_channel,
                                      idempotency_key=f"plan_task:{task.id}",
                                      payload_json={"production_task_id": str(task.id)}, status="queued"))
            await db.commit()
            committed.set()
            return task.id

    monkeypatch.setattr(AsyncSession, "scalars", scalars_with_producer_barrier)
    producer = asyncio.create_task(ordinary_producer())
    try:
        response = await asyncio.wait_for(env.client.post(f"{env.url}/{candidate['id']}/approve", json=approval(
            candidate, predecessor_inventory_id=old["id"], predecessor_closeout_sha256=old_digest,
        )), timeout=12)
        task_id = await asyncio.wait_for(producer, timeout=6)
    finally:
        if not producer.done():
            producer.cancel()
        await asyncio.gather(producer, return_exceptions=True)
        monkeypatch.setattr(AsyncSession, "scalars", original_scalars)
    assert response.status_code == 409
    assert committed.is_set() and absence_reads == []
    async with env.factory() as db:
        assert (await db.get(ProductionTask, task_id)).state == "selected"
        assert (await db.get(OwnedSeedInventory, uuid.UUID(old["id"]))).succession_released_at is None
        assert (await db.get(OwnedSeedInventory, uuid.UUID(candidate["id"]))).approved_at is None
        assert (await db.get(ChannelProfile, env.channel_id)).owned_seed_inventory_id == uuid.UUID(old["id"])


@pytest.mark.parametrize("platform", ["", "youtube"])
@pytest.mark.parametrize("mutation", ["create", "patch"])
async def test_pg_effective_youtube_account_writes_wait_for_platform_scope(inventory_env, monkeypatch, platform, mutation):
    env = inventory_env
    async with env.factory() as db:
        channel = ChannelProfile(name="other writer")
        db.add(channel)
        await db.flush()
        channel_id = channel.id
        if mutation == "patch":
            account = PublishingAccount(channel_profile_id=channel_id, account_label="ordinary",
                                        platform=platform, platform_account_id="unoccupied-writer")
            db.add(account)
            await db.flush()
            account_id = account.id
        await db.commit()
    _, row = await draft(env)
    assert (await env.client.post(f"{env.url}/{row['id']}/approve", json=approval(row))).status_code == 200
    original_lock = inventory_service.lock_platform_scope
    reached = asyncio.Event()
    writer_pid = None

    async def signalled_lock(db, platform_id):
        nonlocal writer_pid
        if platform_id == env.scope["platform_channel_id"]:
            writer_pid = await db.scalar(text("SELECT pg_backend_pid()"))
            reached.set()
        await original_lock(db, platform_id)

    url = f"/api/v1/channel-agent/channels/{channel_id}/accounts"
    async with env.factory() as blocker:
        await original_lock(blocker, env.scope["platform_channel_id"])
        monkeypatch.setattr(inventory_service, "lock_platform_scope", signalled_lock)
        if mutation == "create":
            request = env.client.post(url, json={"account_label": "alias", "platform": platform,
                                                "platform_account_id": env.scope["platform_channel_id"]})
        else:
            request = env.client.patch(f"{url}/{account_id}", json={"platform_account_id": env.scope["platform_channel_id"]})
        writer = asyncio.create_task(request)
        entered = asyncio.create_task(reached.wait())
        try:
            await asyncio.wait({writer, entered}, timeout=5, return_when=asyncio.FIRST_COMPLETED)
            assert reached.is_set() and not writer.done()

            async def has_advisory_wait():
                while not writer.done():
                    blocked = await blocker.scalar(text(
                        "SELECT EXISTS (SELECT 1 FROM pg_locks "
                        "WHERE pid = :pid AND locktype = 'advisory' AND NOT granted)"
                    ), {"pid": writer_pid})
                    if blocked:
                        return True
                    await asyncio.sleep(0.01)
                return False

            assert await asyncio.wait_for(has_advisory_wait(), timeout=5)
        finally:
            await blocker.rollback()
            entered.cancel()
            await asyncio.gather(entered, return_exceptions=True)
            result = await asyncio.wait_for(writer, timeout=5)
    assert result.status_code == 409
    async with env.factory() as db:
        accounts = list((await db.scalars(select(PublishingAccount).where(PublishingAccount.channel_profile_id == channel_id))).all())
        assert [account.platform_account_id for account in accounts] == ([] if mutation == "create" else ["unoccupied-writer"])
