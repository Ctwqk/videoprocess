from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from app.models.channel_agent import ChannelOpsQueueItem, ChannelProfile
from app.models.owned_seed_inventory import OwnedSeedInventory
from app.models.schedule import RuntimeSchedule
from app.services import owned_inventory_feedback as feedback
from app.services.channelops_soak_guard import SoakGuardPolicy, assess_channelops_soak
from tests.api.test_owned_seed_inventory import inventory_env as inventory_env
from tests.channel_agent.test_owned_inventory import owned_env as owned_env, state, tick, Policy


@pytest.mark.asyncio
async def test_profile_read_only_and_durable_hold_preserve_backlog(owned_env):
    env = owned_env
    await tick(env, Policy())
    before = await state(env)
    policy = SoakGuardPolicy(env.channel_id, datetime.now(timezone.utc) - timedelta(hours=2))
    async with env.factory() as db:
        report = await assess_channelops_soak(db, policy, external_conditions=("service_unhealthy",))
    assert report.inventory_id == env.inventory_id
    assert report.metrics["inventory_hold_reason"] == "service_unhealthy"
    assert (await state(env))["inventory"].state == "approved"
    async with env.factory() as db:
        applied = await feedback.check_owned_inventory_feedback(
            db, env.channel_id, env.inventory_id, apply=True,
            external_conditions=("service_unhealthy",),
        )
    assert applied.hold_reason == "service_unhealthy"
    assert applied.metrics["inventory_state"] == "held"
    after = await state(env)
    assert after["inventory"].state == "held"
    assert after["channel"].intake_paused_at is not None
    assert after["channel"].halted_at is None
    assert [(i.state, i.production_task_id) for i in after["items"]] == [(i.state, i.production_task_id) for i in before["items"]]
    assert [(q.id, q.status, q.run_after) for q in after["queue"]] == [(q.id, q.status, q.run_after) for q in before["queue"]]
    assert [t.state for t in after["tasks"]] == [t.state for t in before["tasks"]]
    async with env.factory() as db:
        assert (await db.get(RuntimeSchedule, "videoprocess")).state == "OPEN"
    assert (await tick(env, Policy())).tasks_selected == 0


@pytest.mark.asyncio
async def test_profile_apply_reassesses_and_never_clears_emergency_halt(owned_env):
    env = owned_env
    async with env.factory() as db:
        channel = await db.get(ChannelProfile, env.channel_id)
        channel.halted_at = datetime.now(timezone.utc)
        channel.halt_reason = "operator_emergency"
        await db.commit()
    async with env.factory() as db:
        report = await feedback.check_owned_inventory_feedback(db, env.channel_id, env.inventory_id, apply=True)
    assert report.hold_reason == "channel_halted"
    result = await state(env)
    assert result["channel"].halted_at is not None
    assert result["channel"].halt_reason == "operator_emergency"
    assert result["inventory"].state == "held"
    assert not result["tasks"]


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["held", "expired", "revoked"])
async def test_terminal_hold_is_idempotent_and_preserves_original_reason(owned_env, terminal):
    env = owned_env
    async with env.factory() as db:
        row = await db.get(OwnedSeedInventory, env.inventory_id)
        row.state, row.hold_reason = terminal, "original_stop"
        await db.commit()
    for _ in range(2):
        async with env.factory() as db:
            await feedback.check_owned_inventory_feedback(
                db, env.channel_id, env.inventory_id, apply=True, external_conditions=("service_unhealthy",),
            )
    result = await state(env)
    assert result["inventory"].state == terminal
    assert result["inventory"].hold_reason == "original_stop"
    assert result["channel"].intake_paused_at is not None
    async with env.factory() as db:
        assert not list((await db.scalars(select(ChannelOpsQueueItem))).all())


@pytest.mark.asyncio
async def test_completion_hook_never_commits_or_replaces_an_outstanding_task(owned_env):
    env = owned_env
    await tick(env, Policy())
    before = await state(env)
    async with env.factory() as db:
        await db.begin()
        assert await feedback.finalize_owned_inventory_items(db, env.channel_id) == ()
        assert db.in_transaction()
        await db.rollback()
    after = await state(env)
    assert [(i.state, i.production_task_id) for i in before["items"]] == [(i.state, i.production_task_id) for i in after["items"]]
    assert after["inventory"].state == "approved"


@pytest.mark.asyncio
async def test_seventh_completion_store_hook_is_idempotent_and_transaction_owned(monkeypatch):
    import uuid
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from app.services import owned_seed_inventory as inv
    from app.services import owned_seed_inventory_history as history
    from tests.services.test_owned_inventory_feedback import settled_inventory
    from tests.services.test_owned_seed_inventory_history import NOW, snap, uid

    rows = settled_inventory()
    inventory_id = uuid.UUID(uid(100))
    row = SimpleNamespace(id=inventory_id, channel_profile_id=uuid.UUID(uid(2)), state="exhausted")
    channel = SimpleNamespace(owned_seed_inventory_id=inventory_id, intake_paused_at=NOW, intake_pause_reason="owned_inventory_exhausted")
    item = SimpleNamespace(id=uuid.UUID(uid(1006)), state="reserved", completed_at=None)
    db = AsyncMock()
    db.in_transaction = lambda: True
    db.scalar.return_value = rows["owned_seed_inventories"][0]["platform_channel_id"]
    monkeypatch.setattr(inv, "_row", AsyncMock(side_effect=lambda db, model, id, **kw: channel if model is ChannelProfile else row))
    monkeypatch.setattr(inv, "lock_history_schedule", AsyncMock())
    monkeypatch.setattr(inv, "lock_platform_scope", AsyncMock())
    monkeypatch.setattr(inv, "inventory_items", AsyncMock(return_value=[item]))
    monkeypatch.setattr(inv, "_verify_manifest", AsyncMock())
    monkeypatch.setattr(inv, "_now", AsyncMock(return_value=NOW))
    monkeypatch.setattr(history, "load_owned_history_evidence", AsyncMock(side_effect=lambda *a, **kw: snap(rows)))
    assert await feedback.finalize_owned_inventory_items(db, uuid.UUID(uid(2))) == (uid(1006),)
    assert item.state == "completed" and item.completed_at == NOW
    assert row.state == "exhausted" and channel.intake_paused_at == NOW
    rows["owned_seed_inventory_items"][-1].update(state="completed", completed_at=NOW.isoformat())
    assert await feedback.finalize_owned_inventory_items(db, uuid.UUID(uid(2))) == ()
    db.commit.assert_not_awaited()
    db.rollback.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["fresh", "changed", "read_failed", "cancelled"])
async def test_watcher_fresh_redis_reentry_never_observes_under_transaction(owned_env, monkeypatch, mode):
    import asyncio
    from types import SimpleNamespace

    from app.channel_agent import owned_inventory as admission
    from app.services import owned_seed_inventory as inv

    env = owned_env
    requests = []
    observed = []

    def request(snapshot):
        result = SimpleNamespace(digest="changed" if mode == "changed" and requests else "exact",
                                 observed_at=snapshot.observed_at, sources=())
        requests.append(result)
        return result

    async with env.factory() as db:
        async def observe(value):
            assert not db.in_transaction()
            observed.append(value)
            if mode == "read_failed":
                raise inv.OwnedInventoryError("owned_history_redis_read_failed")
            if mode == "cancelled":
                raise asyncio.CancelledError()
            return ()

        monkeypatch.setattr(admission, "redis_request", request)
        monkeypatch.setattr(admission, "observe_redis", observe)
        if mode == "cancelled":
            with pytest.raises(asyncio.CancelledError):
                await feedback.check_owned_inventory_feedback(db, env.channel_id, env.inventory_id, apply=True)
        else:
            result = await feedback.check_owned_inventory_feedback(db, env.channel_id, env.inventory_id, apply=True)
            if mode == "fresh":
                assert result.hold_reason is None
            else:
                assert result.hold_reason == ("owned_history_observation_stale" if mode == "changed" else "owned_history_redis_read_failed")
    assert len(observed) == 1
    assert (await state(env))["inventory"].state == ("held" if mode in {"changed", "read_failed"} else "approved")
