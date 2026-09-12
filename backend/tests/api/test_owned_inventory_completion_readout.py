from __future__ import annotations

import asyncio
import copy
from unittest.mock import AsyncMock

import pytest
from fastapi.encoders import jsonable_encoder
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.channel_agent import ProductionTask
from app.models.owned_seed_inventory import OwnedSeedInventory
from app.services import owned_inventory_feedback as feedback
from app.services import owned_seed_inventory as inventory
from tests.api.test_owned_seed_inventory import draft, inventory_env as inventory_env
from tests.channel_agent.test_owned_inventory import owned_env as owned_env, Policy, state, tick


@pytest.fixture
async def readout_env(owned_env):
    env = owned_env
    # Test-only V2 fixture; this does not enable the real V2 approval gate.
    async with env.factory() as db:
        row = await db.get(OwnedSeedInventory, env.inventory_id)
        manifest = copy.deepcopy(row.manifest_json)
        manifest.update(version=2, legacy_history={"version": 1, "bindings": [], "retired_unassigned_preupload": None})
        row.manifest_json, row.manifest_sha256 = manifest, inventory.sha256(manifest)
        await db.commit()
    env.readout_url = f"{env.url}/{env.inventory_id}"
    return env


async def test_v1_get_output_and_auth_are_unchanged(inventory_env, monkeypatch):
    env = inventory_env
    _, created = await draft(env)
    check = AsyncMock(side_effect=AssertionError("V1 must not assess"))
    monkeypatch.setattr(feedback, "check_owned_inventory_feedback", check)
    response = await env.client.get(f"{env.url}/{created['id']}", headers={"Authorization": ""})
    assert response.status_code == 200
    assert response.json() == created
    check.assert_not_awaited()


@pytest.mark.parametrize("authorization", ["", "Bearer wrong"])
async def test_v2_operator_auth_precedes_assessment(readout_env, monkeypatch, authorization):
    check = AsyncMock(side_effect=AssertionError("unauthorized assessment"))
    monkeypatch.setattr(feedback, "check_owned_inventory_feedback", check)
    response = await readout_env.client.get(readout_env.readout_url, headers={"Authorization": authorization})
    assert response.status_code == 403
    assert response.json() == {"detail": "owned_inventory_operator_required"}
    check.assert_not_awaited()


@pytest.mark.parametrize("hold_reason", [None, "owned_inventory_operation_unresolved", "owned_history_observation_stale"])
async def test_v2_get_adds_only_existing_bounded_observation(readout_env, monkeypatch, hold_reason):
    env = readout_env
    async with env.factory() as db:
        original = await inventory.read_inventory(db, env.channel_id, env.inventory_id)
    metrics = {"inventory_intake_status": "closed", "inventory_settlement_status": "complete" if not hold_reason else "pending",
               "inventory_feedback_status": "complete" if not hold_reason else "blocked"}
    completed_ids = (original["items"][0]["id"],) if not hold_reason else ()
    check = AsyncMock(return_value=feedback.OwnedInventoryFeedback(str(env.inventory_id), hold_reason, completed_ids, metrics))
    commit = AsyncMock(side_effect=AssertionError("GET must not commit"))
    monkeypatch.setattr(feedback, "check_owned_inventory_feedback", check)
    monkeypatch.setattr(AsyncSession, "commit", commit)
    bodies = []
    for _ in range(2):
        response = await env.client.get(env.readout_url)
        assert response.status_code == 200, response.text
        bodies.append(response.json())
    assert bodies[0] == bodies[1]
    assessment = bodies[0].pop("assessment")
    assert bodies[0] == jsonable_encoder(original)
    assert assessment == {"status": "blocked" if hold_reason else "observed", "observation_only": True,
                          "observed_at": None, "hold_reason": hold_reason, "metrics": metrics,
                          "completed_item_ids": list(completed_ids)}
    for call in check.await_args_list:
        assert call.args[1:] == (env.channel_id, env.inventory_id)
        assert call.kwargs == {"apply": False}
    commit.assert_not_awaited()
    persisted = await state(env)
    assert persisted["inventory"].state == "approved"
    assert all(item.state == "unused" and item.completed_at is None for item in persisted["items"])


async def test_repeated_get_runs_real_readonly_assessment_without_holding_failed_task(readout_env, monkeypatch):
    env = readout_env
    await tick(env, Policy())
    before = await state(env)
    async with env.factory() as db:
        task = await db.get(ProductionTask, before["tasks"][0].id)
        task.state = "failed"
        await db.commit()
    before = await state(env)
    commit = AsyncMock(side_effect=AssertionError("GET must not commit"))
    monkeypatch.setattr(AsyncSession, "commit", commit)
    for _ in range(2):
        response = await env.client.get(env.readout_url)
        assert response.status_code == 200, response.text
        assessment = response.json()["assessment"]
        assert assessment["status"] == "blocked"
        assert assessment["hold_reason"] == "owned_inventory_task_failed"
        assert assessment["metrics"]["inventory_feedback_status"] == "blocked"
        assert assessment["completed_item_ids"] == []
    after = await state(env)
    for name in before:
        old, new = before[name], after[name]
        old = old if isinstance(old, list) else [old]
        new = new if isinstance(new, list) else [new]
        assert [{c.name: getattr(row, c.name) for c in row.__table__.columns} for row in old] == [
            {c.name: getattr(row, c.name) for c in row.__table__.columns} for row in new]
    commit.assert_not_awaited()


async def test_unavailable_assessment_preserves_inventory_and_cannot_claim_complete(readout_env, monkeypatch):
    check = AsyncMock(side_effect=RuntimeError("private transport detail"))
    monkeypatch.setattr(feedback, "check_owned_inventory_feedback", check)
    response = await readout_env.client.get(readout_env.readout_url)
    assert response.status_code == 200
    body = response.json()
    assert body["manifest"]["version"] == 2 and len(body["items"]) == 7 and "closeout" in body
    assert body["assessment"] == {"status": "unavailable", "observation_only": True, "observed_at": None,
                                  "hold_reason": "owned_inventory_assessment_unavailable", "metrics": {}, "completed_item_ids": []}
    assert "private transport detail" not in response.text


async def test_assessment_cancellation_is_not_a_successful_get(readout_env, monkeypatch):
    monkeypatch.setattr(feedback, "check_owned_inventory_feedback", AsyncMock(side_effect=asyncio.CancelledError()))
    with pytest.raises(asyncio.CancelledError):
        await readout_env.client.get(readout_env.readout_url)
