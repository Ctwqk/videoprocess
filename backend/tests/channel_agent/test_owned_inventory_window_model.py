"""Assumption model only: no elapsed-time, real upload or activation evidence."""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

from app.channel_agent import owned_inventory as admission
from app.channel_agent.scheduler import ChannelOpsScheduler, scheduler_bucket
from app.channel_agent.service import ChannelAgentService
from app.events.outbox import event_outbox_table
from app.models import Base
from app.models.channel_agent import ChannelOpsQueueItem, ChannelProfile
from app.models.schedule import RuntimeSchedule
from app.services import owned_seed_inventory as inventory
from app.services import owned_seed_inventory_history as history
from tests.api.test_owned_seed_inventory import approval, inventory_env as inventory_env
from tests.channel_agent.test_owned_inventory import Policy, SQLITE_TABLES, sqlite_history, state
from tests.services.test_owned_seed_inventory_history import completed_rows, uid

MODEL = json.loads((Path(__file__).parents[1] / "fixtures/owned_inventory_window_model.json").read_text())
START = datetime.fromisoformat(MODEL["starts_at"])
DAY = timedelta(days=1)


@pytest.fixture
async def model_env(inventory_env, monkeypatch):
    env = inventory_env
    env.now, env.events = START - timedelta(hours=1), []

    async def clock(db):
        return env.now

    monkeypatch.setattr(inventory, "_now", clock)
    async with env.factory() as db:
        async with db.bind.begin() as conn:
            await conn.run_sync(lambda sync: Base.metadata.create_all(sync, tables=[Base.metadata.tables[n] for n in SQLITE_TABLES]))
            await conn.run_sync(event_outbox_table.create)
    body = await env.data("window-model")
    body.update(starts_at=MODEL["starts_at"], expires_at=MODEL["expires_at"])
    response = await env.client.post(env.url, json=body)
    assert response.status_code == 200, response.text
    row = response.json()
    response = await env.client.post(f"{env.url}/{row['id']}/approve", json=approval(row))
    assert response.status_code == 200, response.text
    env.inventory_id = uuid.UUID(row["id"])
    async with env.factory() as db:
        (await db.get(ChannelProfile, env.channel_id)).intake_paused_at = None
        await db.commit()

    async def observations(db, *, platform_channel_id):
        rows = (await sqlite_history(db, platform_channel_id=platform_channel_id)).rows.as_dict()
        for event in env.events:
            synthetic = lifecycle_rows(env, event)
            rows["production_tasks"] = [r for r in rows["production_tasks"] if r["id"] != event["task"]]
            for table, records in synthetic.items():
                if table not in {"channel_profiles", "publishing_accounts"}:
                    rows[table].extend(records)
            # The normal plan queue was consumed by the synthetic external lifecycle.
            for queue in rows["channel_ops_queue_items"]:
                if queue["kind"] == "plan_task" and queue["payload_json"].get("production_task_id") == event["task"]:
                    queue.update(status="succeeded", attempt_count=1, locked_at=None, locked_by=None)
        return history.OwnedHistorySnapshot.from_rows(rows, platform_channel_id=platform_channel_id, observed_at=env.now)

    monkeypatch.setattr(history, "load_owned_history_evidence", observations)
    return env


def lifecycle_rows(env, event):
    """Synthetic normal effects only; the real A1 reader/assessment validates them."""
    publication_at = event["settled"] - timedelta(minutes=30)
    rows = completed_rows(start=publication_at)
    mapping = {uid(i): str(uuid.uuid5(uuid.UUID(event["task"]), str(i))) for i in range(1, 60)}
    mapping.update({uid(2): str(env.channel_id), uid(3): env.scope["target_account_id"],
                    uid(4): event["task"], uid(5): event["seed"], "abcdefghijk": f"modelvid{event['ordinal']:03d}"})

    def remap(value):
        if isinstance(value, dict):
            return {k: remap(v) for k, v in value.items()}
        if isinstance(value, list):
            return [remap(v) for v in value]
        if isinstance(value, str):
            for old, new in mapping.items():
                value = value.replace(old, new)
        return value

    rows = remap(rows)
    task, operation = rows["production_tasks"][0], rows["youtube_upload_operations"][0]
    task["state"] = "scheduled"
    operation.update(request_attempted_at=event["admitted"].isoformat(), completed_at=event["completed"].isoformat(),
                     content_sha256=f"{event['ordinal'] + 100:064x}")
    rows["jobs"][0]["completed_at"] = rows["node_executions"][0]["completed_at"] = event["completed"].isoformat()
    rows["publication_records"][0]["uploaded_at"] = event["completed"].isoformat()
    rows["feedback_snapshots"] = []
    for index, metric in enumerate(rows["publication_metric_schedules"]):
        done = datetime.fromisoformat(metric["due_at"]) <= env.now
        metric.update(status="succeeded" if done else "pending", attempt_count=int(done),
                      completed_at=metric["due_at"] if done else None, last_attempt_at=metric["due_at"] if done else None)
        queue = rows["channel_ops_queue_items"][index + 2]
        queue.update(status="succeeded" if done else "queued", attempt_count=int(done))
        if done:
            rows["feedback_snapshots"].append({"id": mapping[uid(40 + index)], "publication_id": metric["publication_id"],
                                               "snapshot_stage": metric["snapshot_stage"]})
    if env.now < event["settled"]:
        rows["publication_records"] = rows["publication_metric_schedules"] = rows["feedback_snapshots"] = rows["channel_ops_queue_items"] = []
        task["state"] = "producing"
    if env.now < event["completed"]:
        operation.update(status="submitted", completed_at=None, receipt_json={})
        rows["jobs"][0].update(status="RUNNING", completed_at=None)
        rows["node_executions"][0].update(status="RUNNING", completed_at=None)
    return rows


async def probe(env, at, schedule):
    env.now = at
    async with env.factory() as db:
        (await db.get(RuntimeSchedule, "videoprocess")).state = schedule
        await db.commit()
        phase = await admission.read_phase(db, env.channel_id, env.inventory_id, None)
        assert phase.hold is None, phase.hold
        candidate = phase.unused_id if phase.candidate else None
        await db.rollback()
        return candidate


async def run_window_model(env, case):
    admitted = []
    policy = Policy()
    for day in range(7):
        opens = START + day * DAY
        assert await probe(env, opens - timedelta(seconds=1), "CLOSED") is None
        eligible = opens
        if env.events:
            if env.events[-1]["settled"] > env.events[-1]["completed"] + DAY:
                assert await probe(env, env.events[-1]["completed"] + DAY, "OPEN") is None
            eligible = max(eligible, env.events[-1]["completed"] + DAY, env.events[-1]["settled"])
        # Eligibility just misses a poll boundary. Rounding + queue latency are assumptions.
        period = case["poll_seconds"]
        polled = datetime.fromtimestamp((int(eligible.timestamp()) // period + 1) * period, tz=START.tzinfo)
        at = polled + timedelta(seconds=case["queue_seconds"])
        if day + 1 not in case["missed_days"] and at < opens.replace(hour=13):
            if env.events and eligible == env.events[-1]["completed"] + DAY:
                assert await probe(env, eligible - timedelta(microseconds=1), "OPEN") is None
                assert await probe(env, eligible, "OPEN") is not None
            expected_item = await probe(env, at, "OPEN")
            assert expected_item is not None
            async with env.factory() as db:
                await ChannelOpsScheduler().run_once(db, now=polled)
                queued = (await db.scalars(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "agent_tick")
                    .order_by(ChannelOpsQueueItem.created_at.desc()))).all()
                assert any(q.payload_json["scheduler_bucket"] == polled.strftime("%Y-%m-%d-%H-%M") for q in queued)
                assert scheduler_bucket(polled, 1) == scheduler_bucket(polled, 15)
                audit = await ChannelAgentService(pds_client=policy).tick(db, channel_id=env.channel_id)
                assert audit.tasks_selected == 1
            result = await state(env)
            item = next(i for i in result["items"] if str(i.id) == expected_item)
            assert item.ordinal == len(admitted) + 1
            assert sum(i.state == "reserved" for i in result["items"]) == 1
            event = dict(task=str(item.production_task_id), seed=str(item.manual_seed_id), ordinal=item.ordinal,
                         admitted=at, completed=at + timedelta(seconds=case["upload_seconds"]))
            event["settled"] = event["completed"] + timedelta(seconds=case["settlement_seconds"])
            if env.events:
                assert at - env.events[-1]["admitted"] >= DAY
                assert at - env.events[-1]["completed"] >= DAY
                assert at >= env.events[-1]["settled"]
            env.events.append(event)
            admitted.append(int((at - START).total_seconds() / 60))
        assert await probe(env, opens.replace(hour=13), "DRAINING") is None
        assert await probe(env, opens.replace(hour=14), "CLOSED") is None
    assert len({e["task"] for e in env.events}) == len(admitted) == len(policy.calls)
    result = await state(env)
    used = [i for i in result["items"] if i.production_task_id is not None]
    assert len({i.asset_id for i in used}) == len({i.content_sha256 for i in used}) == len(admitted)
    assert result["inventory"].state == ("exhausted" if len(admitted) == 7 else "approved")
    assert int((env.events[-1]["completed"] - START).total_seconds() / 60) == case["last_completion_minute"]
    assert int((env.events[-1]["settled"] - START).total_seconds() / 60) == case["last_settlement_minute"]
    bounds = MODEL["assumed_bounds_seconds"]
    fits = (len(admitted) == 7 and not case["missed_days"] and case["poll_seconds"] + case["queue_seconds"] <= bounds["tick_and_queue"]
            and case["upload_seconds"] <= bounds["upload_completion"] and case["settlement_seconds"] <= bounds["normal_settlement"])
    assert fits == case["fits_assumptions"]
    if fits:
        assert env.events[-1]["settled"] < (START + 6 * DAY).replace(hour=13)
        assert (env.events[-1]["settled"] - START).total_seconds() < MODEL["observation_required_seconds"]
    env.now = datetime.fromisoformat(MODEL["expires_at"])
    async with env.factory() as db:
        (await db.get(RuntimeSchedule, "videoprocess")).state = "OPEN"
        await db.commit()
        audit = await ChannelAgentService(pds_client=policy).tick(db, channel_id=env.channel_id)
        assert audit.tasks_selected == 0
        if len(admitted) < 7:
            assert audit.guards_triggered_json == ["owned_inventory_expired"]
    assert (await state(env))["inventory"].state == ("exhausted" if len(admitted) == 7 else "expired")
    assert len(policy.calls) == len(admitted)
    return admitted


@pytest.mark.parametrize("case", MODEL["cases"], ids=lambda case: case["name"])
async def test_native_python_seven_day_window_model(model_env, case):
    assert await run_window_model(model_env, case) == case["admission_minutes"]


async def test_each_daily_window_denies_otherwise_eligible_unused_inventory(model_env):
    for day in range(7):
        opens = START + day * DAY
        assert await probe(model_env, opens - timedelta(seconds=1), "CLOSED") is None
        assert await probe(model_env, opens, "OPEN") is not None
        assert await probe(model_env, opens.replace(hour=13), "DRAINING") is None
        assert await probe(model_env, opens.replace(hour=14), "CLOSED") is None
    assert not (await state(model_env))["tasks"]
