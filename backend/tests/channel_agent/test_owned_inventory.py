from __future__ import annotations

import asyncio
import json
import copy
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.channel_agent.scheduler import ChannelOpsScheduler
from app.channel_agent.queue import ChannelOpsQueueService
from app.channel_agent.runner import ChannelAgentRunner
from app.channel_agent import runner as runner_module
from app.channel_agent.service import ChannelAgentService
from app.channel_agent import owned_inventory as admission
from app.config import settings
from app.events.outbox import event_outbox_table
from app.models import Base
from app.models.channel_agent import (
    AgentTickAudit, ChannelOpsQueueItem, ChannelProfile, DecisionAuditEntry,
    ManualSeed, ProductionTask,
)
from app.models.owned_seed_inventory import OwnedSeedInventory, OwnedSeedInventoryItem
from app.models.schedule import RuntimeSchedule
from app.pds_client import PDSDecision
from app.services import owned_seed_inventory as inventory
from app.services import owned_seed_inventory_history as history
from tests.api.test_owned_seed_inventory import approval, inventory_env as inventory_env
from tests.services.test_owned_seed_inventory_history import NOW, completed_rows, retired_rows, snap

SQLITE_TABLES = {
    "assets", "channel_profiles", "topic_lanes", "publishing_accounts", "lane_format_matrix", "manual_seeds",
    "jobs", "node_executions", "production_tasks", "channel_ops_queue_items", "youtube_upload_operations",
    "publication_records", "publication_promotion_operations", "owned_seed_inventories", "owned_seed_inventory_items",
    "runtime_schedules", "publication_metric_schedules", "feedback_snapshots", "agent_tick_audits",
    "decision_audit_entries", "internal_scheduler_runs",
}


async def sqlite_history(db, *, platform_channel_id):
    # Test-only dialect adapter; production must retain A1's single PG statement.
    rows = {}
    for name, model in history.HISTORY_MODELS.items():
        if name not in SQLITE_TABLES:
            rows[name] = []
            continue
        records = (await db.scalars(select(model))).all()
        rows[name] = [json.loads(inventory.canonical({
            c.name: getattr(row, c.name) for c in model.__table__.columns
            if c.name not in {"lease_secret_sha256", "token_sha256"}
        })) for row in records]
    return history.OwnedHistorySnapshot.from_rows(
        rows, platform_channel_id=platform_channel_id, observed_at=await inventory._now(db),
    )


@pytest.fixture
async def owned_env(inventory_env, monkeypatch):
    return await configure_owned_env(inventory_env, monkeypatch, sqlite=True)


async def configure_owned_env(env, monkeypatch, *, sqlite):
    if sqlite:
        async with env.factory() as db:
            async with db.bind.begin() as connection:
                await connection.run_sync(lambda conn: Base.metadata.create_all(
                    conn, tables=[Base.metadata.tables[name] for name in SQLITE_TABLES],
                ))
                await connection.run_sync(event_outbox_table.create)
    original_clock = inventory._now
    earlier = datetime.now(timezone.utc) - timedelta(hours=2)

    async def approval_clock(db):
        return earlier

    monkeypatch.setattr(inventory, "_now", approval_clock)
    data = await env.data()
    starts = earlier + timedelta(hours=1)
    data.update(starts_at=starts.isoformat(), expires_at=(starts + timedelta(days=7)).isoformat())
    result = await env.client.post(env.url, json=data)
    assert result.status_code == 200, result.text
    row = result.json()
    result = await env.client.post(f"{env.url}/{row['id']}/approve", json=approval(row))
    assert result.status_code == 200, result.text
    monkeypatch.setattr(inventory, "_now", original_clock)
    if sqlite:
        monkeypatch.setattr(history, "load_owned_history_evidence", sqlite_history)
    env.inventory_id = uuid.UUID(row["id"])
    async with env.factory() as db:
        channel = await db.get(ChannelProfile, env.channel_id)
        channel.intake_paused_at = None
        (await db.get(RuntimeSchedule, "videoprocess")).state = "OPEN"
        await db.commit()
    return env


class Policy:
    def __init__(self, action=None, verdict="allow", metadata=None):
        self.action, self.verdict, self.metadata = action, verdict, metadata or {}
        self.calls = []

    async def decide(self, request):
        self.calls.append(request)
        if self.action:
            await self.action()
        return PDSDecision(decision_id="fixture", verdict=self.verdict, metadata=self.metadata)


async def tick(env, policy=None, **kwargs):
    async with env.factory() as db:
        return await ChannelAgentService(pds_client=policy).tick(db, channel_id=env.channel_id, **kwargs)


async def state(env):
    async with env.factory() as db:
        return {
            "inventory": await db.get(OwnedSeedInventory, env.inventory_id),
            "channel": await db.get(ChannelProfile, env.channel_id),
            "tasks": list((await db.scalars(select(ProductionTask))).all()),
            "items": list((await db.scalars(select(OwnedSeedInventoryItem).order_by(OwnedSeedInventoryItem.ordinal))).all()),
            "seeds": list((await db.scalars(select(ManualSeed))).all()),
            "audits": list((await db.scalars(select(AgentTickAudit))).all()),
            "decisions": list((await db.scalars(select(DecisionAuditEntry))).all()),
            "queue": list((await db.scalars(select(ChannelOpsQueueItem))).all()),
        }


async def test_exact_lowest_atomic_admission_and_replay(owned_env):
    env = owned_env
    policy = Policy()
    audit = await tick(env, policy)
    result = await state(env)
    assert len(policy.calls) == audit.tasks_selected == len(result["tasks"]) == 1
    task, first = result["tasks"][0], result["items"][0]
    assert first.state == "reserved" and first.production_task_id == task.id
    assert [i.state for i in result["items"]][1:] == ["unused"] * 6
    assert sum(s.status == "exhausted" for s in result["seeds"]) == 1
    assert task.approval_mode == "agent" and task.source == "manual_seed"
    evidence = task.agent_approval_evidence_json["owned_inventory"]
    assert evidence == task.channel_config_snapshot_json["owned_inventory"]
    assert evidence["item_id"] == str(first.id)
    assert task.channel_config_snapshot_json["manual_seed"]["constraints_json"] == next(
        s.constraints_json for s in result["seeds"] if s.id == task.manual_seed_id)
    assert policy.calls[0].context["candidate_id"] == f"owned_inventory:{env.inventory_id}:{first.id}"
    assert policy.calls[0].context["owned_inventory"] == evidence
    assert len(result["decisions"]) == 1 and result["decisions"][0].created_task_id == task.id
    assert [(q.kind, q.idempotency_key) for q in result["queue"]] == [("plan_task", f"plan_task:{task.id}")]
    await tick(env, policy)
    assert len(policy.calls) == len((await state(env))["tasks"]) == 1


@pytest.mark.parametrize("mode", ["closed", "busy", "paused", "not_started", "held", "revoked"])
async def test_pointer_never_falls_back_or_calls_policy_when_not_ready(owned_env, mode):
    env = owned_env
    async with env.factory() as db:
        inv = await db.get(OwnedSeedInventory, env.inventory_id)
        channel = await db.get(ChannelProfile, env.channel_id)
        if mode == "closed":
            (await db.get(RuntimeSchedule, "videoprocess")).state = "CLOSED"
        elif mode == "paused":
            channel.intake_paused_at = datetime.now(timezone.utc)
        elif mode in {"held", "revoked"}:
            inv.state = mode
        elif mode == "busy":
            db.add(ChannelOpsQueueItem(kind="cleanup_expired", idempotency_key="unrelated", payload_json={}))
        else:
            inv.starts_at += timedelta(days=1)
            inv.expires_at += timedelta(days=1)
        await db.commit()
    policy = Policy()
    await tick(env, policy)
    assert not policy.calls and not (await state(env))["tasks"]


@pytest.mark.parametrize("verdict", ["block", "flag", "error", "fallback"])
@pytest.mark.parametrize("drift", ["closed", "busy"])
async def test_policy_failure_durably_holds_despite_runtime_drift(owned_env, verdict, drift):
    env = owned_env

    async def action():
        async with env.factory() as db:
            if drift == "closed":
                (await db.get(RuntimeSchedule, "videoprocess")).state = "CLOSED"
            else:
                db.add(ChannelOpsQueueItem(kind="cleanup_expired", idempotency_key="new-work", payload_json={}))
            await db.commit()
        if verdict == "error":
            raise RuntimeError("credential-must-not-be-audited")

    policy = Policy(action, "allow" if verdict in {"fallback", "error"} else verdict,
                    {"warning": "pds_unavailable", "fail_policy": "allow"} if verdict == "fallback" else None)
    await tick(env, policy)
    result = await state(env)
    assert result["inventory"].state == "held"
    assert result["inventory"].hold_reason == "owned_inventory_pds_denied"
    assert result["channel"].intake_paused_at is not None
    assert not result["tasks"] and all(i.state == "unused" for i in result["items"])
    assert "credential-must-not-be-audited" not in repr(result["audits"][0].decision_summary_json)
    await tick(env, policy)
    assert len(policy.calls) == 1


async def test_policy_runs_without_database_transaction(owned_env):
    async with owned_env.factory() as db:
        async def action():
            assert not db.in_transaction()
        await ChannelAgentService(pds_client=Policy(action)).tick(db, channel_id=owned_env.channel_id)


async def test_policy_advisory_allows_atomic_admission_and_replay(owned_env):
    policy = Policy(metadata={"warning": "advisory"})
    audit = await tick(owned_env, policy)
    result = await state(owned_env)
    assert audit.tasks_selected == len(result["tasks"]) == 1
    task, first = result["tasks"][0], result["items"][0]
    assert first.state == "reserved" and first.production_task_id == task.id
    assert task.agent_approval_evidence_json["candidate_pds"]["metadata"] == {"warning": "advisory"}
    assert sum(s.status == "exhausted" for s in result["seeds"]) == 1
    assert len(result["decisions"]) == 1 and result["decisions"][0].created_task_id == task.id
    assert [(q.kind, q.idempotency_key) for q in result["queue"]] == [("plan_task", f"plan_task:{task.id}")]
    await tick(owned_env, policy)
    assert len(policy.calls) == len((await state(owned_env))["tasks"]) == 1


@pytest.mark.parametrize("metadata", [
    {"warning": "pds_disabled"}, {"warning": "pds_unavailable"}, {"warning": "pds_parse_failed"},
    {"warning": "advisory", "fail_policy": "allow"},
])
async def test_policy_fail_markers_hold_even_with_allow_verdict(owned_env, metadata):
    policy = Policy(metadata=metadata)
    audit = await tick(owned_env, policy)
    result = await state(owned_env)
    assert len(policy.calls) == 1 and audit.tasks_selected == 0
    assert result["inventory"].state == "held"
    assert result["inventory"].hold_reason == "owned_inventory_pds_denied"
    assert result["channel"].intake_paused_at is not None
    assert not result["tasks"] and not result["queue"]
    assert all(i.state == "unused" for i in result["items"])
    assert all(s.status == "active" for s in result["seeds"])


@pytest.mark.parametrize("close_mode", ["success", "error", "timeout"])
async def test_redis_cancel_never_reenters_or_finalizes(owned_env, monkeypatch, close_mode):
    env = owned_env
    item = await claimed_tick(env)
    entered = asyncio.Event()
    lock_calls, finish_calls, close_calls = [], [], []
    original_lock, original_finish = admission.lock_scope, admission.finish
    original_timeout = asyncio.timeout
    monkeypatch.setattr(asyncio, "timeout", lambda seconds: original_timeout(0.01 if seconds == 5 else seconds))

    class Reader:
        async def acl_whoami(self):
            entered.set()
            await asyncio.Future()

        async def aclose(self):
            close_calls.append(True)
            if close_mode == "error":
                raise RuntimeError("close-failed")
            if close_mode == "timeout":
                await asyncio.Future()

    async def locked(*args):
        lock_calls.append(True)
        return await original_lock(*args)

    async def needs_observation(db, phase, observation):
        if observation is None:
            raise admission.ObservationRequired(admission.RedisRequest("fixture", phase.now, ()))
        phase.hold = "owned_history_redis_close_failed"

    async def finished(*args):
        finish_calls.append(True)
        return await original_finish(*args)

    monkeypatch.setattr(admission, "lock_scope", locked)
    monkeypatch.setattr(admission, "assess", needs_observation)
    monkeypatch.setattr(admission, "finish", finished)
    monkeypatch.setattr(inventory, "_history_redis", Reader)
    monkeypatch.setattr(settings, "redis_url", "redis://history-reader:fixture@127.0.0.1:55464/15")
    policy = Policy()
    task = asyncio.create_task(tick(env, policy, queue_item=item))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel("cancel-owned-observation")
        with pytest.raises(asyncio.CancelledError, match="cancel-owned-observation"):
            await task
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert lock_calls == close_calls == [True]
    assert not finish_calls and not policy.calls
    result = await state(env)
    assert result["inventory"].state == "approved" and result["channel"].intake_paused_at is None
    assert not result["tasks"] and not result["audits"] and not result["decisions"]
    assert all(i.state == "unused" for i in result["items"])
    assert all(s.status == "active" for s in result["seeds"])
    assert len(result["queue"]) == 1 and result["queue"][0].status == "running"


@pytest.mark.parametrize("close_mode", ["error", "timeout"])
async def test_redis_ordinary_close_failure_remains_an_error(monkeypatch, close_mode):
    original_timeout = asyncio.timeout
    monkeypatch.setattr(asyncio, "timeout", lambda seconds: original_timeout(0.01 if seconds == 5 else seconds))

    class Reader:
        async def acl_whoami(self):
            return "history-reader"

        async def aclose(self):
            if close_mode == "error":
                raise RuntimeError("close-failed")
            await asyncio.Future()

    monkeypatch.setattr(inventory, "_history_redis", Reader)
    monkeypatch.setattr(settings, "redis_url", "redis://history-reader:fixture@127.0.0.1:55464/15")
    with pytest.raises(inventory.OwnedInventoryError, match="owned_history_redis_close_failed"):
        await admission.observe_redis(admission.RedisRequest("fixture", NOW, ()))


async def test_profile_scheduler_minute_and_inactive_pointer_no_rewrite(owned_env):
    env = owned_env
    async with env.factory() as db:
        scheduler = ChannelOpsScheduler()
        now = datetime.now(timezone.utc)
        assert (await scheduler.run_once(db, now=now)).enqueued_count == 1
        assert (await scheduler.run_once(db, now=now + timedelta(minutes=1))).enqueued_count == 1
        inv = await db.get(OwnedSeedInventory, env.inventory_id)
        inv.state = "held"
        await db.commit()
        assert (await scheduler.run_once(db, now=now + timedelta(minutes=2))).enqueued_count == 0
        assert (await db.get(ChannelProfile, env.channel_id)).tick_interval_minutes == 1


async def claimed_tick(env):
    async with env.factory() as db:
        queue = ChannelOpsQueueService()
        await queue.enqueue(db, kind="agent_tick", idempotency_key=f"agent_tick:{uuid.uuid4()}",
                            payload={"channel_id": str(env.channel_id)}, channel_profile_id=env.channel_id)
        return await queue.claim_next(db, worker_id="python-contender")


@pytest.mark.parametrize("mutation", ["owner", "time", "payload", "status"])
async def test_actual_python_handler_refuses_lost_queue_lease(owned_env, mutation):
    env = owned_env
    item = await claimed_tick(env)

    async def change():
        async with env.factory() as db:
            queue = await db.get(ChannelOpsQueueItem, item.id)
            if mutation == "owner":
                queue.locked_by = "other-contender"
            elif mutation == "time":
                queue.locked_at += timedelta(seconds=1)
            elif mutation == "payload":
                queue.payload_json = {**queue.payload_json, "unexpected": True}
            else:
                queue.status = "queued"
            await db.commit()

    runner = object.__new__(ChannelAgentRunner)
    runner.service = ChannelAgentService(pds_client=Policy(change))
    async with env.factory() as db:
        with pytest.raises(ValueError, match="owned_inventory_queue_authority"):
            await runner.handle_item(db, item)
    assert not (await state(env))["tasks"]


async def test_plan_enqueue_failure_rolls_back_whole_selection(owned_env):
    class BrokenQueue(ChannelOpsQueueService):
        async def enqueue(self, *args, **kwargs):
            raise RuntimeError("injected-plan-enqueue-failure")

    env = owned_env
    async with env.factory() as db:
        with pytest.raises(RuntimeError, match="injected-plan"):
            await ChannelAgentService(queue=BrokenQueue(), pds_client=Policy()).tick(db, channel_id=env.channel_id)
    result = await state(env)
    assert not result["tasks"] and not result["audits"] and not result["queue"]
    assert all(i.state == "unused" for i in result["items"])
    assert all(s.status == "active" for s in result["seeds"])


async def test_runner_does_not_retry_or_finish_after_owned_commit_response_loss(owned_env, monkeypatch):
    env = owned_env
    item = await claimed_tick(env)
    async with env.factory() as db:
        commit = db.commit

        async def lost():
            await commit()
            raise RuntimeError("committed-response-lost")

        monkeypatch.setattr(db, "commit", lost)
        with pytest.raises(RuntimeError, match="committed-response-lost"):
            await ChannelAgentService(pds_client=Policy()).tick(db, channel_id=env.channel_id, queue_item=item)
    result = await state(env)
    assert len(result["tasks"]) == 1
    assert next(q for q in result["queue"] if q.id == item.id).status == "succeeded"
    policy = Policy()
    await tick(env, policy)
    assert not policy.calls and len((await state(env))["tasks"]) == 1


@pytest.mark.parametrize("mode", ["normal", "owner_loss", "commit_response_loss"])
async def test_run_once_respects_owned_atomic_completion_and_lost_authority(owned_env, monkeypatch, mode):
    env = owned_env
    queue = ChannelOpsQueueService()
    async with env.factory() as db:
        engine = db.bind
        item = await queue.enqueue(db, kind="agent_tick", idempotency_key="actual-run-once",
            payload={"channel_id": str(env.channel_id)}, channel_profile_id=env.channel_id)

    class ResponseLossSession(AsyncSession):
        async def commit(self):
            await super().commit()
            if mode == "commit_response_loss" and self.info.get("owned_tick_lease"):
                raise RuntimeError("committed-response-lost")

    monkeypatch.setattr(runner_module, "async_session", async_sessionmaker(engine, class_=ResponseLossSession,
                                                                          expire_on_commit=False))

    async def action():
        if mode == "owner_loss":
            async with env.factory() as db:
                claimed = await db.get(ChannelOpsQueueItem, item.id)
                claimed.locked_by = "replacement-owner"
                await db.commit()

    runner = object.__new__(ChannelAgentRunner)
    runner.worker_id, runner.queue = "real-python-runner", queue
    runner.service = ChannelAgentService(queue=queue, pds_client=Policy(action))
    assert await runner.run_once(run_scheduler_when_idle=False)
    result = await state(env)
    stored = next(q for q in result["queue"] if q.id == item.id)
    if mode == "owner_loss":
        assert not result["tasks"] and stored.status == "running" and stored.locked_by == "replacement-owner"
    else:
        assert len(result["tasks"]) == 1 and stored.status == "succeeded" and stored.attempt_count == 1
        assert stored.last_error is None


class NativeReader:
    def __init__(self, rows):
        self.markers = {"vp:worker-task-dispatch:" + d["dispatch_key"]: d["redis_message_id"]
                        for d in rows["worker_task_dispatches"]}
        self.calls = []
        self.pending = False
        self.identity = "history-reader"

    async def acl_whoami(self):
        self.calls.append("whoami")
        return self.identity

    async def get(self, key):
        self.calls.append(("get", key))
        return self.markers[key]

    async def xpending_range(self, stream, group, start, end, count):
        self.calls.append(("pending", stream, group, start, end, count))
        return [{"message_id": start if start != "-" else "1-0"}] if self.pending else []

    async def aclose(self):
        self.calls.append("close")


@pytest.mark.parametrize("mode", ["clean", "pending", "marker", "identity", "read_error"])
async def test_native_retirement_reader_is_named_readonly_and_never_uses_saved_observations(monkeypatch, mode):
    rows, _ = retired_rows()
    request = admission.redis_request(snap(rows))
    native = NativeReader(rows)
    monkeypatch.setattr(inv := inventory, "_history_redis", lambda: native)
    monkeypatch.setattr(settings, "redis_url", "redis://history-reader:fixture@127.0.0.1:55464/15")
    if mode == "pending":
        native.pending = True
    if mode == "marker":
        native.markers = dict.fromkeys(native.markers, "999-1")
    if mode == "identity":
        native.identity = "other"
    if mode == "read_error":
        async def broken():
            raise RuntimeError("redis://credential-must-not-leak")
        native.acl_whoami = broken
    if mode in {"identity", "read_error"}:
        with pytest.raises(inv.OwnedInventoryError, match="^owned_history_redis_"):
            await admission.observe_redis(request)
    else:
        evidence = await admission.observe_redis(request)
        result = history.assess_owned_history(snap(rows, redis=evidence), now=NOW)
        assert (result.block_reason is None) == (mode == "clean")
    assert native.calls[-1] == "close"
    assert all(c in {"whoami", "close"} if isinstance(c, str) else c[0] in {"get", "pending"} for c in native.calls)


@pytest.mark.parametrize("drift", ["none", "pending", "marker", "graph", "new_blank"])
async def test_complete_history_and_retirement_reentry_during_policy(owned_env, monkeypatch, drift):
    env = owned_env
    legacy, _ = retired_rows()
    native = NativeReader(legacy)
    changed = False
    calls = 0

    async def loader(db, *, platform_channel_id):
        nonlocal calls
        calls += 1
        current = await sqlite_history(db, platform_channel_id=platform_channel_id)
        rows = current.rows.as_dict()
        old = copy.deepcopy(legacy)
        for name in rows:
            if name != "runtime_schedules":
                rows[name] += old[name]
        if changed and drift == "graph":
            rows["worker_task_dispatches"][0]["acknowledged_at"] = None
        if changed and drift == "new_blank":
            rows["youtube_upload_operations"].append({"id": str(uuid.uuid4()), "production_task_id": str(uuid.uuid4())})
        return history.OwnedHistorySnapshot.from_rows(rows, platform_channel_id=platform_channel_id,
                                                     observed_at=current.observed_at)

    monkeypatch.setattr(history, "load_owned_history_evidence", loader)
    monkeypatch.setattr(inventory, "_history_redis", lambda: native)
    monkeypatch.setattr(settings, "redis_url", "redis://history-reader:fixture@127.0.0.1:55464/15")

    async def policy_action():
        nonlocal changed
        changed = True
        if drift == "pending":
            native.pending = True
        if drift == "marker":
            native.markers = dict.fromkeys(native.markers, "999-1")

    policy = Policy(policy_action)
    await tick(env, policy)
    result = await state(env)
    assert len(policy.calls) == 1
    assert bool(result["tasks"]) == (drift == "none")
    assert native.calls.count("whoami") >= 2 and calls >= 5
    if drift != "none":
        assert result["inventory"].state == "held"


@pytest.mark.parametrize("mode", ["complete", "completion_floor", "attempt_floor", "pending_promotion", "retry"])
def test_c_reuses_a1_effect_digest_and_both_floors(mode):
    from tests.services.test_owned_seed_inventory_history import metric_retry
    rows = completed_rows()
    if mode == "completion_floor":
        rows = completed_rows(start=NOW - timedelta(hours=23))
        rows["youtube_upload_operations"][0]["request_attempted_at"] = (NOW - timedelta(hours=26)).isoformat()
    elif mode == "attempt_floor":
        rows = completed_rows(start=NOW - timedelta(hours=23))
    elif mode == "retry":
        metric_retry(rows)
    elif mode == "pending_promotion":
        rows = completed_rows(own=True)
        rows["publication_records"][0]["scheduled_publish_at"] = None
    result = history.assess_owned_history(snap(rows), now=NOW)
    if mode in {"completion_floor", "attempt_floor"}:
        assert result.wait_reason == "owned_inventory_cooldown"
    elif mode in {"complete", "retry"}:
        assert result.block_reason is None
        assert len(result.stable_history_sha256) == 64
        assert admission.queues_safe(rows, result, rows["channel_profiles"][0]["id"], NOW)
    else:
        assert result.block_reason is not None or result.wait_reason is not None
