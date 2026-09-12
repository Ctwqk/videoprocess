"""Finite inventory admission. External observations never own a DB transaction."""
from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import unquote, urlsplit

from sqlalchemy import select, text, update

from app.channel_agent.service import _is_pds_fail_policy_decision
from app.models.asset import Asset
from app.models.channel_agent import (
    AgentTickAudit, ChannelOpsQueueItem, ChannelProfile, DecisionAuditEntry,
    LaneFormatMatrix, ManualSeed, PublishingAccount, TopicLane,
)
from app.models.owned_seed_inventory import OwnedSeedInventory, OwnedSeedInventoryItem
from app.models.schedule import RuntimeSchedule
from app.node_registry.registry import NodeTypeRegistry
from app.pds_client import PDSDecisionRequest
from app.services import owned_seed_inventory as inv
from app.services import owned_seed_inventory_history as history


class OwnedQueueAuthorityLost(ValueError):
    pass


@dataclass(frozen=True)
class QueueLease:
    id: uuid.UUID
    owner: str
    locked_at: datetime
    payload_sha: str

    @classmethod
    def capture(cls, item):
        if item is None:
            return None
        if item.status != "running" or not item.locked_by or item.locked_at is None:
            raise OwnedQueueAuthorityLost("owned_inventory_queue_authority")
        return cls(item.id, item.locked_by, inv.utc(item.locked_at), inv.sha256(item.payload_json))

    def matches(self, item):
        return (item is not None and item.kind == "agent_tick" and item.status == "running"
                and item.locked_by == self.owner and item.locked_at is not None
                and inv.utc(item.locked_at) == self.locked_at and inv.sha256(item.payload_json) == self.payload_sha)


@dataclass
class Phase:
    channel: Any
    inventory: Any
    now: datetime
    items: list = field(default_factory=list)
    candidate: dict | None = None
    unused_id: str = ""
    hold: str | None = None
    wait: str | None = None
    digest: str = ""
    evidence: dict = field(default_factory=dict)
    completed: tuple[str, ...] = ()
    consumed: int = 0


@dataclass(frozen=True)
class Prepared:
    inventory_id: uuid.UUID
    unused_id: str
    digest: str
    request: PDSDecisionRequest | None


@dataclass(frozen=True)
class RedisRequest:
    digest: str
    observed_at: datetime
    sources: tuple[dict, ...]


class ObservationRequired(Exception):
    def __init__(self, request):
        self.request = request


def redis_request(snapshot):
    sources = inv._retirement_sources(snapshot, requested=False)
    if sources is None:
        return None
    _, cert, _ = history._approved_authority(snapshot.rows.as_dict(), snapshot.observed_at)
    graph = sources["terminal_graph"]
    inv.require(history._terminal_projection(graph) == history._terminal_projection(cert.terminal_graph.as_dict()),
                "owned_history_retired_changed")
    result = []
    nodes = {n["id"]: n for n in graph["node_executions"]}
    for row in graph["worker_task_dispatches"]:
        node = NodeTypeRegistry.get().get_type(nodes[row["node_execution_id"]]["node_type"])
        inv.require(node is not None and row["redis_stream"] == f"vp:tasks:{node.worker_type}"
                    and row["consumer_group"] == f"{node.worker_type}-workers", "owned_history_retired_changed")
        result.append(dict(kind="task", redis_stream=row["redis_stream"], consumer_group=row["consumer_group"],
                           message_id=row["redis_message_id"], dispatch_key=row["dispatch_key"],
                           payload_sha256=row["payload_sha256"]))
    events = {}
    for row in graph["registered_worker_event_deliveries"]:
        inv.require(row["redis_stream"] == "vp:events" and row["consumer_group"] == "orchestrator"
                    and row["message_id"] is not None, "owned_history_retired_changed")
        key = (row["redis_stream"], row["consumer_group"], row["message_id"])
        inv.require(key not in events or events[key] == row["payload_sha256"], "owned_history_retired_changed")
        events[key] = row["payload_sha256"]
    for (stream, group, message), sha in sorted(events.items()):
        result.append(dict(kind="event", redis_stream=stream, consumer_group=group,
                           message_id=message, dispatch_key=None, payload_sha256=sha))
    for source in result:
        history.RedisTerminalObservation.parse({**source, "marker_message_id": None,
            "pending_message_ids": [], "observed_at": snapshot.observed_at.isoformat()})
    return RedisRequest(inv.sha256(result), snapshot.observed_at, tuple(result))


async def observe_redis(request):
    client = None
    cancelled = False
    try:
        redis_url = inv._history_redis_url()
        url = urlsplit(redis_url)
        principal = unquote(url.username or "")
        inv.require(url.scheme in {"redis", "rediss"} and bool(url.hostname) and not url.query and not url.fragment
                    and principal not in {"", "default"} and bool(url.password)
                    and (url.path in {"", "/"} or url.path[1:].isdigit() and 0 <= int(url.path[1:]) <= 15),
                    "owned_history_redis_configuration")
        async with asyncio.timeout(30):
            client = inv._history_redis(redis_url)
            inv.require(await client.acl_whoami() == principal, "owned_history_redis_identity")
            result = []
            for source in request.sources:
                marker = None
                if source["kind"] == "task":
                    marker = await client.get("vp:worker-task-dispatch:" + source["dispatch_key"])
                message = source["message_id"]
                pending = await client.xpending_range(source["redis_stream"], source["consumer_group"],
                                                       message or "-", message or "+", 1)
                inv.require(type(pending) is list and len(pending) <= 1, "owned_history_redis_read_failed")
                result.append(history.RedisTerminalObservation.parse({**source, "marker_message_id": marker,
                    "pending_message_ids": [p["message_id"] for p in pending],
                    "observed_at": request.observed_at.isoformat()}))
            return tuple(result)
    except asyncio.CancelledError:
        cancelled = True
        raise
    except (inv.OwnedInventoryError, history.OwnedHistoryError):
        raise
    except Exception:
        raise inv.OwnedInventoryError("owned_history_redis_read_failed") from None
    finally:
        if client is not None:
            try:
                async with asyncio.timeout(5):
                    await client.aclose()
            except Exception:
                # Cleanup must not turn cancellation into a durable admission hold.
                if not cancelled:
                    raise inv.OwnedInventoryError("owned_history_redis_close_failed") from None


def production_target(rows, row, now):
    bindings, cert, _ = history._approved_authority(rows, now)
    target, channel = str(row.target_account_id), str(row.channel_profile_id)
    inv.require(all(key != target and binding.legacy_channel_profile_id != channel for key, binding in bindings.items())
                and (cert is None or cert.legacy_account_id != target and cert.legacy_channel_profile_id != channel),
                "owned_inventory_history_target")
    production = [a["id"] for a in rows["publishing_accounts"] if inv.is_youtube_platform(a["platform"])
                  and a["platform_account_id"] == row.platform_channel_id and a["id"] not in bindings]
    inv.require(production == [target], "owned_inventory_account_alias")


def queues_safe(rows, assessment, channel_id, now):
    allowed = {}
    try:
        for task in rows["production_tasks"]:
            if task["target_account_id"] not in assessment.account_ids:
                continue
            facts = history._task_history(rows, task)
            if len(facts["publications"]) != 1 or facts["publications"][0]["scheduled_publish_at"] is None:
                continue
            pub = facts["publications"][0]
            history._metrics_ready(facts, pub, history._time(pub["scheduled_publish_at"]), now)
            allowed.update({q["id"]: q for q in facts["queues"] if q["kind"] == "collect_metrics"
                            and q["payload_json"].get("metric_schedule_id") is not None})
        active = [q for q in rows["channel_ops_queue_items"] if q["status"] in {"queued", "running"}]
        if len(active) > 1024:
            return False
        return all(q["kind"] == "agent_tick" and q["channel_profile_id"] == channel_id
                   and q["payload_json"].get("channel_id") == channel_id and history._queue_clean(q)
                   or q == allowed.get(q["id"]) for q in active)
    except (history.OwnedHistoryError, KeyError, TypeError):
        return False


async def lock_scope(db, channel_id, inventory_id, lease):
    # Match Go's outer queue authority fence before entering the domain lock order.
    if lease is not None:
        queue = (await db.scalars(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.id == lease.id)
            .with_for_update().execution_options(populate_existing=True))).one_or_none()
        if (not lease.matches(queue) or queue.channel_profile_id != channel_id
                or queue.payload_json.get("channel_id") != str(channel_id)):
            raise OwnedQueueAuthorityLost("owned_inventory_queue_authority")
    channel = await inv._row(db, ChannelProfile, channel_id, lock=True)
    inv.require(channel.owned_seed_inventory_id == inventory_id, "owned_inventory_pointer_changed")
    await inv.lock_history_schedule(db)
    platform = await db.scalar(select(OwnedSeedInventory.platform_channel_id).where(OwnedSeedInventory.id == inventory_id))
    inv.require(platform is not None, "owned_inventory_reference_missing")
    await inv.lock_platform_scope(db, platform)
    row = await inv._row(db, OwnedSeedInventory, inventory_id, lock=True)
    inv.require(row.platform_channel_id == platform and row.channel_profile_id == channel_id, "owned_inventory_scope")
    return Phase(channel, row, await inv._now(db))


async def assess(db, phase, observation):
    row, channel, now = phase.inventory, phase.channel, phase.now
    if row.state != "approved":
        phase.wait = "owned_inventory_terminal"
        return
    inv.require(row.approved_at is not None and inv.utc(row.approved_at) <= now and bool((row.approved_by or "").strip())
                and bool((row.approval_reference or "").strip()) and row.revoked_at is None
                and row.succession_released_at is None, "owned_inventory_approval")
    starts, expires = inv.utc(row.starts_at), inv.utc(row.expires_at)
    inv.require(expires - starts == timedelta(days=7), "owned_inventory_window")
    inv.require(now < expires, "owned_inventory_expired")
    if now < starts:
        phase.wait = "owned_inventory_not_started"
        return
    inv.require(row.privacy == "unlisted" and row.max_admissions == 7 and row.minimum_interval_seconds == 86400
                and channel.tick_interval_minutes == 1, "owned_inventory_limits")
    account = await inv._row(db, PublishingAccount, row.target_account_id, lock=True)
    lane = await inv._row(db, TopicLane, row.topic_lane_id, lock=True)
    fmt = await inv._row(db, LaneFormatMatrix, row.lane_format_id, lock=True)
    inv.require(channel.enabled and not channel.dry_run and account.channel_profile_id == channel.id
                and inv.is_youtube_platform(account.platform) and account.platform_account_id == row.platform_channel_id
                and account.enabled and account.paused_until is None and account.default_privacy == "unlisted"
                and not account.external_asset_auto_publish and lane.channel_profile_id == channel.id
                and lane.enabled and lane.paused_until is None and fmt.topic_lane_id == lane.id and fmt.enabled
                and fmt.default_publish_visibility == "unlisted" and fmt.source_platforms_json == [], "owned_inventory_binding")
    config_sha = inv.configuration_sha256(channel, account, lane, fmt)
    history.decode_history_manifest(row.manifest_json)
    inv.require(row.manifest_json["configuration_sha256"] == config_sha, "owned_inventory_configuration")
    phase.items = await inv.inventory_items(db, row.id, lock=True)
    seeds = {}
    for item in phase.items:
        seed = await inv._row(db, ManualSeed, item.manual_seed_id, lock=True)
        seeds[item.id] = seed
        asset = await inv._row(db, Asset, item.asset_id, lock=True)
        inv.require(seed.channel_profile_id == channel.id and seed.target_account_id == account.id
                    and seed.topic_lane_id == lane.id and seed.source_policy == "owned_only"
                    and seed.source_platforms_json == [] and seed.material_library_ids_json == []
                    and seed.constraints_json.get("input_asset_id") == str(asset.id)
                    and seed.constraints_json.get("source_strategy") == "input_video"
                    and seed.constraints_json.get("planning_mode") == "template", "owned_inventory_seed_binding")
        inv.require(asset.media_info.get("license") == "owned" and asset.media_info.get("provenance") == "generated"
                    and inv.canonical(inv.asset_descriptor(asset)) == inv.canonical(item.storage_descriptor_json)
                    and item.byte_size == asset.file_size, "owned_inventory_asset_changed")
        inv.require(item.platform_channel_id == row.platform_channel_id, "owned_inventory_item_binding")
        if item.state == "unused":
            inv.require(item.production_task_id is None and item.consumed_at is None and seed.status == "active",
                        "owned_inventory_consumption")
            phase.unused_id = phase.unused_id or str(item.id)
        elif item.state in {"reserved", "completed"}:
            inv.require(not phase.unused_id, "owned_inventory_ordinal_gap")
            inv.require(item.production_task_id is not None and item.consumed_at is not None
                        and starts <= inv.utc(item.consumed_at) < expires and inv.utc(item.consumed_at) <= now
                        and seed.status == "exhausted", "owned_inventory_consumption")
            phase.consumed += 1
        else:
            raise inv.OwnedInventoryError("owned_inventory_item_held")
    await inv._verify_manifest(db, row)
    inv.require(sum(i.state == "reserved" for i in phase.items) <= 1, "owned_inventory_multiple_outstanding")
    inv.require(len({i.asset_id for i in phase.items}) == len({i.content_sha256 for i in phase.items}) == 7,
                "owned_inventory_item_binding")
    snapshot = await history.load_owned_history_evidence(db, platform_channel_id=row.platform_channel_id)
    request = redis_request(snapshot)
    if request is not None:
        if observation is None or request.digest != observation[0].digest or not (
                0 <= (snapshot.observed_at - observation[0].observed_at).total_seconds() <= 60):
            raise ObservationRequired(request)
        if isinstance(observation[1], str):
            raise inv.OwnedInventoryError(observation[1])
        snapshot = replace(snapshot, redis_observations=observation[1])
    phase.now = now = await inv._now(db)
    rows = snapshot.rows.as_dict()
    production_target(rows, row, now)
    assessment = history.assess_owned_history(snapshot, now=now)
    inv.require(assessment.block_reason is None, assessment.block_reason or "owned_history_invalid")
    excluded = assessment.retired_source_sha256 + assessment.retired_render_sha256
    inv.require(all(i.content_sha256 not in excluded for i in phase.items), "owned_inventory_retired_hash_reuse")
    phase.completed = assessment.completed_item_ids
    phase.evidence = dict(inventory_id=str(row.id), item_id=phase.unused_id, manifest_sha256=row.manifest_sha256,
        configuration_sha256=config_sha, history_sha256=assessment.stable_history_sha256,
        history_authority_sha256=assessment.authority_sha256, retired_source_sha256=list(assessment.retired_source_sha256),
        retired_render_sha256=list(assessment.retired_render_sha256))
    schedule = await db.get(RuntimeSchedule, "videoprocess")
    busy = (any(j["status"] in {"PENDING", "WAITING_WINDOW", "VALIDATING", "PLANNING", "RUNNING"} for j in rows["jobs"])
            or any(n["status"] in {"QUEUED", "RUNNING"} for n in rows["node_executions"])
            or any(t["state"] not in {"scheduled", "uploaded_private", "measured", "held", "failed", "rejected"}
                   for t in rows["production_tasks"]) or not queues_safe(rows, assessment, str(channel.id), now))
    phase.wait = assessment.wait_reason
    if not phase.wait and not phase.unused_id:
        phase.wait = "owned_inventory_exhausted"
    if not phase.wait and (schedule.state != "OPEN" or schedule.guarded_job_id is not None or busy
                           or channel.intake_paused_at is not None or channel.halted_at is not None):
        phase.wait = "owned_inventory_runtime_not_ready"
    if phase.wait is None:
        item = next(i for i in phase.items if str(i.id) == phase.unused_id)
        seed = seeds[item.id]
        phase.evidence.update(input_asset_id=str(item.asset_id), source_content_sha256=item.content_sha256,
                              seed_sha256=item.seed_sha256)
        phase.candidate = dict(source="manual_seed", seed=seed, account=account, lane=lane, lane_format=fmt,
            prompt=seed.prompt, title_seed=seed.title_seed, source_platforms_json=[], material_library_ids_json=[])
    phase.digest = inv.sha256(dict(inventory={c.name: getattr(row, c.name) for c in row.__table__.columns},
        unused_item_id=phase.unused_id, history_sha=assessment.stable_history_sha256,
        authority_sha=assessment.authority_sha256, retired_source_sha=assessment.retired_source_sha256,
        retired_render_sha=assessment.retired_render_sha256, ready=phase.candidate is not None))


async def read_phase(db, channel_id, inventory_id, lease):
    observation = None
    for attempt in range(2):
        phase = await lock_scope(db, channel_id, inventory_id, lease)
        try:
            await assess(db, phase, observation)
            return phase
        except ObservationRequired as needed:
            await db.rollback()
            if attempt:
                phase = await lock_scope(db, channel_id, inventory_id, lease)
                phase.hold = "owned_history_observation_stale"
                return phase
            try:
                evidence = await observe_redis(needed.request)
            except (inv.OwnedInventoryError, history.OwnedHistoryError) as error:
                evidence = str(error)
            observation = (needed.request, evidence)
        except (inv.OwnedInventoryError, history.OwnedHistoryError) as error:
            if str(error) == "owned_history_read_failed":
                await db.rollback()
                phase = await lock_scope(db, channel_id, inventory_id, lease)
            phase.hold = str(error)
            return phase
        except (ValueError, TypeError, KeyError, AttributeError, IndexError):
            phase.hold = "owned_inventory_invalid"
            return phase
    raise AssertionError("unreachable")


def prepare(phase):
    request = None
    if phase.candidate is not None:
        candidate = phase.candidate
        request = PDSDecisionRequest(actor_id=str(candidate["account"].id), action_type="candidate_accept", platform="youtube",
            content={"title": candidate["title_seed"], "description": candidate["prompt"]},
            context={"channel_profile_id": str(phase.channel.id),
                     "candidate_id": f"owned_inventory:{phase.inventory.id}:{phase.unused_id}",
                     "source_kind": "manual_seed", "topic_lane_id": str(candidate["lane"].id),
                     "lane_format_id": str(candidate["lane_format"].id), "owned_inventory": dict(phase.evidence)})
    return Prepared(phase.inventory.id, phase.unused_id, phase.digest, request)


async def finish(db, service, phase, before, decision, denied, lease):
    row, channel, now = phase.inventory, phase.channel, phase.now
    reason = phase.hold
    if row.state == "approved" and not reason and before.request and before.unused_id == phase.unused_id and denied:
        reason = "owned_inventory_pds_denied"
    if not reason and phase.candidate is not None and before.digest != phase.digest:
        reason = "owned_inventory_inputs_changed"
    if not reason and phase.candidate is not None and (before.request is None or denied):
        reason = "owned_inventory_pds_denied"
    task = None
    if reason and row.state == "approved":
        row.state = "expired" if reason == "owned_inventory_expired" else "held"
        row.hold_reason, row.updated_at = reason, now.replace(tzinfo=None)
        channel.intake_paused_at, channel.intake_pause_reason = now, reason
    elif not reason:
        for item in phase.items:
            if str(item.id) in phase.completed and item.state == "reserved":
                item.state, item.completed_at = "completed", now
        if phase.candidate is not None:
            task = service._task_from_candidate(channel, phase.candidate, created_at=now)
            task.approval_mode = "agent"
            task.agent_approval_evidence_json = {"owned_inventory": phase.evidence, "candidate_pds": decision,
                                                 "candidate_pds_request": asdict(before.request)}
            task.channel_config_snapshot_json = {**task.channel_config_snapshot_json, "owned_inventory": phase.evidence}
            task.state_updated_at = now
            task.transition_history_json = [{**entry, "at": now.isoformat()} for entry in task.transition_history_json]
            db.add(task)
            await db.flush()
            selected = next(i for i in phase.items if str(i.id) == phase.unused_id)
            await reserve_item(db, selected.id, task.id, row)
            phase.candidate["seed"].status = "exhausted"
            phase.candidate["seed"].updated_at = now.replace(tzinfo=None)
            await service.queue.enqueue(db, kind="plan_task", idempotency_key=f"plan_task:{task.id}",
                payload={"production_task_id": str(task.id), "channel_id": str(channel.id)}, priority=100,
                run_after=now, channel_profile_id=channel.id, commit=False)
            if phase.consumed == 6:
                row.state, row.updated_at = "exhausted", now.replace(tzinfo=None)
                channel.intake_paused_at, channel.intake_pause_reason = now, "owned_inventory_exhausted"
    audit = AgentTickAudit(channel_profile_id=channel.id, queue_item_id=lease.id if lease else None,
        tick_id=f"owned_inventory:{uuid.uuid4()}", started_at=now, finished_at=now, dry_run=channel.dry_run,
        candidates_scored=int(before.request is not None), tasks_selected=int(task is not None), tasks_rejected=int(bool(reason)),
        guards_triggered_json=[reason] if reason else [],
        decision_summary_json={"handler_version": "python", "owned_inventory_id": str(row.id),
                               "hold_reason": reason, "wait_reason": phase.wait})
    db.add(audit)
    await db.flush()
    if before.request:
        db.add(DecisionAuditEntry(tick_audit_id=audit.id, channel_profile_id=channel.id,
            candidate_id=before.request.context["candidate_id"], candidate_source="manual_seed",
            topic_lane_id=row.topic_lane_id, lane_format_id=row.lane_format_id, target_account_id=row.target_account_id,
            selected=task is not None, rejection_reason=reason, created_task_id=task.id if task else None,
            pds_decision_json=decision, created_at=now))
    if lease:
        await db.execute(update(ChannelOpsQueueItem).where(ChannelOpsQueueItem.id == lease.id).values(status="succeeded", last_error=None))
    await db.commit()
    await db.refresh(audit)
    return audit


async def reserve_item(db, item_id, task_id, row):
    if db.get_bind().dialect.name == "postgresql":
        result = await db.execute(text("""
            WITH reservation_clock AS MATERIALIZED (SELECT clock_timestamp() AS at)
            UPDATE owned_seed_inventory_items item
            SET state='reserved', production_task_id=:task_id, consumed_at=c.at
            FROM owned_seed_inventories i, reservation_clock c
            WHERE item.id=:item_id AND item.state='unused' AND item.production_task_id IS NULL
              AND i.id=item.inventory_id AND i.state='approved' AND c.at>=i.starts_at AND c.at<i.expires_at
            RETURNING item.id
        """), {"item_id": item_id, "task_id": task_id})
    else:
        at = await inv._now(db)
        inv.require(inv.utc(row.starts_at) <= at < inv.utc(row.expires_at), "owned_inventory_expired")
        result = await db.execute(update(OwnedSeedInventoryItem).where(OwnedSeedInventoryItem.id == item_id,
            OwnedSeedInventoryItem.state == "unused", OwnedSeedInventoryItem.production_task_id.is_(None))
            .values(state="reserved", production_task_id=task_id, consumed_at=at)
            .returning(OwnedSeedInventoryItem.id).execution_options(synchronize_session=False))
    inv.require(result.scalar_one_or_none() is not None, "owned_inventory_consumption")


async def tick(db, service, *, channel_id, inventory_id, queue_item=None, plan_delay_seconds=0):
    inv.require(plan_delay_seconds == 0, "owned_inventory_plan_delay")
    inv.require(not db.new and not db.dirty and not db.deleted, "owned_inventory_pending_session_changes")
    lease = QueueLease.capture(queue_item)
    if lease is not None:
        db.info["owned_tick_lease"] = lease
    await db.rollback()
    try:
        phase = await read_phase(db, channel_id, inventory_id, lease)
        before = prepare(phase)
        if before.request is None:
            return await finish(db, service, phase, before, {}, False, lease)
        await db.rollback()
        # Recheck just before PDS; both this read and finalization obtain fresh native proof.
        phase = await read_phase(db, channel_id, inventory_id, lease)
        if phase.hold or phase.candidate is None or phase.digest != before.digest:
            if phase.candidate is not None and not phase.hold:
                phase.hold = "owned_inventory_inputs_changed"
            return await finish(db, service, phase, before, {}, False, lease)
        await db.rollback()
        decision, denied = {}, True
        try:
            result = await service.pds_client.decide(before.request)
            decision = json.loads(inv.canonical(asdict(result)))
            denied = result.verdict != "allow" or _is_pds_fail_policy_decision(result)
        except Exception:
            decision = {"verdict": "block", "reason": "owned_inventory_pds_unavailable"}
        phase = await read_phase(db, channel_id, inventory_id, lease)
        return await finish(db, service, phase, before, decision, denied, lease)
    except BaseException:
        await db.rollback()
        raise
