"""Inventory intake stops and read-only feedback accounting; never new authority."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from types import MappingProxyType
from typing import Mapping
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.services import owned_seed_inventory_history as history


@dataclass(frozen=True)
class OwnedInventoryFeedback:
    inventory_id: str
    hold_reason: str | None
    completed_item_ids: tuple[str, ...]
    metrics: Mapping[str, str | int]

    def __post_init__(self) -> None:
        object.__setattr__(self, "metrics", MappingProxyType(dict(self.metrics)))


def _blocked(inventory_id: UUID, reason: str) -> OwnedInventoryFeedback:
    return OwnedInventoryFeedback(str(inventory_id), reason, (), {
        "inventory_id": str(inventory_id), "inventory_intake_status": "closed",
        "inventory_settlement_status": "pending", "inventory_feedback_status": "blocked",
    })


def completed_owned_inventory_items(snapshot, inventory_id, *, now, publication_id=None) -> tuple[str, ...]:
    """Accounting only: inspect persisted normal outcomes, never retirement/POST authority."""
    try:
        history._require(0 <= (now - snapshot.observed_at).total_seconds() <= history.MAX_OBSERVATION_AGE_SECONDS,
                         "owned_history_observation_stale")
        rows = snapshot.rows.as_dict()
        row = history._one([r for r in rows["owned_seed_inventories"] if r["id"] == inventory_id], "owned_inventory_scope")
        data = history.decode_history_manifest(row["manifest_json"]).document.as_dict()
        history._require(row["manifest_sha256"] == history.history_sha256(data)
                         and data["inventory_id"] == inventory_id and data["platform_channel_id"] == snapshot.platform_channel_id
                         and all(row[k] == data[k] for k in ("channel_profile_id", "target_account_id", "platform_channel_id"))
                         and row.get("approved_at") is not None and history._time(row["approved_at"]) <= now,
                         "owned_inventory_manifest_changed")
        channel = history._one([c for c in rows["channel_profiles"] if c["id"] == row["channel_profile_id"]], "owned_inventory_scope")
        history._require(channel.get("halted_at") is None, "channel_halted")
        history._require(channel.get("enabled") is True and channel.get("dry_run") is False, "owned_inventory_channel_disabled")
        account = history._one([a for a in rows["publishing_accounts"] if a["id"] == row["target_account_id"]], "owned_inventory_scope")
        history._require(account["channel_profile_id"] == channel["id"] and (account.get("platform") or "youtube") == "youtube"
                         and account["platform_account_id"] == snapshot.platform_channel_id, "owned_inventory_scope")
        items = sorted((i for i in rows["owned_seed_inventory_items"] if i["inventory_id"] == inventory_id), key=lambda i: i["ordinal"])
        history._require(len(items) == 7 and sum(i["state"] == "reserved" for i in items) <= 1, "owned_inventory_cardinality_invalid")
        completed = []
        unused_seen = False
        for item, entry in zip(items, data["entries"]):
            history._require(all(item[k] == entry[k] for k in ("id", "ordinal", "asset_id", "manual_seed_id", "content_sha256"))
                             and item["platform_channel_id"] == snapshot.platform_channel_id, "owned_inventory_item_binding")
            if item["state"] == "unused":
                history._require(all(item.get(k) is None for k in ("production_task_id", "consumed_at", "completed_at")),
                                 "owned_inventory_consumption")
                unused_seen = True
                continue
            history._require(not unused_seen and item["state"] in {"reserved", "completed"}, "owned_inventory_consumption")
            consumed = history._time(item["consumed_at"])
            history._require(history._time(data["starts_at"]) <= consumed < history._time(data["expires_at"]) and consumed <= now,
                             "owned_inventory_consumption")
            history._require(item.get("completed_at") is None if item["state"] == "reserved" else
                             consumed <= history._time(item["completed_at"]) <= now, "owned_inventory_consumption")
            task = history._one([t for t in rows["production_tasks"] if t["id"] == item["production_task_id"]], "owned_inventory_missing_task")
            history._require(task["channel_profile_id"] == channel["id"] and task["target_account_id"] == account["id"]
                             and task["manual_seed_id"] == item["manual_seed_id"], "owned_inventory_history_identity")
            facts = history._task_history(rows, task)
            history._require(all(op["privacy"] == "unlisted" for op in facts["operations"]), "owned_inventory_receipt")
            _, complete, _ = history._normal_history(facts, item, now)
            if complete and (publication_id is None or any(p["id"] == publication_id for p in facts["publications"])):
                completed.append(item["id"])
        return tuple(completed)
    except history.OwnedHistoryError:
        raise
    except (KeyError, ValueError, TypeError, AttributeError, OverflowError):
        raise history.OwnedHistoryError("owned_inventory_history_invalid") from None


async def finalize_owned_inventory_items(
    db: AsyncSession, channel_id: UUID, *, publication_id: UUID | None = None,
) -> tuple[str, ...]:
    """Caller owns the queue fence and transaction, after persisting queue success."""
    from app.models.channel_agent import ChannelProfile, PublicationRecord
    from app.models.owned_seed_inventory import OwnedSeedInventory, OwnedSeedInventoryItem
    from app.services import owned_seed_inventory as inv

    inv.require(db.in_transaction(), "owned_inventory_transaction_required")
    channel = await inv._row(db, ChannelProfile, channel_id, lock=True)
    if publication_id is None:
        inventory_id = channel.owned_seed_inventory_id
    else:
        inventory_id = await db.scalar(select(OwnedSeedInventoryItem.inventory_id).join(
            PublicationRecord, PublicationRecord.production_task_id == OwnedSeedInventoryItem.production_task_id,
        ).where(PublicationRecord.id == publication_id))
    if inventory_id is None:
        return ()
    await inv.lock_history_schedule(db)
    platform = await db.scalar(select(OwnedSeedInventory.platform_channel_id).where(OwnedSeedInventory.id == inventory_id))
    if platform is None:
        raise inv.OwnedInventoryError("owned_inventory_reference_missing")
    await inv.lock_platform_scope(db, platform)
    row = await inv._row(db, OwnedSeedInventory, inventory_id, lock=True)
    inv.require(row.channel_profile_id == channel_id, "owned_inventory_scope")
    items = await inv.inventory_items(db, inventory_id, lock=True)
    snapshot = await history.load_owned_history_evidence(db, platform_channel_id=platform)
    now = await inv._now(db)
    try:
        completed = completed_owned_inventory_items(snapshot, str(inventory_id), now=now,
                                                   publication_id=str(publication_id) if publication_id else None)
    except (inv.OwnedInventoryError, history.OwnedHistoryError) as error:
        reason = str(error)
        if row.state in {"approved", "exhausted"}:
            row.state, row.hold_reason, row.updated_at = "held", reason, now.replace(tzinfo=None)
        if channel.owned_seed_inventory_id == inventory_id:
            channel.intake_paused_at = channel.intake_paused_at or now
            channel.intake_pause_reason = channel.intake_pause_reason or reason
        await db.flush()
        return ()
    changed = []
    for item in items:
        if item.state == "reserved" and str(item.id) in completed:
            item.state, item.completed_at = "completed", now
            changed.append(str(item.id))
    await db.flush()
    return tuple(changed)


async def check_owned_inventory_feedback(
    db: AsyncSession, channel_id: UUID, inventory_id: UUID, *,
    apply: bool = False, external_conditions: tuple[str, ...] = (),
) -> OwnedInventoryFeedback:
    from app.channel_agent import owned_inventory as admission
    from app.models.channel_agent import ChannelProfile, PublishingAccount, TopicLane, LaneFormatMatrix
    from app.models.owned_seed_inventory import OwnedSeedInventory
    from app.services import owned_seed_inventory as inv
    from app.services.channelops_soak_guard import ALLOWED_EXTERNAL_CONDITIONS

    if set(external_conditions) - ALLOWED_EXTERNAL_CONDITIONS:
        raise ValueError("unknown external condition code")
    observation = None
    result = _blocked(inventory_id, "owned_history_observation_stale")
    for _ in range(2):
        if apply:
            phase = await admission.lock_scope(db, channel_id, inventory_id, None)
            channel, row, now = phase.channel, phase.inventory, phase.now
        else:
            channel = await inv._row(db, ChannelProfile, channel_id)
            inv.require(channel.owned_seed_inventory_id == inventory_id, "owned_inventory_pointer_changed")
            row = await inv._row(db, OwnedSeedInventory, inventory_id)
            inv.require(row.channel_profile_id == channel_id, "owned_inventory_scope")
            now = await inv._now(db)
        try:
            await inv._verify_manifest(db, row)
            account = await inv._row(db, PublishingAccount, row.target_account_id)
            lane = await inv._row(db, TopicLane, row.topic_lane_id)
            fmt = await inv._row(db, LaneFormatMatrix, row.lane_format_id)
            # Emergency stops must be reported as such, not hidden by their config digest change.
            if channel.enabled and not channel.dry_run and channel.halted_at is None:
                inv.require(inv.configuration_sha256(channel, account, lane, fmt) == row.manifest_json["configuration_sha256"],
                            "owned_inventory_configuration_changed")
            snapshot = await history.load_owned_history_evidence(db, platform_channel_id=row.platform_channel_id)
            request = admission.redis_request(snapshot)
            if request is not None:
                if observation is None:
                    await db.rollback()
                    try:
                        evidence = await admission.observe_redis(request)
                    except (inv.OwnedInventoryError, history.OwnedHistoryError) as error:
                        evidence = str(error)
                    observation = (request, evidence)
                    continue
                prior, evidence = observation
                inv.require(request.digest == prior.digest
                            and 0 <= (snapshot.observed_at - prior.observed_at).total_seconds() <= history.MAX_OBSERVATION_AGE_SECONDS,
                            "owned_history_observation_stale")
                if isinstance(evidence, str):
                    raise inv.OwnedInventoryError(evidence)
                snapshot = replace(snapshot, redis_observations=evidence)
            elif observation is not None:
                raise inv.OwnedInventoryError("owned_history_observation_stale")
            now = await inv._now(db)
            result = assess_owned_inventory_feedback(snapshot, str(inventory_id), now=now,
                                                     external_conditions=external_conditions)
        except (inv.OwnedInventoryError, history.OwnedHistoryError) as error:
            result = _blocked(inventory_id, str(error))
            if str(error) == "owned_history_read_failed":
                await db.rollback()
                if apply:
                    phase = await admission.lock_scope(db, channel_id, inventory_id, None)
                    channel, row, now = phase.channel, phase.inventory, phase.now
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError):
            result = _blocked(inventory_id, "owned_inventory_history_invalid")
        if apply:
            items = await inv.inventory_items(db, inventory_id, lock=True)
            for item in items:
                if item.state == "reserved" and str(item.id) in result.completed_item_ids:
                    item.state, item.completed_at = "completed", now
            reason = result.hold_reason
            if reason and row.state in {"approved", "exhausted"}:
                row.state = "expired" if reason == "owned_inventory_expired" else "held"
                row.hold_reason, row.updated_at = reason, now.replace(tzinfo=None)
            if not reason and row.state == "approved" and all(i.state != "unused" for i in items):
                row.state, row.updated_at = "exhausted", now.replace(tzinfo=None)
            if reason or row.state in {"held", "expired", "revoked", "exhausted"}:
                channel.intake_paused_at = channel.intake_paused_at or now
                channel.intake_pause_reason = channel.intake_pause_reason or reason or f"owned_inventory_{row.state}"
            metrics = dict(result.metrics)
            metrics["inventory_state"] = row.state
            metrics["inventory_intake_status"] = "closed" if channel.intake_paused_at is not None else metrics.get("inventory_intake_status", "closed")
            for state in ("completed", "reserved", "held", "unused"):
                metrics[f"inventory_{state}_count"] = sum(i.state == state for i in items)
            result = replace(result, metrics=metrics)
            await db.commit()
        return result
    return result


def assess_owned_inventory_feedback(
    snapshot: history.OwnedHistorySnapshot,
    inventory_id: str,
    *,
    now: datetime,
    external_conditions: tuple[str, ...] = (),
) -> OwnedInventoryFeedback:
    from app.services.channelops_soak_guard import ALLOWED_EXTERNAL_CONDITIONS

    if set(external_conditions) - ALLOWED_EXTERNAL_CONDITIONS:
        raise ValueError("unknown external condition code")
    metrics: dict[str, str | int] = {
        "inventory_id": inventory_id,
        "inventory_state": "unknown",
        "inventory_item_count": 0,
        "inventory_completed_count": 0,
        "inventory_reserved_count": 0,
        "inventory_held_count": 0,
        "inventory_unused_count": 0,
        "inventory_intake_status": "closed",
        "inventory_settlement_status": "pending",
        "inventory_feedback_status": "blocked",
        "inventory_due_metric_count": 0,
        "inventory_future_metric_count": 0,
        "inventory_succeeded_metric_count": 0,
    }
    try:
        rows = snapshot.rows.as_dict()
        row = history._one([r for r in rows["owned_seed_inventories"] if r["id"] == inventory_id],
                           "owned_inventory_reference_missing")
        metrics["inventory_state"] = row["state"]
        data = history.decode_history_manifest(row["manifest_json"]).document.as_dict()
        history._require(row.get("approved_at") is not None and history._time(row["approved_at"]) <= now,
                         "owned_inventory_not_approved")
        history._require(row["manifest_sha256"] == history.history_sha256(data)
                         and data["inventory_id"] == inventory_id
                         and data["platform_channel_id"] == snapshot.platform_channel_id
                         and all(row[k] == data[k] for k in ("channel_profile_id", "target_account_id", "platform_channel_id")),
                         "owned_inventory_manifest_changed")
        channel = history._one([r for r in rows["channel_profiles"] if r["id"] == row["channel_profile_id"]],
                               "owned_inventory_scope")
        items = sorted((r for r in rows["owned_seed_inventory_items"] if r["inventory_id"] == inventory_id),
                       key=lambda r: r["ordinal"])
        history._require(len(items) == 7, "owned_inventory_cardinality_invalid")
        for item, entry in zip(items, data["entries"]):
            history._require(all(item[k] == entry[k] for k in
                                 ("id", "ordinal", "asset_id", "manual_seed_id", "content_sha256"))
                             and item["platform_channel_id"] == row["platform_channel_id"],
                             "owned_inventory_item_binding")
            history._require(item["state"] in {"unused", "reserved", "completed", "held"}, "owned_inventory_consumption")
            if item["state"] == "unused":
                history._require(item.get("production_task_id") is None and item.get("consumed_at") is None
                                 and item.get("completed_at") is None, "owned_inventory_consumption")
            else:
                history._require(item.get("production_task_id") is not None and item.get("consumed_at") is not None,
                                 "owned_inventory_consumption")
        for state in ("completed", "reserved", "held", "unused"):
            metrics[f"inventory_{state}_count"] = sum(i["state"] == state for i in items)
        metrics["inventory_item_count"] = len(items)
        history._require(metrics["inventory_held_count"] == 0, "owned_inventory_item_held")
        proof = history.assess_owned_history(snapshot, now=now)
        history._require(proof.block_reason is None, proof.block_reason or "owned_history_invalid")
        reason = None
        if channel.get("halted_at") is not None:
            reason = "channel_halted"
        elif not channel.get("enabled", False):
            reason = "channel_disabled"
        elif channel.get("dry_run", True):
            reason = "channel_dry_run"
        elif external_conditions:
            reason = sorted(set(external_conditions))[0]
        completed = tuple(i["id"] for i in items if i["state"] == "reserved" and i["id"] in proof.completed_item_ids)
        if reason:
            completed = ()
        else:
            completed = completed_owned_inventory_items(snapshot, inventory_id, now=now)
        if not reason and all(i["state"] == "completed" or i["id"] in completed for i in items):
            metrics["inventory_settlement_status"] = "complete"
        task_ids = {i.get("production_task_id") for i in items} - {None}
        pub_ids = {p["id"] for p in rows["publication_records"] if p["production_task_id"] in task_ids}
        schedules = [m for m in rows["publication_metric_schedules"] if m["publication_id"] in pub_ids]
        metrics["inventory_due_metric_count"] = sum(m["status"] != "succeeded" and history._time(m["due_at"]) <= now for m in schedules)
        metrics["inventory_future_metric_count"] = sum(m["status"] != "succeeded" and history._time(m["due_at"]) > now for m in schedules)
        metrics["inventory_succeeded_metric_count"] = sum(m["status"] == "succeeded" for m in schedules)
        if not reason:
            metrics["inventory_feedback_status"] = "complete" if (
                metrics["inventory_settlement_status"] == "complete" and len(schedules) == 35
                and all(m["status"] == "succeeded" for m in schedules)
            ) else "pending"
        if reason is None and now >= history._time(data["expires_at"]) and row["state"] in {"approved", "exhausted"}:
            reason = "owned_inventory_expired"
        elif reason is None and row["state"] in {"held", "expired", "revoked"}:
            reason = f"owned_inventory_{row['state']}"
        if row["state"] == "approved" and reason is None and channel.get("intake_paused_at") is None:
            metrics["inventory_intake_status"] = "open"
        return OwnedInventoryFeedback(inventory_id, reason, completed, metrics)
    except history.OwnedHistoryError as error:
        return OwnedInventoryFeedback(inventory_id, str(error), (), metrics)
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError):
        return OwnedInventoryFeedback(inventory_id, "owned_inventory_history_invalid", (), metrics)
