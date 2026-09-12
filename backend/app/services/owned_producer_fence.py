"""Owned producer authority at existing plan and irreversible-effect boundaries."""
from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, replace
from datetime import timedelta
from types import SimpleNamespace

from sqlalchemy import select

from app.channel_agent.service import _is_pds_fail_policy_decision
from app.models.channel_agent import ChannelProfile, ProductionTask, PublishingAccount, TopicLane, LaneFormatMatrix
from app.models.owned_seed_inventory import OwnedSeedInventory
from app.pds_client import PDSDecision
from app.services import owned_seed_inventory as inv
from app.services import owned_seed_inventory_history as history
from app.services.schedule_service import get_or_create_and_lock_runtime_schedule


def require_real_pds(response):
    inv.require(isinstance(response, dict), "owned_inventory_pds_evidence")
    metadata = response.get("metadata")
    inv.require(isinstance(metadata, dict), "owned_inventory_pds_evidence")
    inv.require(response.get("verdict") == "allow"
                and isinstance(response.get("decision_id"), str) and bool(response["decision_id"].strip())
                and isinstance(response.get("rules_version"), str) and bool(response["rules_version"].strip())
                and isinstance(response.get("evaluated_rules"), list) and bool(response["evaluated_rules"])
                and all(isinstance(r, str) and r.strip() for r in response["evaluated_rules"]),
                "owned_inventory_pds_evidence")
    inv.require(not _is_pds_fail_policy_decision(PDSDecision("", "allow", metadata=metadata))
                and not any(metadata.get(k) for k in ("disabled", "dev", "dev_allow_all", "noop", "fallback", "degraded"))
                and metadata.get("warning") not in {"dev_allow_all", "noop", "degraded", "pds_degraded"},
                "owned_inventory_pds_fallback")


def policy_evidence(request, response):
    return json.loads(inv.canonical({"request": asdict(request), "response": response}))


def require_owned_pipeline(raw, asset_id):
    """Validate the existing deterministic one-input template, never repair it."""
    from app.orchestrator.dag import validate_pipeline
    from app.schemas.pipeline import PipelineDefinition
    try:
        graph = PipelineDefinition.model_validate(raw)
        inv.require(validate_pipeline(graph).valid, "owned_inventory_pipeline_invalid")
        sources = [n for n in graph.nodes if n.type == "source"]
        uploads = [n for n in graph.nodes if n.type == "youtube_upload"]
        inv.require(len(sources) == len(uploads) == 1, "owned_inventory_pipeline_source")
        source, upload = sources[0], uploads[0]
        inv.require(source.data.asset_id == str(asset_id) and source.data.config.get("asset_id") in {None, str(asset_id)}
                    and source.data.config.get("media_type") == "video"
                    and upload.data.config.get("privacy") == "unlisted", "owned_inventory_pipeline_binding")
        allowed = {"source", "trim", "vertical_crop", "title_overlay", "transcode", "export", "youtube_upload"}
        inv.require(all(n.type in allowed and (n is source or n.data.asset_id is None
                    and "asset_id" not in n.data.config) for n in graph.nodes), "owned_inventory_pipeline_source")
        # The existing builder ends in the upload plus its local export preview.
        exports = [n for n in graph.nodes if n.type == "export"]
        inv.require(len(exports) == 1, "owned_inventory_pipeline_branch")
        sinks = {upload.id, exports[0].id}
        parents = {e.source for e in graph.edges if e.target in sinks}
        inv.require(len(parents) == 1 and next(n for n in graph.nodes if n.id in parents).type == "transcode",
                    "owned_inventory_pipeline_branch")
        ids = {n.id for n in graph.nodes}
        inv.require(len(ids) == len(graph.nodes) and len(graph.edges) == len(ids) - 1
                    and all(sum(e.target == n.id for e in graph.edges) == (0 if n is source else 1)
                            and sum(e.source == n.id for e in graph.edges) == (0 if n.id in sinks else 2 if n.id in parents else 1)
                            for n in graph.nodes), "owned_inventory_pipeline_branch")
        return graph
    except (TypeError, ValueError, KeyError, AttributeError):
        raise inv.OwnedInventoryError("owned_inventory_pipeline_invalid") from None


@dataclass(frozen=True)
class ProducerIdentity:
    task_id: str
    platform_channel_id: str
    inventory_id: str | None = None
    item_id: str | None = None
    source_sha256: str | None = None


def _one(rows, predicate, reason="owned_inventory_producer_binding"):
    matches = [row for row in rows if predicate(row)]
    inv.require(len(matches) == 1, reason)
    return matches[0]


def producer_identity(snapshot, task_id, *, now):
    """A supplied inventory ID/snapshot can narrow evidence, never mint authority."""
    rows = snapshot.rows.as_dict()
    task = _one(rows["production_tasks"], lambda r: r["id"] == task_id)
    account = _one(rows["publishing_accounts"], lambda r: r["id"] == task["target_account_id"])
    channel = _one(rows["channel_profiles"], lambda r: r["id"] == task["channel_profile_id"])
    bindings, certificate, _ = history._approved_authority(rows, now)
    inv.require(account["id"] not in bindings and all(b.legacy_channel_profile_id != channel["id"] for b in bindings.values())
                and (certificate is None or account["id"] != certificate.legacy_account_id
                     and channel["id"] != certificate.legacy_channel_profile_id), "owned_inventory_historical_producer_pinned")
    inv.require(account["channel_profile_id"] == channel["id"] and inv.is_youtube_platform(account["platform"])
                and history._UC.fullmatch(account["platform_account_id"] or "") is not None
                and account["platform_account_id"] == snapshot.platform_channel_id, "owned_history_unclassified")
    occupied = [r for r in rows["owned_seed_inventories"] if r.get("approved_at") is not None
                and r.get("succession_released_at") is None and r["platform_channel_id"] == snapshot.platform_channel_id]
    items = [i for i in rows["owned_seed_inventory_items"] if i.get("production_task_id") == task_id]
    if not occupied:
        inv.require(not items and not task.get("agent_approval_evidence_json", {}).get("owned_inventory")
                    and not task.get("channel_config_snapshot_json", {}).get("owned_inventory"),
                    "owned_inventory_producer_binding")
        return ProducerIdentity(task_id, snapshot.platform_channel_id)
    inv.require(len(occupied) == len(items) == 1, "owned_inventory_producer_binding")
    row, item = occupied[0], items[0]
    manifest = history.decode_history_manifest(row["manifest_json"]).document.as_dict()
    inv.require(inv.sha256(manifest) == row["manifest_sha256"], "owned_inventory_manifest_changed")
    inv.require(row["state"] in {"approved", "exhausted"} and row.get("revoked_at") is None
                and row.get("hold_reason") is None and row.get("approved_by") and row.get("approval_reference")
                and history._time(row["approved_at"]) <= now
                and history._time(row["starts_at"]) <= now < history._time(row["expires_at"])
                and history._time(row["expires_at"]) - history._time(row["starts_at"]) == timedelta(days=7),
                "owned_inventory_producer_inactive")
    inv.require(channel.get("owned_seed_inventory_id") == row["id"] and row["channel_profile_id"] == channel["id"]
                and row["target_account_id"] == account["id"] and item["inventory_id"] == row["id"]
                and item["platform_channel_id"] == snapshot.platform_channel_id
                and item["state"] == "reserved" and item.get("consumed_at") is not None
                and task["manual_seed_id"] == item["manual_seed_id"] and task["topic_lane_id"] == row["topic_lane_id"]
                and task["lane_format_id"] == row["lane_format_id"], "owned_inventory_producer_binding")
    inv.require(channel["enabled"] and not channel["dry_run"] and channel["halted_at"] is None
                and (channel["intake_paused_at"] is None or row["state"] == "exhausted"
                     and channel["intake_pause_reason"] == "owned_inventory_exhausted")
                and account["enabled"] and account["paused_until"] is None
                and account["default_privacy"] == row["privacy"] == "unlisted"
                and not account["external_asset_auto_publish"], "owned_inventory_producer_controls")
    inv.require(task["source"] == "manual_seed" and task["approval_mode"] == "agent"
                and not task["uses_external_assets"] and task["source_platforms_json"] == []
                and task["material_library_ids_json"] == [] and task["retry_count"] == 0
                and task["failure_reason"] is None and task["blocked_by_guard"] is None
                and task["state"] in {"selected", "planning", "producing", "uploaded_private", "scheduled"},
                "owned_inventory_producer_task")
    seed = _one(rows["manual_seeds"], lambda r: r["id"] == item["manual_seed_id"])
    entry = _one(manifest["entries"], lambda r: r["id"] == item["id"])
    inv.require(seed["status"] == "exhausted" and inv.item_binding(SimpleNamespace(**item), SimpleNamespace(**seed)) == entry
                and task["prompt"] == seed["prompt"] and task["title_seed"] == seed["title_seed"]
                and seed["target_account_id"] == task["target_account_id"] and seed["channel_profile_id"] == channel["id"]
                and seed["source_policy"] == "owned_only" and seed["source_platforms_json"] == []
                and seed["material_library_ids_json"] == [], "owned_inventory_seed_changed")
    constraints = seed["constraints_json"]
    inv.require(constraints.get("input_asset_id") == item["asset_id"] and constraints.get("source_strategy") == "input_video"
                and constraints.get("planning_mode") == "template"
                and task["channel_config_snapshot_json"].get("manual_seed", {}).get("constraints_json") == constraints,
                "owned_inventory_source_changed")
    asset = _one(rows["assets"], lambda r: r["id"] == item["asset_id"])
    inv.require(inv.asset_descriptor(SimpleNamespace(**asset)) == item["storage_descriptor_json"]
                and asset["media_info"].get("license") == "owned" and asset["media_info"].get("provenance") == "generated",
                "owned_inventory_asset_changed")
    evidence = task["agent_approval_evidence_json"].get("owned_inventory", {})
    inv.require(evidence == task["channel_config_snapshot_json"].get("owned_inventory")
                and all(evidence.get(k) == value for k, value in {
                    "inventory_id": row["id"], "item_id": item["id"], "manifest_sha256": row["manifest_sha256"],
                    "configuration_sha256": manifest["configuration_sha256"], "input_asset_id": item["asset_id"],
                    "source_content_sha256": item["content_sha256"], "seed_sha256": item["seed_sha256"],
                }.items()), "owned_inventory_evidence_changed")
    return ProducerIdentity(task_id, snapshot.platform_channel_id, row["id"], item["id"], item["content_sha256"])


def assess_producer(snapshot, task_id, *, now, render_sha256=None):
    identity = producer_identity(snapshot, task_id, now=now)
    assessment = history.assess_owned_producer_history(snapshot, now=now, current_task_id=task_id)
    inv.require(assessment.block_reason is None, assessment.block_reason or "owned_history_invalid")
    inv.require(assessment.wait_reason is None, assessment.wait_reason or "owned_inventory_outstanding")
    rows = snapshot.rows.as_dict()
    retired = assessment.retired_source_sha256 + assessment.retired_render_sha256
    inv.require(identity.source_sha256 not in retired and render_sha256 not in retired,
                "owned_inventory_retired_hash_reuse")
    inv.require(identity.source_sha256 is not None or not retired, "owned_inventory_source_evidence_missing")
    if identity.source_sha256:
        inv.require(all(i["content_sha256"] != identity.source_sha256 for i in rows["owned_seed_inventory_items"]
                        if i.get("production_task_id") != task_id and i["state"] != "unused"
                        and i["platform_channel_id"] == snapshot.platform_channel_id), "owned_inventory_source_reuse")
    members = {c.operation_id for c in assessment.classifications if c.platform_channel_id == snapshot.platform_channel_id}
    if render_sha256 is not None:
        inv.require(history._SHA.fullmatch(render_sha256) is not None, "owned_inventory_render_sha256")
        inv.require(all(o["content_sha256"] != render_sha256 for o in rows["youtube_upload_operations"]
                        if o["id"] in members and o["production_task_id"] != task_id), "owned_inventory_render_reuse")
    return identity, assessment


@dataclass(frozen=True)
class ProducerAuthority:
    identity: ProducerIdentity
    digest: str
    snapshot: history.OwnedHistorySnapshot


async def lock_producer(db, task_id, *, render_sha256=None, queue_lease=None):
    """Return with the schedule fence held; native observations own no transaction."""
    from app.channel_agent.owned_inventory import redis_request, observe_redis

    inv.require(not db.new and not db.dirty and not db.deleted, "owned_inventory_pending_session_changes")
    task_id = uuid.UUID(str(task_id)) if task_id is not None else None
    observation = None
    try:
        for attempt in range(2):
            if queue_lease is not None:
                await queue_lease.lock(db)
            channel_id = await db.scalar(select(ProductionTask.channel_profile_id).where(ProductionTask.id == task_id))
            channel = await inv._row(db, ChannelProfile, channel_id, lock=True) if channel_id else None
            await get_or_create_and_lock_runtime_schedule(db)
            protected = await db.scalar(select(OwnedSeedInventory.id).where(OwnedSeedInventory.approved_at.is_not(None)).limit(1))
            if protected is None:
                return None
            inv.require(channel is not None, "owned_inventory_producer_missing")
            task = await inv._row(db, ProductionTask, task_id)
            inv.require(task.channel_profile_id == channel.id, "owned_inventory_producer_binding")
            account = await inv._row(db, PublishingAccount, task.target_account_id)
            platform = account.platform_account_id
            inv.require(isinstance(platform, str) and history._UC.fullmatch(platform) is not None,
                        "owned_history_unclassified")
            await inv.lock_platform_scope(db, platform)
            snapshot = await history.load_owned_history_evidence(db, platform_channel_id=platform)
            request = redis_request(snapshot)
            if request is not None:
                if observation is None:
                    inv.require(attempt == 0, "owned_history_observation_stale")
                    await db.rollback()
                    observation = (request, await observe_redis(request))
                    continue
                inv.require(request.digest == observation[0].digest
                            and 0 <= (snapshot.observed_at - observation[0].observed_at).total_seconds() <= 60,
                            "owned_history_observation_stale")
                snapshot = replace(snapshot, redis_observations=observation[1])
            now = await inv._now(db)
            identity, assessment = assess_producer(snapshot, str(task_id), now=now, render_sha256=render_sha256)
            config_sha = None
            if identity.inventory_id is not None:
                row = await inv._row(db, OwnedSeedInventory, uuid.UUID(identity.inventory_id), lock=True)
                await inv._verify_manifest(db, row)
                account = await inv._row(db, PublishingAccount, task.target_account_id, lock=True)
                lane = await inv._row(db, TopicLane, row.topic_lane_id, lock=True)
                fmt = await inv._row(db, LaneFormatMatrix, row.lane_format_id, lock=True)
                config_sha = inv.configuration_sha256(channel, account, lane, fmt)
                inv.require(config_sha == row.manifest_json["configuration_sha256"], "owned_inventory_configuration_changed")
                inv.require(lane.enabled and lane.paused_until is None and fmt.enabled and fmt.source_platforms_json == []
                            and fmt.default_publish_visibility == "unlisted", "owned_inventory_producer_controls")
            return ProducerAuthority(identity, inv.sha256({"identity": asdict(identity), "configuration": config_sha,
                "history": assessment.stable_history_sha256, "authority": assessment.authority_sha256,
                "retired_source": assessment.retired_source_sha256, "retired_render": assessment.retired_render_sha256}), snapshot)
        raise inv.OwnedInventoryError("owned_history_observation_stale")
    except BaseException:
        await db.rollback()
        raise
