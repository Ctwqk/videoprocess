from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any

from sqlalchemy import func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.channel_agent import clients as channel_clients
from app.models.asset import Asset
from app.models.channel_agent import ChannelOpsQueueItem, ChannelProfile, LaneFormatMatrix, ManualSeed, ProductionTask, PublishingAccount, TopicLane
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.owned_seed_inventory import OwnedSeedInventory, OwnedSeedInventoryItem
from app.models.schedule import RuntimeSchedule
from app.models.youtube_upload_operation import YouTubeUploadOperation
from app.schemas.channel_agent import (
    OwnedHistoryLocators, OwnedSeedInventoryApprove, OwnedSeedInventoryCreate, OwnedSeedInventoryCreateV2, OwnedSeedInventoryRevoke,
)
from app.services import owned_seed_inventory_history as history
from app.storage import manager as storage_manager


MAX_ASSET_BYTES = 64 * 1024 * 1024


class OwnedInventoryError(ValueError):
    """Static, safe-to-display inventory conflict."""


def require(condition: bool, code: str = "owned_inventory_conflict") -> None:
    if not condition:
        raise OwnedInventoryError(code)


def utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _json_default(value: Any) -> str:
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, datetime):
        return utc(value).isoformat()
    raise TypeError("inventory_value_invalid")


def canonical(value: Any) -> str:
    return json.dumps(value, default=_json_default, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def sha256(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _fields(row: Any, names: str) -> dict:
    return {name: getattr(row, name) for name in names.split()}


async def _now(db: AsyncSession) -> datetime:
    clock = func.clock_timestamp() if db.get_bind().dialect.name == "postgresql" else func.current_timestamp()
    return utc((await db.execute(select(clock))).scalar_one())


async def lock_platform_scope(db: AsyncSession, platform_channel_id: str) -> None:
    if db.get_bind().dialect.name == "postgresql":
        key = int.from_bytes(hashlib.sha256(f"owned-inventory:{platform_channel_id}".encode()).digest()[:8], "big", signed=True)
        await db.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})


def is_youtube_platform(platform: str | None) -> bool:
    # Match ChannelAgentService's execution fallback without rewriting other providers.
    return str(platform or "youtube") == "youtube"


async def _youtube_account_ids(db: AsyncSession, platform_channel_id: str) -> list[uuid.UUID]:
    return list((await db.scalars(select(PublishingAccount.id).where(
        or_(PublishingAccount.platform.in_(["youtube", ""]), PublishingAccount.platform.is_(None)),
        PublishingAccount.platform_account_id == platform_channel_id,
    ))).all())


async def _row(db: AsyncSession, model: Any, row_id: uuid.UUID, *, lock: bool = False) -> Any:
    statement = select(model).where(model.id == row_id).execution_options(populate_existing=True)
    if lock:
        statement = statement.with_for_update()
    row = (await db.execute(statement)).scalar_one_or_none()
    require(row is not None, "owned_inventory_reference_missing")
    return row


async def _scope(db: AsyncSession, channel_id: uuid.UUID, data: Any, *, lock: bool = False) -> tuple[ChannelProfile, str]:
    channel = await _row(db, ChannelProfile, channel_id, lock=lock)
    if lock:
        await lock_platform_scope(db, data.platform_channel_id)
    account = await _row(db, PublishingAccount, uuid.UUID(str(data.target_account_id)), lock=lock)
    lane = await _row(db, TopicLane, uuid.UUID(str(data.topic_lane_id)), lock=lock)
    lane_format = await _row(db, LaneFormatMatrix, uuid.UUID(str(data.lane_format_id)), lock=lock)
    require(account.channel_profile_id == channel.id and lane.channel_profile_id == channel.id
            and lane_format.topic_lane_id == lane.id, "owned_inventory_scope_mismatch")
    require(is_youtube_platform(account.platform) and account.platform_account_id == data.platform_channel_id
            and account.default_privacy == "unlisted" and not account.external_asset_auto_publish
            and account.enabled and account.paused_until is None, "owned_inventory_account_unsafe")
    require(channel.enabled and not channel.dry_run and channel.halted_at is None
            and channel.intake_paused_at is not None and lane.enabled and lane.paused_until is None
            and lane_format.enabled and lane_format.default_publish_visibility == "unlisted"
            and lane_format.source_platforms_json == [], "owned_inventory_scope_unsafe")
    fingerprint = sha256({
        "channel": _fields(channel, "id config_version name positioning language default_aspect_ratio risk_policy_json content_mix_policy_json cadence_policy_json alert_policy_json enabled dry_run"),
        "account": _fields(account, "id channel_profile_id platform platform_account_id credential_ref platform_specific_config_json default_privacy external_asset_auto_publish enabled paused_until"),
        "lane": _fields(lane, "id channel_profile_id name description weight keywords_json negative_keywords_json min_posts_per_week max_posts_per_day max_consecutive_streak cooldown_after_post_minutes enabled paused_until"),
        "format": _fields(lane_format, "id topic_lane_id format_key enabled weight target_duration_sec template_pool_json source_platforms_json default_publish_visibility"),
    })
    return channel, fingerprint


def asset_descriptor(asset: Asset) -> dict:
    path = asset.storage_path
    require(isinstance(path, str) and path.startswith("assets/") and "\\" not in path
            and "\x00" not in path and str(PurePosixPath(path)) == path
            and all(part not in {".", ".."} for part in path.split("/")), "owned_inventory_asset_path_invalid")
    info = asset.media_info or {}
    require(isinstance(info, dict) and info.get("license") in {None, "owned"}
            and info.get("provenance") in {None, "generated"}, "owned_inventory_asset_provenance_conflict")
    require(isinstance(asset.mime_type, str) and asset.mime_type.startswith("video/")
            and type(asset.file_size) is int and 0 < asset.file_size <= MAX_ASSET_BYTES
            and asset.storage_backend in {"local", "minio"}, "owned_inventory_asset_invalid")
    return {**_fields(asset, "id storage_backend storage_path file_size mime_type"),
            "media_info_sha256": sha256({key: value for key, value in info.items() if key not in {"license", "provenance"}})}


async def _observe_assets(db: AsyncSession, asset_ids: list[uuid.UUID]) -> dict[uuid.UUID, dict]:
    return {asset_id: asset_descriptor(await _row(db, Asset, asset_id)) for asset_id in asset_ids}


async def _hash_assets(descriptors: dict[uuid.UUID, dict]) -> dict[uuid.UUID, str]:
    result = {}
    for asset_id, descriptor in descriptors.items():
        try:
            storage = storage_manager.get_storage(descriptor["storage_backend"], create_bucket=False)
            content = await storage.read_bounded(descriptor["storage_path"], MAX_ASSET_BYTES)
        except Exception:
            raise OwnedInventoryError("owned_inventory_asset_read_failed") from None
        require(type(content) is bytes and len(content) == descriptor["file_size"], "owned_inventory_asset_size_changed")
        result[asset_id] = hashlib.sha256(content).hexdigest()
    return result


async def _lock_assets(db: AsyncSession, descriptors: dict[uuid.UUID, dict]) -> dict[uuid.UUID, Asset]:
    assets = {}
    for asset_id in sorted(descriptors, key=str):
        asset = await _row(db, Asset, asset_id, lock=True)
        require(canonical(asset_descriptor(asset)) == canonical(descriptors[asset_id]), "owned_inventory_asset_changed")
        assets[asset_id] = asset
    return assets


def seed_binding(seed: ManualSeed) -> dict:
    return _fields(seed, "id channel_profile_id topic_lane_id target_account_id prompt title_seed source_policy source_platforms_json material_library_ids_json constraints_json")


def item_binding(item: OwnedSeedInventoryItem, seed: ManualSeed) -> dict:
    require(sha256(seed_binding(seed)) == item.seed_sha256, "owned_inventory_seed_changed")
    require(sha256(item.provenance_evidence_json) == item.provenance_sha256, "owned_inventory_provenance_changed")
    return {"id": str(item.id), "ordinal": item.ordinal, "asset_id": str(item.asset_id),
            "manual_seed_id": str(item.manual_seed_id), "content_sha256": item.content_sha256,
            "byte_size": item.byte_size, "storage_descriptor": item.storage_descriptor_json,
            "provenance_evidence": item.provenance_evidence_json, "provenance_sha256": item.provenance_sha256,
            "seed_sha256": item.seed_sha256, "prompt": seed.prompt, "title_seed": seed.title_seed}


async def inventory_items(db: AsyncSession, inventory_id: uuid.UUID, *, lock: bool = False) -> list[OwnedSeedInventoryItem]:
    statement = select(OwnedSeedInventoryItem).where(OwnedSeedInventoryItem.inventory_id == inventory_id).order_by(OwnedSeedInventoryItem.ordinal).execution_options(populate_existing=True)
    if lock:
        statement = statement.with_for_update()
    return list((await db.scalars(statement)).all())


async def _verify_manifest(db: AsyncSession, row: OwnedSeedInventory) -> list[OwnedSeedInventoryItem]:
    require(sha256(row.manifest_json) == row.manifest_sha256, "owned_inventory_manifest_changed")
    items = await inventory_items(db, row.id)
    require(len(items) == 7 and [item.ordinal for item in items] == list(range(1, 8)), "owned_inventory_cardinality_invalid")
    bindings = [item_binding(item, await _row(db, ManualSeed, item.manual_seed_id)) for item in items]
    require(canonical(bindings) == canonical(row.manifest_json["entries"]), "owned_inventory_manifest_changed")
    require(row.manifest_json["inventory_id"] == str(row.id)
            and row.manifest_json["channel_profile_id"] == str(row.channel_profile_id)
            and row.manifest_json["platform_channel_id"] == row.platform_channel_id
            and row.manifest_json["target_account_id"] == str(row.target_account_id)
            and row.manifest_json["topic_lane_id"] == str(row.topic_lane_id)
            and row.manifest_json["lane_format_id"] == str(row.lane_format_id)
            and row.manifest_json["starts_at"] == utc(row.starts_at).isoformat()
            and row.manifest_json["expires_at"] == utc(row.expires_at).isoformat()
            and row.privacy == "unlisted" and row.max_admissions == 7 and row.minimum_interval_seconds == 86400,
            "owned_inventory_manifest_changed")
    return items


def _history_sources(snapshot: history.OwnedHistorySnapshot, locators: OwnedHistoryLocators, target_account_id: str) -> dict:
    require(locators.retired_unassigned_preupload is None, "owned_inventory_retirement_sealing_unavailable")
    rows = snapshot.rows.as_dict()
    groups: dict[str, dict] = {}
    for locator in locators.operations:
        op = history._one([o for o in rows["youtube_upload_operations"] if o["id"] == locator.operation_id], "owned_history_orphan")
        task = history._one([t for t in rows["production_tasks"] if t["id"] == op["production_task_id"]], "owned_history_orphan")
        account = history._one([a for a in rows["publishing_accounts"] if a["id"] == locator.legacy_account_id], "owned_history_orphan")
        require(account["id"] != target_account_id and account["channel_profile_id"] == task["channel_profile_id"] == locator.legacy_channel_profile_id
                and task["target_account_id"] == account["id"] and is_youtube_platform(account["platform"])
                and account["platform_account_id"] in {"", snapshot.platform_channel_id}, "owned_inventory_history_scope_mismatch")
        history._one([c for c in rows["channel_profiles"] if c["id"] == locator.legacy_channel_profile_id], "owned_history_orphan")
        _, _, effect = history._normal_history(history._task_history(rows, task), None, snapshot.observed_at)
        require(all(other["id"] == op["id"] or
                    (other.get("manager_task_id") != op["manager_task_id"] and
                     other.get("platform_video_id") != op["platform_video_id"])
                    for other in rows["youtube_upload_operations"]), "owned_inventory_history_identity_ambiguous")
        group = groups.setdefault(account["id"], {"legacy_account_id": account["id"],
            "legacy_channel_profile_id": account["channel_profile_id"],
            "account_descriptor_sha256": history.account_descriptor_sha256(account), "operations": [], "effects": []})
        group["operations"].append(op)
        group["effects"].append(effect)
    for account_id, group in groups.items():
        tasks = {t["id"] for t in rows["production_tasks"] if t["target_account_id"] == account_id}
        operations = [o for o in rows["youtube_upload_operations"] if o["production_task_id"] in tasks]
        require([o["id"] for o in operations] == [o["id"] for o in group["operations"]] and
                tasks == {o["production_task_id"] for o in operations}, "owned_inventory_history_membership_changed")
    return groups


async def _observe_history_uploads(groups: dict, platform_channel_id: str) -> tuple[str, dict]:
    try:
        manager = channel_clients.build_youtube_manager_client()
    except (RuntimeError, ValueError):
        raise OwnedInventoryError("owned_inventory_manager_unavailable") from None
    observed = {}
    for group in groups.values():
        for op in group["operations"]:
            try:
                value = await manager.qualify_upload(manager_task_id=op["manager_task_id"], video_id=op["platform_video_id"])
            except channel_clients.YouTubeHistoryQualificationError as error:
                raise OwnedInventoryError(str(error)) from None
            require(value.actual_platform_channel_id == platform_channel_id, "owned_inventory_history_channel_mismatch")
            observed[op["id"]] = value
    return manager.history_endpoint_identity, observed


async def _lock_history_scope(db: AsyncSession, channel_id: uuid.UUID, locators: OwnedHistoryLocators) -> None:
    channels = {channel_id} | {uuid.UUID(locator.legacy_channel_profile_id) for locator in locators.operations}
    if locators.retired_unassigned_preupload:
        channels.add(uuid.UUID(locators.retired_unassigned_preupload.legacy_channel_profile_id))
    for selected in sorted(channels, key=str):
        await _row(db, ChannelProfile, selected, lock=True)
    schedule = (await db.scalars(select(RuntimeSchedule).where(RuntimeSchedule.service_name == "videoprocess")
        .with_for_update().execution_options(populate_existing=True))).one_or_none()
    require(schedule is not None, "owned_inventory_schedule_missing")


def _stable_binding(value: dict) -> dict:
    result = history.FrozenJSON.from_value(value).as_dict()
    qualification = result["qualification"]
    qualification.pop("observed_at")
    qualification.pop("facts_sha256")
    for fact in qualification["sanitized_facts"]:
        fact.pop("observed_at")
    return result


def _qualified_history(snapshot: history.OwnedHistorySnapshot, groups: dict, observation: tuple[str, dict],
                       subject: str, reference: str) -> dict:
    endpoint, observations = observation
    existing, _, _ = history._approved_authority(snapshot.rows.as_dict(), snapshot.observed_at)
    bindings = []
    for account_id in sorted(groups):
        group = groups[account_id]
        facts = [{"operation_id": op["id"], "manager_task_id": observations[op["id"]].manager_task_id,
                  "platform_video_id": observations[op["id"]].platform_video_id,
                  "actual_platform_channel_id": observations[op["id"]].actual_platform_channel_id,
                  "operation_sha256": history.history_sha256(op), "receipt_sha256": history.history_sha256(op["receipt_json"]),
                  "observed_at": snapshot.observed_at.isoformat()} for op in group["operations"]]
        first = facts[0]
        qualification = {"observed_at": snapshot.observed_at.isoformat(), "server_subject": subject,
            "manager_endpoint_identity": endpoint, "manager_task_id": first["manager_task_id"],
            "platform_video_id": first["platform_video_id"], "actual_platform_channel_id": snapshot.platform_channel_id,
            "sanitized_facts": facts, "facts_sha256": history.history_sha256(facts), "approval_reference": reference}
        binding = {key: group[key] for key in ("legacy_account_id", "legacy_channel_profile_id", "account_descriptor_sha256")}
        binding.update(platform="youtube", use="history_only", canonical_platform_channel_id=snapshot.platform_channel_id,
                       qualified_operation_ids=[f["operation_id"] for f in facts], qualification=qualification)
        if account_id in existing:
            original = existing[account_id].document.as_dict()
            binding["qualification"]["approval_reference"] = original["qualification"]["approval_reference"]
            require(_stable_binding(binding) == _stable_binding(original), "owned_inventory_history_authority_conflict")
            binding = original
        history.HistoryOnlyBinding.parse(binding)
        bindings.append(binding)
    return {"version": 1, "bindings": bindings, "retired_unassigned_preupload": None}


def _assess_qualified_draft(snapshot: history.OwnedHistorySnapshot, row: OwnedSeedInventory, subject: str, now: datetime) -> None:
    # Only this server-verified draft is provisionally assessed; runtime stays approved-only.
    rows = snapshot.rows.as_dict()
    rows["owned_seed_inventories"] = [r for r in rows["owned_seed_inventories"] if r["id"] != str(row.id)]
    rows["owned_seed_inventories"].append({"id": str(row.id), "state": "approved", "manifest_json": row.manifest_json,
        "manifest_sha256": row.manifest_sha256, "channel_profile_id": str(row.channel_profile_id),
        "target_account_id": str(row.target_account_id), "platform_channel_id": row.platform_channel_id,
        "approved_at": now.isoformat(), "approved_by": subject, "approval_reference": f"owned-inventory-draft:{row.client_request_id}"})
    result = history.assess_owned_history(history.OwnedHistorySnapshot.from_rows(rows,
        platform_channel_id=snapshot.platform_channel_id, observed_at=snapshot.observed_at,
        redis_observations=snapshot.redis_observations), now=now)
    require(result.block_reason is None, result.block_reason or "owned_inventory_history_invalid")


async def create_inventory(db: AsyncSession, channel_id: uuid.UUID, data: OwnedSeedInventoryCreate | OwnedSeedInventoryCreateV2, subject: str) -> dict:
    request_digest = sha256(data.model_dump(mode="json"))
    existing = (await db.scalars(select(OwnedSeedInventory).where(OwnedSeedInventory.client_request_id == uuid.UUID(data.client_request_id)))).one_or_none()
    if existing is not None:
        require(existing.channel_profile_id == channel_id and existing.request_sha256 == request_digest, "owned_inventory_idempotency_conflict")
        return await read_inventory(db, channel_id, existing.id)
    _, fingerprint = await _scope(db, channel_id, data)
    descriptors = await _observe_assets(db, [uuid.UUID(entry.asset_id) for entry in data.entries])
    sources = {}
    if isinstance(data, OwnedSeedInventoryCreateV2):
        initial = await history.load_owned_history_evidence(db, platform_channel_id=data.platform_channel_id)
        sources = _history_sources(initial, data.history_locators, data.target_account_id)
    # Storage I/O is outside the SQL transaction; all observations are rechecked below.
    await db.rollback()
    hashes = await _hash_assets(descriptors)
    for entry in data.entries:
        require(hashes[uuid.UUID(entry.asset_id)] == entry.expected_content_sha256, "owned_inventory_content_mismatch")
    observation = None
    if isinstance(data, OwnedSeedInventoryCreateV2):
        observation = await _observe_history_uploads(sources, data.platform_channel_id)
        await _lock_history_scope(db, channel_id, data.history_locators)
    _, fresh_fingerprint = await _scope(db, channel_id, data, lock=True)
    require(fingerprint == fresh_fingerprint, "owned_inventory_configuration_changed")
    await _lock_assets(db, descriptors)
    qualified = None
    if isinstance(data, OwnedSeedInventoryCreateV2):
        fresh = await history.load_owned_history_evidence(db, platform_channel_id=data.platform_channel_id)
        fresh_sources = _history_sources(fresh, data.history_locators, data.target_account_id)
        require(sources == fresh_sources, "owned_inventory_history_changed")
        assert observation is not None
        qualified = _qualified_history(fresh, fresh_sources, observation, subject, f"owned-inventory-draft:{data.client_request_id}")
    existing = (await db.scalars(select(OwnedSeedInventory).where(OwnedSeedInventory.client_request_id == uuid.UUID(data.client_request_id)))).one_or_none()
    if existing is not None:
        require(existing.channel_profile_id == channel_id and existing.request_sha256 == request_digest, "owned_inventory_idempotency_conflict")
        return await read_inventory(db, channel_id, existing.id)
    now = await _now(db)
    require(data.expires_at > now, "owned_inventory_expired")
    row = OwnedSeedInventory(id=uuid.uuid4(), client_request_id=uuid.UUID(data.client_request_id),
                             channel_profile_id=channel_id, topic_lane_id=uuid.UUID(data.topic_lane_id),
                             lane_format_id=uuid.UUID(data.lane_format_id), target_account_id=uuid.UUID(data.target_account_id),
                             platform_channel_id=data.platform_channel_id, request_sha256=request_digest,
                             manifest_json={}, manifest_sha256="0" * 64, starts_at=data.starts_at,
                             expires_at=data.expires_at, state="draft", created_by=subject)
    db.add(row)
    await db.flush()
    entries = []
    for ordinal, entry in enumerate(data.entries, 1):
        seed = ManualSeed(id=uuid.uuid4(), channel_profile_id=channel_id, topic_lane_id=row.topic_lane_id,
                          target_account_id=row.target_account_id, prompt=entry.prompt, title_seed=entry.title_seed,
                          source_policy="owned_only", source_platforms_json=[], material_library_ids_json=[],
                          constraints_json={"input_asset_id": entry.asset_id, "source_strategy": "input_video", "planning_mode": "template"},
                          status="inventory_pending")
        db.add(seed)
        await db.flush()
        descriptor = json.loads(canonical(descriptors[uuid.UUID(entry.asset_id)]))
        provenance = entry.provenance_evidence.model_dump(mode="json")
        item = OwnedSeedInventoryItem(id=uuid.uuid4(), inventory_id=row.id, platform_channel_id=row.platform_channel_id,
                                      ordinal=ordinal, manual_seed_id=seed.id, asset_id=uuid.UUID(entry.asset_id),
                                      content_sha256=entry.expected_content_sha256, byte_size=descriptor["file_size"],
                                      storage_descriptor_json=descriptor, provenance_evidence_json=provenance,
                                      provenance_sha256=sha256(provenance), seed_sha256=sha256(seed_binding(seed)), state="unused")
        db.add(item)
        entries.append(item_binding(item, seed))
    row.manifest_json = {"version": 1, "inventory_id": str(row.id), "channel_profile_id": str(channel_id),
                         "topic_lane_id": data.topic_lane_id, "lane_format_id": data.lane_format_id,
                         "target_account_id": data.target_account_id, "platform_channel_id": data.platform_channel_id,
                         "starts_at": data.starts_at.isoformat(), "expires_at": data.expires_at.isoformat(),
                         "privacy": "unlisted", "max_admissions": 7, "minimum_interval_seconds": 86400,
                         "tick_interval_minutes": 1, "configuration_sha256": fingerprint, "entries": entries}
    row.manifest_sha256 = sha256(row.manifest_json)
    if qualified is not None:
        row.manifest_json = {**row.manifest_json, "version": 2, "legacy_history": qualified}
        row.manifest_sha256 = sha256(row.manifest_json)
        history.decode_history_manifest(row.manifest_json)
        _assess_qualified_draft(fresh, row, subject, now)
    await db.commit()
    return await read_inventory(db, channel_id, row.id)


async def closeout(db: AsyncSession, row: OwnedSeedInventory) -> dict:
    unavailable = {"status": "unresolved", "sha256": None}
    if row.state != "revoked" or row.approved_at is None or row.revoked_at is None or not row.revoked_by:
        return unavailable
    account_ids = await _youtube_account_ids(db, row.platform_channel_id)
    if account_ids != [row.target_account_id]:
        return unavailable
    items = await inventory_items(db, row.id)
    # Used-item closeout needs the later verified receipt/settlement authority, not a status label.
    if len(items) != 7 or any(item.state != "unused" or item.production_task_id is not None or item.consumed_at is not None for item in items):
        return unavailable
    tasks = list((await db.scalars(select(ProductionTask).where(or_(
        ProductionTask.target_account_id.in_(account_ids), ProductionTask.manual_seed_id.in_([item.manual_seed_id for item in items]),
    )))).all())
    if tasks:
        return unavailable
    queues = list((await db.scalars(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.channel_profile_id == row.channel_profile_id))).all())
    if any(queue.status not in {"succeeded", "cancelled"} or queue.locked_at is not None
           or queue.locked_by is not None for queue in queues):
        return unavailable
    jobs = list((await db.scalars(select(Job).where(Job.status.not_in([JobStatus.SUCCEEDED, JobStatus.CANCELLED])))).all())
    nodes = list((await db.scalars(select(NodeExecution).where(NodeExecution.status.not_in(
        [NodeStatus.SUCCEEDED, NodeStatus.CANCELLED, NodeStatus.SKIPPED],
    )))).all())
    if jobs or nodes:
        return unavailable
    operations = list((await db.scalars(select(YouTubeUploadOperation).where(or_(
        YouTubeUploadOperation.production_task_id.is_(None),
        YouTubeUploadOperation.production_task_id.in_([task.id for task in tasks]),
    )))).all())
    if operations:
        return unavailable
    evidence = {"version": 1, "inventory_id": str(row.id), "manifest_sha256": row.manifest_sha256,
                "state": row.state, "revoked_at": row.revoked_at,
                "items": [{"id": str(item.id), "state": item.state, "task_id": None} for item in items],
                "queue": [{"id": str(item.id), "status": item.status} for item in sorted(queues, key=lambda item: str(item.id))],
                "task_count": 0, "job_count": 0, "node_count": 0, "operation_count": 0}
    return {"status": "ready", "sha256": sha256(evidence), "evidence": json.loads(canonical(evidence))}


async def _requalify_v2_draft(db: AsyncSession, row: OwnedSeedInventory, subject: str) -> None:
    decoded = history.decode_history_manifest(row.manifest_json)
    assert decoded.legacy_history is not None
    require(decoded.legacy_history.retired_unassigned_preupload is None, "owned_inventory_retirement_sealing_unavailable")
    locators = OwnedHistoryLocators.model_validate({"operations": sorted([
        {"operation_id": operation_id, "legacy_account_id": b.legacy_account_id,
         "legacy_channel_profile_id": b.legacy_channel_profile_id}
        for b in decoded.legacy_history.bindings for operation_id in b.qualified_operation_ids], key=lambda locator: locator["operation_id"])})
    row_id, channel_id, platform, target = row.id, row.channel_profile_id, row.platform_channel_id, str(row.target_account_id)
    digest, reference = row.manifest_sha256, f"owned-inventory-draft:{row.client_request_id}"
    items = await _verify_manifest(db, row)
    _, fingerprint = await _scope(db, channel_id, row)
    require(fingerprint == row.manifest_json["configuration_sha256"], "owned_inventory_configuration_changed")
    descriptors = await _observe_assets(db, [item.asset_id for item in items])
    expected = {item.asset_id: (item.content_sha256, item.storage_descriptor_json) for item in items}
    require(all(canonical(value) == canonical(expected[key][1]) for key, value in descriptors.items()), "owned_inventory_asset_changed")
    initial = await history.load_owned_history_evidence(db, platform_channel_id=platform)
    sources = _history_sources(initial, locators, target)
    await db.rollback()
    hashes = await _hash_assets(descriptors)
    require(all(hashes[key] == expected[key][0] for key in hashes), "owned_inventory_content_mismatch")
    observation = await _observe_history_uploads(sources, platform)
    await _lock_history_scope(db, channel_id, locators)
    await lock_platform_scope(db, platform)
    row = await _row(db, OwnedSeedInventory, row_id, lock=True)
    require(row.manifest_sha256 == digest and row.state == "draft" and row.approved_at is None, "owned_inventory_manifest_changed")
    _, fresh_fingerprint = await _scope(db, channel_id, row, lock=True)
    require(fingerprint == fresh_fingerprint, "owned_inventory_configuration_changed")
    await _verify_manifest(db, row)
    await _lock_assets(db, descriptors)
    fresh = await history.load_owned_history_evidence(db, platform_channel_id=platform)
    fresh_sources = _history_sources(fresh, locators, target)
    require(sources == fresh_sources, "owned_inventory_history_changed")
    qualified = _qualified_history(fresh, fresh_sources, observation, subject, reference)
    require([_stable_binding(b) for b in qualified["bindings"]] ==
            [_stable_binding(b.document.as_dict()) for b in decoded.legacy_history.bindings], "owned_inventory_history_changed")
    _assess_qualified_draft(fresh, row, subject, await _now(db))


async def approve_inventory(db: AsyncSession, channel_id: uuid.UUID, inventory_id: uuid.UUID,
                            data: OwnedSeedInventoryApprove, subject: str) -> dict:
    row = await _row(db, OwnedSeedInventory, inventory_id)
    require(row.channel_profile_id == channel_id and row.manifest_sha256 == data.manifest_sha256, "owned_inventory_manifest_mismatch")
    if row.manifest_json.get("version") == 2:
        await _requalify_v2_draft(db, row, subject)
        raise OwnedInventoryError("owned_inventory_v2_activation_disabled")
    items = await _verify_manifest(db, row)
    _, fingerprint = await _scope(db, channel_id, row)
    require(fingerprint == row.manifest_json["configuration_sha256"], "owned_inventory_configuration_changed")
    descriptors = await _observe_assets(db, [item.asset_id for item in items])
    expected = {item.asset_id: (item.content_sha256, item.storage_descriptor_json) for item in items}
    for asset_id, descriptor in descriptors.items():
        require(canonical(descriptor) == canonical(expected[asset_id][1]), "owned_inventory_asset_changed")
    platform_channel_id = row.platform_channel_id
    await db.rollback()
    hashes = await _hash_assets(descriptors)
    require(all(hashes[asset_id] == expected[asset_id][0] for asset_id in hashes), "owned_inventory_content_mismatch")
    channel = await _row(db, ChannelProfile, channel_id, lock=True)
    await lock_platform_scope(db, platform_channel_id)
    row = await _row(db, OwnedSeedInventory, inventory_id, lock=True)
    _, fresh_fingerprint = await _scope(db, channel_id, row, lock=True)
    require(fingerprint == fresh_fingerprint, "owned_inventory_configuration_changed")
    # Alias producers do not take this scope lock; refuse their bindings before reading work absence.
    require(await _youtube_account_ids(db, platform_channel_id) == [row.target_account_id],
            "owned_inventory_account_alias")
    items = await _verify_manifest(db, row)
    assets = await _lock_assets(db, descriptors)
    require(row.manifest_sha256 == data.manifest_sha256, "owned_inventory_manifest_mismatch")
    if row.approved_at is not None:
        require(row.state == "approved" and row.approval_reference == data.approval_reference
                and row.approved_by == subject and row.predecessor_inventory_id == (uuid.UUID(data.predecessor_inventory_id) if data.predecessor_inventory_id else None)
                and row.predecessor_closeout_sha256 == data.predecessor_closeout_sha256,
                "owned_inventory_approval_conflict")
        return await read_inventory(db, channel_id, inventory_id)
    now = await _now(db)
    require(row.state == "draft" and utc(row.expires_at) > now, "owned_inventory_not_approvable")
    occupied = (await db.scalars(select(OwnedSeedInventory).where(
        OwnedSeedInventory.platform_channel_id == platform_channel_id,
        OwnedSeedInventory.approved_at.is_not(None), OwnedSeedInventory.succession_released_at.is_(None),
    ).with_for_update())).one_or_none()
    if occupied is not None:
        require(data.predecessor_inventory_id == str(occupied.id) and channel.owned_seed_inventory_id == occupied.id,
                "owned_inventory_predecessor_required")
        evidence = await closeout(db, occupied)
        require(evidence["status"] == "ready" and evidence["sha256"] == data.predecessor_closeout_sha256,
                "owned_inventory_closeout_unresolved")
        occupied.succession_released_at = now
        row.predecessor_inventory_id = occupied.id
        row.predecessor_closeout_sha256 = evidence["sha256"]
        await db.flush()
    else:
        require(data.predecessor_inventory_id is None and channel.owned_seed_inventory_id is None,
                "owned_inventory_predecessor_mismatch")
    for item in items:
        seed = await _row(db, ManualSeed, item.manual_seed_id, lock=True)
        require(seed.status == "inventory_pending" and item.state == "unused", "owned_inventory_seed_changed")
        seed.status = "active"
        asset = assets[item.asset_id]
        asset.media_info = {**(asset.media_info or {}), "license": "owned", "provenance": "generated"}
    row.state = "approved"
    row.approved_at = now
    row.approved_by = subject
    row.approval_reference = data.approval_reference
    channel.owned_seed_inventory_id = row.id
    channel.tick_interval_minutes = data.tick_interval_minutes
    await db.commit()
    return await read_inventory(db, channel_id, inventory_id)


async def revoke_inventory(db: AsyncSession, channel_id: uuid.UUID, inventory_id: uuid.UUID,
                           data: OwnedSeedInventoryRevoke, subject: str) -> dict:
    channel = await _row(db, ChannelProfile, channel_id, lock=True)
    row = await _row(db, OwnedSeedInventory, inventory_id)
    require(row.channel_profile_id == channel_id and row.manifest_sha256 == data.manifest_sha256, "owned_inventory_manifest_mismatch")
    await lock_platform_scope(db, row.platform_channel_id)
    row = await _row(db, OwnedSeedInventory, inventory_id, lock=True)
    if row.revoked_at is not None:
        require(row.revoked_by == subject and row.hold_reason == data.reason, "owned_inventory_revocation_conflict")
        return await read_inventory(db, channel_id, inventory_id)
    row.state = "revoked"
    row.revoked_at = await _now(db)
    row.revoked_by = subject
    row.hold_reason = data.reason
    if channel.owned_seed_inventory_id == row.id:
        channel.intake_paused_at = channel.intake_paused_at or row.revoked_at
        channel.intake_pause_reason = "owned_inventory_revoked"
    for item in await inventory_items(db, row.id, lock=True):
        if item.state == "unused":
            seed = await _row(db, ManualSeed, item.manual_seed_id, lock=True)
            seed.status = "inventory_revoked"
    await db.commit()
    return await read_inventory(db, channel_id, inventory_id)


async def read_inventory(db: AsyncSession, channel_id: uuid.UUID, inventory_id: uuid.UUID) -> dict:
    row = await _row(db, OwnedSeedInventory, inventory_id)
    require(row.channel_profile_id == channel_id, "owned_inventory_scope_mismatch")
    require(sha256(row.manifest_json) == row.manifest_sha256, "owned_inventory_manifest_changed")
    return {"id": str(row.id), "state": row.state, "manifest": row.manifest_json,
            "manifest_sha256": row.manifest_sha256, "approved_at": row.approved_at,
            "approved_by": row.approved_by, "approval_reference": row.approval_reference,
            "revoked_at": row.revoked_at, "revoked_by": row.revoked_by, "hold_reason": row.hold_reason,
            "succession_released_at": row.succession_released_at,
            "items": [{"id": str(item.id), "state": item.state, "manual_seed_id": str(item.manual_seed_id),
                       "production_task_id": str(item.production_task_id) if item.production_task_id else None}
                      for item in await inventory_items(db, row.id)],
            "closeout": await closeout(db, row)}


async def assert_account_binding_available(db: AsyncSession, platform_channel_id: str, account_id: uuid.UUID | None = None) -> None:
    await lock_platform_scope(db, platform_channel_id)
    occupied = (await db.scalars(select(OwnedSeedInventory).where(
        OwnedSeedInventory.platform_channel_id == platform_channel_id,
        OwnedSeedInventory.approved_at.is_not(None), OwnedSeedInventory.succession_released_at.is_(None),
    ))).one_or_none()
    require(occupied is None or occupied.target_account_id == account_id, "owned_inventory_platform_slot_occupied")


async def assert_asset_deletable(db: AsyncSession, asset_id: uuid.UUID) -> Asset | None:
    asset = (await db.scalars(select(Asset).where(Asset.id == asset_id).with_for_update()
                              .execution_options(populate_existing=True))).one_or_none()
    if asset is None:
        return None
    referenced = (await db.scalars(select(OwnedSeedInventoryItem.id).where(OwnedSeedInventoryItem.asset_id == asset_id).limit(1))).first()
    require(referenced is None, "owned_inventory_asset_pinned")
    return asset


async def assert_account_mutable(db: AsyncSession, account_id: uuid.UUID) -> None:
    approved = (await db.scalars(select(OwnedSeedInventory.id).where(
        OwnedSeedInventory.target_account_id == account_id, OwnedSeedInventory.approved_at.is_not(None),
    ).limit(1))).first()
    require(approved is None, "owned_inventory_account_pinned")
