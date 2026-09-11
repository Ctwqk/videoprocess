from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any

from sqlalchemy import func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.asset import Asset
from app.models.channel_agent import ChannelOpsQueueItem, ChannelProfile, LaneFormatMatrix, ManualSeed, ProductionTask, PublishingAccount, TopicLane
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.owned_seed_inventory import OwnedSeedInventory, OwnedSeedInventoryItem
from app.models.youtube_upload_operation import YouTubeUploadOperation
from app.schemas.channel_agent import OwnedSeedInventoryApprove, OwnedSeedInventoryCreate, OwnedSeedInventoryRevoke
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


async def create_inventory(db: AsyncSession, channel_id: uuid.UUID, data: OwnedSeedInventoryCreate, subject: str) -> dict:
    request_digest = sha256(data.model_dump(mode="json"))
    existing = (await db.scalars(select(OwnedSeedInventory).where(OwnedSeedInventory.client_request_id == uuid.UUID(data.client_request_id)))).one_or_none()
    if existing is not None:
        require(existing.channel_profile_id == channel_id and existing.request_sha256 == request_digest, "owned_inventory_idempotency_conflict")
        return await read_inventory(db, channel_id, existing.id)
    _, fingerprint = await _scope(db, channel_id, data)
    descriptors = await _observe_assets(db, [uuid.UUID(entry.asset_id) for entry in data.entries])
    # Storage I/O is outside the SQL transaction; all observations are rechecked below.
    await db.rollback()
    hashes = await _hash_assets(descriptors)
    for entry in data.entries:
        require(hashes[uuid.UUID(entry.asset_id)] == entry.expected_content_sha256, "owned_inventory_content_mismatch")
    _, fresh_fingerprint = await _scope(db, channel_id, data, lock=True)
    require(fingerprint == fresh_fingerprint, "owned_inventory_configuration_changed")
    await _lock_assets(db, descriptors)
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


async def approve_inventory(db: AsyncSession, channel_id: uuid.UUID, inventory_id: uuid.UUID,
                            data: OwnedSeedInventoryApprove, subject: str) -> dict:
    row = await _row(db, OwnedSeedInventory, inventory_id)
    require(row.channel_profile_id == channel_id and row.manifest_sha256 == data.manifest_sha256, "owned_inventory_manifest_mismatch")
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
