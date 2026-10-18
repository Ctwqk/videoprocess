"""Explicit, audited recovery of an unused inventory's diagnosed Redis hold."""
from __future__ import annotations

import asyncio
import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from urllib.parse import unquote, urlsplit

from sqlalchemy import or_, select, text

from app.models.channel_agent import ChannelProfile, ManualSeed, ProductionTask
from app.models.owned_seed_inventory import OwnedSeedInventory
from app.models.owned_inventory_recovery import OwnedInventoryRecoveryRecord
from app.models.schedule import RuntimeSchedule
from app.models.worker_registration import WorkerAdmissionGrant, WorkerRegistration
from app.schemas.channel_agent import OwnedHistoryLocators
from app.schemas.owned_inventory_recovery import OwnedInventoryRecovery
from app.services import owned_seed_inventory as inv
from app.services import owned_seed_inventory_history as history


HOLD_REASON = "redis_consumer_identity_invalid"
_UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
# Same fixed stream/group/slot and freshness policy as channelops-soak-watch.sh.
STREAMS = (
    ("vp:tasks:ffmpeg_go", "ffmpeg_go-workers", rf"ffmpeg_go-worker@colima-127:1:{_UUID}"),
    ("vp:tasks:ffmpeg", "ffmpeg-workers", rf"ffmpeg-worker@150-gpu:1:{_UUID}"),
    ("vp:tasks:vision", "vision-workers", rf"vision-worker@150-vision:1:{_UUID}"),
    ("vp:tasks:youtube_publisher", "youtube_publisher-workers", rf"youtube_publisher-worker@150-publisher:1:{_UUID}"),
    ("vp:events", "orchestrator", r"orchestrator-api-[1-9][0-9]*"),
)


async def observe_runtime_redis() -> list[dict]:
    """Read every normal group through the existing qualified named credential."""
    client = None
    try:
        url = inv._history_redis_url()
        parsed = urlsplit(url)
        principal = unquote(parsed.username or "")
        inv.require(parsed.scheme in {"redis", "rediss"} and bool(parsed.hostname)
            and not parsed.query and not parsed.fragment and principal not in {"", "default"}
            and bool(parsed.password) and (parsed.port is None or 1 <= parsed.port <= 65535)
            and (parsed.path in {"", "/"} or parsed.path[1:].isdigit() and 0 <= int(parsed.path[1:]) <= 15),
            "owned_history_redis_configuration")
        async with asyncio.timeout(30):
            client = inv._history_redis(url)
            inv.require(await client.acl_whoami() == principal, "owned_history_redis_identity")
            evidence = []
            for stream, group, pattern in STREAMS:
                groups = await client.xinfo_groups(stream)
                inv.require(type(groups) is list and len(groups) <= 128 and all(type(g) is dict for g in groups),
                            "owned_inventory_recovery_redis_group")
                selected = [g for g in groups if g.get("name") == group]
                inv.require(len(selected) == 1 and all(type(selected[0].get(k)) is int and selected[0][k] == 0
                            for k in ("pending", "lag")), "owned_inventory_recovery_redis_backlog")
                consumers = await client.xinfo_consumers(stream, group)
                inv.require(type(consumers) is list and len(consumers) == 1 and type(consumers[0]) is dict, HOLD_REASON)
                consumer = consumers[0]
                name = consumer.get("name")
                inv.require(isinstance(name, str) and re.fullmatch(pattern, name) is not None
                    and not name.endswith(":00000000-0000-0000-0000-000000000000")
                    and type(consumer.get("idle")) is int and 0 <= consumer["idle"] <= 120000
                    and type(consumer.get("pending")) is int and consumer["pending"] == 0, HOLD_REASON)
                evidence.append({"stream": stream, "group": group, "consumer": name,
                                 "idle_ms": consumer["idle"], "pending": 0, "lag": 0})
            return evidence
    except inv.OwnedInventoryError:
        raise
    except Exception:
        raise inv.OwnedInventoryError("owned_inventory_recovery_redis_read_failed") from None
    finally:
        if client is not None:
            try:
                async with asyncio.timeout(5):
                    await client.aclose()
            except Exception:
                raise inv.OwnedInventoryError("owned_history_redis_close_failed") from None


def require_recoverable_state(row: dict, now: datetime) -> None:
    inv.require(row["manifest_json"].get("version") == 2 and row["state"] == "held"
        and row["hold_reason"] == HOLD_REASON and row["approved_at"] is not None
        and inv.utc(row["approved_at"]) <= now and bool((row["approved_by"] or "").strip())
        and bool((row["approval_reference"] or "").strip()) and row["revoked_at"] is None
        and row["revoked_by"] is None and row["succession_released_at"] is None,
        "owned_inventory_not_recoverable")
    inv.require(inv.utc(row["starts_at"]) <= now < inv.utc(row["expires_at"])
        and inv.utc(row["expires_at"]) - inv.utc(row["starts_at"]) == timedelta(days=7),
        "owned_inventory_recovery_window")
    inv.require(row["privacy"] == "unlisted" and row["max_admissions"] == 7
        and row["minimum_interval_seconds"] == 86400, "owned_inventory_limits")


def bind_runtime_registrations(rows: dict, observations: list[dict], now: datetime) -> list[dict]:
    """Pattern-valid Redis names must be the exact currently admitted live workers."""
    result = []
    try:
        for observation in observations:
            if observation["stream"] == "vp:events":
                continue  # API event consumer retains the watcher's fixed identity rule.
            worker_type = observation["stream"].removeprefix("vp:tasks:")
            registrations = [r for r in rows["worker_registrations"] if r["worker_type"] == worker_type and r["status"] == "active"]
            grants = [g for g in rows["worker_admission_grants"] if g["worker_type"] == worker_type and g["state"] == "active"]
            inv.require(len(registrations) == len(grants) == 1, "owned_inventory_recovery_registration")
            registration, grant = registrations[0], grants[0]
            inv.require(registration["grant_id"] == grant["id"] and registration["revoked_at"] is None
                and registration["revoke_reason"] is None and registration["superseded_by"] is None
                and grant["revoked_at"] is None and grant["revoke_reason"] is None
                and history._time(grant["activated_at"]) <= now and history._time(registration["registered_at"]) <= now
                and 0 <= (now - history._time(registration["heartbeat_at"])).total_seconds() <= 120
                and history._time(registration["lease_expires_at"]) > now
                and registration["worker_slot"] == 1 and registration["lease_epoch"] > 0
                and all(registration[key] == grant[key] for key in (
                    "service_name", "worker_type", "worker_host", "image_identity", "database_principal", "capabilities_json"))
                and grant["redis_stream"] == observation["stream"] and grant["redis_group"] == observation["group"]
                and registration["redis_consumer_id"] == observation["consumer"] ==
                    f"{worker_type}-worker@{registration['worker_host']}:1:{registration['worker_instance_id']}",
                "owned_inventory_recovery_registration")
            result.append({"registration_id": registration["id"], "grant_id": grant["id"],
                "service_name": registration["service_name"], "consumer_id": registration["redis_consumer_id"],
                "worker_instance_id": registration["worker_instance_id"], "lease_epoch": registration["lease_epoch"],
                "generation": grant["generation"], "release_commit": grant["release_commit"],
                "image_identity": grant["image_identity"]})
        inv.require(len(result) == 4, "owned_inventory_recovery_registration")
        return result
    except (KeyError, TypeError, ValueError):
        raise inv.OwnedInventoryError("owned_inventory_recovery_registration") from None


async def _lock_runtime_registrations(db):
    worker_types = [stream.removeprefix("vp:tasks:") for stream, _, _ in STREAMS if stream != "vp:events"]
    # Match native reconnect and grant activation: registration rows precede grants.
    for model, predicate in ((WorkerRegistration, WorkerRegistration.status == "active"),
                             (WorkerAdmissionGrant, WorkerAdmissionGrant.state == "active")):
        await db.execute(select(model.id).where(model.worker_type.in_(worker_types), predicate)
                         .order_by(model.id).with_for_update(read=True))


def _values(row) -> dict:
    return {column.name: getattr(row, column.name) for column in row.__table__.columns}


def hold_fingerprint(row, channel) -> str:
    return inv.sha256({"inventory": _values(row), "channel": _values(channel)})


@dataclass(frozen=True)
class PreparedRecovery:
    inventory: OwnedSeedInventory
    channel: ChannelProfile
    evidence: dict


async def _check_scope(db, row, channel, data, *, lock=False):
    require_recoverable_state(_values(row), await inv._now(db))
    inv.require(row.manifest_sha256 == data.manifest_sha256, "owned_inventory_manifest_mismatch")
    inv.require(channel.id == row.channel_profile_id and channel.owned_seed_inventory_id == row.id
        and channel.tick_interval_minutes == 1 and channel.halted_at is None and channel.halt_reason is None
        and channel.intake_paused_at is not None and channel.intake_pause_reason == HOLD_REASON,
        "owned_inventory_recovery_pause_mismatch")
    inv.require(hold_fingerprint(row, channel) == data.expected_hold_sha256, "owned_inventory_recovery_incident_changed")
    _, fingerprint = await inv._scope(db, channel.id, row, lock=lock)
    inv.require(fingerprint == row.manifest_json["configuration_sha256"], "owned_inventory_configuration_changed")
    schedule = await db.get(RuntimeSchedule, "videoprocess", populate_existing=True)
    inv.require(schedule is not None and schedule.state == "CLOSED" and schedule.guarded_job_id is None,
                "owned_inventory_recovery_schedule")
    items = await inv.inventory_items(db, row.id, lock=lock)
    inv.require(len(items) == 7 and all(item.state == "unused" and item.production_task_id is None
        and item.consumed_at is None and item.completed_at is None and item.hold_reason is None for item in items),
        "owned_inventory_recovery_used")
    for item in items:
        seed = await inv._row(db, ManualSeed, item.manual_seed_id, lock=lock)
        inv.require(seed.status == "active", "owned_inventory_seed_changed")
    await inv._verify_manifest(db, row)
    tasks = await db.scalar(select(ProductionTask.id).where(or_(
        ProductionTask.channel_profile_id == channel.id, ProductionTask.target_account_id == row.target_account_id,
        ProductionTask.manual_seed_id.in_([item.manual_seed_id for item in items]))).limit(1))
    inv.require(tasks is None, "owned_inventory_recovery_current_work")
    return items


def _locators(row):
    decoded = history.decode_history_manifest(row.manifest_json)
    inv.require(decoded.legacy_history is not None, "owned_inventory_not_recoverable")
    legacy = decoded.legacy_history
    certificate = legacy.retired_unassigned_preupload
    return legacy, OwnedHistoryLocators.model_validate({"operations": sorted([
        {"operation_id": operation, "legacy_account_id": binding.legacy_account_id,
         "legacy_channel_profile_id": binding.legacy_channel_profile_id}
        for binding in legacy.bindings for operation in binding.qualified_operation_ids], key=lambda value: value["operation_id"]),
        "retired_unassigned_preupload": {"operation_id": certificate.operation_id,
            "legacy_account_id": certificate.legacy_account_id,
            "legacy_channel_profile_id": certificate.legacy_channel_profile_id} if certificate else None})


async def prepare_recovery(db, channel_id: uuid.UUID, inventory_id: uuid.UUID,
                           data: OwnedInventoryRecovery, subject: str) -> PreparedRecovery:
    """Requalify without provisional approval; caller must commit under these locks."""
    from app.channel_agent import owned_inventory as admission
    from app.services.owned_seed_inventory_retention import stable_certificate

    row = await inv._row(db, OwnedSeedInventory, inventory_id)
    channel = await inv._row(db, ChannelProfile, channel_id)
    items = await _check_scope(db, row, channel, data)
    legacy, locators = _locators(row)
    descriptors = await inv._observe_assets(db, [item.asset_id for item in items])
    expected = {item.asset_id: (item.content_sha256, item.storage_descriptor_json) for item in items}
    inv.require(all(inv.canonical(value) == inv.canonical(expected[key][1]) for key, value in descriptors.items()),
                "owned_inventory_asset_changed")
    platform, target = row.platform_channel_id, str(row.target_account_id)
    initial = await history.load_owned_history_evidence(db, platform_channel_id=platform)
    sources = inv._history_sources(initial, locators, target)
    retirement = inv._retirement_sources(initial, requested=legacy.retired_unassigned_preupload is not None)
    await db.rollback()
    hashes = await inv._hash_assets(descriptors)
    inv.require(all(hashes[key] == expected[key][0] for key in hashes), "owned_inventory_content_mismatch")
    observed_uploads = await inv._observe_history_uploads(sources, platform)
    observed_retirement = await inv._observe_retirement(retirement, observed_at=initial.observed_at)
    runtime_redis = await observe_runtime_redis()
    registrations = bind_runtime_registrations(initial.rows.as_dict(), runtime_redis, initial.observed_at)
    await inv._lock_history_scope(db, channel_id, locators, retirement)
    await inv.lock_platform_scope(db, platform)
    row = await inv._row(db, OwnedSeedInventory, inventory_id, lock=True)
    channel = await inv._row(db, ChannelProfile, channel_id, lock=True)
    await _check_scope(db, row, channel, data, lock=True)
    await inv._lock_assets(db, descriptors)
    if retirement:
        await inv._lock_assets(db, inv._retirement_descriptors(retirement))
    if db.get_bind().dialect.name == "postgresql":
        await _lock_runtime_registrations(db)
    fresh = await history.load_owned_history_evidence(db, platform_channel_id=platform)
    fresh_sources = inv._history_sources(fresh, locators, target)
    inv.require(fresh_sources == sources, "owned_inventory_history_changed")
    qualified = inv._qualified_history(fresh, fresh_sources, observed_uploads, subject, data.recovery_reference,
                                      observed_at=initial.observed_at)
    inv.require([inv._stable_binding(binding) for binding in qualified["bindings"]]
        == [inv._stable_binding(binding.document.as_dict()) for binding in legacy.bindings], "owned_inventory_history_changed")
    inv.require([stable_certificate(cert) for cert in qualified.get("retained_terminal_uploads", [])]
        == [stable_certificate(cert.as_dict()) for cert in legacy.retained_terminal_uploads], "owned_inventory_history_changed")
    retired = inv._qualified_retirement(fresh, retirement, observed_retirement, subject, data.recovery_reference)
    original_retired = legacy.retired_unassigned_preupload
    inv.require((retired is None) == (original_retired is None), "owned_inventory_retirement_changed")
    if retired is not None:
        assert original_retired is not None and observed_retirement is not None
        inv.require(inv._stable_retirement(retired) == inv._stable_retirement(original_retired.document.as_dict()),
                    "owned_inventory_retirement_changed")
        fresh = history.OwnedHistorySnapshot.from_rows(fresh.rows.as_dict(), platform_channel_id=platform,
            observed_at=fresh.observed_at, redis_observations=observed_retirement[1])
    now = await inv._now(db)
    inv.require(0 <= (now - initial.observed_at).total_seconds() <= history.MAX_OBSERVATION_AGE_SECONDS,
                "owned_inventory_history_observation_stale")
    require_recoverable_state(_values(row), now)
    rows = fresh.rows.as_dict()
    inv.require(bind_runtime_registrations(rows, runtime_redis, now) == registrations,
                "owned_inventory_recovery_registration_changed")
    admission.production_target(rows, row, now)
    assessment = history.assess_owned_history(fresh, now=now)
    inv.require(assessment.block_reason is None, assessment.block_reason or "owned_inventory_history_invalid")
    inv.require(assessment.wait_reason is None, "owned_inventory_recovery_history_not_ready")
    inv.require(not any(job["status"] in {"PENDING", "WAITING_WINDOW", "VALIDATING", "PLANNING", "RUNNING"} for job in rows["jobs"])
        and not any(node["status"] in {"QUEUED", "RUNNING"} for node in rows["node_executions"])
        and admission.queues_safe(rows, assessment, str(channel_id), now)
        and not any(queue["channel_profile_id"] == str(channel_id) and queue["status"] in {"queued", "running"}
                    for queue in rows["channel_ops_queue_items"]), "owned_inventory_recovery_current_work")
    inv.require(all(value not in assessment.retired_source_sha256 + assessment.retired_render_sha256 for value in hashes.values()),
                "owned_inventory_retired_hash_reuse")
    return PreparedRecovery(row, channel, {"version": 1, "observed_at": initial.observed_at.isoformat(),
        "qualified_at": now.isoformat(), "history_sha256": assessment.stable_history_sha256,
        "history_authority_sha256": assessment.authority_sha256, "source_sha256": sorted(hashes.values()),
        "runtime_redis": runtime_redis, "registrations": registrations, "operator_runtime_evidence": {
            **data.operator_runtime_evidence.model_dump(), "verification": "authenticated_operator_attestation"}})


async def recover_inventory(db, channel_id, inventory_id, data, subject):
    inv.require(db.get_bind().dialect.name == "postgresql", "owned_inventory_recovery_postgres_required")
    inv.require(not db.new and not db.dirty and not db.deleted, "owned_inventory_pending_session_changes")
    request_id = uuid.UUID(data.client_request_id)
    request_sha = inv.sha256({"channel_id": channel_id, "inventory_id": inventory_id, "subject": subject,
                             "request": data.model_dump(mode="json")})

    async def existing_result():
        record = await db.get(OwnedInventoryRecoveryRecord, request_id, populate_existing=True)
        if record is None:
            return None
        inv.require(record.inventory_id == inventory_id and record.request_sha256 == request_sha
                    and record.recovered_by == subject, "owned_inventory_recovery_idempotency_conflict")
        return await _result(db, channel_id, inventory_id, record)

    replay = await existing_result()
    if replay is not None:
        return replay
    try:
        prepared = await prepare_recovery(db, channel_id, inventory_id, data, subject)
    except (inv.OwnedInventoryError, history.OwnedHistoryError):
        # An identical contender may have committed while this request observed
        # external facts. Replay only its exact immutable record, never requalify
        # by pretending a later held inventory is approved.
        await db.rollback()
        replay = await existing_result()
        if replay is not None:
            return replay
        raise
    inventory_before = await db.scalar(text("SELECT to_jsonb(i) FROM public.owned_seed_inventories i WHERE id=:id"), {"id": inventory_id})
    channel_before = await db.scalar(text("SELECT to_jsonb(c) FROM public.channel_profiles c WHERE id=:id"), {"id": channel_id})
    evidence = {**prepared.evidence, "recovery_reference": data.recovery_reference}
    await db.execute(text("""SELECT public.vp_recover_owned_inventory(
        :channel_id,:inventory_id,:request_id,:subject,:request_sha,:hold_sha,
        CAST(:inventory_before AS jsonb),CAST(:channel_before AS jsonb),CAST(:evidence AS jsonb))"""), {
        "channel_id": channel_id, "inventory_id": inventory_id, "request_id": request_id, "subject": subject,
        "request_sha": request_sha, "hold_sha": data.expected_hold_sha256,
        "inventory_before": json.dumps(inventory_before), "channel_before": json.dumps(channel_before),
        "evidence": inv.canonical(evidence)})
    await db.commit()
    return await existing_result()


async def _result(db, channel_id, inventory_id, record):
    result = await inv.read_inventory(db, channel_id, inventory_id)
    result["recovery"] = {"id": str(record.id), "recovered_at": record.recovered_at,
        "recovered_by": record.recovered_by, "expected_hold_sha256": record.expected_hold_sha256,
        "original_hold_reason": record.before_inventory_json["hold_reason"],
        "recovery_reference": record.evidence_json["recovery_reference"]}
    return result
