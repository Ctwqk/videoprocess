from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.channel_agent import MaterialUsageLedger


@dataclass(frozen=True)
class MaterialReference:
    material_id: str
    asset_id: str | None = None
    start_ms: int | None = None
    end_ms: int | None = None
    segment_signature: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class UsageGuardResult:
    repetition_rejected: bool = False
    cross_account_rejected: bool = False
    hits: list[dict[str, Any]] = field(default_factory=list)

    @property
    def blocked(self) -> bool:
        return self.repetition_rejected or self.cross_account_rejected


def segment_signature(material_id: str, start_ms: int | None, end_ms: int | None) -> str:
    raw = f"{material_id}:{start_ms if start_ms is not None else ''}:{end_ms if end_ms is not None else ''}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def extract_material_references(
    *,
    plan_payload: dict[str, Any],
    run_payload: dict[str, Any],
    upload_metadata: dict[str, Any],
) -> list[MaterialReference]:
    seen: set[tuple[str, str]] = set()
    refs: list[MaterialReference] = []
    for payload in (plan_payload, run_payload, upload_metadata):
        for item in _walk_dicts(payload):
            ref = _reference_from_dict(item)
            if ref is None:
                continue
            key = (ref.material_id, ref.segment_signature)
            if key in seen:
                continue
            seen.add(key)
            refs.append(ref)
    return refs


async def recent_usage_flags(
    db: AsyncSession,
    *,
    channel_id: str,
    lane_id: str | None,
    account_id: str | None,
    references: list[MaterialReference],
    now: datetime,
    same_lane_window: timedelta = timedelta(days=7),
    same_account_window: timedelta = timedelta(days=14),
    sibling_account_window: timedelta = timedelta(days=30),
) -> UsageGuardResult:
    if not references:
        return UsageGuardResult()

    channel_uuid = _uuid_or_none(channel_id)
    lane_uuid = _uuid_or_none(lane_id)
    account_uuid = _uuid_or_none(account_id)
    if channel_uuid is None:
        return UsageGuardResult()

    material_ids = {ref.material_id for ref in references}
    segment_signatures = {ref.segment_signature for ref in references if ref.segment_signature}
    oldest_cutoff = _as_utc(now) - max(same_lane_window, same_account_window, sibling_account_window)
    rows = (
        await db.execute(
            select(MaterialUsageLedger)
            .where(MaterialUsageLedger.channel_profile_id == channel_uuid)
            .where(MaterialUsageLedger.used_at >= oldest_cutoff)
        )
    ).scalars().all()

    hits: list[dict[str, Any]] = []
    same_lane_cutoff = _as_utc(now) - same_lane_window
    same_account_cutoff = _as_utc(now) - same_account_window
    sibling_cutoff = _as_utc(now) - sibling_account_window

    for row in rows:
        used_at = _as_utc(row.used_at)
        if (
            lane_uuid is not None
            and row.topic_lane_id == lane_uuid
            and row.segment_signature in segment_signatures
            and used_at >= same_lane_cutoff
        ):
            hits.append(_hit(row, guard="repetition_rejected", reason="same_lane_segment_recently_used"))
        if (
            account_uuid is not None
            and row.publishing_account_id == account_uuid
            and row.material_id in material_ids
            and used_at >= same_account_cutoff
        ):
            hits.append(_hit(row, guard="repetition_rejected", reason="same_account_material_recently_used"))
        if (
            account_uuid is not None
            and row.publishing_account_id is not None
            and row.publishing_account_id != account_uuid
            and row.material_id in material_ids
            and used_at >= sibling_cutoff
        ):
            hits.append(_hit(row, guard="cross_account_rejected", reason="sibling_account_material_recently_used"))

    guard_names = {hit["guard"] for hit in hits}
    return UsageGuardResult(
        repetition_rejected="repetition_rejected" in guard_names,
        cross_account_rejected="cross_account_rejected" in guard_names,
        hits=hits,
    )


def _reference_from_dict(item: dict[str, Any]) -> MaterialReference | None:
    material_id = str(item.get("material_id") or item.get("materialId") or "").strip()
    if not material_id:
        return None
    start_ms = _millis(item.get("start_ms"), item.get("start_sec"))
    end_ms = _millis(item.get("end_ms"), item.get("end_sec"))
    signature = str(item.get("segment_signature") or item.get("segmentSignature") or "").strip()
    if not signature:
        signature = segment_signature(material_id, start_ms, end_ms)
    asset_id = item.get("asset_id") or item.get("assetId")
    return MaterialReference(
        material_id=material_id,
        asset_id=str(asset_id) if asset_id else None,
        start_ms=start_ms,
        end_ms=end_ms,
        segment_signature=signature,
        metadata=dict(item),
    )


def _walk_dicts(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from _walk_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_dicts(child)


def _millis(ms_value: Any, sec_value: Any) -> int | None:
    if ms_value is not None:
        try:
            return int(ms_value)
        except (TypeError, ValueError):
            return None
    if sec_value is not None:
        try:
            return int(float(sec_value) * 1000)
        except (TypeError, ValueError):
            return None
    return None


def _hit(row: MaterialUsageLedger, *, guard: str, reason: str) -> dict[str, Any]:
    return {
        "guard": guard,
        "reason": reason,
        "material_id": row.material_id,
        "segment_signature": row.segment_signature,
        "publication_id": str(row.publication_id) if row.publication_id else "",
        "used_at": _as_utc(row.used_at).isoformat(),
    }


def _uuid_or_none(value: Any) -> uuid.UUID | None:
    if not value:
        return None
    try:
        return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
    except (TypeError, ValueError):
        return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
