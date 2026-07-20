from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any


PRE_UPLOAD_SCOPE = "external_asset_pre_upload"
PROMOTION_SCOPE = "publication_promotion"
_INVALID_PLAN_STATUSES = frozenset({"blocked", "rejected"})


def task_uses_external_assets(task: Any) -> bool:
    if bool(getattr(task, "uses_external_assets", False)):
        return True
    if getattr(task, "source_platforms_json", None):
        return True
    snapshot = getattr(task, "channel_config_snapshot_json", None)
    if not isinstance(snapshot, dict):
        return False
    lane_format = snapshot.get("lane_format")
    return isinstance(lane_format, dict) and bool(lane_format.get("source_platforms_json"))


def merge_human_review_evidence(current: dict[str, Any] | None, key: str, evidence: dict[str, Any]) -> dict[str, Any]:
    return {**dict(current or {}), key: dict(evidence)}


def build_pre_upload_evidence(
    *,
    plan: Any,
    human_actor: str,
    review_notes: str | None,
) -> dict[str, Any]:
    approved_at = _plan_review_approved_at(plan)
    if approved_at is None:
        raise ValueError("AutoFlow plan has no human review approval token")
    token = _datetime_token(approved_at)
    evidence: dict[str, Any] = {
        "kind": "human_review",
        "scope": PRE_UPLOAD_SCOPE,
        "human_actor": _nonblank(human_actor),
        "reviewed_at": token,
        "autoflow_plan_id": _plan_id(plan),
        "plan_review_approved_at": token,
    }
    if review_notes is not None:
        evidence["review_notes"] = review_notes
    return evidence


def build_promotion_evidence(
    *,
    task: Any,
    publication_id: uuid.UUID,
    target_visibility: str,
    human_actor: str,
    reviewed_at: datetime,
    review_notes: str | None,
    plan: Any | None,
) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "kind": "human_review",
        "scope": PROMOTION_SCOPE,
        "human_actor": _nonblank(human_actor),
        "reviewed_at": _datetime_token(reviewed_at),
        "production_task_id": str(task.id),
        "publication_id": str(publication_id),
        "target_visibility": target_visibility,
    }
    if plan is not None:
        approved_at = _plan_review_approved_at(plan)
        if approved_at is None:
            raise ValueError("AutoFlow plan has no human review approval token")
        evidence.update(
            {
                "autoflow_plan_id": _plan_id(plan),
                "plan_review_approved_at": _datetime_token(approved_at),
            }
        )
    if review_notes is not None:
        evidence["review_notes"] = review_notes
    return evidence


def valid_pre_upload_evidence(task: Any, plan: Any | None) -> bool:
    if plan is None or not _plan_is_currently_reviewable(plan):
        return False
    task_plan_id = getattr(task, "autoflow_plan_id", None)
    approved_at = _plan_review_approved_at(plan)
    plan_id = _plan_id(plan)
    if task_plan_id is None or approved_at is None or str(task_plan_id) != plan_id:
        return False
    evidence_root = getattr(task, "human_review_evidence_json", None)
    evidence = evidence_root.get("pre_upload") if isinstance(evidence_root, dict) else None
    if not isinstance(evidence, dict):
        return False
    if evidence.get("kind") != "human_review" or evidence.get("scope") != PRE_UPLOAD_SCOPE:
        return False
    if not _is_nonblank(evidence.get("human_actor")) or not _is_timestamp(evidence.get("reviewed_at")):
        return False
    if str(evidence.get("autoflow_plan_id", "")) != plan_id:
        return False
    return _same_datetime(evidence.get("plan_review_approved_at"), approved_at)


def valid_promotion_evidence(
    task: Any,
    plan: Any | None,
    *,
    publication_id: uuid.UUID,
    target_visibility: str,
    require_plan: bool,
) -> bool:
    evidence_root = getattr(task, "human_review_evidence_json", None)
    evidence = evidence_root.get("promotion") if isinstance(evidence_root, dict) else None
    if not isinstance(evidence, dict):
        return False
    if evidence.get("kind") != "human_review" or evidence.get("scope") != PROMOTION_SCOPE:
        return False
    if not _is_nonblank(evidence.get("human_actor")) or not _is_timestamp(evidence.get("reviewed_at")):
        return False
    if str(evidence.get("production_task_id", "")) != str(task.id):
        return False
    if str(evidence.get("publication_id", "")) != str(publication_id):
        return False
    if evidence.get("target_visibility") != target_visibility:
        return False
    if not require_plan:
        return True
    if not valid_pre_upload_evidence(task, plan):
        return False
    if not isinstance(evidence_root, dict):
        return False
    pre_upload = evidence_root.get("pre_upload")
    if not isinstance(pre_upload, dict):
        return False
    return (
        evidence.get("autoflow_plan_id") == pre_upload.get("autoflow_plan_id")
        and evidence.get("plan_review_approved_at") == pre_upload.get("plan_review_approved_at")
    )


def _plan_is_currently_reviewable(plan: Any) -> bool:
    if str(getattr(plan, "status", "")).lower() in _INVALID_PLAN_STATUSES:
        return False
    rights = getattr(plan, "rights_json", None)
    if rights is None:
        rights = getattr(plan, "rights", None)
    rights_status = str((rights or {}).get("status", "")).lower() if isinstance(rights, dict) else ""
    return rights_status not in _INVALID_PLAN_STATUSES


def _plan_review_approved_at(plan: Any) -> datetime | None:
    value = getattr(plan, "review_approved_at", None)
    return value if isinstance(value, datetime) else None


def _plan_id(plan: Any) -> str:
    value = getattr(plan, "id", None)
    if value is None:
        value = getattr(plan, "plan_id", "")
    return str(value)


def _same_datetime(raw: Any, expected: datetime) -> bool:
    parsed = _parse_datetime(raw)
    return parsed is not None and _utc(parsed) == _utc(expected)


def _is_timestamp(raw: Any) -> bool:
    return _parse_datetime(raw) is not None


def _parse_datetime(raw: Any) -> datetime | None:
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        return datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
    except ValueError:
        return None


def _datetime_token(value: datetime) -> str:
    return _utc(value).isoformat()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _is_nonblank(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _nonblank(value: str) -> str:
    resolved = value.strip()
    if not resolved:
        raise ValueError("human_actor must be nonblank")
    return resolved
