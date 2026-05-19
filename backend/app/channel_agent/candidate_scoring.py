from __future__ import annotations

from datetime import datetime
from typing import Any

from app.channel_agent.material_usage import UsageGuardResult, extract_material_references


def score_candidate(candidate: dict[str, Any], *, now: datetime) -> dict[str, float]:
    lane = candidate.get("lane")
    account = candidate.get("account")
    lane_weight = _bounded_float(getattr(lane, "weight", 1.0), default=1.0)
    refs = extract_material_references(
        plan_payload={},
        run_payload={},
        upload_metadata={
            "material_refs": _constraints(candidate).get("material_refs")
            or _constraints(candidate).get("material_references")
            or [],
        },
    )
    usage_guard = candidate.get("_material_usage_guard")
    repetition_risk = 1.0 if isinstance(usage_guard, UsageGuardResult) and usage_guard.repetition_rejected else 0.0
    compliance_risk = 0.0
    pds_decision = candidate.get("_pds_decision")
    if getattr(pds_decision, "verdict", "") == "flag":
        compliance_risk = 0.5
    elif getattr(pds_decision, "verdict", "") == "block":
        compliance_risk = 1.0
    material_fit = 1.0 if refs or candidate.get("material_library_ids_json") else 0.6
    freshness = 1.0 - repetition_risk
    account_fit = 1.0 if account is not None else 0.0
    timing = 0.8
    novelty = 1.0 - repetition_risk
    total = (
        0.25 * lane_weight
        + 0.20 * material_fit
        + 0.15 * freshness
        + 0.15 * account_fit
        + 0.10 * timing
        + 0.10 * novelty
        - 0.20 * repetition_risk
        - 0.30 * compliance_risk
    )
    return {
        "lane_weight": round(lane_weight, 4),
        "material_fit": round(material_fit, 4),
        "freshness": round(freshness, 4),
        "account_fit": round(account_fit, 4),
        "timing": round(timing, 4),
        "novelty": round(novelty, 4),
        "repetition_risk": round(repetition_risk, 4),
        "compliance_risk": round(compliance_risk, 4),
        "total_score": round(total, 4),
    }


def _constraints(candidate: dict[str, Any]) -> dict[str, Any]:
    value = candidate.get("constraints_json")
    return dict(value) if isinstance(value, dict) else {}


def _bounded_float(value: Any, *, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if parsed < 0:
        return 0.0
    if parsed > 1:
        return 1.0
    return parsed
