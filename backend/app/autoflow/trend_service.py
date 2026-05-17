from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.autoflow import TrendSignal


class TrendService:
    def __init__(self) -> None:
        self._signals: list[dict[str, Any]] = []

    def add_signal(self, payload: dict[str, Any]) -> dict[str, Any]:
        signal = _signal_from_payload(payload, signal_id=f"trend-{uuid.uuid4()}")
        self._signals.append(signal)
        return signal

    async def add_signal_db(self, payload: dict[str, Any], db: AsyncSession) -> dict[str, Any]:
        signal = _signal_from_payload(payload, signal_id="")
        row = TrendSignal(
            source=signal["source"],
            keyword=signal["keyword"],
            score=signal["score"],
            metadata_json={
                "trend_growth": signal["trend_growth"],
                "cross_platform_mentions": signal["cross_platform_mentions"],
                "material_availability": signal["material_availability"],
                "competition": signal["competition"],
                "rights_risk": signal["rights_risk"],
                "metadata": signal["metadata"],
            },
        )
        db.add(row)
        await db.commit()
        await db.refresh(row)
        return _signal_from_model(row)

    def suggest(
        self,
        *,
        material_library_ids: list[str] | None = None,
        source_policy: str = "owned_only",
        template_performance: list[dict[str, Any]] | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        performance = template_performance or []
        suggestions = [
            self._suggestion(signal, material_library_ids or [], source_policy, performance)
            for signal in self._signals
            if signal.get("keyword")
        ]
        return sorted(suggestions, key=lambda item: item["opportunity_score"], reverse=True)[:limit]

    async def suggest_db(
        self,
        db: AsyncSession,
        *,
        material_library_ids: list[str] | None = None,
        source_policy: str = "owned_only",
        template_performance: list[dict[str, Any]] | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        result = await db.execute(select(TrendSignal).order_by(TrendSignal.observed_at.desc()))
        signals = [_signal_from_model(row) for row in result.scalars().all()]
        performance = template_performance or []
        suggestions = [
            self._suggestion(signal, material_library_ids or [], source_policy, performance)
            for signal in signals
            if signal.get("keyword")
        ]
        return sorted(suggestions, key=lambda item: item["opportunity_score"], reverse=True)[:limit]

    def _suggestion(
        self,
        signal: dict[str, Any],
        material_library_ids: list[str],
        source_policy: str,
        template_performance: list[dict[str, Any]],
    ) -> dict[str, Any]:
        template = _recommend_template(signal["keyword"], template_performance)
        historical = _template_fit(template, template_performance)
        low_competition = 1.0 - min(max(signal["competition"], 0.0), 1.0)
        policy_rights_risk = signal["rights_risk"]
        opportunity = (
            0.30 * signal["trend_growth"]
            + 0.20 * signal["cross_platform_mentions"]
            + 0.20 * historical
            + 0.15 * signal["material_availability"]
            + 0.10 * low_competition
            - 0.20 * policy_rights_risk
            + 0.04 * signal["score"]
        )
        material_count = int(round(signal["material_availability"] * 20)) if material_library_ids else int(
            round(signal["material_availability"] * 8)
        )
        return {
            "keyword": signal["keyword"],
            "opportunity_score": round(max(0.0, min(1.0, opportunity)), 2),
            "recommended_template": template,
            "estimated_material_count": material_count,
            "rights_risk": policy_rights_risk,
            "reason": (
                f"{signal['source']} trend with {material_count} estimated matching clips "
                f"and template {template}"
            ),
        }


def _recommend_template(keyword: str, template_performance: list[dict[str, Any]]) -> str:
    normalized = keyword.lower()
    if any(token in normalized for token in ("cat", "kitten", "pet", "dog", "小猫", "宠物")):
        return "animal_compilation_short"
    if any(token in normalized for token in ("ai", "news", "today", "热点", "解释")):
        return "hot_topic_explainer_short"
    if template_performance:
        return str(max(template_performance, key=lambda item: item.get("avg_virality_score", 0)).get("template_id"))
    return "material_library_remix"


def _template_fit(template_id: str, template_performance: list[dict[str, Any]]) -> float:
    for item in template_performance:
        if item.get("template_id") == template_id:
            return _number(item.get("avg_virality_score"), default=0.5)
    return 0.5


def _number(value: object, default: float) -> float:
    try:
        return max(0.0, min(1.0, float(value)))
    except (TypeError, ValueError):
        return default


def _signal_from_payload(payload: dict[str, Any], *, signal_id: str) -> dict[str, Any]:
    return {
        "signal_id": signal_id,
        "source": str(payload.get("source") or "manual"),
        "keyword": str(payload.get("keyword") or "").strip(),
        "score": _number(payload.get("score"), default=0.5),
        "trend_growth": _number(payload.get("trend_growth"), default=_number(payload.get("score"), default=0.5)),
        "cross_platform_mentions": _number(payload.get("cross_platform_mentions"), default=0.5),
        "material_availability": _number(payload.get("material_availability"), default=0.5),
        "competition": _number(payload.get("competition"), default=0.5),
        "rights_risk": _number(payload.get("rights_risk"), default=0.2),
        "metadata": dict(payload.get("metadata") or {}),
    }


def _signal_from_model(row: TrendSignal) -> dict[str, Any]:
    metadata_json = dict(row.metadata_json or {})
    return {
        "signal_id": f"trend-{row.id}",
        "source": row.source,
        "keyword": row.keyword,
        "score": row.score,
        "trend_growth": _number(metadata_json.get("trend_growth"), default=row.score),
        "cross_platform_mentions": _number(metadata_json.get("cross_platform_mentions"), default=0.5),
        "material_availability": _number(metadata_json.get("material_availability"), default=0.5),
        "competition": _number(metadata_json.get("competition"), default=0.5),
        "rights_risk": _number(metadata_json.get("rights_risk"), default=0.2),
        "metadata": dict(metadata_json.get("metadata") or {}),
    }
