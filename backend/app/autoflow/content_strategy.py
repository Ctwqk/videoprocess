from __future__ import annotations

import uuid
from typing import Any

from app.autoflow.platform_profiles import PlatformProfileService


class ContentStrategyService:
    def generate_ideas(
        self,
        request: dict[str, Any],
        *,
        trend_suggestions: list[dict[str, Any]],
        template_performance: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        count = int(request.get("count") or 10)
        target_platforms = list(request.get("target_platforms") or [])
        source_policy = str(request.get("source_policy") or "owned_only")
        platform_profile = PlatformProfileService().for_platforms(target_platforms).to_dict()

        if trend_suggestions:
            ideas = [
                self._idea_from_trend(
                    suggestion,
                    target_platforms,
                    source_policy,
                    template_performance,
                    platform_profile,
                )
                for suggestion in trend_suggestions
            ]
        else:
            ideas = [
                self._fallback_idea(item, target_platforms, source_policy, platform_profile)
                for item in template_performance
            ]
            if not ideas:
                ideas = [
                    self._fallback_idea(
                        {"template_id": "material_library_remix"},
                        target_platforms,
                        source_policy,
                        platform_profile,
                    )
                ]

        return sorted(ideas, key=lambda item: item["opportunity_score"], reverse=True)[:count]

    def _idea_from_trend(
        self,
        suggestion: dict[str, Any],
        target_platforms: list[str],
        source_policy: str,
        template_performance: list[dict[str, Any]],
        platform_profile: dict[str, object],
    ) -> dict[str, Any]:
        template_id = str(suggestion.get("recommended_template") or "material_library_remix")
        performance_boost = 0.05 * _template_virality(template_id, template_performance)
        opportunity = min(1.0, float(suggestion.get("opportunity_score") or 0.0) + performance_boost)
        rights_risk = float(suggestion.get("rights_risk") or 0.0)
        return {
            "idea_id": f"idea-{uuid.uuid4()}",
            "prompt": _prompt_for_template(template_id, str(suggestion.get("keyword") or "新选题")),
            "template_id": template_id,
            "opportunity_score": round(opportunity, 2),
            "estimated_material_count": int(suggestion.get("estimated_material_count") or 0),
            "risk": _risk_label(source_policy, rights_risk),
            "target_platforms": target_platforms,
            "source_policy": source_policy,
            "platform_profile": platform_profile,
        }

    def _fallback_idea(
        self,
        template_performance: dict[str, Any],
        target_platforms: list[str],
        source_policy: str,
        platform_profile: dict[str, object],
    ) -> dict[str, Any]:
        template_id = str(template_performance.get("template_id") or "material_library_remix")
        virality = float(template_performance.get("avg_virality_score") or 0.45)
        return {
            "idea_id": f"idea-{uuid.uuid4()}",
            "prompt": _prompt_for_template(template_id, "素材库可用片段"),
            "template_id": template_id,
            "opportunity_score": round(0.45 + 0.25 * virality, 2),
            "estimated_material_count": max(3, int((template_performance.get("metric_count") or 1) * 6)),
            "risk": _risk_label(source_policy, 0.05),
            "target_platforms": target_platforms,
            "source_policy": source_policy,
            "platform_profile": platform_profile,
        }


def _prompt_for_template(template_id: str, keyword: str) -> str:
    if template_id == "animal_compilation_short":
        return f"做一个 30 秒 {keyword} 集锦，竖屏，先导出预览。"
    if template_id == "hot_topic_explainer_short":
        return f"做一个 45 秒热点解释短视频，解释 {keyword}。"
    return f"从素材库里找 {keyword}，做一个 20 秒混剪。"


def _template_virality(template_id: str, template_performance: list[dict[str, Any]]) -> float:
    for item in template_performance:
        if item.get("template_id") == template_id:
            return float(item.get("avg_virality_score") or 0.0)
    return 0.0


def _risk_label(source_policy: str, rights_risk: float) -> str:
    if source_policy == "owned_only" and rights_risk <= 0.2:
        return "low"
    if rights_risk >= 0.65:
        return "high"
    return "medium"
