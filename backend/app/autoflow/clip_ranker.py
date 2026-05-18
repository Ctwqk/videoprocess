from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

from app.autoflow.platform_profiles import PlatformProfile
from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowIntent


class ClipRanker:
    def __init__(self, historical_performance: dict[str, Any] | None = None) -> None:
        self.historical_performance = historical_performance or {}

    def rank(
        self,
        intent: AutoFlowIntent,
        candidates: Iterable[AutoFlowClipCandidate],
        historical_performance: dict[str, Any] | None = None,
        *,
        semantic_relevance_scores: dict[str, float] | None = None,
        recent_used_asset_ids: set[str] | None = None,
        platform_profile: PlatformProfile | None = None,
    ) -> list[AutoFlowClipCandidate]:
        deduped = self._dedupe(list(candidates))
        performance = historical_performance if historical_performance is not None else self.historical_performance
        ranked = [
            self._score_candidate(
                intent,
                candidate,
                performance,
                semantic_relevance_scores=semantic_relevance_scores or {},
                recent_used_asset_ids=recent_used_asset_ids or set(),
                platform_profile=platform_profile,
            )
            for candidate in deduped
        ]
        return sorted(ranked, key=lambda candidate: candidate.score, reverse=True)

    def _dedupe(self, candidates: list[AutoFlowClipCandidate]) -> list[AutoFlowClipCandidate]:
        seen_urls: set[str] = set()
        seen_assets: set[str] = set()
        seen_titles: set[tuple[str, int]] = set()
        source_windows: dict[str, list[tuple[float, float]]] = {}
        result: list[AutoFlowClipCandidate] = []

        for candidate in candidates:
            if candidate.url:
                normalized_url = candidate.url.strip().lower()
                if normalized_url in seen_urls:
                    continue
                seen_urls.add(normalized_url)

            if candidate.asset_id:
                if candidate.asset_id in seen_assets:
                    continue
                seen_assets.add(candidate.asset_id)

            title_key = (_normalize_title(candidate.title), round(_duration(candidate)))
            if title_key[0] and title_key in seen_titles:
                continue
            seen_titles.add(title_key)

            source_video_id = candidate.metadata.get("source_video_id")
            if isinstance(source_video_id, str) and source_video_id:
                start = float(candidate.start_sec or 0.0)
                end = float(candidate.end_sec or start)
                windows = source_windows.setdefault(source_video_id, [])
                if any(_overlaps(start, end, other_start, other_end) for other_start, other_end in windows):
                    continue
                windows.append((start, end))

            result.append(candidate)

        return result

    def _score_candidate(
        self,
        intent: AutoFlowIntent,
        candidate: AutoFlowClipCandidate,
        historical_performance: dict[str, Any],
        *,
        semantic_relevance_scores: dict[str, float],
        recent_used_asset_ids: set[str],
        platform_profile: PlatformProfile | None,
    ) -> AutoFlowClipCandidate:
        visual = candidate.metadata.get("visual") if isinstance(candidate.metadata.get("visual"), dict) else {}
        topic_relevance = _topic_relevance(intent, candidate)
        semantic_relevance = _semantic_relevance(candidate, semantic_relevance_scores, topic_relevance)
        duration_fit = _duration_fit(intent, candidate)
        visual_motion_score = _clamp_float(visual.get("motion_score", candidate.metadata.get("motion_score", 0.45)))
        first_seconds_hook_score = _clamp_float(candidate.metadata.get("first_seconds_hook_score", 0.5))
        aspect_ratio_fit = 1.0 if candidate.metadata.get("aspect_ratio") in {None, intent.aspect_ratio, "auto"} else 0.45
        quality_score = _clamp_float(candidate.metadata.get("quality_score", 0.65))
        source_reputation = _source_reputation(candidate)
        novelty_score = _clamp_float(candidate.metadata.get("novelty_score", 0.7))
        copyright_risk = _copyright_risk(candidate)
        duplicate_penalty = _clamp_float(candidate.metadata.get("duplicate_penalty", 0.0))
        watermark_penalty = _clamp_float(visual.get("watermark_score", candidate.metadata.get("watermark_score", 0.0)))
        historical_performance_fit = _historical_performance_fit(intent, candidate, historical_performance)
        face_present = _face_present_score(visual, candidate.metadata)
        scene_change_diversity = _clamp_float(
            visual.get("scene_change_diversity", visual.get("scene_change_score", candidate.metadata.get("scene_change_score", 0.5)))
        )
        brightness_fit = _clamp_float(
            visual.get("brightness_fit", visual.get("brightness_score", candidate.metadata.get("brightness_score", 0.5)))
        )
        platform_fit = _platform_fit(candidate, intent, platform_profile, aspect_ratio_fit)
        recent_used_penalty = 1.0 if _candidate_key(candidate) in recent_used_asset_ids else 0.0
        intent_fit = max(topic_relevance, duration_fit * 0.5 + aspect_ratio_fit * 0.5)

        breakdown = {
            "topic_relevance": topic_relevance,
            "semantic_relevance": semantic_relevance,
            "duration_fit": duration_fit,
            "visual_motion_score": visual_motion_score,
            "first_seconds_hook_score": first_seconds_hook_score,
            "aspect_ratio_fit": aspect_ratio_fit,
            "quality_score": quality_score,
            "source_reputation": source_reputation,
            "novelty_score": novelty_score,
            "copyright_risk": copyright_risk,
            "duplicate_penalty": duplicate_penalty,
            "watermark_penalty": watermark_penalty,
            "historical_performance_fit": historical_performance_fit,
            "intent_fit": intent_fit,
            "face_present": face_present,
            "scene_change_diversity": scene_change_diversity,
            "brightness_fit": brightness_fit,
            "platform_fit": platform_fit,
            "recent_used_penalty": recent_used_penalty,
        }
        score = (
            0.30 * semantic_relevance
            + 0.10 * duration_fit
            + 0.20 * visual_motion_score
            + 0.08 * first_seconds_hook_score
            + 0.06 * aspect_ratio_fit
            + 0.08 * quality_score
            + 0.05 * source_reputation
            + 0.04 * novelty_score
            + 0.05 * historical_performance_fit
            + 0.10 * intent_fit
            + 0.06 * face_present
            + 0.05 * scene_change_diversity
            + 0.04 * brightness_fit
            + 0.03 * platform_fit
            - 0.20 * copyright_risk
            - 0.10 * duplicate_penalty
            - 0.10 * watermark_penalty
            - 0.15 * recent_used_penalty
        )
        return candidate.model_copy(
            update={
                "score": round(_clamp_float(score), 4),
                "score_breakdown": {key: round(value, 4) for key, value in breakdown.items()},
            }
        )


def _topic_relevance(intent: AutoFlowIntent, candidate: AutoFlowClipCandidate) -> float:
    visual = candidate.metadata.get("visual") if isinstance(candidate.metadata.get("visual"), dict) else {}
    text_parts = [
        candidate.title,
        str(candidate.metadata.get("description", "")),
        str(candidate.metadata.get("platform", candidate.metadata.get("source_platform", ""))),
        str(visual.get("dominant_action", candidate.metadata.get("dominant_action", ""))),
    ]
    for key in ("tags", "keywords", "object_labels"):
        value = candidate.metadata.get(key, visual.get(key))
        if isinstance(value, list):
            text_parts.extend(str(item) for item in value)
        elif value:
            text_parts.append(str(value))
    text = " ".join(text_parts).lower()
    keywords = [intent.subject, *intent.keywords]
    tokens = [_normalize_title(keyword) for keyword in keywords if keyword]
    if not tokens:
        return 0.5
    matches = sum(1 for token in tokens if token and token in text)
    return _clamp_float(matches / max(1, min(len(tokens), 4)))


def _semantic_relevance(
    candidate: AutoFlowClipCandidate,
    semantic_relevance_scores: dict[str, float],
    fallback: float,
) -> float:
    for key in (_candidate_key(candidate), candidate.id):
        if key in semantic_relevance_scores:
            return _clamp_float(semantic_relevance_scores[key])
    return fallback


def _candidate_key(candidate: AutoFlowClipCandidate) -> str:
    return candidate.asset_id or candidate.id


def _face_present_score(visual: dict[str, Any], metadata: dict[str, Any]) -> float:
    value = visual.get("face_present", metadata.get("face_present"))
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    return _clamp_float(visual.get("face_score", metadata.get("face_score", 0.5)))


def _platform_fit(
    candidate: AutoFlowClipCandidate,
    intent: AutoFlowIntent,
    platform_profile: PlatformProfile | None,
    aspect_ratio_fit: float,
) -> float:
    if platform_profile is None:
        return aspect_ratio_fit
    aspect_ratio = str(candidate.metadata.get("aspect_ratio") or intent.aspect_ratio or "")
    if aspect_ratio in platform_profile.preferred_aspect_ratios or aspect_ratio in {"", "auto"}:
        return 1.0
    return 0.5


def _duration_fit(intent: AutoFlowIntent, candidate: AutoFlowClipCandidate) -> float:
    duration = _duration(candidate)
    if duration <= 0:
        duration = float(candidate.metadata.get("duration") or candidate.metadata.get("duration_sec") or 0.0)
    if duration <= 0:
        return 0.5
    ideal_clip = max(2.0, min(8.0, intent.duration_sec / 6.0))
    return _clamp_float(1.0 - min(abs(duration - ideal_clip) / ideal_clip, 1.0))


def _duration(candidate: AutoFlowClipCandidate) -> float:
    if candidate.start_sec is not None and candidate.end_sec is not None:
        return max(0.0, float(candidate.end_sec) - float(candidate.start_sec))
    return float(candidate.metadata.get("duration") or candidate.metadata.get("duration_sec") or 0.0)


def _source_reputation(candidate: AutoFlowClipCandidate) -> float:
    if candidate.asset_id or candidate.source_type in {"asset", "material"}:
        return 0.9
    if candidate.source_type in {"youtube", "bilibili"}:
        return 0.55
    return 0.45


def _copyright_risk(candidate: AutoFlowClipCandidate) -> float:
    if candidate.rights_status == "allowed":
        return 0.05
    if candidate.rights_status == "review_required":
        return 0.45
    if candidate.url:
        return 0.55
    return 0.35


def _historical_performance_fit(
    intent: AutoFlowIntent,
    candidate: AutoFlowClipCandidate,
    historical_performance: dict[str, Any] | None,
) -> float:
    if not historical_performance:
        return 0.0

    template_id = str(candidate.metadata.get("template_id") or candidate.metadata.get("workflow_template_id") or "")
    intent_type = str(candidate.metadata.get("intent_type") or intent.intent_type or "")

    templates = (
        historical_performance.get("templates")
        if isinstance(historical_performance.get("templates"), dict)
        else {}
    )
    intent_types = (
        historical_performance.get("intent_types") if isinstance(historical_performance.get("intent_types"), dict) else {}
    )

    for key, source in (
        (template_id, templates),
        (template_id, historical_performance),
        (intent_type, intent_types),
        (intent_type, historical_performance),
    ):
        if not key or not isinstance(source, dict) or key not in source:
            continue
        score = _performance_value(source[key])
        if score is not None:
            return score
    return 0.0


def _performance_value(value: Any) -> float | None:
    if isinstance(value, dict):
        for key in ("score", "performance_score", "historical_performance_fit", "success_rate"):
            if key in value:
                return _clamp_float(value[key])
        return None
    if isinstance(value, (int, float, str)):
        return _clamp_float(value)
    return None


def _normalize_title(value: str) -> str:
    return " ".join(re.findall(r"[\w\u4e00-\u9fff]+", value.lower()))


def _overlaps(a_start: float, a_end: float, b_start: float, b_end: float) -> bool:
    return max(a_start, b_start) < min(a_end, b_end)


def _clamp_float(value: object) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, number))
