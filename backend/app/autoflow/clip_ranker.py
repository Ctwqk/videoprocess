from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowIntent


class ClipRanker:
    def __init__(self, historical_performance: dict[str, Any] | None = None) -> None:
        self.historical_performance = historical_performance or {}

    def rank(
        self,
        intent: AutoFlowIntent,
        candidates: Iterable[AutoFlowClipCandidate],
        historical_performance: dict[str, Any] | None = None,
    ) -> list[AutoFlowClipCandidate]:
        deduped = self._dedupe(list(candidates))
        performance = historical_performance if historical_performance is not None else self.historical_performance
        ranked = [self._score_candidate(intent, candidate, performance) for candidate in deduped]
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
    ) -> AutoFlowClipCandidate:
        visual = candidate.metadata.get("visual") if isinstance(candidate.metadata.get("visual"), dict) else {}
        topic_relevance = _topic_relevance(intent, candidate)
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

        breakdown = {
            "topic_relevance": topic_relevance,
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
        }
        score = (
            0.25 * topic_relevance
            + 0.15 * duration_fit
            + 0.15 * visual_motion_score
            + 0.10 * first_seconds_hook_score
            + 0.10 * aspect_ratio_fit
            + 0.10 * quality_score
            + 0.05 * source_reputation
            + 0.05 * novelty_score
            + 0.05 * historical_performance_fit
            - 0.20 * copyright_risk
            - 0.10 * duplicate_penalty
            - 0.10 * watermark_penalty
        )
        return candidate.model_copy(
            update={
                "score": round(_clamp_float(score), 4),
                "score_breakdown": {key: round(value, 4) for key, value in breakdown.items()},
            }
        )


def _topic_relevance(intent: AutoFlowIntent, candidate: AutoFlowClipCandidate) -> float:
    text = " ".join([candidate.title, str(candidate.metadata.get("description", ""))]).lower()
    keywords = [intent.subject, *intent.keywords]
    tokens = [_normalize_title(keyword) for keyword in keywords if keyword]
    if not tokens:
        return 0.5
    matches = sum(1 for token in tokens if token and token in text)
    return _clamp_float(matches / max(1, min(len(tokens), 4)))


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
