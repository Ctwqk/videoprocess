from __future__ import annotations

import uuid
from collections import defaultdict
from typing import Any


class MetricsService:
    def __init__(self) -> None:
        self._metrics: list[dict[str, Any]] = []

    def save_manual_metrics(self, run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        views = _number(payload.get("views"), default=0)
        likes = _number(payload.get("likes"), default=0)
        comments = _number(payload.get("comments"), default=0)
        shares = _number(payload.get("shares"), default=0)
        avg_view_duration = _number(payload.get("avg_view_duration_sec"), default=0)
        video_duration = max(_number(payload.get("video_duration_sec"), default=0), 1)

        metric = {
            "metric_id": f"metric-{uuid.uuid4()}",
            "run_id": run_id,
            "template_id": str(payload.get("template_id") or "unknown"),
            "intent_type": str(payload.get("intent_type") or "generic_video"),
            "platform": str(payload.get("platform") or "manual"),
            "platform_content_id": str(payload.get("platform_content_id") or ""),
            "views": int(views),
            "likes": int(likes),
            "comments": int(comments),
            "shares": int(shares),
            "watch_time_sec": _number(payload.get("watch_time_sec"), default=0),
            "avg_view_duration_sec": avg_view_duration,
            "video_duration_sec": video_duration,
            "retention": list(payload.get("retention") or []),
        }
        metric.update(_derived_metrics(metric))
        self._metrics.append(metric)
        return metric

    def list_for_run(self, run_id: str) -> list[dict[str, Any]]:
        return [metric for metric in self._metrics if metric["run_id"] == run_id]

    def aggregate_by_template(self) -> list[dict[str, Any]]:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for metric in self._metrics:
            grouped[metric["template_id"]].append(metric)

        summaries = []
        for template_id, metrics in grouped.items():
            metric_count = len(metrics)
            total_views = sum(metric["views"] for metric in metrics)
            summaries.append(
                {
                    "template_id": template_id,
                    "metric_count": metric_count,
                    "total_views": total_views,
                    "avg_views": total_views / max(metric_count, 1),
                    "avg_like_rate": _average(metric["like_rate"] for metric in metrics),
                    "avg_comment_rate": _average(metric["comment_rate"] for metric in metrics),
                    "avg_share_rate": _average(metric["share_rate"] for metric in metrics),
                    "avg_retention": _average(metric["avg_retention"] for metric in metrics),
                    "avg_virality_score": _average(metric["virality_score"] for metric in metrics),
                    "intent_type": _first(metrics, "intent_type", "generic_video"),
                }
            )
        return sorted(summaries, key=lambda item: item["avg_virality_score"], reverse=True)


def _derived_metrics(metric: dict[str, Any]) -> dict[str, float]:
    views = max(float(metric["views"]), 1.0)
    like_rate = metric["likes"] / views
    comment_rate = metric["comments"] / views
    share_rate = metric["shares"] / views
    avg_retention = min(float(metric["avg_view_duration_sec"]) / max(float(metric["video_duration_sec"]), 1.0), 1.0)
    virality_score = min(
        1.0,
        0.35 * min(like_rate / 0.12, 1.0)
        + 0.20 * min(comment_rate / 0.03, 1.0)
        + 0.20 * min(share_rate / 0.04, 1.0)
        + 0.25 * avg_retention,
    )
    return {
        "like_rate": like_rate,
        "comment_rate": comment_rate,
        "share_rate": share_rate,
        "avg_retention": avg_retention,
        "virality_score": virality_score,
    }


def _average(values) -> float:
    items = list(values)
    return sum(items) / max(len(items), 1)


def _first(metrics: list[dict[str, Any]], key: str, default: str) -> str:
    for metric in metrics:
        value = metric.get(key)
        if value:
            return str(value)
    return default


def _number(value: object, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
