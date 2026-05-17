from __future__ import annotations

import uuid
from collections import defaultdict
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.autoflow import AutoFlowPlan as AutoFlowPlanModel
from app.models.autoflow import AutoFlowRun as AutoFlowRunModel
from app.models.autoflow import ContentMetric


class MetricsService:
    def __init__(self) -> None:
        self._metrics: list[dict[str, Any]] = []

    def save_manual_metrics(self, run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        metric = _metric_from_payload(run_id, payload)
        self._metrics.append(metric)
        return metric

    async def save_manual_metrics_db(
        self,
        run_id: str,
        payload: dict[str, Any],
        db: AsyncSession,
    ) -> dict[str, Any]:
        run_uuid = uuid.UUID(str(run_id))
        run = await db.get(AutoFlowRunModel, run_uuid)
        if run is None:
            raise ValueError("AutoFlow run not found")

        plan = await db.get(AutoFlowPlanModel, run.plan_id)
        enriched = dict(payload)
        if plan is not None:
            enriched.setdefault("template_id", plan.template_id)
            enriched.setdefault("intent_type", str((plan.intent_json or {}).get("intent_type") or "generic_video"))

        metric = _metric_from_payload(str(run.id), enriched)
        row = ContentMetric(
            run_id=run.id,
            platform=metric["platform"],
            platform_content_id=metric["platform_content_id"],
            views=metric["views"],
            likes=metric["likes"],
            comments=metric["comments"],
            shares=metric["shares"],
            watch_time_sec=metric["watch_time_sec"],
            avg_view_duration_sec=metric["avg_view_duration_sec"],
            retention_json={
                "retention": metric["retention"],
                "video_duration_sec": metric["video_duration_sec"],
                "derived": {
                    "like_rate": metric["like_rate"],
                    "comment_rate": metric["comment_rate"],
                    "share_rate": metric["share_rate"],
                    "avg_retention": metric["avg_retention"],
                    "virality_score": metric["virality_score"],
                },
                "template_id": metric["template_id"],
                "intent_type": metric["intent_type"],
            },
        )
        db.add(row)
        await db.commit()
        await db.refresh(row)
        return await self._metric_from_db_row(row, db)

    def list_for_run(self, run_id: str) -> list[dict[str, Any]]:
        return [metric for metric in self._metrics if metric["run_id"] == run_id]

    async def list_for_run_db(self, run_id: str, db: AsyncSession) -> list[dict[str, Any]]:
        run_uuid = uuid.UUID(str(run_id))
        stmt = select(ContentMetric).where(ContentMetric.run_id == run_uuid).order_by(ContentMetric.collected_at.asc())
        result = await db.execute(stmt)
        return [await self._metric_from_db_row(row, db) for row in result.scalars().all()]

    def aggregate_by_template(self) -> list[dict[str, Any]]:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for metric in self._metrics:
            grouped[metric["template_id"]].append(metric)

        return _aggregate_groups(grouped)

    async def aggregate_by_template_db(self, db: AsyncSession) -> list[dict[str, Any]]:
        stmt = (
            select(ContentMetric, AutoFlowPlanModel)
            .join(AutoFlowRunModel, ContentMetric.run_id == AutoFlowRunModel.id)
            .join(AutoFlowPlanModel, AutoFlowRunModel.plan_id == AutoFlowPlanModel.id)
        )
        result = await db.execute(stmt)
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for metric_row, plan_row in result.all():
            metric = _metric_from_db_models(metric_row, plan_row)
            grouped[metric["template_id"]].append(metric)
        return _aggregate_groups(grouped)

    async def _metric_from_db_row(self, row: ContentMetric, db: AsyncSession) -> dict[str, Any]:
        run = await db.get(AutoFlowRunModel, row.run_id)
        plan = await db.get(AutoFlowPlanModel, run.plan_id) if run is not None else None
        return _metric_from_db_models(row, plan)


def _metric_from_payload(run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
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
    return metric


def _metric_from_db_models(metric: ContentMetric, plan: AutoFlowPlanModel | None) -> dict[str, Any]:
    retention_json = dict(metric.retention_json or {})
    video_duration = max(_number(retention_json.get("video_duration_sec"), default=1), 1)
    base = {
        "metric_id": f"metric-{metric.id}",
        "run_id": str(metric.run_id),
        "template_id": plan.template_id if plan is not None else str(retention_json.get("template_id") or "unknown"),
        "intent_type": (
            str((plan.intent_json or {}).get("intent_type") or "generic_video")
            if plan is not None
            else str(retention_json.get("intent_type") or "generic_video")
        ),
        "platform": metric.platform,
        "platform_content_id": metric.platform_content_id,
        "views": metric.views,
        "likes": metric.likes,
        "comments": metric.comments,
        "shares": metric.shares,
        "watch_time_sec": metric.watch_time_sec,
        "avg_view_duration_sec": metric.avg_view_duration_sec,
        "video_duration_sec": video_duration,
        "retention": list(retention_json.get("retention") or []),
    }
    derived = retention_json.get("derived")
    if isinstance(derived, dict):
        base.update(
            {
                "like_rate": _number(derived.get("like_rate"), default=0),
                "comment_rate": _number(derived.get("comment_rate"), default=0),
                "share_rate": _number(derived.get("share_rate"), default=0),
                "avg_retention": _number(derived.get("avg_retention"), default=0),
                "virality_score": _number(derived.get("virality_score"), default=0),
            }
        )
    else:
        base.update(_derived_metrics(base))
    return base


def _aggregate_groups(grouped: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
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
