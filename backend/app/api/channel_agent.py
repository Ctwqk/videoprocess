from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.channel_agent.clock import Clock
from app.channel_agent.constants import (
    ACTIVE_TASK_STATES,
    QUEUE_CANCELLED,
    QUEUE_QUEUED,
    TASK_HELD,
    TASK_REJECTED,
    TASK_SEEDED,
    TASK_UPLOADED_PRIVATE,
)
from app.channel_agent.queue import ChannelOpsQueueService, utc_hour_bucket
from app.db import get_db
from app.models.channel_agent import (
    AgentTickAudit,
    ChannelOpsQueueItem,
    ChannelProfile,
    DecisionAuditEntry,
    FeedbackSnapshot,
    LaneFormatMatrix,
    LearningState,
    ManualSeed,
    MaterialUsageLedger,
    ProductionTask,
    PublicationRecord,
    PublishingAccount,
    TopicLane,
)
from app.schemas.channel_agent import (
    ChannelProfileCreate,
    HealthSummary,
    LaneFormatCreate,
    ManualSeedCreate,
    PublishingAccountCreate,
    QueueItemRead,
    TopicLaneCreate,
)


router = APIRouter(prefix="/api/v1/channel-agent", tags=["channel-agent"])


class DryRunPatch(BaseModel):
    dry_run: bool


class HaltRequest(BaseModel):
    reason: str


class PauseRequest(BaseModel):
    reason: str = "operator"
    until: datetime | None = None


@router.post("/channels")
async def create_channel(data: ChannelProfileCreate, db: AsyncSession = Depends(get_db)):
    row = ChannelProfile(**data.model_dump())
    db.add(row)
    await db.commit()
    await db.refresh(row)
    return _channel(row)


@router.get("/channels")
async def list_channels(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(ChannelProfile).order_by(ChannelProfile.created_at.desc()))
    return [_channel(row) for row in result.scalars().all()]


@router.get("/channels/{channel_id}")
async def get_channel(channel_id: str, db: AsyncSession = Depends(get_db)):
    channel = await db.get(ChannelProfile, _uuid(channel_id))
    if channel is None:
        raise HTTPException(status_code=404, detail="Channel not found")
    return _channel(channel)


@router.patch("/channels/{channel_id}")
async def patch_channel(channel_id: str, data: dict[str, Any], db: AsyncSession = Depends(get_db)):
    channel = await db.get(ChannelProfile, _uuid(channel_id))
    if channel is None:
        raise HTTPException(status_code=404, detail="Channel not found")
    for field in (
        "name",
        "positioning",
        "language",
        "default_aspect_ratio",
        "risk_policy_json",
        "content_mix_policy_json",
        "cadence_policy_json",
        "alert_policy_json",
        "enabled",
    ):
        if field in data:
            setattr(channel, field, data[field])
    channel.config_version += 1
    await db.commit()
    await db.refresh(channel)
    return _channel(channel)


@router.post("/channels/{channel_id}/lanes")
async def create_lane(channel_id: str, data: TopicLaneCreate, db: AsyncSession = Depends(get_db)):
    await _require_channel(db, channel_id)
    row = TopicLane(channel_profile_id=_uuid(channel_id), **data.model_dump())
    db.add(row)
    await db.commit()
    await db.refresh(row)
    return _lane(row)


@router.patch("/channels/{channel_id}/lanes/{lane_id}")
async def patch_lane(channel_id: str, lane_id: str, data: dict[str, Any], db: AsyncSession = Depends(get_db)):
    channel = await _require_channel(db, channel_id)
    lane = await db.get(TopicLane, _uuid(lane_id))
    if lane is None or lane.channel_profile_id != channel.id:
        raise HTTPException(status_code=404, detail="Lane not found")
    for field in (
        "name",
        "description",
        "weight",
        "keywords_json",
        "negative_keywords_json",
        "min_posts_per_week",
        "max_posts_per_day",
        "max_consecutive_streak",
        "cooldown_after_post_minutes",
        "enabled",
        "paused_until",
    ):
        if field in data:
            setattr(lane, field, data[field])
    await db.commit()
    await db.refresh(lane)
    return _lane(lane)


@router.post("/channels/{channel_id}/accounts")
async def create_account(channel_id: str, data: PublishingAccountCreate, db: AsyncSession = Depends(get_db)):
    await _require_channel(db, channel_id)
    row = PublishingAccount(channel_profile_id=_uuid(channel_id), **data.model_dump())
    db.add(row)
    await db.commit()
    await db.refresh(row)
    return _account(row)


@router.patch("/channels/{channel_id}/accounts/{account_id}")
async def patch_account(channel_id: str, account_id: str, data: dict[str, Any], db: AsyncSession = Depends(get_db)):
    channel = await _require_channel(db, channel_id)
    account = await db.get(PublishingAccount, _uuid(account_id))
    if account is None or account.channel_profile_id != channel.id:
        raise HTTPException(status_code=404, detail="Account not found")
    for field in (
        "account_label",
        "platform_account_id",
        "credential_ref",
        "platform_specific_config_json",
        "default_privacy",
        "external_asset_auto_publish",
        "enabled",
        "paused_until",
    ):
        if field in data:
            setattr(account, field, data[field])
    await db.commit()
    await db.refresh(account)
    return _account(account)


@router.post("/lanes/{lane_id}/formats")
async def create_lane_format(lane_id: str, data: LaneFormatCreate, db: AsyncSession = Depends(get_db)):
    lane = await db.get(TopicLane, _uuid(lane_id))
    if lane is None:
        raise HTTPException(status_code=404, detail="Lane not found")
    row = LaneFormatMatrix(topic_lane_id=lane.id, **data.model_dump())
    db.add(row)
    await db.commit()
    await db.refresh(row)
    return _lane_format(row)


@router.patch("/lane-formats/{lane_format_id}")
async def patch_lane_format(lane_format_id: str, data: dict[str, Any], db: AsyncSession = Depends(get_db)):
    raise HTTPException(
        status_code=410,
        detail="Use /channels/{channel_id}/lanes/{lane_id}/formats/{lane_format_id}",
    )


@router.patch("/channels/{channel_id}/lanes/{lane_id}/formats/{lane_format_id}")
async def patch_channel_lane_format(
    channel_id: str,
    lane_id: str,
    lane_format_id: str,
    data: dict[str, Any],
    db: AsyncSession = Depends(get_db),
):
    row = await _lane_format_for_channel(db, channel_id, lane_id, lane_format_id)
    _apply_lane_format_patch(row, data)
    await db.commit()
    await db.refresh(row)
    return _lane_format(row)


def _apply_lane_format_patch(row: LaneFormatMatrix, data: dict[str, Any]) -> None:
    for field in (
        "format_key",
        "enabled",
        "weight",
        "target_duration_sec",
        "template_pool_json",
        "source_platforms_json",
        "default_publish_visibility",
    ):
        if field in data:
            setattr(row, field, data[field])


async def _lane_format_for_channel(
    db: AsyncSession,
    channel_id: str,
    lane_id: str,
    lane_format_id: str,
) -> LaneFormatMatrix:
    row = await db.get(LaneFormatMatrix, _uuid(lane_format_id))
    if row is None:
        raise HTTPException(status_code=404, detail="Lane format not found")
    lane = await db.get(TopicLane, _uuid(lane_id))
    if lane is None or lane.channel_profile_id != _uuid(channel_id) or row.topic_lane_id != lane.id:
        raise HTTPException(status_code=404, detail="Lane format not found")
    return row


@router.post("/channels/{channel_id}/manual-seeds")
async def create_manual_seed(channel_id: str, data: ManualSeedCreate, db: AsyncSession = Depends(get_db)):
    channel = await _require_channel(db, channel_id)
    await _validate_manual_seed_references(db, channel.id, data)
    row = ManualSeed(channel_profile_id=_uuid(channel_id), **_seed_payload(data))
    db.add(row)
    await db.commit()
    await db.refresh(row)
    return _seed(row)


@router.post("/channels/{channel_id}/enqueue-tick", response_model=QueueItemRead)
async def enqueue_tick(channel_id: str, db: AsyncSession = Depends(get_db)):
    await _require_channel(db, channel_id)
    now = Clock().now()
    item = await ChannelOpsQueueService().enqueue(
        db,
        kind="agent_tick",
        idempotency_key=f"agent_tick:{channel_id}:{utc_hour_bucket(now)}",
        payload={"channel_id": channel_id},
        priority=20,
        channel_profile_id=_uuid(channel_id),
    )
    return _queue(item)


@router.patch("/channels/{channel_id}/dry-run")
async def patch_dry_run(channel_id: str, data: DryRunPatch, db: AsyncSession = Depends(get_db)):
    channel = await _require_channel(db, channel_id)
    channel.dry_run = data.dry_run
    channel.config_version += 1
    await db.commit()
    await db.refresh(channel)
    return _channel(channel)


@router.post("/channels/{channel_id}/halt")
async def halt_channel(channel_id: str, data: HaltRequest, db: AsyncSession = Depends(get_db)):
    channel = await _require_channel(db, channel_id)
    channel.halted_at = datetime.now(timezone.utc)
    channel.halt_reason = data.reason
    await db.commit()
    await db.refresh(channel)
    return _channel(channel)


@router.post("/channels/{channel_id}/resume")
async def resume_channel(channel_id: str, db: AsyncSession = Depends(get_db)):
    channel = await _require_channel(db, channel_id)
    channel.halted_at = None
    channel.halt_reason = None
    await db.commit()
    await db.refresh(channel)
    return _channel(channel)


@router.get("/channels/{channel_id}/health", response_model=HealthSummary)
async def channel_health(channel_id: str, db: AsyncSession = Depends(get_db)):
    channel = await _require_channel(db, channel_id)
    queued = (
        await db.execute(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.channel_profile_id == channel.id))
    ).scalars().all()
    tasks = (
        await db.execute(select(ProductionTask).where(ProductionTask.channel_profile_id == channel.id))
    ).scalars().all()
    last_successful_measured_at = await db.scalar(
        select(func.max(FeedbackSnapshot.collected_at))
        .join(PublicationRecord, FeedbackSnapshot.publication_id == PublicationRecord.id)
        .join(ProductionTask, PublicationRecord.production_task_id == ProductionTask.id)
        .where(ProductionTask.channel_profile_id == channel.id)
    )
    return HealthSummary(
        channel_id=str(channel.id),
        dry_run=channel.dry_run,
        halted=channel.halted_at is not None,
        active_tasks=sum(1 for task in tasks if task.state in ACTIVE_TASK_STATES),
        queued_items=sum(1 for item in queued if item.status == "queued"),
        recent_failures=sum(1 for item in queued if item.status in {"failed", "dead_lettered"}),
        last_successful_measured_at=_as_utc(last_successful_measured_at),
        warnings=[],
    )


@router.get("/channels/{channel_id}/queue")
async def channel_queue(channel_id: str, db: AsyncSession = Depends(get_db)):
    channel = await _require_channel(db, channel_id)
    result = await db.execute(
        select(ChannelOpsQueueItem)
        .where(ChannelOpsQueueItem.channel_profile_id == channel.id)
        .order_by(ChannelOpsQueueItem.created_at.desc())
    )
    return [_queue(row).model_dump(mode="json") for row in result.scalars().all()]


@router.get("/channels/{channel_id}/ticks")
async def channel_ticks(channel_id: str, db: AsyncSession = Depends(get_db)):
    channel = await _require_channel(db, channel_id)
    result = await db.execute(
        select(AgentTickAudit)
        .where(AgentTickAudit.channel_profile_id == channel.id)
        .order_by(AgentTickAudit.started_at.desc())
    )
    return [_tick(row) for row in result.scalars().all()]


@router.get("/channels/{channel_id}/tasks")
async def channel_tasks(channel_id: str, db: AsyncSession = Depends(get_db)):
    await _require_channel(db, channel_id)
    result = await db.execute(select(ProductionTask).where(ProductionTask.channel_profile_id == _uuid(channel_id)))
    return [_task(row) for row in result.scalars().all()]


@router.get("/tasks/{task_id}")
async def get_task(task_id: str, db: AsyncSession = Depends(get_db)):
    task = await db.get(ProductionTask, _uuid(task_id))
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return _task(task)


@router.get("/channels/{channel_id}/decisions")
async def channel_decisions(
    channel_id: str,
    tick_audit_id: str | None = None,
    candidate_source: str | None = None,
    selected: bool | None = None,
    limit: int = 100,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
):
    channel = await _require_channel(db, channel_id)
    query = select(DecisionAuditEntry).where(DecisionAuditEntry.channel_profile_id == channel.id)
    if tick_audit_id:
        query = query.where(DecisionAuditEntry.tick_audit_id == _uuid(tick_audit_id))
    if candidate_source:
        query = query.where(DecisionAuditEntry.candidate_source == candidate_source)
    if selected is not None:
        query = query.where(DecisionAuditEntry.selected.is_(selected))
    rows = (
        await db.execute(
            query.order_by(DecisionAuditEntry.created_at.desc())
            .offset(max(offset, 0))
            .limit(min(max(limit, 1), 500))
        )
    ).scalars().all()
    return [_decision(row) for row in rows]


@router.get("/tasks/{task_id}/audit")
async def task_audit(task_id: str, db: AsyncSession = Depends(get_db)):
    task = await db.get(ProductionTask, _uuid(task_id))
    if task is None:
        raise HTTPException(status_code=404, detail="Production task not found")
    decision = (
        await db.execute(
            select(DecisionAuditEntry)
            .where(DecisionAuditEntry.created_task_id == task.id)
            .where(DecisionAuditEntry.channel_profile_id == task.channel_profile_id)
            .limit(1)
        )
    ).scalars().first()
    publication = (
        await db.execute(select(PublicationRecord).where(PublicationRecord.production_task_id == task.id).limit(1))
    ).scalars().first()
    material_rows = []
    if publication is not None:
        material_rows = (
            await db.execute(
                select(MaterialUsageLedger)
                .where(MaterialUsageLedger.publication_id == publication.id)
                .where(MaterialUsageLedger.channel_profile_id == task.channel_profile_id)
            )
        ).scalars().all()
    return {
        "task": _task(task),
        "decision": _decision(decision) if decision else None,
        "publication": _publication(publication) if publication else None,
        "material_usage": [_material_usage(row) for row in material_rows],
    }


@router.get("/channels/{channel_id}/failures")
async def channel_failures(channel_id: str, days: int = 7, db: AsyncSession = Depends(get_db)):
    channel = await _require_channel(db, channel_id)
    clamped_days = max(days, 0)
    since = _naive_utc(Clock().now() - timedelta(days=clamped_days))
    rows = (
        await db.execute(
            select(ProductionTask.failure_category, func.count(ProductionTask.id))
            .where(ProductionTask.channel_profile_id == channel.id)
            .where(ProductionTask.created_at >= since)
            .where(ProductionTask.failure_category.is_not(None))
            .group_by(ProductionTask.failure_category)
        )
    ).all()
    return {"days": clamped_days, "categories": {str(category): int(count) for category, count in rows}}


@router.get("/channels/{channel_id}/learning")
async def channel_learning(channel_id: str, db: AsyncSession = Depends(get_db)):
    channel = await _require_channel(db, channel_id)
    rows = (
        await db.execute(
            select(LearningState)
            .where(LearningState.channel_profile_id == channel.id)
            .order_by(
                LearningState.dimension_type.asc(),
                LearningState.dimension_key.asc(),
                LearningState.window_days.asc(),
            )
        )
    ).scalars().all()
    return {"channel_id": str(channel.id), "states": [_learning_state(row) for row in rows]}


@router.get("/channels/{channel_id}/publications")
async def channel_publications(channel_id: str, db: AsyncSession = Depends(get_db)):
    channel = await _require_channel(db, channel_id)
    result = await db.execute(
        select(PublicationRecord)
        .join(ProductionTask, PublicationRecord.production_task_id == ProductionTask.id)
        .where(ProductionTask.channel_profile_id == channel.id)
        .order_by(PublicationRecord.created_at.desc())
    )
    return [_publication(row) for row in result.scalars().all()]


@router.get("/publications/{publication_id}")
async def get_publication(publication_id: str, db: AsyncSession = Depends(get_db)):
    row = await db.get(PublicationRecord, _uuid(publication_id))
    if row is None:
        raise HTTPException(status_code=404, detail="Publication not found")
    return _publication(row)


@router.post("/publications/{publication_id}/enqueue-metrics")
async def enqueue_metrics(publication_id: str, db: AsyncSession = Depends(get_db)):
    publication = await db.get(PublicationRecord, _uuid(publication_id))
    if publication is None:
        raise HTTPException(status_code=404, detail="Publication not found")
    task = await db.get(ProductionTask, publication.production_task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Production task not found")
    bucket = utc_hour_bucket(Clock().now())
    item = await ChannelOpsQueueService().enqueue(
        db,
        kind="collect_metrics",
        idempotency_key=f"collect_metrics:{publication_id}:{bucket}",
        payload={"publication_id": publication_id},
        priority=90,
        channel_profile_id=task.channel_profile_id,
    )
    return _queue(item)


@router.post("/accounts/{account_id}/pause")
async def pause_account(account_id: str, data: PauseRequest, db: AsyncSession = Depends(get_db)):
    account = await db.get(PublishingAccount, _uuid(account_id))
    if account is None:
        raise HTTPException(status_code=404, detail="Account not found")
    account.enabled = False
    account.paused_until = data.until
    await db.commit()
    await db.refresh(account)
    return _account(account)


@router.post("/accounts/{account_id}/resume")
async def resume_account(account_id: str, db: AsyncSession = Depends(get_db)):
    account = await db.get(PublishingAccount, _uuid(account_id))
    if account is None:
        raise HTTPException(status_code=404, detail="Account not found")
    account.enabled = True
    account.paused_until = None
    await db.commit()
    await db.refresh(account)
    return _account(account)


@router.post("/lanes/{lane_id}/pause")
async def pause_lane(lane_id: str, data: PauseRequest, db: AsyncSession = Depends(get_db)):
    lane = await db.get(TopicLane, _uuid(lane_id))
    if lane is None:
        raise HTTPException(status_code=404, detail="Lane not found")
    lane.enabled = False
    lane.paused_until = data.until
    await db.commit()
    await db.refresh(lane)
    return _lane(lane)


@router.post("/lanes/{lane_id}/resume")
async def resume_lane(lane_id: str, db: AsyncSession = Depends(get_db)):
    lane = await db.get(TopicLane, _uuid(lane_id))
    if lane is None:
        raise HTTPException(status_code=404, detail="Lane not found")
    lane.enabled = True
    lane.paused_until = None
    await db.commit()
    await db.refresh(lane)
    return _lane(lane)


@router.post("/publications/{publication_id}/promote")
async def promote_publication(publication_id: str, db: AsyncSession = Depends(get_db)):
    publication, task = await _publication_with_task(db, publication_id)
    if publication.publish_status != "uploaded" or task.state not in {TASK_UPLOADED_PRIVATE, TASK_HELD}:
        raise HTTPException(status_code=409, detail="Publication is not ready for promotion")
    target_visibility = _safe_promotion_visibility(publication.desired_privacy)
    item = await ChannelOpsQueueService().enqueue(
        db,
        kind="promote_publication",
        idempotency_key=f"promote_publication:{publication.id}:{target_visibility}:manual",
        payload={
            "publication_id": str(publication.id),
            "target_visibility": target_visibility,
            "channel_profile_id": str(task.channel_profile_id),
        },
        priority=70,
        channel_profile_id=task.channel_profile_id,
    )
    return _queue(item)


@router.post("/publications/{publication_id}/reject")
async def reject_publication(publication_id: str, db: AsyncSession = Depends(get_db)):
    publication, task = await _publication_with_task(db, publication_id)
    if publication.publish_status not in {"uploaded", "held", "rejected"}:
        raise HTTPException(status_code=409, detail="Publication cannot be safely rejected from its current state")
    now = datetime.now(timezone.utc)
    publication.publish_status = "rejected"
    task.state = TASK_REJECTED
    task.state_updated_at = now
    await _cancel_queued_publication_items(db, publication.id, kinds={"promote_publication", "collect_metrics"})
    await db.commit()
    await db.refresh(publication)
    return _publication(publication)


@router.get("/channels/{channel_id}/metrics/funnel")
async def funnel(channel_id: str, days: int = 7, db: AsyncSession = Depends(get_db)):
    channel = await _require_channel(db, channel_id)
    clamped_days = max(days, 0)
    since = Clock().now() - timedelta(days=clamped_days)
    since_naive = _naive_utc(since)
    states = {
        "days": clamped_days,
        "seeded": 0,
        "selected": 0,
        "planning": 0,
        "producing": 0,
        "uploaded_private": 0,
        "scheduled": 0,
        "published": 0,
        "measured": 0,
        "failed": 0,
        "held": 0,
        "rejected": 0,
        "cancelled": 0,
        "other": 0,
        "repetition_rejected": 0,
        "cross_account_rejected": 0,
    }
    result = await db.execute(
        select(ProductionTask.state, func.count(ProductionTask.id))
        .where(ProductionTask.channel_profile_id == channel.id)
        .where(ProductionTask.created_at >= since_naive)
        .group_by(ProductionTask.state)
    )
    for state, count in result.all():
        if state == TASK_SEEDED:
            continue
        if state in states:
            states[state] = int(count)
        else:
            states["other"] += int(count)
    task_seed_count = await db.scalar(
        select(func.count(ProductionTask.id))
        .where(ProductionTask.channel_profile_id == channel.id)
        .where(ProductionTask.state == TASK_SEEDED)
        .where(ProductionTask.created_at >= since_naive)
    )
    seed_count = await db.scalar(
        select(func.count(ManualSeed.id))
        .where(ManualSeed.channel_profile_id == channel.id)
        .where(ManualSeed.status == "active")
        .where(ManualSeed.created_at >= since_naive)
    )
    states["seeded"] = int(task_seed_count or 0) + int(seed_count or 0)
    tick_rows = (
        await db.execute(
            select(AgentTickAudit.guards_triggered_json)
            .where(AgentTickAudit.channel_profile_id == channel.id)
            .where(AgentTickAudit.started_at >= since)
        )
    ).all()
    for (guards,) in tick_rows:
        for guard in guards or []:
            guard_name = guard.get("guard") if isinstance(guard, dict) else guard
            if guard_name in {"repetition_rejected", "cross_account_rejected"}:
                states[guard_name] += 1
    return states


@router.get("/channels/{channel_id}/lanes/health")
async def lanes_health(channel_id: str, db: AsyncSession = Depends(get_db)):
    await _require_channel(db, channel_id)
    rows = (
        await db.execute(select(TopicLane).where(TopicLane.channel_profile_id == _uuid(channel_id)))
    ).scalars().all()
    return [{"lane_id": str(row.id), "name": row.name, "enabled": row.enabled, "paused_until": row.paused_until} for row in rows]


@router.get("/channels/{channel_id}/accounts/health")
async def accounts_health(channel_id: str, db: AsyncSession = Depends(get_db)):
    await _require_channel(db, channel_id)
    rows = (
        await db.execute(select(PublishingAccount).where(PublishingAccount.channel_profile_id == _uuid(channel_id)))
    ).scalars().all()
    return [_account(row) for row in rows]


async def _require_channel(db: AsyncSession, channel_id: str) -> ChannelProfile:
    channel = await db.get(ChannelProfile, _uuid(channel_id))
    if channel is None:
        raise HTTPException(status_code=404, detail="Channel not found")
    return channel


async def _publication_with_task(db: AsyncSession, publication_id: str) -> tuple[PublicationRecord, ProductionTask]:
    publication = await db.get(PublicationRecord, _uuid(publication_id))
    if publication is None:
        raise HTTPException(status_code=404, detail="Publication not found")
    task = await db.get(ProductionTask, publication.production_task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Production task not found")
    return publication, task


async def _validate_manual_seed_references(
    db: AsyncSession,
    channel_id: uuid.UUID,
    data: ManualSeedCreate,
) -> None:
    if data.topic_lane_id:
        lane = await db.get(TopicLane, _uuid(data.topic_lane_id))
        if lane is None or lane.channel_profile_id != channel_id:
            raise HTTPException(status_code=400, detail="Manual seed topic lane does not belong to channel")
    if data.target_account_id:
        account = await db.get(PublishingAccount, _uuid(data.target_account_id))
        if account is None or account.channel_profile_id != channel_id:
            raise HTTPException(status_code=400, detail="Manual seed target account does not belong to channel")


async def _cancel_queued_publication_items(
    db: AsyncSession,
    publication_id: uuid.UUID,
    *,
    kinds: set[str],
) -> None:
    result = await db.execute(
        select(ChannelOpsQueueItem)
        .where(ChannelOpsQueueItem.kind.in_(sorted(kinds)))
        .where(ChannelOpsQueueItem.status == QUEUE_QUEUED)
    )
    for item in result.scalars().all():
        if str((item.payload_json or {}).get("publication_id") or "") == str(publication_id):
            item.status = QUEUE_CANCELLED


def _safe_promotion_visibility(value: Any) -> str:
    visibility = str(value or "").strip().lower()
    if visibility in {"private", "unlisted"}:
        return visibility
    return "unlisted"


def _uuid(value: str) -> uuid.UUID:
    return uuid.UUID(str(value))


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _seed_payload(data: ManualSeedCreate) -> dict[str, Any]:
    payload = data.model_dump()
    for key in ("topic_lane_id", "target_account_id"):
        if payload.get(key):
            payload[key] = _uuid(payload[key])
    return payload


def _channel(row: ChannelProfile) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "name": row.name,
        "positioning": row.positioning,
        "language": row.language,
        "default_aspect_ratio": row.default_aspect_ratio,
        "risk_policy_json": row.risk_policy_json,
        "content_mix_policy_json": row.content_mix_policy_json,
        "cadence_policy_json": row.cadence_policy_json,
        "alert_policy_json": row.alert_policy_json,
        "enabled": row.enabled,
        "dry_run": row.dry_run,
        "halted_at": row.halted_at,
        "halt_reason": row.halt_reason,
        "config_version": row.config_version,
    }


def _lane(row: TopicLane) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "channel_profile_id": str(row.channel_profile_id),
        "name": row.name,
        "description": row.description,
        "weight": row.weight,
        "keywords_json": row.keywords_json,
        "negative_keywords_json": row.negative_keywords_json,
        "min_posts_per_week": row.min_posts_per_week,
        "max_posts_per_day": row.max_posts_per_day,
        "max_consecutive_streak": row.max_consecutive_streak,
        "cooldown_after_post_minutes": row.cooldown_after_post_minutes,
        "enabled": row.enabled,
        "paused_until": row.paused_until,
    }


def _account(row: PublishingAccount) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "channel_profile_id": str(row.channel_profile_id),
        "platform": row.platform,
        "account_label": row.account_label,
        "platform_account_id": row.platform_account_id,
        "credential_ref": row.credential_ref,
        "platform_specific_config_json": row.platform_specific_config_json,
        "default_privacy": row.default_privacy,
        "external_asset_auto_publish": row.external_asset_auto_publish,
        "enabled": row.enabled,
        "paused_until": row.paused_until,
        "last_token_check_status": row.last_token_check_status,
    }


def _lane_format(row: LaneFormatMatrix) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "topic_lane_id": str(row.topic_lane_id),
        "format_key": row.format_key,
        "enabled": row.enabled,
        "weight": row.weight,
        "target_duration_sec": row.target_duration_sec,
        "template_pool_json": row.template_pool_json,
        "source_platforms_json": row.source_platforms_json,
        "default_publish_visibility": row.default_publish_visibility,
    }


def _seed(row: ManualSeed) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "channel_profile_id": str(row.channel_profile_id),
        "topic_lane_id": str(row.topic_lane_id) if row.topic_lane_id else None,
        "target_account_id": str(row.target_account_id) if row.target_account_id else None,
        "prompt": row.prompt,
        "title_seed": row.title_seed,
        "status": row.status,
    }


def _queue(row: ChannelOpsQueueItem) -> QueueItemRead:
    return QueueItemRead(
        id=str(row.id),
        kind=row.kind,
        idempotency_key=row.idempotency_key,
        channel_profile_id=str(row.channel_profile_id) if row.channel_profile_id else None,
        priority=row.priority,
        status=row.status,
        payload_json=dict(row.payload_json or {}),
        attempt_count=row.attempt_count,
        last_error=row.last_error,
    )


def _tick(row: AgentTickAudit) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "tick_id": row.tick_id,
        "dry_run": row.dry_run,
        "started_at": row.started_at,
        "finished_at": row.finished_at,
        "tasks_selected": row.tasks_selected,
        "tasks_rejected": row.tasks_rejected,
        "guards_triggered_json": row.guards_triggered_json,
        "decision_summary_json": row.decision_summary_json,
        "error_message": row.error_message,
    }


def _task(row: ProductionTask) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "channel_profile_id": str(row.channel_profile_id),
        "target_account_id": str(row.target_account_id),
        "state": row.state,
        "prompt": row.prompt,
        "title_seed": row.title_seed,
        "blocked_by_guard": row.blocked_by_guard,
        "failure_reason": row.failure_reason,
        "failure_category": row.failure_category,
        "discovery_signal_id": str(row.discovery_signal_id) if row.discovery_signal_id else None,
    }


def _publication(row: PublicationRecord) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "production_task_id": str(row.production_task_id),
        "platform": row.platform,
        "platform_content_id": row.platform_content_id,
        "title": row.title,
        "desired_privacy": row.desired_privacy,
        "current_privacy": row.current_privacy,
        "publish_status": row.publish_status,
        "warnings_json": row.warnings_json,
    }


def _decision(row: DecisionAuditEntry) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "tick_audit_id": str(row.tick_audit_id),
        "channel_profile_id": str(row.channel_profile_id),
        "candidate_id": row.candidate_id,
        "candidate_source": row.candidate_source,
        "topic_lane_id": str(row.topic_lane_id) if row.topic_lane_id else None,
        "lane_format_id": str(row.lane_format_id) if row.lane_format_id else None,
        "target_account_id": str(row.target_account_id) if row.target_account_id else None,
        "score_json": row.score_json or {},
        "guard_results_json": row.guard_results_json or [],
        "pds_decision_json": row.pds_decision_json or {},
        "learning_context_json": row.learning_context_json or {},
        "selected": row.selected,
        "rejection_reason": row.rejection_reason,
        "created_task_id": str(row.created_task_id) if row.created_task_id else None,
        "created_at": row.created_at,
    }


def _learning_state(row: LearningState) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "channel_profile_id": str(row.channel_profile_id),
        "dimension_type": row.dimension_type,
        "dimension_key": row.dimension_key,
        "window_days": row.window_days,
        "sample_count": row.sample_count,
        "avg_reward": row.avg_reward,
        "confidence": row.confidence,
        "recommendation_json": row.recommendation_json or {},
        "last_computed_at": row.last_computed_at,
    }


def _material_usage(row: MaterialUsageLedger) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "material_id": row.material_id,
        "asset_id": str(row.asset_id) if row.asset_id else None,
        "segment_signature": row.segment_signature,
        "metadata_json": row.metadata_json or {},
    }
