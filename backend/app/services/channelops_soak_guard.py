from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from typing import Iterable, Mapping

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.channel_agent import (
    ChannelOpsQueueItem,
    ChannelProfile,
    FeedbackSnapshot,
    LaneFormatMatrix,
    ProductionTask,
    PublicationRecord,
    PublishingAccount,
    TopicLane,
)
from app.models.youtube_upload_operation import YouTubeUploadOperation


SOAK_GUARD_REASON = "automated_channelops_soak_guard"
ALLOWED_EXTERNAL_CONDITIONS = frozenset(
    {
        "forbidden_node_placement",
        "redis_group_missing",
        "redis_pending_exceeded",
        "service_missing",
        "service_unhealthy",
    }
)
_INTERNAL_CRITICAL_CODES = frozenset(
    {
        "ambiguous_upload_operation",
        "channel_disabled",
        "channel_dry_run",
        "channel_halted",
        "channel_missing",
        "channelops_queue_failure",
        "external_asset_human_approval_missing",
        "external_asset_auto_publish_enabled",
        "failed_upload_operation",
        "feedback_missing_after_grace",
        "production_task_failure",
        "publication_cadence_exceeded",
        "stale_upload_operation",
        "unsafe_account_privacy",
        "unsafe_lane_privacy",
        "unsafe_publication_privacy",
    }
)
CRITICAL_CODES = _INTERNAL_CRITICAL_CODES | ALLOWED_EXTERNAL_CONDITIONS
_ALLOWED_PRIVACY = frozenset({"private", "unlisted"})
_FAILED_TASK_STATES = frozenset({"failed", "held"})
_FAILED_QUEUE_STATUSES = frozenset({"failed", "dead_lettered"})
_EXTERNAL_ASSET_REVIEW_STATES = frozenset(
    {"uploaded_private", "scheduled", "published", "measured"}
)
_STALE_UPLOAD_STATUSES = frozenset({"reserved", "submitted"})


@dataclass(frozen=True)
class SoakGuardPolicy:
    channel_id: uuid.UUID
    started_at: datetime
    max_publications_per_24h: int = 1
    upload_stale_minutes: int = 45
    feedback_grace_hours: int = 30

    def __post_init__(self) -> None:
        try:
            resolved_channel_id = uuid.UUID(str(self.channel_id))
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError("channel_id must be a UUID") from exc
        object.__setattr__(self, "channel_id", resolved_channel_id)
        object.__setattr__(self, "started_at", _utc(self.started_at))
        for field_name in (
            "max_publications_per_24h",
            "upload_stale_minutes",
            "feedback_grace_hours",
        ):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{field_name} must be a positive integer")


@dataclass(frozen=True)
class SoakGuardAssessment:
    critical_codes: tuple[str, ...]
    metrics: Mapping[str, str | int]

    def __post_init__(self) -> None:
        codes = tuple(sorted(set(self.critical_codes)))
        unknown = set(codes) - CRITICAL_CODES
        if unknown:
            raise ValueError("assessment contains unknown critical codes")
        object.__setattr__(self, "critical_codes", codes)
        object.__setattr__(self, "metrics", MappingProxyType(dict(self.metrics)))

    @property
    def healthy(self) -> bool:
        return not self.critical_codes


async def assess_channelops_soak(
    db: AsyncSession,
    policy: SoakGuardPolicy,
    *,
    external_conditions: Iterable[str] = (),
    now: datetime | None = None,
) -> SoakGuardAssessment:
    external_codes = set(external_conditions)
    unknown_external = external_codes - ALLOWED_EXTERNAL_CONDITIONS
    if unknown_external:
        raise ValueError("unknown external condition code")

    assessed_at = _utc(now or datetime.now(timezone.utc))
    started_at = _utc(policy.started_at)
    critical_codes = set(external_codes)
    metrics = _empty_metrics(policy.channel_id)
    metrics["external_condition_count"] = len(external_codes)

    channel = (
        await db.execute(select(ChannelProfile).where(ChannelProfile.id == policy.channel_id))
    ).scalar_one_or_none()
    if channel is None:
        critical_codes.add("channel_missing")
        return _assessment(critical_codes, metrics)

    metrics["channel_count"] = 1
    if not channel.enabled:
        critical_codes.add("channel_disabled")
    if channel.dry_run:
        critical_codes.add("channel_dry_run")
    if channel.halted_at is not None:
        critical_codes.add("channel_halted")

    accounts = list(
        (
            await db.execute(
                select(PublishingAccount).where(
                    PublishingAccount.channel_profile_id == policy.channel_id,
                    PublishingAccount.enabled.is_(True),
                )
            )
        )
        .scalars()
        .all()
    )
    unsafe_accounts = [account for account in accounts if account.default_privacy not in _ALLOWED_PRIVACY]
    automatic_external_accounts = [
        account for account in accounts if account.external_asset_auto_publish
    ]
    metrics["enabled_account_count"] = len(accounts)
    metrics["unsafe_account_privacy_count"] = len(unsafe_accounts)
    metrics["external_asset_auto_publish_count"] = len(automatic_external_accounts)
    if unsafe_accounts:
        critical_codes.add("unsafe_account_privacy")
    if automatic_external_accounts:
        critical_codes.add("external_asset_auto_publish_enabled")

    lane_formats = list(
        (
            await db.execute(
                select(LaneFormatMatrix)
                .join(TopicLane, LaneFormatMatrix.topic_lane_id == TopicLane.id)
                .where(
                    TopicLane.channel_profile_id == policy.channel_id,
                    TopicLane.enabled.is_(True),
                    LaneFormatMatrix.enabled.is_(True),
                )
            )
        )
        .scalars()
        .all()
    )
    unsafe_lane_formats = [
        lane_format
        for lane_format in lane_formats
        if lane_format.default_publish_visibility not in _ALLOWED_PRIVACY
    ]
    metrics["enabled_lane_format_count"] = len(lane_formats)
    metrics["unsafe_lane_privacy_count"] = len(unsafe_lane_formats)
    if unsafe_lane_formats:
        critical_codes.add("unsafe_lane_privacy")

    task_recency = or_(
        ProductionTask.created_at >= started_at,
        ProductionTask.updated_at >= started_at,
        ProductionTask.state_updated_at >= started_at,
    )
    tasks = list(
        (
            await db.execute(
                select(ProductionTask).where(
                    ProductionTask.channel_profile_id == policy.channel_id,
                    task_recency,
                )
            )
        )
        .scalars()
        .all()
    )
    failed_tasks = [task for task in tasks if task.state in _FAILED_TASK_STATES]
    external_review_violations = [
        task
        for task in tasks
        if task.uses_external_assets
        and task.state in _EXTERNAL_ASSET_REVIEW_STATES
        and task.approval_mode != "human"
    ]
    metrics["production_task_count"] = len(tasks)
    metrics["production_task_failure_count"] = len(failed_tasks)
    metrics["external_asset_review_violation_count"] = len(external_review_violations)
    if failed_tasks:
        critical_codes.add("production_task_failure")
    if external_review_violations:
        critical_codes.add("external_asset_human_approval_missing")

    publication_recency = or_(
        PublicationRecord.created_at >= started_at,
        PublicationRecord.updated_at >= started_at,
        PublicationRecord.uploaded_at >= started_at,
        PublicationRecord.public_at >= started_at,
    )
    publications = list(
        (
            await db.execute(
                select(PublicationRecord)
                .join(
                    ProductionTask,
                    PublicationRecord.production_task_id == ProductionTask.id,
                )
                .where(
                    ProductionTask.channel_profile_id == policy.channel_id,
                    publication_recency,
                )
            )
        )
        .scalars()
        .all()
    )
    unsafe_publications = [
        publication
        for publication in publications
        if publication.current_privacy not in _ALLOWED_PRIVACY
    ]
    cadence_start = max(started_at, assessed_at - timedelta(hours=24))
    recent_publications = [
        publication
        for publication in publications
        if (published_at := _publication_timestamp(publication)) is not None
        and cadence_start <= published_at <= assessed_at
    ]
    metrics["publication_count"] = len(publications)
    metrics["unsafe_publication_privacy_count"] = len(unsafe_publications)
    metrics["publication_last_24h_count"] = len(recent_publications)
    if unsafe_publications:
        critical_codes.add("unsafe_publication_privacy")
    if len(recent_publications) > policy.max_publications_per_24h:
        critical_codes.add("publication_cadence_exceeded")

    feedback_publication_ids = set(
        (
            await db.execute(
                select(FeedbackSnapshot.publication_id)
                .join(PublicationRecord, FeedbackSnapshot.publication_id == PublicationRecord.id)
                .join(
                    ProductionTask,
                    PublicationRecord.production_task_id == ProductionTask.id,
                )
                .where(ProductionTask.channel_profile_id == policy.channel_id)
            )
        )
        .scalars()
        .all()
    )
    feedback_cutoff = assessed_at - timedelta(hours=policy.feedback_grace_hours)
    missing_feedback = [
        publication
        for publication in publications
        if publication.id not in feedback_publication_ids
        and (published_at := _publication_timestamp(publication)) is not None
        and started_at <= published_at <= feedback_cutoff
    ]
    metrics["feedback_snapshot_count"] = len(feedback_publication_ids)
    metrics["feedback_missing_after_grace_count"] = len(missing_feedback)
    if missing_feedback:
        critical_codes.add("feedback_missing_after_grace")

    operation_recency = or_(
        YouTubeUploadOperation.created_at >= started_at,
        YouTubeUploadOperation.updated_at >= started_at,
        YouTubeUploadOperation.request_attempted_at >= started_at,
        YouTubeUploadOperation.completed_at >= started_at,
    )
    operations = list(
        (
            await db.execute(
                select(YouTubeUploadOperation)
                .join(
                    ProductionTask,
                    YouTubeUploadOperation.production_task_id == ProductionTask.id,
                )
                .where(
                    ProductionTask.channel_profile_id == policy.channel_id,
                    operation_recency,
                )
            )
        )
        .scalars()
        .all()
    )
    ambiguous_operations = [operation for operation in operations if operation.status == "uncertain"]
    failed_operations = [operation for operation in operations if operation.status == "failed"]
    upload_cutoff = assessed_at - timedelta(minutes=policy.upload_stale_minutes)
    stale_operations = [
        operation
        for operation in operations
        if operation.status in _STALE_UPLOAD_STATUSES
        and _upload_activity_timestamp(operation) <= upload_cutoff
    ]
    metrics["upload_operation_count"] = len(operations)
    metrics["ambiguous_upload_operation_count"] = len(ambiguous_operations)
    metrics["failed_upload_operation_count"] = len(failed_operations)
    metrics["stale_upload_operation_count"] = len(stale_operations)
    if ambiguous_operations:
        critical_codes.add("ambiguous_upload_operation")
    if failed_operations:
        critical_codes.add("failed_upload_operation")
    if stale_operations:
        critical_codes.add("stale_upload_operation")

    queue_recency = or_(
        ChannelOpsQueueItem.created_at >= started_at,
        ChannelOpsQueueItem.updated_at >= started_at,
        ChannelOpsQueueItem.dead_letter_at >= started_at,
    )
    queue_items = list(
        (
            await db.execute(
                select(ChannelOpsQueueItem).where(
                    ChannelOpsQueueItem.channel_profile_id == policy.channel_id,
                    queue_recency,
                )
            )
        )
        .scalars()
        .all()
    )
    failed_queue_items = [item for item in queue_items if item.status in _FAILED_QUEUE_STATUSES]
    metrics["channelops_queue_item_count"] = len(queue_items)
    metrics["channelops_queue_failure_count"] = len(failed_queue_items)
    if failed_queue_items:
        critical_codes.add("channelops_queue_failure")

    return _assessment(critical_codes, metrics)


def _empty_metrics(channel_id: uuid.UUID) -> dict[str, str | int]:
    return {
        "channel_id": str(channel_id),
        "channel_count": 0,
        "enabled_account_count": 0,
        "unsafe_account_privacy_count": 0,
        "external_asset_auto_publish_count": 0,
        "enabled_lane_format_count": 0,
        "unsafe_lane_privacy_count": 0,
        "production_task_count": 0,
        "production_task_failure_count": 0,
        "external_asset_review_violation_count": 0,
        "publication_count": 0,
        "unsafe_publication_privacy_count": 0,
        "publication_last_24h_count": 0,
        "feedback_snapshot_count": 0,
        "feedback_missing_after_grace_count": 0,
        "upload_operation_count": 0,
        "ambiguous_upload_operation_count": 0,
        "failed_upload_operation_count": 0,
        "stale_upload_operation_count": 0,
        "channelops_queue_item_count": 0,
        "channelops_queue_failure_count": 0,
        "external_condition_count": 0,
    }


def _assessment(
    critical_codes: set[str],
    metrics: Mapping[str, str | int],
) -> SoakGuardAssessment:
    return SoakGuardAssessment(tuple(sorted(critical_codes)), metrics)


def _publication_timestamp(publication: PublicationRecord) -> datetime | None:
    value = publication.public_at or publication.uploaded_at or publication.created_at
    return _utc(value) if value is not None else None


def _upload_activity_timestamp(operation: YouTubeUploadOperation) -> datetime:
    value = operation.request_attempted_at or operation.updated_at or operation.created_at
    return _utc(value)


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError("timestamp must be a datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
