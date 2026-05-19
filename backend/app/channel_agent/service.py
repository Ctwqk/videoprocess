from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, get_args

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.channel_agent.alerts import build_alert_payload
from app.channel_agent.clock import Clock
from app.channel_agent.clients import (
    AutoFlowClient,
    FakeAutoFlowClient,
    FakeYouTubeClient,
    MiniMaxImageClient,
    MiniMaxClient,
    YouTubeClient,
)
from app.channel_agent.constants import (
    ALERT_MATERIAL_SUPPLY_LOW,
    ALERT_QUOTA_LOW,
    ALERT_TAKEDOWN,
    ALERT_TOKEN_EXPIRING,
    TASK_FAILED,
    TASK_HELD,
    TASK_PLANNING,
    TASK_SCHEDULED,
    TASK_SELECTED,
    TASK_UPLOADED_PRIVATE,
)
from app.channel_agent.queue import ChannelOpsQueueService, utc_hour_bucket
from app.models.channel_agent import (
    AgentTickAudit,
    ChannelOpsQueueItem,
    ChannelProfile,
    FeedbackSnapshot,
    LaneFormatMatrix,
    ManualSeed,
    ProductionTask,
    PublicationRecord,
    PublishingAccount,
    TakedownEvent,
    TopicLane,
)
from app.schemas.autoflow import AutoFlowRequest, PlanningMode, SourceStrategy


_SAFE_PRIVACY_VALUES = {"private", "unlisted"}
_SOURCE_STRATEGY_ALIASES = {"external_search": "external_research"}
_ALLOWED_SOURCE_STRATEGIES = set(get_args(SourceStrategy))
_ALLOWED_PLANNING_MODES = set(get_args(PlanningMode))


class ChannelAgentService:
    def __init__(
        self,
        *,
        queue: ChannelOpsQueueService | None = None,
        clock: Clock | None = None,
        autoflow_client: AutoFlowClient | None = None,
        youtube_client: YouTubeClient | None = None,
        minimax_client: MiniMaxClient | None = None,
    ) -> None:
        self.clock = clock or Clock()
        self.queue = queue or ChannelOpsQueueService(clock=self.clock)
        self.autoflow_client = autoflow_client or FakeAutoFlowClient()
        self.youtube_client = youtube_client or FakeYouTubeClient()
        self.minimax_client = minimax_client or MiniMaxImageClient()

    async def tick(self, db: AsyncSession, *, channel_id) -> AgentTickAudit:
        channel = await db.get(ChannelProfile, _uuid(channel_id))
        if channel is None:
            raise ValueError("Channel not found")

        lanes = (
            await db.execute(
                select(TopicLane).where(TopicLane.channel_profile_id == channel.id).where(TopicLane.enabled.is_(True))
            )
        ).scalars().all()
        accounts = (
            await db.execute(
                select(PublishingAccount)
                .where(PublishingAccount.channel_profile_id == channel.id)
                .where(PublishingAccount.enabled.is_(True))
            )
        ).scalars().all()
        seeds = (
            await db.execute(
                select(ManualSeed)
                .where(ManualSeed.channel_profile_id == channel.id)
                .where(ManualSeed.status == "active")
                .order_by(ManualSeed.created_at.asc())
            )
        ).scalars().all()

        per_lane = {str(lane.id): 0 for lane in lanes}
        for seed in seeds:
            lane_id = str(seed.topic_lane_id or (lanes[0].id if lanes else "unassigned"))
            per_lane[lane_id] = per_lane.get(lane_id, 0) + 1

        audit = AgentTickAudit(
            channel_profile_id=channel.id,
            tick_id=f"tick:{channel.id}:{utc_hour_bucket(self.clock.now())}",
            dry_run=bool(channel.dry_run),
            started_at=self.clock.now(),
            finished_at=self.clock.now(),
            ideas_discovered=len(seeds),
            candidates_scored=len(seeds),
            tasks_selected=0,
            tasks_rejected=0,
            decision_summary_json={"per_lane_eligible_count": per_lane},
        )
        db.add(audit)

        low_supply_alerts = await self._maybe_alert_low_supply(db, channel, per_lane)
        audit.guards_triggered_json = low_supply_alerts

        if channel.dry_run or channel.halted_at is not None:
            await db.commit()
            await db.refresh(audit)
            return audit

        selected = 0
        for seed in seeds:
            account = await self._resolve_account(db, seed, accounts)
            lane_id = seed.topic_lane_id or (lanes[0].id if lanes else None)
            lane_format = await self._resolve_lane_format(db, lane_id)
            source_platforms = _string_list(seed.source_platforms_json) or _string_list(
                lane_format.source_platforms_json if lane_format else []
            )
            material_library_ids = _string_list(seed.material_library_ids_json)
            task = ProductionTask(
                channel_profile_id=channel.id,
                topic_lane_id=lane_id,
                lane_format_id=lane_format.id if lane_format else None,
                target_account_id=account.id,
                manual_seed_id=seed.id,
                source="manual_seed",
                title_seed=seed.title_seed,
                prompt=seed.prompt,
                portfolio_bucket="explore",
                source_platforms_json=source_platforms,
                material_library_ids_json=material_library_ids,
                uses_external_assets=bool(source_platforms),
                state=TASK_SELECTED,
                state_updated_at=self.clock.now(),
                channel_config_version_snapshot=channel.config_version,
                channel_config_snapshot_json=_snapshot(channel, account, lane_format, manual_seed=seed),
                transition_history_json=[
                    _transition("seeded", TASK_SELECTED, "agent_tick", self.clock.now()),
                ],
            )
            db.add(task)
            seed.status = "exhausted"
            await db.flush()
            await self.queue.enqueue(
                db,
                kind="plan_task",
                idempotency_key=f"plan_task:{task.id}",
                payload={"production_task_id": str(task.id)},
                priority=50,
                channel_profile_id=channel.id,
            )
            selected += 1

        audit.tasks_selected = selected
        await db.commit()
        await db.refresh(audit)
        return audit

    async def handle_plan_task(self, db: AsyncSession, item: ChannelOpsQueueItem) -> ProductionTask:
        task = await self._task_from_item(db, item)
        request = self._autoflow_request(task)
        observation = await self.autoflow_client.plan_task(task, request)
        if observation.upload_node_count != 1:
            task.state = TASK_HELD
            task.blocked_by_guard = "missing_youtube_upload_node"
            task.failure_reason = "AutoFlow plan must contain exactly one youtube_upload node"
            task.state_updated_at = self.clock.now()
            task.transition_history_json = [
                *list(task.transition_history_json or []),
                _transition(task.state, TASK_HELD, "plan_task", self.clock.now()),
            ]
            await db.commit()
            await db.refresh(task)
            return task

        task.autoflow_plan_id = uuid.UUID(observation.plan_id)
        task.state = TASK_PLANNING
        task.state_updated_at = self.clock.now()
        await self.queue.enqueue(
            db,
            kind="execute_task",
            idempotency_key=f"execute_task:{task.id}",
            payload={"production_task_id": str(task.id)},
            priority=60,
            channel_profile_id=task.channel_profile_id,
            parent_queue_item_id=item.id,
        )
        await db.commit()
        await db.refresh(task)
        return task

    async def handle_publish_task(self, db: AsyncSession, item: ChannelOpsQueueItem) -> PublicationRecord | None:
        task = await self._task_from_item(db, item)
        account = await db.get(PublishingAccount, task.target_account_id)
        if account is None:
            raise ValueError("Publishing account not found")

        remaining = await self.youtube_client.quota_remaining_fraction(account)
        if remaining < 0.2:
            task.state = TASK_HELD
            task.blocked_by_guard = "quota_below_20pct"
            task.failure_reason = "YouTube quota remaining below 20%"
            await self._enqueue_alert(
                db,
                ALERT_QUOTA_LOW,
                resource_id=str(account.id),
                severity="warning",
                message="YouTube quota remaining below 20%",
                details={"remaining_fraction": remaining},
                channel_profile_id=task.channel_profile_id,
            )
            await db.commit()
            return None

        youtube = dict(item.payload_json.get("youtube") or {})
        video_id = str(youtube.get("video_id") or "")
        if not video_id:
            task.state = TASK_FAILED
            task.failure_reason = "publish_task missing YouTube video id"
            await db.commit()
            return None

        publication = await self._publication_for_task(db, task)
        if publication is None:
            publication = PublicationRecord(
                production_task_id=task.id,
                platform="youtube",
                account_id=account.id,
                platform_content_id=video_id,
                title=task.title_seed or task.prompt[:80],
                description=task.prompt,
                desired_privacy=self._desired_privacy(task, account),
                current_privacy="private",
                publish_status="uploaded",
                uploaded_at=self.clock.now(),
                compliance_disposition="assumed_fair_use" if task.source == "manual_seed" else "known_risk_accepted",
                quota_units_estimated=1600,
            )
            db.add(publication)
            await db.flush()

        if self._uses_external_assets(task) and not account.external_asset_auto_publish:
            task.state = TASK_HELD
            task.blocked_by_guard = "external_asset_auto_publish_required"
            publication.publish_status = "held"
            await db.commit()
            await db.refresh(publication)
            return publication

        try:
            thumbnail = await self.minimax_client.generate_thumbnail(prompt=task.prompt, title=publication.title)
            publication.thumbnail_storage_path = str(thumbnail.get("storage_path") or thumbnail.get("image_url") or "")
        except Exception as exc:
            publication.warnings_json = [*list(publication.warnings_json or []), f"thumbnail_failed:{exc}"]

        task.state = TASK_UPLOADED_PRIVATE
        task.state_updated_at = self.clock.now()
        scheduled = self.clock.now() + timedelta(hours=1)
        await self.queue.enqueue(
            db,
            kind="promote_publication",
            idempotency_key=f"promote_publication:{publication.id}:{publication.desired_privacy}:{scheduled.isoformat()}",
            payload={
                "publication_id": str(publication.id),
                "scheduled_at": scheduled.isoformat(),
                "target_visibility": publication.desired_privacy,
            },
            priority=70,
            channel_profile_id=task.channel_profile_id,
            parent_queue_item_id=item.id,
        )
        await db.commit()
        await db.refresh(publication)
        return publication

    async def handle_promote_publication(self, db: AsyncSession, item: ChannelOpsQueueItem) -> PublicationRecord:
        publication_id = _uuid(item.payload_json["publication_id"])
        publication = await db.get(PublicationRecord, publication_id)
        if publication is None:
            raise ValueError("Publication not found")
        scheduled_at = _parse_datetime(str(item.payload_json.get("scheduled_at") or self.clock.now().isoformat()))
        visibility = str(item.payload_json.get("target_visibility") or publication.desired_privacy or "public")
        await self.youtube_client.schedule_publish(
            video_id=publication.platform_content_id,
            scheduled_at=scheduled_at,
            privacy=visibility,
        )
        publication.publish_status = "scheduled"
        publication.desired_privacy = visibility
        publication.scheduled_publish_at = scheduled_at
        await db.commit()
        await db.refresh(publication)
        return publication

    async def handle_account_health(self, db: AsyncSession, item: ChannelOpsQueueItem) -> PublishingAccount:
        account = await db.get(PublishingAccount, _uuid(item.payload_json["account_id"]))
        if account is None:
            raise ValueError("Account not found")
        ok = await self.youtube_client.refresh_token(account)
        account.last_token_check_at = self.clock.now()
        account.last_token_check_status = "ok" if ok else "invalid"
        if not ok:
            account.enabled = False
            await self._enqueue_alert(
                db,
                ALERT_TOKEN_EXPIRING,
                resource_id=str(account.id),
                severity="warning",
                message="YouTube OAuth token refresh failed",
                details={"account_label": account.account_label},
                channel_profile_id=account.channel_profile_id,
            )
        await db.commit()
        await db.refresh(account)
        return account

    async def log_takedown_event(
        self,
        db: AsyncSession,
        *,
        publication_id,
        event_type: str,
        severity: str,
        raw_payload: dict[str, Any],
    ) -> TakedownEvent:
        publication = await db.get(PublicationRecord, _uuid(publication_id))
        if publication is None:
            raise ValueError("Publication not found")
        event = TakedownEvent(
            publication_id=publication.id,
            event_type=event_type,
            severity=severity,
            raw_payload_json=dict(raw_payload),
        )
        db.add(event)
        account = await db.get(PublishingAccount, publication.account_id)
        task = await db.get(ProductionTask, publication.production_task_id)
        channel_profile_id = task.channel_profile_id if task is not None else None
        if channel_profile_id is None and account is not None:
            channel_profile_id = account.channel_profile_id
        actions: list[str] = []
        if account is not None and severity == "severe":
            account.enabled = False
            actions.append(f"paused_account:{account.id}")
        event.auto_actions_taken_json = actions
        await self._enqueue_alert(
            db,
            ALERT_TAKEDOWN,
            resource_id=str(publication.id),
            severity=severity,
            message=f"YouTube takedown event logged: {event_type}",
            details={"event_type": event_type, "publication_id": str(publication.id)},
            channel_profile_id=channel_profile_id,
        )
        await db.commit()
        await db.refresh(event)
        return event

    async def _maybe_alert_low_supply(
        self,
        db: AsyncSession,
        channel: ChannelProfile,
        per_lane: dict[str, int],
    ) -> list[dict[str, Any]]:
        triggered: list[dict[str, Any]] = []
        for lane_id, count in per_lane.items():
            if count >= 1:
                continue
            recent = (
                await db.execute(
                    select(AgentTickAudit)
                    .where(AgentTickAudit.channel_profile_id == channel.id)
                    .order_by(AgentTickAudit.started_at.desc())
                    .limit(2)
                )
            ).scalars().all()
            previous_low = all(
                int((audit.decision_summary_json or {}).get("per_lane_eligible_count", {}).get(lane_id, 0)) < 1
                for audit in recent
            )
            if len(recent) >= 2 and previous_low:
                await self._enqueue_alert(
                    db,
                    ALERT_MATERIAL_SUPPLY_LOW,
                    resource_id=lane_id,
                    severity="warning",
                    message="Lane material supply below candidate threshold for three ticks",
                    details={"channel_id": str(channel.id), "eligible_count": count},
                    channel_profile_id=channel.id,
                )
                triggered.append({"guard": "material_supply_low", "lane_id": lane_id})
        return triggered

    async def _enqueue_alert(
        self,
        db: AsyncSession,
        alert_type: str,
        *,
        resource_id: str,
        severity: str,
        message: str,
        details: dict[str, Any] | None = None,
        channel_profile_id=None,
    ) -> ChannelOpsQueueItem:
        payload = build_alert_payload(
            alert_type,
            resource_id=resource_id,
            severity=severity,
            message=message,
            details=details or {},
            now=self.clock.now(),
        )
        return await self.queue.enqueue(
            db,
            kind="send_alert",
            idempotency_key=str(payload["dedupe_key"]),
            payload=payload,
            priority=5,
            channel_profile_id=channel_profile_id,
        )

    async def _resolve_account(
        self,
        db: AsyncSession,
        seed: ManualSeed,
        accounts: list[PublishingAccount],
    ) -> PublishingAccount:
        if seed.target_account_id:
            account = await db.get(PublishingAccount, seed.target_account_id)
            if account is not None:
                return account
        if accounts:
            return accounts[0]
        raise ValueError("No enabled publishing account")

    async def _resolve_lane_format(self, db: AsyncSession, lane_id) -> LaneFormatMatrix | None:
        if not lane_id:
            return None
        result = await db.execute(
            select(LaneFormatMatrix)
            .where(LaneFormatMatrix.topic_lane_id == lane_id)
            .where(LaneFormatMatrix.enabled.is_(True))
            .order_by(LaneFormatMatrix.weight.desc())
        )
        return result.scalars().first()

    async def _task_from_item(self, db: AsyncSession, item: ChannelOpsQueueItem) -> ProductionTask:
        task = await db.get(ProductionTask, _uuid(item.payload_json["production_task_id"]))
        if task is None:
            raise ValueError("Production task not found")
        return task

    async def _publication_for_task(self, db: AsyncSession, task: ProductionTask) -> PublicationRecord | None:
        result = await db.execute(select(PublicationRecord).where(PublicationRecord.production_task_id == task.id))
        return result.scalar_one_or_none()

    def _desired_privacy(self, task: ProductionTask, account: PublishingAccount) -> str:
        snapshot_privacy = self._desired_privacy_from_snapshot(task)
        if snapshot_privacy is not None:
            return snapshot_privacy
        account_privacy = _safe_privacy(account.default_privacy)
        if account_privacy is not None:
            return account_privacy
        return "unlisted"

    def _desired_privacy_from_snapshot(self, task: ProductionTask) -> str | None:
        snapshot = _dict_value(task.channel_config_snapshot_json)
        lane_format = _dict_value(snapshot.get("lane_format"))
        return _safe_privacy(lane_format.get("default_publish_visibility"))

    def _autoflow_request(self, task: ProductionTask) -> dict[str, Any]:
        snapshot = _dict_value(task.channel_config_snapshot_json)
        channel = _dict_value(snapshot.get("channel"))
        lane_format = _dict_value(snapshot.get("lane_format"))
        manual_seed = _dict_value(snapshot.get("manual_seed"))
        lane = _dict_value(snapshot.get("lane"))
        risk_policy = _dict_value(channel.get("risk_policy_json"))
        manual_seed_constraints = _dict_value(manual_seed.get("constraints_json"))
        constraints = {
            "lane_id": lane.get("id"),
            "lane_format_id": lane_format.get("id"),
            "template_pool_json": _string_list(lane_format.get("template_pool_json")),
        }
        constraints.update(manual_seed_constraints)

        source_platforms = self._effective_source_platforms(task)
        request = {
            "prompt": task.prompt,
            "target_platforms": ["youtube"],
            "source_platforms": source_platforms,
            "duration_sec": _positive_int(lane_format.get("target_duration_sec"), default=30),
            "aspect_ratio": str(channel.get("default_aspect_ratio") or "9:16"),
            "source_policy": "remix_with_review" if self._uses_external_assets(task) else "owned_only",
            "publish_mode": self._autoflow_publish_mode(task),
            "material_library_ids": _string_list(task.material_library_ids_json),
            "source_strategy": _normalize_source_strategy(
                manual_seed.get("source_strategy")
                or manual_seed_constraints.get("source_strategy")
                or risk_policy.get("source_strategy")
            ),
            "planning_mode": _normalize_planning_mode(
                manual_seed.get("planning_mode")
                or manual_seed_constraints.get("planning_mode")
                or risk_policy.get("planning_mode")
            ),
            "constraints": constraints,
        }
        validated = AutoFlowRequest.model_validate(request)
        return validated.model_dump(include=set(request))

    def _autoflow_publish_mode(self, task: ProductionTask) -> str:
        privacy = (
            self._desired_privacy_from_snapshot(task)
            or self._account_default_privacy_from_snapshot(task)
            or "unlisted"
        )
        if privacy == "unlisted":
            return "unlisted_upload"
        return "private_upload"

    def _account_default_privacy_from_snapshot(self, task: ProductionTask) -> str | None:
        snapshot = _dict_value(task.channel_config_snapshot_json)
        account = _dict_value(snapshot.get("account"))
        return _safe_privacy(account.get("default_privacy"))

    def _effective_source_platforms(self, task: ProductionTask) -> list[str]:
        snapshot = _dict_value(task.channel_config_snapshot_json)
        lane_format = _dict_value(snapshot.get("lane_format"))
        return _string_list(task.source_platforms_json) or _string_list(lane_format.get("source_platforms_json"))

    def _uses_external_assets(self, task: ProductionTask) -> bool:
        return bool(task.uses_external_assets) or bool(self._effective_source_platforms(task))


def _snapshot(
    channel: ChannelProfile,
    account: PublishingAccount,
    lane_format: LaneFormatMatrix | None,
    *,
    manual_seed: ManualSeed | None = None,
) -> dict[str, Any]:
    snapshot = {
        "channel": {
            "id": str(channel.id),
            "dry_run": channel.dry_run,
            "default_aspect_ratio": channel.default_aspect_ratio,
            "risk_policy_json": dict(channel.risk_policy_json or {}),
            "cadence_policy_json": dict(channel.cadence_policy_json or {}),
            "content_mix_policy_json": dict(channel.content_mix_policy_json or {}),
        },
        "account": {
            "id": str(account.id),
            "default_privacy": account.default_privacy,
            "external_asset_auto_publish": account.external_asset_auto_publish,
        },
        "lane": {
            "id": str(lane_format.topic_lane_id) if lane_format else None,
        },
        "lane_format": {
            "id": str(lane_format.id) if lane_format else None,
            "format_key": lane_format.format_key if lane_format else "",
            "default_publish_visibility": lane_format.default_publish_visibility if lane_format else "private",
            "target_duration_sec": lane_format.target_duration_sec if lane_format else 30,
            "template_pool_json": _string_list(lane_format.template_pool_json) if lane_format else [],
            "source_platforms_json": _string_list(lane_format.source_platforms_json) if lane_format else [],
        },
    }
    if manual_seed is not None:
        snapshot["manual_seed"] = {"constraints_json": _dict_value(manual_seed.constraints_json)}
    return snapshot


def _transition(from_state: str, to_state: str, actor: str, now: datetime) -> dict[str, Any]:
    return {
        "from": from_state,
        "to": to_state,
        "actor": actor,
        "at": now.isoformat(),
    }


def _uuid(value) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))


def _parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _dict_value(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        cleaned = value.strip()
        return [cleaned] if cleaned else []
    if isinstance(value, (list, tuple, set)):
        cleaned_items = []
        for item in value:
            if item is None:
                continue
            cleaned = str(item).strip()
            if cleaned:
                cleaned_items.append(cleaned)
        return cleaned_items
    return []


def _positive_int(value: Any, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _safe_privacy(value: Any) -> str | None:
    desired = str(value or "").strip().lower()
    if desired in _SAFE_PRIVACY_VALUES:
        return desired
    return None


def _normalize_source_strategy(value: Any) -> str:
    requested = str(value or "auto").strip().lower()
    normalized = _SOURCE_STRATEGY_ALIASES.get(requested, requested)
    if normalized in _ALLOWED_SOURCE_STRATEGIES:
        return normalized
    return "auto"


def _normalize_planning_mode(value: Any) -> str:
    requested = str(value or "auto").strip().lower()
    if requested in _ALLOWED_PLANNING_MODES:
        return requested
    return "auto"
