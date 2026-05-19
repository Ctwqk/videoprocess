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
    TASK_PRODUCING,
    TASK_SCHEDULED,
    TASK_SELECTED,
    TASK_UPLOADED_PRIVATE,
)
from app.channel_agent.lane_prompts import build_lane_prompt
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
_ACTIVE_TASK_STATES = {
    TASK_SELECTED,
    TASK_PLANNING,
    TASK_HELD,
    TASK_PRODUCING,
    TASK_UPLOADED_PRIVATE,
    TASK_SCHEDULED,
}


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

        now = self.clock.now()
        bucket = utc_hour_bucket(now)
        lanes = (
            await db.execute(
                select(TopicLane)
                .where(TopicLane.channel_profile_id == channel.id)
                .where(TopicLane.enabled.is_(True))
                .order_by(TopicLane.weight.desc(), TopicLane.created_at.asc())
            )
        ).scalars().all()
        accounts = (
            await db.execute(
                select(PublishingAccount)
                .where(PublishingAccount.channel_profile_id == channel.id)
                .where(PublishingAccount.enabled.is_(True))
                .order_by(PublishingAccount.created_at.asc())
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

        lane_formats_by_lane = await self._lane_formats_by_lane(db, lanes)
        candidates = await self._build_tick_candidates(
            db,
            channel=channel,
            lanes=lanes,
            accounts=accounts,
            seeds=seeds,
            lane_formats_by_lane=lane_formats_by_lane,
            bucket=bucket,
        )
        accepted_candidates, rejected_candidates = await self._evaluate_tick_candidates(db, candidates)
        per_lane = _per_lane_counts(lanes, candidates)
        side_effects_disabled = bool(channel.dry_run or channel.halted_at is not None)
        low_supply_alerts = await self._maybe_alert_low_supply(
            db,
            channel,
            per_lane,
            enqueue_alerts=not side_effects_disabled,
        )

        audit = AgentTickAudit(
            channel_profile_id=channel.id,
            tick_id=f"tick:{channel.id}:{bucket}",
            dry_run=bool(channel.dry_run),
            started_at=now,
            finished_at=self.clock.now(),
            ideas_discovered=len(candidates),
            candidates_scored=len(candidates),
            tasks_selected=0 if side_effects_disabled else len(accepted_candidates),
            tasks_rejected=len(rejected_candidates),
            decision_summary_json={
                "per_lane_eligible_count": per_lane,
                "rejected_candidates": rejected_candidates,
                "low_supply_alerts": low_supply_alerts,
            },
        )
        db.add(audit)

        audit.guards_triggered_json = [
            *low_supply_alerts,
            *[
                {
                    "guard": rejected["guard"],
                    "candidate_id": rejected["candidate_id"],
                    "lane_id": rejected["lane_id"],
                    "account_id": rejected["account_id"],
                }
                for rejected in rejected_candidates
            ],
        ]

        if side_effects_disabled:
            await db.commit()
            await db.refresh(audit)
            return audit

        selected = 0
        for candidate in accepted_candidates:
            task = self._task_from_candidate(
                channel,
                candidate,
                created_at=now + timedelta(microseconds=selected),
            )
            db.add(task)
            seed = candidate.get("seed")
            if seed is not None:
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

    async def _lane_formats_by_lane(
        self,
        db: AsyncSession,
        lanes: list[TopicLane],
    ) -> dict[str, list[LaneFormatMatrix]]:
        if not lanes:
            return {}
        result = await db.execute(
            select(LaneFormatMatrix)
            .where(LaneFormatMatrix.topic_lane_id.in_([lane.id for lane in lanes]))
            .where(LaneFormatMatrix.enabled.is_(True))
            .order_by(LaneFormatMatrix.weight.desc(), LaneFormatMatrix.created_at.asc())
        )
        grouped: dict[str, list[LaneFormatMatrix]] = {str(lane.id): [] for lane in lanes}
        for lane_format in result.scalars().all():
            grouped.setdefault(str(lane_format.topic_lane_id), []).append(lane_format)
        return grouped

    async def _build_tick_candidates(
        self,
        db: AsyncSession,
        *,
        channel: ChannelProfile,
        lanes: list[TopicLane],
        accounts: list[PublishingAccount],
        seeds: list[ManualSeed],
        lane_formats_by_lane: dict[str, list[LaneFormatMatrix]],
        bucket: str,
    ) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        lane_by_id = {str(lane.id): lane for lane in lanes}
        fallback_lane = lanes[0] if lanes else None
        claimed_account_ids: set[str] = set()
        manual_count_by_lane: dict[str, int] = {}

        for seed in seeds:
            lane = lane_by_id.get(str(seed.topic_lane_id)) if seed.topic_lane_id else fallback_lane
            lane_key = str(lane.id) if lane is not None else "unassigned"
            lane_formats = lane_formats_by_lane.get(lane_key, [])
            lane_format = lane_formats[0] if lane_formats else None
            account = await self._resolve_candidate_account(db, seed, accounts, claimed_account_ids)
            if account is not None:
                claimed_account_ids.add(str(account.id))
            manual_count_by_lane[lane_key] = manual_count_by_lane.get(lane_key, 0) + 1
            source_platforms = _string_list(seed.source_platforms_json) or _string_list(
                lane_format.source_platforms_json if lane_format else []
            )
            candidates.append(
                {
                    "candidate_id": _candidate_id(
                        "manual_seed",
                        lane.id if lane is not None else None,
                        lane_format.id if lane_format is not None else None,
                        bucket,
                        seed_id=seed.id,
                    ),
                    "source": "manual_seed",
                    "seed": seed,
                    "lane": lane,
                    "lane_format": lane_format,
                    "account": account,
                    "prompt": seed.prompt,
                    "title_seed": seed.title_seed,
                    "source_platforms_json": source_platforms,
                    "material_library_ids_json": _string_list(seed.material_library_ids_json),
                    "constraints_json": _dict_value(seed.constraints_json),
                }
            )

        for lane in lanes:
            lane_key = str(lane.id)
            lane_budget = max(_positive_int(lane.max_posts_per_day, default=1), 1)
            remaining = lane_budget - manual_count_by_lane.get(lane_key, 0)
            if remaining <= 0:
                continue

            generated = 0
            for lane_format in lane_formats_by_lane.get(lane_key, []):
                if generated >= remaining:
                    break
                account = await self._select_candidate_account_for_tick(
                    db,
                    accounts,
                    claimed_account_ids,
                    prefer_unblocked=True,
                )
                if account is not None:
                    claimed_account_ids.add(str(account.id))
                candidates.append(
                    {
                        "candidate_id": _candidate_id("lane_seed", lane.id, lane_format.id, bucket),
                        "source": "lane_seed",
                        "seed": None,
                        "lane": lane,
                        "lane_format": lane_format,
                        "account": account,
                        "prompt": build_lane_prompt(
                            lane_name=lane.name,
                            lane_description=lane.description,
                            keywords=_string_list(lane.keywords_json),
                            format_key=lane_format.format_key,
                            duration_sec=_positive_int(lane_format.target_duration_sec, default=30),
                            aspect_ratio=channel.default_aspect_ratio or "9:16",
                        ),
                        "title_seed": lane.name,
                        "source_platforms_json": self._lane_source_platforms(channel, lane_format),
                        "material_library_ids_json": [],
                        "constraints_json": {
                            "template_pool_json": _string_list(lane_format.template_pool_json),
                        },
                    }
                )
                generated += 1

        return candidates

    async def _resolve_candidate_account(
        self,
        db: AsyncSession,
        seed: ManualSeed,
        accounts: list[PublishingAccount],
        claimed_account_ids: set[str],
    ) -> PublishingAccount | None:
        if seed.target_account_id:
            account = await db.get(PublishingAccount, seed.target_account_id)
            if account is not None:
                return account
        return await self._select_candidate_account_for_tick(
            db,
            accounts,
            claimed_account_ids,
            prefer_unblocked=True,
        )

    async def _select_candidate_account_for_tick(
        self,
        db: AsyncSession,
        accounts: list[PublishingAccount],
        claimed_account_ids: set[str],
        *,
        prefer_unblocked: bool = False,
    ) -> PublishingAccount | None:
        if not accounts:
            return None
        unclaimed_accounts = [account for account in accounts if str(account.id) not in claimed_account_ids]
        candidate_accounts = unclaimed_accounts or accounts
        if prefer_unblocked:
            for account in candidate_accounts:
                if await self._active_task_count_for_account(db, account.id) <= 0:
                    return account
        return candidate_accounts[0]

    def _select_account_for_tick(
        self,
        accounts: list[PublishingAccount],
        claimed_account_ids: set[str],
    ) -> PublishingAccount | None:
        if not accounts:
            return None
        for account in accounts:
            if str(account.id) not in claimed_account_ids:
                return account
        return accounts[0]

    def _lane_source_platforms(
        self,
        channel: ChannelProfile,
        lane_format: LaneFormatMatrix,
    ) -> list[str]:
        return (
            _string_list(lane_format.source_platforms_json)
            or _string_list(_dict_value(channel.risk_policy_json).get("default_source_platforms"))
            or ["youtube"]
        )

    async def _evaluate_tick_candidates(
        self,
        db: AsyncSession,
        candidates: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        accepted: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        selected_account_counts: dict[str, int] = {}
        for candidate in candidates:
            rejection = await self._evaluate_candidate_guards(db, candidate, selected_account_counts)
            if rejection is not None:
                rejected.append(rejection)
                continue
            accepted.append(candidate)
            account_id = str(candidate["account"].id)
            selected_account_counts[account_id] = selected_account_counts.get(account_id, 0) + 1
        return accepted, rejected

    async def _evaluate_candidate_guards(
        self,
        db: AsyncSession,
        candidate: dict[str, Any],
        selected_account_counts: dict[str, int],
    ) -> dict[str, Any] | None:
        account = candidate.get("account")
        lane = candidate.get("lane")
        lane_format = candidate.get("lane_format")
        if account is None:
            return {
                "candidate_id": candidate["candidate_id"],
                "lane_id": str(lane.id) if lane is not None else "",
                "format_id": str(lane_format.id) if lane_format is not None else "",
                "account_id": "",
                "guard": "no_enabled_account",
                "reason": "No enabled publishing account is available for this candidate.",
            }

        account_id = str(account.id)
        active_count = await self._active_task_count_for_account(db, account.id)
        active_count += selected_account_counts.get(account_id, 0)
        if active_count <= 0:
            return None

        task_label = "task" if active_count == 1 else "tasks"
        return {
            "candidate_id": candidate["candidate_id"],
            "lane_id": str(lane.id) if lane is not None else "",
            "format_id": str(lane_format.id) if lane_format is not None else "",
            "account_id": account_id,
            "guard": "account_concurrency",
            "reason": f"Account {account_id} has {active_count} active {task_label}; account concurrency is limited to 1.",
        }

    async def _active_task_count_for_account(self, db: AsyncSession, account_id) -> int:
        result = await db.execute(
            select(func.count(ProductionTask.id))
            .where(ProductionTask.target_account_id == account_id)
            .where(ProductionTask.state.in_(_ACTIVE_TASK_STATES))
        )
        return int(result.scalar_one() or 0)

    def _task_from_candidate(
        self,
        channel: ChannelProfile,
        candidate: dict[str, Any],
        *,
        created_at: datetime,
    ) -> ProductionTask:
        account = candidate["account"]
        lane = candidate.get("lane")
        lane_format = candidate.get("lane_format")
        seed = candidate.get("seed")
        task = ProductionTask(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id if lane is not None else None,
            lane_format_id=lane_format.id if lane_format is not None else None,
            target_account_id=account.id,
            manual_seed_id=seed.id if seed is not None else None,
            source=candidate["source"],
            title_seed=candidate["title_seed"],
            prompt=candidate["prompt"],
            portfolio_bucket="explore",
            source_platforms_json=list(candidate["source_platforms_json"]),
            material_library_ids_json=list(candidate["material_library_ids_json"]),
            uses_external_assets=False,
            state=TASK_SELECTED,
            created_at=created_at,
            updated_at=created_at,
            state_updated_at=self.clock.now(),
            channel_config_version_snapshot=channel.config_version,
            channel_config_snapshot_json=_snapshot(
                channel,
                account,
                lane_format,
                lane=lane,
                manual_seed=seed,
            ),
            transition_history_json=[
                _transition("seeded", TASK_SELECTED, "agent_tick", self.clock.now()),
            ],
        )
        task.uses_external_assets = self._uses_external_assets(task)
        return task

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
        *,
        enqueue_alerts: bool = True,
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
                if enqueue_alerts:
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
    lane: TopicLane | None = None,
    manual_seed: ManualSeed | None = None,
) -> dict[str, Any]:
    lane_id = lane.id if lane is not None else (lane_format.topic_lane_id if lane_format else None)
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
            "id": str(lane_id) if lane_id else None,
            "name": lane.name if lane is not None else "",
            "description": lane.description if lane is not None else "",
            "keywords_json": _string_list(lane.keywords_json) if lane is not None else [],
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


def _per_lane_counts(lanes: list[TopicLane], candidates: list[dict[str, Any]]) -> dict[str, int]:
    per_lane = {str(lane.id): 0 for lane in lanes}
    for candidate in candidates:
        lane = candidate.get("lane")
        lane_id = str(lane.id) if lane is not None else "unassigned"
        per_lane[lane_id] = per_lane.get(lane_id, 0) + 1
    return per_lane


def _candidate_id(source: str, lane_id, format_id, bucket: str, *, seed_id=None) -> str:
    if source == "manual_seed" and seed_id is not None:
        return f"{source}:{seed_id}:lane:{lane_id or 'unassigned'}:format:{format_id or 'none'}:{bucket}"
    return f"{source}:lane:{lane_id or 'unassigned'}:format:{format_id or 'none'}:{bucket}"


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
