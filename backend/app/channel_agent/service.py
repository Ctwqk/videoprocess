from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, get_args

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.channel_agent.alerts import build_alert_payload
from app.channel_agent.clock import Clock
from app.channel_agent.candidate_scoring import score_candidate
from app.channel_agent.clients import (
    AutoFlowClient,
    FakeAutoFlowClient,
    FakeYouTubeClient,
    MiniMaxImageClient,
    MiniMaxClient,
    YouTubeClient,
)
from app.channel_agent.constants import (
    ACTIVE_TASK_STATES,
    ALERT_CONSECUTIVE_UPLOAD_FAILURE,
    ALERT_MATERIAL_SUPPLY_LOW,
    ALERT_QUOTA_LOW,
    ALERT_TAKEDOWN,
    ALERT_TOKEN_EXPIRING,
    TASK_FAILED,
    TASK_HELD,
    TASK_MEASURED,
    TASK_PLANNING,
    TASK_PRODUCING,
    TASK_REJECTED,
    TASK_SCHEDULED,
    TASK_SELECTED,
    TASK_UPLOADED_PRIVATE,
    TERMINAL_TASK_STATES,
    UPLOAD_FAILURE_KEYWORDS,
)
from app.channel_agent.lane_prompts import build_lane_prompt
from app.channel_agent.material_usage import (
    MaterialReference,
    UsageGuardResult,
    extract_material_references,
    recent_usage_flags,
)
from app.channel_agent.pds_health import should_enqueue_pds_outage_alert
from app.channel_agent.queue import ChannelOpsQueueService, utc_hour_bucket
from app.events.outbox import EventOutbox
from app.events.schemas import TOPIC_VP_ACTIONS, build_actor_action_event
from app.models.channel_agent import (
    AgentTickAudit,
    ChannelOpsQueueItem,
    ChannelProfile,
    FeedbackSnapshot,
    LaneFormatMatrix,
    ManualSeed,
    MaterialUsageLedger,
    ProductionTask,
    PublicationRecord,
    PublishingAccount,
    TakedownEvent,
    TopicLane,
)
from app.models.autoflow import AutoFlowPlan as AutoFlowPlanModel
from app.models.autoflow import AutoFlowRun as AutoFlowRunModel
from app.pds_client import (
    NoopPDSClient,
    PDSDecision,
    PDSDecisionRequest,
    PolicyDecisionClient,
)
from app.schemas.autoflow import AutoFlowRequest, PlanningMode, SourceStrategy


_SAFE_PRIVACY_VALUES = {"private", "unlisted"}
_SOURCE_STRATEGY_ALIASES = {"external_search": "external_research"}
_ALLOWED_SOURCE_STRATEGIES = set(get_args(SourceStrategy))
_ALLOWED_PLANNING_MODES = set(get_args(PlanningMode))
_PUBLICATION_CADENCE_STATUSES = {"public", "scheduled"}
_MAX_AUTOFLOW_OBSERVE_POLLS = 20
_MAX_METRICS_POLLS = 24
_METRICS_POLL_DELAY = timedelta(hours=1)
_RECOGNIZED_METRIC_KEYS = {
    "views",
    "likes",
    "comments",
    "shares",
    "avg_view_duration_sec",
    "retention_curve_json",
    "retention_curve",
    "ctr",
    "impressions",
    "virality_score",
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
        pds_client: PolicyDecisionClient | None = None,
        event_outbox: EventOutbox | None = None,
        pds_health_monitor_enabled: bool = False,
    ) -> None:
        self.clock = clock or Clock()
        self.queue = queue or ChannelOpsQueueService(clock=self.clock)
        self.autoflow_client = autoflow_client or FakeAutoFlowClient()
        self.youtube_client = youtube_client or FakeYouTubeClient()
        self.minimax_client = minimax_client or MiniMaxImageClient()
        self.pds_client = pds_client or NoopPDSClient()
        self.event_outbox = event_outbox or EventOutbox()
        self.pds_health_monitor_enabled = pds_health_monitor_enabled
        self._pds_last_success_at: datetime | None = None
        self._pds_last_alert_bucket: str | None = None

    async def tick(
        self,
        db: AsyncSession,
        *,
        channel_id,
        plan_delay_seconds: int = 0,
    ) -> AgentTickAudit:
        if (
            isinstance(plan_delay_seconds, bool)
            or not isinstance(plan_delay_seconds, int)
            or not 0 <= plan_delay_seconds <= 3_600
        ):
            raise ValueError("plan_delay_seconds must be an integer from 0 through 3600")
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
                .where(or_(TopicLane.paused_until.is_(None), TopicLane.paused_until <= now))
                .order_by(TopicLane.weight.desc(), TopicLane.created_at.asc())
            )
        ).scalars().all()
        accounts = (
            await db.execute(
                select(PublishingAccount)
                .where(PublishingAccount.channel_profile_id == channel.id)
                .where(PublishingAccount.enabled.is_(True))
                .where(or_(PublishingAccount.paused_until.is_(None), PublishingAccount.paused_until <= now))
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
        manual_candidates = await self._build_tick_candidates(
            db,
            channel=channel,
            lanes=lanes,
            accounts=accounts,
            seeds=seeds,
            lane_formats_by_lane=lane_formats_by_lane,
            bucket=bucket,
        )
        side_effects_disabled = bool(channel.dry_run or channel.halted_at is not None)
        (
            accepted_manual_candidates,
            rejected_manual_candidates,
            selected_account_counts,
            selected_lane_counts,
        ) = await self._evaluate_tick_candidates(
            db,
            manual_candidates,
            enqueue_alerts=not side_effects_disabled,
            pds_enabled=not side_effects_disabled,
        )
        lane_candidates = await self._build_lane_driven_candidates(
            db,
            channel=channel,
            lanes=lanes,
            accounts=accounts,
            lane_formats_by_lane=lane_formats_by_lane,
            bucket=bucket,
            selected_lane_counts=selected_lane_counts,
            claimed_account_ids=set(selected_account_counts),
            suppressed_lane_ids=_suppressed_lane_ids_from_manual_rejections(rejected_manual_candidates),
        )
        (
            accepted_lane_candidates,
            rejected_lane_candidates,
            _selected_account_counts,
            _selected_lane_counts,
        ) = await self._evaluate_tick_candidates(
            db,
            lane_candidates,
            enqueue_alerts=not side_effects_disabled,
            pds_enabled=not side_effects_disabled,
            initial_account_counts=selected_account_counts,
            initial_lane_counts=selected_lane_counts,
        )
        candidates = [*manual_candidates, *lane_candidates]
        accepted_candidates = [*accepted_manual_candidates, *accepted_lane_candidates]
        rejected_candidates = [*rejected_manual_candidates, *rejected_lane_candidates]
        per_lane = _per_lane_counts(lanes, candidates)
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
                run_after=now + timedelta(seconds=plan_delay_seconds),
                channel_profile_id=channel.id,
                commit=False,
            )
            await self._emit_candidate_accepted(db, candidate, task)
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

        for seed in seeds:
            explicit_lane_id = str(seed.topic_lane_id) if seed.topic_lane_id else None
            lane = lane_by_id.get(explicit_lane_id) if explicit_lane_id else fallback_lane
            pre_rejection = None
            if explicit_lane_id and lane is None:
                pre_rejection = {
                    "guard": "lane_unavailable",
                    "reason": f"Target topic lane {explicit_lane_id} is disabled, paused, or unavailable.",
                    "lane_id": explicit_lane_id,
                }
            lane_key = str(lane.id) if lane is not None else "unassigned"
            lane_formats = lane_formats_by_lane.get(lane_key, [])
            lane_format = lane_formats[0] if lane_formats else None
            account, account_rejection = await self._resolve_candidate_account(db, seed, accounts, claimed_account_ids)
            if account is not None and pre_rejection is None:
                claimed_account_ids.add(str(account.id))
            source_platforms = _string_list(seed.source_platforms_json) or _string_list(
                lane_format.source_platforms_json if lane_format else []
            )
            candidates.append(
                {
                    "candidate_id": _candidate_id(
                        "manual_seed",
                        lane.id if lane is not None else explicit_lane_id,
                        lane_format.id if lane_format is not None else None,
                        bucket,
                        seed_id=seed.id,
                    ),
                    "source": "manual_seed",
                    "seed": seed,
                    "lane": lane,
                    "lane_format": lane_format,
                    "account": account,
                    "account_rejection": account_rejection,
                    "pre_rejection": pre_rejection,
                    "prompt": seed.prompt,
                    "title_seed": seed.title_seed,
                    "source_platforms_json": source_platforms,
                    "material_library_ids_json": _string_list(seed.material_library_ids_json),
                    "constraints_json": _dict_value(seed.constraints_json),
                }
            )

        return candidates

    async def _build_lane_driven_candidates(
        self,
        db: AsyncSession,
        *,
        channel: ChannelProfile,
        lanes: list[TopicLane],
        accounts: list[PublishingAccount],
        lane_formats_by_lane: dict[str, list[LaneFormatMatrix]],
        bucket: str,
        selected_lane_counts: dict[str, int],
        claimed_account_ids: set[str],
        suppressed_lane_ids: set[str],
    ) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        for lane in lanes:
            lane_key = str(lane.id)
            if lane_key in suppressed_lane_ids:
                continue
            lane_budget = max(_positive_int(lane.max_posts_per_day, default=1), 1)
            remaining = lane_budget - selected_lane_counts.get(lane_key, 0)
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
    ) -> tuple[PublishingAccount | None, dict[str, str] | None]:
        if seed.target_account_id:
            account = await db.get(PublishingAccount, seed.target_account_id)
            if account is None:
                return None, {
                    "guard": "account_unavailable",
                    "reason": f"Target publishing account {seed.target_account_id} was not found.",
                    "account_id": str(seed.target_account_id),
                }
            if account.channel_profile_id != seed.channel_profile_id:
                return None, {
                    "guard": "account_unavailable",
                    "reason": f"Target publishing account {account.id} does not belong to channel {seed.channel_profile_id}.",
                    "account_id": str(account.id),
                }
            if not self._account_available(account):
                return None, {
                    "guard": "account_unavailable",
                    "reason": f"Target publishing account {account.id} is disabled or paused.",
                    "account_id": str(account.id),
                }
            return account, None
        return await self._select_candidate_account_for_tick(
            db,
            accounts,
            claimed_account_ids,
            prefer_unblocked=True,
        ), None

    async def _select_candidate_account_for_tick(
        self,
        db: AsyncSession,
        accounts: list[PublishingAccount],
        claimed_account_ids: set[str],
        *,
        prefer_unblocked: bool = False,
    ) -> PublishingAccount | None:
        available_accounts = [account for account in accounts if self._account_available(account)]
        if not available_accounts:
            return None
        unclaimed_accounts = [account for account in available_accounts if str(account.id) not in claimed_account_ids]
        candidate_accounts = unclaimed_accounts or available_accounts
        if prefer_unblocked:
            for account in candidate_accounts:
                if await self._active_task_count_for_account(db, account.id) <= 0:
                    return account
        return candidate_accounts[0]

    def _account_available(self, account: PublishingAccount) -> bool:
        if not account.enabled:
            return False
        if account.paused_until is None:
            return True
        return _datetime_to_utc(account.paused_until) <= _datetime_to_utc(self.clock.now())

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
        *,
        enqueue_alerts: bool,
        pds_enabled: bool,
        initial_account_counts: dict[str, int] | None = None,
        initial_lane_counts: dict[str, int] | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int], dict[str, int]]:
        accepted: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        selected_account_counts: dict[str, int] = dict(initial_account_counts or {})
        selected_lane_counts: dict[str, int] = dict(initial_lane_counts or {})
        for candidate in candidates:
            rejection = await self._evaluate_candidate_guards(
                db,
                candidate,
                selected_account_counts,
                selected_lane_counts,
                enqueue_alerts=enqueue_alerts,
                pds_enabled=pds_enabled,
            )
            if rejection is not None:
                rejected.append(rejection)
                continue
            accepted.append(candidate)
            account_id = str(candidate["account"].id)
            selected_account_counts[account_id] = selected_account_counts.get(account_id, 0) + 1
            lane = candidate.get("lane")
            if lane is not None:
                lane_id = str(lane.id)
                selected_lane_counts[lane_id] = selected_lane_counts.get(lane_id, 0) + 1
        return accepted, rejected, selected_account_counts, selected_lane_counts

    async def _evaluate_candidate_guards(
        self,
        db: AsyncSession,
        candidate: dict[str, Any],
        selected_account_counts: dict[str, int],
        selected_lane_counts: dict[str, int],
        *,
        enqueue_alerts: bool,
        pds_enabled: bool,
    ) -> dict[str, Any] | None:
        pre_rejection = candidate.get("pre_rejection")
        if isinstance(pre_rejection, dict):
            return _candidate_rejection(
                candidate,
                guard=str(pre_rejection.get("guard") or "candidate_unavailable"),
                reason=str(pre_rejection.get("reason") or "Candidate is unavailable."),
            )

        account = candidate.get("account")
        if account is None:
            account_rejection = candidate.get("account_rejection")
            if isinstance(account_rejection, dict):
                return _candidate_rejection(
                    candidate,
                    guard=str(account_rejection.get("guard") or "account_unavailable"),
                    reason=str(account_rejection.get("reason") or "Target publishing account is unavailable."),
                )
            return _candidate_rejection(
                candidate,
                guard="no_enabled_account",
                reason="No enabled publishing account is available for this candidate.",
            )

        account_id = str(account.id)
        active_count = await self._active_task_count_for_account(db, account.id)
        active_count += selected_account_counts.get(account_id, 0)
        if active_count > 0:
            task_label = "task" if active_count == 1 else "tasks"
            return _candidate_rejection(
                candidate,
                guard="account_concurrency",
                reason=(
                    f"Account {account_id} has {active_count} active {task_label}; "
                    "account concurrency is limited to 1."
                ),
            )

        upload_failure_rejection = await self._consecutive_upload_failure_guard(
            db,
            candidate,
            enqueue_alerts=enqueue_alerts,
        )
        if upload_failure_rejection is not None:
            return upload_failure_rejection

        lane = candidate.get("lane")
        accepted_lane_count = (
            selected_lane_counts.get(str(lane.id), 0) if lane is not None else 0
        )
        lane_cadence_rejection = await self._lane_cadence_guard(
            db,
            candidate,
            accepted_lane_count=accepted_lane_count,
        )
        if lane_cadence_rejection is not None:
            return lane_cadence_rejection

        usage_rejection = await self._material_usage_guard(db, candidate)
        if usage_rejection is not None:
            return usage_rejection

        if pds_enabled:
            return await self._pds_candidate_guard(
                db,
                candidate,
                emit_outbox=enqueue_alerts,
            )
        return None

    async def _material_usage_guard(
        self,
        db: AsyncSession,
        candidate: dict[str, Any],
    ) -> dict[str, Any] | None:
        references = _candidate_material_references(candidate)
        if not references:
            return None
        account = candidate.get("account")
        lane = candidate.get("lane")
        if account is None:
            return None
        result = await recent_usage_flags(
            db,
            channel_id=str(account.channel_profile_id),
            lane_id=str(lane.id) if lane is not None else None,
            account_id=str(account.id),
            references=references,
            now=self.clock.now(),
        )
        if not result.blocked:
            return None
        candidate["_material_usage_guard"] = result
        if candidate.get("source") == "manual_seed":
            return None
        guard = "cross_account_rejected" if result.cross_account_rejected else "repetition_rejected"
        return _candidate_rejection(
            candidate,
            guard=guard,
            reason=f"Material usage guard rejected candidate: {guard}",
        )

    async def _pds_candidate_guard(
        self,
        db: AsyncSession,
        candidate: dict[str, Any],
        *,
        emit_outbox: bool,
    ) -> dict[str, Any] | None:
        account = candidate.get("account")
        if account is None:
            return None

        lane = candidate.get("lane")
        decision = await self._decide_pds(
            db,
            PDSDecisionRequest(
                actor_id=str(account.id),
                action_type="candidate_accept",
                platform=_account_platform(account),
                content={
                    "title": str(
                        candidate.get("title")
                        or candidate.get("title_seed")
                        or candidate.get("prompt")
                        or ""
                    ),
                    "description": str(candidate.get("description") or candidate.get("prompt") or ""),
                },
                context={
                    "lane_id": str(lane.id) if lane is not None else "",
                    "candidate_id": str(candidate.get("candidate_id") or ""),
                },
            ),
        )
        candidate["_pds_decision"] = decision
        if decision.verdict not in {"block", "flag"}:
            return None

        marker = "pds_blocked" if decision.verdict == "block" else "pds_flagged_for_review"
        if emit_outbox:
            await self._emit_actor_action_event(
                db,
                actor_id=str(account.id),
                action_type="candidate_blocked" if decision.verdict == "block" else "candidate_flagged",
                platform=_account_platform(account),
                metadata={
                    "lane_id": str(lane.id) if lane is not None else "",
                    "candidate_id": str(candidate.get("candidate_id") or ""),
                    "guard": marker,
                    **_pds_decision_event_metadata(decision),
                },
            )
        return _candidate_rejection(
            candidate,
            guard=marker,
            reason=f"PDS {decision.verdict} candidate: {decision.decision_id}",
        )

    async def _active_task_count_for_account(self, db: AsyncSession, account_id) -> int:
        result = await db.execute(
            select(func.count(ProductionTask.id))
            .where(ProductionTask.target_account_id == account_id)
            .where(ProductionTask.state.in_(ACTIVE_TASK_STATES))
        )
        return int(result.scalar_one() or 0)

    async def _consecutive_upload_failure_guard(
        self,
        db: AsyncSession,
        candidate: dict[str, Any],
        *,
        enqueue_alerts: bool,
    ) -> dict[str, Any] | None:
        account = candidate.get("account")
        if account is None:
            return None

        recent_tasks = (
            await db.execute(
                select(ProductionTask)
                .where(ProductionTask.target_account_id == account.id)
                .where(ProductionTask.state.in_(TERMINAL_TASK_STATES))
                .order_by(ProductionTask.created_at.desc())
                .limit(5)
            )
        ).scalars().all()
        created_times = [_datetime_to_utc(task.created_at) for task in recent_tasks if task.created_at is not None]
        if not created_times:
            return None

        oldest_considered = min(created_times)
        if oldest_considered < self.clock.now() - timedelta(hours=24):
            return None

        failed_upload_tasks = [task for task in recent_tasks if _is_upload_failure_task(task)]
        if len(failed_upload_tasks) < 3:
            return None

        if enqueue_alerts:
            await self._enqueue_consecutive_upload_failure_alert(db, account, failed_upload_tasks)
        return _candidate_rejection(
            candidate,
            guard="consecutive_upload_failure",
            reason=(
                f"Account {account.id} has {len(failed_upload_tasks)} upload-like failures "
                "in the recent task window."
            ),
        )

    async def _enqueue_consecutive_upload_failure_alert(
        self,
        db: AsyncSession,
        account: PublishingAccount,
        failed_tasks: list[ProductionTask],
    ) -> None:
        channel_id = str(account.channel_profile_id)
        account_id = str(account.id)
        await self._enqueue_alert(
            db,
            ALERT_CONSECUTIVE_UPLOAD_FAILURE,
            resource_id=account_id,
            severity="critical",
            message=(
                f"Account {account_id} has repeated upload/publish failures; pause the account with "
                f"POST /api/v1/channel-agent/accounts/{account_id}/pause, then inspect failed tasks at "
                f"/api/v1/channel-agent/channels/{channel_id}/tasks?account_id={account_id}&state=failed."
            ),
            details={
                "account_id": account_id,
                "failed_task_ids": [str(task.id) for task in failed_tasks],
                "failure_reasons": [task.failure_reason or "" for task in failed_tasks],
            },
            channel_profile_id=account.channel_profile_id,
        )

    async def _lane_cadence_guard(
        self,
        db: AsyncSession,
        candidate: dict[str, Any],
        *,
        accepted_lane_count: int = 0,
    ) -> dict[str, Any] | None:
        lane = candidate.get("lane")
        if lane is None:
            return None

        publications = await self._published_publications_for_lane(db, lane)
        now = self.clock.now()
        max_posts_per_day = int(lane.max_posts_per_day or 0)
        if max_posts_per_day > 0:
            cutoff = now - timedelta(hours=24)
            recent_count = sum(
                1 for _publication, effective_at in publications if cutoff <= effective_at <= now
            )
            effective_count = recent_count + accepted_lane_count
            if effective_count >= max_posts_per_day:
                return _candidate_rejection(
                    candidate,
                    guard="lane_cadence",
                    reason=(
                        f"Lane {lane.id} has {effective_count} scheduled/public publications or "
                        "same-tick reservations in the past 24 hours; "
                        f"max_posts_per_day is {max_posts_per_day}."
                    ),
                )

        cooldown_minutes = int(lane.cooldown_after_post_minutes or 0)
        if cooldown_minutes > 0 and publications:
            latest_effective_at = publications[0][1]
            cooldown = timedelta(minutes=cooldown_minutes)
            # Future scheduled records reserve cooldown/streak slots.
            # Daily cap stays a past-24h lookback.
            if now - latest_effective_at < cooldown:
                return _candidate_rejection(
                    candidate,
                    guard="lane_cadence",
                    reason=(
                        f"Lane {lane.id} is inside the {cooldown_minutes} minute post cooldown "
                        f"after {latest_effective_at.isoformat()}."
                    ),
                )

        max_streak = int(lane.max_consecutive_streak or 0)
        if max_streak > 0:
            current_streak = await self._current_lane_publication_streak(db, lane)
            if current_streak >= max_streak:
                return _candidate_rejection(
                    candidate,
                    guard="lane_cadence",
                    reason=(
                        f"Lane {lane.id} has a publication streak of {current_streak}; "
                        f"max_consecutive_streak is {max_streak}."
                    ),
                )

        return None

    async def _published_publications_for_lane(
        self,
        db: AsyncSession,
        lane: TopicLane,
    ) -> list[tuple[PublicationRecord, datetime]]:
        result = await db.execute(
            select(PublicationRecord, ProductionTask)
            .join(ProductionTask, PublicationRecord.production_task_id == ProductionTask.id)
            .where(ProductionTask.topic_lane_id == lane.id)
            .where(PublicationRecord.publish_status.in_(_PUBLICATION_CADENCE_STATUSES))
        )
        publications: list[tuple[PublicationRecord, datetime]] = []
        for publication, _task in result.all():
            effective_at = _publication_effective_time(publication)
            if effective_at is not None:
                publications.append((publication, effective_at))
        return sorted(publications, key=lambda item: item[1], reverse=True)

    async def _current_lane_publication_streak(self, db: AsyncSession, lane: TopicLane) -> int:
        result = await db.execute(
            select(PublicationRecord, ProductionTask)
            .join(ProductionTask, PublicationRecord.production_task_id == ProductionTask.id)
            .where(ProductionTask.channel_profile_id == lane.channel_profile_id)
            .where(PublicationRecord.publish_status.in_(_PUBLICATION_CADENCE_STATUSES))
        )
        rows: list[tuple[ProductionTask, datetime]] = []
        for publication, task in result.all():
            effective_at = _publication_effective_time(publication)
            if effective_at is not None:
                rows.append((task, effective_at))
        streak = 0
        for task, _effective_at in sorted(rows, key=lambda item: item[1], reverse=True):
            if task.topic_lane_id != lane.id:
                break
            streak += 1
        return streak

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
        created_timestamp = _naive_utc(created_at)
        rationale = {}
        usage_guard = candidate.get("_material_usage_guard")
        if isinstance(usage_guard, UsageGuardResult):
            rationale["material_usage_guard"] = {
                "manual_override": candidate.get("source") == "manual_seed",
                "repetition_rejected": usage_guard.repetition_rejected,
                "cross_account_rejected": usage_guard.cross_account_rejected,
                "hits": list(usage_guard.hits),
            }
        task = ProductionTask(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id if lane is not None else None,
            lane_format_id=lane_format.id if lane_format is not None else None,
            target_account_id=account.id,
            manual_seed_id=seed.id if seed is not None else None,
            source=candidate["source"],
            title_seed=candidate["title_seed"],
            prompt=candidate["prompt"],
            rationale_json=rationale,
            score_breakdown_json=score_candidate(candidate, now=self.clock.now()),
            portfolio_bucket="explore",
            source_platforms_json=list(candidate["source_platforms_json"]),
            material_library_ids_json=list(candidate["material_library_ids_json"]),
            uses_external_assets=False,
            approval_mode="human" if candidate["source"] == "manual_seed" else "agent",
            agent_approval_evidence_json={},
            state=TASK_SELECTED,
            created_at=created_timestamp,
            updated_at=created_timestamp,
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
        if task.approval_mode == "agent":
            account = await db.get(PublishingAccount, task.target_account_id)
            decision = await self._decide_pds(
                db,
                PDSDecisionRequest(
                    actor_id=str(task.target_account_id),
                    action_type="plan_approval",
                    platform=_account_platform(account) if account is not None else "youtube",
                    content={
                        "title": task.title_seed,
                        "description": task.prompt,
                    },
                    context={
                        "production_task_id": str(task.id),
                        "autoflow_plan_id": str(task.autoflow_plan_id),
                    },
                ),
            )
            evidence = _pds_decision_event_metadata(decision)
            if decision.verdict == "allow":
                await self.autoflow_client.approve_plan(
                    str(task.autoflow_plan_id),
                    approved_by="channel_agent",
                    evidence=evidence,
                )
                task.agent_approval_evidence_json = evidence
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

    async def handle_execute_task(self, db: AsyncSession, item: ChannelOpsQueueItem) -> ProductionTask:
        task = await self._task_from_item(db, item)
        if task.autoflow_run_id and task.job_id:
            run_id = str(task.autoflow_run_id)
            job_id = str(task.job_id)
            previous_state = task.state
            if task.state not in TERMINAL_TASK_STATES and task.state != TASK_HELD:
                task.state = TASK_PRODUCING
                task.state_updated_at = self.clock.now()
                if previous_state != TASK_PRODUCING:
                    task.transition_history_json = [
                        *list(task.transition_history_json or []),
                        _transition(previous_state, TASK_PRODUCING, "execute_task", self.clock.now()),
                    ]
            await self.queue.enqueue(
                db,
                kind="observe_job",
                idempotency_key=f"observe_job:{task.id}:{run_id}:{job_id}:0",
                payload={
                    "production_task_id": str(task.id),
                    "run_id": run_id,
                    "job_id": job_id,
                    "observe_count": 0,
                },
                priority=65,
                channel_profile_id=task.channel_profile_id,
                parent_queue_item_id=item.id,
            )
            await db.commit()
            await db.refresh(task)
            return task

        observation = await self.autoflow_client.execute_task(task, self._autoflow_request(task))
        if _status_value(observation.status) == "failed":
            previous_state = task.state
            task.state = TASK_FAILED
            task.failure_reason = observation.error_message or "AutoFlow execution failed"
            task.state_updated_at = self.clock.now()
            task.transition_history_json = [
                *list(task.transition_history_json or []),
                _transition(previous_state, TASK_FAILED, "execute_task", self.clock.now()),
            ]
            await db.commit()
            await db.refresh(task)
            return task

        run_id = _uuid_or_none(observation.run_id)
        job_id = _uuid_or_none(observation.job_id)
        pipeline_id = _uuid_or_none(observation.pipeline_id) if observation.pipeline_id else None
        validation_error = None
        if run_id is None:
            validation_error = "AutoFlow execution observation missing valid run_id"
        elif job_id is None:
            validation_error = "AutoFlow execution observation missing valid job_id"
        elif observation.pipeline_id and pipeline_id is None:
            validation_error = "AutoFlow execution observation contains invalid pipeline_id"
        if validation_error is not None:
            previous_state = task.state
            task.state = TASK_FAILED
            task.failure_reason = validation_error
            task.state_updated_at = self.clock.now()
            task.transition_history_json = [
                *list(task.transition_history_json or []),
                _transition(previous_state, TASK_FAILED, "execute_task", self.clock.now()),
            ]
            await db.commit()
            await db.refresh(task)
            return task

        task.autoflow_run_id = run_id
        task.job_id = job_id
        if pipeline_id:
            task.pipeline_id = pipeline_id
        previous_state = task.state
        task.state = TASK_PRODUCING
        task.state_updated_at = self.clock.now()
        task.transition_history_json = [
            *list(task.transition_history_json or []),
            _transition(previous_state, TASK_PRODUCING, "execute_task", self.clock.now()),
        ]
        await self.queue.enqueue(
            db,
            kind="observe_job",
            idempotency_key=f"observe_job:{task.id}:{run_id}:{job_id}:0",
            payload={
                "production_task_id": str(task.id),
                "run_id": str(run_id),
                "job_id": str(job_id),
                "observe_count": 0,
            },
            priority=65,
            channel_profile_id=task.channel_profile_id,
            parent_queue_item_id=item.id,
        )
        await db.commit()
        await db.refresh(task)
        return task

    async def handle_observe_job(self, db: AsyncSession, item: ChannelOpsQueueItem) -> ProductionTask:
        task = await self._task_from_item(db, item)
        payload = dict(item.payload_json or {})
        run_id = str(payload.get("run_id") or task.autoflow_run_id or "")
        job_id = str(payload.get("job_id") or task.job_id or "")
        observe_count = _nonnegative_int(payload.get("observe_count"), default=0)
        if observe_count >= _MAX_AUTOFLOW_OBSERVE_POLLS:
            previous_state = task.state
            task.state = TASK_HELD
            task.blocked_by_guard = "autoflow_observe_timeout"
            task.failure_reason = "AutoFlow job observation timed out"
            task.state_updated_at = self.clock.now()
            task.transition_history_json = [
                *list(task.transition_history_json or []),
                _transition(previous_state, TASK_HELD, "observe_job", self.clock.now()),
            ]
            await db.commit()
            await db.refresh(task)
            return task

        observation = await self.autoflow_client.observe_job(db, run_id=run_id, job_id=job_id)
        status = _status_value(observation.status)

        if status in {"pending", "running", "queued", "waiting_window", "validating", "planning"}:
            next_count = observe_count + 1
            delay_seconds = min(30 * 2 ** observe_count, 300)
            await self.queue.enqueue(
                db,
                kind="observe_job",
                idempotency_key=f"observe_job:{task.id}:{run_id}:{job_id}:{next_count}",
                payload={
                    "production_task_id": str(task.id),
                    "run_id": run_id,
                    "job_id": job_id,
                    "observe_count": next_count,
                },
                priority=65,
                run_after=self.clock.now() + timedelta(seconds=delay_seconds),
                channel_profile_id=task.channel_profile_id,
                parent_queue_item_id=item.id,
            )
            await db.commit()
            await db.refresh(task)
            return task

        if status == "failed":
            previous_state = task.state
            task.state = TASK_FAILED
            task.failure_reason = observation.error_message or "AutoFlow job failed"
            task.state_updated_at = self.clock.now()
            task.transition_history_json = [
                *list(task.transition_history_json or []),
                _transition(previous_state, TASK_FAILED, "observe_job", self.clock.now()),
            ]
            await db.commit()
            await db.refresh(task)
            return task

        youtube = dict(observation.youtube or {})
        video_id = str(youtube.get("video_id") or "").strip()
        if not video_id:
            previous_state = task.state
            task.state = TASK_HELD
            task.blocked_by_guard = "missing_youtube_observation"
            task.failure_reason = "AutoFlow job succeeded without a YouTube video id observation"
            task.state_updated_at = self.clock.now()
            task.transition_history_json = [
                *list(task.transition_history_json or []),
                _transition(previous_state, TASK_HELD, "observe_job", self.clock.now()),
            ]
            await db.commit()
            await db.refresh(task)
            return task

        await self.queue.enqueue(
            db,
            kind="publish_task",
            idempotency_key=f"publish_task:{task.id}",
            payload={"production_task_id": str(task.id), "youtube": youtube},
            priority=66,
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
            material_references = await self._material_references_for_publish(
                db,
                task=task,
                upload_metadata=youtube,
            )
            blocked_guard = await self._publish_time_material_usage_guard(
                db,
                task=task,
                references=material_references,
            )
            if blocked_guard is not None:
                await db.commit()
                await db.refresh(task)
                return None
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
        else:
            material_references = await self._material_references_for_publish(
                db,
                task=task,
                upload_metadata=youtube,
            )
        await self._write_material_usage_ledger(
            db,
            task=task,
            publication=publication,
            upload_metadata=youtube,
            references=material_references,
        )

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
        task = await db.get(ProductionTask, publication.production_task_id)
        if publication.publish_status == "rejected" or (task is not None and task.state == TASK_REJECTED):
            return publication
        if publication.publish_status != "uploaded" or task is None or task.state not in {
            TASK_UPLOADED_PRIVATE,
            TASK_HELD,
        }:
            raise ValueError("Publication is not ready for promotion")
        scheduled_at = _parse_datetime(str(item.payload_json.get("scheduled_at") or self.clock.now().isoformat()))
        visibility = _safe_privacy(item.payload_json.get("target_visibility") or publication.desired_privacy) or "unlisted"
        promotion_metadata = {
            "task_id": str(task.id),
            "publication_id": str(publication.id),
            "target_visibility": visibility,
            "scheduled_at": scheduled_at.isoformat(),
        }
        await self._emit_actor_action_event(
            db,
            actor_id=str(publication.account_id),
            action_type="publication_promotion_attempted",
            platform=str(publication.platform or "youtube"),
            metadata=promotion_metadata,
        )
        decision = await self._decide_pds(
            db,
            PDSDecisionRequest(
                actor_id=str(publication.account_id),
                action_type="publish",
                platform=str(publication.platform or "youtube"),
                content={
                    "title": publication.title,
                    "description": publication.description or "",
                },
                context={
                    "publication_id": str(publication.id),
                    "task_id": str(task.id),
                    "target_visibility": visibility,
                },
            ),
        )
        if decision.verdict in {"block", "flag"}:
            publication.publish_status = "held"
            previous_state = task.state
            task.state = TASK_HELD
            task.state_updated_at = self.clock.now()
            task.transition_history_json = [
                *list(task.transition_history_json or []),
                _transition(previous_state, TASK_HELD, "pds_gate", self.clock.now()),
            ]
            marker = "pds_blocked" if decision.verdict == "block" else "pds_flagged_for_review"
            publication.warnings_json = [
                *list(publication.warnings_json or []),
                f"{marker}:{decision.decision_id}",
            ]
            await self._emit_actor_action_event(
                db,
                actor_id=str(publication.account_id),
                action_type="publication_promotion_blocked",
                platform=str(publication.platform or "youtube"),
                metadata={
                    **promotion_metadata,
                    "guard": marker,
                    **_pds_decision_event_metadata(decision),
                },
            )
            await db.commit()
            await db.refresh(publication)
            return publication

        await self.youtube_client.schedule_publish(
            video_id=publication.platform_content_id,
            scheduled_at=scheduled_at,
            privacy=visibility,
        )
        publication.publish_status = "scheduled"
        publication.desired_privacy = visibility
        publication.scheduled_publish_at = scheduled_at
        previous_state = task.state
        task.state = TASK_SCHEDULED
        task.state_updated_at = self.clock.now()
        task.transition_history_json = [
            *list(task.transition_history_json or []),
            _transition(previous_state, TASK_SCHEDULED, "promote_publication", self.clock.now()),
        ]
        await self._emit_actor_action_event(
            db,
            actor_id=str(publication.account_id),
            action_type="publication_scheduled",
            platform=str(publication.platform or "youtube"),
            metadata={
                **promotion_metadata,
                **_pds_decision_event_metadata(decision),
            },
        )
        await self.queue.enqueue(
            db,
            kind="collect_metrics",
            idempotency_key=f"collect_metrics:{publication.id}:poll:0",
            payload={"publication_id": str(publication.id), "metrics_poll_count": 0},
            priority=90,
            run_after=scheduled_at + _METRICS_POLL_DELAY,
            parent_queue_item_id=item.id,
            channel_profile_id=task.channel_profile_id,
        )
        await self.queue.enqueue(
            db,
            kind="reconcile_publication",
            idempotency_key=f"reconcile_publication:{publication.id}:{scheduled_at.isoformat()}",
            payload={"publication_id": str(publication.id)},
            priority=80,
            run_after=scheduled_at + timedelta(minutes=30),
            parent_queue_item_id=item.id,
            channel_profile_id=task.channel_profile_id,
        )
        await db.commit()
        await db.refresh(publication)
        return publication

    async def handle_reconcile_publication(
        self,
        db: AsyncSession,
        item: ChannelOpsQueueItem,
    ) -> PublicationRecord:
        publication_id = _uuid(item.payload_json["publication_id"])
        publication = await db.get(PublicationRecord, publication_id)
        if publication is None:
            raise ValueError("Publication not found")

        status = await self.youtube_client.fetch_status(video_id=publication.platform_content_id)
        privacy = str(status.get("privacy") or status.get("current_privacy") or "").strip().lower()
        if privacy:
            publication.current_privacy = privacy

        publish_status = str(
            status.get("publish_status")
            or status.get("processing_state")
            or status.get("upload_status")
            or publication.publish_status
        ).strip().lower()
        if publish_status:
            publication.publish_status = publish_status

        permalink = str(status.get("permalink") or status.get("url") or "").strip()
        if permalink:
            publication.permalink = permalink

        error_message = str(status.get("error_message") or status.get("failure_reason") or "").strip()
        task = await db.get(ProductionTask, publication.production_task_id)
        severe_states = {"rejected", "removed", "failed", "claim", "claimed", "blocked"}
        if error_message:
            publication.warnings_json = [*list(publication.warnings_json or []), f"platform_status:{error_message}"]
            if task is not None:
                task.failure_reason = error_message
        if publish_status in severe_states:
            publication.publish_status = "removed" if publish_status in {"removed", "claim", "claimed"} else "rejected"
            if task is not None:
                previous_state = task.state
                task.state = TASK_HELD
                task.blocked_by_guard = "platform_rejected"
                task.failure_reason = error_message or f"YouTube reported {publish_status}"
                task.state_updated_at = self.clock.now()
                task.transition_history_json = [
                    *list(task.transition_history_json or []),
                    _transition(previous_state, TASK_HELD, "reconcile_publication", self.clock.now()),
                ]
            db.add(
                TakedownEvent(
                    publication_id=publication.id,
                    event_type=publish_status,
                    severity="severe",
                    raw_payload_json=dict(status),
                )
            )

        await db.commit()
        await db.refresh(publication)
        return publication

    async def handle_collect_metrics(self, db: AsyncSession, item: ChannelOpsQueueItem) -> FeedbackSnapshot | None:
        publication_id = _uuid(item.payload_json["publication_id"])
        publication = await db.get(PublicationRecord, publication_id)
        if publication is None:
            raise ValueError("Publication not found")

        payload = dict(item.payload_json or {})
        metrics = _dict_value(payload.get("metrics"))
        task = await db.get(ProductionTask, publication.production_task_id)
        if publication.publish_status == "rejected" or (task is not None and task.state == TASK_REJECTED):
            return None
        if not _has_real_metrics(metrics) and publication.platform_content_id:
            try:
                fetched_metrics = await self.youtube_client.fetch_metrics(video_id=publication.platform_content_id)
            except Exception:
                fetched_metrics = {}
            if _has_real_metrics(fetched_metrics):
                metrics = fetched_metrics
                payload["metrics"] = metrics
                payload["metrics_source"] = "youtube_manager"
        if not _has_real_metrics(metrics):
            poll_count = _nonnegative_int(payload.get("metrics_poll_count"), default=0)
            next_count = poll_count + 1
            publication.last_metrics_polled_at = self.clock.now()
            if next_count >= _MAX_METRICS_POLLS:
                if task is not None:
                    previous_state = task.state
                    task.state = TASK_HELD
                    task.blocked_by_guard = "metrics_unavailable"
                    task.failure_reason = "Publication metrics were unavailable after polling"
                    task.state_updated_at = self.clock.now()
                    task.transition_history_json = [
                        *list(task.transition_history_json or []),
                        _transition(previous_state, TASK_HELD, "collect_metrics", self.clock.now()),
                    ]
                await db.commit()
                return None

            await self.queue.enqueue(
                db,
                kind="collect_metrics",
                idempotency_key=f"collect_metrics:{publication.id}:poll:{next_count}",
                payload={"publication_id": str(publication.id), "metrics_poll_count": next_count},
                priority=90,
                run_after=self.clock.now() + _METRICS_POLL_DELAY,
                channel_profile_id=task.channel_profile_id if task else item.channel_profile_id,
                parent_queue_item_id=item.id,
            )
            await db.commit()
            return None

        snapshot = await self._feedback_snapshot_for_publication(db, publication)
        if snapshot is None:
            snapshot = FeedbackSnapshot(publication_id=publication.id)
            db.add(snapshot)
        snapshot.collected_at = self.clock.now()
        snapshot.views = _nonnegative_int(metrics.get("views"), default=0)
        snapshot.likes = _nonnegative_int(metrics.get("likes"), default=0)
        snapshot.comments = _nonnegative_int(metrics.get("comments"), default=0)
        snapshot.shares = _nonnegative_int(metrics.get("shares"), default=0)
        snapshot.avg_view_duration_sec = _nonnegative_float(metrics.get("avg_view_duration_sec"), default=0.0)
        snapshot.retention_curve_json = _list_value(
            metrics.get("retention_curve_json") or metrics.get("retention_curve")
        )
        snapshot.ctr = _optional_float(metrics.get("ctr"))
        snapshot.impressions = _optional_int(metrics.get("impressions"))
        snapshot.virality_score = _nonnegative_float(metrics.get("virality_score"), default=0.0)
        snapshot.raw_json = payload
        publication.last_metrics_polled_at = self.clock.now()

        if task is not None:
            previous_state = task.state
            task.state = TASK_MEASURED
            task.state_updated_at = self.clock.now()
            task.transition_history_json = [
                *list(task.transition_history_json or []),
                _transition(previous_state, TASK_MEASURED, "collect_metrics", self.clock.now()),
            ]

        await db.commit()
        await db.refresh(snapshot)
        return snapshot

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

    async def _decide_pds(self, db: AsyncSession, request: PDSDecisionRequest) -> PDSDecision:
        decision = await self.pds_client.decide(request)
        if _is_pds_fail_policy_decision(decision):
            await self._maybe_enqueue_pds_outage_alert(db, decision=decision, request=request)
            return decision
        self._pds_last_success_at = self.clock.now()
        return decision

    async def _maybe_enqueue_pds_outage_alert(
        self,
        db: AsyncSession,
        *,
        decision: PDSDecision,
        request: PDSDecisionRequest,
    ) -> None:
        if not self.pds_health_monitor_enabled:
            return
        now = self.clock.now()
        health = should_enqueue_pds_outage_alert(
            now=now,
            last_success_at=self._pds_last_success_at,
            last_alert_bucket=self._pds_last_alert_bucket,
        )
        if not health.should_alert:
            return

        metadata = decision.metadata if isinstance(decision.metadata, dict) else {}
        payload = build_alert_payload(
            "pds_outage",
            resource_id="service:pds",
            severity="critical",
            message="Policy Decision Service is unavailable or returning fail-policy decisions",
            details={
                "action_type": request.action_type,
                "verdict": decision.verdict,
                "decision_id": decision.decision_id,
                "warning": str(metadata.get("warning") or ""),
                "fail_policy": str(metadata.get("fail_policy") or ""),
            },
            now=now,
        )
        await self.queue.enqueue(
            db,
            kind="send_alert",
            idempotency_key=str(payload["dedupe_key"]),
            payload=payload,
            priority=5,
            commit=False,
        )
        self._pds_last_alert_bucket = utc_hour_bucket(now)

    async def _material_payloads_for_task(
        self,
        db: AsyncSession,
        task: ProductionTask,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        plan_payload: dict[str, Any] = {}
        run_payload: dict[str, Any] = {}
        if task.autoflow_plan_id is not None:
            plan = await db.get(AutoFlowPlanModel, task.autoflow_plan_id)
            if plan is not None:
                plan_payload = {
                    "candidates": list(plan.candidates_json or []),
                    "metadata": dict(plan.metadata_json or {}),
                    "rights": dict(plan.rights_json or {}),
                }
        if task.autoflow_run_id is not None:
            run = await db.get(AutoFlowRunModel, task.autoflow_run_id)
            if run is not None:
                run_payload = {
                    "artifacts": dict(run.artifacts_json or {}),
                    "publish": dict(run.publish_json or {}),
                }
        return plan_payload, run_payload

    async def _material_references_for_publish(
        self,
        db: AsyncSession,
        *,
        task: ProductionTask,
        upload_metadata: dict[str, Any],
    ) -> list[MaterialReference]:
        plan_payload, run_payload = await self._material_payloads_for_task(db, task)
        return extract_material_references(
            plan_payload=plan_payload,
            run_payload=run_payload,
            upload_metadata=upload_metadata,
        )

    async def _publish_time_material_usage_guard(
        self,
        db: AsyncSession,
        *,
        task: ProductionTask,
        references: list[MaterialReference],
    ) -> str | None:
        if not references:
            return None
        result = await recent_usage_flags(
            db,
            channel_id=str(task.channel_profile_id),
            lane_id=str(task.topic_lane_id) if task.topic_lane_id is not None else None,
            account_id=str(task.target_account_id) if task.target_account_id is not None else None,
            references=references,
            now=self.clock.now(),
        )
        if not result.blocked:
            return None

        manual_override = task.source == "manual_seed"
        rationale = dict(task.rationale_json or {})
        rationale["material_usage_guard"] = _material_usage_guard_payload(
            result,
            manual_override=manual_override,
        )
        task.rationale_json = rationale
        if manual_override:
            return None

        guard = "cross_account_rejected" if result.cross_account_rejected else "repetition_rejected"
        previous_state = task.state
        task.state = TASK_HELD
        task.blocked_by_guard = guard
        task.failure_reason = f"publish-time material usage guard rejected task: {guard}"
        task.state_updated_at = self.clock.now()
        task.transition_history_json = [
            *list(task.transition_history_json or []),
            _transition(previous_state, TASK_HELD, "publish_task_material_guard", self.clock.now()),
        ]
        return guard

    async def _write_material_usage_ledger(
        self,
        db: AsyncSession,
        *,
        task: ProductionTask,
        publication: PublicationRecord,
        upload_metadata: dict[str, Any],
        references: list[MaterialReference] | None = None,
    ) -> None:
        if references is None:
            references = await self._material_references_for_publish(
                db,
                task=task,
                upload_metadata=upload_metadata,
            )
        if not references:
            return
        existing_rows = (
            await db.execute(
                select(MaterialUsageLedger)
                .where(MaterialUsageLedger.publication_id == publication.id)
            )
        ).scalars().all()
        existing = {(row.material_id, row.segment_signature) for row in existing_rows}
        for ref in references:
            key = (ref.material_id, ref.segment_signature)
            if key in existing:
                continue
            db.add(
                MaterialUsageLedger(
                    material_id=ref.material_id,
                    asset_id=_uuid_or_none(ref.asset_id),
                    channel_profile_id=task.channel_profile_id,
                    topic_lane_id=task.topic_lane_id,
                    publishing_account_id=task.target_account_id,
                    publication_id=publication.id,
                    used_at=self.clock.now(),
                    segment_signature=ref.segment_signature,
                    metadata_json=ref.metadata,
                )
            )
            existing.add(key)

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

    async def _emit_candidate_accepted(
        self,
        db: AsyncSession,
        candidate: dict[str, Any],
        task: ProductionTask,
    ) -> None:
        account = candidate.get("account")
        if account is None:
            return
        lane = candidate.get("lane")
        decision = candidate.get("_pds_decision")
        metadata = {
            "lane_id": str(lane.id) if lane is not None else "",
            "candidate_id": str(candidate.get("candidate_id") or ""),
            "task_id": str(task.id),
            "source": str(candidate.get("source") or ""),
        }
        if isinstance(decision, PDSDecision):
            metadata.update(_pds_decision_event_metadata(decision))
        await self._emit_actor_action_event(
            db,
            actor_id=str(account.id),
            action_type="candidate_accepted",
            platform=_account_platform(account),
            metadata=metadata,
        )

    async def _emit_actor_action_event(
        self,
        db: AsyncSession,
        *,
        actor_id: str,
        action_type: str,
        platform: str,
        metadata: dict[str, Any],
    ) -> None:
        payload = build_actor_action_event(
            actor_id=actor_id,
            action_type=action_type,
            platform=platform,
            metadata=metadata,
            occurred_at=self.clock.now(),
        )
        await self.event_outbox.enqueue(
            db,
            topic=TOPIC_VP_ACTIONS,
            key=payload["actor_id"],
            payload=payload,
        )

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

    async def _feedback_snapshot_for_publication(
        self,
        db: AsyncSession,
        publication: PublicationRecord,
    ) -> FeedbackSnapshot | None:
        result = await db.execute(
            select(FeedbackSnapshot)
            .where(FeedbackSnapshot.publication_id == publication.id)
            .order_by(FeedbackSnapshot.collected_at.desc())
        )
        return result.scalars().first()

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
        input_asset_id, owned_input_profile = _owned_input_asset_id(manual_seed_constraints)
        constraints = {
            "lane_id": lane.get("id"),
            "lane_format_id": lane_format.get("id"),
            "template_pool_json": _string_list(lane_format.get("template_pool_json")),
        }
        constraints.update(
            {key: value for key, value in manual_seed_constraints.items() if key != "input_asset_id"}
        )

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
        if owned_input_profile:
            if input_asset_id is not None:
                request["input_asset_id"] = input_asset_id
            request["source_platforms"] = []
            request["source_policy"] = "owned_only"
            request["source_strategy"] = "input_video"
            request["planning_mode"] = "template"
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


def _suppressed_lane_ids_from_manual_rejections(rejected_candidates: list[dict[str, Any]]) -> set[str]:
    return {
        str(candidate.get("lane_id"))
        for candidate in rejected_candidates
        if candidate.get("guard") in {"account_unavailable", "lane_unavailable"} and candidate.get("lane_id")
    }


def _candidate_rejection(candidate: dict[str, Any], *, guard: str, reason: str) -> dict[str, Any]:
    lane = candidate.get("lane")
    lane_format = candidate.get("lane_format")
    account = candidate.get("account")
    account_rejection = candidate.get("account_rejection")
    pre_rejection = candidate.get("pre_rejection")
    account_id = str(account.id) if account is not None else ""
    if not account_id and isinstance(account_rejection, dict):
        account_id = str(account_rejection.get("account_id") or "")
    lane_id = str(lane.id) if lane is not None else ""
    if not lane_id and isinstance(pre_rejection, dict):
        lane_id = str(pre_rejection.get("lane_id") or "")
    return {
        "candidate_id": candidate["candidate_id"],
        "lane_id": lane_id,
        "format_id": str(lane_format.id) if lane_format is not None else "",
        "account_id": account_id,
        "guard": guard,
        "reason": reason,
    }


def _candidate_material_references(candidate: dict[str, Any]) -> list[MaterialReference]:
    constraints = _dict_value(candidate.get("constraints_json"))
    refs_payload = {
        "material_refs": constraints.get("material_refs") or constraints.get("material_references") or [],
    }
    seed = candidate.get("seed")
    if seed is not None:
        seed_constraints = _dict_value(getattr(seed, "constraints_json", None))
        refs_payload["seed_material_refs"] = (
            seed_constraints.get("material_refs") or seed_constraints.get("material_references") or []
        )
    return extract_material_references(plan_payload={}, run_payload={}, upload_metadata=refs_payload)


def _candidate_id(source: str, lane_id, format_id, bucket: str, *, seed_id=None) -> str:
    if source == "manual_seed" and seed_id is not None:
        return f"{source}:{seed_id}:lane:{lane_id or 'unassigned'}:format:{format_id or 'none'}:{bucket}"
    return f"{source}:lane:{lane_id or 'unassigned'}:format:{format_id or 'none'}:{bucket}"


def _account_platform(account: PublishingAccount) -> str:
    return str(account.platform or "youtube")


def _pds_decision_event_metadata(decision: PDSDecision) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "decision_id": str(decision.decision_id or ""),
        "verdict": str(decision.verdict or ""),
        "score": float(decision.score or 0.0),
        "rules_version": str(decision.rules_version or ""),
        "reason_codes": [
            str(reason.get("code"))
            for reason in decision.reasons
            if isinstance(reason, dict) and reason.get("code") not in {None, ""}
        ],
    }
    warning = decision.metadata.get("warning") if isinstance(decision.metadata, dict) else None
    if warning is not None:
        metadata["warning"] = str(warning)
    return metadata


def _is_pds_fail_policy_decision(decision: PDSDecision) -> bool:
    metadata = decision.metadata if isinstance(decision.metadata, dict) else {}
    warning = str(metadata.get("warning") or "")
    return bool(metadata.get("fail_policy")) or warning in {
        "pds_disabled",
        "pds_unavailable",
        "pds_parse_failed",
    }


def _material_usage_guard_payload(
    result: UsageGuardResult,
    *,
    manual_override: bool,
) -> dict[str, Any]:
    return {
        "manual_override": manual_override,
        "repetition_rejected": result.repetition_rejected,
        "cross_account_rejected": result.cross_account_rejected,
        "hits": list(result.hits),
    }


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


def _uuid_or_none(value: Any) -> uuid.UUID | None:
    try:
        return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
    except (TypeError, ValueError):
        return None


def _owned_input_asset_id(constraints: dict[str, Any]) -> tuple[str | None, bool]:
    if "input_asset_id" not in constraints:
        return None, False
    value = constraints["input_asset_id"]
    if value == "":
        return None, False
    if not isinstance(value, str):
        return None, True
    parsed = _uuid_or_none(value)
    if parsed is None or str(parsed) != value:
        return None, True
    return value, True


def _parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _datetime_to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _publication_effective_time(publication: PublicationRecord) -> datetime | None:
    if publication.publish_status == "public":
        value = (
            publication.public_at
            or publication.scheduled_publish_at
            or publication.uploaded_at
            or publication.created_at
        )
    elif publication.publish_status == "scheduled":
        value = (
            publication.scheduled_publish_at
            or publication.public_at
            or publication.uploaded_at
            or publication.created_at
        )
    else:
        value = (
            publication.scheduled_publish_at
            or publication.public_at
            or publication.uploaded_at
            or publication.created_at
        )
    if value is None:
        return None
    return _datetime_to_utc(value)


def _is_upload_failure_task(task: ProductionTask) -> bool:
    if task.state != TASK_FAILED:
        return False
    reason = str(task.failure_reason or "").casefold()
    return any(keyword.casefold() in reason for keyword in UPLOAD_FAILURE_KEYWORDS)


def _dict_value(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _has_real_metrics(metrics: dict[str, Any]) -> bool:
    return any(key in metrics for key in _RECOGNIZED_METRIC_KEYS)


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


def _nonnegative_int(value: Any, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _nonnegative_float(value: Any, *, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _list_value(value: Any) -> list | None:
    if isinstance(value, list):
        return list(value)
    return None


def _safe_privacy(value: Any) -> str | None:
    desired = str(value or "").strip().lower()
    if desired in _SAFE_PRIVACY_VALUES:
        return desired
    return None


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _status_value(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw or "").strip().lower()


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
