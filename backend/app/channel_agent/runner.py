from __future__ import annotations

import asyncio
import contextlib
import logging

from app.channel_agent.alerts import AlertService
from app.channel_agent.clients import LocalAutoFlowClient, MiniMaxImageClient, YouTubeManagerClient
from app.channel_agent.queue import ChannelOpsQueueService
from app.channel_agent.retention import cleanup_expired
from app.channel_agent.scheduler import ChannelOpsScheduler
from app.channel_agent.service import ChannelAgentService
from app.config import settings
from app.db import async_session
from app.models.channel_agent import ChannelOpsQueueItem
from app.pds_client import NoopPDSClient, PDSClient, PolicyDecisionClient

logger = logging.getLogger(__name__)


def _build_pds_client() -> PolicyDecisionClient:
    if not settings.pds_enabled:
        return NoopPDSClient()
    return PDSClient(
        base_url=settings.pds_base_url,
        client_id=settings.pds_client_id,
        timeout_seconds=settings.pds_timeout_seconds,
    )


def _build_youtube_client() -> YouTubeManagerClient:
    if not settings.youtube_manager_url.strip():
        raise RuntimeError("YOUTUBE_MANAGER_URL is required for live ChannelOps runner mode")
    return YouTubeManagerClient(base_url=settings.youtube_manager_url)


class ChannelAgentRunner:
    def __init__(
        self,
        *,
        worker_id: str = "channel-agent-runner",
        alert_service: AlertService | None = None,
    ) -> None:
        self.worker_id = worker_id
        self.queue = ChannelOpsQueueService()
        self.scheduler = ChannelOpsScheduler(queue=self.queue)
        self.service = ChannelAgentService(
            queue=self.queue,
            autoflow_client=LocalAutoFlowClient(),
            youtube_client=_build_youtube_client(),
            minimax_client=MiniMaxImageClient(),
            pds_client=_build_pds_client(),
            pds_health_monitor_enabled=settings.pds_enabled,
        )
        self.alert_service = alert_service or AlertService()

    async def run_once(self, *, run_scheduler_when_idle: bool = True) -> bool:
        async with async_session() as db:
            item = await self.queue.claim_next(db, worker_id=self.worker_id)
            if item is None:
                if not run_scheduler_when_idle:
                    return False
                scheduler_result = await self.scheduler.run_once(db)
                return scheduler_result.enqueued_count > 0
            item_id = item.id
            try:
                await self.handle_item(db, item)
            except Exception as exc:
                logger.exception("ChannelOps queue item failed: %s", item_id)
                await db.rollback()
                failed_item = await db.get(ChannelOpsQueueItem, item_id)
                if failed_item is None:
                    raise
                await self.queue.mark_failed_or_retry(db, failed_item, str(exc))
                return True
            await self.queue.mark_succeeded(db, item)
            return True

    async def run_forever(self, *, poll_seconds: float = 5.0) -> None:
        scheduler_task = asyncio.create_task(
            self._run_scheduler_forever(poll_seconds=settings.channel_agent_scheduler_poll_seconds)
        )
        try:
            while True:
                handled = await self.run_once(run_scheduler_when_idle=False)
                if not handled:
                    await asyncio.sleep(poll_seconds)
        finally:
            scheduler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await scheduler_task

    async def _run_scheduler_forever(self, *, poll_seconds: float) -> None:
        while True:
            try:
                async with async_session() as db:
                    await self.scheduler.run_once(db)
            except Exception:
                logger.exception("ChannelOps scheduler tick failed")
            await asyncio.sleep(poll_seconds)

    async def handle_item(self, db, item) -> None:
        if item.kind == "agent_tick":
            await self.service.tick(db, channel_id=item.payload_json["channel_id"])
        elif item.kind == "plan_task":
            await self.service.handle_plan_task(db, item)
        elif item.kind == "execute_task":
            await self.service.handle_execute_task(db, item)
        elif item.kind == "observe_job":
            await self.service.handle_observe_job(db, item)
        elif item.kind == "publish_task":
            await self.service.handle_publish_task(db, item)
        elif item.kind == "promote_publication":
            await self.service.handle_promote_publication(db, item)
        elif item.kind == "reconcile_publication":
            await self.service.handle_reconcile_publication(db, item)
        elif item.kind == "collect_metrics":
            await self.service.handle_collect_metrics(db, item)
        elif item.kind == "account_health":
            await self.service.handle_account_health(db, item)
        elif item.kind == "send_alert":
            await self.alert_service.send(dict(item.payload_json or {}))
        elif item.kind == "cleanup_expired":
            await cleanup_expired(
                db,
                now=self.service.clock.now(),
                queue_retention_days=settings.channel_agent_retention_queue_days,
                audit_retention_days=settings.channel_agent_retention_audit_days,
                feedback_retention_days=settings.channel_agent_retention_feedback_days,
            )
        else:
            raise ValueError(f"Unsupported ChannelOps queue kind: {item.kind}")
