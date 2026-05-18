from __future__ import annotations

import asyncio
import logging

from app.channel_agent.queue import ChannelOpsQueueService
from app.channel_agent.service import ChannelAgentService
from app.db import async_session

logger = logging.getLogger(__name__)


class ChannelAgentRunner:
    def __init__(self, *, worker_id: str = "channel-agent-runner") -> None:
        self.worker_id = worker_id
        self.queue = ChannelOpsQueueService()
        self.service = ChannelAgentService(queue=self.queue)

    async def run_once(self) -> bool:
        async with async_session() as db:
            item = await self.queue.claim_next(db, worker_id=self.worker_id)
            if item is None:
                return False
            try:
                await self.handle_item(db, item)
            except Exception as exc:
                logger.exception("ChannelOps queue item failed: %s", item.id)
                await self.queue.mark_failed_or_retry(db, item, str(exc))
                return True
            await self.queue.mark_succeeded(db, item)
            return True

    async def run_forever(self, *, poll_seconds: float = 5.0) -> None:
        while True:
            handled = await self.run_once()
            if not handled:
                await asyncio.sleep(poll_seconds)

    async def handle_item(self, db, item) -> None:
        if item.kind == "agent_tick":
            await self.service.tick(db, channel_id=item.payload_json["channel_id"])
        elif item.kind == "plan_task":
            await self.service.handle_plan_task(db, item)
        elif item.kind == "publish_task":
            await self.service.handle_publish_task(db, item)
        elif item.kind == "promote_publication":
            await self.service.handle_promote_publication(db, item)
        elif item.kind == "account_health":
            await self.service.handle_account_health(db, item)
        elif item.kind in {"send_alert", "collect_metrics", "execute_task", "observe_job"}:
            # These kinds are durable no-ops until the concrete external
            # integrations are configured; keeping them recognized prevents
            # dead-letter loops during alpha dry runs.
            return
        else:
            raise ValueError(f"Unsupported ChannelOps queue kind: {item.kind}")

