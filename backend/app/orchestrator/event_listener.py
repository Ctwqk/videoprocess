from __future__ import annotations
import asyncio
import json
import logging
import uuid

import redis.asyncio as aioredis
from app.config import settings
from app.orchestrator.engine import engine, EVENT_STREAM

logger = logging.getLogger(__name__)

CONSUMER_GROUP = "orchestrator"
CONSUMER_NAME = "orchestrator-api-1"
PEL_RECLAIM_INTERVAL = 60  # seconds
PEL_MIN_IDLE = 30000       # ms


async def _reclaim_pending(r: aioredis.Redis) -> None:
    """Reclaim stale pending events from any consumer in the group."""
    try:
        claimed = await r.xautoclaim(
            EVENT_STREAM, CONSUMER_GROUP, CONSUMER_NAME,
            min_idle_time=PEL_MIN_IDLE,
            start_id="0-0",
            count=100,
        )
        if claimed and len(claimed) > 1 and claimed[1]:
            for msg_id, data in claimed[1]:
                if data:
                    logger.info(f"Reclaimed pending event {msg_id}")
                    try:
                        await _handle_event(data)
                        await r.xack(EVENT_STREAM, CONSUMER_GROUP, msg_id)
                    except Exception:
                        logger.exception(f"Failed to process reclaimed event {msg_id}")
    except Exception:
        logger.exception("PEL reclaim failed")


async def event_listener() -> None:
    """Background task that consumes worker events from Redis Stream and drives the orchestrator."""
    r = aioredis.from_url(settings.redis_url, decode_responses=True)

    # Create consumer group (ignore if already exists)
    try:
        await r.xgroup_create(EVENT_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
    except aioredis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    logger.info("Orchestrator event listener started")

    # Initial PEL recovery on startup
    await _reclaim_pending(r)

    last_reclaim = asyncio.get_event_loop().time()

    try:
        while True:
            try:
                # Periodic PEL reclaim
                now = asyncio.get_event_loop().time()
                if now - last_reclaim > PEL_RECLAIM_INTERVAL:
                    await _reclaim_pending(r)
                    last_reclaim = now

                messages = await r.xreadgroup(
                    CONSUMER_GROUP,
                    CONSUMER_NAME,
                    {EVENT_STREAM: ">"},
                    count=10,
                    block=5000,  # block 5 seconds
                )

                if not messages:
                    continue

                for stream_name, entries in messages:
                    for msg_id, data in entries:
                        try:
                            await _handle_event(data)
                            await r.xack(EVENT_STREAM, CONSUMER_GROUP, msg_id)
                        except Exception:
                            logger.exception(f"Failed to handle event {msg_id}: {data}")

            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Event listener error, reconnecting in 2s")
                await asyncio.sleep(2)
    finally:
        await r.aclose()


async def _handle_event(data: dict) -> None:
    event_type = data.get("event")
    job_id = uuid.UUID(data["job_id"])
    node_execution_id = uuid.UUID(data["node_execution_id"])

    if event_type == "node_completed":
        output_artifact_id = uuid.UUID(data["output_artifact_id"])
        await engine.on_node_completed(job_id, node_execution_id, output_artifact_id)
    elif event_type == "node_failed":
        error = data.get("error", "Unknown error")
        await engine.on_node_failed(job_id, node_execution_id, error)
    else:
        logger.warning(f"Unknown event type: {event_type}")
