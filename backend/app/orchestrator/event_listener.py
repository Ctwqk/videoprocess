from __future__ import annotations
import asyncio
import logging
import uuid
from datetime import datetime

import redis.asyncio as aioredis
from app.config import settings
from app.orchestrator.engine import engine, EVENT_STREAM
from app.services.job_execution_authority import NodeExecutionClaim

logger = logging.getLogger(__name__)

CONSUMER_GROUP = "orchestrator"
CONSUMER_NAME = "orchestrator-api-1"
PEL_RECLAIM_INTERVAL = 60  # seconds
PEL_MIN_IDLE = 30000       # ms
REDIS_BLOCK_MILLISECONDS = 5000
REDIS_SOCKET_TIMEOUT_SECONDS = 30.0


class UnverifiableExecutionClaimEvent(RuntimeError):
    """Keep a legacy event pending until deployment reconciliation."""


def _redis() -> aioredis.Redis:
    return aioredis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_timeout=REDIS_SOCKET_TIMEOUT_SECONDS,
        socket_connect_timeout=5.0,
        health_check_interval=30,
        retry_on_timeout=True,
    )


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
    r = _redis()

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
                    block=REDIS_BLOCK_MILLISECONDS,
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
    if event_type not in {"node_completed", "node_failed"}:
        logger.warning("Unknown event type: %s", event_type)
        return

    try:
        job_id = uuid.UUID(data["job_id"])
        node_execution_id = uuid.UUID(data["node_execution_id"])
    except (KeyError, TypeError, ValueError):
        logger.warning("Ignoring malformed %s event identifiers", event_type)
        return

    if "worker_id" not in data and "started_at" not in data:
        raise UnverifiableExecutionClaimEvent(
            f"{event_type} event is missing execution claim"
        )

    claim = _execution_claim_from_event(data, job_id, node_execution_id)
    if claim is None:
        logger.warning(
            "Ignoring %s event without a valid execution claim job=%s node=%s",
            event_type,
            job_id,
            node_execution_id,
        )
        return

    if event_type == "node_completed":
        try:
            output_artifact_id = uuid.UUID(data["output_artifact_id"])
        except (KeyError, TypeError, ValueError):
            logger.warning(
                "Ignoring malformed node_completed artifact job=%s node=%s",
                job_id,
                node_execution_id,
            )
            return
        await engine.on_node_completed(
            job_id,
            node_execution_id,
            output_artifact_id,
            claim=claim,
        )
    else:
        error = data.get("error", "Unknown error")
        await engine.on_node_failed(
            job_id,
            node_execution_id,
            error,
            claim=claim,
        )


def _execution_claim_from_event(
    data: dict,
    job_id: uuid.UUID,
    node_execution_id: uuid.UUID,
) -> NodeExecutionClaim | None:
    worker_id = data.get("worker_id")
    started_at_raw = data.get("started_at")
    if not isinstance(worker_id, str) or not worker_id.strip():
        return None
    if not isinstance(started_at_raw, str):
        return None
    try:
        started_at = datetime.fromisoformat(started_at_raw)
    except ValueError:
        return None
    if started_at.tzinfo is None or started_at.utcoffset() is None:
        return None
    return NodeExecutionClaim(
        job_id=job_id,
        node_execution_id=node_execution_id,
        worker_id=worker_id.strip(),
        started_at=started_at,
    )
