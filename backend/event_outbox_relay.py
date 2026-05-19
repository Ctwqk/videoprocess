from __future__ import annotations

import asyncio
import logging

import sqlalchemy as sa
from prometheus_client import Counter, Gauge, start_http_server

from app.config import settings
from app.db import async_session
from app.events.outbox import event_outbox_table
from app.events.producer import KafkaEventProducer
from app.events.relay import EventOutboxRelay


logger = logging.getLogger(__name__)

OutboxUnsent = Gauge(
    "vp_event_outbox_unsent",
    "Current count of undelivered events in the VideoProcess outbox.",
)
OutboxSent = Counter(
    "vp_event_outbox_sent_total",
    "Total events successfully delivered from the VideoProcess outbox.",
)
OutboxSendErrors = Counter(
    "vp_event_outbox_send_errors_total",
    "Total failed delivery attempts from the VideoProcess outbox.",
)


async def _refresh_unsent_gauge(db) -> None:
    row = (
        await db.execute(
            sa.select(sa.func.count())
            .select_from(event_outbox_table)
            .where(event_outbox_table.c.delivered_at.is_(None))
        )
    ).one()
    OutboxUnsent.set(int(row[0]))


async def run_forever() -> None:
    batch_size = settings.risk_outbox_batch_size
    base_delay = settings.risk_outbox_poll_seconds
    max_delay = settings.risk_outbox_max_backoff_seconds
    metrics_port = settings.risk_outbox_metrics_port

    start_http_server(metrics_port)
    logger.info("event_outbox_relay metrics on :%d", metrics_port)

    delay = base_delay
    producer = None
    try:
        startup_delay = base_delay
        while producer is None:
            candidate = KafkaEventProducer(brokers=settings.risk_kafka_brokers)
            try:
                await candidate.start()
            except Exception:
                logger.exception("event_outbox_relay producer startup failed")
                OutboxSendErrors.inc()
                await asyncio.sleep(startup_delay)
                startup_delay = min(startup_delay * 2, max_delay)
            else:
                producer = candidate

        relay = EventOutboxRelay(producer=producer)
        while True:
            try:
                async with async_session() as db:
                    result = await relay.run_once(db, batch_size=batch_size)
                    await db.commit()
                    await _refresh_unsent_gauge(db)
                OutboxSent.inc(result.delivered)
                OutboxSendErrors.inc(result.errors)
                delay = min(delay * 2, max_delay) if result.errors else base_delay
            except Exception:
                logger.exception("event_outbox_relay cycle failed")
                OutboxSendErrors.inc()
                delay = min(delay * 2, max_delay)
            await asyncio.sleep(delay)
    finally:
        if producer is not None:
            await producer.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_forever())
