from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from app.channel_agent.queue import utc_hour_bucket


@dataclass(frozen=True)
class PDSHealthDecision:
    should_alert: bool
    idempotency_key: str


def should_enqueue_pds_outage_alert(
    *,
    now: datetime,
    last_success_at: datetime | None,
    last_alert_bucket: str | None,
    outage_after: timedelta = timedelta(minutes=5),
) -> PDSHealthDecision:
    current_bucket = utc_hour_bucket(_as_utc(now))
    if last_alert_bucket == current_bucket:
        return PDSHealthDecision(False, f"pds_outage:{current_bucket}")
    if last_success_at is not None and _as_utc(now) - _as_utc(last_success_at) <= outage_after:
        return PDSHealthDecision(False, f"pds_outage:{current_bucket}")
    return PDSHealthDecision(True, f"pds_outage:{current_bucket}")


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
