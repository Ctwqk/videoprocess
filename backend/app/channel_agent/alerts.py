from __future__ import annotations

from datetime import datetime
from typing import Any

from app.channel_agent.queue import utc_hour_bucket


def build_alert_payload(
    alert_type: str,
    *,
    resource_id: str,
    severity: str,
    message: str,
    details: dict[str, Any] | None = None,
    now: datetime,
) -> dict[str, Any]:
    bucket = utc_hour_bucket(now)
    return {
        "type": alert_type,
        "resource_id": resource_id,
        "severity": severity,
        "message": message,
        "details": dict(details or {}),
        "created_at": now.isoformat(),
        "dedupe_key": f"send_alert:{alert_type}:{resource_id}:{bucket}",
    }


class AlertService:
    def __init__(self) -> None:
        self.sent_payloads: list[dict[str, Any]] = []

    async def send(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.sent_payloads.append(dict(payload))
        return {"status": "sent", "type": payload.get("type")}

