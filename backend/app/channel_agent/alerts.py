from __future__ import annotations

from datetime import datetime
from typing import Any

import httpx

from app.channel_agent.queue import utc_hour_bucket
from app.config import settings


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
    def __init__(
        self,
        *,
        slack_webhook_url: str | None = None,
        email_to: str | None = None,
        timeout_seconds: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.slack_webhook_url = (
            settings.channel_agent_alert_slack_webhook_url if slack_webhook_url is None else slack_webhook_url
        )
        self.email_to = settings.channel_agent_alert_email_to if email_to is None else email_to
        self.timeout_seconds = timeout_seconds
        self.transport = transport
        self.sent_payloads: list[dict[str, Any]] = []

    async def send(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.sent_payloads.append(dict(payload))
        result: dict[str, Any] = {"status": "recorded", "type": payload.get("type")}

        if self.slack_webhook_url:
            async with httpx.AsyncClient(timeout=self.timeout_seconds, transport=self.transport) as client:
                response = await client.post(self.slack_webhook_url, json=_slack_message(payload))
                response.raise_for_status()
                result["slack_status_code"] = response.status_code
                result["status"] = "sent"

        if self.email_to:
            # Email delivery is a config placeholder for alpha; Slack is the
            # concrete push path. Keep the destination in the result so queue
            # audit rows explain why no email transport was invoked.
            result["email_to"] = self.email_to

        return result


def _slack_message(payload: dict[str, Any]) -> dict[str, Any]:
    alert_type = str(payload.get("type") or "channel_ops_alert")
    severity = str(payload.get("severity") or "info")
    resource_id = str(payload.get("resource_id") or "-")
    message = str(payload.get("message") or alert_type)
    details = payload.get("details") if isinstance(payload.get("details"), dict) else {}
    detail_text = "\n".join(f"{key}: {value}" for key, value in details.items())
    text = f"[ChannelOps:{severity}] {alert_type} {resource_id} - {message}"
    if detail_text:
        text = f"{text}\n{detail_text}"
    return {"text": text}
