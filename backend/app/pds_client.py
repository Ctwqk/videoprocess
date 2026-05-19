from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

import httpx

from app.config import settings


@dataclass(frozen=True)
class PDSDecisionRequest:
    actor_id: str
    action_type: str
    platform: str | None = None
    content: dict[str, Any] = field(default_factory=dict)
    context: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PDSDecision:
    decision_id: str
    verdict: str
    score: float = 0.0
    reasons: list[dict[str, Any]] = field(default_factory=list)
    evaluated_rules: list[str] = field(default_factory=list)
    rules_version: str = ""
    latency_ms: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


class PolicyDecisionClient(Protocol):
    async def decide(self, request: PDSDecisionRequest) -> PDSDecision:
        ...


class NoopPDSClient:
    async def decide(self, _request: PDSDecisionRequest) -> PDSDecision:
        return _fail_open("pds_disabled")


class PDSClient:
    def __init__(
        self,
        *,
        base_url: str | None = None,
        client_id: str | None = None,
        timeout_seconds: float | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = (settings.pds_base_url if base_url is None else base_url).rstrip("/")
        self.client_id = settings.pds_client_id if client_id is None else client_id
        self.timeout_seconds = settings.pds_timeout_seconds if timeout_seconds is None else timeout_seconds
        self._http_client = http_client

    async def decide(self, request: PDSDecisionRequest) -> PDSDecision:
        payload = {
            "actor_id": request.actor_id,
            "action": {"type": request.action_type, "platform": request.platform},
            "content": request.content,
            "context": request.context,
        }
        headers = {"X-Client-Id": self.client_id}
        url = f"{self.base_url}/v1/decide"

        try:
            response = await self._post(url, headers=headers, payload=payload)
        except httpx.RequestError:
            return _fail_open("pds_unavailable")

        if response.status_code >= 500:
            return _fail_open("pds_unavailable")

        response.raise_for_status()

        try:
            data = response.json()
            return _decision_from_payload(data)
        except (TypeError, ValueError):
            return _fail_open("pds_unavailable")

    async def _post(self, url: str, *, headers: dict[str, str], payload: dict[str, Any]) -> httpx.Response:
        if self._http_client is not None:
            return await self._http_client.post(
                url,
                json=payload,
                headers=headers,
                timeout=self.timeout_seconds,
            )

        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            return await client.post(url, json=payload, headers=headers)


def _decision_from_payload(data: Any) -> PDSDecision:
    if not isinstance(data, dict):
        raise TypeError("PDS decision response must be an object")

    return PDSDecision(
        decision_id=str(data.get("decision_id") or ""),
        verdict=str(data.get("verdict") or "allow"),
        score=float(data.get("score") or 0.0),
        reasons=_dict_list(data.get("reasons")),
        evaluated_rules=_str_list(data.get("evaluated_rules")),
        rules_version=str(data.get("rules_version") or ""),
        latency_ms=int(data.get("latency_ms") or 0),
        metadata=_dict(data.get("metadata")),
    )


def _dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise TypeError("expected object")
    return dict(value)


def _dict_list(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise TypeError("expected list of objects")
    return [dict(item) for item in value]


def _str_list(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise TypeError("expected list")
    return [str(item) for item in value]


def _fail_open(warning: str) -> PDSDecision:
    return PDSDecision(decision_id="", verdict="allow", metadata={"warning": warning})
