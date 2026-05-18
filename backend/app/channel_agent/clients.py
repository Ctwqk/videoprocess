from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

import httpx

from app.config import settings


@dataclass(frozen=True)
class AutoFlowPlanObservation:
    plan_id: str
    pipeline_definition: dict[str, Any]

    @property
    def upload_node_count(self) -> int:
        return sum(1 for node in self.pipeline_definition.get("nodes", []) if node.get("type") == "youtube_upload")


class AutoFlowClient(Protocol):
    async def plan_task(self, task, request: dict[str, Any]) -> AutoFlowPlanObservation:
        ...


class YouTubeClient(Protocol):
    async def quota_remaining_fraction(self, account) -> float:
        ...

    async def schedule_publish(self, *, video_id: str, scheduled_at: datetime, privacy: str) -> dict[str, Any]:
        ...

    async def refresh_token(self, account) -> bool:
        ...


class MiniMaxClient(Protocol):
    async def generate_thumbnail(self, *, prompt: str, title: str) -> dict[str, Any]:
        ...


class FakeAutoFlowClient:
    def __init__(self, *, include_upload: bool = True):
        self.include_upload = include_upload
        self.requests: list[dict[str, Any]] = []

    async def plan_task(self, task, request: dict[str, Any]) -> AutoFlowPlanObservation:
        self.requests.append(dict(request))
        nodes = [{"id": "transcode_1", "type": "transcode"}]
        if self.include_upload:
            nodes.append({"id": "youtube_upload_1", "type": "youtube_upload"})
        return AutoFlowPlanObservation(
            plan_id=str(uuid.uuid4()),
            pipeline_definition={"nodes": nodes, "edges": []},
        )


class FakeYouTubeClient:
    def __init__(self, *, quota_remaining_fraction: float = 1.0, token_valid: bool = True):
        self._quota_remaining_fraction = quota_remaining_fraction
        self.token_valid = token_valid
        self.scheduled: list[dict[str, Any]] = []

    async def quota_remaining_fraction(self, account) -> float:
        return self._quota_remaining_fraction

    async def schedule_publish(self, *, video_id: str, scheduled_at: datetime, privacy: str) -> dict[str, Any]:
        payload = {"video_id": video_id, "scheduled_at": scheduled_at.isoformat(), "privacy": privacy}
        self.scheduled.append(payload)
        return payload

    async def refresh_token(self, account) -> bool:
        return self.token_valid


class FakeMiniMaxClient:
    def __init__(self, *, fail: bool = False):
        self.fail = fail
        self.calls: list[dict[str, Any]] = []

    async def generate_thumbnail(self, *, prompt: str, title: str) -> dict[str, Any]:
        self.calls.append({"prompt": prompt, "title": title})
        if self.fail:
            raise RuntimeError("minimax failed")
        return {"storage_path": f"/tmp/{title or 'thumbnail'}.png", "provider": "minimax"}


class MiniMaxImageClient:
    def __init__(
        self,
        *,
        api_key: str | None = None,
        endpoint: str | None = None,
        model: str | None = None,
        timeout_seconds: float | None = None,
        retry_count: int | None = None,
        max_qps: float | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.api_key = settings.minimax_api_key if api_key is None else api_key
        self.endpoint = settings.minimax_image_generation_url if endpoint is None else endpoint
        self.model = settings.minimax_model if model is None else model
        self.timeout_seconds = settings.minimax_timeout_seconds if timeout_seconds is None else timeout_seconds
        self.retry_count = settings.minimax_retry_count if retry_count is None else retry_count
        self.max_qps = settings.minimax_max_qps if max_qps is None else max_qps
        self.transport = transport
        self._rate_lock = asyncio.Lock()
        self._last_request_at = 0.0

    async def generate_thumbnail(self, *, prompt: str, title: str) -> dict[str, Any]:
        if not self.api_key:
            raise RuntimeError("MINIMAX_API_KEY is not configured")

        payload = {
            "model": self.model,
            "prompt": _thumbnail_prompt(prompt=prompt, title=title),
            "aspect_ratio": "16:9",
            "response_format": "url",
            "n": 1,
            "prompt_optimizer": True,
            "aigc_watermark": False,
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        last_error: Exception | None = None
        for attempt in range(self.retry_count + 1):
            try:
                await self._pace()
                async with httpx.AsyncClient(timeout=self.timeout_seconds, transport=self.transport) as client:
                    response = await client.post(self.endpoint, headers=headers, json=payload)
                    response.raise_for_status()
                body = response.json()
                urls = list(((body.get("data") or {}).get("image_urls") or []))
                if not urls:
                    raise RuntimeError("MiniMax image_generation returned no image_urls")
                return {
                    "provider": "minimax",
                    "request_id": body.get("id"),
                    "image_url": urls[0],
                    "raw": body,
                }
            except Exception as exc:
                last_error = exc
                if attempt >= self.retry_count:
                    break
                await asyncio.sleep(0.5 * (attempt + 1))

        raise RuntimeError(f"MiniMax thumbnail generation failed: {last_error}") from last_error

    async def _pace(self) -> None:
        if self.max_qps <= 0:
            return
        interval = 1.0 / self.max_qps
        loop = asyncio.get_running_loop()
        async with self._rate_lock:
            now = loop.time()
            wait_seconds = interval - (now - self._last_request_at)
            if wait_seconds > 0:
                await asyncio.sleep(wait_seconds)
            self._last_request_at = loop.time()


def _thumbnail_prompt(*, prompt: str, title: str) -> str:
    base = title.strip() or prompt.strip()[:80] or "YouTube thumbnail"
    return (
        f"YouTube thumbnail for: {base}. "
        "High contrast, clear subject, readable composition, no text overlay, 16:9."
    )
