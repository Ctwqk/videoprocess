from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol


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

