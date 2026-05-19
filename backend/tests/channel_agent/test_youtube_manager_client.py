from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import httpx
import pytest

from app.channel_agent.clients import YouTubeManagerClient


@pytest.mark.asyncio
async def test_youtube_manager_client_maps_quota_schedule_metrics_and_status():
    seen: list[tuple[str, str, dict | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.read()
        payload = None
        if body:
            payload = httpx.Response(200, content=body).json()
        seen.append((request.method, request.url.path, payload))
        if request.method == "GET" and request.url.path == "/api/auth/status":
            return httpx.Response(
                200,
                json={
                    "authenticated": True,
                    "quota_estimate": {
                        "daily_limit": 10000,
                        "estimated_units_remaining": 6400,
                    },
                },
            )
        if request.method == "POST" and request.url.path == "/api/videos/yt-video-1/schedule":
            return httpx.Response(200, json={"video_id": "yt-video-1", "privacy": "private", "status": "scheduled"})
        if request.method == "GET" and request.url.path == "/api/videos/yt-video-1/metrics":
            return httpx.Response(200, json={"metrics": {"views": 42, "likes": 5, "comments": 1}})
        if request.method == "GET" and request.url.path == "/api/videos/yt-video-1/status":
            return httpx.Response(200, json={"video_id": "yt-video-1", "privacy": "private", "upload_status": "processed"})
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    client = YouTubeManagerClient(base_url="http://youtube-manager", transport=httpx.MockTransport(handler))

    assert await client.quota_remaining_fraction(SimpleNamespace()) == 0.64
    schedule = await client.schedule_publish(
        video_id="yt-video-1",
        scheduled_at=datetime(2026, 5, 19, 20, 0, tzinfo=timezone.utc),
        privacy="private",
    )
    metrics = await client.fetch_metrics(video_id="yt-video-1")
    status = await client.fetch_status(video_id="yt-video-1")

    assert schedule["status"] == "scheduled"
    assert metrics["views"] == 42
    assert status["upload_status"] == "processed"
    assert (
        "POST",
        "/api/videos/yt-video-1/schedule",
        {"scheduled_at": "2026-05-19T20:00:00+00:00", "privacy": "private"},
    ) in seen
