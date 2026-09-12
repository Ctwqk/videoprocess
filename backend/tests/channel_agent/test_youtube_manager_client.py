from __future__ import annotations

import copy
import json
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


TASK_ID = "00000000-0000-0000-0000-000000000001"
VIDEO_ID = "abcdefghijk"
CHANNEL_ID = "UC" + "a" * 22


def history_responses():
    task = {"id": TASK_ID, "type": "upload", "status": "completed", "progress": 100,
            "result": {"video_id": VIDEO_ID, "url": f"https://www.youtube.com/watch?v={VIDEO_ID}"}, "error": None}
    video = {"video_id": VIDEO_ID, "privacy": "unlisted", "upload_status": "processed",
             "made_for_kids": False, "public_stats_viewable": True, "title": "Synthetic owned video",
             "published_at": "2026-09-10T01:00:00Z", "processing_status": "succeeded",
             "raw": {"status": {"privacyStatus": "unlisted", "uploadStatus": "processed", "madeForKids": False,
                                "publicStatsViewable": True, "license": "youtube", "embeddable": True},
                     "snippet": {"channelId": CHANNEL_ID, "title": "Synthetic owned video",
                                 "publishedAt": "2026-09-10T01:00:00Z", "description": "Not authority", "tags": ["owned"]},
                     "processingDetails": {"processingStatus": "succeeded", "fileDetailsAvailability": "available"}}}
    return task, video


@pytest.mark.asyncio
async def test_history_qualification_observes_exact_task_video_and_actual_channel_with_only_two_gets():
    task, video = history_responses()
    seen = []

    def handler(request):
        seen.append((request.method, request.url.path))
        assert not request.content
        return httpx.Response(200, json=task if request.url.path == f"/api/status/{TASK_ID}" else video)

    client = YouTubeManagerClient(base_url="http://youtube-manager", transport=httpx.MockTransport(handler))
    result = await client.qualify_upload(manager_task_id=TASK_ID, video_id=VIDEO_ID)
    assert result.manager_task_id == TASK_ID
    assert result.platform_video_id == VIDEO_ID
    assert result.actual_platform_channel_id == CHANNEL_ID
    assert seen == [("GET", f"/api/status/{TASK_ID}"), ("GET", f"/api/videos/{VIDEO_ID}/status")]
    assert "Synthetic owned" not in repr(result)


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["task_id", "task_type", "submitted", "result_video", "result_url", "task_extra",
    "video_id", "public", "processing", "missing_uc", "invalid_uc", "raw_privacy", "raw_upload", "raw_processing",
    "raw_title", "raw_published", "raw_kids", "raw_stats", "bool_type", "video_extra", "missing_raw"])
async def test_history_qualification_rejects_incomplete_or_conflicting_native_observation(bad):
    from app.channel_agent import clients
    task, video = history_responses()
    if bad in {"task_id", "task_type", "submitted", "task_extra"}:
        key, value = {"task_id": ("id", "00000000-0000-0000-0000-000000000002"), "task_type": ("type", "download"),
                      "submitted": ("status", "processing"), "task_extra": ("channel_id", CHANNEL_ID)}[bad]
        task[key] = value
    elif bad.startswith("result_"):
        task["result"]["video_id" if bad == "result_video" else "url"] = "foreign"
    elif bad == "missing_uc":
        del video["raw"]["snippet"]["channelId"]
    elif bad == "invalid_uc":
        video["raw"]["snippet"]["channelId"] = "default-account-label"
    elif bad.startswith("raw_"):
        section, field = {"raw_privacy": ("status", "privacyStatus"), "raw_upload": ("status", "uploadStatus"),
            "raw_processing": ("processingDetails", "processingStatus"), "raw_title": ("snippet", "title"),
            "raw_published": ("snippet", "publishedAt"), "raw_kids": ("status", "madeForKids"),
            "raw_stats": ("status", "publicStatsViewable")}[bad]
        video["raw"][section][field] = "conflict"
    elif bad == "missing_raw":
        del video["raw"]
    else:
        key, value = {"video_id": ("video_id", "foreign"), "public": ("privacy", "public"),
            "processing": ("processing_status", "processing"), "bool_type": ("made_for_kids", 0),
            "video_extra": ("actual_channel_id", CHANNEL_ID)}[bad]
        video[key] = value
    client = YouTubeManagerClient(base_url="http://youtube-manager", transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json=task if request.url.path.startswith("/api/status/") else video)))
    with pytest.raises(clients.YouTubeHistoryQualificationError, match="^owned_inventory_manager_observation_invalid$"):
        await client.qualify_upload(manager_task_id=TASK_ID, video_id=VIDEO_ID)


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["duplicate", "nonfinite", "oversized", "http_error", "transport"])
async def test_history_qualification_fails_closed_without_retry_or_response_leaks(bad):
    from app.channel_agent import clients
    task, _ = history_responses()
    seen = []

    def handler(request):
        seen.append(request.url.path)
        if bad == "transport":
            raise httpx.ConnectError("secret-sentinel", request=request)
        if bad == "http_error":
            return httpx.Response(500, text="secret-sentinel")
        raw = json.dumps(copy.deepcopy(task))
        if bad == "duplicate":
            raw = raw.replace('"completed"', '"failed", "status": "completed"')
        elif bad == "nonfinite":
            raw = raw.replace('100', 'NaN')
        else:
            raw += " " * (512 * 1024)
        return httpx.Response(200, text=raw)

    client = YouTubeManagerClient(base_url="http://youtube-manager", transport=httpx.MockTransport(handler))
    with pytest.raises(clients.YouTubeHistoryQualificationError) as error:
        await client.qualify_upload(manager_task_id=TASK_ID, video_id=VIDEO_ID)
    assert str(error.value).startswith("owned_inventory_manager_")
    assert "secret-sentinel" not in str(error.value)
    assert len(seen) == 1
