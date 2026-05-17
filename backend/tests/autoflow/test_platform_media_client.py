from __future__ import annotations

import httpx
import json
import pytest

from app.autoflow.platform_media_client import PlatformMediaClient, PlatformMediaClientError


@pytest.mark.asyncio
async def test_platform_media_client_searches_youtube_manager_and_normalizes_results() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == "http://youtube.test/api/search"
        assert request.method == "POST"
        assert json.loads(request.content) == {"query": "cats", "max_results": 2}
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "id": "abc123",
                        "title": "Cat clip",
                        "url": "https://www.youtube.com/watch?v=abc123",
                        "thumbnail": "https://img.test/cat.jpg",
                        "duration": 9,
                        "channel": "cat channel",
                    }
                ]
            },
        )

    client = PlatformMediaClient(
        youtube_manager_url="http://youtube.test",
        platform_manager_url="http://platform.test",
        transport=httpx.MockTransport(handler),
    )

    results = await client.search("youtube", "cats", max_results=2)

    assert len(results) == 1
    assert results[0].source_type == "youtube"
    assert results[0].id == "abc123"
    assert results[0].url == "https://www.youtube.com/watch?v=abc123"
    assert results[0].rights_status == "review_required"


@pytest.mark.asyncio
async def test_platform_media_client_searches_browser_manager_platforms() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == "http://platform.test/api/platforms/bilibili/search"
        assert request.method == "POST"
        assert json.loads(request.content) == {"query": "cats", "max_results": 3}
        return httpx.Response(
            200,
            json={
                "platform": "bilibili",
                "results": [
                    {
                        "id": "BV1xx411c7mD",
                        "platform": "bilibili",
                        "title": "Bili cats",
                        "url": "https://www.bilibili.com/video/BV1xx411c7mD",
                        "duration": 12,
                    }
                ],
            },
        )

    client = PlatformMediaClient(
        youtube_manager_url="http://youtube.test",
        platform_manager_url="http://platform.test",
        transport=httpx.MockTransport(handler),
    )

    results = await client.search("bilibili", "cats", max_results=3)

    assert len(results) == 1
    assert results[0].source_type == "bilibili"
    assert results[0].source_type == "bilibili"
    assert results[0].metadata["duration"] == 12


@pytest.mark.asyncio
async def test_platform_media_client_maps_platform_error_category() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"detail": "X login_required: browser login required"})

    client = PlatformMediaClient(
        youtube_manager_url="http://youtube.test",
        platform_manager_url="http://platform.test",
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(PlatformMediaClientError) as exc:
        await client.search("x", "cats", max_results=1)

    assert exc.value.platform == "x"
    assert exc.value.category == "login_required"
    assert "browser login required" in str(exc.value)
