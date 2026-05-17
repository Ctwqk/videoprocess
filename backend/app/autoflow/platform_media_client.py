from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from app.config import settings
from app.schemas.autoflow import AutoFlowClipCandidate


SUPPORTED_SOURCE_PLATFORMS = ("youtube", "bilibili", "x", "xiaohongshu")
PLATFORM_ERROR_CATEGORIES = (
    "login_required",
    "platform_unavailable",
    "unsupported_url",
    "platform_search_failed",
    "platform_download_failed",
)


@dataclass
class PlatformMediaClientError(RuntimeError):
    platform: str
    category: str
    detail: str

    def __str__(self) -> str:
        return self.detail


class PlatformMediaClient:
    def __init__(
        self,
        *,
        youtube_manager_url: str | None = None,
        platform_manager_url: str | None = None,
        platform_manager_urls: dict[str, str] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.youtube_manager_url = (youtube_manager_url or settings.youtube_manager_url).rstrip("/")
        default_platform_url = (platform_manager_url or settings.platform_browser_manager_url).rstrip("/")
        self.platform_manager_urls = {
            "bilibili": (settings.bilibili_platform_browser_manager_url or default_platform_url).rstrip("/"),
            "x": (settings.x_platform_browser_manager_url or default_platform_url).rstrip("/"),
            "xiaohongshu": (settings.xiaohongshu_platform_browser_manager_url or default_platform_url).rstrip("/"),
            **{key: value.rstrip("/") for key, value in (platform_manager_urls or {}).items()},
        }
        self.transport = transport

    async def search(self, platform: str, query: str, max_results: int = 8) -> list[AutoFlowClipCandidate]:
        platform_key = _normalize_platform(platform)
        payload = {"query": query, "max_results": max(1, min(int(max_results), 50))}
        if platform_key == "youtube":
            url = f"{self.youtube_manager_url}/api/search"
        else:
            url = f"{self.platform_manager_urls.get(platform_key, self.platform_manager_urls['bilibili'])}/api/platforms/{platform_key}/search"

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(60.0, connect=10.0),
            transport=self.transport,
        ) as client:
            try:
                response = await client.post(url, json=payload)
            except httpx.HTTPError as exc:
                raise PlatformMediaClientError(
                    platform_key,
                    "platform_unavailable",
                    f"{platform_key} search service unavailable: {exc}",
                ) from exc

        if response.status_code >= 400:
            raise self._error_from_response(platform_key, response)

        data = response.json()
        raw_results = data.get("results") if isinstance(data, dict) else data
        if not isinstance(raw_results, list):
            return []
        return [
            _candidate_from_result(platform_key, item, index)
            for index, item in enumerate(raw_results, start=1)
            if isinstance(item, dict)
        ]

    @staticmethod
    def _error_from_response(platform: str, response: httpx.Response) -> PlatformMediaClientError:
        detail = response.text
        try:
            data = response.json()
        except ValueError:
            data = None
        if isinstance(data, dict) and data.get("detail"):
            detail = str(data["detail"])

        lowered = detail.lower()
        category = "platform_search_failed"
        for candidate in PLATFORM_ERROR_CATEGORIES:
            if candidate in lowered:
                category = candidate
                break
        if response.status_code == 401:
            category = "login_required"
        elif response.status_code == 503:
            category = "platform_unavailable"
        elif response.status_code in {400, 404}:
            category = "unsupported_url"
        return PlatformMediaClientError(platform, category, detail)


def _normalize_platform(platform: str) -> str:
    value = platform.strip().lower()
    if value == "youtube_shorts":
        return "youtube"
    if value not in SUPPORTED_SOURCE_PLATFORMS:
        raise PlatformMediaClientError(value or "unknown", "unsupported_url", f"Unsupported source platform '{platform}'")
    return value


def _candidate_from_result(platform: str, item: dict[str, Any], index: int) -> AutoFlowClipCandidate:
    identifier = str(item.get("id") or f"{platform}-{index}")
    title = str(item.get("title") or item.get("url") or identifier)
    url = str(item.get("url") or "").strip() or None
    duration = _number_or_none(item.get("duration"))
    metadata: dict[str, Any] = {
        "platform": str(item.get("platform") or platform),
        "duration": duration,
        "thumbnail": item.get("thumbnail"),
        "channel": item.get("channel"),
    }
    return AutoFlowClipCandidate(
        id=identifier,
        title=title,
        source_type=str(item.get("platform") or platform),
        url=url,
        start_sec=0,
        end_sec=duration if duration and duration <= 30 else None,
        rights_status="review_required",
        metadata={key: value for key, value in metadata.items() if value is not None},
    )


def _number_or_none(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None
