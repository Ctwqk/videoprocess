import asyncio
import hashlib
import logging
import os
import re
import shutil
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import httpx

from app.config import settings
from app.storage.manager import get_storage
from worker.handlers.base import BaseHandler, CancelledError

logger = logging.getLogger(__name__)


class UrlDownloadHandler(BaseHandler):
    async def execute(self, node_config: dict, input_paths: dict[str, str], output_path: str) -> dict:
        url = node_config.get("url", "")
        if not url:
            raise ValueError("No URL provided")

        normalized_url = self._normalize_url(url)
        fmt = node_config.get("format", "best")
        cache_path = self._cache_storage_path(normalized_url, fmt, output_path)
        if await self._restore_from_cache(cache_path, output_path):
            logger.info("URL download cache hit for %s (%s)", normalized_url, fmt)
            return {
                "_storage_path": cache_path,
                "_skip_upload": True,
                "cache_hit": True,
                "source_url": normalized_url,
            }

        logger.info("URL download cache miss for %s (%s)", normalized_url, fmt)

        platform = self._detect_platform(normalized_url)
        if platform in {"xiaohongshu", "bilibili", "x"}:
            await self._download_via_platform_manager(platform, normalized_url, fmt, output_path)
        else:
            await self._download_via_ytdlp(normalized_url, fmt, output_path)

        await self._save_to_cache(cache_path, output_path)
        return {
            "_storage_path": cache_path,
            "_skip_upload": True,
            "cache_hit": False,
            "source_url": normalized_url,
        }

    async def _download_via_ytdlp(self, normalized_url: str, fmt: str, output_path: str) -> None:
        yt_dlp = self.resolve_executable("yt-dlp")

        args = [
            yt_dlp,
            "--no-playlist",
            "--merge-output-format", "mp4",
            "-o", output_path,
        ]

        format_map = {
            "1080p": "bestvideo[height<=1080]+bestaudio/best[height<=1080]",
            "720p": "bestvideo[height<=720]+bestaudio/best[height<=720]",
            "480p": "bestvideo[height<=480]+bestaudio/best[height<=480]",
            "audio_only": "bestaudio",
        }
        if fmt in format_map:
            args.extend(["-f", format_map[fmt]])

        args.append(normalized_url)

        if self._cancelled:
            raise CancelledError("Cancelled before download")

        logger.info(f"Running: {' '.join(args)}")
        self._proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await self._proc.communicate()

        if self._cancelled:
            raise CancelledError("Cancelled during download")
        if self._proc.returncode != 0:
            stderr_text = stderr.decode("utf-8", errors="replace")
            raise RuntimeError(self._format_download_error(normalized_url, self._proc.returncode, stderr_text))

    async def _download_via_platform_manager(self, platform: str, normalized_url: str, fmt: str, output_path: str) -> None:
        base_url = self._platform_manager_base_url(platform)
        request_url = f"{base_url}/api/platforms/{platform}/download"

        async with httpx.AsyncClient(timeout=httpx.Timeout(300.0, connect=10.0)) as client:
            try:
                response = await client.post(request_url, json={"url": normalized_url, "format": fmt})
            except httpx.HTTPError as exc:
                raise RuntimeError(self._format_platform_error(
                    platform,
                    normalized_url,
                    "platform_unavailable",
                    f"could not reach platform browser manager: {exc}",
                )) from exc

            if response.status_code >= 400:
                raise RuntimeError(self._format_platform_service_failure(platform, normalized_url, response))

            payload = response.json()
            download_id = str(payload.get("download_id") or "").strip()
            if not download_id:
                raise RuntimeError(self._format_platform_error(
                    platform,
                    normalized_url,
                    "platform_download_failed",
                    "missing download_id in platform response",
                ))

            download_url = f"{base_url}/api/platforms/{platform}/downloads/{download_id}"
            try:
                async with client.stream("GET", download_url) as download_response:
                    if download_response.status_code >= 400:
                        detail = await self._extract_error_detail(download_response)
                        raise RuntimeError(self._format_platform_error(
                            platform,
                            normalized_url,
                            "platform_download_failed",
                            detail,
                        ))
                    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
                    with open(output_path, "wb") as output_file:
                        async for chunk in download_response.aiter_bytes():
                            if self._cancelled:
                                raise CancelledError("Cancelled during platform download")
                            output_file.write(chunk)
            except httpx.HTTPError as exc:
                raise RuntimeError(self._format_platform_error(
                    platform,
                    normalized_url,
                    "platform_download_failed",
                    f"failed to fetch downloaded media: {exc}",
                )) from exc

    @staticmethod
    def _platform_manager_base_url(platform: str) -> str:
        if platform == "xiaohongshu" and settings.xiaohongshu_platform_browser_manager_url:
            return settings.xiaohongshu_platform_browser_manager_url.rstrip("/")
        if platform == "bilibili" and settings.bilibili_platform_browser_manager_url:
            return settings.bilibili_platform_browser_manager_url.rstrip("/")
        if platform == "x" and settings.x_platform_browser_manager_url:
            return settings.x_platform_browser_manager_url.rstrip("/")
        return settings.platform_browser_manager_url.rstrip("/")

    @staticmethod
    def _cache_storage_path(url: str, fmt: str, output_path: str) -> str:
        cache_key = hashlib.sha256(f"{url}\n{fmt}".encode("utf-8")).hexdigest()
        suffix = Path(output_path).suffix or ".mp4"
        return f"download-cache/{cache_key}{suffix}"

    @staticmethod
    def _normalize_url(url: str) -> str:
        parsed = urlparse(url.strip())
        host = parsed.netloc.lower()

        if "youtu.be" in host:
            video_id = parsed.path.strip("/")
            if video_id:
                return f"https://www.youtube.com/watch?v={video_id}"

        if "youtube.com" in host:
            query = dict(parse_qsl(parsed.query, keep_blank_values=False))
            video_id = query.get("v", "").strip()
            if video_id:
                return f"https://www.youtube.com/watch?v={video_id}"

        bilibili_id = UrlDownloadHandler._extract_bilibili_bvid(url)
        if bilibili_id:
            return f"https://www.bilibili.com/video/{bilibili_id}"

        xiaohongshu_note_id = UrlDownloadHandler._extract_xiaohongshu_note_id(url)
        if xiaohongshu_note_id:
            return f"https://www.xiaohongshu.com/explore/{xiaohongshu_note_id}"

        x_post_id, x_screen_name = UrlDownloadHandler._extract_x_post_components(url)
        if x_post_id and x_screen_name:
            return f"https://x.com/{x_screen_name}/status/{x_post_id}"

        normalized_query = urlencode(sorted(parse_qsl(parsed.query, keep_blank_values=True)))
        cleaned = parsed._replace(
            scheme=(parsed.scheme or "https").lower(),
            netloc=host,
            fragment="",
            query=normalized_query,
        )
        return urlunparse(cleaned)

    async def _restore_from_cache(self, cache_path: str, output_path: str) -> bool:
        storage = get_storage(settings.storage_backend)
        if not await storage.exists(cache_path):
            return False

        local_cached_path = storage.get_local_path(cache_path)
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        if local_cached_path and os.path.exists(local_cached_path):
            shutil.copy2(local_cached_path, output_path)
            return True

        content = await storage.read(cache_path)
        with open(output_path, "wb") as f:
            f.write(content)
        return True

    async def _save_to_cache(self, cache_path: str, output_path: str) -> None:
        storage = get_storage(settings.storage_backend)
        if await storage.exists(cache_path):
            return
        with open(output_path, "rb") as f:
            await storage.save(cache_path, f)

    @staticmethod
    def _detect_platform(url: str) -> str | None:
        host = urlparse(url).netloc.lower()
        if any(domain in host for domain in ("xiaohongshu.com", "xhslink.com")):
            return "xiaohongshu"
        if any(domain in host for domain in ("bilibili.com", "b23.tv")):
            return "bilibili"
        if any(domain in host for domain in ("x.com", "twitter.com")):
            return "x"
        if any(domain in host for domain in ("youtube.com", "youtu.be")):
            return "youtube"
        return None

    @staticmethod
    def _extract_bilibili_bvid(url: str) -> str | None:
        match = re.search(r"(BV[0-9A-Za-z]+)", url)
        return match.group(1) if match else None

    @staticmethod
    def _extract_xiaohongshu_note_id(url: str) -> str | None:
        match = re.search(r"([0-9a-f]{24})", url, flags=re.IGNORECASE)
        return match.group(1) if match else None

    @staticmethod
    def _extract_x_post_components(url: str) -> tuple[str | None, str | None]:
        match = re.search(r"(?:x|twitter)\.com/([^/]+)/status/(\d+)", url, flags=re.IGNORECASE)
        if not match:
            return None, None
        return match.group(2), match.group(1)

    @staticmethod
    async def _extract_error_detail(response: httpx.Response) -> str:
        try:
            payload = await response.aread()
        except httpx.HTTPError:
            return f"status {response.status_code}"
        text = payload.decode("utf-8", errors="replace")
        try:
            data = response.json()
        except ValueError:
            return text or f"status {response.status_code}"
        if isinstance(data, dict):
            detail = data.get("detail")
            if detail:
                return str(detail)
        return text or f"status {response.status_code}"

    @classmethod
    def _format_platform_service_failure(cls, platform: str, url: str, response: httpx.Response) -> str:
        detail = response.text
        try:
            payload = response.json()
        except ValueError:
            payload = None
        if isinstance(payload, dict) and payload.get("detail"):
            detail = str(payload["detail"])

        lowered = detail.lower()
        if response.status_code == 401 or "login_required" in lowered:
            category = "login_required"
        elif response.status_code == 503 or "platform_unavailable" in lowered:
            category = "platform_unavailable"
        elif response.status_code in {400, 404}:
            category = "unsupported_url"
        else:
            category = "platform_download_failed"
        return cls._format_platform_error(platform, url, category, detail)

    @staticmethod
    def _format_platform_error(platform: str, url: str, category: str, detail: str) -> str:
        platform_label = {
            "xiaohongshu": "Xiaohongshu",
            "bilibili": "Bilibili",
        }.get(platform, platform)
        trimmed = detail.strip() or "(no detail provided)"
        return (
            f"URL Download failed: {platform_label} download error.\n"
            f"URL: {url}\n"
            f"Category: {category}\n"
            f"Details:\n{trimmed}"
        )

    @staticmethod
    def _format_download_error(url: str, exit_code: int, stderr_text: str) -> str:
        lowered = stderr_text.lower()
        normalized = lowered.replace("’", "'")
        details = UrlDownloadHandler._trim_error_details(stderr_text)

        if "sign in to confirm you're not a bot" in normalized:
            return (
                "URL Download failed: YouTube is rate-limiting or bot-checking this request.\n"
                f"URL: {url}\n"
                "Category: rate_limited\n"
                f"Details:\n{details}"
            )

        if "http error 429" in lowered or "too many requests" in lowered:
            return (
                "URL Download failed: YouTube returned Too Many Requests.\n"
                f"URL: {url}\n"
                "Category: rate_limited\n"
                f"Details:\n{details}"
            )

        if "private video" in lowered or "this is a private video" in lowered:
            return (
                "URL Download failed: this video is private.\n"
                f"URL: {url}\n"
                "Category: private_video\n"
                f"Details:\n{details}"
            )

        if "members-only" in lowered or "members only" in lowered:
            return (
                "URL Download failed: this video is members-only.\n"
                f"URL: {url}\n"
                "Category: membership_restricted\n"
                f"Details:\n{details}"
            )

        if "video unavailable" in lowered or "this video is not available" in lowered:
            return (
                "URL Download failed: video unavailable or non-video YouTube result.\n"
                f"URL: {url}\n"
                "Category: unavailable_video\n"
                "Hint: this often means the search result was deleted, made private, region/age restricted, "
                "or was actually a channel/playlist result instead of a real video.\n"
                f"Details:\n{details}"
            )

        if "unsupported url" in lowered or "unsupported url:" in lowered:
            return (
                "URL Download failed: unsupported URL.\n"
                f"URL: {url}\n"
                "Category: unsupported_url\n"
                f"Details:\n{details}"
            )

        return (
            f"yt-dlp failed (exit {exit_code}).\n"
            f"URL: {url}\n"
            "Category: unknown_download_error\n"
            f"Details:\n{details}"
        )

    @staticmethod
    def _trim_error_details(stderr_text: str) -> str:
        lines = [line.rstrip() for line in stderr_text.splitlines() if line.strip()]
        if not lines:
            return "(no stderr output)"
        return "\n".join(lines[-12:])
