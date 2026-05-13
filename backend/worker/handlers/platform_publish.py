from __future__ import annotations

import contextlib
import logging
import shutil
from pathlib import Path
from typing import Any

import httpx

from app.config import settings
from worker.handlers.base import BaseHandler


logger = logging.getLogger("worker")


class BasePlatformPublishHandler(BaseHandler):
    platform: str = ""
    manager_setting_name: str | None = None

    def _platform_manager_base_url(self) -> str:
        if self.manager_setting_name:
            override = getattr(settings, self.manager_setting_name, "")
            if override:
                return str(override).rstrip("/")
        return settings.platform_browser_manager_url.rstrip("/")

    def _media_port_names(self, input_paths: dict[str, str], node_config: dict[str, Any]) -> list[str]:
        return ["input"] if "input" in input_paths else []

    def _form_fields(self, node_config: dict[str, Any]) -> dict[str, str]:
        return {
            key: self._stringify_value(value)
            for key, value in node_config.items()
            if value not in (None, "")
        }

    @staticmethod
    def _stringify_value(value: Any) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    async def execute(self, node_config: dict, input_paths: dict[str, str], output_path: str) -> dict:
        media_port_names = self._media_port_names(input_paths, node_config)
        if not media_port_names:
            raise RuntimeError(f"{self.platform} upload requires at least one input media file")

        request_url = f"{self._platform_manager_base_url()}/api/platforms/{self.platform}/publish"
        data = self._form_fields(node_config)

        with contextlib.ExitStack() as stack:
            files = []
            primary_media = None
            for port_name in media_port_names:
                media_path = input_paths.get(port_name)
                if not media_path:
                    continue
                if primary_media is None:
                    primary_media = media_path
                file_handle = stack.enter_context(open(media_path, "rb"))
                files.append(
                    (
                        "media",
                        (
                            Path(media_path).name,
                            file_handle,
                            self._guess_mime_type(media_path),
                        ),
                    )
                )

            if not files:
                raise RuntimeError(f"{self.platform} upload did not receive any readable media files")

            async with httpx.AsyncClient(timeout=httpx.Timeout(600.0, connect=20.0)) as client:
                response = await client.post(request_url, data=data, files=files)

        if response.status_code >= 400:
            detail = response.text
            try:
                payload = response.json()
            except ValueError:
                payload = None
            if isinstance(payload, dict) and payload.get("detail"):
                detail = str(payload["detail"])
            raise RuntimeError(f"{self.platform} upload failed: {detail}")

        payload = response.json()
        if primary_media:
            shutil.copy2(primary_media, output_path)
        return {self.platform: payload}

    @staticmethod
    def _guess_mime_type(path: str) -> str:
        suffix = Path(path).suffix.lower()
        return {
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".png": "image/png",
            ".webp": "image/webp",
            ".gif": "image/gif",
            ".mp4": "video/mp4",
            ".mov": "video/quicktime",
            ".mkv": "video/x-matroska",
            ".webm": "video/webm",
        }.get(suffix, "application/octet-stream")


class XUploadHandler(BasePlatformPublishHandler):
    platform = "x"
    manager_setting_name = "x_platform_browser_manager_url"


class XiaohongshuUploadHandler(BasePlatformPublishHandler):
    platform = "xiaohongshu"
    manager_setting_name = "xiaohongshu_platform_browser_manager_url"

    async def execute(self, node_config: dict, input_paths: dict[str, str], output_path: str) -> dict:
        raise RuntimeError(
            "xiaohongshu upload is disabled by policy after account safety warnings; use Xiaohongshu search/download only"
        )

    def _media_port_names(self, input_paths: dict[str, str], node_config: dict[str, Any]) -> list[str]:
        ordered = [name for name in ("input", "image_2", "image_3", "image_4", "image_5", "image_6", "image_7", "image_8", "image_9") if name in input_paths]
        publish_mode = str(node_config.get("publish_mode") or "").strip().lower()
        if publish_mode == "video_note":
            return ordered[:1]
        return ordered
