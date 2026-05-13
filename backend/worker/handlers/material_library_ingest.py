from __future__ import annotations

import asyncio
import json
import os
import tempfile
import uuid

from app.db import async_session
from app.services.material_service import ingest_material_asset
from worker.handlers.base import BaseHandler
from worker.handlers.speech_to_subtitle import SpeechToSubtitleHandler
from worker.handlers.subtitle_utils import parse_srt


class MaterialLibraryIngestHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        video_path = input_paths["video"]
        source_meta = ((node_config.get("_input_artifact_meta") or {}).get("video") or {})
        source_asset_id = source_meta.get("source_asset_id") or source_meta.get("asset_id")
        if not source_asset_id:
            raise RuntimeError("material_library_ingest requires a source asset_id from the upstream Source node")

        target_library_ids = self._parse_uuid_list(node_config.get("target_library_ids"))
        if not target_library_ids:
            raise RuntimeError("material_library_ingest requires one or more target_library_ids")

        clip_len = float(node_config.get("clip_len", 8) or 8)
        stride = float(node_config.get("stride", 4) or 4)
        subtitle_mode = str(node_config.get("subtitle_mode", "asr_if_missing") or "asr_if_missing")
        store_neighbors = self.parse_bool_param(node_config.get("store_neighbors"), True)
        source_probe = await self.run_ffprobe(video_path)
        fallback_media_info = {
            "duration": float((source_probe.get("format", {}) or {}).get("duration", 0) or 0),
            "format_name": (source_probe.get("format", {}) or {}).get("format_name"),
        }
        for stream in source_probe.get("streams", []):
            if stream.get("codec_type") == "video" and "video" not in fallback_media_info:
                fallback_media_info["video"] = {
                    "codec": stream.get("codec_name"),
                    "width": stream.get("width"),
                    "height": stream.get("height"),
                    "fps": stream.get("r_frame_rate"),
                }
            if stream.get("codec_type") == "audio" and "audio" not in fallback_media_info:
                fallback_media_info["audio"] = {
                    "codec": stream.get("codec_name"),
                    "sample_rate": stream.get("sample_rate"),
                    "channels": stream.get("channels"),
                }

        subtitle_handler = SpeechToSubtitleHandler()
        fd, subtitle_path = tempfile.mkstemp(prefix="material_ingest_", suffix=".srt")
        os.close(fd)
        try:
            result = await subtitle_handler.execute(
                {
                    "model": node_config.get("asr_model", "small"),
                    "language": node_config.get("language", "zh"),
                    "merge_adjacent": True,
                },
                {"media": video_path},
                subtitle_path,
            )
            with open(subtitle_path, "r", encoding="utf-8") as handle:
                subtitle_cues = parse_srt(handle.read())
            async with async_session() as db:
                ingest_result = await ingest_material_asset(
                    db,
                    asset_id=uuid.UUID(str(source_asset_id)),
                    library_ids=target_library_ids,
                    clip_len=clip_len,
                    stride=stride,
                    subtitle_mode=subtitle_mode,
                    subtitle_cues=subtitle_cues,
                    fallback_media_info=fallback_media_info,
                    store_neighbors=store_neighbors,
                )
            payload = {
                **ingest_result,
                "subtitle_segments": result.get("subtitle_segments"),
            }
            with open(output_path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2)
            return payload
        finally:
            try:
                os.unlink(subtitle_path)
            except OSError:
                pass

    @staticmethod
    def _parse_uuid_list(value) -> list[uuid.UUID]:
        if isinstance(value, list):
            raw_values = value
        else:
            raw_values = [part.strip() for part in str(value or "").split(",") if part.strip()]
        parsed: list[uuid.UUID] = []
        for raw in raw_values:
            parsed.append(uuid.UUID(str(raw)))
        return parsed
