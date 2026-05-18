from __future__ import annotations

import re
from typing import Any

from worker.handlers.base import BaseHandler

VIDEO_INPUT_RE = re.compile(r"^video_(\d+)$")
LEGACY_TIMELINE_INPUT_ORDER = {"video_first": 1, "video_second": 2}


class ConcatManyHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        args = self.build_concat_args(node_config, input_paths, output_path)
        await self.run_ffmpeg(args)

    def build_concat_args(self, node_config: dict[str, Any], input_paths: dict[str, str], output_path: str) -> list[str]:
        selected_items = self._selected_input_items(node_config, input_paths)
        selected = [path for _handle, path in selected_items]
        if len(selected) < 2:
            raise ValueError("concat_many requires at least two video inputs")

        width, height = _target_dimensions(node_config, [handle for handle, _path in selected_items])
        normalize = self.parse_bool_param(node_config.get("normalize_resolution"), True)
        target_duration = _positive_float_or_none(node_config.get("target_duration"))

        args: list[str] = []
        for path in selected:
            args.extend(["-i", path])
        args.extend(["-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=48000"])

        filters = []
        for index in range(len(selected)):
            if normalize:
                filters.append(
                    f"[{index}:v]{self.scale_filter(width, height, force_original_aspect_ratio='decrease')},"
                    f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2,setsar=1[v{index}]"
                )
            else:
                filters.append(f"[{index}:v]setsar=1[v{index}]")
        filters.append("".join(f"[v{index}]" for index in range(len(selected))) + f"concat=n={len(selected)}:v=1:a=0[v]")

        args.extend(
            [
                "-filter_complex",
                ";".join(filters),
                "-map",
                "[v]",
                "-map",
                f"{len(selected)}:a",
                *self.intermediate_video_encode_args("libx264"),
                "-c:a",
                "aac",
                "-shortest",
            ]
        )
        if target_duration is not None:
            args.extend(["-t", _format_seconds(target_duration)])
        args.append(output_path)
        return args

    def _selected_inputs(self, node_config: dict[str, Any], input_paths: dict[str, str]) -> list[str]:
        return selected_video_inputs(input_paths)

    def _selected_input_items(self, node_config: dict[str, Any], input_paths: dict[str, str]) -> list[tuple[str, str]]:
        return selected_video_input_items(input_paths)


def selected_video_inputs(input_paths: dict[str, str]) -> list[str]:
    return [path for _handle, path in selected_video_input_items(input_paths)]


def selected_video_input_items(input_paths: dict[str, str]) -> list[tuple[str, str]]:
    indexed: dict[int, tuple[str, str]] = {}
    for handle, path in input_paths.items():
        match = VIDEO_INPUT_RE.match(handle)
        if match:
            indexed[int(match.group(1))] = (handle, path)
            continue
        legacy_index = LEGACY_TIMELINE_INPUT_ORDER.get(handle)
        if legacy_index is not None and legacy_index not in indexed:
            indexed[legacy_index] = (handle, path)
    return [indexed[index] for index in sorted(indexed)]


def _target_dimensions(node_config: dict[str, Any], selected_handles: list[str]) -> tuple[int, int]:
    aspect_ratio = str(node_config.get("aspect_ratio") or "9:16").strip().lower()
    if aspect_ratio == "auto":
        auto_dimensions = _auto_dimensions(node_config, selected_handles)
        if auto_dimensions is not None:
            return auto_dimensions
        return _configured_or_default_dimensions(node_config, (1080, 1920))
    return _configured_or_default_dimensions(node_config, _dimensions_for_aspect_ratio(aspect_ratio))


def _configured_or_default_dimensions(
    node_config: dict[str, Any],
    default_dimensions: tuple[int, int],
) -> tuple[int, int]:
    default_width, default_height = default_dimensions
    width = _positive_int_or_none(node_config.get("width")) or default_width
    height = _positive_int_or_none(node_config.get("height")) or default_height
    return width, height


def _dimensions_for_aspect_ratio(aspect_ratio: str) -> tuple[int, int]:
    if aspect_ratio == "16:9":
        return 1920, 1080
    if aspect_ratio == "1:1":
        return 1080, 1080
    return 1080, 1920


def _auto_dimensions(node_config: dict[str, Any], selected_handles: list[str]) -> tuple[int, int] | None:
    input_meta = node_config.get("_input_artifact_meta") or {}
    if not isinstance(input_meta, dict):
        return None
    for handle in selected_handles:
        dimensions = _dimensions_from_media_info(input_meta.get(handle) or {})
        if dimensions is not None:
            return dimensions
    return None


def _dimensions_from_media_info(media_info: Any) -> tuple[int, int] | None:
    if not isinstance(media_info, dict):
        return None
    width = _positive_int_or_none(media_info.get("width"))
    height = _positive_int_or_none(media_info.get("height"))
    if width is None or height is None:
        return None
    return width, height


def _positive_int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        numeric = int(float(value))
    except (TypeError, ValueError):
        return None
    return numeric if numeric > 0 else None


def _positive_float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    numeric = float(value)
    return numeric if numeric > 0 else None


def _format_seconds(value: float) -> str:
    return str(int(value)) if value.is_integer() else f"{value:.3f}".rstrip("0").rstrip(".")
