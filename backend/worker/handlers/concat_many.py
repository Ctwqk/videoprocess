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
        selected = self._selected_inputs(node_config, input_paths)
        if len(selected) < 2:
            raise ValueError("concat_many requires at least two video inputs")

        width = int(node_config.get("width") or 1080)
        height = int(node_config.get("height") or 1920)
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


def selected_video_inputs(input_paths: dict[str, str]) -> list[str]:
    indexed: dict[int, str] = {}
    for handle, path in input_paths.items():
        match = VIDEO_INPUT_RE.match(handle)
        if match:
            indexed[int(match.group(1))] = path
            continue
        legacy_index = LEGACY_TIMELINE_INPUT_ORDER.get(handle)
        if legacy_index is not None and legacy_index not in indexed:
            indexed[legacy_index] = path
    return [indexed[index] for index in sorted(indexed)]


def _positive_float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    numeric = float(value)
    return numeric if numeric > 0 else None


def _format_seconds(value: float) -> str:
    return str(int(value)) if value.is_integer() else f"{value:.3f}".rstrip("0").rstrip(".")
