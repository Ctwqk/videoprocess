from __future__ import annotations

from typing import Any

from worker.handlers.base import BaseHandler


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
                    f"[{index}:v]scale={width}:{height}:force_original_aspect_ratio=decrease,"
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
                *self.build_video_encode_args("libx264", preset="fast", crf=23),
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
        input_count = node_config.get("input_count")
        max_index = min(int(input_count), 12) if input_count is not None else 12
        return [
            input_paths[f"video_{index}"]
            for index in range(1, max_index + 1)
            if input_paths.get(f"video_{index}")
        ]


def _positive_float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    numeric = float(value)
    return numeric if numeric > 0 else None


def _format_seconds(value: float) -> str:
    return str(int(value)) if value.is_integer() else f"{value:.3f}".rstrip("0").rstrip(".")
