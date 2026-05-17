from __future__ import annotations

from worker.handlers.concat_many import ConcatManyHandler


class MontageAssemblerHandler(ConcatManyHandler):
    async def execute(self, node_config, input_paths, output_path):
        config = dict(node_config)
        width, height = _dimensions(config)
        config.setdefault("width", width)
        config.setdefault("height", height)
        config.setdefault("normalize_resolution", True)
        config.setdefault("input_count", _input_count(input_paths))
        await super().execute(config, input_paths, output_path)


def _dimensions(config: dict) -> tuple[int, int]:
    width = config.get("width")
    height = config.get("height")
    if width and height:
        return int(width), int(height)

    aspect_ratio = str(config.get("aspect_ratio") or "9:16")
    if aspect_ratio == "16:9":
        return 1920, 1080
    if aspect_ratio == "1:1":
        return 1080, 1080
    return 1080, 1920


def _input_count(input_paths: dict[str, str]) -> int:
    return len([key for key in input_paths if key.startswith("video_")])
