from __future__ import annotations

from worker.handlers.base import BaseHandler


class ConcatManyHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        input_count = int(node_config.get("input_count") or len(input_paths) or 2)
        selected = [
            input_paths[f"video_{index}"]
            for index in range(1, min(input_count, 12) + 1)
            if input_paths.get(f"video_{index}")
        ]
        if len(selected) < 2:
            raise ValueError("concat_many requires at least two video inputs")

        width = int(node_config.get("width") or 1080)
        height = int(node_config.get("height") or 1920)
        args: list[str] = []
        for path in selected:
            args.extend(["-i", path])

        filters = []
        for index in range(len(selected)):
            filters.append(
                f"[{index}:v]scale={width}:{height}:force_original_aspect_ratio=decrease,"
                f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2,setsar=1[v{index}]"
            )
        filters.append("".join(f"[v{index}]" for index in range(len(selected))) + f"concat=n={len(selected)}:v=1:a=0[v]")

        args.extend([
            "-filter_complex",
            ";".join(filters),
            "-map",
            "[v]",
            *self.build_video_encode_args("libx264", preset="fast", crf=23),
            output_path,
        ])
        await self.run_ffmpeg(args)
