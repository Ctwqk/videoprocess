from __future__ import annotations

from worker.handlers.base import BaseHandler


class TitleOverlayHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        video = input_paths["input"]
        text = str(node_config.get("text") or "")
        position = node_config.get("position", "top")
        start = float(node_config.get("start_time") or 0)
        duration = float(node_config.get("duration") or 3)
        font_size = int(node_config.get("font_size") or 72)
        safe_area = self.parse_bool_param(node_config.get("safe_area"), True)

        y_expr = {
            "top": "h*0.12" if safe_area else "h*0.06",
            "center": "(h-text_h)/2",
            "bottom": "h-text_h-h*0.18" if safe_area else "h-text_h-h*0.08",
        }.get(position, "h*0.12")
        escaped = text.replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'")
        enable = f"between(t,{start},{start + duration})"
        drawtext = (
            f"drawtext=text='{escaped}':fontcolor=white:fontsize={font_size}:"
            f"box=1:boxcolor=black@0.45:boxborderw=18:x=(w-text_w)/2:y={y_expr}:enable='{enable}'"
        )
        args = [
            "-i",
            video,
            "-vf",
            drawtext,
            *self.build_video_encode_args("libx264", preset="fast", crf=23),
            "-c:a",
            "aac",
            output_path,
        ]
        await self.run_ffmpeg(args)
