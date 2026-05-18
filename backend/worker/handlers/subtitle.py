from worker.handlers.base import BaseHandler


class SubtitleHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        video = input_paths["video"]
        subtitle_file = input_paths["subtitle_file"]
        font_size = int(node_config.get("font_size", 24))
        font_color = node_config.get("font_color", "white")
        outline_color = node_config.get("outline_color", "black")
        position = node_config.get("position", "bottom")

        alignment_map = {
            "bottom": 2,
            "top": 8,
            "center": 5,
        }
        alignment = alignment_map.get(position, 2)
        probe = await self.run_ffprobe(video)
        height = _video_height(probe)
        font_size_px = max(1, int(font_size * height / 720))

        escaped_subtitle = subtitle_file.replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'")

        style = (
            f"FontName=PingFang SC,"
            f"FontSize={font_size_px},"
            f"PrimaryColour={_color_to_ass(font_color)},"
            f"OutlineColour={_color_to_ass(outline_color)},"
            f"BorderStyle=1,"
            f"Outline=2,"
            f"Shadow=1,"
            f"MarginV={int(height * 0.05)},"
            f"Alignment={alignment}"
        )

        vf = f"subtitles='{escaped_subtitle}':force_style='{style}'"

        args = [
            "-i", video,
            "-vf", vf,
            *self.intermediate_video_encode_args("libx264"),
            "-c:a", "copy",
            output_path,
        ]
        await self.run_ffmpeg(args)


def _video_height(probe: dict) -> int:
    for stream in probe.get("streams", []):
        if stream.get("codec_type") == "video":
            try:
                return int(stream.get("height") or 1080)
            except (TypeError, ValueError):
                return 1080
    return 1080


def _color_to_ass(color: str) -> str:
    value = str(color or "").strip()
    presets = {
        "white": "&H00FFFFFF",
        "black": "&H00000000",
        "yellow": "&H0000FFFF",
        "red": "&H000000FF",
        "green": "&H0000FF00",
        "blue": "&H00FF0000",
    }
    if value.startswith("#") and len(value) == 7:
        return f"&H00{value[5:7]}{value[3:5]}{value[1:3]}".upper()
    return presets.get(value.lower(), "&H00FFFFFF")
