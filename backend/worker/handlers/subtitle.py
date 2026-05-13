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

        escaped_subtitle = subtitle_file.replace("\\", "\\\\").replace(":", "\\:").replace("'", "\\'")

        style = (
            f"FontSize={font_size},"
            f"PrimaryColour=&H00FFFFFF,"
            f"OutlineColour=&H00000000,"
            f"Alignment={alignment}"
        )

        vf = f"subtitles='{escaped_subtitle}':force_style='{style}'"

        args = [
            "-i", video,
            "-vf", vf,
            *self.build_video_encode_args("libx264", preset="fast", crf=23),
            "-c:a", "copy",
            output_path,
        ]
        await self.run_ffmpeg(args)
