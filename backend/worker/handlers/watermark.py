from worker.handlers.base import BaseHandler


class WatermarkHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        video = input_paths["video"]
        image = input_paths["overlay"]
        position = node_config.get("position", "bottom_right")
        opacity = float(node_config.get("opacity", 0.8))
        scale = float(node_config.get("scale", 0.15))
        margin = int(node_config.get("margin", 10))

        # Scale watermark relative to video width
        scale_filter = f"[1:v]scale=iw*{scale}:-1,format=rgba,colorchannelmixer=aa={opacity}[wm]"

        # Position mapping
        positions = {
            "top_left": f"{margin}:{margin}",
            "top_right": f"W-w-{margin}:{margin}",
            "bottom_left": f"{margin}:H-h-{margin}",
            "bottom_right": f"W-w-{margin}:H-h-{margin}",
            "center": "(W-w)/2:(H-h)/2",
        }
        overlay_pos = positions.get(position, positions["bottom_right"])

        filter_complex = f"{scale_filter};[0:v][wm]overlay={overlay_pos}[v]"

        args = [
            "-i", video,
            "-i", image,
            "-filter_complex", filter_complex,
            "-map", "[v]", "-map", "0:a?",
            *self.build_video_encode_args("libx264", preset="fast", crf=23),
            "-c:a", "copy",
            output_path,
        ]
        await self.run_ffmpeg(args)
