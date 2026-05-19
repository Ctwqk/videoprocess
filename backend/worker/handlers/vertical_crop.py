from __future__ import annotations

from worker.handlers.base import BaseHandler


class VerticalCropHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        video = input_paths["input"]
        width = int(node_config.get("width") or 1080)
        height = int(node_config.get("height") or 1920)
        mode = node_config.get("mode", "center_crop")

        if mode == "blur_bg":
            vf = (
                f"[0:v]{self.scale_filter(width, height, force_original_aspect_ratio='increase')},"
                f"crop={width}:{height},boxblur=20:1[bg];"
                f"[0:v]{self.scale_filter(width, height, force_original_aspect_ratio='decrease')}[fg];"
                f"[bg][fg]overlay=(W-w)/2:(H-h)/2,setsar=1[v]"
            )
            args = (
                [
                    "-i",
                    video,
                    "-filter_complex",
                    vf,
                    "-map",
                    "[v]",
                    "-map",
                    "0:a?",
                ]
                + self.intermediate_video_encode_args("libx264")
                + [
                    "-c:a",
                    "aac",
                    output_path,
                ]
            )
        else:
            vf = f"{self.scale_filter(width, height, force_original_aspect_ratio='increase')},crop={width}:{height},setsar=1"
            args = [
                "-i",
                video,
                "-vf",
                vf,
                *self.intermediate_video_encode_args("libx264"),
                "-c:a",
                "aac",
                output_path,
            ]
        await self.run_ffmpeg(args)
