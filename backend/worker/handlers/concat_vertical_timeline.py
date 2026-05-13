import asyncio
import os
import tempfile

from worker.handlers.base import BaseHandler


class ConcatVerticalTimelineHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        first_video = input_paths["video_first"]
        second_video = input_paths["video_second"]
        top_image = input_paths.get("image_top")
        bottom_image = input_paths.get("image_bottom")

        pane_width = int(node_config.get("pane_width", 640) or 640)
        pane_height = int(node_config.get("pane_height", 360) or 360)
        background_color = str(node_config.get("background_color", "black") or "black")

        generated_top_image = None
        generated_bottom_image = None
        if not top_image:
            generated_top_image = await self._extract_default_frame(
                video_path=first_video,
                prefer_from_end=True,
            )
            top_image = generated_top_image
        if not bottom_image:
            generated_bottom_image = await self._extract_default_frame(
                video_path=second_video,
                prefer_from_end=False,
            )
            bottom_image = generated_bottom_image

        first_segment = await self._render_segment(
            active_video=first_video,
            static_image=bottom_image,
            active_position="top",
            pane_width=pane_width,
            pane_height=pane_height,
            background_color=background_color,
        )
        second_segment = await self._render_segment(
            active_video=second_video,
            static_image=top_image,
            active_position="bottom",
            pane_width=pane_width,
            pane_height=pane_height,
            background_color=background_color,
        )

        try:
            filter_complex = "[0:v][0:a][1:v][1:a]concat=n=2:v=1:a=1[v][a]"
            args = [
                "-i", first_segment,
                "-i", second_segment,
                "-filter_complex", filter_complex,
                "-map", "[v]",
                "-map", "[a]",
                *self.build_video_encode_args("libx264", preset="fast", crf=23),
                "-c:a", "aac",
                output_path,
            ]
            await self.run_ffmpeg(args)
        finally:
            self._safe_unlink(first_segment)
            self._safe_unlink(second_segment)
            if generated_top_image:
                self._safe_unlink(generated_top_image)
            if generated_bottom_image:
                self._safe_unlink(generated_bottom_image)

    async def _render_segment(
        self,
        *,
        active_video: str,
        static_image: str,
        active_position: str,
        pane_width: int,
        pane_height: int,
        background_color: str,
    ) -> str:
        probe = await self.run_ffprobe(active_video)
        duration = float(probe.get("format", {}).get("duration", 0) or 0)
        if duration <= 0:
            duration = 5.0

        has_audio = any(stream.get("codec_type") == "audio" for stream in probe.get("streams", []))
        output_file = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
        output_file.close()

        if active_position == "top":
            top_source = "[0:v]"
            bottom_source = "[1:v]"
        else:
            top_source = "[1:v]"
            bottom_source = "[0:v]"

        filter_complex = (
            f"{top_source}scale={pane_width}:{pane_height}:force_original_aspect_ratio=decrease,"
            f"pad={pane_width}:{pane_height}:(ow-iw)/2:(oh-ih)/2:color={background_color},fps=30[top];"
            f"{bottom_source}scale={pane_width}:{pane_height}:force_original_aspect_ratio=decrease,"
            f"pad={pane_width}:{pane_height}:(ow-iw)/2:(oh-ih)/2:color={background_color},fps=30[bottom];"
            "[top][bottom]vstack=inputs=2[v]"
        )

        args = [
            "-i", active_video,
            "-loop", "1",
            "-t", f"{duration:.3f}",
            "-i", static_image,
        ]

        if not has_audio:
            args.extend([
                "-f", "lavfi",
                "-t", f"{duration:.3f}",
                "-i", "anullsrc=r=44100:cl=stereo",
            ])

        args.extend([
            "-filter_complex", filter_complex,
            "-map", "[v]",
        ])

        if has_audio:
            args.extend(["-map", "0:a:0"])
        else:
            args.extend(["-map", "2:a:0"])

        args.extend([
            *self.build_video_encode_args("libx264", preset="fast", crf=23),
            "-c:a", "aac",
            "-pix_fmt", "yuv420p",
            "-shortest",
            output_file.name,
        ])

        await self.run_ffmpeg(args)
        return output_file.name

    async def _extract_default_frame(self, *, video_path: str, prefer_from_end: bool) -> str:
        frame_count = await self._count_video_frames(video_path)
        if frame_count <= 0:
            frame_count = 1

        target_offset = 15
        if prefer_from_end:
            frame_index = max(frame_count - target_offset, 0)
        else:
            frame_index = min(target_offset - 1, frame_count - 1)

        output_file = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
        output_file.close()

        filter_expr = f"select=eq(n\\,{frame_index})"
        args = [
            "-i", video_path,
            "-vf", filter_expr,
            "-vsync", "vfr",
            "-frames:v", "1",
            output_file.name,
        ]
        await self.run_ffmpeg(args)
        return output_file.name

    async def _count_video_frames(self, video_path: str) -> int:
        cmd = [
            "ffprobe",
            "-v", "error",
            "-select_streams", "v:0",
            "-count_frames",
            "-show_entries", "stream=nb_read_frames",
            "-of", "default=nokey=1:noprint_wrappers=1",
            video_path,
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _stderr = await proc.communicate()
        if proc.returncode != 0:
            return 0
        raw = stdout.decode("utf-8", errors="replace").strip()
        try:
            return int(raw)
        except ValueError:
            return 0

    def _safe_unlink(self, path: str) -> None:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
