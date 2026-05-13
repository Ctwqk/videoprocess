from __future__ import annotations

from worker.handlers.base import BaseHandler


class ReplaceAudioHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        video = input_paths["video"]
        audio = input_paths["audio"]
        loop_if_shorter = bool(node_config.get("loop_if_shorter", True))
        audio_volume = float(node_config.get("audio_volume", 1.0))

        probe = await self.run_ffprobe(video)
        duration = float(probe.get("format", {}).get("duration", 0) or 0)
        if duration <= 0:
            raise RuntimeError("Unable to determine input video duration")

        input_args = ["-i", video]
        if loop_if_shorter:
            input_args.extend(["-stream_loop", "-1", "-i", audio])
        else:
            input_args.extend(["-i", audio])

        filter_chain = f"[1:a]volume={audio_volume}"
        if not loop_if_shorter:
            filter_chain += ",apad"
        filter_chain += "[aout]"

        args = input_args + [
            "-filter_complex", filter_chain,
            "-map", "0:v:0",
            "-map", "[aout]",
            "-c:v", "copy",
            "-c:a", "aac",
            "-t", f"{duration:.3f}",
            output_path,
        ]
        await self.run_ffmpeg(args)
        return {
            "audio_replaced": True,
            "video_duration": duration,
        }
