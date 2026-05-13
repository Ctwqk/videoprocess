from worker.handlers.base import BaseHandler


class TrimHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        video = input_paths["input"]
        start = node_config.get("start_time", "00:00:00")
        end = node_config.get("end_time", "")
        duration = node_config.get("duration", "")

        args = []
        if start:
            args.extend(["-ss", start])
        args.extend(["-i", video])
        if end:
            args.extend(["-to", end])
        elif duration:
            args.extend(["-t", duration])
        args.extend([
            "-map", "0:v:0",
            "-map", "0:a?",
            *self.build_video_encode_args("libx264", preset="fast", crf=23),
            "-c:a", "aac",
            output_path,
        ])

        await self.run_ffmpeg(args)
