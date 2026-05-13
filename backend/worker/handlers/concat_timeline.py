import os
import tempfile
from worker.handlers.base import BaseHandler


class ConcatTimelineHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        first = input_paths["video_first"]
        second = input_paths["video_second"]
        transition = node_config.get("transition", "none")
        transition_dur = float(node_config.get("transition_duration", 0.5))

        if transition == "none" or transition_dur <= 0:
            # Use concat demuxer for simple concatenation
            with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
                f.write(f"file '{first}'\n")
                f.write(f"file '{second}'\n")
                concat_file = f.name

            try:
                args = [
                    "-f", "concat", "-safe", "0",
                    "-i", concat_file,
                    "-c", "copy",
                    output_path,
                ]
                await self.run_ffmpeg(args)
            finally:
                os.unlink(concat_file)
        else:
            # Use xfade filter for transitions
            # Need to know duration of first video
            probe = await self.run_ffprobe(first)
            duration = float(probe.get("format", {}).get("duration", 5))
            offset = max(0, duration - transition_dur)
            first_has_audio = any(stream.get("codec_type") == "audio" for stream in probe.get("streams", []))
            second_probe = await self.run_ffprobe(second)
            second_has_audio = any(stream.get("codec_type") == "audio" for stream in second_probe.get("streams", []))

            transition_name = "fade" if transition == "fade" else "dissolve"
            filter_complex = [
                f"[0:v][1:v]xfade=transition={transition_name}:duration={transition_dur}:offset={offset}[v]"
            ]
            args = [
                "-i", first,
                "-i", second,
            ]
            if first_has_audio and second_has_audio:
                filter_complex.append(f"[0:a][1:a]acrossfade=d={transition_dur}[a]")
            args.extend([
                "-filter_complex", ";".join(filter_complex),
                "-map", "[v]",
                *self.build_video_encode_args("libx264", preset="fast", crf=23),
            ])
            if first_has_audio and second_has_audio:
                args.extend(["-map", "[a]", "-c:a", "aac"])
            elif first_has_audio:
                args.extend(["-map", "0:a:0", "-c:a", "aac"])
            elif second_has_audio:
                args.extend(["-map", "1:a:0", "-c:a", "aac"])
            args.append(output_path)
            await self.run_ffmpeg(args)
