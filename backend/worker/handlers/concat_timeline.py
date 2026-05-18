import os
import tempfile
from worker.handlers.base import BaseHandler
from worker.handlers.concat_many import ConcatManyHandler, selected_video_inputs


class ConcatTimelineHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        selected = selected_video_inputs(input_paths)
        if len(selected) < 2:
            raise ValueError("concat_timeline requires at least two video inputs")

        transition = node_config.get("transition", "none")
        transition_dur = float(node_config.get("transition_duration", 0.5))

        if transition == "none" or transition_dur <= 0:
            # Use concat demuxer for simple concatenation
            with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
                for path in selected:
                    f.write(f"file '{path}'\n")
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
            if len(selected) > 2:
                config = dict(node_config)
                config.setdefault("normalize_resolution", True)
                await ConcatManyHandler().execute(config, input_paths, output_path)
                return

            first, second = selected
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
                    *self.intermediate_video_encode_args("libx264"),
            ])
            if first_has_audio and second_has_audio:
                args.extend(["-map", "[a]", "-c:a", "aac"])
            elif first_has_audio:
                args.extend(["-map", "0:a:0", "-c:a", "aac"])
            elif second_has_audio:
                args.extend(["-map", "1:a:0", "-c:a", "aac"])
            args.append(output_path)
            await self.run_ffmpeg(args)
