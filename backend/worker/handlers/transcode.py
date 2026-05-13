from worker.handlers.base import BaseHandler


class TranscodeHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        video = input_paths["input"]
        video_codec = self.preferred_video_codec(node_config.get("video_codec", "libx264"))
        audio_codec = node_config.get("audio_codec", "aac")
        resolution = node_config.get("resolution", "")
        bitrate = node_config.get("bitrate", "")
        crf = node_config.get("crf", 23)
        preset = node_config.get("preset", "medium")

        args = ["-i", video]

        if video_codec == "copy":
            args.extend(["-c:v", "copy"])
        elif video_codec == "libvpx-vp9":
            args.extend(["-c:v", "libvpx-vp9", "-crf", str(int(crf)), "-b:v", "0"])
            if bitrate:
                args.extend(["-b:v", bitrate])
        else:
            args.extend(self.build_video_encode_args(video_codec, preset=preset, crf=crf, bitrate=bitrate))

        # Resolution (convert WxH → W:H for ffmpeg scale filter)
        if video_codec != "copy" and resolution and resolution != "original":
            scale_val = resolution.replace("x", ":")
            args.extend(["-vf", f"scale={scale_val}"])

        # Audio codec
        args.extend(["-c:a", audio_codec])

        args.append(output_path)
        await self.run_ffmpeg(args)
