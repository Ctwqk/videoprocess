from worker.handlers.base import BaseHandler


class BgmHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        video = input_paths["video"]
        audio = input_paths["audio"]
        volume = float(node_config.get("volume", 0.3))
        original_volume = float(node_config.get("original_volume", 1.0))
        loop = node_config.get("loop", True)
        fade_in = float(node_config.get("fade_in", 0))
        fade_out = float(node_config.get("fade_out", 0))

        probe = await self.run_ffprobe(video)
        video_duration = float(probe.get("format", {}).get("duration", 0))
        video_has_audio = any(stream.get("codec_type") == "audio" for stream in probe.get("streams", []))

        bgm_filters = [f"volume={volume}"]
        if fade_in > 0:
            bgm_filters.append(f"afade=t=in:d={fade_in}")
        if fade_out > 0 and video_duration > 0:
            fade_start = max(0, video_duration - fade_out)
            bgm_filters.append(f"afade=t=out:st={fade_start}:d={fade_out}")

        bgm_filter_chain = ",".join(bgm_filters)
        sidechain_audio_format = "aformat=sample_fmts=fltp:channel_layouts=stereo"

        input_args = ["-i", video]
        if loop:
            input_args.extend(["-stream_loop", "-1", "-i", audio])
        else:
            input_args.extend(["-i", audio])

        if video_has_audio:
            filter_complex = (
                f"[0:a]aresample=48000:async=1,{sidechain_audio_format},"
                f"volume={original_volume},asplit=2[orig_mix][orig_sidechain];"
                f"[1:a]aresample=48000:async=1,{sidechain_audio_format},{bgm_filter_chain}[bgm];"
                "[bgm][orig_sidechain]sidechaincompress=threshold=0.03:ratio=8:attack=200:release=800[ducked];"
                "[orig_mix][ducked]amix=inputs=2:duration=first:normalize=0[mix];"
                "[mix]loudnorm=I=-16:LRA=11:TP=-1.5[a]"
            )
        else:
            filter_complex = f"[1:a]aresample=48000:async=1,{bgm_filter_chain},loudnorm=I=-16:LRA=11:TP=-1.5[a]"

        args = input_args + [
            "-filter_complex", filter_complex,
            "-map", "0:v", "-map", "[a]",
            "-c:v", "copy",
            "-c:a", "aac",
            "-ar", "48000",
            "-ac", "2",
            "-shortest",
            output_path,
        ]
        await self.run_ffmpeg(args)
