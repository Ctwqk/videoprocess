from worker.handlers.base import BaseHandler


async def execute_stack_concat(
    handler: BaseHandler,
    output_path: str,
    primary_path: str,
    secondary_path: str,
    *,
    primary_label: str,
    secondary_label: str,
    stack_axis: str,
    resize_mode: str,
) -> None:
    primary_probe = await handler.run_ffprobe(primary_path)
    secondary_probe = await handler.run_ffprobe(secondary_path)
    primary_has_audio = any(stream.get("codec_type") == "audio" for stream in primary_probe.get("streams", []))
    secondary_has_audio = any(stream.get("codec_type") == "audio" for stream in secondary_probe.get("streams", []))

    if stack_axis == "horizontal":
        if resize_mode == "match_height":
            filter_complex = (
                f"[0:v]scale=-2:480[{primary_label}];"
                f"[1:v]scale=-2:480[{secondary_label}];"
                f"[{primary_label}][{secondary_label}]hstack=inputs=2[v]"
            )
        elif resize_mode == "match_width":
            filter_complex = (
                f"[0:v]scale=640:-2[{primary_label}];"
                f"[1:v]scale=640:-2[{secondary_label}];"
                f"[{primary_label}][{secondary_label}]hstack=inputs=2[v]"
            )
        else:
            filter_complex = "[0:v][1:v]hstack=inputs=2[v]"
    else:
        if resize_mode == "match_width":
            filter_complex = (
                f"[0:v]scale=640:-2[{primary_label}];"
                f"[1:v]scale=640:-2[{secondary_label}];"
                f"[{primary_label}][{secondary_label}]vstack=inputs=2[v]"
            )
        elif resize_mode == "match_height":
            filter_complex = (
                f"[0:v]scale=-2:480[{primary_label}];"
                f"[1:v]scale=-2:480[{secondary_label}];"
                f"[{primary_label}][{secondary_label}]vstack=inputs=2[v]"
            )
        else:
            filter_complex = "[0:v][1:v]vstack=inputs=2[v]"

    if primary_has_audio and secondary_has_audio:
        filter_complex += ";[0:a][1:a]amix=inputs=2:duration=longest:dropout_transition=2[a]"

    args = [
        "-i", primary_path,
        "-i", secondary_path,
        "-filter_complex", filter_complex,
        "-map", "[v]",
    ]
    if primary_has_audio and secondary_has_audio:
        args.extend(["-map", "[a]", "-c:a", "aac"])
    elif primary_has_audio:
        args.extend(["-map", "0:a:0", "-c:a", "aac"])
    elif secondary_has_audio:
        args.extend(["-map", "1:a:0", "-c:a", "aac"])
    args.extend([
        *handler.build_video_encode_args("libx264", preset="fast", crf=23),
        output_path,
    ])
    await handler.run_ffmpeg(args)

