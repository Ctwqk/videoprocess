from __future__ import annotations

import asyncio
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from worker.handlers.bgm import BgmHandler
from worker.handlers.concat_horizontal import ConcatHorizontalHandler
from worker.handlers.concat_timeline import ConcatTimelineHandler
from worker.handlers.concat_vertical import ConcatVerticalHandler
from worker.handlers.subtitle import SubtitleHandler
from worker.handlers.transcode import TranscodeHandler
from worker.handlers.trim import TrimHandler
from worker.handlers.watermark import WatermarkHandler


TMP_ROOT = Path("/tmp/vp_node_matrix")


def run_cmd(args: list[str]) -> None:
    subprocess.run(args, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def create_fixture_files(tmp_dir: Path) -> dict[str, str]:
    video_a = tmp_dir / "video_a.mp4"
    video_b = tmp_dir / "video_b.mp4"
    bgm = tmp_dir / "bgm.mp3"
    logo = tmp_dir / "logo.png"
    subtitle = tmp_dir / "sample.srt"

    run_cmd(
        [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "color=c=blue:s=640x360:d=2",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=660:duration=2",
            "-vf",
            "drawtext=text='A':fontcolor=white:fontsize=72:x=(w-text_w)/2:y=(h-text_h)/2",
            "-c:v",
            "libx264",
            "-c:a",
            "aac",
            "-shortest",
            str(video_a),
        ]
    )
    run_cmd(
        [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "color=c=green:s=640x360:d=2",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=880:duration=2",
            "-vf",
            "drawtext=text='B':fontcolor=white:fontsize=72:x=(w-text_w)/2:y=(h-text_h)/2",
            "-c:v",
            "libx264",
            "-c:a",
            "aac",
            "-shortest",
            str(video_b),
        ]
    )
    run_cmd(
        [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=220:duration=5",
            "-c:a",
            "libmp3lame",
            str(bgm),
        ]
    )
    run_cmd(
        [
            "ffmpeg",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "color=c=white@0.8:s=160x90",
            "-frames:v",
            "1",
            str(logo),
        ]
    )
    subtitle.write_text(
        "1\n00:00:00,000 --> 00:00:01,500\nVideoProcess subtitle test\n",
        encoding="utf-8",
    )

    return {
        "video_a": str(video_a),
        "video_b": str(video_b),
        "bgm": str(bgm),
        "logo": str(logo),
        "subtitle": str(subtitle),
    }


def probe_media(path: str) -> dict:
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
            path,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def has_audio(meta: dict) -> bool:
    return any(stream.get("codec_type") == "audio" for stream in meta.get("streams", []))


def duration(meta: dict) -> float:
    return float(meta.get("format", {}).get("duration", 0))


def expect(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


async def run_case(
    mode: str,
    name: str,
    handler,
    config: dict,
    inputs: dict[str, str],
    output_path: str,
    *,
    min_duration: float,
    max_duration: float,
    expect_audio: bool = True,
) -> dict:
    await handler.execute(config, inputs, output_path)
    meta = probe_media(output_path)
    out_duration = duration(meta)
    expect(Path(output_path).exists(), f"{mode}:{name} output missing")
    expect(min_duration <= out_duration <= max_duration, f"{mode}:{name} duration={out_duration}")
    expect(any(stream.get("codec_type") == "video" for stream in meta.get("streams", [])), f"{mode}:{name} missing video")
    if expect_audio:
        expect(has_audio(meta), f"{mode}:{name} missing audio")
    return {
        "mode": mode,
        "name": name,
        "duration": out_duration,
        "audio": has_audio(meta),
        "path": output_path,
    }


async def run_mode(mode: str, fixtures: dict[str, str], tmp_dir: Path) -> list[dict]:
    os.environ["VIDEO_USE_GPU"] = "true" if mode == "gpu" else "false"
    os.environ["VIDEO_GPU_FALLBACK_TO_CPU"] = "true"
    os.environ["VIDEO_GPU_BUSY_UTIL_THRESHOLD"] = "101"
    os.environ["VIDEO_GPU_BUSY_MEM_THRESHOLD"] = "101"

    cases: list[dict] = []
    cases.append(
        await run_case(
            mode,
            "trim",
            TrimHandler(),
            {"start_time": "00:00:00", "duration": "1"},
            {"input": fixtures["video_a"]},
            str(tmp_dir / f"{mode}-trim.mp4"),
            min_duration=0.8,
            max_duration=1.3,
        )
    )
    cases.append(
        await run_case(
            mode,
            "watermark",
            WatermarkHandler(),
            {"position": "bottom_right", "opacity": 0.7, "scale": 0.2, "margin": 12},
            {"video": fixtures["video_a"], "overlay": fixtures["logo"]},
            str(tmp_dir / f"{mode}-watermark.mp4"),
            min_duration=1.8,
            max_duration=2.3,
        )
    )
    cases.append(
        await run_case(
            mode,
            "subtitle",
            SubtitleHandler(),
            {"font_size": 24, "font_color": "white", "outline_color": "black", "position": "bottom"},
            {"video": fixtures["video_a"], "subtitle_file": fixtures["subtitle"]},
            str(tmp_dir / f"{mode}-subtitle.mp4"),
            min_duration=1.8,
            max_duration=2.3,
        )
    )
    cases.append(
        await run_case(
            mode,
            "bgm",
            BgmHandler(),
            {"volume": 0.3, "original_volume": 1.0, "loop": True, "fade_in": 0, "fade_out": 0},
            {"video": fixtures["video_a"], "audio": fixtures["bgm"]},
            str(tmp_dir / f"{mode}-bgm.mp4"),
            min_duration=1.8,
            max_duration=2.3,
        )
    )
    cases.append(
        await run_case(
            mode,
            "concat_horizontal",
            ConcatHorizontalHandler(),
            {"resize_mode": "match_height"},
            {"video_left": fixtures["video_a"], "video_right": fixtures["video_b"]},
            str(tmp_dir / f"{mode}-concat-horizontal.mp4"),
            min_duration=1.8,
            max_duration=2.3,
        )
    )
    cases.append(
        await run_case(
            mode,
            "concat_vertical",
            ConcatVerticalHandler(),
            {"resize_mode": "match_width"},
            {"video_top": fixtures["video_a"], "video_bottom": fixtures["video_b"]},
            str(tmp_dir / f"{mode}-concat-vertical.mp4"),
            min_duration=1.8,
            max_duration=2.3,
        )
    )
    cases.append(
        await run_case(
            mode,
            "concat_timeline",
            ConcatTimelineHandler(),
            {"transition": "fade", "transition_duration": 0.5},
            {"video_first": fixtures["video_a"], "video_second": fixtures["video_b"]},
            str(tmp_dir / f"{mode}-concat-timeline.mp4"),
            min_duration=3.2,
            max_duration=3.8,
        )
    )
    cases.append(
        await run_case(
            mode,
            "transcode",
            TranscodeHandler(),
            {
                "format": "mp4",
                "video_codec": "libx264",
                "audio_codec": "aac",
                "resolution": "640x360",
                "bitrate": "",
                "crf": 23,
                "preset": "medium",
            },
            {"input": fixtures["video_a"]},
            str(tmp_dir / f"{mode}-transcode.mp4"),
            min_duration=1.8,
            max_duration=2.3,
        )
    )
    return cases


async def main() -> int:
    if shutil.which("ffmpeg") is None or shutil.which("ffprobe") is None:
        print(json.dumps({"ok": False, "error": "ffmpeg/ffprobe not found"}))
        return 1

    TMP_ROOT.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=TMP_ROOT) as raw_tmp_dir:
        tmp_dir = Path(raw_tmp_dir)
        fixtures = create_fixture_files(tmp_dir)

        try:
            cpu_results = await run_mode("cpu", fixtures, tmp_dir)
            gpu_results = await run_mode("gpu", fixtures, tmp_dir)
        except Exception as exc:
            print(json.dumps({"ok": False, "error": str(exc)}, indent=2))
            return 1

    print(json.dumps({"ok": True, "results": cpu_results + gpu_results}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
