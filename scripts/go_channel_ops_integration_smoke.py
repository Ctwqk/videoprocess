#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import requests


TMP_DIR = Path(os.environ.get("VP_GO_CHANNEL_OPS_SMOKE_DIR", "/tmp/vp_go_channel_ops_smoke"))
TERMINAL_JOB_STATES = {"SUCCEEDED", "FAILED", "CANCELLED", "PARTIALLY_FAILED"}
TERMINAL_YOUTUBE_TASK_STATES = {"completed", "failed"}


class SmokeFailure(RuntimeError):
    pass


def print_step(message: str) -> None:
    print(f"[go-channel-ops-smoke] {message}", file=sys.stderr, flush=True)


def run_command(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def create_fixture_media(tmp_dir: Path = TMP_DIR) -> dict[str, Path]:
    tmp_dir.mkdir(parents=True, exist_ok=True)
    video = tmp_dir / "go-channel-ops-source.mp4"
    audio = tmp_dir / "go-channel-ops-bgm.wav"
    image = tmp_dir / "go-channel-ops-watermark.png"
    subtitle = tmp_dir / "go-channel-ops-subtitle.srt"

    run_command(
        [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=640x360:rate=30",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=660:sample_rate=48000",
            "-t",
            "6",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            str(video),
        ]
    )
    run_command(
        [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=48000",
            "-t",
            "8",
            "-c:a",
            "pcm_s16le",
            str(audio),
        ]
    )
    run_command(
        [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "color=c=red:s=240x120",
            "-frames:v",
            "1",
            str(image),
        ]
    )
    subtitle.write_text(
        "1\n00:00:00,000 --> 00:00:02,500\nGo migration smoke\n\n"
        "2\n00:00:02,500 --> 00:00:05,000\nChannelOps integration private upload\n",
        encoding="utf-8",
    )
    return {"video": video, "audio": audio, "image": image, "subtitle": subtitle}


def source_node(node_id: str, label: str, asset_id: str, media_type: str, x: int, y: int) -> dict[str, Any]:
    return {
        "id": node_id,
        "type": "source",
        "position": {"x": x, "y": y},
        "data": {
            "label": label,
            "config": {"asset_id": asset_id, "media_type": media_type},
            "asset_id": asset_id,
        },
    }


def worker_node(node_id: str, node_type: str, label: str, config: dict[str, Any], x: int, y: int) -> dict[str, Any]:
    return {
        "id": node_id,
        "type": node_type,
        "position": {"x": x, "y": y},
        "data": {"label": label, "config": config, "asset_id": None},
    }


def edge(edge_id: str, source: str, target: str, source_handle: str, target_handle: str) -> dict[str, str]:
    return {
        "id": edge_id,
        "source": source,
        "target": target,
        "sourceHandle": source_handle,
        "targetHandle": target_handle,
    }


def build_pipeline_definition(asset_ids: dict[str, str]) -> dict[str, Any]:
    required = {"video", "audio", "image", "subtitle"}
    missing = sorted(required - set(asset_ids))
    if missing:
        raise ValueError(f"missing asset ids: {', '.join(missing)}")

    nodes = [
        source_node("source_video", "Source Video", asset_ids["video"], "video", 0, 0),
        source_node("source_audio", "BGM Audio", asset_ids["audio"], "audio", 0, 240),
        source_node("source_image", "Watermark Image", asset_ids["image"], "image", 0, 480),
        source_node("source_subtitle", "Subtitle File", asset_ids["subtitle"], "subtitle", 0, 720),
        worker_node("trim_1", "trim", "Go Trim A", {"start_time": "0", "duration": "3"}, 260, -80),
        worker_node("trim_2", "trim", "Go Trim B", {"start_time": "2", "duration": "3"}, 260, 80),
        worker_node(
            "concat_1",
            "concat_many",
            "Concat Go Trims",
            {
                "input_count": 2,
                "output_format": "mp4",
                "transition": "none",
                "target_duration": 6,
                "normalize_resolution": True,
                "aspect_ratio": "16:9",
                "width": 640,
                "height": 360,
            },
            520,
            0,
        ),
        worker_node(
            "title_1",
            "title_overlay",
            "Title Overlay",
            {
                "text": "Go + ChannelOps Integration",
                "position": "top",
                "start_time": 0,
                "duration": 5,
                "font_size": 32,
                "safe_area": True,
            },
            780,
            0,
        ),
        worker_node(
            "subtitle_1",
            "subtitle",
            "Burn Subtitles",
            {"font_size": 26, "font_color": "white", "outline_color": "black", "position": "bottom"},
            1040,
            0,
        ),
        worker_node(
            "watermark_1",
            "watermark",
            "Watermark",
            {"position": "top_right", "opacity": 0.75, "scale": 0.18, "margin": 12},
            1300,
            0,
        ),
        worker_node(
            "vertical_1",
            "vertical_crop",
            "Vertical Crop",
            {"mode": "blur_bg", "width": 360, "height": 640, "background": "blur"},
            1560,
            0,
        ),
        worker_node(
            "replace_audio_1",
            "replace_audio",
            "Replace Audio",
            {"loop_if_shorter": True, "audio_volume": 0.9},
            1820,
            0,
        ),
        worker_node(
            "bgm_1",
            "bgm",
            "Background Music",
            {"volume": 0.25, "original_volume": 0.85, "loop": True, "fade_in": 0.2, "fade_out": 0.2},
            2080,
            0,
        ),
        worker_node(
            "transcode_1",
            "transcode",
            "Transcode",
            {
                "format": "mp4",
                "video_codec": "libx264",
                "audio_codec": "aac",
                "resolution": "original",
                "bitrate": "",
                "crf": 24,
                "preset": "veryfast",
            },
            2340,
            0,
        ),
        worker_node(
            "export_1",
            "export",
            "Export",
            {
                "output_dir": "/tmp/vp_go_channel_ops_exports",
                "filename": "go-channel-ops-integration.mp4",
                "enable_quality_qa": False,
            },
            2600,
            0,
        ),
    ]
    edges = [
        edge("e_video_trim_1", "source_video", "trim_1", "output", "input"),
        edge("e_video_trim_2", "source_video", "trim_2", "output", "input"),
        edge("e_trim_1_concat", "trim_1", "concat_1", "output", "video_1"),
        edge("e_trim_2_concat", "trim_2", "concat_1", "output", "video_2"),
        edge("e_concat_title", "concat_1", "title_1", "output", "input"),
        edge("e_title_subtitle", "title_1", "subtitle_1", "output", "video"),
        edge("e_source_subtitle", "source_subtitle", "subtitle_1", "output", "subtitle_file"),
        edge("e_subtitle_watermark", "subtitle_1", "watermark_1", "output", "video"),
        edge("e_source_image", "source_image", "watermark_1", "output", "overlay"),
        edge("e_watermark_vertical", "watermark_1", "vertical_1", "output", "input"),
        edge("e_vertical_replace_audio", "vertical_1", "replace_audio_1", "output", "video"),
        edge("e_source_audio_replace", "source_audio", "replace_audio_1", "output", "audio"),
        edge("e_replace_bgm", "replace_audio_1", "bgm_1", "output", "video"),
        edge("e_source_audio_bgm", "source_audio", "bgm_1", "output", "audio"),
        edge("e_bgm_transcode", "bgm_1", "transcode_1", "output", "input"),
        edge("e_transcode_export", "transcode_1", "export_1", "output", "input"),
    ]
    return {"nodes": nodes, "edges": edges, "viewport": {"x": 0, "y": 0, "zoom": 0.75}}


def final_artifact_node_ids() -> Sequence[str]:
    return ("export_1", "transcode_1", "bgm_1")


def normalize_api_base(value: str) -> str:
    base = value.rstrip("/")
    return base if base.endswith("/api/v1") else f"{base}/api/v1"


def normalize_youtube_base(value: str) -> str:
    base = value.rstrip("/")
    return base if base.endswith("/api") else f"{base}/api"


def request_raw(method: str, url: str, *, expected: int | None = None, timeout: int = 120, **kwargs: Any):
    response = requests.request(method, url, timeout=timeout, **kwargs)
    if expected is not None and response.status_code != expected:
        raise SmokeFailure(
            f"{method} {url} returned {response.status_code}, expected {expected}: {response.text[:500]}"
        )
    return response


def request_json(method: str, url: str, *, expected: int | None = None, timeout: int = 120, **kwargs: Any) -> dict:
    response = request_raw(method, url, expected=expected, timeout=timeout, **kwargs)
    try:
        return response.json()
    except ValueError as exc:
        raise SmokeFailure(f"{method} {url} did not return JSON: {response.text[:500]}") from exc


def upload_asset(api_base: str, path: Path, content_type: str) -> str:
    with path.open("rb") as handle:
        payload = request_json(
            "POST",
            f"{api_base}/assets/upload",
            expected=200,
            files={"file": (path.name, handle, content_type)},
            timeout=180,
        )
    return str(payload["id"])


def wait_for_job(api_base: str, job_id: str, *, timeout_seconds: int) -> dict:
    deadline = time.time() + timeout_seconds
    last: dict[str, Any] = {}
    while time.time() < deadline:
        last = request_json("GET", f"{api_base}/jobs/{job_id}", expected=200, timeout=30)
        if last.get("status") in TERMINAL_JOB_STATES:
            return last
        time.sleep(2)
    raise SmokeFailure(f"job {job_id} did not finish before timeout; last={json.dumps(last, default=str)[:1000]}")


def wait_for_youtube_task(yt_base: str, task_id: str, *, timeout_seconds: int) -> dict:
    deadline = time.time() + timeout_seconds
    last: dict[str, Any] = {}
    while time.time() < deadline:
        last = request_json("GET", f"{yt_base}/status/{task_id}", expected=200, timeout=30)
        if last.get("status") in TERMINAL_YOUTUBE_TASK_STATES:
            return last
        time.sleep(3)
    raise SmokeFailure(f"YouTube task {task_id} did not finish before timeout; last={json.dumps(last)[:1000]}")


def probe_duration(path: Path) -> float:
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "quiet",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return float(result.stdout.strip())


def download_artifact(api_base: str, artifact_id: str, target: Path) -> Path:
    response = request_raw("GET", f"{api_base}/artifacts/{artifact_id}/download", expected=200, timeout=180)
    target.write_bytes(response.content)
    return target


def select_final_artifact(job_detail: dict) -> tuple[str, str]:
    by_node = {node["node_id"]: node for node in job_detail.get("node_executions", [])}
    for node_id in final_artifact_node_ids():
        node = by_node.get(node_id)
        artifact_id = node.get("output_artifact_id") if node else None
        if artifact_id:
            return node_id, str(artifact_id)
    raise SmokeFailure("no final artifact found on preferred nodes")


def assert_go_trim_nodes(job_detail: dict) -> list[str]:
    trim_nodes = [
        node for node in job_detail.get("node_executions", [])
        if str(node.get("node_id", "")).startswith("trim_")
    ]
    if len(trim_nodes) != 2:
        raise SmokeFailure(f"expected two trim nodes, found {len(trim_nodes)}")
    worker_ids: list[str] = []
    for node in trim_nodes:
        if node.get("status") != "SUCCEEDED":
            raise SmokeFailure(f"trim node did not succeed: {node}")
        if not node.get("output_artifact_id"):
            raise SmokeFailure(f"trim node has no output artifact: {node}")
        worker_id = str(node.get("worker_id") or "")
        if not worker_id.startswith("ffmpeg_go-worker@"):
            raise SmokeFailure(f"trim node did not run on Go worker: {node}")
        worker_ids.append(worker_id)
    return worker_ids


def upload_to_youtube(yt_base: str, artifact_path: Path, *, privacy: str, title: str) -> dict:
    auth_status = request_json("GET", f"{yt_base}/auth/status", expected=200, timeout=30)
    if auth_status.get("authenticated") is not True:
        raise SmokeFailure(f"YouTubeManager is reachable but not authenticated: {auth_status}")
    with artifact_path.open("rb") as handle:
        upload = request_json(
            "POST",
            f"{yt_base}/upload",
            expected=200,
            files={"file": (artifact_path.name, handle, "video/mp4")},
            data={
                "title": title,
                "description": "VideoProcess Go migration + ChannelOps integration smoke",
                "tags": "videoprocess,go-migration,channelops,smoke",
                "privacy_status": privacy,
            },
            timeout=180,
        )
    status = wait_for_youtube_task(yt_base, str(upload["task_id"]), timeout_seconds=900)
    if status.get("status") != "completed":
        raise SmokeFailure(f"YouTube upload did not complete: {status}")
    return status


def run_smoke(args: argparse.Namespace) -> dict:
    api_base = normalize_api_base(args.api_base)
    yt_base = normalize_youtube_base(args.yt_base)
    api_root = api_base.rsplit("/api/v1", 1)[0]
    title_slug = str(int(time.time()))

    print_step("checking API health")
    request_json("GET", f"{api_root}/health", expected=200, timeout=30)

    print_step("creating fixture media")
    fixtures = create_fixture_media(TMP_DIR)
    content_types = {
        "video": "video/mp4",
        "audio": "audio/wav",
        "image": "image/png",
        "subtitle": "application/x-subrip",
    }

    print_step("uploading fixture assets")
    asset_ids = {
        name: upload_asset(api_base, path, content_types[name])
        for name, path in fixtures.items()
    }

    print_step("validating pipeline definition")
    definition = build_pipeline_definition(asset_ids)
    validation = request_json("POST", f"{api_base}/pipelines/validate", expected=200, json=definition)
    if validation.get("valid") is not True:
        raise SmokeFailure(f"pipeline validation failed: {validation}")

    print_step("creating pipeline and submitting job")
    pipeline = request_json(
        "POST",
        f"{api_base}/pipelines",
        expected=201,
        json={
            "name": f"go-channel-ops-integration-{title_slug}",
            "description": "Docker integration smoke for Go trim worker and ChannelOps merge",
            "definition": definition,
            "is_template": False,
            "template_tags": [],
        },
    )
    job = request_json("POST", f"{api_base}/jobs", expected=201, json={"pipeline_id": pipeline["id"], "inputs": {}})

    print_step(f"waiting for job {job['id']}")
    detail = wait_for_job(api_base, str(job["id"]), timeout_seconds=args.job_timeout_seconds)
    if detail.get("status") != "SUCCEEDED":
        raise SmokeFailure(f"job failed: {json.dumps(detail, default=str)[:2000]}")
    trim_worker_ids = assert_go_trim_nodes(detail)

    final_node_id, artifact_id = select_final_artifact(detail)
    artifact_path = TMP_DIR / f"go-channel-ops-final-{job['id']}.mp4"
    print_step(f"downloading final artifact {artifact_id}")
    download_artifact(api_base, artifact_id, artifact_path)
    duration = probe_duration(artifact_path)
    if duration <= 1.0:
        raise SmokeFailure(f"downloaded artifact duration is too short: {duration}")

    result: dict[str, Any] = {
        "ok": True,
        "api_base": api_base,
        "pipeline_id": pipeline["id"],
        "job_id": job["id"],
        "final_node_id": final_node_id,
        "artifact_id": artifact_id,
        "artifact_path": str(artifact_path),
        "artifact_duration_seconds": duration,
        "trim_worker_ids": trim_worker_ids,
        "youtube_privacy": args.privacy,
    }

    if args.upload_youtube:
        print_step("uploading final artifact to YouTubeManager as private/unlisted per args")
        upload_status = upload_to_youtube(
            yt_base,
            artifact_path,
            privacy=args.privacy,
            title=f"VideoProcess Go ChannelOps Smoke {title_slug}",
        )
        youtube_result = upload_status.get("result") or {}
        result.update(
            {
                "youtube_task_status": upload_status.get("status"),
                "youtube_video_id": youtube_result.get("video_id"),
                "youtube_url": youtube_result.get("url")
                or (
                    f"https://www.youtube.com/watch?v={youtube_result.get('video_id')}"
                    if youtube_result.get("video_id")
                    else None
                ),
            }
        )
    return result


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Go ChannelOps Docker integration video smoke.")
    parser.add_argument(
        "--api-base",
        default=os.environ.get("VP_API_BASE", "http://127.0.0.1:18080/api/v1"),
        help="Python API base URL, with or without /api/v1.",
    )
    parser.add_argument(
        "--yt-base",
        default=os.environ.get("VP_YT_BASE", "http://127.0.0.1:3001/youtube/api"),
        help="YouTubeManager API base URL, with or without /api.",
    )
    parser.add_argument("--upload-youtube", action="store_true", help="Upload the generated artifact to YouTube.")
    parser.add_argument("--privacy", default="private", choices=["private", "unlisted"])
    parser.add_argument("--job-timeout-seconds", type=int, default=600)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        result = run_smoke(parse_args(argv or sys.argv[1:]))
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2), flush=True)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
