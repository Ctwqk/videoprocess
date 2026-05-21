from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
from typing import Any

import requests

from go_phase6_acceptance import (
    artifact_download_ok,
    create_job,
    create_pipeline,
    edge,
    graph,
    redis_pending,
    run_ffmpeg,
    wait_for_job,
)


EXPECTED_NODE_TYPES = {
    "trim",
    "vertical_crop",
    "title_overlay",
    "watermark",
    "transcode",
    "replace_audio",
    "bgm",
    "concat_horizontal",
    "concat_vertical",
    "concat_timeline",
    "concat_vertical_timeline",
    "concat_many",
    "montage_assembler",
    "export",
}


def upload_asset(api_url: str, path: Path, content_type: str) -> str:
    with path.open("rb") as fh:
        response = requests.post(
            f"{api_url}/api/v1/assets/upload",
            files={"file": (path.name, fh, content_type)},
            timeout=60,
        )
    response.raise_for_status()
    return response.json()["id"]


def generate_video(path: Path, size: str, frequency: int, duration: str) -> None:
    run_ffmpeg(
        [
            "-f",
            "lavfi",
            "-i",
            f"testsrc2=size={size}:rate=30",
            "-f",
            "lavfi",
            "-i",
            f"sine=frequency={frequency}:sample_rate=48000",
            "-t",
            duration,
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            path.as_posix(),
        ]
    )


def generate_audio(path: Path) -> None:
    run_ffmpeg(
        [
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=440:sample_rate=48000",
            "-t",
            "8",
            "-c:a",
            "pcm_s16le",
            path.as_posix(),
        ]
    )


def generate_image(path: Path) -> None:
    run_ffmpeg(
        [
            "-f",
            "lavfi",
            "-i",
            "color=c=0x2f80ed:s=640x360:d=1",
            "-frames:v",
            "1",
            path.as_posix(),
        ]
    )


def upload_synthetic_assets(api_url: str) -> dict[str, str]:
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp = Path(tmp_dir)
        video_a = tmp / "go-max-video-a.mp4"
        video_b = tmp / "go-max-video-b.mp4"
        video_c = tmp / "go-max-video-c.mp4"
        audio = tmp / "go-max-audio.wav"
        image = tmp / "go-max-watermark.png"

        generate_video(video_a, "640x360", 660, "6")
        generate_video(video_b, "360x640", 880, "5")
        generate_video(video_c, "854x480", 990, "5")
        generate_audio(audio)
        generate_image(image)

        return {
            "video_a": upload_asset(api_url, video_a, "video/mp4"),
            "video_b": upload_asset(api_url, video_b, "video/mp4"),
            "video_c": upload_asset(api_url, video_c, "video/mp4"),
            "audio": upload_asset(api_url, audio, "audio/wav"),
            "image": upload_asset(api_url, image, "image/png"),
        }


def source_node(node_id: str, asset_id: str, media_type: str, x: int, y: int) -> dict[str, Any]:
    return {
        "id": node_id,
        "type": "source",
        "position": {"x": x, "y": y},
        "data": {
            "label": node_id,
            "asset_id": asset_id,
            "config": {"asset_id": asset_id, "media_type": media_type},
        },
    }


def work_node(node_id: str, node_type: str, config: dict[str, Any], x: int, y: int) -> dict[str, Any]:
    return {
        "id": node_id,
        "type": node_type,
        "position": {"x": x, "y": y},
        "data": {"label": node_type, "config": config},
    }


def max_coverage_definition(asset_ids: dict[str, str]) -> dict[str, Any]:
    nodes = [
        source_node("source_video_a", asset_ids["video_a"], "video", 0, 0),
        source_node("source_video_b", asset_ids["video_b"], "video", 0, 180),
        source_node("source_video_c", asset_ids["video_c"], "video", 0, 360),
        source_node("source_audio", asset_ids["audio"], "audio", 0, 540),
        source_node("source_image", asset_ids["image"], "image", 0, 720),
        work_node("trim_a", "trim", {"start_time": "0", "duration": "4"}, 280, 0),
        work_node("trim_b", "trim", {"start_time": "0.25", "duration": "3.5"}, 280, 180),
        work_node("trim_c", "trim", {"start_time": "0.5", "duration": "3.5"}, 280, 360),
        work_node("vertical_crop_1", "vertical_crop", {"mode": "blur_bg", "width": 720, "height": 1280}, 560, 0),
        work_node(
            "title_overlay_1",
            "title_overlay",
            {"text": "Go max coverage", "position": "top", "start_time": 0, "duration": 3, "font_size": 48},
            840,
            0,
        ),
        work_node(
            "watermark_1",
            "watermark",
            {"position": "bottom_right", "opacity": 0.65, "scale": 0.18, "margin": 16},
            1120,
            0,
        ),
        work_node(
            "transcode_1",
            "transcode",
            {"format": "mp4", "video_codec": "libx264", "audio_codec": "aac", "resolution": "854x480", "crf": 20},
            1400,
            0,
        ),
        work_node(
            "replace_audio_1",
            "replace_audio",
            {"loop_if_shorter": True, "audio_volume": 0.9},
            1680,
            0,
        ),
        work_node(
            "bgm_1",
            "bgm",
            {"volume": 0.25, "original_volume": 0.85, "loop": True, "fade_in": 0.25, "fade_out": 0.25},
            1960,
            0,
        ),
        work_node("concat_horizontal_1", "concat_horizontal", {"resize_mode": "match_height"}, 560, 220),
        work_node("concat_vertical_1", "concat_vertical", {"resize_mode": "match_width"}, 560, 420),
        work_node(
            "concat_timeline_1",
            "concat_timeline",
            {"input_count": 2, "transition": "fade", "transition_duration": 0.25},
            840,
            320,
        ),
        work_node(
            "concat_vertical_timeline_1",
            "concat_vertical_timeline",
            {"pane_width": 360, "pane_height": 640, "background_color": "black"},
            840,
            560,
        ),
        work_node(
            "concat_many_1",
            "concat_many",
            {
                "input_count": 2,
                "target_duration": 7,
                "normalize_resolution": True,
                "aspect_ratio": "9:16",
                "width": 720,
                "height": 1280,
            },
            1120,
            440,
        ),
        work_node(
            "montage_assembler_1",
            "montage_assembler",
            {
                "style": "balanced",
                "target_duration": 6,
                "aspect_ratio": "9:16",
                "max_clip_duration": 3,
                "min_clip_duration": 1,
                "width": 720,
                "height": 1280,
            },
            1400,
            440,
        ),
        work_node(
            "export_1",
            "export",
            {"output_dir": "/tmp/vp_go_max_coverage", "filename": "go-max-coverage.mp4", "enable_quality_qa": True},
            1680,
            440,
        ),
    ]
    edges = [
        edge("e_source_a_trim", "source_video_a", "output", "trim_a", "input"),
        edge("e_source_b_trim", "source_video_b", "output", "trim_b", "input"),
        edge("e_source_c_trim", "source_video_c", "output", "trim_c", "input"),
        edge("e_trim_a_vertical_crop", "trim_a", "output", "vertical_crop_1", "input"),
        edge("e_vertical_crop_title", "vertical_crop_1", "output", "title_overlay_1", "input"),
        edge("e_title_watermark_video", "title_overlay_1", "output", "watermark_1", "video"),
        edge("e_image_watermark_overlay", "source_image", "output", "watermark_1", "overlay"),
        edge("e_watermark_transcode", "watermark_1", "output", "transcode_1", "input"),
        edge("e_transcode_replace_video", "transcode_1", "output", "replace_audio_1", "video"),
        edge("e_audio_replace", "source_audio", "output", "replace_audio_1", "audio"),
        edge("e_replace_bgm_video", "replace_audio_1", "output", "bgm_1", "video"),
        edge("e_audio_bgm", "source_audio", "output", "bgm_1", "audio"),
        edge("e_trim_b_horizontal_left", "trim_b", "output", "concat_horizontal_1", "video_left"),
        edge("e_trim_c_horizontal_right", "trim_c", "output", "concat_horizontal_1", "video_right"),
        edge("e_trim_a_vertical_top", "trim_a", "output", "concat_vertical_1", "video_top"),
        edge("e_trim_b_vertical_bottom", "trim_b", "output", "concat_vertical_1", "video_bottom"),
        edge("e_horizontal_timeline_1", "concat_horizontal_1", "output", "concat_timeline_1", "video_1"),
        edge("e_vertical_timeline_2", "concat_vertical_1", "output", "concat_timeline_1", "video_2"),
        edge("e_trim_b_vertical_timeline_first", "trim_b", "output", "concat_vertical_timeline_1", "video_first"),
        edge("e_trim_c_vertical_timeline_second", "trim_c", "output", "concat_vertical_timeline_1", "video_second"),
        edge("e_image_vertical_timeline_top", "source_image", "output", "concat_vertical_timeline_1", "image_top"),
        edge("e_image_vertical_timeline_bottom", "source_image", "output", "concat_vertical_timeline_1", "image_bottom"),
        edge("e_timeline_many_1", "concat_timeline_1", "output", "concat_many_1", "video_1"),
        edge("e_vertical_timeline_many_2", "concat_vertical_timeline_1", "output", "concat_many_1", "video_2"),
        edge("e_many_montage_1", "concat_many_1", "output", "montage_assembler_1", "video_1"),
        edge("e_bgm_montage_2", "bgm_1", "output", "montage_assembler_1", "video_2"),
        edge("e_montage_export", "montage_assembler_1", "output", "export_1", "input"),
    ]
    return graph(nodes, edges)


def collect_evidence(api_go_url: str, redis_url: str, job: dict[str, Any], terminal: dict[str, Any]) -> dict[str, Any]:
    nodes = terminal.get("node_executions", [])
    worker_nodes = [node for node in nodes if node.get("node_type") != "source"]
    covered_types = {node.get("node_type") for node in worker_nodes}
    final_nodes = [node for node in nodes if node.get("node_id") == "export_1"]
    final_artifact_id = final_nodes[0].get("output_artifact_id") if final_nodes else None
    wrong_workers = [
        {
            "node_id": node.get("node_id"),
            "node_type": node.get("node_type"),
            "worker_id": node.get("worker_id"),
        }
        for node in worker_nodes
        if "ffmpeg_go-worker@" not in (node.get("worker_id") or "")
    ]
    failed_nodes = [
        {
            "node_id": node.get("node_id"),
            "node_type": node.get("node_type"),
            "status": node.get("status"),
            "error": node.get("error_message"),
        }
        for node in worker_nodes
        if node.get("status") != "SUCCEEDED"
    ]
    return {
        "job_id": job["id"],
        "status": terminal.get("status"),
        "orchestrator_owner": job.get("orchestrator_owner"),
        "node_count": len(nodes),
        "worker_node_count": len(worker_nodes),
        "covered_types": sorted(covered_types),
        "missing_types": sorted(EXPECTED_NODE_TYPES - covered_types),
        "failed_nodes": failed_nodes,
        "wrong_workers": wrong_workers,
        "final_artifact_id": final_artifact_id,
        "final_artifact_ok": bool(final_artifact_id and artifact_download_ok(api_go_url, final_artifact_id)),
        "go_task_pending": redis_pending(redis_url, "vp:tasks:ffmpeg_go", "ffmpeg_go-workers"),
        "go_event_pending": redis_pending(redis_url, "vp:events:go", "orchestrator-go"),
    }


def run_acceptance(api_go_url: str, python_api_url: str, redis_url: str, timeout_seconds: int) -> dict[str, Any]:
    asset_ids = upload_synthetic_assets(python_api_url)
    print("uploaded_assets", json.dumps(asset_ids, indent=2, sort_keys=True))

    pipeline_id = create_pipeline(api_go_url, "go-max-coverage-acceptance", max_coverage_definition(asset_ids))
    created = create_job(api_go_url, pipeline_id)
    print(
        "created_job",
        json.dumps(
            {
                "id": created["id"],
                "status": created.get("status"),
                "orchestrator_owner": created.get("orchestrator_owner"),
            },
            indent=2,
            sort_keys=True,
        ),
    )

    terminal = wait_for_job(api_go_url, created["id"], timeout_seconds)
    evidence = collect_evidence(api_go_url, redis_url, created, terminal)
    evidence["pipeline_id"] = pipeline_id
    return evidence


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-go-url", default="http://127.0.0.1:18081")
    parser.add_argument("--python-api-url", default="http://127.0.0.1:18080")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6380/0")
    parser.add_argument("--timeout-seconds", type=int, default=600)
    args = parser.parse_args()

    evidence = run_acceptance(args.api_go_url, args.python_api_url, args.redis_url, args.timeout_seconds)
    print("FINAL_EVIDENCE")
    print(json.dumps(evidence, indent=2, sort_keys=True))
    if (
        evidence["status"] != "SUCCEEDED"
        or evidence["orchestrator_owner"] != "go"
        or evidence["missing_types"]
        or evidence["failed_nodes"]
        or evidence["wrong_workers"]
        or not evidence["final_artifact_ok"]
        or evidence["go_task_pending"] != 0
        or evidence["go_event_pending"] != 0
    ):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
