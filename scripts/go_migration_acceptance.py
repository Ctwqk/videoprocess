from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import requests


NODES = [
    "trim",
    "transcode",
    "export",
    "vertical_crop",
    "watermark",
    "title_overlay",
    "bgm",
    "replace_audio",
    "concat_horizontal",
    "concat_vertical",
    "concat_many",
    "concat_timeline",
    "concat_vertical_timeline",
    "montage_assembler",
]

TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED", "PARTIALLY_FAILED"}


@dataclass
class NodeEvidence:
    node_type: str
    completed: int
    p95_seconds: float
    redis_pending: int
    missing_output_artifact_id: int
    missing_storage_path: int
    wrong_worker: int


def redis_pending(redis_url: str) -> int:
    output = subprocess.check_output(
        ["redis-cli", "-u", redis_url, "XPENDING", "vp:tasks:ffmpeg_go", "ffmpeg_go-workers"],
        text=True,
    )
    return int(output.splitlines()[0].strip())


def percentile95(values: list[float]) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    return statistics.quantiles(values, n=100)[94]


def run_ffmpeg(args: list[str]) -> None:
    subprocess.run(["ffmpeg", "-y", "-hide_banner", "-loglevel", "error", *args], check=True)


def create_fixture_assets(api_url: str, tmp_path: Path) -> dict[str, str]:
    video = tmp_path / "go-acceptance-video.mp4"
    audio = tmp_path / "go-acceptance-audio.wav"
    image = tmp_path / "go-acceptance-image.png"
    run_ffmpeg(
        [
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=320x180:rate=30",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=1000:sample_rate=48000",
            "-t",
            "3",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            str(video),
        ]
    )
    run_ffmpeg(["-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-t", "3", str(audio)])
    run_ffmpeg(["-f", "lavfi", "-i", "color=c=red:s=96x96", "-frames:v", "1", str(image)])
    return {
        "video": upload_asset(api_url, video, "video/mp4"),
        "audio": upload_asset(api_url, audio, "audio/wav"),
        "image": upload_asset(api_url, image, "image/png"),
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


def source_node(node_id: str, asset_id: str, media_type: str, x: int) -> dict[str, Any]:
    return {
        "id": node_id,
        "type": "source",
        "position": {"x": x, "y": 0},
        "data": {
            "label": node_id,
            "asset_id": asset_id,
            "config": {"asset_id": asset_id, "media_type": media_type},
        },
    }


def work_node(node_id: str, node_type: str, config: dict[str, Any], x: int) -> dict[str, Any]:
    return {
        "id": node_id,
        "type": node_type,
        "position": {"x": x, "y": 0},
        "data": {"label": node_type, "config": config},
    }


def edge(edge_id: str, source: str, source_handle: str, target: str, target_handle: str) -> dict[str, Any]:
    return {
        "id": edge_id,
        "source": source,
        "sourceHandle": source_handle,
        "target": target,
        "targetHandle": target_handle,
    }


def graph(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> dict[str, Any]:
    return {"nodes": nodes, "edges": edges, "viewport": {"x": 0, "y": 0, "zoom": 1}}


def pipeline_definition_for(node_type: str, assets: dict[str, str]) -> dict[str, Any]:
    export = work_node(
        "export_1",
        "export",
        {"output_dir": "/tmp/vp_go_acceptance_exports", "filename": f"{node_type}.mp4"},
        900,
    )
    source_video = source_node("video_1", assets["video"], "video", 0)
    source_audio = source_node("audio_1", assets["audio"], "audio", 0)
    source_image = source_node("image_1", assets["image"], "image", 0)

    if node_type in {"trim", "transcode", "vertical_crop", "title_overlay"}:
        config = {
            "trim": {"start_time": "0", "duration": "1", "output_format": "mp4"},
            "transcode": {"video_codec": "libx264", "audio_codec": "aac", "crf": 20, "preset": "medium"},
            "vertical_crop": {"width": 320, "height": 568, "mode": "center_crop"},
            "title_overlay": {"text": "Go", "position": "top", "duration": 1, "font_size": 32},
        }[node_type]
        node = work_node("node_1", node_type, config, 450)
        return graph(
            [source_video, node, export],
            [
                edge("e1", "video_1", "output", "node_1", "input"),
                edge("e2", "node_1", "output", "export_1", "input"),
            ],
        )

    if node_type == "export":
        node = work_node(
            "node_1",
            "export",
            {"output_dir": "/tmp/vp_go_acceptance_exports", "filename": "export-direct.mp4"},
            450,
        )
        return graph([source_video, node], [edge("e1", "video_1", "output", "node_1", "input")])

    if node_type == "watermark":
        node = work_node("node_1", "watermark", {"position": "bottom_right", "opacity": 0.8, "scale": 0.15, "margin": 10}, 450)
        return graph(
            [source_video, source_image, node, export],
            [
                edge("e1", "video_1", "output", "node_1", "video"),
                edge("e2", "image_1", "output", "node_1", "overlay"),
                edge("e3", "node_1", "output", "export_1", "input"),
            ],
        )

    if node_type in {"bgm", "replace_audio"}:
        node = work_node("node_1", node_type, {}, 450)
        return graph(
            [source_video, source_audio, node, export],
            [
                edge("e1", "video_1", "output", "node_1", "video"),
                edge("e2", "audio_1", "output", "node_1", "audio"),
                edge("e3", "node_1", "output", "export_1", "input"),
            ],
        )

    if node_type == "concat_horizontal":
        node = work_node("node_1", node_type, {"resize_mode": "match_height"}, 450)
        return two_video_graph(node, export, "video_left", "video_right", assets)

    if node_type == "concat_vertical":
        node = work_node("node_1", node_type, {"resize_mode": "match_width"}, 450)
        return two_video_graph(node, export, "video_top", "video_bottom", assets)

    if node_type in {"concat_many", "montage_assembler", "concat_timeline"}:
        node = work_node("node_1", node_type, {"normalize_resolution": True, "aspect_ratio": "9:16", "transition": "none"}, 450)
        return two_video_graph(node, export, "video_1", "video_2", assets)

    if node_type == "concat_vertical_timeline":
        node = work_node("node_1", node_type, {"pane_width": 320, "pane_height": 180, "background_color": "black"}, 450)
        return two_video_graph(node, export, "video_first", "video_second", assets)

    raise AssertionError(node_type)


def two_video_graph(node: dict[str, Any], export: dict[str, Any], first_handle: str, second_handle: str, assets: dict[str, str]) -> dict[str, Any]:
    video_one = source_node("video_1", assets["video"], "video", 0)
    video_two = source_node("video_2", assets["video"], "video", 0)
    return graph(
        [video_one, video_two, node, export],
        [
            edge("e1", "video_1", "output", "node_1", first_handle),
            edge("e2", "video_2", "output", "node_1", second_handle),
            edge("e3", "node_1", "output", "export_1", "input"),
        ],
    )


def create_pipeline(api_url: str, node_type: str, assets: dict[str, str]) -> str:
    payload = {
        "name": f"go-acceptance-{node_type}",
        "description": f"Go migration acceptance for {node_type}",
        "definition": pipeline_definition_for(node_type, assets),
        "is_template": False,
        "template_tags": [],
    }
    response = requests.post(f"{api_url}/api/v1/pipelines", json=payload, timeout=20)
    response.raise_for_status()
    return response.json()["id"]


def start_job(api_url: str, pipeline_id: str) -> str:
    response = requests.post(f"{api_url}/api/v1/jobs", json={"pipeline_id": pipeline_id, "inputs": {}}, timeout=20)
    if response.status_code == 501:
        raise RuntimeError("job start is Python-owned; run this script against the Python API with Go worker cutover")
    response.raise_for_status()
    return response.json()["id"]


def wait_for_job(api_url: str, job_id: str, timeout_seconds: int) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        response = requests.get(f"{api_url}/api/v1/jobs/{job_id}", timeout=10)
        response.raise_for_status()
        last = response.json()
        if last["status"] in TERMINAL:
            return last
        time.sleep(2)
    raise RuntimeError(f"job {job_id} did not finish before timeout; last={last}")


def target_node(job: dict[str, Any]) -> dict[str, Any]:
    matches = [node for node in job.get("node_executions", []) if node.get("node_id") == "node_1"]
    if len(matches) != 1:
        raise RuntimeError(f"expected one node_1 execution, got {job.get('node_executions')}")
    return matches[0]


def artifact_download_ok(api_url: str, artifact_id: str) -> bool:
    response = requests.get(f"{api_url}/api/v1/artifacts/{artifact_id}/download", timeout=30)
    return response.status_code == 200 and bool(response.content)


def run_node_batch(api_url: str, redis_url: str, node_type: str, count: int, assets: dict[str, str], timeout_seconds: int) -> NodeEvidence:
    pipeline_id = create_pipeline(api_url, node_type, assets)
    durations: list[float] = []
    completed = 0
    missing_output_artifact_id = 0
    missing_storage_path = 0
    wrong_worker = 0

    for _ in range(count):
        started = time.monotonic()
        job_id = start_job(api_url, pipeline_id)
        body = wait_for_job(api_url, job_id, timeout_seconds)
        if body.get("status") != "SUCCEEDED":
            raise RuntimeError(f"{node_type} job {job_id} finished with {body}")
        durations.append(time.monotonic() - started)
        completed += 1
        node = target_node(body)
        if "ffmpeg_go-worker@" not in (node.get("worker_id") or ""):
            wrong_worker += 1
        artifact_id = node.get("output_artifact_id")
        if not artifact_id:
            missing_output_artifact_id += 1
            continue
        if not artifact_download_ok(api_url, artifact_id):
            missing_storage_path += 1

    return NodeEvidence(
        node_type=node_type,
        completed=completed,
        p95_seconds=percentile95(durations),
        redis_pending=redis_pending(redis_url),
        missing_output_artifact_id=missing_output_artifact_id,
        missing_storage_path=missing_storage_path,
        wrong_worker=wrong_worker,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default="http://127.0.0.1:18080")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6380/0")
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument("--timeout-seconds", type=int, default=240)
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        assets = create_fixture_assets(args.api_url, Path(tmp))
        evidence = [
            asdict(run_node_batch(args.api_url, args.redis_url, node, args.count, assets, args.timeout_seconds))
            for node in NODES
        ]

    print(json.dumps({"nodes": evidence}, indent=2, sort_keys=True))
    failures = [
        item
        for item in evidence
        if item["completed"] != args.count
        or item["redis_pending"] != 0
        or item["missing_output_artifact_id"] != 0
        or item["missing_storage_path"] != 0
        or item["wrong_worker"] != 0
    ]
    if failures:
        raise SystemExit(json.dumps({"failed_acceptance": failures}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
