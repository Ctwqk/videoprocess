from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest
import requests


STRICT = os.getenv("VP_GO_WORKER_NODE_STRICT", "").lower() in {"1", "true", "yes", "on"}
PY_API = os.getenv("VP_PY_API_URL") or os.getenv("VP_PYTHON_API", "http://127.0.0.1:18080")
REDIS_URL = os.getenv("VP_REDIS_URL", "redis://127.0.0.1:6380/0")


NODE_CASES = [
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


@pytest.mark.skipif(not STRICT, reason="set VP_GO_WORKER_NODE_STRICT=1 for live mixed-mode node tests")
@pytest.mark.parametrize("node_type", NODE_CASES)
def test_node_runs_through_go_worker(node_type: str, tmp_path: Path) -> None:
    payload = build_pipeline_payload(node_type, tmp_path)
    created = requests.post(f"{PY_API}/api/v1/pipelines", json=payload["pipeline"], timeout=20)
    assert created.status_code in {200, 201}, created.text
    pipeline_id = created.json()["id"]

    job = requests.post(f"{PY_API}/api/v1/jobs", json={"pipeline_id": pipeline_id, "inputs": {}}, timeout=20)
    assert job.status_code in {200, 201}, job.text
    job_id = job.json()["id"]

    body: dict[str, Any] = {}
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        response = requests.get(f"{PY_API}/api/v1/jobs/{job_id}", timeout=10)
        assert response.status_code == 200
        body = response.json()
        if body["status"] in {"SUCCEEDED", "FAILED", "CANCELLED", "PARTIALLY_FAILED"}:
            break
        time.sleep(2)

    assert body["status"] == "SUCCEEDED", body
    target_nodes = [node for node in body["node_executions"] if node["node_id"] == "node_1"]
    assert len(target_nodes) == 1, body["node_executions"]
    assert target_nodes[0]["status"] == "SUCCEEDED", target_nodes[0]
    assert "ffmpeg_go-worker@" in (target_nodes[0]["worker_id"] or ""), target_nodes[0]
    assert pending_summary_count() == 0


def pending_summary_count() -> int:
    pending = subprocess.check_output(
        ["redis-cli", "-u", REDIS_URL, "XPENDING", "vp:tasks:ffmpeg_go", "ffmpeg_go-workers"],
        text=True,
    )
    first_line = pending.splitlines()[0].strip()
    return int(first_line)


def build_pipeline_payload(node_type: str, tmp_path: Path) -> dict[str, Any]:
    assets = {
        "video": ensure_uploaded_asset("video", tmp_path),
        "audio": ensure_uploaded_asset("audio", tmp_path),
        "image": ensure_uploaded_asset("image", tmp_path),
    }
    return {
        "pipeline": {
            "name": f"go-node-{node_type}",
            "description": f"Mixed-mode Go worker smoke for {node_type}",
            "definition": pipeline_definition_for(node_type, assets),
            "is_template": False,
            "template_tags": [],
        }
    }


def ensure_uploaded_asset(kind: str, tmp_path: Path) -> str:
    env_key = f"VP_GO_SMOKE_{kind.upper()}_ASSET_ID"
    if asset_id := os.getenv(env_key):
        return asset_id

    source = tmp_path / f"go-{kind}-fixture"
    if kind == "video":
        source = source.with_suffix(".mp4")
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
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
                str(source),
            ],
            check=True,
        )
        content_type = "video/mp4"
    elif kind == "audio":
        source = source.with_suffix(".wav")
        subprocess.run(
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
                "3",
                str(source),
            ],
            check=True,
        )
        content_type = "audio/wav"
    elif kind == "image":
        source = source.with_suffix(".png")
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-f",
                "lavfi",
                "-i",
                "color=c=red:s=96x96",
                "-frames:v",
                "1",
                str(source),
            ],
            check=True,
        )
        content_type = "image/png"
    else:
        raise AssertionError(kind)

    with source.open("rb") as fh:
        response = requests.post(
            f"{PY_API}/api/v1/assets/upload",
            files={"file": (source.name, fh, content_type)},
            timeout=60,
        )
    assert response.status_code in {200, 201}, response.text
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


def pipeline_definition_for(node_type: str, assets: dict[str, str]) -> dict[str, Any]:
    export = work_node(
        "export_1",
        "export",
        {"output_dir": "/tmp/vp_go_node_exports", "filename": f"{node_type}.mp4"},
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
        node = work_node("node_1", "export", {"output_dir": "/tmp/vp_go_node_exports", "filename": "export-direct.mp4"}, 450)
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
        node = work_node(
            "node_1",
            node_type,
            {"normalize_resolution": True, "aspect_ratio": "9:16", "transition": "none"},
            450,
        )
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


def graph(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> dict[str, Any]:
    return {"nodes": nodes, "edges": edges, "viewport": {"x": 0, "y": 0, "zoom": 1}}
