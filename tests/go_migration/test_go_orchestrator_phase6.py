from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest
import requests


STRICT = os.getenv("VP_GO_PHASE6_STRICT", "").lower() in {"1", "true", "yes", "on"}
GO_API = os.getenv("VP_GO_API_URL", "http://127.0.0.1:18081")
PY_API = os.getenv("VP_PYTHON_API", "http://127.0.0.1:18080")
REDIS_URL = os.getenv("VP_REDIS_URL", "redis://127.0.0.1:6380/0")


@pytest.mark.skipif(not STRICT, reason="set VP_GO_PHASE6_STRICT=1 for live Go orchestrator tests")
def test_go_api_creates_and_completes_go_owned_job(tmp_path: Path) -> None:
    asset_id = upload_video_asset(tmp_path)
    pipeline_id = create_go_pipeline(asset_id)
    created = requests.post(f"{GO_API}/api/v1/jobs", json={"pipeline_id": pipeline_id, "inputs": {}}, timeout=20)
    assert created.status_code == 201, created.text
    body = created.json()
    assert body["orchestrator_owner"] == "go"
    job_id = body["id"]

    terminal = wait_for_terminal_job(GO_API, job_id)
    assert terminal["status"] == "SUCCEEDED", terminal
    worker_nodes = [node for node in terminal["node_executions"] if node["node_type"] != "source"]
    assert worker_nodes
    assert all("ffmpeg_go-worker@" in (node["worker_id"] or "") for node in worker_nodes)
    assert any(node["output_artifact_id"] for node in terminal["node_executions"])

    python_view = requests.get(f"{PY_API}/api/v1/jobs/{job_id}", timeout=10)
    assert python_view.status_code == 200
    assert python_view.json()["status"] == terminal["status"]

    assert pending_count("vp:events:go", "orchestrator-go") == 0
    assert pending_count("vp:tasks:ffmpeg_go", "ffmpeg_go-workers") == 0


@pytest.mark.skipif(not STRICT, reason="set VP_GO_PHASE6_STRICT=1 for live Go orchestrator tests")
def test_non_eligible_pipeline_rejected_without_job(tmp_path: Path) -> None:
    asset_id = upload_video_asset(tmp_path)
    pipeline_id = create_pipeline(asset_id, "smart_trim", PY_API)
    before = job_count()
    response = requests.post(f"{GO_API}/api/v1/jobs", json={"pipeline_id": pipeline_id, "inputs": {}}, timeout=20)
    assert response.status_code == 501, response.text
    assert "Python-owned" in response.json()["detail"]
    assert job_count() == before


def pending_count(stream: str, group: str) -> int:
    output = subprocess.check_output(["redis-cli", "-u", REDIS_URL, "XPENDING", stream, group], text=True)
    return int(output.splitlines()[0].strip())


def job_count() -> int:
    response = requests.get(f"{PY_API}/api/v1/jobs", timeout=10)
    assert response.status_code == 200, response.text
    return int(response.json()["total"])


def wait_for_terminal_job(api_url: str, job_id: str) -> dict[str, Any]:
    body: dict[str, Any] = {}
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        response = requests.get(f"{api_url}/api/v1/jobs/{job_id}", timeout=10)
        assert response.status_code == 200, response.text
        body = response.json()
        if body["status"] in {"SUCCEEDED", "FAILED", "CANCELLED", "PARTIALLY_FAILED"}:
            return body
        time.sleep(2)
    raise AssertionError(f"job {job_id} did not reach terminal status: {body}")


def create_go_pipeline(asset_id: str) -> str:
    payload = {
        "name": "go-phase6-trim-transcode-export",
        "description": "Phase 6 Go orchestrator strict test graph",
        "definition": go_pipeline_definition(asset_id),
        "is_template": False,
        "template_tags": [],
    }
    response = requests.post(f"{GO_API}/api/v1/pipelines", json=payload, timeout=20)
    assert response.status_code in {200, 201}, response.text
    return response.json()["id"]


def create_pipeline(asset_id: str, node_type: str, api_url: str) -> str:
    payload = {
        "name": f"go-phase6-noneligible-{node_type}",
        "description": "Phase 6 non-eligible no-fallback graph",
        "definition": noneligible_pipeline_definition(asset_id, node_type),
        "is_template": False,
        "template_tags": [],
    }
    response = requests.post(f"{api_url}/api/v1/pipelines", json=payload, timeout=20)
    assert response.status_code in {200, 201}, response.text
    return response.json()["id"]


def upload_video_asset(tmp_path: Path) -> str:
    if asset_id := os.getenv("VP_GO_PHASE6_VIDEO_ASSET_ID"):
        return asset_id
    source = tmp_path / "go-phase6-source.mp4"
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
    with source.open("rb") as fh:
        response = requests.post(
            f"{PY_API}/api/v1/assets/upload",
            files={"file": (source.name, fh, "video/mp4")},
            timeout=60,
        )
    assert response.status_code in {200, 201}, response.text
    return response.json()["id"]


def go_pipeline_definition(asset_id: str) -> dict[str, Any]:
    return graph(
        [
            source_node("source_1", asset_id, 0),
            work_node("trim_1", "trim", {"start_time": "0", "duration": "1.5"}, 300),
            work_node("transcode_1", "transcode", {"video_codec": "libx264", "audio_codec": "aac", "crf": 20}, 600),
            work_node("export_1", "export", {"output_dir": "/tmp/vp_phase6_exports", "filename": "phase6.mp4"}, 900),
        ],
        [
            edge("e1", "source_1", "output", "trim_1", "input"),
            edge("e2", "trim_1", "output", "transcode_1", "input"),
            edge("e3", "transcode_1", "output", "export_1", "input"),
        ],
    )


def noneligible_pipeline_definition(asset_id: str, node_type: str) -> dict[str, Any]:
    if node_type != "smart_trim":
        raise AssertionError(node_type)
    return graph(
        [
            source_node("source_1", asset_id, 0),
            work_node(
                "smart_trim_1",
                "smart_trim",
                {
                    "prompt": "test pattern",
                    "mode": "auto",
                    "target_duration": 1,
                    "min_clip_duration": 0.5,
                    "max_clip_duration": 2,
                    "max_clips": 1,
                    "sample_fps": 1,
                    "match_threshold": 0.35,
                    "return_full_threshold": 0.65,
                    "padding_before": 0,
                    "padding_after": 0,
                    "merge_gap": 0,
                    "output_format": "mp4",
                    "no_match_policy": "placeholder",
                },
                300,
            ),
            work_node("export_1", "export", {"output_dir": "/tmp/vp_phase6_exports", "filename": "noneligible.mp4"}, 600),
        ],
        [
            edge("e1", "source_1", "output", "smart_trim_1", "input"),
            edge("e2", "smart_trim_1", "output", "export_1", "input"),
        ],
    )


def source_node(node_id: str, asset_id: str, x: int) -> dict[str, Any]:
    return {
        "id": node_id,
        "type": "source",
        "position": {"x": x, "y": 0},
        "data": {
            "label": node_id,
            "asset_id": asset_id,
            "config": {"asset_id": asset_id, "media_type": "video"},
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
