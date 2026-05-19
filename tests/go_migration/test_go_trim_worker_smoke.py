from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import httpx
import pytest
import redis


STRICT = os.environ.get("VP_GO_WORKER_SMOKE_STRICT", "").lower() in {"1", "true", "yes", "on"}
PYTHON_API = os.environ.get("VP_PYTHON_API", "http://127.0.0.1:18080")


def require_strict() -> None:
    if not STRICT:
        pytest.skip("set VP_GO_WORKER_SMOKE_STRICT=1 after compose services and fixture media are ready")


def post_json(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    response = httpx.post(f"{PYTHON_API}{path}", json=payload, timeout=20)
    response.raise_for_status()
    return response.json()


def get_json(path: str) -> dict[str, Any]:
    response = httpx.get(f"{PYTHON_API}{path}", timeout=20)
    response.raise_for_status()
    return response.json()


def redis_client() -> redis.Redis:
    url = os.environ.get("VP_REDIS_URL", "redis://127.0.0.1:6380/0")
    return redis.Redis.from_url(url, decode_responses=True)


def pending_count() -> int:
    pending = redis_client().xpending("vp:tasks:ffmpeg_go", "ffmpeg_go-workers")
    if isinstance(pending, dict):
        return int(pending.get("pending", 0))
    return int(pending["pending"])


def wait_for_job(job_id: str) -> dict[str, Any]:
    deadline = time.time() + 180
    last_payload: dict[str, Any] = {}
    while time.time() < deadline:
        last_payload = get_json(f"/api/v1/jobs/{job_id}")
        if last_payload["status"] in {"SUCCEEDED", "FAILED", "CANCELLED", "PARTIALLY_FAILED"}:
            return last_payload
        time.sleep(2)
    pytest.fail(f"job {job_id} did not finish before timeout; last payload={last_payload}")


def ensure_asset_id() -> str:
    if asset_id := os.environ.get("VP_GO_SMOKE_ASSET_ID"):
        return asset_id

    with TemporaryDirectory() as tmp:
        source = Path(tmp) / "go-trim-smoke-source.mp4"
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
            response = httpx.post(
                f"{PYTHON_API}/api/v1/assets/upload",
                files={"file": ("go-trim-smoke-source.mp4", fh, "video/mp4")},
                timeout=60,
            )
        response.raise_for_status()
        return response.json()["id"]


def test_trim_worker_mixed_mode_smoke_requires_real_job_completion() -> None:
    require_strict()
    asset_id = ensure_asset_id()
    pipeline_payload = {
        "name": "go-trim-smoke",
        "description": "Mixed-mode smoke: Python orchestrator dispatches trim to ffmpeg_go.",
        "definition": {
            "nodes": [
                {
                    "id": "source_1",
                    "type": "source",
                    "position": {"x": 0, "y": 0},
                    "data": {
                        "label": "Source",
                        "config": {"asset_id": asset_id, "media_type": "video"},
                        "asset_id": asset_id,
                    },
                },
                {
                    "id": "trim_1",
                    "type": "trim",
                    "position": {"x": 260, "y": 0},
                    "data": {
                        "label": "Trim",
                        "config": {"start_time": "0", "duration": "1", "output_format": "mp4"},
                    },
                },
                {
                    "id": "export_1",
                    "type": "export",
                    "position": {"x": 520, "y": 0},
                    "data": {
                        "label": "Export",
                        "config": {
                            "output_dir": "/tmp/vp_autoflow_exports",
                            "filename": "go-trim-smoke.mp4",
                        },
                    },
                },
            ],
            "edges": [
                {
                    "id": "e1",
                    "source": "source_1",
                    "target": "trim_1",
                    "sourceHandle": "output",
                    "targetHandle": "input",
                },
                {
                    "id": "e2",
                    "source": "trim_1",
                    "target": "export_1",
                    "sourceHandle": "output",
                    "targetHandle": "input",
                },
            ],
            "viewport": {"x": 0, "y": 0, "zoom": 1},
        },
        "is_template": False,
        "template_tags": [],
    }

    pipeline = post_json("/api/v1/pipelines", pipeline_payload)
    job = post_json("/api/v1/jobs", {"pipeline_id": pipeline["id"], "inputs": {}})
    final_job = wait_for_job(job["id"])

    assert final_job["status"] == "SUCCEEDED", final_job
    trim_nodes = [node for node in final_job["node_executions"] if node["node_id"] == "trim_1"]
    assert len(trim_nodes) == 1
    assert trim_nodes[0]["status"] == "SUCCEEDED"
    assert trim_nodes[0]["output_artifact_id"]
    assert trim_nodes[0]["worker_id"]
    assert "ffmpeg_go-worker@" in trim_nodes[0]["worker_id"]
    assert pending_count() == 0
