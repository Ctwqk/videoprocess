from __future__ import annotations

import os
import time
from typing import Any

import httpx
import pytest


STRICT = os.environ.get("VP_GO_WORKER_SMOKE_STRICT", "").lower() in {"1", "true", "yes", "on"}
PYTHON_API = os.environ.get("VP_PYTHON_API", "http://127.0.0.1:18080")


def require_strict() -> None:
    if not STRICT:
        pytest.skip("set VP_GO_WORKER_SMOKE_STRICT=1 after compose services and fixture media are ready")
    if not os.environ.get("VP_GO_SMOKE_ASSET_ID"):
        pytest.fail("VP_GO_SMOKE_ASSET_ID must point to an existing video asset id")


def post_json(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    response = httpx.post(f"{PYTHON_API}{path}", json=payload, timeout=20)
    response.raise_for_status()
    return response.json()


def get_json(path: str) -> dict[str, Any]:
    response = httpx.get(f"{PYTHON_API}{path}", timeout=20)
    response.raise_for_status()
    return response.json()


def wait_for_job(job_id: str) -> dict[str, Any]:
    deadline = time.time() + 180
    last_payload: dict[str, Any] = {}
    while time.time() < deadline:
        last_payload = get_json(f"/api/v1/jobs/{job_id}")
        if last_payload["status"] in {"SUCCEEDED", "FAILED", "CANCELLED", "PARTIALLY_FAILED"}:
            return last_payload
        time.sleep(2)
    pytest.fail(f"job {job_id} did not finish before timeout; last payload={last_payload}")


def test_trim_worker_mixed_mode_smoke_requires_real_job_completion() -> None:
    require_strict()
    asset_id = os.environ["VP_GO_SMOKE_ASSET_ID"]
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
