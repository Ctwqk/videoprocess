from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
from datetime import datetime, timezone
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


def pending_node_execution_ids(client: Any, node_execution_ids: set[str]) -> list[str]:
    matching: list[str] = []
    pending_entries = client.xpending_range(
        "vp:tasks:ffmpeg_go",
        "ffmpeg_go-workers",
        min="-",
        max="+",
        count=1000,
    )
    for pending in pending_entries:
        message_id = str(pending["message_id"])
        entries = client.xrange(
            "vp:tasks:ffmpeg_go",
            min=message_id,
            max=message_id,
            count=1,
        )
        for _, data in entries:
            node_execution_id = str(data.get("node_execution_id", ""))
            if node_execution_id in node_execution_ids:
                matching.append(node_execution_id)
    return matching


def wait_for_node_acknowledgements(node_execution_ids: set[str]) -> None:
    client = redis_client()
    deadline = time.time() + 10
    while time.time() < deadline:
        pending = pending_node_execution_ids(client, node_execution_ids)
        if not pending:
            return
        time.sleep(0.25)
    pytest.fail(f"smoke node messages remain pending: {pending}")


class _PendingRedisStub:
    def xpending_range(self, *_args: Any, **_kwargs: Any) -> list[dict[str, str]]:
        return [{"message_id": "1-0"}, {"message_id": "2-0"}]

    def xrange(
        self,
        _stream: str,
        *,
        min: str,
        max: str,
        count: int,
    ) -> list[tuple[str, dict[str, str]]]:
        assert min == max
        assert count == 1
        node_id = "target-node" if min == "1-0" else "unrelated-node"
        return [(min, {"node_execution_id": node_id})]


def test_pending_node_execution_ids_filters_unrelated_production_work() -> None:
    assert pending_node_execution_ids(_PendingRedisStub(), {"target-node"}) == [
        "target-node"
    ]


def wait_for_job(job_id: str) -> dict[str, Any]:
    deadline = time.time() + 180
    last_payload: dict[str, Any] = {}
    while time.time() < deadline:
        last_payload = get_json(f"/api/v1/jobs/{job_id}")
        if last_payload["status"] in {"SUCCEEDED", "FAILED", "CANCELLED", "PARTIALLY_FAILED"}:
            return last_payload
        time.sleep(2)
    pytest.fail(f"job {job_id} did not finish before timeout; last payload={last_payload}")


def node_by_id(job: dict[str, Any], node_id: str) -> dict[str, Any]:
    matches = [node for node in job["node_executions"] if node["node_id"] == node_id]
    assert len(matches) == 1, job
    return matches[0]


def download_artifact(artifact_id: str, output_path: Path) -> None:
    response = httpx.get(
        f"{PYTHON_API}/api/v1/artifacts/{artifact_id}/download",
        timeout=60,
        follow_redirects=True,
    )
    response.raise_for_status()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(response.content)
    assert output_path.stat().st_size > 0


def probe_video(output_path: Path) -> dict[str, Any]:
    result = subprocess.run(
        [
            "ffprobe",
            "-v",
            "error",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
            str(output_path),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    probe = json.loads(result.stdout)
    assert any(stream.get("codec_type") == "video" for stream in probe.get("streams", []))
    assert float(probe.get("format", {}).get("duration", 0)) > 0
    return probe


def write_smoke_evidence(
    output_path: Path,
    *,
    asset_id: str,
    pipeline_id: str,
    job: dict[str, Any],
    artifact_id: str,
    worker_ids: list[str],
    probe: dict[str, Any],
) -> Path:
    evidence_path = output_path.with_suffix(".json")
    evidence = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "api_url": PYTHON_API,
        "source_commit": os.environ.get("VP_SMOKE_COMMIT", ""),
        "deployed_commit": os.environ.get("VP_SMOKE_DEPLOYED_COMMIT", ""),
        "asset_id": asset_id,
        "pipeline_id": pipeline_id,
        "job_id": job["id"],
        "job_status": job["status"],
        "artifact_id": artifact_id,
        "worker_ids": worker_ids,
        "sha256": hashlib.sha256(output_path.read_bytes()).hexdigest(),
        "file_size": output_path.stat().st_size,
        "probe": probe,
    }
    evidence_path.write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return evidence_path


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
    source_node = node_by_id(final_job, "source_1")
    trim_node = node_by_id(final_job, "trim_1")
    export_node = node_by_id(final_job, "export_1")
    for node in (source_node, trim_node, export_node):
        assert node["status"] == "SUCCEEDED", final_job
    for node in (trim_node, export_node):
        assert node["output_artifact_id"]
        assert node["worker_id"]
        assert node["worker_id"].startswith("ffmpeg_go-worker@colima-127:")

    if output_value := os.environ.get("VP_GO_SMOKE_OUTPUT"):
        output_path = Path(output_value).expanduser().resolve()
        artifact_id = export_node["output_artifact_id"]
        download_artifact(artifact_id, output_path)
        probe = probe_video(output_path)
        evidence_path = write_smoke_evidence(
            output_path,
            asset_id=asset_id,
            pipeline_id=pipeline["id"],
            job=final_job,
            artifact_id=artifact_id,
            worker_ids=[trim_node["worker_id"], export_node["worker_id"]],
            probe=probe,
        )
        print(f"retained_video={output_path}")
        print(f"retained_evidence={evidence_path}")
    wait_for_node_acknowledgements({trim_node["id"], export_node["id"]})
