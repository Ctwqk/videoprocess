from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

import requests


TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED", "PARTIALLY_FAILED"}


def run_ffmpeg(args: list[str]) -> None:
    subprocess.run(["ffmpeg", "-y", "-hide_banner", "-loglevel", "error", *args], check=True)


def redis_pending(redis_url: str, stream: str, group: str) -> int:
    output = subprocess.check_output(["redis-cli", "-u", redis_url, "XPENDING", stream, group], text=True)
    return int(output.splitlines()[0].strip())


def upload_video_asset(api_url: str, tmp_path: Path) -> str:
    video = tmp_path / "go-phase6-acceptance.mp4"
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
            "4",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            str(video),
        ]
    )
    with video.open("rb") as fh:
        response = requests.post(
            f"{api_url}/api/v1/assets/upload",
            files={"file": (video.name, fh, "video/mp4")},
            timeout=60,
        )
    response.raise_for_status()
    return response.json()["id"]


def create_pipeline(api_url: str, name: str, definition: dict[str, Any]) -> str:
    response = requests.post(
        f"{api_url}/api/v1/pipelines",
        json={
            "name": name,
            "description": "Go Phase 6 orchestrator acceptance",
            "definition": definition,
            "is_template": False,
            "template_tags": [],
        },
        timeout=20,
    )
    response.raise_for_status()
    return response.json()["id"]


def create_job(api_go_url: str, pipeline_id: str) -> dict[str, Any]:
    response = requests.post(f"{api_go_url}/api/v1/jobs", json={"pipeline_id": pipeline_id, "inputs": {}}, timeout=20)
    response.raise_for_status()
    return response.json()


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


def artifact_download_ok(api_url: str, artifact_id: str) -> bool:
    response = requests.get(f"{api_url}/api/v1/artifacts/{artifact_id}/download", timeout=30)
    return response.status_code == 200 and bool(response.content)


def job_count(api_url: str) -> int:
    response = requests.get(f"{api_url}/api/v1/jobs", timeout=10)
    response.raise_for_status()
    return int(response.json()["total"])


def reject_non_eligible(api_go_url: str, python_api_url: str, asset_id: str) -> bool:
    pipeline_id = create_pipeline(python_api_url, "go-phase6-noneligible-smart-trim", smart_trim_definition(asset_id))
    before = job_count(python_api_url)
    response = requests.post(f"{api_go_url}/api/v1/jobs", json={"pipeline_id": pipeline_id, "inputs": {}}, timeout=20)
    after = job_count(python_api_url)
    if response.status_code != 501:
        return False
    detail = response.json().get("detail", "")
    return "Python-owned" in detail and before == after


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


def acceptance_definition(asset_id: str) -> dict[str, Any]:
    return graph(
        [
            source_node("source_a", asset_id, 0),
            source_node("source_b", asset_id, 0),
            work_node("trim_a", "trim", {"start_time": "0", "duration": "1.5"}, 260),
            work_node("trim_b", "trim", {"start_time": "1", "duration": "1.5"}, 260),
            work_node("concat_1", "concat_vertical", {"resize_mode": "match_width"}, 520),
            work_node("title_1", "title_overlay", {"text": "Go Phase 6", "position": "top", "duration": 1.5}, 780),
            work_node("transcode_1", "transcode", {"video_codec": "libx264", "audio_codec": "aac", "crf": 20}, 1040),
            work_node("export_1", "export", {"output_dir": "/tmp/vp_phase6_acceptance", "filename": "phase6.mp4"}, 1300),
        ],
        [
            edge("e1", "source_a", "output", "trim_a", "input"),
            edge("e2", "source_b", "output", "trim_b", "input"),
            edge("e3", "trim_a", "output", "concat_1", "video_top"),
            edge("e4", "trim_b", "output", "concat_1", "video_bottom"),
            edge("e5", "concat_1", "output", "title_1", "input"),
            edge("e6", "title_1", "output", "transcode_1", "input"),
            edge("e7", "transcode_1", "output", "export_1", "input"),
        ],
    )


def smart_trim_definition(asset_id: str) -> dict[str, Any]:
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
            work_node("export_1", "export", {"output_dir": "/tmp/vp_phase6_acceptance", "filename": "noneligible.mp4"}, 600),
        ],
        [
            edge("e1", "source_1", "output", "smart_trim_1", "input"),
            edge("e2", "smart_trim_1", "output", "export_1", "input"),
        ],
    )


def run_acceptance(api_go_url: str, python_api_url: str, redis_url: str, count: int, timeout_seconds: int) -> dict[str, Any]:
    evidence = {
        "jobs_completed": 0,
        "go_event_pending": 0,
        "go_task_pending": 0,
        "wrong_owner": 0,
        "wrong_worker": 0,
        "missing_final_artifact": 0,
        "non_eligible_rejected": False,
    }
    with tempfile.TemporaryDirectory() as tmp:
        asset_id = upload_video_asset(python_api_url, Path(tmp))
        pipeline_id = create_pipeline(api_go_url, "go-phase6-acceptance", acceptance_definition(asset_id))
        for _ in range(count):
            created = create_job(api_go_url, pipeline_id)
            if created.get("orchestrator_owner") != "go":
                evidence["wrong_owner"] += 1
            terminal = wait_for_job(api_go_url, created["id"], timeout_seconds)
            if terminal.get("status") != "SUCCEEDED":
                raise RuntimeError(f"job {created['id']} finished with {terminal}")
            evidence["jobs_completed"] += 1
            worker_nodes = [node for node in terminal.get("node_executions", []) if node.get("node_type") != "source"]
            for node in worker_nodes:
                if "ffmpeg_go-worker@" not in (node.get("worker_id") or ""):
                    evidence["wrong_worker"] += 1
            final_nodes = [node for node in terminal.get("node_executions", []) if node.get("node_id") == "export_1"]
            final_artifact_id = final_nodes[0].get("output_artifact_id") if final_nodes else None
            if not final_artifact_id or not artifact_download_ok(api_go_url, final_artifact_id):
                evidence["missing_final_artifact"] += 1
        evidence["non_eligible_rejected"] = reject_non_eligible(api_go_url, python_api_url, asset_id)

    evidence["go_event_pending"] = redis_pending(redis_url, "vp:events:go", "orchestrator-go")
    evidence["go_task_pending"] = redis_pending(redis_url, "vp:tasks:ffmpeg_go", "ffmpeg_go-workers")
    return evidence


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-go-url", default="http://127.0.0.1:18081")
    parser.add_argument("--python-api-url", default="http://127.0.0.1:18080")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6380/0")
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument("--timeout-seconds", type=int, default=240)
    args = parser.parse_args()

    evidence = run_acceptance(args.api_go_url, args.python_api_url, args.redis_url, args.count, args.timeout_seconds)
    print(json.dumps(evidence, indent=2, sort_keys=True))
    if (
        evidence["jobs_completed"] != args.count
        or evidence["go_event_pending"] != 0
        or evidence["go_task_pending"] != 0
        or evidence["wrong_owner"] != 0
        or evidence["wrong_worker"] != 0
        or evidence["missing_final_artifact"] != 0
        or not evidence["non_eligible_rejected"]
    ):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
