from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import requests


ROOT_DIR = Path(__file__).resolve().parents[2]
CONSTRUCTURE_ROOT = ROOT_DIR.parent.parent
PLATFORM_UPLOAD_ROOT = Path(
    os.environ.get("VP_PLATFORM_UPLOAD_ROOT", CONSTRUCTURE_ROOT / "platform-upload")
)
TMP_DIR = Path("/tmp/vp_smoke")
TMP_DIR.mkdir(parents=True, exist_ok=True)

API_BASE = os.environ.get("VP_API_BASE", "http://localhost:8080/api/v1")
API_ROOT = API_BASE.rsplit("/api/v1", 1)[0]
FRONTEND_BASE = os.environ.get("VP_FRONTEND_BASE", "http://localhost:3001")
VITE_BASE = os.environ.get("VP_VITE_BASE", "http://localhost:5173")
YT_BASE_CANDIDATES = [
    os.environ.get("VP_YT_BASE", "").rstrip("/"),
    "http://localhost:8899/api",
    f"{FRONTEND_BASE.rstrip('/')}/youtube/api",
    f"{VITE_BASE.rstrip('/')}/youtube/api",
]
YT_DOWNLOAD_DIR = PLATFORM_UPLOAD_ROOT / "YouTubeManager" / "downloads"
YT_TOKEN_FILE = PLATFORM_UPLOAD_ROOT / "YouTubeManager" / "credentials" / "token.json"
YT_QUOTA_FILE = PLATFORM_UPLOAD_ROOT / "YouTubeManager" / "credentials" / "quota_usage.json"

TERMINAL_JOB_STATES = {"SUCCEEDED", "FAILED", "CANCELLED", "PARTIALLY_FAILED"}
TERMINAL_YT_TASK_STATES = {"completed", "failed"}


class SmokeFailure(RuntimeError):
    pass


def now_slug() -> str:
    return str(int(time.time()))


def print_step(message: str) -> None:
    print(f"[smoke] {message}", flush=True)


def request(method: str, url: str, *, expected: int | None = None, **kwargs):
    response = requests.request(method, url, timeout=120, **kwargs)
    if expected is not None and response.status_code != expected:
        raise SmokeFailure(
            f"{method} {url} -> {response.status_code}, expected {expected}, body={response.text[:500]}"
        )
    return response


def resolve_youtube_base() -> str:
    for candidate in YT_BASE_CANDIDATES:
        if not candidate:
            continue
        base = candidate.rstrip("/")
        if not base.endswith("/api"):
            continue
        try:
            response = requests.get(f"{base}/auth/status", timeout=5)
        except Exception:
            continue
        if response.status_code == 200:
            return base
    raise SmokeFailure("Could not reach any YouTube API base candidate")


def json_request(method: str, url: str, *, expected: int | None = None, **kwargs):
    response = request(method, url, expected=expected, **kwargs)
    try:
        return response.json()
    except Exception as exc:
        raise SmokeFailure(f"{method} {url} did not return JSON: {response.text[:500]}") from exc


def expect(results: list[dict], name: str, condition: bool, detail: str = "") -> None:
    results.append({"name": name, "ok": condition, "detail": detail})
    if not condition:
        raise SmokeFailure(f"{name} failed: {detail}")


def create_test_video() -> Path:
    video_path = TMP_DIR / "vp_smoke.mp4"
    cmd = [
        "ffmpeg",
        "-y",
        "-f",
        "lavfi",
        "-i",
        "color=c=black:s=640x360:d=2",
        "-f",
        "lavfi",
        "-i",
        "sine=frequency=1000:duration=2",
        "-c:v",
        "libx264",
        "-c:a",
        "aac",
        "-shortest",
        str(video_path),
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return video_path


def probe_duration(file_path: Path) -> float:
    cmd = [
        "ffprobe",
        "-v",
        "quiet",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(file_path),
    ]
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return float(result.stdout.strip())


def download_artifact_file(artifact_id: str, filename: str) -> Path:
    target = TMP_DIR / filename
    response = request("GET", f"{API_BASE}/artifacts/{artifact_id}/download", expected=200)
    target.write_bytes(response.content)
    return target


def wait_for_job(job_id: str, *, timeout_seconds: int = 120) -> dict:
    deadline = time.time() + timeout_seconds
    last = None
    while time.time() < deadline:
        last = json_request("GET", f"{API_BASE}/jobs/{job_id}", expected=200)
        if last["status"] in TERMINAL_JOB_STATES:
            return last
        time.sleep(1)
    raise SmokeFailure(f"Job {job_id} did not reach a terminal state: {last}")


def wait_for_task(yt_base: str, task_id: str, *, timeout_seconds: int = 180) -> dict:
    deadline = time.time() + timeout_seconds
    last = None
    while time.time() < deadline:
        last = json_request("GET", f"{yt_base}/status/{task_id}", expected=200)
        if last["status"] in TERMINAL_YT_TASK_STATES:
            return last
        time.sleep(1)
    raise SmokeFailure(f"YouTubeManager task {task_id} did not reach a terminal state: {last}")


def restore_token(token_bytes: bytes | None) -> None:
    if token_bytes is None:
        return
    YT_TOKEN_FILE.unlink(missing_ok=True)
    YT_TOKEN_FILE.write_bytes(token_bytes)


def restore_quota_usage(quota_bytes: bytes | None) -> None:
    if quota_bytes is None:
        return
    YT_QUOTA_FILE.unlink(missing_ok=True)
    YT_QUOTA_FILE.write_bytes(quota_bytes)


def delete_youtube_video(video_id: str) -> None:
    script = f"""
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

creds = Credentials.from_authorized_user_file('/app/credentials/token.json', [
    'https://www.googleapis.com/auth/youtube.upload',
    'https://www.googleapis.com/auth/youtube',
])
youtube = build('youtube', 'v3', credentials=creds)
youtube.videos().delete(id={video_id!r}).execute()
"""
    try:
        subprocess.run(
            ["docker", "exec", "-i", "youtube_manager", "python", "-c", script],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass


def backend_definition(asset_id: str) -> dict:
    return {
        "nodes": [
            {
                "id": "src",
                "type": "source",
                "position": {"x": 0, "y": 0},
                "data": {
                    "label": "src",
                    "config": {"asset_id": asset_id, "media_type": "video"},
                    "asset_id": None,
                },
            },
            {
                "id": "trim",
                "type": "trim",
                "position": {"x": 240, "y": 0},
                "data": {
                    "label": "trim",
                    "config": {"start_time": "00:00:00", "duration": "1"},
                    "asset_id": None,
                },
            },
            {
                "id": "trans",
                "type": "transcode",
                "position": {"x": 480, "y": 0},
                "data": {
                    "label": "trans",
                    "config": {
                        "format": "mp4",
                        "video_codec": "libx264",
                        "audio_codec": "aac",
                        "resolution": "original",
                        "bitrate": "",
                        "crf": 23,
                        "preset": "medium",
                    },
                    "asset_id": None,
                },
            },
        ],
        "edges": [
            {
                "id": "e1",
                "source": "src",
                "target": "trim",
                "sourceHandle": "output",
                "targetHandle": "input",
            },
            {
                "id": "e2",
                "source": "trim",
                "target": "trans",
                "sourceHandle": "output",
                "targetHandle": "input",
            },
        ],
        "viewport": {"x": 0, "y": 0, "zoom": 1},
    }


def run_backend_smoke(results: list[dict], cleanup: dict) -> None:
    print_step("testing frontend proxy configuration")
    response = request("GET", f"{FRONTEND_BASE}/api/v1/node-types", expected=200)
    expect(results, "frontend proxy /api on 3001", response.headers.get("content-type", "").startswith("application/json"))
    response = request("GET", f"{FRONTEND_BASE}/youtube/api/auth/status", expected=200)
    expect(results, "frontend proxy /youtube on 3001", response.headers.get("content-type", "").startswith("application/json"))
    response = request("GET", f"{VITE_BASE}/api/v1/node-types", expected=200)
    expect(results, "frontend proxy /api on 5173", response.headers.get("content-type", "").startswith("application/json"))
    response = request("GET", f"{VITE_BASE}/youtube/api/auth/status", expected=200)
    expect(results, "frontend proxy /youtube on 5173", response.headers.get("content-type", "").startswith("application/json"))

    print_step("testing backend REST API")
    health = json_request("GET", f"{API_ROOT}/health", expected=200)
    expect(results, "backend /health", health.get("status") == "ok", json.dumps(health))

    node_types = json_request("GET", f"{API_BASE}/node-types", expected=200)
    expect(results, "list node types", len(node_types) > 0, f"count={len(node_types)}")
    sample_type = node_types[0]["type_name"]
    node_type = json_request("GET", f"{API_BASE}/node-types/{sample_type}", expected=200)
    expect(results, "get node type", node_type["type_name"] == sample_type, sample_type)

    video_path = create_test_video()
    with video_path.open("rb") as f:
        asset = json_request(
            "POST",
            f"{API_BASE}/assets/upload",
            expected=200,
            files={"file": ("vp_smoke.mp4", f, "video/mp4")},
        )
    cleanup["assets"].append(asset["id"])
    expect(results, "asset upload", asset["original_name"] == "vp_smoke.mp4", asset["id"])

    assets = json_request("GET", f"{API_BASE}/assets", expected=200)
    expect(results, "list assets", any(item["id"] == asset["id"] for item in assets["items"]), asset["id"])
    fetched_asset = json_request("GET", f"{API_BASE}/assets/{asset['id']}", expected=200)
    expect(results, "get asset", fetched_asset["id"] == asset["id"])
    response = request("GET", f"{API_BASE}/assets/{asset['id']}/download", expected=200)
    expect(results, "download asset", response.headers.get("content-type", "").startswith("video/mp4"), response.headers.get("content-type", ""))

    definition = backend_definition(asset["id"])
    validation = json_request("POST", f"{API_BASE}/pipelines/validate", expected=200, json=definition)
    expect(results, "validate pipeline", validation["valid"] is True, json.dumps(validation))

    hard_delete_payload = {
        "name": f"hard-delete-{now_slug()}",
        "description": "hard delete smoke",
        "definition": definition,
        "is_template": False,
        "template_tags": [],
    }
    hard_delete_pipeline = json_request("POST", f"{API_BASE}/pipelines", expected=201, json=hard_delete_payload)
    hard_delete_result = json_request("DELETE", f"{API_BASE}/pipelines/{hard_delete_pipeline['id']}", expected=200)
    expect(results, "delete unreferenced pipeline", hard_delete_result["status"] == "deleted")

    payload = {
        "name": f"api-smoke-{now_slug()}",
        "description": "comprehensive smoke",
        "definition": definition,
        "is_template": True,
        "template_tags": ["smoke"],
    }
    pipeline = json_request("POST", f"{API_BASE}/pipelines", expected=201, json=payload)
    cleanup["pipelines"].append(pipeline["id"])
    expect(results, "create pipeline", pipeline["is_template"] is True, pipeline["id"])

    fetched_pipeline = json_request("GET", f"{API_BASE}/pipelines/{pipeline['id']}", expected=200)
    expect(results, "get pipeline", fetched_pipeline["id"] == pipeline["id"])

    updated_pipeline = json_request(
        "PUT",
        f"{API_BASE}/pipelines/{pipeline['id']}",
        expected=200,
        json={"description": "comprehensive smoke updated"},
    )
    expect(results, "update pipeline", updated_pipeline["description"] == "comprehensive smoke updated")

    duplicate = json_request("POST", f"{API_BASE}/pipelines/{pipeline['id']}/duplicate", expected=201)
    cleanup["pipelines"].append(duplicate["id"])
    expect(results, "duplicate pipeline", duplicate["id"] != pipeline["id"])

    templates = json_request("GET", f"{API_BASE}/templates", expected=200)
    expect(results, "list templates", any(item["id"] == pipeline["id"] for item in templates["items"]), pipeline["id"])

    job = json_request(
        "POST",
        f"{API_BASE}/jobs",
        expected=201,
        json={
            "pipeline_id": pipeline["id"],
            "inputs": {
                "src": {"asset_id": asset["id"]},
                "trim": {"start_time": "00:00:00", "duration": "2"},
            },
        },
    )
    cleanup["jobs"].append(job["id"])
    expect(results, "submit job", job["status"] in {"PENDING", "PLANNING", "RUNNING", "SUCCEEDED"})

    cancel_job = json_request("POST", f"{API_BASE}/jobs", expected=201, json={"pipeline_id": duplicate["id"]})
    cleanup["jobs"].append(cancel_job["id"])
    cancelled = json_request("POST", f"{API_BASE}/jobs/{cancel_job['id']}/cancel", expected=200)
    expect(results, "cancel job endpoint", cancelled["status"] in TERMINAL_JOB_STATES | {"RUNNING", "PENDING", "PLANNING"}, cancelled["status"])

    detail = wait_for_job(job["id"])
    expect(results, "job terminal state", detail["status"] == "SUCCEEDED", detail["status"])

    trim_artifact_id = next(node for node in detail["node_executions"] if node["node_id"] == "trim")["output_artifact_id"]
    final_artifact_id = next(node for node in detail["node_executions"] if node["node_id"] == "trans")["output_artifact_id"]
    expect(results, "job produced artifacts", bool(trim_artifact_id and final_artifact_id), json.dumps(detail["node_executions"]))
    rendered_path = download_artifact_file(final_artifact_id, f"job-{job['id']}.mp4")
    rendered_duration = probe_duration(rendered_path)
    expect(results, "job input overrides applied", rendered_duration >= 1.8, f"duration={rendered_duration}")

    artifact = json_request("GET", f"{API_BASE}/artifacts/{trim_artifact_id}", expected=200)
    expect(results, "get artifact", artifact["id"] == trim_artifact_id)
    response = request("GET", f"{API_BASE}/artifacts/{final_artifact_id}/download", expected=200)
    expect(results, "download artifact", response.headers.get("content-type", "").startswith("video/"), response.headers.get("content-type", ""))

    cleanup_result = json_request("DELETE", f"{API_BASE}/artifacts/cleanup?job_id={job['id']}", expected=200)
    expect(results, "artifact cleanup", cleanup_result["deleted_count"] >= 1, json.dumps(cleanup_result))
    refreshed = json_request("GET", f"{API_BASE}/jobs/{job['id']}", expected=200)
    trim_node = next(node for node in refreshed["node_executions"] if node["node_id"] == "trim")
    trans_node = next(node for node in refreshed["node_executions"] if node["node_id"] == "trans")
    expect(results, "intermediate artifact cleared", trim_node["output_artifact_id"] is None, json.dumps(trim_node))
    expect(results, "final artifact preserved", trans_node["output_artifact_id"] == final_artifact_id, json.dumps(trans_node))

    batch_jobs = json_request(
        "POST",
        f"{API_BASE}/jobs/batch",
        expected=201,
        json={"pipeline_id": pipeline["id"], "inputs": [{"asset_id": asset["id"]}, {"asset_id": asset["id"]}]},
    )
    cleanup["jobs"].extend(job["id"] for job in batch_jobs)
    expect(results, "batch jobs submit", len(batch_jobs) == 2, str(len(batch_jobs)))
    for batch_job in batch_jobs:
        batch_detail = wait_for_job(batch_job["id"])
        expect(results, f"batch job {batch_job['id'][:8]}", batch_detail["status"] == "SUCCEEDED", batch_detail["status"])

    template_job = json_request(
        "POST",
        f"{API_BASE}/templates/{pipeline['id']}/execute",
        expected=201,
        json={
            "inputs": {
                "src.asset_id": asset["id"],
                "trim.start_time": "00:00:00",
                "trim.duration": "1",
            }
        },
    )
    cleanup["jobs"].append(template_job["id"])
    expect(results, "template execute", template_job["status"] in {"PENDING", "PLANNING", "RUNNING", "SUCCEEDED"})
    template_detail = wait_for_job(template_job["id"])
    expect(results, "template execute terminal", template_detail["status"] == "SUCCEEDED", template_detail["status"])
    template_final_artifact_id = next(
        node for node in template_detail["node_executions"] if node["node_id"] == "trans"
    )["output_artifact_id"]
    template_path = download_artifact_file(template_final_artifact_id, f"template-{template_job['id']}.mp4")
    template_duration = probe_duration(template_path)
    expect(results, "template execute overrides applied", 0.8 <= template_duration <= 1.3, f"duration={template_duration}")

    template_batch_jobs = json_request(
        "POST",
        f"{API_BASE}/templates/{pipeline['id']}/execute/batch",
        expected=201,
        json={
            "items": [
                {
                    "src.asset_id": asset["id"],
                    "trim.start_time": "00:00:00",
                    "trim.duration": "1",
                },
                {
                    "src": {"asset_id": asset["id"]},
                    "trim": {"start_time": "00:00:00", "duration": "2"},
                },
            ]
        },
    )
    cleanup["jobs"].extend(job["id"] for job in template_batch_jobs)
    expect(results, "template execute batch", len(template_batch_jobs) == 2, str(len(template_batch_jobs)))
    expected_ranges = [(0.8, 1.3), (1.8, 2.3)]
    for index, template_batch_job in enumerate(template_batch_jobs):
        template_batch_detail = wait_for_job(template_batch_job["id"])
        expect(
            results,
            f"template batch terminal {index + 1}",
            template_batch_detail["status"] == "SUCCEEDED",
            template_batch_detail["status"],
        )
        template_batch_artifact_id = next(
            node for node in template_batch_detail["node_executions"] if node["node_id"] == "trans"
        )["output_artifact_id"]
        template_batch_path = download_artifact_file(
            template_batch_artifact_id,
            f"template-batch-{index + 1}-{template_batch_job['id']}.mp4",
        )
        template_batch_duration = probe_duration(template_batch_path)
        min_duration, max_duration = expected_ranges[index]
        expect(
            results,
            f"template batch override applied {index + 1}",
            min_duration <= template_batch_duration <= max_duration,
            f"duration={template_batch_duration}",
        )

    rerun = json_request("POST", f"{API_BASE}/jobs/{job['id']}/rerun", expected=201)
    cleanup["jobs"].append(rerun["id"])
    expect(results, "rerun job", rerun["id"] != job["id"])
    rerun_detail = wait_for_job(rerun["id"])
    expect(results, "rerun terminal", rerun_detail["status"] == "SUCCEEDED", rerun_detail["status"])

    deleted_job = json_request("DELETE", f"{API_BASE}/jobs/{rerun['id']}", expected=200)
    cleanup["jobs"].remove(rerun["id"])
    expect(results, "delete job", deleted_job["status"] == "deleted")

    deleted_pipeline = json_request("DELETE", f"{API_BASE}/pipelines/{pipeline['id']}", expected=200)
    cleanup["pipelines"].remove(pipeline["id"])
    expect(results, "delete referenced template", deleted_pipeline["status"] == "deleted")

    templates = json_request("GET", f"{API_BASE}/templates", expected=200)
    expect(results, "referenced template hidden from templates", all(item["id"] != pipeline["id"] for item in templates["items"]))
    surviving_job = json_request("GET", f"{API_BASE}/jobs/{job['id']}", expected=200)
    expect(results, "job survives template delete", surviving_job["id"] == job["id"])

    asset_delete = json_request("DELETE", f"{API_BASE}/assets/{asset['id']}", expected=200)
    cleanup["assets"].remove(asset["id"])
    expect(results, "delete asset", asset_delete["status"] == "deleted")


def run_youtube_smoke(results: list[dict], cleanup: dict) -> None:
    print_step("testing YouTubeManager API")
    yt_base = resolve_youtube_base()
    cleanup["yt_base"] = yt_base
    status = json_request("GET", f"{yt_base}/auth/status", expected=200)
    expect(results, "youtube auth status", isinstance(status.get("authenticated"), bool), json.dumps(status))

    auth_url = json_request("GET", f"{yt_base}/auth/url?return_to={FRONTEND_BASE}/editor", expected=200)
    expect(results, "youtube auth url", "accounts.google.com" in auth_url["url"])

    start_response = request(
        "GET",
        f"{yt_base}/auth/start?return_to={FRONTEND_BASE}/editor&mode=popup",
        expected=307,
        allow_redirects=False,
    )
    expect(results, "youtube auth start redirect", "accounts.google.com" in start_response.headers.get("location", ""))

    invalid_callback = request(
        "GET",
        f"{yt_base}/auth/callback?state=invalid-state&code=invalid-code",
        expected=500,
    )
    expect(results, "youtube auth callback invalid state", "Authorization failed" in invalid_callback.text, invalid_callback.text)

    tasks = json_request("GET", f"{yt_base}/tasks", expected=200)
    expect(results, "youtube tasks list", "tasks" in tasks)

    search = json_request(
        "POST",
        f"{yt_base}/search",
        expected=200,
        json={"query": "Rick Astley Never Gonna Give You Up", "max_results": 3},
    )
    expect(results, "youtube search", len(search["results"]) > 0, json.dumps(search["results"][:1]))

    download_task = json_request(
        "POST",
        f"{yt_base}/download",
        expected=200,
        json={"url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ", "format": "worst[ext=mp4]/worst"},
    )
    download_status = wait_for_task(yt_base, download_task["task_id"], timeout_seconds=240)
    expect(results, "youtube download task", download_status["status"] == "completed", json.dumps(download_status))
    downloaded_filename = download_status["result"]["filename"]
    cleanup["yt_downloads"].append(downloaded_filename)

    downloads = json_request("GET", f"{yt_base}/downloads", expected=200)
    expect(results, "youtube downloads list", any(item["filename"] == downloaded_filename for item in downloads["files"]), downloaded_filename)
    response = request("GET", f"{yt_base}/download/{downloaded_filename}", expected=200)
    expect(results, "youtube download file", response.headers.get("content-type", "").startswith("video/"), response.headers.get("content-type", ""))

    token_backup = YT_TOKEN_FILE.read_bytes() if YT_TOKEN_FILE.exists() else None
    quota_backup = YT_QUOTA_FILE.read_bytes() if YT_QUOTA_FILE.exists() else None
    try:
        logout = json_request("POST", f"{yt_base}/auth/logout", expected=200)
        expect(results, "youtube auth logout", logout["message"] == "Logged out successfully", json.dumps(logout))
        logged_out_status = json_request("GET", f"{yt_base}/auth/status", expected=200)
        expect(results, "youtube logged out status", logged_out_status["authenticated"] is False, json.dumps(logged_out_status))
    finally:
        restore_token(token_backup)
        restored_status = json_request("GET", f"{yt_base}/auth/status", expected=200)
        expect(results, "youtube auth restore token", restored_status["authenticated"] is True, json.dumps(restored_status))

    try:
        video_path = create_test_video()
        upload_title = f"api-smoke-upload-{now_slug()}"
        with video_path.open("rb") as f:
            upload_task = json_request(
                "POST",
                f"{yt_base}/upload",
                expected=200,
                files={"file": ("vp_smoke.mp4", f, "video/mp4")},
                data={
                    "title": upload_title,
                    "description": "smoke upload",
                    "tags": "smoke,upload",
                    "privacy_status": "private",
                },
            )
        upload_status = wait_for_task(yt_base, upload_task["task_id"], timeout_seconds=600)
        expect(results, "youtube upload", upload_status["status"] == "completed", json.dumps(upload_status))
        uploaded_video_id = upload_status["result"]["video_id"]
        cleanup["yt_videos"].append(uploaded_video_id)

        local_copy = YT_DOWNLOAD_DIR / f"local-upload-{now_slug()}.mp4"
        shutil.copy2(video_path, local_copy)
        cleanup["yt_downloads"].append(local_copy.name)
        local_upload_task = json_request(
            "POST",
            f"{yt_base}/upload/local",
            expected=200,
            json={
                "filename": local_copy.name,
                "title": f"api-smoke-local-upload-{now_slug()}",
                "description": "smoke local upload",
                "tags": "smoke,local-upload",
                "privacy_status": "private",
            },
        )
        local_upload_status = wait_for_task(yt_base, local_upload_task["task_id"], timeout_seconds=600)
        expect(results, "youtube upload local", local_upload_status["status"] == "completed", json.dumps(local_upload_status))
        cleanup["yt_videos"].append(local_upload_status["result"]["video_id"])
    finally:
        restore_quota_usage(quota_backup)


def cleanup_resources(cleanup: dict) -> None:
    print_step("cleaning up smoke resources")
    yt_base = cleanup.get("yt_base")

    for video_id in cleanup["yt_videos"]:
        delete_youtube_video(video_id)

    for filename in cleanup["yt_downloads"]:
        try:
            if yt_base:
                request("DELETE", f"{yt_base}/download/{filename}", expected=200)
        except Exception:
            pass

    for job_id in list(cleanup["jobs"]):
        try:
            job = json_request("GET", f"{API_BASE}/jobs/{job_id}", expected=200)
            if job["status"] not in TERMINAL_JOB_STATES:
                try:
                    request("POST", f"{API_BASE}/jobs/{job_id}/cancel", expected=200)
                    time.sleep(1)
                except Exception:
                    pass
            request("DELETE", f"{API_BASE}/jobs/{job_id}", expected=200)
        except Exception:
            pass

    for pipeline_id in list(cleanup["pipelines"]):
        try:
            request("DELETE", f"{API_BASE}/pipelines/{pipeline_id}", expected=200)
        except Exception:
            pass

    for asset_id in list(cleanup["assets"]):
        try:
            request("DELETE", f"{API_BASE}/assets/{asset_id}", expected=200)
        except Exception:
            pass


def main() -> int:
    results: list[dict] = []
    cleanup = {
        "assets": [],
        "pipelines": [],
        "jobs": [],
        "yt_downloads": [],
        "yt_videos": [],
    }

    try:
        run_backend_smoke(results, cleanup)
        run_youtube_smoke(results, cleanup)
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc), "results": results}, indent=2))
        cleanup_resources(cleanup)
        return 1

    cleanup_resources(cleanup)
    print(json.dumps({"ok": True, "results": results}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
