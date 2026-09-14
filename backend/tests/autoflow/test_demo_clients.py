from __future__ import annotations

import json
import subprocess
import sys
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = (
    ("autoflow_demo_cat_compilation.py", "animal_compilation", "animal_compilation_short"),
    ("autoflow_demo_material_remix.py", "material_library_remix", "material_library_remix"),
)
LIBRARY_IDS = (
    "00000000-0000-0000-0000-000000000101",
    "00000000-0000-0000-0000-000000000102",
)


@contextmanager
def _plan_server(response_payload):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers["Content-Length"])
            requests.append((self.path, json.loads(self.rfile.read(length))))
            body = json.dumps(response_payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def _run(script_name, base_url=None, *extra_args):
    command = [sys.executable, str(REPO_ROOT / "scripts" / script_name)]
    if base_url is not None:
        command.extend(["--base-url", base_url])
    command.extend(extra_args)
    return subprocess.run(command, capture_output=True, text=True, check=False, timeout=10)


def _success_plan(intent_type, template_id):
    return {
        "plan_id": "plan-1",
        "status": "drafted",
        "intent": {"intent_type": intent_type},
        "template_id": template_id,
        "validation": {"valid": True},
        "rights": {"status": "allowed"},
        "needs_review": False,
        "candidates": [{"id": "candidate-1"}],
        "pipeline_definition": {"nodes": [{"type": "source"}]},
    }


@pytest.mark.parametrize(("script_name", "intent_type", "template_id"), SCRIPTS)
def test_demo_cli_posts_explicit_real_library_ids(script_name, intent_type, template_id):
    with _plan_server(_success_plan(intent_type, template_id)) as (base_url, requests):
        result = _run(
            script_name,
            base_url,
            "--material-library-id",
            LIBRARY_IDS[0],
            "--material-library-id",
            LIBRARY_IDS[1],
        )

    assert result.returncode == 0, result.stderr
    assert len(requests) == 1
    path, payload = requests[0]
    assert path == "/api/v1/autoflow/plan"
    assert payload["material_library_ids"] == list(LIBRARY_IDS)
    assert payload["planning_mode"] == "template"
    assert payload["prompt"]
    if intent_type == "animal_compilation":
        assert payload["target_platforms"] == ["youtube_shorts"]


@pytest.mark.parametrize(("script_name", "_intent_type", "_template_id"), SCRIPTS)
def test_demo_cli_requires_material_library_id(script_name, _intent_type, _template_id):
    result = _run(script_name)

    assert result.returncode == 2
    assert "--material-library-id" in result.stderr


@pytest.mark.parametrize(("script_name", "_intent_type", "_template_id"), SCRIPTS)
def test_demo_cli_rejects_non_uuid_material_library_id(script_name, _intent_type, _template_id):
    result = _run(script_name, None, "--material-library-id", "travel-library")

    assert result.returncode == 2
    assert "invalid material library UUID" in result.stderr


@pytest.mark.parametrize(("script_name", "intent_type", "_template_id"), SCRIPTS)
def test_demo_cli_reports_blocked_materials_before_template_mismatch(
    script_name,
    intent_type,
    _template_id,
):
    blocked_plan = {
        "status": "blocked",
        "intent": {"intent_type": intent_type},
        "template_id": "blocked-placeholder",
        "validation": {
            "valid": False,
            "material_status": "no_material",
            "errors": [{"type": "no_material", "message": "no_material"}],
        },
        "rights": {"status": "blocked", "reasons": ["no_material"]},
        "warnings": ["no_material"],
    }
    with _plan_server(blocked_plan) as (base_url, _requests):
        result = _run(script_name, base_url, "--material-library-id", LIBRARY_IDS[0])

    assert result.returncode == 1
    assert "AutoFlow planning blocked" in result.stderr
    assert "no_material" in result.stderr
    assert "Unexpected template_id" not in result.stderr
