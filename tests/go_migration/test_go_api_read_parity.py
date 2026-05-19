from __future__ import annotations

import os
from typing import Any

import httpx
import pytest


PYTHON_API = os.environ.get("VP_PYTHON_API", "http://127.0.0.1:18080")
GO_API = os.environ.get("VP_GO_API", "http://127.0.0.1:18081")
STRICT = os.environ.get("VP_GO_PARITY_STRICT", "").lower() in {"1", "true", "yes", "on"}


def request_json(base_url: str, path: str) -> tuple[int, Any]:
    try:
        response = httpx.get(f"{base_url}{path}", timeout=10)
    except httpx.HTTPError as exc:
        if STRICT:
            raise
        pytest.skip(f"{base_url} unavailable for Go parity: {exc}")
    try:
        payload = response.json()
    except ValueError:
        payload = response.text
    return response.status_code, payload


def get_json(base_url: str, path: str) -> Any:
    status, payload = request_json(base_url, path)
    if status >= 400:
        raise AssertionError(f"{base_url}{path} returned {status}: {payload}")
    return payload


def assert_page_shape(payload: Any) -> None:
    assert isinstance(payload, dict)
    assert isinstance(payload.get("items"), list)
    assert isinstance(payload.get("total"), int)


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/pipelines?skip=0&limit=50",
        "/api/v1/templates?skip=0&limit=50",
        "/api/v1/assets?skip=0&limit=50",
        "/api/v1/jobs?skip=0&limit=50",
    ],
)
def test_read_page_shape_matches_python_contract(path: str) -> None:
    assert_page_shape(get_json(PYTHON_API, path))
    assert_page_shape(get_json(GO_API, path))


@pytest.mark.parametrize(
    ("list_path", "id_key", "detail_path"),
    [
        ("/api/v1/pipelines?skip=0&limit=1", "id", "/api/v1/pipelines/{id}"),
        ("/api/v1/assets?skip=0&limit=1", "id", "/api/v1/assets/{id}"),
        ("/api/v1/jobs?skip=0&limit=1", "id", "/api/v1/jobs/{id}"),
    ],
)
def test_detail_shape_matches_python_for_live_records(list_path: str, id_key: str, detail_path: str) -> None:
    py_page = get_json(PYTHON_API, list_path)
    go_page = get_json(GO_API, list_path)
    assert_page_shape(py_page)
    assert_page_shape(go_page)
    if not py_page["items"]:
        pytest.skip(f"no live rows for {list_path}")
    record_id = py_page["items"][0][id_key]
    py_detail = get_json(PYTHON_API, detail_path.format(id=record_id))
    go_detail = get_json(GO_API, detail_path.format(id=record_id))
    assert set(go_detail.keys()) == set(py_detail.keys())


def test_artifact_detail_shape_matches_python_when_job_has_output() -> None:
    jobs = get_json(PYTHON_API, "/api/v1/jobs?skip=0&limit=20")
    assert_page_shape(jobs)
    artifact_id = None
    for job in jobs["items"]:
        detail = get_json(PYTHON_API, f"/api/v1/jobs/{job['id']}")
        for node in detail.get("node_executions", []):
            if node.get("output_artifact_id"):
                artifact_id = node["output_artifact_id"]
                break
        if artifact_id:
            break
    if artifact_id is None:
        pytest.skip("no live output artifact available for artifact detail parity")

    py_detail = get_json(PYTHON_API, f"/api/v1/artifacts/{artifact_id}")
    go_detail = get_json(GO_API, f"/api/v1/artifacts/{artifact_id}")
    assert set(go_detail.keys()) == set(py_detail.keys())


def test_unknown_detail_ids_match_python_status_and_error_shape() -> None:
    missing = "00000000-0000-0000-0000-000000000000"
    for path in [
        f"/api/v1/pipelines/{missing}",
        f"/api/v1/assets/{missing}",
        f"/api/v1/artifacts/{missing}",
        f"/api/v1/jobs/{missing}",
    ]:
        py_status, py_payload = request_json(PYTHON_API, path)
        go_status, go_payload = request_json(GO_API, path)
        assert go_status == py_status
        assert set(go_payload.keys()) == set(py_payload.keys()) == {"detail"}


def test_go_schedule_status_is_not_fixed_fake_open() -> None:
    py_status, py_payload = request_json(PYTHON_API, "/internal/schedule/video/status")
    go_status, go_payload = request_json(GO_API, "/internal/schedule/video/status")
    assert go_status == py_status
    assert set(go_payload.keys()) == set(py_payload.keys())
    assert go_payload["state"] == py_payload["state"]


def test_go_readyz_reports_dependencies() -> None:
    payload = get_json(GO_API, "/readyz")
    assert payload["status"] in {"ready", "not_ready"}
    assert "postgres" in payload
