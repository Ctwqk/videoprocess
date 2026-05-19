"""Cutover parity smoke between the Python and Go HTTP APIs.

These tests run in two modes:

- ``STRICT`` (env ``VP_GO_PARITY_STRICT=1``): a missing or unhealthy service
  fails the test. Use this in CI before flipping any traffic.
- non-strict (default): missing services are reported as ``skip`` so the suite
  is safe to run locally without both binaries up.

The tests intentionally check the response *shape* the frontend consumes,
not the database contents. We compare key sets, response envelope, and
intersection of stable identifiers (e.g. node type names) so the assertions
stay stable across fixture refreshes.
"""

from __future__ import annotations

import os
from typing import Any

import httpx
import pytest


PYTHON_API = os.environ.get("VP_PYTHON_API", "http://127.0.0.1:18080")
GO_API = os.environ.get("VP_GO_API", "http://127.0.0.1:18081")
STRICT = os.environ.get("VP_GO_PARITY_STRICT", "").lower() in {"1", "true", "yes", "on"}


def get_json(base_url: str, path: str) -> Any:
    try:
        response = httpx.get(f"{base_url}{path}", timeout=10)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        if STRICT:
            raise
        pytest.skip(f"{base_url} is not available for Go API parity smoke: {exc}")
    return response.json()


def _assert_list_envelope(payload: Any, label: str) -> None:
    assert isinstance(payload, dict), f"{label}: response must be an object, got {type(payload).__name__}"
    assert "items" in payload, f"{label}: missing items key"
    assert "total" in payload, f"{label}: missing total key"
    assert isinstance(payload["items"], list), f"{label}: items must be an array"
    assert isinstance(payload["total"], int), f"{label}: total must be an int"


def test_health_parity():
    assert get_json(PYTHON_API, "/health") == get_json(GO_API, "/health")


def test_pipelines_list_envelope_parity():
    py = get_json(PYTHON_API, "/api/v1/pipelines?limit=1")
    go = get_json(GO_API, "/api/v1/pipelines?limit=1")
    _assert_list_envelope(py, "python /pipelines")
    _assert_list_envelope(go, "go /pipelines")


def test_templates_list_envelope_parity():
    py = get_json(PYTHON_API, "/api/v1/templates?limit=1")
    go = get_json(GO_API, "/api/v1/templates?limit=1")
    _assert_list_envelope(py, "python /templates")
    _assert_list_envelope(go, "go /templates")


def test_jobs_list_envelope_parity():
    py = get_json(PYTHON_API, "/api/v1/jobs?limit=1")
    go = get_json(GO_API, "/api/v1/jobs?limit=1")
    _assert_list_envelope(py, "python /jobs")
    _assert_list_envelope(go, "go /jobs")


def test_assets_list_envelope_parity():
    py = get_json(PYTHON_API, "/api/v1/assets?limit=1")
    go = get_json(GO_API, "/api/v1/assets?limit=1")
    _assert_list_envelope(py, "python /assets")
    _assert_list_envelope(go, "go /assets")


def test_schedule_status_parity():
    py = get_json(PYTHON_API, "/internal/schedule/video/status")
    go = get_json(GO_API, "/internal/schedule/video/status")
    assert isinstance(py, dict) and isinstance(go, dict)
    assert "state" in py and "state" in go


def test_node_types_trim_exists_in_both_services():
    python_payload = get_json(PYTHON_API, "/api/v1/node-types/trim")
    go_payload = get_json(GO_API, "/api/v1/node-types/trim")

    assert python_payload["type_name"] == go_payload["type_name"]
    assert python_payload["worker_type"] == go_payload["worker_type"]


def _node_type_names(base_url: str) -> set[str]:
    payload = get_json(base_url, "/api/v1/node-types")
    # Both Python and Go return either a bare list or a {items:[...]} envelope
    # for `/node-types`. Accept both so this stays resilient to a future
    # response-shape alignment.
    items = payload.get("items", payload) if isinstance(payload, dict) else payload
    return {item["type_name"] for item in items if isinstance(item, dict) and "type_name" in item}


def test_go_registry_does_not_silently_drop_python_node_types():
    """The Python registry is the source of truth.

    Until the Go registry has explicitly migrated every node type, the Go
    response is a strict subset of Python. Any extra Python type that exists
    in Python but is missing from Go is recorded as an xfail with the
    missing list, so CI surfaces the migration gap without blocking the
    sidecar smoke. Once the gap closes, flip this to a hard equality.
    """
    python_types = _node_type_names(PYTHON_API)
    go_types = _node_type_names(GO_API)
    missing = sorted(python_types - go_types)
    if missing:
        pytest.xfail(
            f"Go registry is missing {len(missing)} node types still present "
            f"in Python: {missing}. Migrate them before flipping frontend "
            f"traffic to api-go."
        )
    extra = sorted(go_types - python_types)
    assert not extra, f"Go registry has unexpected node types: {extra}"
