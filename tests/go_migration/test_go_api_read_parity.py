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
        pytest.skip(f"{base_url} unavailable for Go parity: {exc}")
    return response.json()


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


def test_go_readyz_reports_dependencies() -> None:
    payload = get_json(GO_API, "/readyz")
    assert payload["status"] in {"ready", "not_ready"}
    assert "postgres" in payload
