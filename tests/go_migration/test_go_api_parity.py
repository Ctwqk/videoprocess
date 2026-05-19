import os

import httpx
import pytest


PYTHON_API = os.environ.get("VP_PYTHON_API", "http://127.0.0.1:18080")
GO_API = os.environ.get("VP_GO_API", "http://127.0.0.1:18081")
STRICT = os.environ.get("VP_GO_PARITY_STRICT", "").lower() in {"1", "true", "yes", "on"}


def get_json(base_url: str, path: str):
    try:
        response = httpx.get(f"{base_url}{path}", timeout=10)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        if STRICT:
            raise
        pytest.skip(f"{base_url} is not available for Go API parity smoke: {exc}")
    return response.json()


def test_health_parity():
    assert get_json(PYTHON_API, "/health") == get_json(GO_API, "/health")


def test_node_types_trim_exists_in_both_services():
    python_payload = get_json(PYTHON_API, "/api/v1/node-types/trim")
    go_payload = get_json(GO_API, "/api/v1/node-types/trim")

    assert python_payload["type_name"] == go_payload["type_name"]
    assert python_payload["worker_type"] == go_payload["worker_type"]
