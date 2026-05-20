"""Strict live parity for the Go node registry endpoint."""

from __future__ import annotations

import os
from typing import Any

import httpx
import pytest


PYTHON_API = os.environ.get("VP_PYTHON_API", "http://127.0.0.1:18080")
GO_API = os.environ.get("VP_GO_API", "http://127.0.0.1:18081")
STRICT = os.environ.get("VP_GO_PARITY_STRICT", "").lower() in {"1", "true", "yes", "on"}
COMPARE_KEYS = (
    "type_name",
    "display_name",
    "category",
    "worker_type",
    "inputs",
    "outputs",
)


def get_json(base_url: str, path: str) -> Any:
    try:
        response = httpx.get(f"{base_url}{path}", timeout=10)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        if STRICT:
            raise
        pytest.skip(f"{base_url} unavailable for Go registry parity: {exc}")
    return response.json()


def normalize_node_types(payload: Any, label: str) -> dict[str, dict[str, Any]]:
    items = payload.get("items", payload) if isinstance(payload, dict) else payload
    assert isinstance(items, list), f"{label}: expected list or items list"
    normalized: dict[str, dict[str, Any]] = {}
    for item in items:
        assert isinstance(item, dict), f"{label}: node type item must be object"
        type_name = item.get("type_name")
        assert isinstance(type_name, str), f"{label}: node type missing type_name"
        normalized[type_name] = {key: item.get(key) for key in COMPARE_KEYS}
    return normalized


def test_go_node_types_match_python_registry_contract() -> None:
    if not STRICT:
        pytest.skip("set VP_GO_PARITY_STRICT=1 with matching Python and Go services")

    python_types = normalize_node_types(get_json(PYTHON_API, "/api/v1/node-types"), "python")
    go_types = normalize_node_types(get_json(GO_API, "/api/v1/node-types"), "go")

    assert go_types == python_types
