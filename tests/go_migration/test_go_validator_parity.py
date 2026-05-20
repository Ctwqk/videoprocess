"""Strict live parity for the Go pipeline validator endpoint."""

from __future__ import annotations

import os
from typing import Any

import httpx
import pytest


PYTHON_API = os.environ.get("VP_PYTHON_API", "http://127.0.0.1:18080")
GO_API = os.environ.get("VP_GO_API", "http://127.0.0.1:18081")
STRICT = os.environ.get("VP_GO_PARITY_STRICT", "").lower() in {"1", "true", "yes", "on"}


def post_json(base_url: str, path: str, payload: dict[str, Any]) -> Any:
    try:
        response = httpx.post(f"{base_url}{path}", json=payload, timeout=10)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        if STRICT:
            raise
        pytest.skip(f"{base_url} unavailable for Go validator parity: {exc}")
    return response.json()


def first_wave_ffmpeg_graph() -> dict[str, Any]:
    return {
        "nodes": [
            {
                "id": "source_1",
                "type": "source",
                "position": {},
                "data": {
                    "label": "Source",
                    "asset_id": "00000000-0000-0000-0000-000000000001",
                    "config": {"media_type": "video"},
                },
            },
            {
                "id": "crop_1",
                "type": "vertical_crop",
                "position": {},
                "data": {"label": "Vertical Crop", "config": {}},
            },
            {
                "id": "title_1",
                "type": "title_overlay",
                "position": {},
                "data": {"label": "Title Overlay", "config": {}},
            },
            {
                "id": "export_1",
                "type": "export",
                "position": {},
                "data": {"label": "Export", "config": {}},
            },
        ],
        "edges": [
            {
                "id": "edge_1",
                "source": "source_1",
                "target": "crop_1",
                "sourceHandle": "output",
                "targetHandle": "input",
            },
            {
                "id": "edge_2",
                "source": "crop_1",
                "target": "title_1",
                "sourceHandle": "output",
                "targetHandle": "input",
            },
            {
                "id": "edge_3",
                "source": "title_1",
                "target": "export_1",
                "sourceHandle": "output",
                "targetHandle": "input",
            },
        ],
        "viewport": {"x": 0, "y": 0, "zoom": 1},
    }


def unsupported_graph() -> dict[str, Any]:
    return {
        "nodes": [
            {
                "id": "search_1",
                "type": "youtube_search",
                "position": {},
                "data": {"label": "YouTube Search", "config": {}},
            }
        ],
        "edges": [],
        "viewport": {"x": 0, "y": 0, "zoom": 1},
    }


def test_go_validator_matches_python_for_first_wave_ffmpeg_graph() -> None:
    if not STRICT:
        pytest.skip("set VP_GO_PARITY_STRICT=1 with matching Python and Go services")

    python_result = post_json(PYTHON_API, "/api/v1/pipelines/validate", first_wave_ffmpeg_graph())
    go_result = post_json(GO_API, "/api/v1/pipelines/validate", first_wave_ffmpeg_graph())

    assert go_result == python_result


def test_go_validator_explicitly_refuses_python_owned_graph() -> None:
    if not STRICT:
        pytest.skip("set VP_GO_PARITY_STRICT=1 with matching Go service")

    go_result = post_json(GO_API, "/api/v1/pipelines/validate", unsupported_graph())

    assert go_result == {
        "valid": False,
        "errors": [
            {
                "type": "unsupported_go_validation",
                "message": "Go validator does not own validation for node type 'youtube_search'; route this graph to Python",
                "node_id": "search_1",
                "edge_id": None,
                "nodes": None,
                "source_port": None,
                "target_port": None,
                "param_name": None,
            }
        ],
        "warnings": [],
    }
