from __future__ import annotations

import os
from typing import Any

import pytest
import requests


STRICT = os.getenv("VP_GO_WRITE_STRICT", "").lower() in {"1", "true", "yes", "on"}
GO_API = os.getenv("VP_GO_API_URL") or os.getenv("VP_GO_API", "http://127.0.0.1:18081")


def first_wave_pipeline(name: str = "go-write-parity") -> dict[str, Any]:
    return {
        "name": name,
        "description": "go write parity",
        "is_template": False,
        "template_tags": [],
        "definition": {
            "nodes": [
                {
                    "id": "source",
                    "type": "source",
                    "position": {},
                    "data": {
                        "label": "Source",
                        "asset_id": "00000000-0000-0000-0000-000000000001",
                        "config": {},
                    },
                },
                {
                    "id": "export",
                    "type": "export",
                    "position": {},
                    "data": {"label": "Export", "config": {}},
                },
            ],
            "edges": [
                {
                    "id": "e1",
                    "source": "source",
                    "sourceHandle": "output",
                    "target": "export",
                    "targetHandle": "input",
                }
            ],
            "viewport": {},
        },
    }


@pytest.mark.skipif(not STRICT, reason="set VP_GO_WRITE_STRICT=1 for live Go write parity")
def test_go_pipeline_create_update_duplicate_delete() -> None:
    payload = first_wave_pipeline()
    created = requests.post(f"{GO_API}/api/v1/pipelines", json=payload, timeout=10)
    assert created.status_code in {200, 201}, created.text
    pipeline_id = created.json()["id"]
    duplicate_id: str | None = None
    try:
        updated = requests.put(
            f"{GO_API}/api/v1/pipelines/{pipeline_id}",
            json={**payload, "name": "go-write-parity-updated"},
            timeout=10,
        )
        assert updated.status_code == 200, updated.text
        assert updated.json()["name"] == "go-write-parity-updated"

        duplicate = requests.post(f"{GO_API}/api/v1/pipelines/{pipeline_id}/duplicate", timeout=10)
        assert duplicate.status_code in {200, 201}, duplicate.text
        duplicate_id = duplicate.json()["id"]
    finally:
        deleted = requests.delete(f"{GO_API}/api/v1/pipelines/{pipeline_id}", timeout=10)
        assert deleted.status_code in {200, 204}, deleted.text
        if duplicate_id:
            duplicate_deleted = requests.delete(f"{GO_API}/api/v1/pipelines/{duplicate_id}", timeout=10)
            assert duplicate_deleted.status_code in {200, 204}, duplicate_deleted.text


@pytest.mark.skipif(not STRICT, reason="set VP_GO_WRITE_STRICT=1 for live Go write parity")
def test_go_job_start_is_explicitly_python_owned_or_handed_off() -> None:
    response = requests.post(
        f"{GO_API}/api/v1/jobs",
        json={"pipeline_id": "00000000-0000-0000-0000-000000000000"},
        timeout=10,
    )
    assert response.status_code in {201, 202, 404, 501}
    if response.status_code == 501:
        assert "Python" in response.json()["detail"]
