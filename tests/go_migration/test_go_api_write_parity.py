from __future__ import annotations

import os
from pathlib import Path
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


@pytest.mark.skipif(not STRICT, reason="set VP_GO_WRITE_STRICT=1 for live Go write parity")
def test_go_asset_upload_download_delete(tmp_path: Path) -> None:
    source = tmp_path / "go-asset-write.txt"
    source.write_text("go asset write parity\n")
    with source.open("rb") as fh:
        uploaded = requests.post(
            f"{GO_API}/api/v1/assets/upload",
            files={"file": (source.name, fh, "text/plain")},
            timeout=20,
        )
    assert uploaded.status_code in {200, 201}, uploaded.text
    asset_id = uploaded.json()["id"]
    try:
        downloaded = requests.get(f"{GO_API}/api/v1/assets/{asset_id}/download", timeout=20)
        assert downloaded.status_code == 200, downloaded.text
        assert downloaded.content == b"go asset write parity\n"
    finally:
        deleted = requests.delete(f"{GO_API}/api/v1/assets/{asset_id}", timeout=20)
        assert deleted.status_code in {200, 204}, deleted.text


@pytest.mark.skipif(not STRICT, reason="set VP_GO_WRITE_STRICT=1 for live Go write parity")
def test_schedule_open_drain_close_round_trip() -> None:
    for action in ["open", "drain", "close", "open"]:
        response = requests.post(f"{GO_API}/internal/schedule/video/{action}", timeout=10)
        assert response.status_code == 200, response.text
        assert response.json()["state"] in {"OPEN", "DRAINING", "CLOSED"}


@pytest.mark.skipif(not STRICT, reason="set VP_GO_WRITE_STRICT=1 for live Go write parity")
def test_artifact_cleanup_is_private_operation() -> None:
    response = requests.delete(f"{GO_API}/api/v1/artifacts/cleanup", timeout=10)
    assert response.status_code in {200, 204}, response.text
