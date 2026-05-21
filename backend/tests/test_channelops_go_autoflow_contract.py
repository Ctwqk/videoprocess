from __future__ import annotations

import json
from pathlib import Path

from app.schemas.autoflow import AutoFlowRequest


def test_channelops_go_autoflow_request_fixture_matches_schema():
    root = Path(__file__).resolve().parents[2]
    fixture_path = root / "internal" / "channelops" / "testdata" / "autoflow_request.json"
    request = json.loads(fixture_path.read_text(encoding="utf-8"))

    parsed = AutoFlowRequest.model_validate(request)

    assert parsed.publish_mode != "preview_only"
    assert parsed.publish_mode == "unlisted_upload"
