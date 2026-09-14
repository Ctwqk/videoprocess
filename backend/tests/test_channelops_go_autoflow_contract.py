from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest
from pydantic import ValidationError

from app.schemas.autoflow import AutoFlowRequest


def test_channelops_go_autoflow_request_fixture_matches_schema():
    root = Path(__file__).resolve().parents[2]
    fixture_path = root / "internal" / "channelops" / "testdata" / "autoflow_request.json"
    request = json.loads(fixture_path.read_text(encoding="utf-8"))

    parsed = AutoFlowRequest.model_validate(request)

    assert parsed.publish_mode != "preview_only"
    assert parsed.publish_mode == "unlisted_upload"
    assert request["planning_options"] == {
        "version": 1,
        "planning_mode": request["planning_mode"],
        "provider_config_id": request["provider_config_id"],
        "model": request["model"],
        "allow_experimental_graph_planning": request["allow_experimental_graph_planning"],
        "max_repair_attempts": request["max_repair_attempts"],
    }
    assert parsed.planning_options is not None
    assert parsed.planning_options.model_dump() == request["planning_options"]


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [
        ("version", 2),
        ("planning_mode", "future_mode"),
        ("provider_config_id", {"invalid": True}),
        ("model", 42),
        ("allow_experimental_graph_planning", "true"),
        ("max_repair_attempts", 9),
    ],
)
def test_channelops_versioned_planning_values_are_rejected_without_coercion(
    field,
    invalid_value,
):
    root = Path(__file__).resolve().parents[2]
    fixture_path = root / "internal" / "channelops" / "testdata" / "autoflow_request.json"
    request = deepcopy(json.loads(fixture_path.read_text(encoding="utf-8")))
    request["planning_options"][field] = invalid_value
    if field != "version":
        request[field] = invalid_value

    with pytest.raises(ValidationError):
        AutoFlowRequest.model_validate(request)
