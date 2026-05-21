from __future__ import annotations

from app.schemas.autoflow import AutoFlowRequest


def test_channelops_go_autoflow_request_fixture_matches_schema():
    request = {
        "prompt": "Make a short operational update",
        "target_platforms": ["youtube"],
        "source_platforms": ["bilibili"],
        "duration_sec": 45,
        "aspect_ratio": "9:16",
        "source_policy": "remix_with_review",
        "publish_mode": "unlisted_upload",
        "material_library_ids": ["library-1"],
        "source_strategy": "external_research",
        "planning_mode": "template",
        "constraints": {
            "lane_id": "lane-1",
            "lane_format_id": "format-1",
            "template_pool_json": ["channelops-live"],
            "tone": "dry",
        },
    }

    parsed = AutoFlowRequest.model_validate(request)

    assert parsed.publish_mode != "preview_only"
    assert parsed.publish_mode == "unlisted_upload"
