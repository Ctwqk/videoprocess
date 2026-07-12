from __future__ import annotations

from app.autoflow.capability_manifest import get_capability_manifest


def _node_by_type():
    manifest = get_capability_manifest()
    return {node.type_name: node for node in manifest.nodes}


def test_manifest_includes_core_autoflow_nodes_with_tags():
    nodes = _node_by_type()

    for type_name in ("source", "trim", "url_download", "material_search", "youtube_upload"):
        assert type_name in nodes
        assert nodes[type_name].autoflow_tags
        assert nodes[type_name].suitable_for

    assert "source" in nodes["source"].autoflow_tags
    assert {"clip", "duration", "transform"}.issubset(set(nodes["trim"].autoflow_tags))
    assert {"search", "planner", "clip_selection"}.issubset(set(nodes["material_search"].autoflow_tags))
    assert "publish" in nodes["youtube_upload"].autoflow_tags


def test_manifest_preserves_registry_ports_params_and_worker_type():
    nodes = _node_by_type()

    trim = nodes["trim"]
    assert trim.category == "transform"
    assert trim.inputs[0].name == "input"
    assert trim.outputs[0].name == "output"
    assert {param.name for param in trim.params} >= {"start_time", "duration"}
    assert trim.worker_type == "ffmpeg_go"


def test_manifest_exposes_supported_source_platforms():
    manifest = get_capability_manifest()

    assert manifest.source_platforms == ["youtube", "bilibili", "x", "xiaohongshu"]


def test_manifest_exposes_supported_target_platforms_without_bilibili():
    manifest = get_capability_manifest()

    assert manifest.target_platforms == ["youtube", "youtube_shorts", "x", "xiaohongshu"]
    assert "bilibili" not in manifest.target_platforms


def test_manifest_exposes_dynamic_video_inputs_for_timeline_concat_nodes():
    nodes = _node_by_type()

    concat_many = nodes["concat_many"]
    assert concat_many.dynamic_inputs
    assert concat_many.dynamic_inputs[0].pattern == "video_{n}"
    assert concat_many.dynamic_inputs[0].port_type == "video"
    assert concat_many.dynamic_inputs[0].min_count == 2
    assert concat_many.dynamic_inputs[0].max_count == 64
    assert concat_many.dynamic_inputs[0].ordered is True


def test_manifest_exposes_policy_and_execution_contracts_for_upload_nodes():
    nodes = _node_by_type()

    upload = nodes["youtube_upload"]
    assert "external_platform_write" in upload.execution.effects
    assert upload.worker_type == "youtube_publisher"
    assert upload.execution.worker_type == "youtube_publisher"
    assert upload.policy.requires_review is True
    assert upload.policy.default_privacy == "private"
    assert upload.policy.allowed_privacy == ["private", "unlisted"]


def test_manifest_exposes_planner_hints_for_vertical_timeline():
    nodes = _node_by_type()

    vertical_timeline = nodes["concat_vertical_timeline"]
    assert "vertical_split" in vertical_timeline.planner_hints.tags
    assert "smart_trim" in vertical_timeline.planner_hints.common_upstream
    assert "transcode" in vertical_timeline.planner_hints.common_downstream
    assert any("top" in text and "bottom" in text for text in vertical_timeline.planner_hints.use_when)
