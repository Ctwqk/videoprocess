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
    assert trim.worker_type == "ffmpeg"
