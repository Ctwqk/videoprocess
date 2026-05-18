from __future__ import annotations

from app.node_registry.registry import NodeTypeRegistry
from worker.handlers import HANDLER_MAP


def test_autoflow_video_nodes_are_registered_with_handlers():
    registry = NodeTypeRegistry.get()

    for type_name in ("smart_trim", "concat_many", "montage_assembler", "vertical_crop", "title_overlay"):
        assert registry.get_type(type_name) is not None
        assert type_name in HANDLER_MAP


def test_smart_trim_node_contract_matches_storyboard_builder_needs():
    definition = NodeTypeRegistry.get().get_type("smart_trim")

    assert definition is not None
    assert definition.worker_type == "vision"
    assert [port.name for port in definition.inputs] == ["input"]
    assert [port.name for port in definition.outputs] == ["output"]
    params = {param.name: param for param in definition.params}
    assert params["prompt"].required is True
    assert params["mode"].options == ["auto", "best_clip", "all_matches_montage", "full_if_match", "no_full_video"]
    assert params["mode"].default == "auto"
    assert params["target_duration"].default == 0
    assert params["return_full_threshold"].default == 0.65
    assert params["no_match_policy"].default == "placeholder"


def test_montage_assembler_node_contract_is_fixed_for_autoflow():
    definition = NodeTypeRegistry.get().get_type("montage_assembler")

    assert definition is not None
    assert [port.name for port in definition.inputs] == [f"video_{index}" for index in range(1, 13)]
    assert definition.inputs[0].required is True
    assert definition.inputs[1].required is True
    assert all(port.required is False for port in definition.inputs[2:])
    assert [port.name for port in definition.outputs] == ["output"]
    assert {param.name for param in definition.params} >= {
        "style",
        "target_duration",
        "aspect_ratio",
        "beat_sync",
        "max_clip_duration",
        "min_clip_duration",
        "intro_hook",
        "width",
        "height",
    }


def test_concat_many_node_contract_exposes_aspect_ratio():
    definition = NodeTypeRegistry.get().get_type("concat_many")

    assert definition is not None
    params = {param.name: param for param in definition.params}
    assert params["aspect_ratio"].default == "9:16"
    assert params["aspect_ratio"].options == ["9:16", "16:9", "1:1", "auto"]


def test_concat_timeline_exposes_only_default_dynamic_inputs():
    definition = NodeTypeRegistry.get().get_type("concat_timeline")

    assert definition is not None
    assert [port.name for port in definition.inputs] == ["video_1", "video_2"]
    assert all(port.required for port in definition.inputs)
    params = {param.name: param for param in definition.params}
    assert params["input_count"].default == 2
    assert params["input_count"].max_value is None


def test_export_node_contract_exposes_quality_qa_params():
    definition = NodeTypeRegistry.get().get_type("export")

    assert definition is not None
    params = {param.name: param for param in definition.params}
    assert params["enable_quality_qa"].default is True
    assert params["quality_gate_mode"].default == "soft_repair_once"
    assert params["quality_gate_mode"].options == ["soft_repair_once"]
    assert params["vmaf_min_score"].default == 80
    assert params["loudnorm_target_i"].default == -16
    assert params["loudnorm_target_lra"].default == 11
    assert params["loudnorm_target_tp"].default == -1.5
