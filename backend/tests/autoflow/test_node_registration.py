from __future__ import annotations

from app.node_registry.registry import NodeTypeRegistry
from worker.handlers import HANDLER_MAP


def test_autoflow_video_nodes_are_registered_with_handlers():
    registry = NodeTypeRegistry.get()

    for type_name in ("concat_many", "montage_assembler", "vertical_crop", "title_overlay"):
        assert registry.get_type(type_name) is not None
        assert type_name in HANDLER_MAP


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
