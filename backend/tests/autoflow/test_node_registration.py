from __future__ import annotations

from app.node_registry.registry import NodeTypeRegistry
from worker.handlers import HANDLER_MAP


def test_autoflow_video_nodes_are_registered_with_handlers():
    registry = NodeTypeRegistry.get()

    for type_name in ("concat_many", "vertical_crop", "title_overlay"):
        assert registry.get_type(type_name) is not None
        assert type_name in HANDLER_MAP
