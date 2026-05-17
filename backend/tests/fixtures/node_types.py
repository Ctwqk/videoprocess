from __future__ import annotations

from app.node_registry.registry import NodeTypeRegistry


def registered_node_type_names() -> set[str]:
    registry = NodeTypeRegistry.get()
    return {node_type.type_name for node_type in registry.list_types()}


def require_node_types(*type_names: str) -> None:
    missing = sorted(set(type_names) - registered_node_type_names())
    if missing:
        raise AssertionError(f"Missing registered node types: {', '.join(missing)}")
