from __future__ import annotations
from app.node_registry.base import NodeTypeDefinition, PortType


class NodeTypeRegistry:
    _instance: NodeTypeRegistry | None = None

    def __init__(self) -> None:
        self._types: dict[str, NodeTypeDefinition] = {}

    @classmethod
    def get(cls) -> NodeTypeRegistry:
        if cls._instance is None:
            cls._instance = cls()
            cls._instance._load_builtins()
        return cls._instance

    def register(self, definition: NodeTypeDefinition) -> None:
        self._types[definition.type_name] = definition

    def get_type(self, type_name: str) -> NodeTypeDefinition | None:
        return self._types.get(type_name)

    def list_types(self) -> list[NodeTypeDefinition]:
        return list(self._types.values())

    def validate_edge(
        self,
        source_type: str,
        source_port: str,
        target_type: str,
        target_port: str,
    ) -> bool:
        src_def = self._types.get(source_type)
        tgt_def = self._types.get(target_type)
        if not src_def or not tgt_def:
            return False
        src_port_def = next((p for p in src_def.outputs if p.name == source_port), None)
        tgt_port_def = next((p for p in tgt_def.inputs if p.name == target_port), None)
        if not src_port_def or not tgt_port_def:
            return False
        if tgt_port_def.port_type == PortType.ANY_MEDIA:
            return True
        if src_port_def.port_type == PortType.ANY_MEDIA:
            return True
        return src_port_def.port_type == tgt_port_def.port_type

    def _load_builtins(self) -> None:
        from app.node_registry.builtin import BUILTIN_MODULES

        for mod in BUILTIN_MODULES:
            self.register(mod.DEFINITION)
