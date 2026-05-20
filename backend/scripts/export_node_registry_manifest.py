from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.node_registry.base import NodeTypeDefinition, ParamDefinition, PortDefinition
from app.node_registry.registry import NodeTypeRegistry


SCHEMA_VERSION = 1


def _serialize_port(port: PortDefinition) -> dict[str, Any]:
    return {
        "name": port.name,
        "port_type": port.port_type.value,
        "required": port.required,
        "description": port.description,
    }


def _serialize_param(param: ParamDefinition) -> dict[str, Any]:
    return {
        "name": param.name,
        "param_type": param.param_type,
        "default": param.default,
        "required": param.required,
        "description": param.description,
        "options": param.options,
        "min_value": param.min_value,
        "max_value": param.max_value,
    }


def _serialize_node_type(definition: NodeTypeDefinition) -> dict[str, Any]:
    return {
        "type_name": definition.type_name,
        "display_name": definition.display_name,
        "category": definition.category,
        "description": definition.description,
        "icon": definition.icon,
        "inputs": [_serialize_port(port) for port in definition.inputs],
        "outputs": [_serialize_port(port) for port in definition.outputs],
        "params": [_serialize_param(param) for param in definition.params],
        "worker_type": definition.worker_type,
    }


def build_manifest() -> dict[str, Any]:
    node_types = sorted(
        NodeTypeRegistry.get().list_types(),
        key=lambda definition: definition.type_name,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "node_types": [_serialize_node_type(definition) for definition in node_types],
    }


def manifest_json(manifest: dict[str, Any] | None = None) -> str:
    return json.dumps(
        build_manifest() if manifest is None else manifest,
        indent=2,
        sort_keys=True,
    ) + "\n"


def write_manifest(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(manifest_json())


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export builtin node registry definitions as stable JSON.",
    )
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="Output paths. Writes to stdout when omitted.",
    )
    args = parser.parse_args()

    if not args.paths:
        print(manifest_json(), end="")
        return

    for path in args.paths:
        write_manifest(path)


if __name__ == "__main__":
    main()
