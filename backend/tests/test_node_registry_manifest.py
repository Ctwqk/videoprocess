from __future__ import annotations

import json
from pathlib import Path

from app.node_registry.builtin import BUILTIN_MODULES
from app.node_registry.registry import NodeTypeRegistry
from scripts.export_node_registry_manifest import (
    build_manifest,
    builtin_definition_modules,
    manifest_json,
    registered_builtin_type_names,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_GOLDEN = (
    REPO_ROOT / "backend/tests/golden/go_migration/node_registry_manifest.json"
)
GO_EMBEDDED_COPY = REPO_ROOT / "internal/pipeline/testdata/node_registry_manifest.json"


def test_manifest_contains_exactly_builtin_registry_node_types() -> None:
    manifest = build_manifest()
    registry_type_names = {
        definition.type_name for definition in NodeTypeRegistry.get().list_types()
    }
    builtin_type_names = {module.DEFINITION.type_name for module in BUILTIN_MODULES}

    assert registry_type_names == builtin_type_names
    assert {node["type_name"] for node in manifest["node_types"]} == builtin_type_names
    assert [node["type_name"] for node in manifest["node_types"]] == sorted(
        builtin_type_names
    )


def test_all_builtin_definition_modules_are_registered() -> None:
    definition_modules = builtin_definition_modules()

    assert registered_builtin_type_names() == set(definition_modules)
    assert "xiaohongshu_upload" in definition_modules


def test_trim_ports_and_worker_type_are_serialized() -> None:
    manifest = build_manifest()
    trim_node = next(
        node for node in manifest["node_types"] if node["type_name"] == "trim"
    )

    assert trim_node["worker_type"] == "ffmpeg_go"
    assert trim_node["inputs"] == [
        {
            "description": "Input video",
            "name": "input",
            "port_type": "video",
            "required": True,
        }
    ]
    assert trim_node["outputs"] == [
        {
            "description": "Trimmed video",
            "name": "output",
            "port_type": "video",
            "required": True,
        }
    ]


def test_backend_golden_equals_build_manifest() -> None:
    assert json.loads(BACKEND_GOLDEN.read_text()) == build_manifest()
    assert BACKEND_GOLDEN.read_text() == manifest_json(build_manifest())


def test_go_embedded_copy_equals_backend_golden() -> None:
    assert GO_EMBEDDED_COPY.read_text() == BACKEND_GOLDEN.read_text()
