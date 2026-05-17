from __future__ import annotations

import pytest

from app.autoflow.capability_manifest import get_capability_manifest
from app.autoflow.template_library import TemplateLibrary
from app.schemas.autoflow import AutoFlowIntent


def test_template_library_loads_three_builtin_templates():
    library = TemplateLibrary()
    templates = library.list_templates()

    assert {template.id for template in templates} == {
        "animal_compilation_short",
        "hot_topic_explainer_short",
        "material_library_remix",
    }
    assert all(template.node_blueprint for template in templates)
    assert all(template.edge_blueprint for template in templates)


def test_template_required_capabilities_exist_in_manifest():
    manifest_types = {node.type_name for node in get_capability_manifest().nodes}
    library = TemplateLibrary()

    for template in library.list_templates():
        missing = set(template.required_capabilities) - manifest_types
        assert missing == set()


def test_animal_template_prefers_montage_assembly_blueprint():
    template = TemplateLibrary().get_template("animal_compilation_short")

    assert "montage_assembler" in template.required_capabilities
    assert any(node["role"] == "assembly" and node["type"] == "montage_assembler|concat_many" for node in template.node_blueprint)


def test_template_selection_matches_intent_classes():
    library = TemplateLibrary()

    assert library.select_template(
        AutoFlowIntent(intent_type="animal_compilation", subject="小猫")
    ).id == "animal_compilation_short"
    assert library.select_template(
        AutoFlowIntent(intent_type="hot_topic_explainer", subject="AI 工具")
    ).id == "hot_topic_explainer_short"
    assert library.select_template(
        AutoFlowIntent(intent_type="material_library_remix", subject="旅行素材")
    ).id == "material_library_remix"


def test_unknown_template_raises_clear_error():
    library = TemplateLibrary()

    with pytest.raises(KeyError, match="Unknown AutoFlow template"):
        library.get_template("missing")
