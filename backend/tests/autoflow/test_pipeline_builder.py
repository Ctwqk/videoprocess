from __future__ import annotations

from app.autoflow.metadata_generator import MetadataGenerator
from app.autoflow.pipeline_builder import PipelineBuilder
from app.autoflow.template_library import TemplateLibrary
from app.orchestrator.dag import validate_pipeline
from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowIntent


def candidate(index: int) -> AutoFlowClipCandidate:
    return AutoFlowClipCandidate(
        id=f"clip-{index}",
        title=f"素材 {index}",
        source_type="asset",
        asset_id=f"asset-{index}",
        start_sec=float(index),
        end_sec=float(index + 5),
    )


def test_builder_creates_valid_two_clip_asset_pipeline():
    intent = AutoFlowIntent(intent_type="animal_compilation", subject="小猫", duration_sec=10)
    candidates = [candidate(1), candidate(2)]
    template = TemplateLibrary().get_template("animal_compilation_short")
    metadata = MetadataGenerator().generate(intent, candidates)

    definition = PipelineBuilder().build(template, intent, candidates, metadata)
    validation = validate_pipeline(definition)

    assert validation.valid, [error.message for error in validation.errors]
    assert [node.id for node in definition.nodes] == [
        "source_1",
        "source_2",
        "trim_1",
        "trim_2",
        "vertical_crop_1",
        "vertical_crop_2",
        "montage_1",
        "title_overlay_1",
        "transcode_1",
        "export_1",
    ]
    assert definition.nodes[0].position == {"x": 0, "y": 0}
    assert definition.nodes[2].data.config["duration"] == "5"
    assert next(node for node in definition.nodes if node.id == "montage_1").type == "montage_assembler"
    assert next(node for node in definition.nodes if node.id == "title_overlay_1").data.config["text"] == metadata.selected_title
    assert definition.nodes[-1].type == "export"


def test_builder_uses_single_montage_node_for_three_clips():
    intent = AutoFlowIntent(intent_type="material_library_remix", subject="旅行素材", duration_sec=15)
    candidates = [candidate(1), candidate(2), candidate(3)]
    template = TemplateLibrary().get_template("material_library_remix")
    metadata = MetadataGenerator().generate(intent, candidates)

    builder = PipelineBuilder()
    first = builder.build(template, intent, candidates, metadata)
    second = builder.build(template, intent, candidates, metadata)

    assert first.model_dump() == second.model_dump()
    assert validate_pipeline(first).valid
    montage = next(node for node in first.nodes if node.id == "montage_1")
    assert montage.type == "montage_assembler"
    assert montage.data.config["target_duration"] == 15
    assert [edge.targetHandle for edge in first.edges if edge.target == "montage_1"] == ["video_1", "video_2", "video_3"]


def test_builder_uses_url_download_when_source_policy_allows_research_preview():
    intent = AutoFlowIntent(
        intent_type="animal_compilation",
        subject="小猫",
        source_policy="research_only",
        publish_mode="preview_only",
    )
    candidates = [
        AutoFlowClipCandidate(
            id="url-1",
            title="外部小猫素材",
            source_type="youtube",
            url="https://example.test/cat.mp4",
            start_sec=0,
            end_sec=4,
        ),
        candidate(2),
    ]
    template = TemplateLibrary().get_template("animal_compilation_short")
    metadata = MetadataGenerator().generate(intent, candidates)

    definition = PipelineBuilder().build(template, intent, candidates, metadata)

    assert validate_pipeline(definition).valid
    assert definition.nodes[0].type == "url_download"
    assert definition.nodes[0].data.config["url"] == "https://example.test/cat.mp4"
    assert "vertical_crop_1" in {node.id for node in definition.nodes}


def test_builder_adds_private_upload_node_when_publish_mode_requests_it():
    intent = AutoFlowIntent(
        intent_type="animal_compilation",
        subject="小猫",
        publish_mode="private_upload",
        target_platforms=["youtube"],
    )
    candidates = [candidate(1), candidate(2)]
    template = TemplateLibrary().get_template("animal_compilation_short")
    metadata = MetadataGenerator().generate(intent, candidates)

    definition = PipelineBuilder().build(template, intent, candidates, metadata)

    assert validate_pipeline(definition).valid
    upload = next(node for node in definition.nodes if node.id == "youtube_upload_1")
    assert upload.data.config["privacy"] == "private"
    assert upload.data.config["title"] == metadata.selected_title
