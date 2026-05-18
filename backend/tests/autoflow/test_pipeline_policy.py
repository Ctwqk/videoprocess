from __future__ import annotations

from app.autoflow.pipeline_policy import validate_pipeline_policy
from app.schemas.autoflow import AutoFlowRequest
from app.schemas.pipeline import PipelineDefinition, PipelineEdge, PipelineNode, PipelineNodeData


def _node(node_id: str, node_type: str, config: dict | None = None) -> PipelineNode:
    return PipelineNode(
        id=node_id,
        type=node_type,
        position={"x": 0, "y": 0},
        data=PipelineNodeData(label=node_id, config=config or {}),
    )


def _edge(source: str, target: str, source_handle: str = "output", target_handle: str = "input") -> PipelineEdge:
    return PipelineEdge(
        id=f"e-{source}-{target}",
        source=source,
        target=target,
        sourceHandle=source_handle,
        targetHandle=target_handle,
    )


def _definition_with_upload(privacy: str = "public") -> PipelineDefinition:
    return PipelineDefinition(
        nodes=[
            _node("source_1", "source", {"asset_id": "asset-1", "media_type": "video"}),
            _node("transcode_1", "transcode", {"format": "mp4", "video_codec": "libx264", "audio_codec": "aac"}),
            _node("youtube_upload_1", "youtube_upload", {"title": "Demo", "privacy": privacy}),
        ],
        edges=[
            _edge("source_1", "transcode_1"),
            _edge("transcode_1", "youtube_upload_1"),
        ],
    )


def test_owned_only_policy_blocks_external_search_and_download_nodes():
    definition = PipelineDefinition(
        nodes=[
            _node("search_1", "youtube_search", {"query": "dog", "max_results": 3}),
            _node("download_1", "url_download", {"url": "https://example.test/video.mp4", "format": "best"}),
        ],
        edges=[],
    )

    result = validate_pipeline_policy(definition, AutoFlowRequest(prompt="make a dog video", source_policy="owned_only"))

    assert result.valid is False
    assert any(issue.code == "external_source_blocked" for issue in result.errors)
    assert result.definition == definition


def test_preview_only_policy_removes_upload_nodes_as_safe_repair():
    definition = _definition_with_upload("private")

    result = validate_pipeline_policy(definition, AutoFlowRequest(prompt="preview only", publish_mode="preview_only"))

    assert result.valid is True
    assert result.repairs == ["removed_upload:youtube_upload_1"]
    assert [node.type for node in result.definition.nodes] == ["source", "transcode"]
    assert all(edge.target != "youtube_upload_1" for edge in result.definition.edges)


def test_private_upload_policy_clamps_youtube_privacy_to_private():
    definition = _definition_with_upload("public")

    result = validate_pipeline_policy(definition, AutoFlowRequest(prompt="upload private", publish_mode="private_upload"))

    upload = next(node for node in result.definition.nodes if node.type == "youtube_upload")
    assert result.valid is True
    assert result.requires_review is True
    assert result.repairs == ["privacy:youtube_upload_1:private"]
    assert upload.data.config["privacy"] == "private"


def test_unlisted_upload_policy_clamps_public_privacy_to_unlisted():
    definition = _definition_with_upload("public")

    result = validate_pipeline_policy(definition, AutoFlowRequest(prompt="upload unlisted", publish_mode="unlisted_upload"))

    upload = next(node for node in result.definition.nodes if node.type == "youtube_upload")
    assert result.valid is True
    assert result.requires_review is True
    assert result.repairs == ["privacy:youtube_upload_1:unlisted"]
    assert upload.data.config["privacy"] == "unlisted"


def test_public_after_review_defaults_upload_privacy_to_private_and_requires_review():
    definition = _definition_with_upload("public")

    result = validate_pipeline_policy(
        definition,
        AutoFlowRequest(prompt="public after review", publish_mode="public_after_review"),
    )

    upload = next(node for node in result.definition.nodes if node.type == "youtube_upload")
    assert result.valid is True
    assert result.requires_review is True
    assert result.repairs == ["privacy:youtube_upload_1:private"]
    assert upload.data.config["privacy"] == "private"
    assert any(issue.code == "public_requires_approval" for issue in result.warnings)
