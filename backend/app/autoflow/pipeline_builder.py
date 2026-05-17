from __future__ import annotations

from app.node_registry.registry import NodeTypeRegistry
from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowIntent, AutoFlowMetadata, WorkflowTemplate
from app.schemas.pipeline import PipelineDefinition, PipelineEdge, PipelineNode, PipelineNodeData


class PipelineBuilder:
    def build(
        self,
        template: WorkflowTemplate,
        intent: AutoFlowIntent,
        candidates: list[AutoFlowClipCandidate],
        metadata: AutoFlowMetadata,
    ) -> PipelineDefinition:
        if not candidates:
            raise ValueError("AutoFlow requires at least one candidate to build a pipeline")

        source_nodes: list[PipelineNode] = []
        trim_nodes: list[PipelineNode] = []
        edges: list[PipelineEdge] = []
        trimmed_node_ids: list[str] = []

        for index, candidate in enumerate(candidates, start=1):
            source_node = self._source_node(index, candidate, intent)
            trim_node = self._trim_node(index, candidate)
            source_nodes.append(source_node)
            trim_nodes.append(trim_node)
            source_port = "output"
            edges.append(
                PipelineEdge(
                    id=f"e-{source_node.id}-{trim_node.id}",
                    source=source_node.id,
                    target=trim_node.id,
                    sourceHandle=source_port,
                    targetHandle="input",
                )
            )
            trimmed_node_ids.append(trim_node.id)

        nodes: list[PipelineNode] = [*source_nodes, *trim_nodes]

        assembly_output = trimmed_node_ids[0]
        concat_count = 0
        while len(trimmed_node_ids) > 1:
            first = trimmed_node_ids.pop(0)
            second = trimmed_node_ids.pop(0)
            concat_count += 1
            concat_id = f"concat_timeline_{concat_count}"
            nodes.append(
                PipelineNode(
                    id=concat_id,
                    type="concat_timeline",
                    position=self._position(2 + concat_count, concat_count - 1),
                    data=PipelineNodeData(
                        label=f"Concat {concat_count}",
                        config={"output_format": "mp4", "transition": "none", "transition_duration": 0},
                    ),
                )
            )
            edges.extend(
                [
                    PipelineEdge(
                        id=f"e-{first}-{concat_id}",
                        source=first,
                        target=concat_id,
                        sourceHandle="output",
                        targetHandle="video_first",
                    ),
                    PipelineEdge(
                        id=f"e-{second}-{concat_id}",
                        source=second,
                        target=concat_id,
                        sourceHandle="output",
                        targetHandle="video_second",
                    ),
                ]
            )
            trimmed_node_ids.insert(0, concat_id)
            assembly_output = concat_id

        transcode = PipelineNode(
            id="transcode_1",
            type="transcode",
            position=self._position(4, 0),
            data=PipelineNodeData(
                label="Transcode",
                config={"format": "mp4", "video_codec": "libx264", "audio_codec": "aac", "crf": 23},
            ),
        )
        export = PipelineNode(
            id="export_1",
            type="export",
            position=self._position(5, 0),
            data=PipelineNodeData(
                label="Export Preview",
                config={"output_dir": "/tmp/vp_autoflow_exports", "filename": _safe_filename(metadata.selected_title)},
            ),
        )
        nodes.extend([transcode, export])
        edges.append(
            PipelineEdge(
                id=f"e-{assembly_output}-transcode_1",
                source=assembly_output,
                target="transcode_1",
                sourceHandle="output",
                targetHandle="input",
            )
        )
        edges.append(
            PipelineEdge(
                id="e-transcode_1-export_1",
                source="transcode_1",
                target="export_1",
                sourceHandle="output",
                targetHandle="input",
            )
        )

        if intent.publish_mode in {"private_upload", "unlisted_upload", "public_after_review"}:
            upload = self._upload_node(intent, metadata)
            nodes.append(upload)
            edges.append(
                PipelineEdge(
                    id="e-transcode_1-youtube_upload_1",
                    source="transcode_1",
                    target="youtube_upload_1",
                    sourceHandle="output",
                    targetHandle="input",
                )
            )

        return PipelineDefinition(nodes=nodes, edges=edges)

    def _source_node(
        self,
        index: int,
        candidate: AutoFlowClipCandidate,
        intent: AutoFlowIntent,
    ) -> PipelineNode:
        can_use_url = (
            candidate.url is not None
            and intent.source_policy in {"research_only", "remix_with_review", "public_domain_or_cc"}
        )
        if can_use_url:
            return PipelineNode(
                id=f"url_download_{index}",
                type="url_download",
                position=self._position(0, index - 1),
                data=PipelineNodeData(
                    label=f"Download {index}",
                    config={"url": candidate.url, "format": "best"},
                ),
            )
        asset_id = candidate.asset_id or candidate.id
        return PipelineNode(
            id=f"src_{index}",
            type="source",
            position=self._position(0, index - 1),
            data=PipelineNodeData(
                label=f"Source {index}",
                config={"asset_id": asset_id, "media_type": "video"},
                asset_id=asset_id,
            ),
        )

    def _trim_node(self, index: int, candidate: AutoFlowClipCandidate) -> PipelineNode:
        duration = 5.0
        if candidate.start_sec is not None and candidate.end_sec is not None:
            duration = max(0.5, candidate.end_sec - candidate.start_sec)
        return PipelineNode(
            id=f"trim_{index}",
            type="trim",
            position=self._position(1, index - 1),
            data=PipelineNodeData(
                label=f"Trim {index}",
                config={
                    "start_time": str(candidate.start_sec or 0),
                    "duration": str(int(duration)) if duration.is_integer() else f"{duration:.2f}",
                },
            ),
        )

    def _upload_node(self, intent: AutoFlowIntent, metadata: AutoFlowMetadata) -> PipelineNode:
        privacy = "private"
        if intent.publish_mode == "unlisted_upload":
            privacy = "unlisted"
        if intent.publish_mode == "public_after_review":
            privacy = "private"
        return PipelineNode(
            id="youtube_upload_1",
            type="youtube_upload",
            position=self._position(6, 0),
            data=PipelineNodeData(
                label="YouTube Upload",
                config={
                    "title": metadata.selected_title or "AutoFlow Preview",
                    "description": metadata.description,
                    "privacy": privacy,
                    "made_for_kids": "not_set",
                    "tags": ",".join(metadata.tags),
                },
            ),
        )

    def _position(self, stage_index: int, row_index: int) -> dict[str, float]:
        return {"x": stage_index * 260, "y": row_index * 140}


def _safe_filename(title: str | None) -> str:
    if not title:
        return "autoflow-preview.mp4"
    cleaned = "".join(ch for ch in title if ch.isalnum() or ch in {"-", "_"}).strip()
    return f"{cleaned[:48] or 'autoflow-preview'}.mp4"
