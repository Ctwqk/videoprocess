from __future__ import annotations

from app.node_registry.registry import NodeTypeRegistry
from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowIntent, AutoFlowMetadata, StoryboardPlan, WorkflowTemplate
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

        nodes: list[PipelineNode] = []
        edges: list[PipelineEdge] = []
        source_nodes: list[PipelineNode] = []
        trim_nodes: list[PipelineNode] = []
        vertical_nodes: list[PipelineNode] = []
        assembly_inputs: list[str] = []
        needs_vertical = _needs_vertical_crop(intent, template)

        for index, candidate in enumerate(candidates, start=1):
            source_node = self._source_node(index, candidate, intent)
            trim_node = self._trim_node(index, candidate)
            source_nodes.append(source_node)
            trim_nodes.append(trim_node)
            edges.append(
                PipelineEdge(
                    id=f"e-{source_node.id}-{trim_node.id}",
                    source=source_node.id,
                    target=trim_node.id,
                    sourceHandle="output",
                    targetHandle="input",
                )
            )

            current_output = trim_node.id
            if needs_vertical:
                vertical_node = self._vertical_crop_node(index, intent)
                vertical_nodes.append(vertical_node)
                edges.append(
                    PipelineEdge(
                        id=f"e-{trim_node.id}-{vertical_node.id}",
                        source=trim_node.id,
                        target=vertical_node.id,
                        sourceHandle="output",
                        targetHandle="input",
                    )
                )
                current_output = vertical_node.id
            assembly_inputs.append(current_output)

        nodes.extend([*source_nodes, *trim_nodes, *vertical_nodes])
        target_duration = _target_duration(intent, template)
        width, height = _target_dimensions(intent.aspect_ratio)

        if len(assembly_inputs) >= 2:
            assembly = self._assembly_node(intent, target_duration, width, height, len(assembly_inputs), metadata)
            nodes.append(assembly)
            for index, input_node_id in enumerate(assembly_inputs, start=1):
                edges.append(
                    PipelineEdge(
                        id=f"e-{input_node_id}-{assembly.id}",
                        source=input_node_id,
                        target=assembly.id,
                        sourceHandle="output",
                        targetHandle=f"video_{index}",
                    )
                )
            assembly_output = assembly.id
        else:
            assembly_output = assembly_inputs[0]

        title = self._title_node(metadata, target_duration)
        transcode = PipelineNode(
            id="transcode_1",
            type="transcode",
            position=self._position(5, 0),
            data=PipelineNodeData(
                label="Transcode",
                config={"format": "mp4", "video_codec": "libx264", "audio_codec": "aac", "crf": 20},
            ),
        )
        export = PipelineNode(
            id="export_1",
            type="export",
            position=self._position(6, 0),
            data=PipelineNodeData(
                label="Export Preview",
                config={"output_dir": "/tmp/vp_autoflow_exports", "filename": _safe_filename(metadata.selected_title)},
            ),
        )
        nodes.extend([title, transcode, export])
        edges.extend(
            [
                PipelineEdge(
                    id=f"e-{assembly_output}-title_overlay_1",
                    source=assembly_output,
                    target="title_overlay_1",
                    sourceHandle="output",
                    targetHandle="input",
                ),
                PipelineEdge(
                    id="e-title_overlay_1-transcode_1",
                    source="title_overlay_1",
                    target="transcode_1",
                    sourceHandle="output",
                    targetHandle="input",
                ),
                PipelineEdge(
                    id="e-transcode_1-export_1",
                    source="transcode_1",
                    target="export_1",
                    sourceHandle="output",
                    targetHandle="input",
                ),
            ]
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

    def build_storyboard_input_video(
        self,
        storyboard: StoryboardPlan,
        *,
        input_asset_id: str,
        metadata: AutoFlowMetadata | None = None,
    ) -> PipelineDefinition:
        nodes: list[PipelineNode] = [
            PipelineNode(
                id="source_1",
                type="source",
                position=self._position(0, 0),
                data=PipelineNodeData(
                    label="Storyboard Source",
                    config={"asset_id": input_asset_id, "media_type": "video"},
                    asset_id=input_asset_id,
                ),
            )
        ]
        edges: list[PipelineEdge] = []
        trim_node_ids: list[str] = []

        for index, shot in enumerate(storyboard.shots, start=1):
            node_id = f"smart_trim_{index}"
            trim_node_ids.append(node_id)
            nodes.append(
                PipelineNode(
                    id=node_id,
                    type="smart_trim",
                    position=self._position(1, index - 1),
                    data=PipelineNodeData(
                        label=f"Smart Trim {shot.id}",
                        config={
                            "prompt": shot.search_query,
                            "negative_prompt": ", ".join([*shot.negative_queries, *shot.must_not_have]),
                            "mode": "best_clip",
                            "target_duration": shot.target_duration,
                            "min_clip_duration": shot.min_duration,
                            "max_clip_duration": shot.max_duration,
                            "max_clips": 1,
                            "sample_fps": 1,
                            "match_threshold": 0.35,
                            "return_full_threshold": 0.65,
                            "padding_before": 0.5,
                            "padding_after": 0.5,
                            "merge_gap": 1.0,
                            "use_visual": True,
                            "use_asr": True,
                            "use_vlm_verify": False,
                            "language": "zh",
                            "output_format": "mp4",
                            "no_match_policy": "placeholder",
                            "storyboard_shot_id": shot.id,
                        },
                    ),
                )
            )
            edges.append(
                PipelineEdge(
                    id=f"e-source_1-{node_id}",
                    source="source_1",
                    target=node_id,
                    sourceHandle="output",
                    targetHandle="input",
                )
            )

        assembly_output = self._append_storyboard_assembly(
            nodes,
            edges,
            trim_node_ids,
            storyboard=storyboard,
            metadata=metadata,
        )
        self._append_storyboard_output(nodes, edges, assembly_output, storyboard=storyboard, metadata=metadata)
        return PipelineDefinition(nodes=nodes, edges=edges)

    def build_storyboard_material_library(
        self,
        storyboard: StoryboardPlan,
        *,
        metadata: AutoFlowMetadata | None = None,
    ) -> PipelineDefinition:
        nodes: list[PipelineNode] = []
        edges: list[PipelineEdge] = []
        source_node_ids: list[str] = []

        matched_shots = [shot for shot in storyboard.shots if shot.match_status == "matched" and shot.matched_asset_id]
        for index, shot in enumerate(matched_shots, start=1):
            node_id = f"source_{index}"
            source_node_ids.append(node_id)
            nodes.append(
                PipelineNode(
                    id=node_id,
                    type="source",
                    position=self._position(0, index - 1),
                    data=PipelineNodeData(
                        label=f"Matched {shot.id}",
                        config={"asset_id": shot.matched_asset_id, "media_type": "video"},
                        asset_id=shot.matched_asset_id,
                    ),
                )
            )

        if not source_node_ids:
            return PipelineDefinition(nodes=[], edges=[])

        assembly_output = self._append_storyboard_assembly(
            nodes,
            edges,
            source_node_ids,
            storyboard=storyboard,
            metadata=metadata,
        )
        self._append_storyboard_output(nodes, edges, assembly_output, storyboard=storyboard, metadata=metadata)
        return PipelineDefinition(nodes=nodes, edges=edges)

    def _append_storyboard_assembly(
        self,
        nodes: list[PipelineNode],
        edges: list[PipelineEdge],
        input_node_ids: list[str],
        *,
        storyboard: StoryboardPlan,
        metadata: AutoFlowMetadata | None,
    ) -> str:
        if len(input_node_ids) < 2:
            return input_node_ids[0]

        width, height = _target_dimensions(storyboard.aspect_ratio)
        assembly = PipelineNode(
            id="concat_timeline_1",
            type="concat_timeline",
            position=self._position(3, 0),
            data=PipelineNodeData(
                label="Storyboard Assembly",
                config={
                    "input_count": len(input_node_ids),
                    "output_format": "mp4",
                    "transition": "none",
                    "transition_duration": 0,
                    "target_duration": storyboard.total_duration,
                    "normalize_resolution": True,
                    "width": width,
                    "height": height,
                },
            ),
        )
        nodes.append(assembly)
        for index, source_id in enumerate(input_node_ids, start=1):
            edges.append(
                PipelineEdge(
                    id=f"e-{source_id}-{assembly.id}",
                    source=source_id,
                    target=assembly.id,
                    sourceHandle="output",
                    targetHandle=f"video_{index}",
                )
            )
        return assembly.id

    def _append_storyboard_output(
        self,
        nodes: list[PipelineNode],
        edges: list[PipelineEdge],
        input_node_id: str,
        *,
        storyboard: StoryboardPlan,
        metadata: AutoFlowMetadata | None,
    ) -> None:
        title = (metadata.selected_title if metadata else None) or storyboard.title or "Storyboard Preview"
        transcode = PipelineNode(
            id="transcode_1",
            type="transcode",
            position=self._position(4, 0),
            data=PipelineNodeData(
                label="Transcode",
                config={"format": "mp4", "video_codec": "libx264", "audio_codec": "aac", "crf": 20},
            ),
        )
        export = PipelineNode(
            id="export_1",
            type="export",
            position=self._position(5, 0),
            data=PipelineNodeData(
                label="Export Preview",
                config={"output_dir": "/tmp/vp_autoflow_exports", "filename": _safe_filename(title)},
            ),
        )
        nodes.extend([transcode, export])
        edges.extend(
            [
                PipelineEdge(
                    id=f"e-{input_node_id}-transcode_1",
                    source=input_node_id,
                    target="transcode_1",
                    sourceHandle="output",
                    targetHandle="input",
                ),
                PipelineEdge(
                    id="e-transcode_1-export_1",
                    source="transcode_1",
                    target="export_1",
                    sourceHandle="output",
                    targetHandle="input",
                ),
            ]
        )

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
            id=f"source_{index}",
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

    def _vertical_crop_node(self, index: int, intent: AutoFlowIntent) -> PipelineNode:
        width, height = _target_dimensions(intent.aspect_ratio)
        return PipelineNode(
            id=f"vertical_crop_{index}",
            type="vertical_crop",
            position=self._position(2, index - 1),
            data=PipelineNodeData(
                label=f"Vertical Crop {index}",
                config={"mode": "smart_subject", "width": width, "height": height, "background": "blur"},
            ),
        )

    def _assembly_node(
        self,
        intent: AutoFlowIntent,
        target_duration: int,
        width: int,
        height: int,
        input_count: int,
        metadata: AutoFlowMetadata,
    ) -> PipelineNode:
        use_montage = NodeTypeRegistry.get().get_type("montage_assembler") is not None
        if use_montage:
            return PipelineNode(
                id="montage_1",
                type="montage_assembler",
                position=self._position(3, 0),
                data=PipelineNodeData(
                    label="Montage",
                    config={
                        "style": intent.style if intent.style != "auto" else "fast_cuts",
                        "target_duration": target_duration,
                        "aspect_ratio": intent.aspect_ratio,
                        "beat_sync": bool(intent.needs_bgm),
                        "max_clip_duration": 6,
                        "min_clip_duration": 1,
                        "intro_hook": metadata.selected_title or intent.subject,
                        "width": width,
                        "height": height,
                    },
                ),
            )

        return PipelineNode(
            id="concat_timeline_1",
            type="concat_timeline",
            position=self._position(3, 0),
            data=PipelineNodeData(
                label="Timeline Concat",
                config={
                    "input_count": input_count,
                    "output_format": "mp4",
                    "transition": "none",
                    "transition_duration": 0,
                    "target_duration": target_duration,
                    "normalize_resolution": True,
                    "width": width,
                    "height": height,
                },
            ),
        )

    def _title_node(self, metadata: AutoFlowMetadata, target_duration: int) -> PipelineNode:
        title = metadata.selected_title or "AutoFlow Preview"
        return PipelineNode(
            id="title_overlay_1",
            type="title_overlay",
            position=self._position(4, 0),
            data=PipelineNodeData(
                label="Title Overlay",
                config={
                    "text": title,
                    "position": "top",
                    "start_time": 0,
                    "duration": min(3, max(1, target_duration)),
                    "font_size": 72,
                    "safe_area": True,
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
            position=self._position(7, 0),
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


def _needs_vertical_crop(intent: AutoFlowIntent, template: WorkflowTemplate) -> bool:
    return intent.aspect_ratio == "9:16" or template.default_slots.get("aspect_ratio") == "9:16"


def _target_duration(intent: AutoFlowIntent, template: WorkflowTemplate) -> int:
    return int(intent.duration_sec or template.default_slots.get("target_duration") or 30)


def _target_dimensions(aspect_ratio: str) -> tuple[int, int]:
    if aspect_ratio == "16:9":
        return 1920, 1080
    if aspect_ratio == "1:1":
        return 1080, 1080
    return 1080, 1920


def _safe_filename(title: str | None) -> str:
    if not title:
        return "autoflow-preview.mp4"
    cleaned = "".join(ch for ch in title if ch.isalnum() or ch in {"-", "_"}).strip()
    return f"{cleaned[:48] or 'autoflow-preview'}.mp4"
