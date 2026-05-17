from __future__ import annotations

from app.schemas.autoflow import AutoFlowIntent, WorkflowTemplate


class TemplateLibrary:
    def __init__(self) -> None:
        self._templates = {template.id: template for template in _builtin_templates()}

    def list_templates(self) -> list[WorkflowTemplate]:
        return list(self._templates.values())

    def get_template(self, template_id: str) -> WorkflowTemplate:
        try:
            return self._templates[template_id]
        except KeyError as exc:
            raise KeyError(f"Unknown AutoFlow template '{template_id}'") from exc

    def select_template(self, intent: AutoFlowIntent) -> WorkflowTemplate:
        for template in self._templates.values():
            if intent.intent_type in template.intent_types:
                return template
        return self.get_template("material_library_remix")


def _builtin_templates() -> list[WorkflowTemplate]:
    return [
        WorkflowTemplate(
            id="animal_compilation_short",
            name="Animal Compilation Short",
            description="Fast short-form animal compilation assembled from selected clips.",
            intent_types=["animal_compilation"],
            required_capabilities=[
                "source",
                "trim",
                "vertical_crop",
                "montage_assembler",
                "title_overlay",
                "transcode",
                "export",
            ],
            default_slots={"target_duration": 30, "aspect_ratio": "9:16"},
            node_blueprint=[
                {"role": "sources", "type": "source|url_download"},
                {"role": "clip_trim", "type": "trim"},
                {"role": "verticalize", "type": "vertical_crop"},
                {"role": "assembly", "type": "montage_assembler|concat_many"},
                {"role": "title", "type": "title_overlay"},
                {"role": "output", "type": "transcode"},
                {"role": "artifact", "type": "export"},
            ],
            edge_blueprint=[
                {"from": "source", "to": "trim"},
                {"from": "trim", "to": "verticalize"},
                {"from": "verticalize", "to": "assembly"},
                {"from": "assembly", "to": "title"},
                {"from": "title", "to": "output"},
                {"from": "output", "to": "artifact"},
            ],
            slot_mapping={"title": "metadata.selected_title", "duration": "intent.duration_sec"},
        ),
        WorkflowTemplate(
            id="hot_topic_explainer_short",
            name="Hot Topic Explainer Short",
            description="Short explainer with research material, speech, subtitles, and export.",
            intent_types=["hot_topic_explainer"],
            required_capabilities=[
                "youtube_search",
                "url_download",
                "subtitle_to_speech",
                "subtitle",
                "concat_timeline",
                "transcode",
                "export",
            ],
            default_slots={"target_duration": 45, "aspect_ratio": "9:16"},
            node_blueprint=[
                {"role": "research", "type": "youtube_search"},
                {"role": "download", "type": "url_download"},
                {"role": "voiceover", "type": "subtitle_to_speech"},
                {"role": "caption", "type": "subtitle"},
                {"role": "artifact", "type": "export"},
            ],
            edge_blueprint=[
                {"from": "download", "to": "caption"},
                {"from": "caption", "to": "artifact"},
            ],
            slot_mapping={"script": "intent.subject", "duration": "intent.duration_sec"},
        ),
        WorkflowTemplate(
            id="material_library_remix",
            name="Material Library Remix",
            description="Lowest-risk remix from owned material-library clips.",
            intent_types=["material_library_remix", "generic_video"],
            required_capabilities=[
                "material_search",
                "source",
                "trim",
                "vertical_crop",
                "montage_assembler",
                "title_overlay",
                "transcode",
                "export",
            ],
            default_slots={"target_duration": 20, "source_policy": "owned_only"},
            node_blueprint=[
                {"role": "search", "type": "material_search"},
                {"role": "sources", "type": "source"},
                {"role": "clip_trim", "type": "trim"},
                {"role": "verticalize", "type": "vertical_crop"},
                {"role": "assembly", "type": "montage_assembler|concat_many"},
                {"role": "title", "type": "title_overlay"},
                {"role": "output", "type": "transcode"},
                {"role": "artifact", "type": "export"},
            ],
            edge_blueprint=[
                {"from": "source", "to": "trim"},
                {"from": "trim", "to": "verticalize"},
                {"from": "verticalize", "to": "assembly"},
                {"from": "assembly", "to": "title"},
                {"from": "title", "to": "output"},
                {"from": "output", "to": "artifact"},
            ],
            slot_mapping={"query": "intent.keywords", "duration": "intent.duration_sec"},
        ),
    ]
