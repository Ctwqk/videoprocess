from app.node_registry.base import NodeTypeDefinition, ParamDefinition, PortDefinition, PortType


DEFINITION = NodeTypeDefinition(
    type_name="material_library_ingest",
    display_name="Material Library Ingest",
    category="transform",
    description="Slice a source video into indexed material clips and store them in one or more material libraries",
    icon="database-zap",
    inputs=[
        PortDefinition(name="video", port_type=PortType.VIDEO, description="Source video asset to index"),
    ],
    outputs=[
        PortDefinition(name="output", port_type=PortType.ANY_MEDIA, description="JSON summary artifact"),
    ],
    params=[
        ParamDefinition(
            name="target_library_ids",
            param_type="string",
            required=True,
            description="Material libraries to ingest into",
        ),
        ParamDefinition(
            name="clip_len",
            param_type="number",
            default=8,
            required=True,
            min_value=1,
            max_value=120,
            description="Clip length in seconds",
        ),
        ParamDefinition(
            name="stride",
            param_type="number",
            default=4,
            required=True,
            min_value=1,
            max_value=120,
            description="Sliding window stride in seconds",
        ),
        ParamDefinition(
            name="subtitle_mode",
            param_type="select",
            default="asr_if_missing",
            required=True,
            options=["asr_if_missing", "asr_always"],
            description="How subtitles are generated for indexing",
        ),
        ParamDefinition(
            name="store_neighbors",
            param_type="boolean",
            default=True,
            description="Store neighboring clip ids for later expansion",
        ),
    ],
    worker_type="ffmpeg",
)
