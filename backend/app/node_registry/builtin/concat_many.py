from app.node_registry.base import NodeTypeDefinition, ParamDefinition, PortDefinition, PortType


DEFINITION = NodeTypeDefinition(
    type_name="concat_many",
    display_name="Concat Many",
    category="combine",
    description="Concatenate videos sequentially with dynamic timeline inputs.",
    icon="git-merge",
    inputs=[
        PortDefinition(name="video_1", port_type=PortType.VIDEO, description="Video input 1"),
        PortDefinition(name="video_2", port_type=PortType.VIDEO, description="Video input 2"),
    ],
    outputs=[
        PortDefinition(name="output", port_type=PortType.VIDEO, description="Concatenated video"),
    ],
    params=[
        ParamDefinition(
            name="input_count",
            param_type="number",
            default=2,
            min_value=2,
            description="Visible timeline input ports; expands automatically as inputs are connected",
        ),
        ParamDefinition(name="output_format", param_type="select", default="mp4", options=["mp4", "mkv", "webm"]),
        ParamDefinition(name="transition", param_type="select", default="none", options=["none", "fade", "dissolve"]),
        ParamDefinition(name="transition_duration", param_type="number", default=0.3, min_value=0, max_value=5),
        ParamDefinition(name="target_duration", param_type="number", default=30, min_value=0, max_value=3600),
        ParamDefinition(name="normalize_resolution", param_type="boolean", default=True),
        ParamDefinition(
            name="aspect_ratio",
            param_type="select",
            default="9:16",
            options=["9:16", "16:9", "1:1", "auto"],
        ),
        ParamDefinition(name="width", param_type="number", default=1080, min_value=64, max_value=7680),
        ParamDefinition(name="height", param_type="number", default=1920, min_value=64, max_value=7680),
    ],
)
