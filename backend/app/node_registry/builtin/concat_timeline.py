from app.node_registry.base import NodeTypeDefinition, PortDefinition, ParamDefinition, PortType

DEFINITION = NodeTypeDefinition(
    type_name="concat_timeline",
    display_name="Timeline Concat",
    category="combine",
    description="Concatenate videos sequentially on a dynamic timeline.",
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
        ParamDefinition(name="output_format", param_type="select", default="mp4",
                       options=["mp4", "mkv", "webm"]),
        ParamDefinition(name="transition", param_type="select", default="none",
                       options=["none", "fade", "dissolve"],
                       description="Transition effect between clips"),
        ParamDefinition(name="transition_duration", param_type="number", default=0.5,
                       min_value=0.0, max_value=5.0,
                       description="Transition duration in seconds"),
    ],
)
