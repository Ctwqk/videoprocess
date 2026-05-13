from app.node_registry.base import NodeTypeDefinition, PortDefinition, ParamDefinition, PortType

DEFINITION = NodeTypeDefinition(
    type_name="concat_timeline",
    display_name="Timeline Concat",
    category="combine",
    description="Concatenate videos sequentially on a timeline",
    icon="git-merge",
    inputs=[
        PortDefinition(name="video_first", port_type=PortType.VIDEO, description="First video"),
        PortDefinition(name="video_second", port_type=PortType.VIDEO, description="Second video"),
    ],
    outputs=[
        PortDefinition(name="output", port_type=PortType.VIDEO, description="Concatenated video"),
    ],
    params=[
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
