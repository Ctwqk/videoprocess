from app.node_registry.base import NodeTypeDefinition, PortDefinition, ParamDefinition, PortType

DEFINITION = NodeTypeDefinition(
    type_name="concat_horizontal",
    display_name="Horizontal Concat",
    category="combine",
    description="Place two videos side by side horizontally",
    icon="columns",
    inputs=[
        PortDefinition(name="video_left", port_type=PortType.VIDEO, description="Left video"),
        PortDefinition(name="video_right", port_type=PortType.VIDEO, description="Right video"),
    ],
    outputs=[
        PortDefinition(name="output", port_type=PortType.VIDEO, description="Combined video"),
    ],
    params=[
        ParamDefinition(name="output_format", param_type="select", default="mp4",
                       options=["mp4", "mkv", "webm"]),
        ParamDefinition(name="resize_mode", param_type="select", default="match_height",
                       options=["match_height", "match_width", "none"],
                       description="How to handle different resolutions"),
    ],
)
