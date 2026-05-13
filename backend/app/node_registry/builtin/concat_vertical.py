from app.node_registry.base import NodeTypeDefinition, PortDefinition, ParamDefinition, PortType

DEFINITION = NodeTypeDefinition(
    type_name="concat_vertical",
    display_name="Vertical Concat",
    category="combine",
    description="Stack two videos vertically (top and bottom)",
    icon="rows",
    inputs=[
        PortDefinition(name="video_top", port_type=PortType.VIDEO, description="Top video"),
        PortDefinition(name="video_bottom", port_type=PortType.VIDEO, description="Bottom video"),
    ],
    outputs=[
        PortDefinition(name="output", port_type=PortType.VIDEO, description="Combined video"),
    ],
    params=[
        ParamDefinition(name="output_format", param_type="select", default="mp4",
                       options=["mp4", "mkv", "webm"]),
        ParamDefinition(name="resize_mode", param_type="select", default="match_width",
                       options=["match_height", "match_width", "none"],
                       description="How to handle different resolutions"),
    ],
)
