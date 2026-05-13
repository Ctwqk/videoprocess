from app.node_registry.base import NodeTypeDefinition, PortDefinition, ParamDefinition, PortType

DEFINITION = NodeTypeDefinition(
    type_name="subtitle",
    display_name="Subtitle",
    category="transform",
    description="Burn subtitles into a video",
    icon="captions",
    inputs=[
        PortDefinition(name="video", port_type=PortType.VIDEO, description="Input video"),
        PortDefinition(name="subtitle_file", port_type=PortType.SUBTITLE, description="Subtitle file (SRT/ASS)"),
    ],
    outputs=[
        PortDefinition(name="output", port_type=PortType.VIDEO, description="Video with subtitles"),
    ],
    params=[
        ParamDefinition(name="font_size", param_type="number", default=24,
                       min_value=8, max_value=72,
                       description="Subtitle font size"),
        ParamDefinition(name="font_color", param_type="string", default="white",
                       description="Subtitle font color"),
        ParamDefinition(name="outline_color", param_type="string", default="black",
                       description="Subtitle outline color"),
        ParamDefinition(name="position", param_type="select", default="bottom",
                       options=["top", "center", "bottom"],
                       description="Subtitle vertical position"),
    ],
)
