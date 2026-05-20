from app.node_registry.base import NodeTypeDefinition, ParamDefinition, PortDefinition, PortType


DEFINITION = NodeTypeDefinition(
    type_name="title_overlay",
    display_name="Title Overlay",
    category="transform",
    description="Overlay short title text on video.",
    icon="type",
    worker_type="ffmpeg_go",
    inputs=[
        PortDefinition(name="input", port_type=PortType.VIDEO, description="Input video"),
    ],
    outputs=[
        PortDefinition(name="output", port_type=PortType.VIDEO, description="Titled video"),
    ],
    params=[
        ParamDefinition(name="text", param_type="string", default="", description="Overlay text"),
        ParamDefinition(name="position", param_type="select", default="top", options=["top", "center", "bottom"]),
        ParamDefinition(name="start_time", param_type="number", default=0, min_value=0),
        ParamDefinition(name="duration", param_type="number", default=3, min_value=0.1, max_value=3600),
        ParamDefinition(name="font_size", param_type="number", default=72, min_value=8, max_value=240),
        ParamDefinition(name="safe_area", param_type="boolean", default=True),
    ],
)
