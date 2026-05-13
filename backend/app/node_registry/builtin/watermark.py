from app.node_registry.base import NodeTypeDefinition, PortDefinition, ParamDefinition, PortType

DEFINITION = NodeTypeDefinition(
    type_name="watermark",
    display_name="Watermark",
    category="transform",
    description="Add an image watermark overlay to a video",
    icon="image",
    inputs=[
        PortDefinition(name="video", port_type=PortType.VIDEO, description="Input video"),
        PortDefinition(name="overlay", port_type=PortType.IMAGE, description="Watermark image"),
    ],
    outputs=[
        PortDefinition(name="output", port_type=PortType.VIDEO, description="Watermarked video"),
    ],
    params=[
        ParamDefinition(name="position", param_type="select", default="bottom_right",
                       options=["top_left", "top_right", "bottom_left", "bottom_right", "center"],
                       description="Watermark position"),
        ParamDefinition(name="opacity", param_type="number", default=0.8,
                       min_value=0.0, max_value=1.0,
                       description="Watermark opacity (0-1)"),
        ParamDefinition(name="scale", param_type="number", default=0.15,
                       min_value=0.01, max_value=1.0,
                       description="Watermark scale relative to video width"),
        ParamDefinition(name="margin", param_type="number", default=10,
                       min_value=0, max_value=500,
                       description="Margin from edge in pixels"),
    ],
)
