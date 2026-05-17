from app.node_registry.base import NodeTypeDefinition, ParamDefinition, PortDefinition, PortType


DEFINITION = NodeTypeDefinition(
    type_name="vertical_crop",
    display_name="Vertical Crop",
    category="transform",
    description="Convert video to a vertical frame with crop or blurred background.",
    icon="crop",
    inputs=[
        PortDefinition(name="input", port_type=PortType.VIDEO, description="Input video"),
    ],
    outputs=[
        PortDefinition(name="output", port_type=PortType.VIDEO, description="Vertical video"),
    ],
    params=[
        ParamDefinition(name="mode", param_type="select", default="center_crop", options=["center_crop", "blur_bg", "smart_subject"]),
        ParamDefinition(name="width", param_type="number", default=1080, min_value=64, max_value=4320),
        ParamDefinition(name="height", param_type="number", default=1920, min_value=64, max_value=7680),
        ParamDefinition(name="background", param_type="select", default="blur", options=["blur", "black", "white"]),
    ],
)
