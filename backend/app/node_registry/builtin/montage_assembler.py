from app.node_registry.base import NodeTypeDefinition, ParamDefinition, PortDefinition, PortType


DEFINITION = NodeTypeDefinition(
    type_name="montage_assembler",
    display_name="Montage Assembler",
    category="combine",
    description="Assemble 2 to 12 clips into a short-form montage.",
    icon="film",
    inputs=[
        PortDefinition(name=f"video_{index}", port_type=PortType.VIDEO, required=index <= 2, description=f"Video input {index}")
        for index in range(1, 13)
    ],
    outputs=[
        PortDefinition(name="output", port_type=PortType.VIDEO, description="Assembled montage video"),
    ],
    params=[
        ParamDefinition(
            name="style",
            param_type="select",
            default="fast_cuts",
            options=["fast_cuts", "balanced", "cinematic"],
            description="Montage pacing style",
        ),
        ParamDefinition(name="target_duration", param_type="number", default=30, min_value=0, max_value=3600),
        ParamDefinition(
            name="aspect_ratio",
            param_type="select",
            default="9:16",
            options=["9:16", "16:9", "1:1", "auto"],
        ),
        ParamDefinition(name="beat_sync", param_type="boolean", default=False),
        ParamDefinition(name="max_clip_duration", param_type="number", default=6, min_value=0.5, max_value=120),
        ParamDefinition(name="min_clip_duration", param_type="number", default=1, min_value=0.1, max_value=120),
        ParamDefinition(name="intro_hook", param_type="string", default=""),
        ParamDefinition(name="width", param_type="number", default=1080, min_value=64, max_value=7680),
        ParamDefinition(name="height", param_type="number", default=1920, min_value=64, max_value=7680),
    ],
)
