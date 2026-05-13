from app.node_registry.base import NodeTypeDefinition, PortDefinition, ParamDefinition, PortType

DEFINITION = NodeTypeDefinition(
    type_name="bgm",
    display_name="Background Music",
    category="audio",
    description="Add background music to a video",
    icon="music",
    inputs=[
        PortDefinition(name="video", port_type=PortType.VIDEO, description="Input video"),
        PortDefinition(name="audio", port_type=PortType.AUDIO, description="Background music"),
    ],
    outputs=[
        PortDefinition(name="output", port_type=PortType.VIDEO, description="Video with background music"),
    ],
    params=[
        ParamDefinition(name="volume", param_type="number", default=0.3,
                       min_value=0.0, max_value=2.0,
                       description="Background music volume (0-2)"),
        ParamDefinition(name="original_volume", param_type="number", default=1.0,
                       min_value=0.0, max_value=2.0,
                       description="Original audio volume (0-2)"),
        ParamDefinition(name="loop", param_type="boolean", default=True,
                       description="Loop music if shorter than video"),
        ParamDefinition(name="fade_in", param_type="number", default=0.0,
                       min_value=0.0, max_value=10.0,
                       description="Fade in duration in seconds"),
        ParamDefinition(name="fade_out", param_type="number", default=0.0,
                       min_value=0.0, max_value=10.0,
                       description="Fade out duration in seconds"),
    ],
)
