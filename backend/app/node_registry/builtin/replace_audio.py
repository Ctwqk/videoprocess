from app.node_registry.base import NodeTypeDefinition, ParamDefinition, PortDefinition, PortType


DEFINITION = NodeTypeDefinition(
    type_name="replace_audio",
    display_name="Replace Audio",
    category="audio",
    description="Replace a video's audio track with a supplied audio file",
    icon="replace",
    inputs=[
        PortDefinition(name="video", port_type=PortType.VIDEO, description="Input video"),
        PortDefinition(name="audio", port_type=PortType.AUDIO, description="Replacement audio"),
    ],
    outputs=[
        PortDefinition(name="output", port_type=PortType.VIDEO, description="Video with replaced audio"),
    ],
    params=[
        ParamDefinition(
            name="loop_if_shorter",
            param_type="boolean",
            default=True,
            description="Loop the replacement audio when it is shorter than the video",
        ),
        ParamDefinition(
            name="audio_volume",
            param_type="number",
            default=1.0,
            min_value=0.0,
            max_value=4.0,
            description="Replacement audio volume multiplier",
        ),
    ],
)
