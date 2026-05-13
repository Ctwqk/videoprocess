from app.node_registry.base import NodeTypeDefinition, PortDefinition, ParamDefinition, PortType

DEFINITION = NodeTypeDefinition(
    type_name="trim",
    display_name="Trim",
    category="transform",
    description="Trim a video to a specific time range",
    icon="scissors",
    inputs=[
        PortDefinition(name="input", port_type=PortType.VIDEO, description="Input video"),
    ],
    outputs=[
        PortDefinition(name="output", port_type=PortType.VIDEO, description="Trimmed video"),
    ],
    params=[
        ParamDefinition(name="start_time", param_type="string", default="00:00:00",
                       required=True, description="Start time (HH:MM:SS or seconds)"),
        ParamDefinition(name="end_time", param_type="string", default="",
                       description="End time (HH:MM:SS or seconds, empty for end of video)"),
        ParamDefinition(name="duration", param_type="string", default="",
                       description="Duration (alternative to end_time)"),
    ],
)
