from app.node_registry.base import NodeTypeDefinition, ParamDefinition, PortDefinition, PortType


DEFINITION = NodeTypeDefinition(
    type_name="subtitle_translate",
    display_name="Subtitle Translate",
    category="audio",
    description="Translate an SRT subtitle file into another language",
    icon="languages",
    inputs=[
        PortDefinition(name="subtitle_file", port_type=PortType.SUBTITLE, description="Input subtitle file"),
    ],
    outputs=[
        PortDefinition(name="output", port_type=PortType.SUBTITLE, description="Translated subtitle file"),
    ],
    params=[
        ParamDefinition(
            name="target_language",
            param_type="string",
            default="en",
            required=True,
            description="Target language code or name",
        ),
        ParamDefinition(
            name="source_language",
            param_type="string",
            default="",
            description="Optional source language hint",
        ),
        ParamDefinition(
            name="model",
            param_type="string",
            default="",
            description="Optional watchdog route/model override",
        ),
    ],
)
