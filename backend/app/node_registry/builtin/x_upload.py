from app.node_registry.base import NodeTypeDefinition, ParamDefinition, PortDefinition, PortType


DEFINITION = NodeTypeDefinition(
    type_name="x_upload",
    display_name="X Upload",
    category="output",
    description="Publish a media post to X through the unified platform browser service",
    icon="upload",
    inputs=[
        PortDefinition(
            name="input",
            port_type=PortType.ANY_MEDIA,
            description="Media file to attach to the X post",
        ),
    ],
    outputs=[],
    params=[
        ParamDefinition(
            name="text",
            param_type="string",
            required=True,
            description="Post text to publish on X",
        ),
        ParamDefinition(
            name="reply_to_url",
            param_type="string",
            default="",
            description="Optional X post URL to reply to",
        ),
    ],
    worker_type="ffmpeg",
)
