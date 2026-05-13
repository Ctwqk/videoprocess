from app.node_registry.base import NodeTypeDefinition, PortDefinition, ParamDefinition, PortType

DEFINITION = NodeTypeDefinition(
    type_name="youtube_upload",
    display_name="YouTube Upload",
    category="output",
    description="Upload video to YouTube via API",
    icon="upload",
    inputs=[
        PortDefinition(name="input", port_type=PortType.ANY_MEDIA, description="Video to upload"),
    ],
    outputs=[],
    params=[
        ParamDefinition(name="title", param_type="string", required=True,
                       description="Video title"),
        ParamDefinition(name="description", param_type="string", default="",
                       description="Video description"),
        ParamDefinition(name="privacy", param_type="select", default="private",
                       options=["public", "unlisted", "private"],
                       description="Privacy status"),
        ParamDefinition(name="made_for_kids", param_type="select", default="not_set",
                       options=["not_set", "yes", "no"],
                       description="Set YouTube audience flag during upload"),
        ParamDefinition(name="tags", param_type="string", default="",
                       description="Comma-separated tags"),
    ],
    worker_type="ffmpeg",
)
