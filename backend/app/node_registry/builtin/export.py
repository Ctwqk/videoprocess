from app.node_registry.base import NodeTypeDefinition, PortDefinition, ParamDefinition, PortType

DEFINITION = NodeTypeDefinition(
    type_name="export",
    display_name="Export",
    category="output",
    description="Export processed video to a directory or destination",
    icon="folder-output",
    inputs=[
        PortDefinition(name="input", port_type=PortType.ANY_MEDIA, description="File to export"),
    ],
    outputs=[],  # terminal node, no outputs
    params=[
        ParamDefinition(name="output_dir", param_type="string", default="/tmp/vp_export",
                       description="Destination directory path"),
        ParamDefinition(name="filename", param_type="string", default="",
                       description="Output filename (empty = use original name)"),
    ],
    worker_type="ffmpeg",
)
