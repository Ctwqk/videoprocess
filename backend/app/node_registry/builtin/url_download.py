from app.node_registry.base import NodeTypeDefinition, PortDefinition, ParamDefinition, PortType

DEFINITION = NodeTypeDefinition(
    type_name="url_download",
    display_name="URL Download",
    category="source",
    description="Download media from YouTube, X, Bilibili, Xiaohongshu, or direct URLs",
    icon="download",
    inputs=[
        PortDefinition(
            name="url_input",
            port_type=PortType.URL_VALUE,
            required=False,
            description="Planner-provided URL",
        ),
    ],
    outputs=[
        PortDefinition(name="output", port_type=PortType.ANY_MEDIA, description="Downloaded video"),
    ],
    params=[
        ParamDefinition(name="url", param_type="string", required=True,
                       description="YouTube, X, Bilibili, Xiaohongshu, or direct media URL"),
        ParamDefinition(name="format", param_type="select", default="best",
                       options=["best", "1080p", "720p", "480p", "audio_only"],
                       description="Download quality"),
    ],
    worker_type="ffmpeg",
)
