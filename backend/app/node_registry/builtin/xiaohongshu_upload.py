from app.node_registry.base import NodeTypeDefinition, ParamDefinition, PortDefinition, PortType


DEFINITION = NodeTypeDefinition(
    type_name="xiaohongshu_upload",
    display_name="Xiaohongshu Upload",
    category="output",
    description="Publish a Xiaohongshu image note or video note through the unified platform browser service",
    icon="upload",
    inputs=[
        PortDefinition(
            name="input",
            port_type=PortType.ANY_MEDIA,
            description="Primary media file for the Xiaohongshu note",
        ),
        PortDefinition(
            name="image_2",
            port_type=PortType.ANY_MEDIA,
            required=False,
            description="Optional second image for image-note publishing",
        ),
        PortDefinition(
            name="image_3",
            port_type=PortType.ANY_MEDIA,
            required=False,
            description="Optional third image for image-note publishing",
        ),
        PortDefinition(
            name="image_4",
            port_type=PortType.ANY_MEDIA,
            required=False,
            description="Optional fourth image for image-note publishing",
        ),
        PortDefinition(
            name="image_5",
            port_type=PortType.ANY_MEDIA,
            required=False,
            description="Optional fifth image for image-note publishing",
        ),
        PortDefinition(
            name="image_6",
            port_type=PortType.ANY_MEDIA,
            required=False,
            description="Optional sixth image for image-note publishing",
        ),
        PortDefinition(
            name="image_7",
            port_type=PortType.ANY_MEDIA,
            required=False,
            description="Optional seventh image for image-note publishing",
        ),
        PortDefinition(
            name="image_8",
            port_type=PortType.ANY_MEDIA,
            required=False,
            description="Optional eighth image for image-note publishing",
        ),
        PortDefinition(
            name="image_9",
            port_type=PortType.ANY_MEDIA,
            required=False,
            description="Optional ninth image for image-note publishing",
        ),
    ],
    outputs=[],
    params=[
        ParamDefinition(
            name="content",
            param_type="string",
            required=True,
            description="Note body content",
        ),
        ParamDefinition(
            name="title",
            param_type="string",
            default="",
            description="Optional note title",
        ),
        ParamDefinition(
            name="topics",
            param_type="string",
            default="",
            description="Comma-separated Xiaohongshu topics without #",
        ),
        ParamDefinition(
            name="draft",
            param_type="boolean",
            default=False,
            description="Save as draft instead of publishing immediately",
        ),
        ParamDefinition(
            name="publish_mode",
            param_type="select",
            default="image_note",
            options=["image_note", "video_note"],
            description="Choose whether to publish an image note or a video note",
        ),
    ],
    worker_type="ffmpeg",
)
