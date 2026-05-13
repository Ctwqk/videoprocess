from app.node_registry.base import NodeTypeDefinition, ParamDefinition, PortDefinition, PortType


DEFINITION = NodeTypeDefinition(
    type_name="concat_vertical_timeline",
    display_name="Vertical Timeline Concat",
    category="combine",
    description="Play one video on top with a static bottom image, then switch to a second video on bottom with a static top image.",
    icon="rows-3",
    inputs=[
        PortDefinition(name="video_first", port_type=PortType.VIDEO, description="Video shown in the first segment"),
        PortDefinition(name="video_second", port_type=PortType.VIDEO, description="Video shown in the second segment"),
        PortDefinition(name="image_top", port_type=PortType.IMAGE, required=False, description="Optional static image shown on top during the second segment. Defaults to the fifteenth frame from the end of the first video, or the last frame if the video is shorter."),
        PortDefinition(name="image_bottom", port_type=PortType.IMAGE, required=False, description="Optional static image shown on bottom during the first segment. Defaults to the fifteenth frame of the second video, or the last frame if the video is shorter."),
    ],
    outputs=[
        PortDefinition(name="output", port_type=PortType.VIDEO, description="Combined vertical timeline video"),
    ],
    params=[
        ParamDefinition(name="pane_width", param_type="number", default=640, min_value=160, max_value=2160,
                       description="Width of each pane in pixels"),
        ParamDefinition(name="pane_height", param_type="number", default=360, min_value=90, max_value=2160,
                       description="Height of each pane in pixels"),
        ParamDefinition(name="background_color", param_type="string", default="black",
                       description="Pad color used when media aspect ratios do not match the pane size"),
        ParamDefinition(name="output_format", param_type="select", default="mp4",
                       options=["mp4", "mkv", "webm"]),
    ],
)
