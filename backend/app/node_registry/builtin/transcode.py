from app.node_registry.base import NodeTypeDefinition, PortDefinition, ParamDefinition, PortType

DEFINITION = NodeTypeDefinition(
    type_name="transcode",
    display_name="Transcode",
    category="output",
    description="Transcode video to a different format or codec",
    icon="file-output",
    inputs=[
        PortDefinition(name="input", port_type=PortType.VIDEO, description="Input video"),
    ],
    outputs=[
        PortDefinition(name="output", port_type=PortType.VIDEO, description="Transcoded video"),
    ],
    params=[
        ParamDefinition(name="format", param_type="select", default="mp4",
                       options=["mp4", "mkv", "webm", "avi", "mov"],
                       description="Output container format"),
        ParamDefinition(name="video_codec", param_type="select", default="libx264",
                       options=["libx264", "libx265", "h264_nvenc", "hevc_nvenc", "libvpx-vp9", "copy"],
                       description="Video codec"),
        ParamDefinition(name="audio_codec", param_type="select", default="aac",
                       options=["aac", "libopus", "libmp3lame", "copy"],
                       description="Audio codec"),
        ParamDefinition(name="resolution", param_type="select", default="original",
                       options=["original", "3840x2160", "1920x1080", "1280x720", "854x480"],
                       description="Output resolution"),
        ParamDefinition(name="bitrate", param_type="string", default="",
                       description="Video bitrate (e.g. 5M, 2000k, empty for auto)"),
        ParamDefinition(name="crf", param_type="number", default=23,
                       min_value=0, max_value=51,
                       description="Constant Rate Factor (lower = better quality)"),
        ParamDefinition(name="preset", param_type="select", default="medium",
                       options=["ultrafast", "superfast", "veryfast", "faster", "fast", "medium", "slow", "slower", "veryslow"],
                       description="Encoding speed preset"),
    ],
)
