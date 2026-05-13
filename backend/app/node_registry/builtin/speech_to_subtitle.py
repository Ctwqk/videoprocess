from app.node_registry.base import NodeTypeDefinition, ParamDefinition, PortDefinition, PortType


DEFINITION = NodeTypeDefinition(
    type_name="speech_to_subtitle",
    display_name="Speech To Subtitle",
    category="audio",
    description="Transcribe spoken audio from a media input into an SRT subtitle file",
    icon="mic",
    inputs=[
        PortDefinition(name="media", port_type=PortType.ANY_MEDIA, description="Audio or video source"),
    ],
    outputs=[
        PortDefinition(name="output", port_type=PortType.SUBTITLE, description="Generated subtitle file"),
    ],
    params=[
        ParamDefinition(
            name="model",
            param_type="select",
            default="small",
            options=["tiny", "base", "small", "medium", "large-v3"],
            description="Whisper model size",
        ),
        ParamDefinition(
            name="language",
            param_type="string",
            default="",
            description="Optional language hint such as en or zh",
        ),
        ParamDefinition(
            name="beam_size",
            param_type="number",
            default=5,
            min_value=1,
            max_value=10,
            description="Decoding beam size",
        ),
        ParamDefinition(
            name="merge_adjacent",
            param_type="boolean",
            default=True,
            description="Merge adjacent short subtitle segments to make downstream translation/TTS smoother",
        ),
        ParamDefinition(
            name="merge_max_gap_seconds",
            param_type="number",
            default=0.6,
            min_value=0,
            max_value=5,
            description="Maximum silence gap allowed when merging neighboring subtitle cues",
        ),
        ParamDefinition(
            name="merge_min_chars",
            param_type="number",
            default=40,
            min_value=1,
            max_value=200,
            description="Treat subtitle cues shorter than this as merge candidates",
        ),
        ParamDefinition(
            name="merge_min_duration_seconds",
            param_type="number",
            default=2.2,
            min_value=0.1,
            max_value=10,
            description="Treat subtitle cues shorter than this duration as merge candidates",
        ),
        ParamDefinition(
            name="merge_max_duration_seconds",
            param_type="number",
            default=8.0,
            min_value=0.5,
            max_value=30,
            description="Never merge subtitle cues into a segment longer than this duration",
        ),
    ],
)
