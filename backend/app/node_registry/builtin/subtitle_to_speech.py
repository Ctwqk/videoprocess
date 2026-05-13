from app.node_registry.base import NodeTypeDefinition, ParamDefinition, PortDefinition, PortType


DEFINITION = NodeTypeDefinition(
    type_name="subtitle_to_speech",
    display_name="Subtitle To Speech",
    category="audio",
    description="Generate a timed speech track from subtitle cues using the configured TTS service",
    icon="audio-lines",
    inputs=[
        PortDefinition(name="subtitle_file", port_type=PortType.SUBTITLE, description="Input subtitle file"),
        PortDefinition(
            name="reference_audio",
            port_type=PortType.ANY_MEDIA,
            required=False,
            description="Optional reference voice audio/video",
        ),
        PortDefinition(
            name="ref_text",
            port_type=PortType.SUBTITLE,
            required=False,
            description="Optional transcript/subtitle file matching the reference voice audio",
        ),
    ],
    outputs=[
        PortDefinition(name="output", port_type=PortType.AUDIO, description="Generated speech audio"),
    ],
    params=[
        ParamDefinition(
            name="language",
            param_type="string",
            default="en",
            description="TTS language code",
        ),
        ParamDefinition(
            name="block_merge_gap_seconds",
            param_type="number",
            default=0.6,
            description="Merge nearby subtitle cues into one speech block when the gap is small",
        ),
        ParamDefinition(
            name="block_min_chars",
            param_type="number",
            default=70,
            description="Try to merge short subtitle cues until each speech block is at least this long",
        ),
        ParamDefinition(
            name="block_max_chars",
            param_type="number",
            default=220,
            description="Soft upper bound for text length inside one speech block",
        ),
        ParamDefinition(
            name="block_min_duration_seconds",
            param_type="number",
            default=2.5,
            description="Try to merge subtitle cues until each speech block lasts at least this long",
        ),
        ParamDefinition(
            name="block_max_duration_seconds",
            param_type="number",
            default=10.0,
            description="Soft upper bound for a single speech block duration",
        ),
        ParamDefinition(
            name="alignment_max_speedup",
            param_type="number",
            default=1.35,
            description="Maximum atempo speed-up used to prevent overlap when generated speech runs long",
        ),
        ParamDefinition(
            name="alignment_max_leading_delay_ms",
            param_type="number",
            default=800,
            description="Maximum extra delay before a short speech block when there is spare room",
        ),
    ],
)
