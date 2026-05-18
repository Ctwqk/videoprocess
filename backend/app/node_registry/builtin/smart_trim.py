from app.node_registry.base import NodeTypeDefinition, ParamDefinition, PortDefinition, PortType


DEFINITION = NodeTypeDefinition(
    type_name="smart_trim",
    display_name="Smart Trim",
    category="ai_transform",
    description="Find and trim video segments by natural language prompt",
    icon="sparkles-scissors",
    inputs=[
        PortDefinition(name="input", port_type=PortType.VIDEO, description="Input video"),
    ],
    outputs=[
        PortDefinition(name="output", port_type=PortType.VIDEO, description="Matched video clip, montage, or original video"),
    ],
    params=[
        ParamDefinition(name="prompt", param_type="string", required=True, description="Natural language segment description"),
        ParamDefinition(name="negative_prompt", param_type="string", default="", description="Things that should not appear"),
        ParamDefinition(
            name="mode",
            param_type="select",
            default="auto",
            required=True,
            options=["auto", "best_clip", "all_matches_montage", "full_if_match", "no_full_video"],
            description="Output selection policy",
        ),
        ParamDefinition(name="target_duration", param_type="number", default=0, required=True, min_value=0, max_value=600),
        ParamDefinition(name="min_clip_duration", param_type="number", default=1.5, required=True, min_value=0.3, max_value=30),
        ParamDefinition(name="max_clip_duration", param_type="number", default=8, required=True, min_value=1, max_value=120),
        ParamDefinition(name="max_clips", param_type="number", default=8, required=True, min_value=1, max_value=30),
        ParamDefinition(name="sample_fps", param_type="number", default=1, required=True, min_value=0.1, max_value=4),
        ParamDefinition(name="match_threshold", param_type="number", default=0.35, required=True, min_value=0, max_value=1),
        ParamDefinition(name="return_full_threshold", param_type="number", default=0.65, required=True, min_value=0, max_value=1),
        ParamDefinition(name="padding_before", param_type="number", default=0.5, required=True, min_value=0, max_value=10),
        ParamDefinition(name="padding_after", param_type="number", default=0.5, required=True, min_value=0, max_value=10),
        ParamDefinition(name="merge_gap", param_type="number", default=1.0, required=True, min_value=0, max_value=10),
        ParamDefinition(name="use_visual", param_type="boolean", default=True),
        ParamDefinition(name="use_asr", param_type="boolean", default=True),
        ParamDefinition(name="use_vlm_verify", param_type="boolean", default=False),
        ParamDefinition(name="language", param_type="string", default="zh"),
        ParamDefinition(
            name="whisper_model",
            param_type="select",
            default="medium",
            options=["tiny", "base", "small", "medium", "large-v3"],
            description="Whisper model used when ASR scoring is enabled",
        ),
        ParamDefinition(name="output_format", param_type="select", default="mp4", required=True, options=["mp4", "mkv", "webm"]),
        ParamDefinition(
            name="no_match_policy",
            param_type="select",
            default="placeholder",
            required=True,
            options=["placeholder", "fail"],
            description="How to handle no matched segment",
        ),
    ],
    worker_type="vision",
)
