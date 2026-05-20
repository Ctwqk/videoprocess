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
        ParamDefinition(
            name="output_dir",
            param_type="string",
            default="/tmp/vp_export",
            description="Destination directory path",
        ),
        ParamDefinition(
            name="filename",
            param_type="string",
            default="",
            description="Output filename (empty = use original name)",
        ),
        ParamDefinition(
            name="enable_quality_qa",
            param_type="boolean",
            default=True,
            description="Run export quality QA",
        ),
        ParamDefinition(
            name="quality_gate_mode",
            param_type="select",
            default="soft_repair_once",
            options=["soft_repair_once"],
            description="Soft quality repair policy",
        ),
        ParamDefinition(
            name="vmaf_min_score",
            param_type="number",
            default=80,
            min_value=0,
            max_value=100,
            description="Minimum VMAF score before soft repair",
        ),
        ParamDefinition(
            name="loudnorm_target_i",
            param_type="number",
            default=-16,
            min_value=-40,
            max_value=0,
            description="Integrated loudness target",
        ),
        ParamDefinition(
            name="loudnorm_target_lra",
            param_type="number",
            default=11,
            min_value=1,
            max_value=30,
            description="Loudness range target",
        ),
        ParamDefinition(
            name="loudnorm_target_tp",
            param_type="number",
            default=-1.5,
            min_value=-9,
            max_value=0,
            description="True peak target",
        ),
    ],
    worker_type="ffmpeg_go",
)
