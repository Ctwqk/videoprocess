from app.node_registry.base import NodeTypeDefinition, ParamDefinition

DEFINITION = NodeTypeDefinition(
    type_name="zip_records",
    display_name="Zip Records",
    category="planner",
    description="Zip multiple search result channels into aligned batch records",
    icon="workflow",
    inputs=[],
    outputs=[],
    params=[
        ParamDefinition(
            name="channel_count",
            param_type="number",
            default=2,
            required=True,
            min_value=1,
            max_value=12,
            description="Number of search channels to zip",
        ),
        ParamDefinition(
            name="record_limit",
            param_type="number",
            default=0,
            required=True,
            min_value=0,
            max_value=500,
            description="Maximum number of output records (0 = auto)",
        ),
    ],
    worker_type="planner",
)
