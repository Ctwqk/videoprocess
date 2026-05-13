from app.node_registry.base import NodeTypeDefinition, ParamDefinition, PortDefinition, PortType


DEFINITION = NodeTypeDefinition(
    type_name="material_search",
    display_name="Material Search",
    category="planner",
    description="Search one or more material libraries with natural language and materialize refined clip assets",
    icon="library",
    inputs=[],
    outputs=[
        PortDefinition(
            name="results",
            port_type=PortType.SEARCH_RESULTS,
            description="Material search result list for planner nodes",
        ),
    ],
    params=[
        ParamDefinition(name="query", param_type="string", required=True, description="Natural language query"),
        ParamDefinition(
            name="source_library_ids",
            param_type="string",
            required=True,
            description="Material libraries to search",
        ),
        ParamDefinition(
            name="result_library_ids",
            param_type="string",
            required=False,
            description="Material libraries to save refined clips into",
        ),
        ParamDefinition(name="top_k", param_type="number", default=50, required=True, min_value=1, max_value=200, description="Coarse recall top-k"),
        ParamDefinition(name="merge_gap", param_type="number", default=5, required=True, min_value=0, max_value=30, description="Cluster merge gap in seconds"),
        ParamDefinition(name="expand_left", param_type="number", default=4, required=True, min_value=0, max_value=30, description="Context expansion before cluster"),
        ParamDefinition(name="expand_right", param_type="number", default=4, required=True, min_value=0, max_value=30, description="Context expansion after cluster"),
        ParamDefinition(name="rerank_top_m", param_type="number", default=8, required=True, min_value=1, max_value=50, description="Top windows to rerank/refine"),
        ParamDefinition(name="min_duration", param_type="number", default=1.5, required=True, min_value=0.5, max_value=60, description="Minimum final duration"),
        ParamDefinition(name="max_duration", param_type="number", default=20, required=True, min_value=1, max_value=120, description="Maximum final duration"),
        ParamDefinition(name="dedupe_overlap_threshold", param_type="number", default=0.6, required=True, min_value=0, max_value=1, description="Overlap ratio used to dedupe refined results"),
    ],
    worker_type="planner",
)
