from app.node_registry.base import NodeTypeDefinition, ParamDefinition, PortDefinition, PortType


DEFINITION = NodeTypeDefinition(
    type_name="x_search",
    display_name="X Search",
    category="planner",
    description="Search X and prepare a selectable result list for batch planning",
    icon="search",
    inputs=[],
    outputs=[
        PortDefinition(
            name="results",
            port_type=PortType.SEARCH_RESULTS,
            description="Search result list for planner nodes",
        ),
    ],
    params=[
        ParamDefinition(
            name="query",
            param_type="string",
            required=True,
            description="X search query",
        ),
        ParamDefinition(
            name="max_results",
            param_type="number",
            default=8,
            required=True,
            min_value=1,
            max_value=50,
            description="Maximum number of search results to keep",
        ),
    ],
    worker_type="planner",
)
