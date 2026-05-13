from fastapi import APIRouter, HTTPException
from app.node_registry.registry import NodeTypeRegistry
from app.schemas.node_types import NodeTypeSchema, PortDefinitionSchema, ParamDefinitionSchema

router = APIRouter(prefix="/api/v1/node-types", tags=["node-types"])


def _to_schema(defn) -> NodeTypeSchema:
    return NodeTypeSchema(
        type_name=defn.type_name,
        display_name=defn.display_name,
        category=defn.category,
        description=defn.description,
        icon=defn.icon,
        inputs=[
            PortDefinitionSchema(
                name=p.name, port_type=p.port_type.value,
                required=p.required, description=p.description,
            )
            for p in defn.inputs
        ],
        outputs=[
            PortDefinitionSchema(
                name=p.name, port_type=p.port_type.value,
                required=p.required, description=p.description,
            )
            for p in defn.outputs
        ],
        params=[
            ParamDefinitionSchema(
                name=p.name, param_type=p.param_type,
                default=p.default, required=p.required,
                description=p.description, options=p.options,
                min_value=p.min_value, max_value=p.max_value,
            )
            for p in defn.params
        ],
        worker_type=defn.worker_type,
    )


@router.get("", response_model=list[NodeTypeSchema])
async def list_node_types():
    registry = NodeTypeRegistry.get()
    return [_to_schema(d) for d in registry.list_types()]


@router.get("/{type_name}", response_model=NodeTypeSchema)
async def get_node_type(type_name: str):
    registry = NodeTypeRegistry.get()
    defn = registry.get_type(type_name)
    if defn is None:
        raise HTTPException(status_code=404, detail=f"Node type '{type_name}' not found")
    return _to_schema(defn)
