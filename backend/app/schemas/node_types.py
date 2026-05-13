from __future__ import annotations
from pydantic import BaseModel


class PortDefinitionSchema(BaseModel):
    name: str
    port_type: str
    required: bool
    description: str


class ParamDefinitionSchema(BaseModel):
    name: str
    param_type: str
    default: object | None = None
    required: bool
    description: str
    options: list[str] | None = None
    min_value: float | None = None
    max_value: float | None = None


class NodeTypeSchema(BaseModel):
    type_name: str
    display_name: str
    category: str
    description: str
    icon: str
    inputs: list[PortDefinitionSchema]
    outputs: list[PortDefinitionSchema]
    params: list[ParamDefinitionSchema]
    worker_type: str
