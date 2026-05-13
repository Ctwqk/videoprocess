from __future__ import annotations
from datetime import datetime
from typing import Any
from pydantic import BaseModel, Field


class PipelineNodeData(BaseModel):
    label: str = ""
    config: dict[str, Any] = Field(default_factory=dict)
    asset_id: str | None = None


class PipelineNode(BaseModel):
    id: str
    type: str
    position: dict[str, float]
    data: PipelineNodeData


class PipelineEdge(BaseModel):
    id: str
    source: str
    target: str
    sourceHandle: str
    targetHandle: str


class PipelineDefinition(BaseModel):
    nodes: list[PipelineNode]
    edges: list[PipelineEdge]
    viewport: dict[str, float] = Field(default_factory=lambda: {"x": 0, "y": 0, "zoom": 1})


class PipelineCreate(BaseModel):
    name: str
    description: str = ""
    definition: PipelineDefinition
    is_template: bool = False
    template_tags: list[str] = Field(default_factory=list)


class PipelineUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    definition: PipelineDefinition | None = None
    is_template: bool | None = None
    template_tags: list[str] | None = None


class PipelineResponse(BaseModel):
    id: str
    name: str
    description: str
    definition: dict
    is_template: bool
    template_tags: list[str]
    created_at: datetime
    updated_at: datetime
    version: int

    model_config = {"from_attributes": True}


class PipelineListResponse(BaseModel):
    items: list[PipelineResponse]
    total: int


class ValidationError(BaseModel):
    type: str
    message: str
    node_id: str | None = None
    edge_id: str | None = None
    nodes: list[str] | None = None
    source_port: str | None = None
    target_port: str | None = None
    param_name: str | None = None


class ValidationWarning(BaseModel):
    type: str
    message: str
    node_id: str | None = None


class ValidationResult(BaseModel):
    valid: bool
    errors: list[ValidationError]
    warnings: list[ValidationWarning]
