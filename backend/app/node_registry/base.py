from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class PortType(str, Enum):
    VIDEO = "video"
    AUDIO = "audio"
    IMAGE = "image"
    SUBTITLE = "subtitle"
    ANY_MEDIA = "any_media"
    SEARCH_RESULTS = "search_results"
    URL_VALUE = "url_value"
    ASSET_VALUE = "asset_value"


@dataclass
class PortDefinition:
    name: str
    port_type: PortType
    required: bool = True
    description: str = ""


@dataclass
class ParamDefinition:
    name: str
    param_type: str  # "string" | "number" | "boolean" | "select" | "file"
    default: Any = None
    required: bool = False
    description: str = ""
    options: list[str] | None = None
    min_value: float | None = None
    max_value: float | None = None


@dataclass
class NodeTypeDefinition:
    type_name: str
    display_name: str
    category: str
    description: str = ""
    icon: str = ""
    inputs: list[PortDefinition] = field(default_factory=list)
    outputs: list[PortDefinition] = field(default_factory=list)
    params: list[ParamDefinition] = field(default_factory=list)
    worker_type: str = "ffmpeg"
