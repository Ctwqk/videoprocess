from __future__ import annotations

from app.schemas.pipeline import PipelineDefinition

SEARCH_NODE_TYPES = {
    "youtube_search",
    "x_search",
    "xiaohongshu_search",
    "bilibili_search",
    "material_search",
}
PLANNER_NODE_TYPES = SEARCH_NODE_TYPES | {"zip_records"}
SEARCH_RESULTS_HANDLE = "results"
ZIP_INPUT_PREFIX = "input_"
ZIP_OUTPUT_PREFIX = "output_"
URL_INPUT_HANDLE = "url_input"
ASSET_INPUT_HANDLE = "asset_input"


def is_planner_node_type(type_name: str) -> bool:
    return type_name in PLANNER_NODE_TYPES


def is_search_node_type(type_name: str) -> bool:
    return type_name in SEARCH_NODE_TYPES


def get_zip_channel_count(config: dict | None) -> int:
    raw = (config or {}).get("channel_count", 2)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return 2
    return max(1, value)


def is_zip_input_handle(handle: str, channel_count: int) -> bool:
    if not handle.startswith(ZIP_INPUT_PREFIX):
        return False
    try:
        index = int(handle.removeprefix(ZIP_INPUT_PREFIX))
    except ValueError:
        return False
    return 1 <= index <= channel_count


def is_zip_output_handle(handle: str, channel_count: int) -> bool:
    if not handle.startswith(ZIP_OUTPUT_PREFIX):
        return False
    try:
        index = int(handle.removeprefix(ZIP_OUTPUT_PREFIX))
    except ValueError:
        return False
    return 1 <= index <= channel_count


def compile_runtime_definition(definition: PipelineDefinition) -> PipelineDefinition:
    planner_node_ids = {
        node.id for node in definition.nodes
        if is_planner_node_type(node.type)
    }

    data = definition.model_dump()
    data["nodes"] = [
        node for node in data["nodes"]
        if node["id"] not in planner_node_ids
    ]
    data["edges"] = [
        edge for edge in data["edges"]
        if edge["source"] not in planner_node_ids and edge["target"] not in planner_node_ids
    ]
    return PipelineDefinition.model_validate(data)
