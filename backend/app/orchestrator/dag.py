from __future__ import annotations
from collections import defaultdict, deque
from app.orchestrator.planner import (
    ASSET_INPUT_HANDLE,
    SEARCH_RESULTS_HANDLE,
    URL_INPUT_HANDLE,
    get_zip_channel_count,
    is_planner_node_type,
    is_search_node_type,
    is_zip_input_handle,
    is_zip_output_handle,
)
from app.schemas.pipeline import (
    PipelineDefinition, ValidationError, ValidationWarning, ValidationResult,
)
from app.node_registry.base import PortType
from app.node_registry.registry import NodeTypeRegistry


def _infer_actual_output_port_type(node) -> PortType | None:
    if node.type == "source":
        media_type = str(node.data.config.get("media_type") or "").strip().lower()
        return {
            "video": PortType.VIDEO,
            "audio": PortType.AUDIO,
            "image": PortType.IMAGE,
            "subtitle": PortType.SUBTITLE,
        }.get(media_type)

    if node.type == "url_download":
        download_format = str(node.data.config.get("format") or "").strip().lower()
        if download_format == "audio_only":
            return PortType.AUDIO
        return PortType.VIDEO

    return None


def _build_graph(definition: PipelineDefinition) -> tuple[dict[str, int], dict[str, list[str]], list[ValidationError]]:
    adjacency: dict[str, list[str]] = defaultdict(list)
    in_degree: dict[str, int] = {n.id: 0 for n in definition.nodes}
    nodes_by_id = {n.id: n for n in definition.nodes}
    errors: list[ValidationError] = []

    for edge in definition.edges:
        if edge.source not in nodes_by_id:
            errors.append(ValidationError(
                type="invalid_edge",
                edge_id=edge.id,
                message=f"Edge source '{edge.source}' does not exist",
            ))
            continue
        if edge.target not in nodes_by_id:
            errors.append(ValidationError(
                type="invalid_edge",
                edge_id=edge.id,
                message=f"Edge target '{edge.target}' does not exist",
            ))
            continue
        adjacency[edge.source].append(edge.target)
        in_degree[edge.target] = in_degree.get(edge.target, 0) + 1

    return in_degree, adjacency, errors


def _kahn_topological_order(
    in_degree: dict[str, int],
    adjacency: dict[str, list[str]],
) -> list[str]:
    queue = deque([nid for nid, deg in in_degree.items() if deg == 0])
    order: list[str] = []
    remaining = dict(in_degree)

    while queue:
        node_id = queue.popleft()
        order.append(node_id)
        for downstream in adjacency.get(node_id, []):
            remaining[downstream] -= 1
            if remaining[downstream] == 0:
                queue.append(downstream)

    return order


def validate_pipeline(definition: PipelineDefinition) -> ValidationResult:
    """Validate a pipeline definition for correctness."""
    errors: list[ValidationError] = []
    warnings: list[ValidationWarning] = []
    registry = NodeTypeRegistry.get()

    nodes_by_id = {n.id: n for n in definition.nodes}
    planner_bound_url_nodes = {
        edge.target
        for edge in definition.edges
        if nodes_by_id.get(edge.source)
        and nodes_by_id.get(edge.target)
        and nodes_by_id[edge.source].type == "zip_records"
        and nodes_by_id[edge.target].type == "url_download"
        and edge.targetHandle == URL_INPUT_HANDLE
    }
    planner_bound_source_nodes = {
        edge.target
        for edge in definition.edges
        if nodes_by_id.get(edge.source)
        and nodes_by_id.get(edge.target)
        and nodes_by_id[edge.source].type == "zip_records"
        and nodes_by_id[edge.target].type == "source"
        and edge.targetHandle == ASSET_INPUT_HANDLE
    }

    # 1. Check all node types exist
    for node in definition.nodes:
        if registry.get_type(node.type) is None:
            errors.append(ValidationError(
                type="unknown_node_type",
                node_id=node.id,
                message=f"Unknown node type '{node.type}'",
            ))

    in_degree, adjacency, graph_errors = _build_graph(definition)
    errors.extend(graph_errors)

    topo_order = _kahn_topological_order(in_degree, adjacency)

    if len(topo_order) < len(nodes_by_id):
        cycle_nodes = [nid for nid in nodes_by_id if nid not in topo_order]
        labels = [nodes_by_id[nid].data.label or nid for nid in cycle_nodes]
        errors.append(ValidationError(
            type="cycle_detected",
            nodes=cycle_nodes,
            message=f"Cycle detected involving nodes: {', '.join(labels)}",
        ))

    # 4. Port type validation
    for edge in definition.edges:
        src_node = nodes_by_id.get(edge.source)
        tgt_node = nodes_by_id.get(edge.target)
        if not src_node or not tgt_node:
            continue

        if is_search_node_type(src_node.type) or tgt_node.type == "zip_records" or src_node.type == "zip_records":
            planner_valid = False
            if is_search_node_type(src_node.type) and tgt_node.type == "zip_records":
                channel_count = get_zip_channel_count(tgt_node.data.config)
                planner_valid = (
                    edge.sourceHandle == SEARCH_RESULTS_HANDLE
                    and is_zip_input_handle(edge.targetHandle, channel_count)
                )
            elif src_node.type == "zip_records" and tgt_node.type == "url_download":
                channel_count = get_zip_channel_count(src_node.data.config)
                planner_valid = (
                    is_zip_output_handle(edge.sourceHandle, channel_count)
                    and edge.targetHandle == URL_INPUT_HANDLE
                )
            elif src_node.type == "zip_records" and tgt_node.type == "source":
                channel_count = get_zip_channel_count(src_node.data.config)
                planner_valid = (
                    is_zip_output_handle(edge.sourceHandle, channel_count)
                    and edge.targetHandle == ASSET_INPUT_HANDLE
                )
            if not planner_valid:
                errors.append(ValidationError(
                    type="port_type_mismatch",
                    edge_id=edge.id,
                    source_port=edge.sourceHandle,
                    target_port=edge.targetHandle,
                    message=f"Invalid planner connection '{edge.sourceHandle}' -> '{edge.targetHandle}'",
                ))
            continue

        if not registry.validate_edge(
            source_type=src_node.type,
            source_port=edge.sourceHandle,
            target_type=tgt_node.type,
            target_port=edge.targetHandle,
        ):
            errors.append(ValidationError(
                type="port_type_mismatch",
                edge_id=edge.id,
                source_port=edge.sourceHandle,
                target_port=edge.targetHandle,
                message=f"Cannot connect '{edge.sourceHandle}' to '{edge.targetHandle}' (type mismatch)",
            ))
            continue

        actual_source_type = _infer_actual_output_port_type(src_node)
        target_type_def = registry.get_type(tgt_node.type)
        if actual_source_type and target_type_def:
            target_port_def = next((p for p in target_type_def.inputs if p.name == edge.targetHandle), None)
            if (
                target_port_def
                and target_port_def.port_type != PortType.ANY_MEDIA
                and actual_source_type != target_port_def.port_type
            ):
                errors.append(ValidationError(
                    type="port_type_mismatch",
                    edge_id=edge.id,
                    source_port=edge.sourceHandle,
                    target_port=edge.targetHandle,
                    message=(
                        f"Cannot connect '{src_node.data.label or src_node.type}' "
                        f"({actual_source_type.value}) to '{tgt_node.data.label or tgt_node.type}' "
                        f"input '{edge.targetHandle}' ({target_port_def.port_type.value})"
                    ),
                ))

    # 5. Duplicate input port check + required input check
    connected_inputs: dict[str, set[str]] = defaultdict(set)
    for edge in definition.edges:
        key = (edge.target, edge.targetHandle)
        if edge.targetHandle in connected_inputs.get(edge.target, set()):
            tgt_node = nodes_by_id.get(edge.target)
            tgt_label = (tgt_node.data.label or tgt_node.type) if tgt_node else edge.target
            errors.append(ValidationError(
                type="duplicate_input_port",
                node_id=edge.target,
                target_port=edge.targetHandle,
                message=f"Input port '{edge.targetHandle}' on '{tgt_label}' has multiple connections (only one allowed)",
            ))
        connected_inputs[edge.target].add(edge.targetHandle)

    for node in definition.nodes:
        node_def = registry.get_type(node.type)
        if not node_def:
            continue
        if node.type == "zip_records":
            channel_count = get_zip_channel_count(node.data.config)
            for index in range(1, channel_count + 1):
                handle = f"input_{index}"
                if handle not in connected_inputs.get(node.id, set()):
                    errors.append(ValidationError(
                        type="missing_required_input",
                        node_id=node.id,
                        target_port=handle,
                        message=f"Required input '{handle}' on '{node.data.label or node.type}' is not connected",
                    ))
            continue
        for port in node_def.inputs:
            if port.required and port.name not in connected_inputs.get(node.id, set()):
                errors.append(ValidationError(
                    type="missing_required_input",
                    node_id=node.id,
                    target_port=port.name,
                    message=f"Required input '{port.name}' on '{node.data.label or node.type}' is not connected",
                ))

    # 6. Disconnected node warning
    has_outgoing = set()
    for edge in definition.edges:
        has_outgoing.add(edge.source)

    terminal_types = {"transcode", "material_library_ingest"}
    for node in definition.nodes:
        node_def = registry.get_type(node.type)
        if not node_def:
            continue
        if is_planner_node_type(node.type):
            continue
        if node_def.outputs and node.id not in has_outgoing and node.type not in terminal_types:
            warnings.append(ValidationWarning(
                type="disconnected_node",
                node_id=node.id,
                message=f"Node '{node.data.label or node.type}' has outputs but none are connected",
            ))

    # 7. Source node asset_id check
    for node in definition.nodes:
        if node.type == "source":
            asset_id = node.data.config.get("asset_id") or node.data.asset_id
            if not asset_id and node.id not in planner_bound_source_nodes:
                errors.append(ValidationError(
                    type="missing_asset",
                    node_id=node.id,
                    message=f"Source node '{node.data.label or 'Source'}' has no asset_id configured",
                ))

    # 8. Node parameter validation
    for node in definition.nodes:
        node_def = registry.get_type(node.type)
        if not node_def:
            continue
        config = node.data.config
        for param in node_def.params:
            value = config.get(param.name)

            # Required param missing or empty string
            if (
                node.type == "url_download"
                and param.name == "url"
                and node.id in planner_bound_url_nodes
            ):
                continue
            if (
                node.type == "source"
                and param.name == "asset_id"
                and node.id in planner_bound_source_nodes
            ):
                continue

            if param.required and (value is None or value == ""):
                errors.append(ValidationError(
                    type="invalid_param",
                    node_id=node.id,
                    param_name=param.name,
                    message=f"Required parameter '{param.name}' on '{node.data.label or node.type}' is missing or empty",
                ))
                continue

            # Number range checks
            if param.param_type == "number" and value is not None:
                try:
                    numeric_value = float(value)
                except (TypeError, ValueError):
                    errors.append(ValidationError(
                        type="invalid_param",
                        node_id=node.id,
                        param_name=param.name,
                        message=f"Parameter '{param.name}' on '{node.data.label or node.type}' must be a number",
                    ))
                    continue
                if param.min_value is not None and numeric_value < param.min_value:
                    errors.append(ValidationError(
                        type="invalid_param",
                        node_id=node.id,
                        param_name=param.name,
                        message=f"Parameter '{param.name}' on '{node.data.label or node.type}' must be >= {param.min_value} (got {numeric_value})",
                    ))
                if param.max_value is not None and numeric_value > param.max_value:
                    errors.append(ValidationError(
                        type="invalid_param",
                        node_id=node.id,
                        param_name=param.name,
                        message=f"Parameter '{param.name}' on '{node.data.label or node.type}' must be <= {param.max_value} (got {numeric_value})",
                    ))

            # Select options check
            if param.param_type == "select" and param.options is not None and value is not None:
                if value not in param.options:
                    errors.append(ValidationError(
                        type="invalid_param",
                        node_id=node.id,
                        param_name=param.name,
                        message=f"Parameter '{param.name}' on '{node.data.label or node.type}' must be one of {param.options} (got '{value}')",
                    ))

    # 9. Planner-specific structural checks
    for node in definition.nodes:
        if node.type != "zip_records":
            continue

        channel_count = get_zip_channel_count(node.data.config)
        outgoing_by_handle: dict[str, list[str]] = defaultdict(list)
        for edge in definition.edges:
            if edge.source == node.id:
                outgoing_by_handle[edge.sourceHandle].append(edge.target)

        for index in range(1, channel_count + 1):
            handle = f"output_{index}"
            targets = outgoing_by_handle.get(handle, [])
            if not targets:
                errors.append(ValidationError(
                    type="missing_required_output",
                    node_id=node.id,
                    source_port=handle,
                    message=f"Required output '{handle}' on '{node.data.label or node.type}' is not connected",
                ))
            elif len(targets) > 1:
                errors.append(ValidationError(
                    type="duplicate_output_port",
                    node_id=node.id,
                    source_port=handle,
                    message=f"Output port '{handle}' on '{node.data.label or node.type}' has multiple connections (only one allowed)",
                ))

    return ValidationResult(
        valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
    )


def topological_sort(definition: PipelineDefinition) -> list[str]:
    """Return topologically sorted list of node IDs."""
    in_degree, adjacency, _ = _build_graph(definition)
    return _kahn_topological_order(in_degree, adjacency)


def build_dependency_map(definition: PipelineDefinition) -> dict[str, list[str]]:
    """Build a map of node_id -> list of upstream node_ids it depends on."""
    deps: dict[str, list[str]] = {n.id: [] for n in definition.nodes}
    for edge in definition.edges:
        if edge.target in deps:
            deps[edge.target].append(edge.source)
    return deps
