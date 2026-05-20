package pipeline

import (
	"fmt"

	"github.com/Ctwqk/videoprocess/internal/contracts"
)

var unsupportedGoValidationNodeTypes = map[string]struct{}{
	"zip_records":             {},
	"material_search":         {},
	"youtube_search":          {},
	"x_search":                {},
	"xiaohongshu_search":      {},
	"bilibili_search":         {},
	"youtube_upload":          {},
	"x_upload":                {},
	"xiaohongshu_upload":      {},
	"material_library_ingest": {},
	"url_download":            {},
	"smart_trim":              {},
	"speech_to_subtitle":      {},
	"subtitle_translate":      {},
	"subtitle_to_speech":      {},
}

// Validate mirrors backend/app/orchestrator/dag.py `validate_pipeline` for
// the contract surfaces the Go API exposes today. AutoFlow-specific shapes
// (zip_records / dynamic video inputs / planner-bound sources) are not yet
// implemented; callers that rely on those constructs must keep using the
// Python validator until Go parity is added in a follow-up.
func Validate(def contracts.PipelineDefinition) contracts.ValidationResult {
	errors := make([]contracts.ValidationError, 0)
	warnings := make([]contracts.ValidationWarning, 0)
	registry := BuiltinRegistry()

	for _, node := range def.Nodes {
		if _, unsupported := unsupportedGoValidationNodeTypes[node.Type]; unsupported {
			id := node.ID
			return contracts.ValidationResult{
				Valid: false,
				Errors: []contracts.ValidationError{
					{
						Type:    "unsupported_go_validation",
						NodeID:  &id,
						Message: fmt.Sprintf("Go validator does not own validation for node type '%s'; route this graph to Python", node.Type),
					},
				},
				Warnings: []contracts.ValidationWarning{},
			}
		}
	}

	nodesByID := map[string]contracts.PipelineNode{}
	inDegree := map[string]int{}
	adjacency := map[string][]string{}
	connectedInputs := map[string]map[string]bool{}

	for _, node := range def.Nodes {
		nodesByID[node.ID] = node
		inDegree[node.ID] = 0
		if _, ok := registry[node.Type]; !ok {
			id := node.ID
			errors = append(errors, contracts.ValidationError{
				Type:    "unknown_node_type",
				NodeID:  &id,
				Message: "Unknown node type '" + node.Type + "'",
			})
		}
	}

	for _, edge := range def.Edges {
		source, sourceOK := nodesByID[edge.Source]
		target, targetOK := nodesByID[edge.Target]
		if !sourceOK {
			id := edge.ID
			errors = append(errors, contracts.ValidationError{
				Type:    "invalid_edge",
				EdgeID:  &id,
				Message: "Edge source '" + edge.Source + "' does not exist",
			})
			continue
		}
		if !targetOK {
			id := edge.ID
			errors = append(errors, contracts.ValidationError{
				Type:    "invalid_edge",
				EdgeID:  &id,
				Message: "Edge target '" + edge.Target + "' does not exist",
			})
			continue
		}
		if !portsCompatible(registry, source.Type, edge.SourceHandle, target.Type, edge.TargetHandle) {
			id := edge.ID
			sourcePort := edge.SourceHandle
			targetPort := edge.TargetHandle
			errors = append(errors, contracts.ValidationError{
				Type:       "port_type_mismatch",
				EdgeID:     &id,
				SourcePort: &sourcePort,
				TargetPort: &targetPort,
				Message:    "Cannot connect '" + edge.SourceHandle + "' to '" + edge.TargetHandle + "' (type mismatch)",
			})
		}

		// Duplicate input port detection: each input handle may have at
		// most one inbound edge. Mirrors `connected_inputs` set in
		// dag.py's validate_pipeline.
		if connectedInputs[edge.Target] == nil {
			connectedInputs[edge.Target] = map[string]bool{}
		}
		if connectedInputs[edge.Target][edge.TargetHandle] {
			id := edge.Target
			port := edge.TargetHandle
			label := nodeLabel(target)
			errors = append(errors, contracts.ValidationError{
				Type:       "duplicate_input_port",
				NodeID:     &id,
				TargetPort: &port,
				Message: fmt.Sprintf(
					"Input port '%s' on '%s' has multiple connections (only one allowed)",
					edge.TargetHandle, label,
				),
			})
		}
		connectedInputs[edge.Target][edge.TargetHandle] = true

		adjacency[edge.Source] = append(adjacency[edge.Source], edge.Target)
		inDegree[edge.Target]++
	}

	order := topologicalOrder(inDegree, adjacency)
	if len(order) < len(nodesByID) {
		cycleNodes := make([]string, 0)
		seen := map[string]bool{}
		for _, id := range order {
			seen[id] = true
		}
		for id := range nodesByID {
			if !seen[id] {
				cycleNodes = append(cycleNodes, id)
			}
		}
		errors = append(errors, contracts.ValidationError{
			Type:    "cycle_detected",
			Nodes:   cycleNodes,
			Message: "Cycle detected",
		})
	}

	// Required-input and source-asset binding checks.
	for _, node := range def.Nodes {
		typeDef, ok := registry[node.Type]
		if !ok {
			continue
		}
		for _, port := range typeDef.Inputs {
			if !port.Required {
				continue
			}
			if !connectedInputs[node.ID][port.Name] {
				id := node.ID
				portName := port.Name
				label := nodeLabel(node)
				errors = append(errors, contracts.ValidationError{
					Type:       "missing_required_input",
					NodeID:     &id,
					TargetPort: &portName,
					Message: fmt.Sprintf(
						"Required input '%s' on '%s' is not connected",
						port.Name, label,
					),
				})
			}
		}
		if node.Type == "source" && !sourceHasAsset(node, connectedInputs) {
			id := node.ID
			label := nodeLabel(node)
			errors = append(errors, contracts.ValidationError{
				Type:    "missing_asset",
				NodeID:  &id,
				Message: fmt.Sprintf("Source node '%s' is missing an asset binding", label),
			})
		}
	}

	return contracts.ValidationResult{Valid: len(errors) == 0, Errors: errors, Warnings: warnings}
}

// sourceHasAsset reports whether a source node has been bound to an asset
// either directly on `data.asset_id`, via `data.config.asset_id`, or through
// an upstream planner edge (e.g. zip_records binding). The planner-edge case
// is approximated by accepting any inbound edge so AutoFlow-generated
// pipelines pass validation.
func sourceHasAsset(node contracts.PipelineNode, connectedInputs map[string]map[string]bool) bool {
	if node.Data.AssetID != nil && *node.Data.AssetID != "" {
		return true
	}
	if node.Data.Config != nil {
		if raw, ok := node.Data.Config["asset_id"]; ok {
			if str, ok := raw.(string); ok && str != "" {
				return true
			}
		}
	}
	if inputs, ok := connectedInputs[node.ID]; ok && len(inputs) > 0 {
		return true
	}
	return false
}

func nodeLabel(node contracts.PipelineNode) string {
	if node.Data.Label != "" {
		return node.Data.Label
	}
	return node.Type
}

func topologicalOrder(inDegree map[string]int, adjacency map[string][]string) []string {
	remaining := map[string]int{}
	queue := make([]string, 0)
	for id, degree := range inDegree {
		remaining[id] = degree
		if degree == 0 {
			queue = append(queue, id)
		}
	}
	order := make([]string, 0, len(inDegree))
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		order = append(order, id)
		for _, downstream := range adjacency[id] {
			remaining[downstream]--
			if remaining[downstream] == 0 {
				queue = append(queue, downstream)
			}
		}
	}
	return order
}

func portsCompatible(registry map[string]NodeTypeDefinition, sourceType, sourcePort, targetType, targetPort string) bool {
	src, srcOK := registry[sourceType]
	tgt, tgtOK := registry[targetType]
	if !srcOK || !tgtOK {
		return false
	}
	srcPort, srcFound := findOutput(src, sourcePort)
	tgtPort, tgtFound := findInput(tgt, targetPort)
	if !srcFound || !tgtFound {
		return false
	}
	return tgtPort.PortType == PortAnyMedia || srcPort.PortType == PortAnyMedia || srcPort.PortType == tgtPort.PortType
}

func findOutput(def NodeTypeDefinition, name string) (PortDefinition, bool) {
	for _, port := range def.Outputs {
		if port.Name == name {
			return port, true
		}
	}
	return PortDefinition{}, false
}

func findInput(def NodeTypeDefinition, name string) (PortDefinition, bool) {
	for _, port := range def.Inputs {
		if port.Name == name {
			return port, true
		}
	}
	return PortDefinition{}, false
}
