package pipeline

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

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

// Dynamic video inputs mirror DYNAMIC_VIDEO_INPUT_* in
// backend/app/orchestrator/dag.py: these nodes accept video_1..video_64, and
// concat_timeline also accepts its legacy first/second handles.
const dynamicVideoInputMaxCount = 64

var dynamicVideoInputNodeTypes = map[string]struct{}{
	"concat_many":     {},
	"concat_timeline": {},
}

var legacyTimelineInputHandles = map[string]string{
	"video_first":  "video_1",
	"video_second": "video_2",
}

var videoInputHandlePattern = regexp.MustCompile(`^video_(\d+)$`)

// Node types whose unconnected outputs do not produce a disconnected_node warning.
var terminalNodeTypes = map[string]struct{}{
	"transcode":               {},
	"material_library_ingest": {},
}

// Validate mirrors backend/app/orchestrator/dag.py `validate_pipeline` for
// the contract surfaces the Go API exposes today. Planner shapes
// (zip_records / search nodes / planner-bound sources) are not implemented;
// graphs containing them are rejected as unsupported_go_validation so callers
// route them to the Python validator. Shared parity cases live in
// backend/tests/golden/go_migration/pipeline_validation_cases.json.
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
		if edgeErr := edgePortError(registry, edge, source, target); edgeErr != nil {
			errors = append(errors, *edgeErr)
		}

		// Duplicate input port detection: each input handle may have at
		// most one inbound edge. Mirrors `connected_inputs` set in
		// dag.py's validate_pipeline, which compares canonical handles.
		targetHandle := canonicalTargetHandle(target.Type, edge.TargetHandle)
		if connectedInputs[edge.Target] == nil {
			connectedInputs[edge.Target] = map[string]bool{}
		}
		if connectedInputs[edge.Target][targetHandle] {
			id := edge.Target
			port := targetHandle
			label := nodeLabel(target)
			errors = append(errors, contracts.ValidationError{
				Type:       "duplicate_input_port",
				NodeID:     &id,
				TargetPort: &port,
				Message: fmt.Sprintf(
					"Input port '%s' on '%s' has multiple connections (only one allowed)",
					targetHandle, label,
				),
			})
		}
		connectedInputs[edge.Target][targetHandle] = true

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
		// Dynamic-input nodes require any two video_* inputs instead of the
		// manifest's fixed required ports.
		_, dynamic := dynamicVideoInputNodeTypes[node.Type]
		if dynamic {
			if countDynamicVideoInputs(node, connectedInputs) < 2 {
				id := node.ID
				portName := "video_*"
				errors = append(errors, contracts.ValidationError{
					Type:       "missing_required_input",
					NodeID:     &id,
					TargetPort: &portName,
					Message: fmt.Sprintf(
						"At least two video inputs on '%s' must be connected",
						nodeLabel(node),
					),
				})
			}
		}
		for _, port := range typeDef.Inputs {
			if dynamic || !port.Required {
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
		for _, param := range typeDef.Params {
			value := node.Data.Config[param.Name]
			if param.Required && requiredParamMissing(value) {
				id := node.ID
				paramName := param.Name
				label := nodeLabel(node)
				errors = append(errors, contracts.ValidationError{
					Type:      "invalid_param",
					NodeID:    &id,
					ParamName: &paramName,
					Message:   fmt.Sprintf("Required parameter '%s' on '%s' is missing or empty", param.Name, label),
				})
			}
		}
	}

	hasOutgoing := map[string]bool{}
	for _, edge := range def.Edges {
		hasOutgoing[edge.Source] = true
	}
	for _, node := range def.Nodes {
		typeDef, ok := registry[node.Type]
		if !ok || len(typeDef.Outputs) == 0 || hasOutgoing[node.ID] {
			continue
		}
		if _, terminal := terminalNodeTypes[node.Type]; terminal {
			continue
		}
		id := node.ID
		warnings = append(warnings, contracts.ValidationWarning{
			Type:    "disconnected_node",
			NodeID:  &id,
			Message: fmt.Sprintf("Node '%s' has outputs but none are connected", nodeLabel(node)),
		})
	}

	return contracts.ValidationResult{Valid: len(errors) == 0, Errors: errors, Warnings: warnings}
}

// edgePortError applies dag.py's per-edge port checks in the same order:
// dynamic input limit, dynamic video input typing, registry port
// compatibility, then the inferred media type of source nodes.
func edgePortError(registry map[string]NodeTypeDefinition, edge contracts.PipelineEdge, source, target contracts.PipelineNode) *contracts.ValidationError {
	mismatch := func(message string) *contracts.ValidationError {
		id := edge.ID
		sourcePort := edge.SourceHandle
		targetPort := edge.TargetHandle
		return &contracts.ValidationError{
			Type:       "port_type_mismatch",
			EdgeID:     &id,
			SourcePort: &sourcePort,
			TargetPort: &targetPort,
			Message:    message,
		}
	}
	typeMismatch := "Cannot connect '" + edge.SourceHandle + "' to '" + edge.TargetHandle + "' (type mismatch)"

	if index, dynamic := dynamicVideoInputIndex(target.Type, edge.TargetHandle); dynamic {
		if index > dynamicVideoInputMaxCount {
			err := mismatch(fmt.Sprintf(
				"Dynamic input '%s' exceeds max input count %d",
				edge.TargetHandle, dynamicVideoInputMaxCount,
			))
			err.Type = "invalid_dynamic_input"
			return err
		}
		if !dynamicVideoSourceCompatible(registry, source, edge.SourceHandle) {
			return mismatch(typeMismatch)
		}
		return nil
	}

	if !portsCompatible(registry, source.Type, edge.SourceHandle, target.Type, edge.TargetHandle) {
		return mismatch(typeMismatch)
	}

	actual, inferred := inferActualOutputPortType(source)
	if !inferred {
		return nil
	}
	targetPort, found := findInput(registry[target.Type], edge.TargetHandle)
	if !found || targetPort.PortType == PortAnyMedia || targetPort.PortType == actual {
		return nil
	}
	return mismatch(fmt.Sprintf(
		"Cannot connect '%s' (%s) to '%s' input '%s' (%s)",
		nodeLabel(source), actual, nodeLabel(target), edge.TargetHandle, targetPort.PortType,
	))
}

func canonicalTargetHandle(nodeType, handle string) string {
	if nodeType == "concat_timeline" {
		if canonical, ok := legacyTimelineInputHandles[handle]; ok {
			return canonical
		}
	}
	return handle
}

// dynamicVideoInputIndex reports the 1-based video input index of handle on a
// dynamic-input node. Indexes too large to parse are reported as exceeding
// the limit, matching Python's unbounded integers.
func dynamicVideoInputIndex(nodeType, handle string) (int, bool) {
	if _, ok := dynamicVideoInputNodeTypes[nodeType]; !ok {
		return 0, false
	}
	match := videoInputHandlePattern.FindStringSubmatch(canonicalTargetHandle(nodeType, handle))
	if match == nil {
		return 0, false
	}
	index, err := strconv.Atoi(match[1])
	if err != nil {
		return dynamicVideoInputMaxCount + 1, true
	}
	return index, index >= 1
}

func countDynamicVideoInputs(node contracts.PipelineNode, connectedInputs map[string]map[string]bool) int {
	count := 0
	for handle := range connectedInputs[node.ID] {
		if _, dynamic := dynamicVideoInputIndex(node.Type, handle); dynamic {
			count++
		}
	}
	return count
}

func dynamicVideoSourceCompatible(registry map[string]NodeTypeDefinition, source contracts.PipelineNode, sourceHandle string) bool {
	sourceDef, ok := registry[source.Type]
	if !ok {
		return false
	}
	port, found := findOutput(sourceDef, sourceHandle)
	if !found || (port.PortType != PortVideo && port.PortType != PortAnyMedia) {
		return false
	}
	actual, inferred := inferActualOutputPortType(source)
	return !inferred || actual == PortVideo
}

// inferActualOutputPortType narrows any_media source nodes by their configured
// media_type. dag.py also narrows url_download, which Go rejects as unsupported.
func inferActualOutputPortType(node contracts.PipelineNode) (PortType, bool) {
	if node.Type != "source" {
		return "", false
	}
	mediaType, _ := node.Data.Config["media_type"].(string)
	switch strings.ToLower(strings.TrimSpace(mediaType)) {
	case "video":
		return PortVideo, true
	case "audio":
		return PortAudio, true
	case "image":
		return PortImage, true
	case "subtitle":
		return PortSubtitle, true
	}
	return "", false
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

func requiredParamMissing(value any) bool {
	if value == nil {
		return true
	}
	if str, ok := value.(string); ok && str == "" {
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
