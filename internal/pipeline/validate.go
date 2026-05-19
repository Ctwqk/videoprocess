package pipeline

import "github.com/Ctwqk/videoprocess/internal/contracts"

func Validate(def contracts.PipelineDefinition) contracts.ValidationResult {
	errors := make([]contracts.ValidationError, 0)
	warnings := make([]contracts.ValidationWarning, 0)
	registry := BuiltinRegistry()
	nodesByID := map[string]contracts.PipelineNode{}
	inDegree := map[string]int{}
	adjacency := map[string][]string{}

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

	return contracts.ValidationResult{Valid: len(errors) == 0, Errors: errors, Warnings: warnings}
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
