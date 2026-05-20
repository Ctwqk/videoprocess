package orchestrator

import "github.com/Ctwqk/videoprocess/internal/contracts"

func LeafNodeIDs(def contracts.PipelineDefinition) map[string]bool {
	hasOutgoing := map[string]bool{}
	for _, edge := range def.Edges {
		hasOutgoing[edge.Source] = true
	}
	leaf := map[string]bool{}
	for _, node := range def.Nodes {
		if !hasOutgoing[node.ID] {
			leaf[node.ID] = true
		}
	}
	return leaf
}

func DependencyMap(def contracts.PipelineDefinition) map[string][]string {
	deps := map[string][]string{}
	for _, node := range def.Nodes {
		deps[node.ID] = []string{}
	}
	for _, edge := range def.Edges {
		deps[edge.Target] = append(deps[edge.Target], edge.Source)
	}
	return deps
}

func TopologicalOrder(def contracts.PipelineDefinition) []string {
	inDegree := map[string]int{}
	adjacency := map[string][]string{}
	for _, node := range def.Nodes {
		inDegree[node.ID] = 0
	}
	for _, edge := range def.Edges {
		if _, ok := inDegree[edge.Source]; !ok {
			continue
		}
		if _, ok := inDegree[edge.Target]; !ok {
			continue
		}
		adjacency[edge.Source] = append(adjacency[edge.Source], edge.Target)
		inDegree[edge.Target]++
	}

	queue := make([]string, 0)
	for _, node := range def.Nodes {
		if inDegree[node.ID] == 0 {
			queue = append(queue, node.ID)
		}
	}

	order := make([]string, 0, len(def.Nodes))
	for len(queue) > 0 {
		nodeID := queue[0]
		queue = queue[1:]
		order = append(order, nodeID)
		for _, downstream := range adjacency[nodeID] {
			inDegree[downstream]--
			if inDegree[downstream] == 0 {
				queue = append(queue, downstream)
			}
		}
	}

	if len(order) == len(def.Nodes) {
		return order
	}
	seen := make(map[string]bool, len(order))
	for _, nodeID := range order {
		seen[nodeID] = true
	}
	for _, node := range def.Nodes {
		if !seen[node.ID] {
			order = append(order, node.ID)
		}
	}
	return order
}
