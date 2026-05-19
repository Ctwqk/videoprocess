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
