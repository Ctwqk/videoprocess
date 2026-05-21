package orchestrator

import (
	"strings"

	"github.com/Ctwqk/videoprocess/internal/contracts"
)

// ApplyInputOverrides is owned by Task 2. It mirrors
// backend/app/services/job_service.py input override semantics:
// top-level asset_id, dotted node paths, and nested node maps.
func ApplyInputOverrides(def contracts.PipelineDefinition, overrides map[string]any) contracts.PipelineDefinition {
	resDef := copyPipelineDefinition(def)
	if len(overrides) == 0 {
		return resDef
	}

	nodeOverrides := normalizeNodeOverrides(overrides)
	topLevelAssetID, hasTopLevelAssetID := overrides["asset_id"]
	topLevelAssetApplied := false

	for i := range resDef.Nodes {
		node := &resDef.Nodes[i]
		if node.Data.Config == nil {
			node.Data.Config = map[string]any{}
		}

		if nodeOverride, ok := nodeOverrides[node.ID]; ok {
			mergeOverrideMap(node.Data.Config, nodeOverride)
		}

		if node.Type == "source" && hasTopLevelAssetID && !topLevelAssetApplied {
			node.Data.Config["asset_id"] = topLevelAssetID
			topLevelAssetApplied = true
		}

		if assetID, ok := node.Data.Config["asset_id"].(string); ok {
			node.Data.AssetID = &assetID
		}
	}

	return resDef
}

func copyPipelineDefinition(def contracts.PipelineDefinition) contracts.PipelineDefinition {
	resDef := def

	resDef.Nodes = make([]contracts.PipelineNode, len(def.Nodes))
	copy(resDef.Nodes, def.Nodes)
	for i, node := range def.Nodes {
		if node.Position != nil {
			resDef.Nodes[i].Position = make(map[string]float64, len(node.Position))
			for k, v := range node.Position {
				resDef.Nodes[i].Position[k] = v
			}
		}
		if node.Data.AssetID != nil {
			assetID := *node.Data.AssetID
			resDef.Nodes[i].Data.AssetID = &assetID
		}
		resDef.Nodes[i].Data.Config = copyOverrideAnyMap(node.Data.Config)
	}

	resDef.Edges = make([]contracts.PipelineEdge, len(def.Edges))
	copy(resDef.Edges, def.Edges)

	if def.Viewport != nil {
		resDef.Viewport = make(map[string]float64, len(def.Viewport))
		for k, v := range def.Viewport {
			resDef.Viewport[k] = v
		}
	}

	return resDef
}

func normalizeNodeOverrides(inputOverrides map[string]any) map[string]map[string]any {
	nodeOverrides := map[string]map[string]any{}
	for key, value := range inputOverrides {
		if key == "asset_id" {
			continue
		}

		if strings.Contains(key, ".") && !isOverrideMap(value) {
			nodeID, paramName, _ := strings.Cut(key, ".")
			bucket := nodeOverrideBucket(nodeOverrides, nodeID)
			setNestedValue(bucket, paramName, value)
			continue
		}

		if override, ok := value.(map[string]any); ok {
			bucket := nodeOverrideBucket(nodeOverrides, key)
			mergeOverrideMap(bucket, override)
			continue
		}

		bucket := nodeOverrideBucket(nodeOverrides, key)
		bucket["asset_id"] = value
	}
	return nodeOverrides
}

func nodeOverrideBucket(nodeOverrides map[string]map[string]any, nodeID string) map[string]any {
	bucket := nodeOverrides[nodeID]
	if bucket == nil {
		bucket = map[string]any{}
		nodeOverrides[nodeID] = bucket
	}
	return bucket
}

func mergeOverrideMap(target map[string]any, override map[string]any) map[string]any {
	for key, value := range override {
		if valueMap, ok := value.(map[string]any); ok && !strings.Contains(key, ".") {
			existing, _ := target[key].(map[string]any)
			nested := copyOverrideAnyMap(existing)
			if nested == nil {
				nested = map[string]any{}
			}
			target[key] = mergeOverrideMap(nested, valueMap)
			continue
		}
		setNestedValue(target, key, value)
	}
	return target
}

func setNestedValue(target map[string]any, path string, value any) {
	if !strings.Contains(path, ".") {
		target[path] = value
		return
	}

	current := target
	parts := strings.Split(path, ".")
	for _, part := range parts[:len(parts)-1] {
		existing, _ := current[part].(map[string]any)
		if existing == nil {
			existing = map[string]any{}
			current[part] = existing
		}
		current = existing
	}
	current[parts[len(parts)-1]] = value
}

func isOverrideMap(value any) bool {
	_, ok := value.(map[string]any)
	return ok
}

func copyOverrideAnyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = copyOverrideAnyValue(value)
	}
	return out
}

func copyOverrideAnyValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return copyOverrideAnyMap(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = copyOverrideAnyValue(item)
		}
		return out
	default:
		return value
	}
}
