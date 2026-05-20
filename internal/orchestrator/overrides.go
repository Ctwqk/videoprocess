package orchestrator

import "github.com/Ctwqk/videoprocess/internal/contracts"

// ApplyInputOverrides is owned by Task 2. It must mirror
// backend/app/services/job_service.py input override semantics:
// top-level asset_id, dotted node paths, and nested node maps.
func ApplyInputOverrides(def contracts.PipelineDefinition, overrides map[string]any) contracts.PipelineDefinition {
	return def
}
