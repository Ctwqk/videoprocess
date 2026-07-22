package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Ctwqk/videoprocess/internal/contracts"
	"github.com/Ctwqk/videoprocess/internal/store"
)

type StoreAdapter struct {
	Store *store.Store
}

func NewStoreAdapter(s *store.Store) *StoreAdapter {
	return &StoreAdapter{Store: s}
}

func (a *StoreAdapter) GetJobDetail(ctx context.Context, id string) (JobView, error) {
	row, err := a.Store.GetJobDetail(ctx, id)
	if err != nil {
		return JobView{}, err
	}
	return JobViewFromStoreRow(row)
}

func (a *StoreAdapter) CreateSourceArtifact(ctx context.Context, jobID string, nodeExecutionID string, assetID string) (string, error) {
	return a.Store.CreateSourceArtifact(ctx, jobID, nodeExecutionID, assetID)
}

func (a *StoreAdapter) GetVideoScheduleAuthority(ctx context.Context) (VideoScheduleAuthority, error) {
	status, err := a.Store.GetVideoScheduleStatus(ctx)
	if err != nil {
		return VideoScheduleAuthority{}, err
	}
	guardedJobID := ""
	if status.GuardedJobID != nil {
		guardedJobID = *status.GuardedJobID
	}
	return VideoScheduleAuthority{State: status.State, GuardedJobID: guardedJobID}, nil
}

func (a *StoreAdapter) MarkGoJobPlanning(ctx context.Context, jobID string, executionPlan map[string]any) error {
	return a.Store.MarkGoJobPlanning(ctx, jobID, executionPlan)
}

func (a *StoreAdapter) MarkGoJobRunning(ctx context.Context, jobID string) error {
	return a.Store.MarkGoJobRunning(ctx, jobID)
}

func (a *StoreAdapter) MarkGoJobWaitingWindow(ctx context.Context, jobID string) error {
	return a.Store.MarkGoJobWaitingWindow(ctx, jobID)
}

func (a *StoreAdapter) MarkGoNodeQueued(ctx context.Context, nodeExecutionID string, inputArtifactIDs []string) (bool, error) {
	return a.Store.MarkGoNodeQueued(ctx, nodeExecutionID, inputArtifactIDs)
}

func (a *StoreAdapter) ReleaseGoNodeQueueClaim(ctx context.Context, nodeExecutionID string) error {
	return a.Store.ReleaseGoNodeQueueClaim(ctx, nodeExecutionID)
}

func (a *StoreAdapter) MarkGoNodeSucceeded(ctx context.Context, jobID string, nodeExecutionID string, outputArtifactID string) error {
	return a.Store.MarkGoNodeSucceeded(ctx, jobID, nodeExecutionID, outputArtifactID)
}

func (a *StoreAdapter) MarkGoNodeFailed(ctx context.Context, jobID string, nodeExecutionID string, errorMessage string) error {
	return a.Store.MarkGoNodeFailed(ctx, jobID, nodeExecutionID, errorMessage)
}

func (a *StoreAdapter) IncrementGoNodeRetry(ctx context.Context, jobID string, nodeExecutionID string) error {
	return a.Store.IncrementGoNodeRetry(ctx, jobID, nodeExecutionID)
}

func (a *StoreAdapter) SkipGoDownstreamNodes(ctx context.Context, jobID string, nodeIDs []string) error {
	return a.Store.SkipGoDownstreamNodes(ctx, jobID, nodeIDs)
}

func (a *StoreAdapter) FinalizeGoJob(ctx context.Context, jobID string, status string, errorMessage *string, finalArtifactNodeIDs []string) error {
	return a.Store.FinalizeGoJob(ctx, jobID, status, errorMessage, finalArtifactNodeIDs)
}

func (a *StoreAdapter) ListRecoverableGoJobs(ctx context.Context) ([]JobView, error) {
	rows, err := a.Store.ListRecoverableGoJobs(ctx)
	if err != nil {
		return nil, err
	}
	jobs := make([]JobView, 0, len(rows))
	for _, row := range rows {
		job, err := JobViewFromStoreRow(row)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func (a *StoreAdapter) ResetStaleGoNodes(ctx context.Context, jobID string, staleBefore time.Time) error {
	return a.Store.ResetStaleGoNodes(ctx, jobID, staleBefore)
}

func JobViewFromStoreRow(row store.JobDetailRow) (JobView, error) {
	snapshot, err := pipelineDefinitionFromAny(row.PipelineSnapshot)
	if err != nil {
		return JobView{}, fmt.Errorf("convert pipeline snapshot: %w", err)
	}
	executionPlan, err := executionPlanFromAny(row.ExecutionPlan)
	if err != nil {
		return JobView{}, fmt.Errorf("convert execution plan: %w", err)
	}
	nodes := make([]NodeExecutionView, 0, len(row.NodeExecutions))
	for _, node := range row.NodeExecutions {
		outputArtifactID := ""
		if node.OutputArtifactID != nil {
			outputArtifactID = *node.OutputArtifactID
		}
		workerID := ""
		if node.WorkerID != nil {
			workerID = *node.WorkerID
		}
		errorMessage := ""
		if node.ErrorMessage != nil {
			errorMessage = *node.ErrorMessage
		}
		nodes = append(nodes, NodeExecutionView{
			ID:               node.ID,
			NodeID:           node.NodeID,
			NodeType:         node.NodeType,
			NodeLabel:        node.NodeLabel,
			Status:           node.Status,
			RetryCount:       node.RetryCount,
			NodeConfig:       copyStringAnyMap(node.NodeConfig),
			OutputArtifactID: outputArtifactID,
			InputArtifactIDs: append([]string(nil), node.InputArtifactIDs...),
			WorkerID:         workerID,
			ErrorMessage:     errorMessage,
		})
	}
	return JobView{
		ID:                row.ID,
		Status:            row.Status,
		OrchestratorOwner: row.OrchestratorOwner,
		PipelineSnapshot:  snapshot,
		ExecutionPlan:     executionPlan,
		Nodes:             nodes,
	}, nil
}

func pipelineDefinitionFromAny(value any) (contracts.PipelineDefinition, error) {
	if value == nil {
		return contracts.PipelineDefinition{}, nil
	}
	if def, ok := value.(contracts.PipelineDefinition); ok {
		return def, nil
	}
	if def, ok := value.(*contracts.PipelineDefinition); ok && def != nil {
		return *def, nil
	}
	var def contracts.PipelineDefinition
	if err := remarshal(value, &def); err != nil {
		return contracts.PipelineDefinition{}, err
	}
	return def, nil
}

func executionPlanFromAny(value any) (map[string]any, error) {
	if value == nil {
		return nil, nil
	}
	plan, ok := value.(map[string]any)
	if !ok {
		if err := remarshal(value, &plan); err != nil {
			return nil, err
		}
	}
	out := copyStringAnyMap(plan)
	if deps, ok := normalizeStringSliceMap(plan["dependencies"]); ok {
		out["dependencies"] = deps
	}
	if topo, ok := normalizeStringSlice(plan["topo_order"]); ok {
		out["topo_order"] = topo
	}
	return out, nil
}

func normalizeStringSliceMap(value any) (map[string][]string, bool) {
	switch typed := value.(type) {
	case map[string][]string:
		out := make(map[string][]string, len(typed))
		for key, values := range typed {
			out[key] = append([]string(nil), values...)
		}
		return out, true
	case map[string]any:
		out := make(map[string][]string, len(typed))
		for key, values := range typed {
			slice, ok := normalizeStringSlice(values)
			if !ok {
				return nil, false
			}
			out[key] = slice
		}
		return out, true
	default:
		return nil, false
	}
}

func normalizeStringSlice(value any) ([]string, bool) {
	switch typed := value.(type) {
	case []string:
		return append([]string(nil), typed...), true
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, false
			}
			out = append(out, text)
		}
		return out, true
	default:
		return nil, false
	}
}

func copyStringAnyMap(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}
	out := make(map[string]any, len(value))
	for key, item := range value {
		out[key] = item
	}
	return out
}

func remarshal(in any, out any) error {
	raw, err := json.Marshal(in)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}
