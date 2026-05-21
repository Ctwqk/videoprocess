package channelops

import "context"

type AutoFlowClient interface {
	PlanTask(ctx context.Context, task ProductionTaskRow, request map[string]any) (AutoFlowPlanObservation, error)
	ApprovePlan(ctx context.Context, planID string, evidence map[string]any) error
	ExecuteTask(ctx context.Context, task ProductionTaskRow, request map[string]any) (AutoFlowExecuteObservation, error)
	GetJob(ctx context.Context, jobID string) (AutoFlowJobObservation, error)
}

type AutoFlowPlanObservation struct {
	PlanID          string
	UploadNodeCount int
	PlanPayload     map[string]any
}

type AutoFlowExecuteObservation struct {
	RunID      string
	JobID      string
	Status     string
	RunPayload map[string]any
}

type AutoFlowJobObservation struct {
	Status         string
	RunPayload     map[string]any
	UploadMetadata map[string]any
	ErrorMessage   string
}
