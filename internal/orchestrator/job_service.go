package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Ctwqk/videoprocess/internal/contracts"
	"github.com/Ctwqk/videoprocess/internal/pipeline"
	"github.com/Ctwqk/videoprocess/internal/store"
)

type JobServiceStore interface {
	GetPipeline(ctx context.Context, id string) (store.PipelineRow, error)
	GetJobDetail(ctx context.Context, id string) (store.JobDetailRow, error)
	LoadGoJobForUpdate(ctx context.Context, jobID string) (store.JobDetailRow, error)
	CreateGoJob(ctx context.Context, in store.GoJobCreateInput) (store.JobDetailRow, error)
	CreateGoJobs(ctx context.Context, inputs []store.GoJobCreateInput) ([]store.JobDetailRow, error)
}

type UnsupportedJobError struct {
	Reason string
}

func (e UnsupportedJobError) Error() string {
	return "job orchestration for this pipeline remains Python-owned: " + e.UnsupportedReason()
}

func (e UnsupportedJobError) UnsupportedReason() string {
	if e.Reason == "" {
		return "pipeline is not eligible for Go orchestration"
	}
	return e.Reason
}

type JobService struct {
	Store        JobServiceStore
	Starter      JobStarter
	StartContext context.Context
	Logger       *slog.Logger
}

func (s *JobService) CreateJob(ctx context.Context, pipelineID string, inputs map[string]any) (store.JobDetailRow, error) {
	snapshot, err := s.snapshotForPipeline(ctx, pipelineID, inputs)
	if err != nil {
		return store.JobDetailRow{}, err
	}
	row, err := s.store().CreateGoJob(ctx, store.GoJobCreateInput{
		PipelineID:       pipelineID,
		PipelineSnapshot: snapshot,
	})
	if err != nil {
		return store.JobDetailRow{}, err
	}
	s.startAsync(row.ID)
	return row, nil
}

func (s *JobService) CreateJobBatch(ctx context.Context, pipelineID string, inputs []map[string]any) ([]store.JobDetailRow, error) {
	createInputs := make([]store.GoJobCreateInput, 0, len(inputs))
	for _, item := range inputs {
		snapshot, err := s.snapshotForPipeline(ctx, pipelineID, item)
		if err != nil {
			return nil, err
		}
		createInputs = append(createInputs, store.GoJobCreateInput{
			PipelineID:       pipelineID,
			PipelineSnapshot: snapshot,
		})
	}
	rows, err := s.store().CreateGoJobs(ctx, createInputs)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		s.startAsync(row.ID)
	}
	return rows, nil
}

func (s *JobService) RerunJob(ctx context.Context, jobID string) (store.JobDetailRow, error) {
	oldJob, err := s.store().GetJobDetail(ctx, jobID)
	if err != nil {
		return store.JobDetailRow{}, err
	}
	if oldJob.OrchestratorOwner != goOrchestratorOwner {
		return store.JobDetailRow{}, UnsupportedJobError{Reason: "job is Python-owned"}
	}
	oldJob, err = s.store().LoadGoJobForUpdate(ctx, jobID)
	if err != nil {
		return store.JobDetailRow{}, err
	}
	snapshot, err := pipelineDefinitionFromAny(oldJob.PipelineSnapshot)
	if err != nil {
		return store.JobDetailRow{}, fmt.Errorf("convert old job snapshot: %w", err)
	}
	if err := validateGoSnapshot(snapshot); err != nil {
		return store.JobDetailRow{}, err
	}
	row, err := s.store().CreateGoJob(ctx, store.GoJobCreateInput{
		PipelineID:       oldJob.PipelineID,
		PipelineSnapshot: snapshot,
	})
	if err != nil {
		return store.JobDetailRow{}, err
	}
	s.startAsync(row.ID)
	return row, nil
}

func (s *JobService) snapshotForPipeline(ctx context.Context, pipelineID string, inputs map[string]any) (contracts.PipelineDefinition, error) {
	pipelineRow, err := s.store().GetPipeline(ctx, pipelineID)
	if err != nil {
		return contracts.PipelineDefinition{}, err
	}
	def, err := pipelineDefinitionFromAny(pipelineRow.Definition)
	if err != nil {
		return contracts.PipelineDefinition{}, fmt.Errorf("convert pipeline definition: %w", err)
	}
	snapshot := ApplyInputOverrides(def, inputs)
	if err := validateGoSnapshot(snapshot); err != nil {
		return contracts.PipelineDefinition{}, err
	}
	return snapshot, nil
}

func validateGoSnapshot(snapshot contracts.PipelineDefinition) error {
	result := pipeline.Validate(snapshot)
	if !result.Valid {
		return UnsupportedJobError{Reason: validationFailureReason(result)}
	}
	eligibility := ClassifyGoEligibility(snapshot)
	if !eligibility.Eligible {
		return UnsupportedJobError{Reason: eligibility.Reason}
	}
	return nil
}

func validationFailureReason(result contracts.ValidationResult) string {
	if len(result.Errors) == 0 {
		return "pipeline validation failed"
	}
	return result.Errors[0].Message
}

func (s *JobService) startAsync(jobID string) {
	if s.Starter == nil {
		return
	}
	base := s.StartContext
	if base == nil {
		base = context.Background()
	}
	go func() {
		ctx, cancel := context.WithTimeout(base, 30*time.Second)
		defer cancel()
		if err := s.Starter.StartJob(ctx, jobID); err != nil {
			s.logger().Warn("start Go-owned job failed", "job_id", jobID, "error", err)
		}
	}()
}

func (s *JobService) store() JobServiceStore {
	if s.Store == nil {
		return nilJobServiceStore{}
	}
	return s.Store
}

func (s *JobService) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

type nilJobServiceStore struct{}

func (nilJobServiceStore) GetPipeline(context.Context, string) (store.PipelineRow, error) {
	return store.PipelineRow{}, fmt.Errorf("Go job service store is nil")
}

func (nilJobServiceStore) GetJobDetail(context.Context, string) (store.JobDetailRow, error) {
	return store.JobDetailRow{}, fmt.Errorf("Go job service store is nil")
}

func (nilJobServiceStore) LoadGoJobForUpdate(context.Context, string) (store.JobDetailRow, error) {
	return store.JobDetailRow{}, fmt.Errorf("Go job service store is nil")
}

func (nilJobServiceStore) CreateGoJob(context.Context, store.GoJobCreateInput) (store.JobDetailRow, error) {
	return store.JobDetailRow{}, fmt.Errorf("Go job service store is nil")
}

func (nilJobServiceStore) CreateGoJobs(context.Context, []store.GoJobCreateInput) ([]store.JobDetailRow, error) {
	return nil, fmt.Errorf("Go job service store is nil")
}
