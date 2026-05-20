package httpapi

import (
	"context"

	"github.com/Ctwqk/videoprocess/internal/store"
)

type GoJobService interface {
	CreateJob(ctx context.Context, pipelineID string, inputs map[string]any) (store.JobDetailRow, error)
	CreateJobBatch(ctx context.Context, pipelineID string, inputs []map[string]any) ([]store.JobDetailRow, error)
	RerunJob(ctx context.Context, jobID string) (store.JobDetailRow, error)
}
