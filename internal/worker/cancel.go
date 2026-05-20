package worker

import (
	"context"
	"time"

	"github.com/Ctwqk/videoprocess/internal/contracts"
)

func executionStateCancelled(jobStatus contracts.JobStatus, nodeStatus contracts.NodeStatus) bool {
	return jobStatus == contracts.JobStatusCancelled || nodeStatus == contracts.NodeStatusCancelled
}

func cancelPollInterval(env RuntimeEnv) time.Duration {
	if env.CancelPollInterval > 0 {
		return env.CancelPollInterval
	}
	return 2 * time.Second
}

func (h MediaTaskHandler) watchCancellation(ctx context.Context, cancel context.CancelFunc, nodeExecutionID string, cancelled chan<- struct{}) {
	ticker := time.NewTicker(cancelPollInterval(h.env))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			state, err := h.env.Store.LoadExecutionState(ctx, nodeExecutionID)
			if err != nil {
				if h.env.Logger != nil {
					h.env.Logger.Warn("load cancellation state failed", "node_execution_id", nodeExecutionID, "error", err)
				}
				continue
			}
			if executionStateCancelled(state.JobStatus, state.NodeStatus) {
				select {
				case cancelled <- struct{}{}:
				default:
				}
				cancel()
				return
			}
		}
	}
}
