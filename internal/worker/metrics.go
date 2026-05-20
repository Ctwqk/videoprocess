package worker

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	workerTasksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vp_worker_tasks_total",
		Help: "Total tasks claimed by Go workers.",
	}, []string{"worker_type", "node_type", "result"})
	workerTaskDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "vp_worker_task_duration_seconds",
		Help:    "Task duration for Go workers.",
		Buckets: prometheus.DefBuckets,
	}, []string{"worker_type", "node_type"})
	workerTaskFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vp_worker_task_failures_total",
		Help: "Total failed Go worker tasks.",
	}, []string{"worker_type", "node_type"})
	workerTaskCancellationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vp_worker_task_cancellations_total",
		Help: "Total confirmed Go worker cancellations.",
	}, []string{"worker_type", "node_type"})
	workerPendingReclaimsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vp_worker_pending_reclaims_total",
		Help: "Total pending Redis stream tasks reclaimed by Go workers.",
	}, []string{"worker_type"})
	workerHeartbeatFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vp_worker_heartbeat_failures_total",
		Help: "Total heartbeat refresh failures for Go workers.",
	}, []string{"worker_type"})
)

func init() {
	workerTasksTotal.WithLabelValues("ffmpeg_go", "unknown", "succeeded")
	workerTaskDuration.WithLabelValues("ffmpeg_go", "unknown")
	workerTaskFailuresTotal.WithLabelValues("ffmpeg_go", "unknown")
	workerTaskCancellationsTotal.WithLabelValues("ffmpeg_go", "unknown")
	workerPendingReclaimsTotal.WithLabelValues("ffmpeg_go")
	workerHeartbeatFailuresTotal.WithLabelValues("ffmpeg_go")
}

func observeTask(workerType string, task TaskMessage, result string, started time.Time) {
	nodeType := task.NodeType
	if nodeType == "" {
		nodeType = "unknown"
	}
	workerTasksTotal.WithLabelValues(workerType, nodeType, result).Inc()
	workerTaskDuration.WithLabelValues(workerType, nodeType).Observe(time.Since(started).Seconds())
}
