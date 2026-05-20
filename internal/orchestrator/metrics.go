package orchestrator

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricGoOrchestratorJobsStarted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vp_go_orchestrator_jobs_started_total",
		Help: "Total Go-owned jobs started by the Go orchestrator.",
	}, []string{"result"})
	metricGoOrchestratorJobsFinalized = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vp_go_orchestrator_jobs_finalized_total",
		Help: "Total Go-owned jobs finalized by the Go orchestrator.",
	}, []string{"status"})
	metricGoOrchestratorEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vp_go_orchestrator_events_total",
		Help: "Total Go event stream messages handled by the Go orchestrator.",
	}, []string{"event", "result"})
	metricGoOrchestratorEventFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vp_go_orchestrator_event_failures_total",
		Help: "Total Go event stream messages that failed processing.",
	}, []string{"event"})
	metricGoOrchestratorDispatches = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vp_go_orchestrator_dispatches_total",
		Help: "Total tasks dispatched by the Go orchestrator.",
	}, []string{"node_type"})
	metricGoOrchestratorRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vp_go_orchestrator_retries_total",
		Help: "Total node retries scheduled by the Go orchestrator.",
	}, []string{"node_type"})
	metricGoOrchestratorRecoveries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vp_go_orchestrator_recoveries_total",
		Help: "Total recovery actions taken for Go-owned jobs.",
	}, []string{"result"})
	metricGoOrchestratorPendingReclaims = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vp_go_orchestrator_pending_reclaims_total",
		Help: "Total pending Go event stream messages reclaimed.",
	}, []string{"result"})
)

func observeGoOrchestratorEvent(event string, result string) {
	metricGoOrchestratorEvents.WithLabelValues(eventMetricLabel(event), result).Inc()
}

func observeGoOrchestratorEventFailure(event string) {
	metricGoOrchestratorEventFailures.WithLabelValues(eventMetricLabel(event)).Inc()
}

func observeGoOrchestratorRecovery(result string) {
	metricGoOrchestratorRecoveries.WithLabelValues(result).Inc()
}

func observeGoOrchestratorPendingReclaim(result string, count int) {
	if count <= 0 {
		return
	}
	metricGoOrchestratorPendingReclaims.WithLabelValues(result).Add(float64(count))
}

func eventMetricLabel(event string) string {
	switch event {
	case "node_completed", "node_failed":
		return event
	case "":
		return "malformed"
	default:
		return "unknown"
	}
}
