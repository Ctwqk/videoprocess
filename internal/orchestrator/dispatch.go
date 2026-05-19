package orchestrator

type TaskPayload struct {
	JobID              string
	NodeExecutionID    string
	NodeID             string
	NodeType           string
	ConfigJSON         string
	InputArtifactsJSON string
	PreferredHostsJSON string
	AffinityEnqueuedAt string
	AffinityBounces    string
}

func (p TaskPayload) RedisValues() map[string]any {
	return map[string]any{
		"job_id":               p.JobID,
		"node_execution_id":    p.NodeExecutionID,
		"node_id":              p.NodeID,
		"node_type":            p.NodeType,
		"config":               p.ConfigJSON,
		"input_artifacts":      p.InputArtifactsJSON,
		"preferred_hosts":      p.PreferredHostsJSON,
		"affinity_enqueued_at": p.AffinityEnqueuedAt,
		"affinity_bounces":     p.AffinityBounces,
	}
}
