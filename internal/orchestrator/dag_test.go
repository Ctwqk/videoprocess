package orchestrator

import "testing"

func TestTaskPayloadRedisValuesUsesPythonKeys(t *testing.T) {
	values := TaskPayload{
		JobID:              "job",
		NodeExecutionID:    "node-exec",
		NodeID:             "trim_1",
		NodeType:           "trim",
		ConfigJSON:         "{}",
		InputArtifactsJSON: "{}",
		PreferredHostsJSON: "[]",
		AffinityEnqueuedAt: "1779120000",
		AffinityBounces:    "0",
		EventStream:        "vp:events:go",
		OrchestratorOwner:  "go",
	}.RedisValues()

	for _, key := range []string{"job_id", "node_execution_id", "node_id", "node_type", "config", "input_artifacts", "preferred_hosts", "affinity_enqueued_at", "affinity_bounces", "event_stream", "orchestrator_owner"} {
		if _, ok := values[key]; !ok {
			t.Fatalf("missing key %s", key)
		}
	}
}
