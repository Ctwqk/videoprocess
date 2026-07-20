package channelops

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPAutoFlowPlanTaskParsesPlanAndUploadNodes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s", r.Method)
		}
		if r.URL.Path != "/api/v1/autoflow/plan" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if payload["prompt"] != "make a private upload" {
			t.Fatalf("prompt = %#v", payload["prompt"])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"plan_id":"plan-1",
			"pipeline_definition":{
				"nodes":[
					{"id":"n1","type":"trim"},
					{"id":"n2","type":"youtube_upload"}
				],
				"edges":[]
			}
		}`))
	}))
	defer server.Close()

	client := HTTPAutoFlowClient{BaseURL: server.URL}
	observation, err := client.PlanTask(context.Background(), ProductionTaskRow{ID: "task-1"}, map[string]any{
		"prompt": "make a private upload",
	})
	if err != nil {
		t.Fatalf("PlanTask returned error: %v", err)
	}
	if observation.PlanID != "plan-1" {
		t.Fatalf("PlanID = %q", observation.PlanID)
	}
	if observation.UploadNodeCount != 1 {
		t.Fatalf("UploadNodeCount = %d, want 1", observation.UploadNodeCount)
	}
	if observation.PlanPayload["plan_id"] != "plan-1" {
		t.Fatalf("PlanPayload = %#v", observation.PlanPayload)
	}
}

func TestHTTPAutoFlowApprovePlanPostsReviewNotes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s", r.Method)
		}
		if r.URL.Path != "/api/v1/autoflow/plans/plan-1/approve" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		notes, _ := payload["review_notes"].(string)
		if !strings.Contains(notes, "decision_id") || !strings.Contains(notes, "dec-1") {
			t.Fatalf("review_notes = %q", notes)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"plan_id":"plan-1"}`))
	}))
	defer server.Close()

	client := HTTPAutoFlowClient{BaseURL: server.URL}
	if err := client.ApprovePlan(context.Background(), "plan-1", map[string]any{"decision_id": "dec-1"}); err != nil {
		t.Fatalf("ApprovePlan returned error: %v", err)
	}
}

func TestHTTPAutoFlowExecuteTaskUsesTaskPlanID(t *testing.T) {
	planID := "plan-from-task"
	approvedRevisionHash := "approved-revision-hash"
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s", r.Method)
		}
		if r.URL.Path != "/api/v1/autoflow/execute" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if payload["plan_id"] != planID {
			t.Fatalf("plan_id = %#v", payload["plan_id"])
		}
		if payload["execute"] != true {
			t.Fatalf("execute = %#v", payload["execute"])
		}
		wantKey := "channelops-execute:task-1:" + planID + ":" + approvedRevisionHash
		if payload["idempotency_key"] != wantKey {
			t.Fatalf("idempotency_key = %#v, want %q", payload["idempotency_key"], wantKey)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"run_id":"run-1","job_id":"job-1","status":"pending"}`))
	}))
	defer server.Close()

	client := HTTPAutoFlowClient{BaseURL: server.URL}
	task := ProductionTaskRow{
		ID:                           "task-1",
		AutoFlowPlanID:               &planID,
		AutoFlowApprovedRevisionHash: &approvedRevisionHash,
	}
	observation, err := client.ExecuteTask(context.Background(), task, nil)
	if err != nil {
		t.Fatalf("ExecuteTask returned error: %v", err)
	}
	if observation.RunID != "run-1" || observation.JobID != "job-1" || observation.Status != "pending" {
		t.Fatalf("observation = %#v", observation)
	}
	if observation.RunPayload["run_id"] != "run-1" {
		t.Fatalf("RunPayload = %#v", observation.RunPayload)
	}
	replay, err := client.ExecuteTask(context.Background(), task, nil)
	if err != nil {
		t.Fatalf("ExecuteTask replay returned error: %v", err)
	}
	if replay.RunID != observation.RunID || requestCount != 2 {
		t.Fatalf("replay = %#v request_count = %d", replay, requestCount)
	}
}

func TestHTTPAutoFlowExecuteTaskRequiresApprovedRevisionHash(t *testing.T) {
	planID := "plan-from-task"
	client := HTTPAutoFlowClient{BaseURL: "http://should-not-be-called.invalid"}
	_, err := client.ExecuteTask(
		context.Background(),
		ProductionTaskRow{ID: "task-1", AutoFlowPlanID: &planID},
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "approved revision") {
		t.Fatalf("ExecuteTask error = %v, want approved revision failure", err)
	}
}

func TestHTTPAutoFlowExecuteTaskMapsFailedRun(t *testing.T) {
	planID := "plan-from-task"
	approvedRevisionHash := "approved-revision-hash"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"run_id":"","job_id":"","status":"failed","error_message":"execute blocked"}`))
	}))
	defer server.Close()

	client := HTTPAutoFlowClient{BaseURL: server.URL}
	observation, err := client.ExecuteTask(context.Background(), ProductionTaskRow{
		ID:                           "task-1",
		AutoFlowPlanID:               &planID,
		AutoFlowApprovedRevisionHash: &approvedRevisionHash,
	}, nil)
	if err != nil {
		t.Fatalf("ExecuteTask returned error: %v", err)
	}
	if observation.Status != "failed" {
		t.Fatalf("Status = %q", observation.Status)
	}
	if observation.ErrorMessage != "execute blocked" {
		t.Fatalf("ErrorMessage = %q", observation.ErrorMessage)
	}
}

func TestHTTPAutoFlowGetJobMapsStatusAndExtractsYouTubeMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s", r.Method)
		}
		switch r.URL.Path {
		case "/api/v1/autoflow/runs/run-1":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"run_id":"run-1","job_id":"job-1","status":"pending"}`))
			return
		case "/api/v1/jobs/job-1":
		default:
			t.Fatalf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"id":"job-1",
			"status":"succeeded",
			"node_executions":[
				{"node_type":"transcode","output_artifact_media_info":{}},
				{"node_type":"youtube_upload","output_artifact_media_info":{"youtube":{"video_id":"yt-1","privacy":"private"}}}
			]
		}`))
	}))
	defer server.Close()

	client := HTTPAutoFlowClient{BaseURL: server.URL}
	observation, err := client.GetJob(context.Background(), "run-1", "job-1")
	if err != nil {
		t.Fatalf("GetJob returned error: %v", err)
	}
	if observation.Status != "succeeded" {
		t.Fatalf("Status = %q", observation.Status)
	}
	if observation.UploadMetadata["video_id"] != "yt-1" {
		t.Fatalf("UploadMetadata = %#v", observation.UploadMetadata)
	}
}

func TestHTTPAutoFlowGetJobRejectsRunJobMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/autoflow/runs/run-1" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"run_id":"run-1","job_id":"other-job","status":"pending"}`))
	}))
	defer server.Close()

	client := HTTPAutoFlowClient{BaseURL: server.URL}
	observation, err := client.GetJob(context.Background(), "run-1", "job-1")
	if err != nil {
		t.Fatalf("GetJob returned error: %v", err)
	}
	if observation.Status != "failed" {
		t.Fatalf("Status = %q, want failed", observation.Status)
	}
	if !strings.Contains(observation.ErrorMessage, "run/job mismatch") {
		t.Fatalf("ErrorMessage = %q", observation.ErrorMessage)
	}
}

func TestHTTPAutoFlowGetJobReturnsFailedWhenRunHasNoLinkedJob(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/autoflow/runs/run-1" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"run_id":"run-1","job_id":null,"status":"failed"}`))
	}))
	defer server.Close()

	client := HTTPAutoFlowClient{BaseURL: server.URL}
	observation, err := client.GetJob(context.Background(), "run-1", "job-1")
	if err != nil {
		t.Fatalf("GetJob returned error: %v", err)
	}
	if observation.Status != "failed" {
		t.Fatalf("Status = %q, want failed", observation.Status)
	}
	if !strings.Contains(observation.ErrorMessage, "has no linked job_id") {
		t.Fatalf("ErrorMessage = %q", observation.ErrorMessage)
	}
}

func TestHTTPAutoFlowErrorIncludesMethodPathStatusAndBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad plan", http.StatusBadRequest)
	}))
	defer server.Close()

	client := HTTPAutoFlowClient{BaseURL: server.URL}
	_, err := client.PlanTask(context.Background(), ProductionTaskRow{}, map[string]any{"prompt": "x"})
	if err == nil {
		t.Fatal("expected error")
	}
	message := err.Error()
	for _, want := range []string{"POST", "/api/v1/autoflow/plan", "400", "bad plan"} {
		if !strings.Contains(message, want) {
			t.Fatalf("error %q does not contain %q", message, want)
		}
	}
}
