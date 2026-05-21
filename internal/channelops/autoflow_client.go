package channelops

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type AutoFlowClient interface {
	PlanTask(ctx context.Context, task ProductionTaskRow, request map[string]any) (AutoFlowPlanObservation, error)
	ApprovePlan(ctx context.Context, planID string, evidence map[string]any) error
	ExecuteTask(ctx context.Context, task ProductionTaskRow, request map[string]any) (AutoFlowExecuteObservation, error)
	GetJob(ctx context.Context, jobID string) (AutoFlowJobObservation, error)
}

type HTTPAutoFlowClient struct {
	BaseURL    string
	Timeout    time.Duration
	HTTPClient *http.Client
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

func (c HTTPAutoFlowClient) PlanTask(ctx context.Context, task ProductionTaskRow, request map[string]any) (AutoFlowPlanObservation, error) {
	payload, err := c.postJSON(ctx, "/api/v1/autoflow/plan", jsonObject(request))
	if err != nil {
		return AutoFlowPlanObservation{}, err
	}
	pipelineDefinition := mapFromAny(payload["pipeline_definition"])
	return AutoFlowPlanObservation{
		PlanID:          firstString(payload, "plan_id"),
		UploadNodeCount: countUploadNodes(pipelineDefinition),
		PlanPayload:     payload,
	}, nil
}

func (c HTTPAutoFlowClient) ApprovePlan(ctx context.Context, planID string, evidence map[string]any) error {
	planID = strings.TrimSpace(planID)
	if planID == "" {
		return fmt.Errorf("autoflow plan_id is required")
	}
	notes := "ChannelOps Go runner approval"
	if len(evidence) > 0 {
		raw, err := json.Marshal(evidence)
		if err != nil {
			return err
		}
		notes = "ChannelOps Go runner approval evidence: " + string(raw)
	}
	_, err := c.postJSON(ctx, "/api/v1/autoflow/plans/"+url.PathEscape(planID)+"/approve", map[string]any{
		"review_notes": notes,
	})
	return err
}

func (c HTTPAutoFlowClient) ExecuteTask(ctx context.Context, task ProductionTaskRow, request map[string]any) (AutoFlowExecuteObservation, error) {
	planID := ""
	if task.AutoFlowPlanID != nil {
		planID = strings.TrimSpace(*task.AutoFlowPlanID)
	}
	if planID == "" {
		planID = strings.TrimSpace(fmt.Sprint(firstAny(request, "autoflow_plan_id", "plan_id")))
	}
	if planID == "" {
		return AutoFlowExecuteObservation{}, fmt.Errorf("autoflow plan_id is required")
	}
	payload, err := c.postJSON(ctx, "/api/v1/autoflow/execute", map[string]any{
		"plan_id": planID,
		"execute": true,
	})
	if err != nil {
		return AutoFlowExecuteObservation{}, err
	}
	return AutoFlowExecuteObservation{
		RunID:      firstString(payload, "run_id"),
		JobID:      firstString(payload, "job_id"),
		Status:     firstString(payload, "status"),
		RunPayload: payload,
	}, nil
}

func (c HTTPAutoFlowClient) GetJob(ctx context.Context, jobID string) (AutoFlowJobObservation, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return AutoFlowJobObservation{}, fmt.Errorf("autoflow job_id is required")
	}
	payload, err := c.getJSON(ctx, "/api/v1/jobs/"+url.PathEscape(jobID))
	if err != nil {
		return AutoFlowJobObservation{}, err
	}
	status := autoflowJobStatus(firstString(payload, "status"))
	observation := AutoFlowJobObservation{
		Status:         status,
		RunPayload:     payload,
		UploadMetadata: map[string]any{},
	}
	if status == "failed" {
		observation.ErrorMessage = firstString(payload, "error_message", "detail")
	}
	if status == "succeeded" {
		observation.UploadMetadata = youtubeUploadMetadata(payload)
	}
	return observation, nil
}

func (c HTTPAutoFlowClient) getJSON(ctx context.Context, path string) (map[string]any, error) {
	return c.doJSON(ctx, http.MethodGet, path, nil)
}

func (c HTTPAutoFlowClient) postJSON(ctx context.Context, path string, payload map[string]any) (map[string]any, error) {
	raw, err := json.Marshal(jsonObject(payload))
	if err != nil {
		return nil, err
	}
	return c.doJSON(ctx, http.MethodPost, path, bytes.NewReader(raw))
}

func (c HTTPAutoFlowClient) doJSON(ctx context.Context, method string, path string, body *bytes.Reader) (map[string]any, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("AUTOFLOW_BASE_URL is required for live ChannelOps runner mode")
	}
	var requestBody io.Reader
	if body != nil {
		requestBody = body
	}
	request, err := http.NewRequestWithContext(ctx, method, baseURL+path, requestBody)
	if err != nil {
		return nil, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	client := c.HTTPClient
	if client == nil {
		timeout := c.Timeout
		if timeout <= 0 {
			timeout = 10 * time.Second
		}
		client = &http.Client{Timeout: timeout}
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		raw, _ := io.ReadAll(response.Body)
		return nil, fmt.Errorf("autoflow %s %s returned %s: %s", method, path, response.Status, strings.TrimSpace(string(raw)))
	}
	var payload map[string]any
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return jsonObject(payload), nil
}

func countUploadNodes(pipelineDefinition map[string]any) int {
	nodes, ok := pipelineDefinition["nodes"].([]any)
	if !ok {
		return 0
	}
	count := 0
	for _, item := range nodes {
		node := mapFromAny(item)
		if firstString(node, "type") == "youtube_upload" {
			count++
		}
	}
	return count
}

func autoflowJobStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "succeeded", "success", "completed", "complete":
		return "succeeded"
	case "failed", "cancelled", "canceled", "partially_failed", "error":
		return "failed"
	default:
		return "running"
	}
}

func youtubeUploadMetadata(jobPayload map[string]any) map[string]any {
	nodeExecutions, ok := jobPayload["node_executions"].([]any)
	if !ok {
		return map[string]any{}
	}
	for _, item := range nodeExecutions {
		node := mapFromAny(item)
		if firstString(node, "node_type") != "youtube_upload" {
			continue
		}
		mediaInfo := mapFromAny(node["output_artifact_media_info"])
		youtube := mapFromAny(mediaInfo["youtube"])
		if len(youtube) > 0 {
			return youtube
		}
	}
	return map[string]any{}
}
