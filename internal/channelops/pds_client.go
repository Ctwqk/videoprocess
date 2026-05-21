package channelops

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
)

const defaultPDSTimeout = 500 * time.Millisecond

var failPolicyByAction = map[string]string{
	"candidate_accept":    "allow",
	"plan_approval":       "flag",
	"publish":             "block",
	"promote_publication": "block",
}

type PDSDecisionRequest struct {
	ActorID    string         `json:"actor_id"`
	ActionType string         `json:"-"`
	Platform   string         `json:"-"`
	Content    map[string]any `json:"content"`
	Context    map[string]any `json:"context"`
}

type PDSDecision struct {
	DecisionID     string           `json:"decision_id"`
	Verdict        string           `json:"verdict"`
	Score          float64          `json:"score"`
	Reasons        []map[string]any `json:"reasons"`
	EvaluatedRules []string         `json:"evaluated_rules"`
	RulesVersion   string           `json:"rules_version"`
	LatencyMS      int              `json:"latency_ms"`
	Metadata       map[string]any   `json:"metadata"`
}

type PDSClient struct {
	Enabled     bool
	DevAllowAll bool
	BaseURL     string
	ClientID    string
	Timeout     time.Duration
	HTTPClient  *http.Client
}

func (c PDSClient) Decide(ctx context.Context, request PDSDecisionRequest) (PDSDecision, error) {
	if c.DevAllowAll {
		return PDSDecision{
			Verdict: "allow",
			Metadata: map[string]any{
				"warning":     "dev_allow_all",
				"fail_policy": "allow",
			},
		}, nil
	}

	if !c.Enabled {
		return failPolicyDecision(request.ActionType, "pds_disabled"), nil
	}

	payload := map[string]any{
		"actor_id": request.ActorID,
		"action": map[string]any{
			"type":     request.ActionType,
			"platform": request.Platform,
		},
		"content": mapOrEmpty(request.Content),
		"context": mapOrEmpty(request.Context),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return failPolicyDecision(request.ActionType, "pds_parse_failed"), nil
	}

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+"/v1/decide", bytes.NewReader(body))
	if err != nil {
		return failPolicyDecision(request.ActionType, "pds_unavailable"), nil
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(c.ClientID) != "" {
		httpRequest.Header.Set("X-Client-Id", c.ClientID)
	}

	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: c.timeout()}
	}
	response, err := httpClient.Do(httpRequest)
	if err != nil {
		return failPolicyDecision(request.ActionType, "pds_unavailable"), nil
	}
	defer response.Body.Close()

	if response.StatusCode >= http.StatusBadRequest {
		_, _ = io.Copy(io.Discard, response.Body)
		return failPolicyDecision(request.ActionType, "pds_unavailable"), nil
	}

	var decision PDSDecision
	if err := json.NewDecoder(response.Body).Decode(&decision); err != nil {
		return failPolicyDecision(request.ActionType, "pds_parse_failed"), nil
	}
	verdict, ok := normalizeVerdict(decision.Verdict)
	if !ok {
		return failPolicyDecision(request.ActionType, "pds_parse_failed"), nil
	}
	decision.Verdict = verdict
	if decision.Metadata == nil {
		decision.Metadata = map[string]any{}
	}
	if decision.Reasons == nil {
		decision.Reasons = []map[string]any{}
	}
	if decision.EvaluatedRules == nil {
		decision.EvaluatedRules = []string{}
	}
	return decision, nil
}

func (c PDSClient) timeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return defaultPDSTimeout
}

func failPolicyDecision(actionType string, warning string) PDSDecision {
	verdict := failPolicy(actionType)
	return PDSDecision{
		Verdict: verdict,
		Metadata: map[string]any{
			"warning":     warning,
			"fail_policy": verdict,
		},
	}
}

func failPolicy(actionType string) string {
	if verdict, ok := failPolicyByAction[actionType]; ok {
		return verdict
	}
	return "allow"
}

func normalizeVerdict(value string) (string, bool) {
	verdict := strings.ToLower(strings.TrimSpace(value))
	switch verdict {
	case "allow", "flag", "block":
		return verdict, true
	default:
		return "", false
	}
}

func mapOrEmpty(value map[string]any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	return value
}
