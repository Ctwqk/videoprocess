# Go Orchestrator Phase 6 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let `api-go` create, schedule, listen, recover, retry, and finalize Go-owned pure-ffmpeg jobs without Python orchestration.

**Architecture:** `api-go` embeds a Go orchestrator behind explicit feature flags. Go-owned jobs are marked with `jobs.orchestrator_owner='go'`, emit worker events to `vp:events:go`, and are processed only by the Go event listener. Non-eligible jobs are rejected without fallback to Python.

**Tech Stack:** Go 1.25, chi, pgx, go-redis, Prometheus client, Python Alembic/SQLAlchemy models for schema ownership, pytest live parity tests, Docker Compose, Redis Streams.

---

## Execution Ownership

The user owns exactly one learning-sized package:

```text
Task 2: Go eligibility and input override package
Estimated effort: 1-2 focused days
Files owned by user:
- internal/orchestrator/eligibility.go
- internal/orchestrator/eligibility_test.go
- internal/orchestrator/overrides.go
- internal/orchestrator/overrides_test.go
```

This task is intentionally pure Go and has no Postgres, Redis, HTTP, Docker, or concurrency dependency. It teaches table-driven tests, structs, maps, slices, error reasons, and JSON-shaped pipeline data. The assistant owns every other task and must not edit the user-owned files except to integrate after review.

## File Structure

Create:

- `backend/alembic/versions/018_go_orchestrator_owner.py`: Python-owned migration adding `jobs.orchestrator_owner`.
- `internal/orchestrator/eligibility.go`: user-owned classifier for Go-owned pure ffmpeg jobs.
- `internal/orchestrator/eligibility_test.go`: user-owned eligibility tests.
- `internal/orchestrator/overrides.go`: user-owned Python-compatible job input overrides.
- `internal/orchestrator/overrides_test.go`: user-owned override parity tests.
- `internal/orchestrator/engine.go`: Go job engine orchestration flow.
- `internal/orchestrator/engine_test.go`: unit tests for start, dispatch, complete, fail, retry, skip, finalize.
- `internal/orchestrator/events.go`: Redis event listener for `vp:events:go`.
- `internal/orchestrator/events_test.go`: miniredis tests for event ack/reclaim behavior.
- `internal/orchestrator/recovery.go`: Go-owned startup and periodic recovery.
- `internal/orchestrator/recovery_test.go`: stale node reset and owner guard tests.
- `internal/orchestrator/metrics.go`: Go orchestrator metrics.
- `internal/store/go_jobs.go`: Go-owned job create, batch create, rerun, owner-guarded state updates.
- `internal/store/go_jobs_test.go`: SQL/pgx behavior tests for job owner and node rows.
- `internal/httpapi/go_jobs.go`: Go-owned job write route handlers.
- `tests/go_migration/test_go_orchestrator_phase6.py`: live Docker strict tests.
- `scripts/go_phase6_acceptance.py`: production-style Phase 6 acceptance runner.

Modify:

- `backend/app/models/job.py`: add `orchestrator_owner` model column.
- `backend/app/schemas/job.py`: include `orchestrator_owner` in `JobResponse` and `JobDetailResponse`.
- `backend/app/services/job_runtime.py`: expose Python owner value in API responses.
- `internal/config/config.go`: add Go orchestrator flags.
- `internal/config/config_test.go`: assert fail-closed defaults.
- `internal/store/store.go`: add owner to `JobRow` list responses.
- `internal/store/details.go`: add owner to `JobDetailRow` detail responses.
- `internal/httpapi/router.go`: route job writes to Go-owned handlers when enabled.
- `internal/httpapi/write_responses.go`: add shared no-fallback response helper if not already sufficient.
- `internal/redisstream/streams.go`: make event stream configurable per task/event.
- `internal/redisstream/streams_test.go`: preserve default `vp:events`.
- `internal/worker/consumer.go`: read `event_stream` from task payload.
- `internal/worker/worker.go`: add `EventStream` to task message.
- `cmd/vp-api/main.go`: wire Redis client and orchestrator background lifecycle.
- `docker-compose.yml`: expose Go orchestrator flags for local/staging.
- `docs/go-migration-acceptance/README.md`: add Phase 6 acceptance evidence section.

## Task 1: Schema, Owner Contract, And Config Flags

**Owner:** assistant

**Files:**
- Create: `backend/alembic/versions/018_go_orchestrator_owner.py`
- Modify: `backend/app/models/job.py`
- Modify: `backend/app/schemas/job.py`
- Modify: `backend/app/services/job_runtime.py`
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`
- Modify: `internal/store/store.go`
- Modify: `internal/store/details.go`
- Test: `backend/tests/test_go_orchestrator_owner_schema.py`

- [ ] **Step 1: Write Python schema/model tests**

Create `backend/tests/test_go_orchestrator_owner_schema.py`:

```python
from __future__ import annotations

from app.models.job import Job
from app.schemas.job import JobResponse


def test_job_model_has_orchestrator_owner_column() -> None:
    assert "orchestrator_owner" in Job.__table__.columns
    column = Job.__table__.columns["orchestrator_owner"]
    assert column.default is not None


def test_job_response_exposes_orchestrator_owner() -> None:
    fields = JobResponse.model_fields
    assert "orchestrator_owner" in fields
    assert fields["orchestrator_owner"].default == "python"
```

- [ ] **Step 2: Run the failing Python owner test**

Run:

```bash
cd backend
python3 -m pytest tests/test_go_orchestrator_owner_schema.py -q
```

Expected:

```text
FAIL: orchestrator_owner is not present yet.
```

- [ ] **Step 3: Add Alembic migration**

Create `backend/alembic/versions/018_go_orchestrator_owner.py`:

```python
"""add go orchestrator owner

Revision ID: 018_go_orchestrator_owner
Revises: 017_channel_ops_self_driving
Create Date: 2026-05-20
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "018_go_orchestrator_owner"
down_revision = "017_channel_ops_self_driving"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column(
            "orchestrator_owner",
            sa.String(length=32),
            nullable=False,
            server_default="python",
        ),
    )
    op.create_check_constraint(
        "ck_jobs_orchestrator_owner",
        "jobs",
        "orchestrator_owner IN ('python', 'go')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_jobs_orchestrator_owner", "jobs", type_="check")
    op.drop_column("jobs", "orchestrator_owner")
```

- [ ] **Step 4: Add Python model and response field**

In `backend/app/models/job.py`, add to `Job`:

```python
    orchestrator_owner: Mapped[str] = mapped_column(String(32), default="python", nullable=False)
```

In `backend/app/schemas/job.py`, add to `JobResponse`:

```python
    orchestrator_owner: str = "python"
```

In `backend/app/services/job_runtime.py`, update `to_job_response()`:

```python
        orchestrator_owner=getattr(job, "orchestrator_owner", "python"),
```

- [ ] **Step 5: Add Go config flags**

In `internal/config/config.go`, add fields:

```go
	GoOrchestratorEnabled                 bool
	GoOrchestratorJobWrites               bool
	GoEventStream                         string
	GoOrchestratorRecoveryIntervalSeconds int
	GoOrchestratorStaleNodeSeconds        int
```

Add defaults in `Load()`:

```go
		GoOrchestratorEnabled:                 boolEnv("VP_GO_ORCHESTRATOR_ENABLED", false),
		GoOrchestratorJobWrites:               boolEnv("VP_GO_ORCHESTRATOR_JOB_WRITES", false),
		GoEventStream:                         env("VP_GO_EVENT_STREAM", "vp:events:go"),
		GoOrchestratorRecoveryIntervalSeconds: intEnv("VP_GO_ORCHESTRATOR_RECOVERY_INTERVAL_SECONDS", 60),
		GoOrchestratorStaleNodeSeconds:        intEnv("VP_GO_ORCHESTRATOR_STALE_NODE_SECONDS", 600),
```

- [ ] **Step 6: Add Go config tests**

Append to `internal/config/config_test.go`:

```go
func TestGoOrchestratorFlagsDefaultClosed(t *testing.T) {
	t.Setenv("VP_GO_ORCHESTRATOR_ENABLED", "")
	t.Setenv("VP_GO_ORCHESTRATOR_JOB_WRITES", "")
	t.Setenv("VP_GO_EVENT_STREAM", "")

	cfg := Load()

	if cfg.GoOrchestratorEnabled {
		t.Fatal("GoOrchestratorEnabled must default false")
	}
	if cfg.GoOrchestratorJobWrites {
		t.Fatal("GoOrchestratorJobWrites must default false")
	}
	if cfg.GoEventStream != "vp:events:go" {
		t.Fatalf("GoEventStream = %q", cfg.GoEventStream)
	}
	if cfg.GoOrchestratorRecoveryIntervalSeconds != 60 {
		t.Fatalf("GoOrchestratorRecoveryIntervalSeconds = %d", cfg.GoOrchestratorRecoveryIntervalSeconds)
	}
	if cfg.GoOrchestratorStaleNodeSeconds != 600 {
		t.Fatalf("GoOrchestratorStaleNodeSeconds = %d", cfg.GoOrchestratorStaleNodeSeconds)
	}
}
```

- [ ] **Step 7: Add owner to Go job rows**

In `internal/store/store.go`, add to `JobRow`:

```go
	OrchestratorOwner string     `json:"orchestrator_owner"`
```

Update list query select:

```go
"error_message, submitted_by, retry_count, orchestrator_owner FROM jobs" + where +
```

Update `rows.Scan`:

```go
&row.CompletedAt, &row.ErrorMessage, &row.SubmittedBy, &row.RetryCount, &row.OrchestratorOwner
```

In `internal/store/details.go`, include `orchestrator_owner` in `GetJobDetail` select and scan into `row.OrchestratorOwner`.

- [ ] **Step 8: Run focused tests**

Run:

```bash
cd backend && python3 -m pytest tests/test_go_orchestrator_owner_schema.py -q
go test ./internal/config ./internal/store
```

Expected:

```text
Python schema owner tests pass.
Go config/store tests pass.
```

- [ ] **Step 9: Commit**

Run:

```bash
git add backend/alembic/versions/018_go_orchestrator_owner.py backend/app/models/job.py backend/app/schemas/job.py backend/app/services/job_runtime.py backend/tests/test_go_orchestrator_owner_schema.py internal/config/config.go internal/config/config_test.go internal/store/store.go internal/store/details.go
git commit -m "feat: add go orchestrator ownership contract"
```

## Task 2: Eligibility And Input Overrides

**Owner:** user

**Files:**
- Create: `internal/orchestrator/eligibility.go`
- Create: `internal/orchestrator/eligibility_test.go`
- Create: `internal/orchestrator/overrides.go`
- Create: `internal/orchestrator/overrides_test.go`
- Read: `internal/contracts/pipeline.go`
- Read: `internal/pipeline/validate.go`

- [ ] **Step 1: Write eligibility tests**

Create `internal/orchestrator/eligibility_test.go`:

```go
package orchestrator

import (
	"strings"
	"testing"

	"github.com/Ctwqk/videoprocess/internal/contracts"
)

func TestEligiblePureFFmpegGraph(t *testing.T) {
	def := pipelineDefinition("trim")

	result := ClassifyGoEligibility(def)

	if !result.Eligible {
		t.Fatalf("expected eligible, got reason %q", result.Reason)
	}
}

func TestRejectsUnsupportedNode(t *testing.T) {
	def := pipelineDefinition("smart_trim")

	result := ClassifyGoEligibility(def)

	if result.Eligible {
		t.Fatal("smart_trim must remain Python-owned")
	}
	if !strings.Contains(result.Reason, "smart_trim") {
		t.Fatalf("reason = %q", result.Reason)
	}
}

func TestRejectsSourceWithoutAsset(t *testing.T) {
	def := pipelineDefinition("trim")
	def.Nodes[0].Data.AssetID = nil
	def.Nodes[0].Data.Config = map[string]any{}

	result := ClassifyGoEligibility(def)

	if result.Eligible {
		t.Fatal("source without asset_id must not be eligible")
	}
	if !strings.Contains(result.Reason, "asset_id") {
		t.Fatalf("reason = %q", result.Reason)
	}
}

func TestEveryFirstWaveNodeIsEligible(t *testing.T) {
	for _, nodeType := range FirstWaveGoNodeTypes() {
		t.Run(nodeType, func(t *testing.T) {
			def := pipelineDefinition(nodeType)
			result := ClassifyGoEligibility(def)
			if !result.Eligible {
				t.Fatalf("%s should be eligible: %s", nodeType, result.Reason)
			}
		})
	}
}

func pipelineDefinition(nodeType string) contracts.PipelineDefinition {
	assetID := "00000000-0000-0000-0000-000000000001"
	return contracts.PipelineDefinition{
		Nodes: []contracts.PipelineNode{
			{
				ID:   "source",
				Type: "source",
				Data: contracts.PipelineNodeData{
					Label:   "Source",
					AssetID: &assetID,
					Config:  map[string]any{"asset_id": assetID},
				},
			},
			{
				ID:   "node",
				Type: nodeType,
				Data: contracts.PipelineNodeData{
					Label:  nodeType,
					Config: map[string]any{},
				},
			},
		},
		Edges: []contracts.PipelineEdge{
			{ID: "e1", Source: "source", SourceHandle: "output", Target: "node", TargetHandle: "input"},
		},
		Viewport: map[string]float64{},
	}
}
```

- [ ] **Step 2: Write input override tests**

Create `internal/orchestrator/overrides_test.go`:

```go
package orchestrator

import "testing"

func TestApplyInputOverridesTopLevelAssetID(t *testing.T) {
	def := pipelineDefinition("trim")
	def.Nodes[0].Data.AssetID = nil
	def.Nodes[0].Data.Config = map[string]any{}

	got := ApplyInputOverrides(def, map[string]any{"asset_id": "asset-1"})

	if got.Nodes[0].Data.Config["asset_id"] != "asset-1" {
		t.Fatalf("config asset_id = %#v", got.Nodes[0].Data.Config["asset_id"])
	}
	if got.Nodes[0].Data.AssetID == nil || *got.Nodes[0].Data.AssetID != "asset-1" {
		t.Fatalf("data asset_id = %#v", got.Nodes[0].Data.AssetID)
	}
}

func TestApplyInputOverridesDottedPath(t *testing.T) {
	def := pipelineDefinition("trim")

	got := ApplyInputOverrides(def, map[string]any{"node.start_time": "00:00:01"})

	if got.Nodes[1].Data.Config["start_time"] != "00:00:01" {
		t.Fatalf("start_time = %#v", got.Nodes[1].Data.Config["start_time"])
	}
}

func TestApplyInputOverridesNestedNodeMap(t *testing.T) {
	def := pipelineDefinition("trim")

	got := ApplyInputOverrides(def, map[string]any{"node": map[string]any{"duration": 1.5}})

	if got.Nodes[1].Data.Config["duration"] != 1.5 {
		t.Fatalf("duration = %#v", got.Nodes[1].Data.Config["duration"])
	}
}

func TestApplyInputOverridesNestedDottedValue(t *testing.T) {
	def := pipelineDefinition("trim")

	got := ApplyInputOverrides(def, map[string]any{"node.render.filename": "out.mp4"})

	render, ok := got.Nodes[1].Data.Config["render"].(map[string]any)
	if !ok {
		t.Fatalf("render config = %#v", got.Nodes[1].Data.Config["render"])
	}
	if render["filename"] != "out.mp4" {
		t.Fatalf("filename = %#v", render["filename"])
	}
}
```

- [ ] **Step 3: Run tests to verify failure**

Run:

```bash
go test ./internal/orchestrator -run 'TestEligible|TestRejects|TestEveryFirstWave|TestApplyInputOverrides' -count=1
```

Expected:

```text
FAIL with undefined ClassifyGoEligibility, FirstWaveGoNodeTypes, or ApplyInputOverrides.
```

- [ ] **Step 4: Implement eligibility classifier**

Create `internal/orchestrator/eligibility.go`:

```go
package orchestrator

import (
	"fmt"
	"sort"

	"github.com/Ctwqk/videoprocess/internal/contracts"
	"github.com/Ctwqk/videoprocess/internal/pipeline"
)

type EligibilityResult struct {
	Eligible bool
	Reason   string
}

var firstWaveGoNodes = map[string]struct{}{
	"trim":                     {},
	"transcode":                {},
	"export":                   {},
	"vertical_crop":            {},
	"watermark":                {},
	"title_overlay":            {},
	"bgm":                      {},
	"replace_audio":            {},
	"concat_horizontal":        {},
	"concat_vertical":          {},
	"concat_many":              {},
	"concat_timeline":          {},
	"concat_vertical_timeline": {},
	"montage_assembler":        {},
}

func FirstWaveGoNodeTypes() []string {
	items := make([]string, 0, len(firstWaveGoNodes))
	for nodeType := range firstWaveGoNodes {
		items = append(items, nodeType)
	}
	sort.Strings(items)
	return items
}

func ClassifyGoEligibility(def contracts.PipelineDefinition) EligibilityResult {
	validation := pipeline.Validate(def)
	if !validation.Valid {
		if len(validation.Errors) > 0 {
			return EligibilityResult{Eligible: false, Reason: validation.Errors[0].Message}
		}
		return EligibilityResult{Eligible: false, Reason: "pipeline validation failed"}
	}
	for _, node := range def.Nodes {
		if node.Type == "source" {
			if !sourceNodeHasAsset(node) {
				return EligibilityResult{Eligible: false, Reason: fmt.Sprintf("source node %q is missing asset_id", node.ID)}
			}
			continue
		}
		if _, ok := firstWaveGoNodes[node.Type]; !ok {
			return EligibilityResult{Eligible: false, Reason: fmt.Sprintf("node type %q remains Python-owned", node.Type)}
		}
	}
	return EligibilityResult{Eligible: true}
}

func sourceNodeHasAsset(node contracts.PipelineNode) bool {
	if node.Data.AssetID != nil && *node.Data.AssetID != "" {
		return true
	}
	if node.Data.Config == nil {
		return false
	}
	raw, ok := node.Data.Config["asset_id"]
	if !ok {
		return false
	}
	value, ok := raw.(string)
	return ok && value != ""
}
```

- [ ] **Step 5: Implement input overrides**

Create `internal/orchestrator/overrides.go`:

```go
package orchestrator

import (
	"strings"

	"github.com/Ctwqk/videoprocess/internal/contracts"
)

func ApplyInputOverrides(def contracts.PipelineDefinition, overrides map[string]any) contracts.PipelineDefinition {
	if len(overrides) == 0 {
		return def
	}
	out := def
	nodeOverrides := normalizeNodeOverrides(overrides)
	topLevelAssetApplied := false
	for idx := range out.Nodes {
		node := &out.Nodes[idx]
		config := copyMap(node.Data.Config)
		if override, ok := nodeOverrides[node.ID]; ok {
			mergeOverride(config, override)
		}
		if node.Type == "source" && !topLevelAssetApplied {
			if raw, ok := overrides["asset_id"]; ok {
				if value, ok := raw.(string); ok && value != "" {
					config["asset_id"] = value
					node.Data.AssetID = &value
					topLevelAssetApplied = true
				}
			}
		}
		if raw, ok := config["asset_id"]; ok {
			if value, ok := raw.(string); ok && value != "" {
				node.Data.AssetID = &value
			}
		}
		node.Data.Config = config
	}
	return out
}

func normalizeNodeOverrides(overrides map[string]any) map[string]map[string]any {
	result := map[string]map[string]any{}
	for key, value := range overrides {
		if key == "asset_id" {
			continue
		}
		if strings.Contains(key, ".") {
			parts := strings.SplitN(key, ".", 2)
			bucket := ensureOverrideBucket(result, parts[0])
			setNested(bucket, parts[1], value)
			continue
		}
		if nested, ok := value.(map[string]any); ok {
			bucket := ensureOverrideBucket(result, key)
			mergeOverride(bucket, nested)
			continue
		}
		bucket := ensureOverrideBucket(result, key)
		bucket["asset_id"] = value
	}
	return result
}

func ensureOverrideBucket(target map[string]map[string]any, nodeID string) map[string]any {
	if target[nodeID] == nil {
		target[nodeID] = map[string]any{}
	}
	return target[nodeID]
}

func copyMap(src map[string]any) map[string]any {
	dst := map[string]any{}
	for key, value := range src {
		if nested, ok := value.(map[string]any); ok {
			dst[key] = copyMap(nested)
		} else {
			dst[key] = value
		}
	}
	return dst
}

func mergeOverride(target map[string]any, override map[string]any) {
	for key, value := range override {
		if nested, ok := value.(map[string]any); ok && !strings.Contains(key, ".") {
			existing, _ := target[key].(map[string]any)
			child := copyMap(existing)
			mergeOverride(child, nested)
			target[key] = child
			continue
		}
		setNested(target, key, value)
	}
}

func setNested(target map[string]any, path string, value any) {
	if !strings.Contains(path, ".") {
		target[path] = value
		return
	}
	parts := strings.Split(path, ".")
	current := target
	for _, part := range parts[:len(parts)-1] {
		next, ok := current[part].(map[string]any)
		if !ok {
			next = map[string]any{}
			current[part] = next
		}
		current = next
	}
	current[parts[len(parts)-1]] = value
}
```

- [ ] **Step 6: Run user-owned package tests**

Run:

```bash
go test ./internal/orchestrator -run 'TestEligible|TestRejects|TestEveryFirstWave|TestApplyInputOverrides' -count=1
```

Expected:

```text
PASS
```

- [ ] **Step 7: Commit user task**

Run:

```bash
git add internal/orchestrator/eligibility.go internal/orchestrator/eligibility_test.go internal/orchestrator/overrides.go internal/orchestrator/overrides_test.go
git commit -m "feat: classify go orchestrator eligible pipelines"
```

## Task 3: Redis Event Stream Selection

**Owner:** assistant

**Files:**
- Modify: `internal/redisstream/streams.go`
- Modify: `internal/redisstream/streams_test.go`
- Modify: `internal/worker/worker.go`
- Modify: `internal/worker/consumer.go`
- Modify: `internal/worker/consumer_test.go`

- [ ] **Step 1: Add event stream tests**

Append to `internal/redisstream/streams_test.go`:

```go
func TestPublishNodeCompletedUsesExplicitEventStream(t *testing.T) {
	event := NodeEvent{EventStream: "vp:events:go", JobID: "job", NodeExecutionID: "node", OutputArtifactID: "artifact"}
	if event.streamOrDefault() != "vp:events:go" {
		t.Fatalf("streamOrDefault = %q", event.streamOrDefault())
	}
}

func TestPublishNodeCompletedDefaultsToPythonEventStream(t *testing.T) {
	event := NodeEvent{JobID: "job", NodeExecutionID: "node", OutputArtifactID: "artifact"}
	if event.streamOrDefault() != EventStream {
		t.Fatalf("streamOrDefault = %q", event.streamOrDefault())
	}
}
```

- [ ] **Step 2: Run failing stream tests**

Run:

```bash
go test ./internal/redisstream -run TestPublishNodeCompleted -count=1
```

Expected:

```text
FAIL with missing EventStream field or streamOrDefault method.
```

- [ ] **Step 3: Add event stream to NodeEvent**

Update `internal/redisstream/streams.go`:

```go
type NodeEvent struct {
	EventStream      string
	Event            string
	JobID            string
	NodeExecutionID  string
	OutputArtifactID string
	Error            string
}

func (e NodeEvent) streamOrDefault() string {
	if e.EventStream != "" {
		return e.EventStream
	}
	return EventStream
}
```

Change both `PublishNodeCompleted` and `PublishNodeFailed`:

```go
Stream: event.streamOrDefault(),
```

- [ ] **Step 4: Add task event stream field**

In `internal/worker/worker.go`, add to `TaskMessage`:

```go
	EventStream       string
	OrchestratorOwner string
```

In the task decode path in `internal/worker/consumer.go`, populate:

```go
EventStream:       values["event_stream"],
OrchestratorOwner: values["orchestrator_owner"],
```

When publishing completion/failure, pass:

```go
EventStream: task.EventStream,
```

- [ ] **Step 5: Add worker consumer test**

In `internal/worker/consumer_test.go`, add a test that submits a task with `event_stream=vp:events:go`, runs a successful handler, and asserts the event appears in `vp:events:go` and not `vp:events`.

Use the existing miniredis setup in the file and assert:

```go
events, err := redisClient.XRange(ctx, "vp:events:go", "-", "+").Result()
if err != nil {
	t.Fatal(err)
}
if len(events) != 1 {
	t.Fatalf("go event stream entries = %d", len(events))
}
pythonEvents, err := redisClient.XRange(ctx, redisstream.EventStream, "-", "+").Result()
if err != nil {
	t.Fatal(err)
}
if len(pythonEvents) != 0 {
	t.Fatalf("python event stream entries = %d", len(pythonEvents))
}
```

- [ ] **Step 6: Run worker stream tests**

Run:

```bash
go test ./internal/redisstream ./internal/worker -count=1
```

Expected:

```text
PASS
```

- [ ] **Step 7: Commit**

Run:

```bash
git add internal/redisstream/streams.go internal/redisstream/streams_test.go internal/worker/worker.go internal/worker/consumer.go internal/worker/consumer_test.go
git commit -m "feat: route go-owned worker events to go stream"
```

## Task 4: Go-Owned Job Store Methods

**Owner:** assistant

**Files:**
- Create: `internal/store/go_jobs.go`
- Create: `internal/store/go_jobs_test.go`
- Modify: `internal/store/errors.go`

- [ ] **Step 1: Define store tests for owner guarded writes**

Create `internal/store/go_jobs_test.go` with tests using the existing store test helpers. Required cases:

```go
func TestCreateGoJobStoresOwnerAndNodes(t *testing.T) {
	// Create a pipeline row, call CreateGoJob, assert:
	// row.OrchestratorOwner == "go"
	// len(row.NodeExecutions) == len(def.Nodes)
	// every node status == "PENDING"
}

func TestOwnerGuardedUpdateRejectsPythonOwnedJob(t *testing.T) {
	// Insert a Python-owned job and node, call MarkGoNodeSucceeded,
	// assert ErrNotFound or ErrConflict and unchanged status.
}

func TestCreateGoJobBatchIsAllOrNothing(t *testing.T) {
	// Pass two inputs where the second fails validation,
	// assert no Go-owned jobs were inserted.
}
```

- [ ] **Step 2: Run failing store tests**

Run:

```bash
go test ./internal/store -run 'TestCreateGoJob|TestOwnerGuarded|TestCreateGoJobBatch' -count=1
```

Expected:

```text
FAIL with missing CreateGoJob or owner guarded methods.
```

- [ ] **Step 3: Implement Go job store input types**

Create `internal/store/go_jobs.go`:

```go
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Ctwqk/videoprocess/internal/contracts"
	"github.com/jackc/pgx/v5"
)

type GoJobCreateInput struct {
	PipelineID       string
	PipelineSnapshot contracts.PipelineDefinition
	SubmittedBy      string
}

type GoNodeExecutionInput struct {
	NodeID     string
	NodeType   string
	NodeLabel  string
	NodeConfig map[string]any
}
```

- [ ] **Step 4: Implement transaction create method**

Add:

```go
func (s *Store) CreateGoJob(ctx context.Context, in GoJobCreateInput) (JobDetailRow, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return JobDetailRow{}, err
	}
	defer tx.Rollback(ctx)

	snapshot, err := json.Marshal(in.PipelineSnapshot)
	if err != nil {
		return JobDetailRow{}, err
	}
	submittedBy := in.SubmittedBy
	if submittedBy == "" {
		submittedBy = "system"
	}

	var jobID [16]byte
	err = tx.QueryRow(ctx, `
		INSERT INTO jobs (pipeline_id, pipeline_snapshot, status, submitted_by, orchestrator_owner)
		VALUES ($1, $2, 'PENDING', $3, 'go')
		RETURNING id
	`, in.PipelineID, snapshot, submittedBy).Scan(&jobID)
	if err != nil {
		return JobDetailRow{}, err
	}
	jobIDStr := uuidString(jobID)

	for _, node := range in.PipelineSnapshot.Nodes {
		config := node.Data.Config
		if config == nil {
			config = map[string]any{}
		}
		if node.Type == "source" && node.Data.AssetID != nil && config["asset_id"] == nil {
			config = copyAnyMap(config)
			config["asset_id"] = *node.Data.AssetID
		}
		label := node.Data.Label
		if label == "" {
			label = node.Type
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO node_executions (job_id, node_id, node_type, node_label, node_config, status)
			VALUES ($1, $2, $3, $4, $5, 'PENDING')
		`, jobIDStr, node.ID, node.Type, label, config); err != nil {
			return JobDetailRow{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return JobDetailRow{}, err
	}
	return s.GetJobDetail(ctx, jobIDStr)
}
```

Also add:

```go
func copyAnyMap(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}
```

- [ ] **Step 5: Implement owner guarded update methods**

Add methods used by the engine:

```go
func (s *Store) LoadGoJobForUpdate(ctx context.Context, jobID string) (JobDetailRow, error)
func (s *Store) MarkGoJobPlanning(ctx context.Context, jobID string, executionPlan map[string]any) error
func (s *Store) MarkGoJobRunning(ctx context.Context, jobID string) error
func (s *Store) MarkGoNodeQueued(ctx context.Context, nodeExecutionID string, inputArtifactIDs []string) error
func (s *Store) MarkGoNodeSucceeded(ctx context.Context, jobID string, nodeExecutionID string, outputArtifactID string) error
func (s *Store) MarkGoNodeFailed(ctx context.Context, jobID string, nodeExecutionID string, errorMessage string) error
func (s *Store) IncrementGoNodeRetry(ctx context.Context, jobID string, nodeExecutionID string) error
func (s *Store) SkipGoDownstreamNodes(ctx context.Context, jobID string, nodeIDs []string) error
func (s *Store) FinalizeGoJob(ctx context.Context, jobID string, status string, errorMessage *string, finalArtifactNodeIDs []string) error
```

Node update queries must include this owner guard pattern:

```sql
WHERE id = $1
  AND EXISTS (
  SELECT 1 FROM jobs j
  WHERE j.id = node_executions.job_id
    AND j.orchestrator_owner = 'go'
)
```

Job update queries must include:

```sql
WHERE id = $1 AND orchestrator_owner = 'go'
```

If `RowsAffected()==0`, return `ErrNotFound` or `ErrConflict`.

- [ ] **Step 6: Run store tests**

Run:

```bash
go test ./internal/store -count=1
```

Expected:

```text
PASS
```

- [ ] **Step 7: Commit**

Run:

```bash
git add internal/store/go_jobs.go internal/store/go_jobs_test.go internal/store/errors.go
git commit -m "feat: add go-owned job store methods"
```

## Task 5: Go Orchestrator Engine

**Owner:** assistant

**Files:**
- Create: `internal/orchestrator/engine.go`
- Create: `internal/orchestrator/engine_test.go`
- Modify: `internal/orchestrator/dispatch.go`
- Modify: `internal/orchestrator/dag.go`

- [ ] **Step 1: Write engine unit tests**

Create `internal/orchestrator/engine_test.go` with in-memory fakes for store and dispatcher. Tests must cover:

```go
func TestStartJobDispatchesReadyNodeWithGoEventStream(t *testing.T)
func TestNodeCompletedDispatchesDownstreamOnce(t *testing.T)
func TestNodeFailedRetriesOnce(t *testing.T)
func TestNodeFailedSkipsDownstreamAfterRetryExhausted(t *testing.T)
func TestFinalizeMarksSuccessfulLeafArtifacts(t *testing.T)
func TestCancelledJobCompletionIsIgnored(t *testing.T)
```

Each fake dispatch assertion must verify:

```go
payload.RedisValues()["event_stream"] == "vp:events:go"
payload.RedisValues()["orchestrator_owner"] == "go"
```

- [ ] **Step 2: Run failing engine tests**

Run:

```bash
go test ./internal/orchestrator -run 'TestStartJob|TestNodeCompleted|TestNodeFailed|TestFinalize|TestCancelled' -count=1
```

Expected:

```text
FAIL with missing Engine type or methods.
```

- [ ] **Step 3: Extend dispatch payload**

Modify `internal/orchestrator/dispatch.go`:

```go
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
	EventStream        string
	OrchestratorOwner  string
}
```

Add to `RedisValues()`:

```go
"event_stream":        p.EventStream,
"orchestrator_owner":  p.OrchestratorOwner,
```

Update `internal/orchestrator/dag_test.go` to assert the new keys.

- [ ] **Step 4: Implement engine interfaces**

Create `internal/orchestrator/engine.go`:

```go
package orchestrator

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/Ctwqk/videoprocess/internal/contracts"
)

type EngineStore interface {
	GetJobDetail(ctx context.Context, id string) (JobView, error)
	CreateSourceArtifact(ctx context.Context, jobID string, nodeExecutionID string, assetID string) (string, error)
	MarkGoJobPlanning(ctx context.Context, jobID string, executionPlan map[string]any) error
	MarkGoJobRunning(ctx context.Context, jobID string) error
	MarkGoNodeQueued(ctx context.Context, nodeExecutionID string, inputArtifactIDs []string) error
	MarkGoNodeSucceeded(ctx context.Context, jobID string, nodeExecutionID string, outputArtifactID string) error
	MarkGoNodeFailed(ctx context.Context, jobID string, nodeExecutionID string, errorMessage string) error
	IncrementGoNodeRetry(ctx context.Context, jobID string, nodeExecutionID string) error
	SkipGoDownstreamNodes(ctx context.Context, jobID string, nodeIDs []string) error
	FinalizeGoJob(ctx context.Context, jobID string, status string, errorMessage *string, finalArtifactNodeIDs []string) error
}

type Dispatcher interface {
	Dispatch(ctx context.Context, workerType string, payload TaskPayload) error
}

type Engine struct {
	Store       EngineStore
	Dispatcher  Dispatcher
	EventStream string
	Clock       func() time.Time
	Logger      *slog.Logger
}
```

Define compact engine-specific view structs in the same file or convert from store rows in adapter code:

```go
type JobView struct {
	ID                string
	Status            string
	OrchestratorOwner string
	PipelineSnapshot  contracts.PipelineDefinition
	ExecutionPlan     map[string]any
	Nodes             []NodeExecutionView
}

type NodeExecutionView struct {
	ID                string
	NodeID            string
	NodeType          string
	NodeLabel         string
	Status            string
	RetryCount        int
	NodeConfig        map[string]any
	OutputArtifactID  string
	InputArtifactIDs  []string
}
```

- [ ] **Step 5: Implement start and dispatch**

Implement:

```go
func (e *Engine) StartJob(ctx context.Context, jobID string) error
func (e *Engine) dispatchReadyNodes(ctx context.Context, job JobView, depMap map[string][]string) error
func (e *Engine) resolveSourceNodes(ctx context.Context, job JobView) error
```

Rules:

- Require `job.OrchestratorOwner == "go"`.
- Ignore terminal jobs.
- Compute `topo_order` and dependencies using existing `DependencyMap`.
- Source nodes with asset config create intermediate artifacts and become succeeded through store method.
- Non-source ready nodes dispatch to worker type `ffmpeg_go`.
- Payload includes `EventStream: e.EventStream` and `OrchestratorOwner: "go"`.

- [ ] **Step 6: Implement event handlers**

Implement:

```go
func (e *Engine) OnNodeCompleted(ctx context.Context, jobID string, nodeExecutionID string, outputArtifactID string) error
func (e *Engine) OnNodeFailed(ctx context.Context, jobID string, nodeExecutionID string, errorMessage string) error
```

Rules:

- Cancelled jobs: return nil without mutation.
- Completion marks node succeeded, then dispatches downstream or finalizes.
- Failure retries once. After retry exhaustion, mark failed, skip downstream, finalize when no active nodes remain.
- Duplicate completion for already terminal node returns nil.

- [ ] **Step 7: Run engine tests**

Run:

```bash
go test ./internal/orchestrator -count=1
```

Expected:

```text
PASS
```

- [ ] **Step 8: Commit**

Run:

```bash
git add internal/orchestrator/engine.go internal/orchestrator/engine_test.go internal/orchestrator/dispatch.go internal/orchestrator/dag.go internal/orchestrator/dag_test.go
git commit -m "feat: add go orchestrator engine"
```

## Task 6: Go Event Listener And Recovery

**Owner:** assistant

**Files:**
- Create: `internal/orchestrator/events.go`
- Create: `internal/orchestrator/events_test.go`
- Create: `internal/orchestrator/recovery.go`
- Create: `internal/orchestrator/recovery_test.go`
- Create: `internal/orchestrator/metrics.go`

- [ ] **Step 1: Write event listener tests**

Create tests using miniredis:

```go
func TestEventListenerHandlesCompletionAndAcks(t *testing.T)
func TestEventListenerDoesNotAckWhenEngineFails(t *testing.T)
func TestEventListenerReclaimsPendingEvents(t *testing.T)
func TestEventListenerAcksPythonOwnedEventAfterGuard(t *testing.T)
```

Use fake engine methods that record calls. Assert:

```go
pending, err := redisClient.XPending(ctx, "vp:events:go", "orchestrator-go").Result()
if err != nil {
	t.Fatal(err)
}
if pending.Count != 0 {
	t.Fatalf("pending count = %d", pending.Count)
}
```

- [ ] **Step 2: Implement event listener**

Create `internal/orchestrator/events.go`:

```go
type EventListener struct {
	Client        *redis.Client
	Engine        *Engine
	Stream        string
	Group         string
	Consumer      string
	ReclaimMinIdle time.Duration
	Logger        *slog.Logger
}

func (l *EventListener) EnsureGroup(ctx context.Context) error
func (l *EventListener) Run(ctx context.Context) error
func (l *EventListener) reclaim(ctx context.Context) error
func (l *EventListener) handle(ctx context.Context, msg redis.XMessage) error
```

Use:

```go
XGroupCreateMkStream(ctx, l.Stream, l.Group, "0")
XAutoClaim(ctx, &redis.XAutoClaimArgs{Stream: l.Stream, Group: l.Group, Consumer: l.Consumer, MinIdle: l.ReclaimMinIdle, Start: "0-0", Count: 100})
XReadGroup(ctx, &redis.XReadGroupArgs{Group: l.Group, Consumer: l.Consumer, Streams: []string{l.Stream, ">"}, Count: 10, Block: 5 * time.Second})
XAck(ctx, l.Stream, l.Group, msg.ID)
```

- [ ] **Step 3: Write recovery tests**

Create `internal/orchestrator/recovery_test.go`:

```go
func TestRecoveryStartsPendingGoOwnedJobs(t *testing.T)
func TestRecoveryDoesNotTouchPythonOwnedJobs(t *testing.T)
func TestRecoveryResetsStaleQueuedNode(t *testing.T)
func TestRecoveryFinalizesTerminalJobInsteadOfDispatching(t *testing.T)
```

- [ ] **Step 4: Implement recovery**

Create `internal/orchestrator/recovery.go`:

```go
type RecoveryStore interface {
	ListRecoverableGoJobs(ctx context.Context) ([]JobView, error)
	ResetStaleGoNodes(ctx context.Context, jobID string, staleBefore time.Time) error
}

type RecoveryRunner struct {
	Store            RecoveryStore
	Engine           *Engine
	Interval         time.Duration
	StaleNodeAge     time.Duration
	Clock            func() time.Time
	Logger           *slog.Logger
}

func (r *RecoveryRunner) RunOnce(ctx context.Context) error
func (r *RecoveryRunner) Run(ctx context.Context) error
```

`RunOnce` resets stale nodes and calls `Engine.StartJob` for recoverable jobs.

- [ ] **Step 5: Add orchestrator metrics**

Create `internal/orchestrator/metrics.go` with counters:

```go
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
```

Use labels `result`, `event`, and `status` only where needed to avoid high cardinality.

- [ ] **Step 6: Run listener and recovery tests**

Run:

```bash
go test ./internal/orchestrator -count=1
```

Expected:

```text
PASS
```

- [ ] **Step 7: Commit**

Run:

```bash
git add internal/orchestrator/events.go internal/orchestrator/events_test.go internal/orchestrator/recovery.go internal/orchestrator/recovery_test.go internal/orchestrator/metrics.go
git commit -m "feat: add go orchestrator listener and recovery"
```

## Task 7: Go Job HTTP Routes

**Owner:** assistant

**Files:**
- Create: `internal/httpapi/go_jobs.go`
- Modify: `internal/httpapi/job_writes.go`
- Modify: `internal/httpapi/router.go`
- Modify: `internal/httpapi/httpapi_test.go`
- Modify: `cmd/vp-api/main.go`

- [ ] **Step 1: Write route tests**

Add tests to `internal/httpapi/httpapi_test.go`:

```go
func TestCreateJobRejectedWhenGoWritesDisabled(t *testing.T)
func TestCreateJobRejectsNonEligiblePipelineWithoutCreatingJob(t *testing.T)
func TestCreateJobDelegatesEligiblePipelineToGoJobService(t *testing.T)
func TestCreateJobBatchIsAllOrNothing(t *testing.T)
func TestRerunRejectsPythonOwnedJob(t *testing.T)
```

Expected response for disabled writes:

```json
{"detail":"Go orchestrator job writes are disabled"}
```

Expected response for non-eligible:

```json
{"detail":"job orchestration for this pipeline remains Python-owned: node type \"smart_trim\" remains Python-owned"}
```

- [ ] **Step 2: Add HTTP service interface**

Create `internal/httpapi/go_jobs.go`:

```go
type GoJobService interface {
	CreateJob(ctx context.Context, pipelineID string, inputs map[string]any) (store.JobDetailRow, error)
	CreateJobBatch(ctx context.Context, pipelineID string, inputs []map[string]any) ([]store.JobDetailRow, error)
	RerunJob(ctx context.Context, jobID string) (store.JobDetailRow, error)
}
```

Add to `Server`:

```go
	goJobsEnabled bool
	goJobs        GoJobService
```

Add to `ServerOptions`:

```go
	GoJobsEnabled bool
	GoJobs        GoJobService
```

- [ ] **Step 3: Implement route handlers**

Replace current `createJob`, `createJobBatch`, and `rerunJob` behavior:

```go
func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	if !s.goJobsEnabled || s.goJobs == nil {
		unsupportedWrite(w, "Go orchestrator job writes are disabled")
		return
	}
	var body struct {
		PipelineID string         `json:"pipeline_id"`
		Inputs     map[string]any `json:"inputs"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		badRequest(w, "Invalid request body")
		return
	}
	row, err := s.goJobs.CreateJob(r.Context(), body.PipelineID, body.Inputs)
	if err != nil {
		writeGoJobError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, row)
}
```

Implement batch and rerun with the same service boundary.

- [ ] **Step 4: Wire service in cmd/vp-api**

In `cmd/vp-api/main.go`:

- Create Redis client for orchestrator when enabled.
- Create `orchestrator.Engine`.
- Start `EventListener.Run` and `RecoveryRunner.Run` goroutines when `cfg.GoOrchestratorEnabled`.
- Pass job service to `httpapi.NewServerWithOptions`.
- If `cfg.GoOrchestratorJobWrites` is true while orchestrator is disabled, keep routes fail-closed.

- [ ] **Step 5: Run HTTP tests**

Run:

```bash
go test ./internal/httpapi ./cmd/vp-api -count=1
```

Expected:

```text
PASS
```

- [ ] **Step 6: Commit**

Run:

```bash
git add internal/httpapi/go_jobs.go internal/httpapi/job_writes.go internal/httpapi/router.go internal/httpapi/httpapi_test.go cmd/vp-api/main.go
git commit -m "feat: expose go-owned job routes"
```

## Task 8: Docker And Live Phase 6 Tests

**Owner:** assistant

**Files:**
- Modify: `docker-compose.yml`
- Create: `tests/go_migration/test_go_orchestrator_phase6.py`

- [ ] **Step 1: Enable flags in Docker Compose for Go API**

In `docker-compose.yml`, under `api-go.environment`, add:

```yaml
      VP_GO_ORCHESTRATOR_ENABLED: ${VP_GO_ORCHESTRATOR_ENABLED:-true}
      VP_GO_ORCHESTRATOR_JOB_WRITES: ${VP_GO_ORCHESTRATOR_JOB_WRITES:-true}
      VP_GO_EVENT_STREAM: ${VP_GO_EVENT_STREAM:-vp:events:go}
      VP_GO_ORCHESTRATOR_RECOVERY_INTERVAL_SECONDS: ${VP_GO_ORCHESTRATOR_RECOVERY_INTERVAL_SECONDS:-60}
      VP_GO_ORCHESTRATOR_STALE_NODE_SECONDS: ${VP_GO_ORCHESTRATOR_STALE_NODE_SECONDS:-600}
```

- [ ] **Step 2: Write live Phase 6 tests**

Create `tests/go_migration/test_go_orchestrator_phase6.py`:

```python
from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest
import requests


STRICT = os.getenv("VP_GO_PHASE6_STRICT", "").lower() in {"1", "true", "yes", "on"}
GO_API = os.getenv("VP_GO_API_URL", "http://127.0.0.1:18081")
PY_API = os.getenv("VP_PYTHON_API", "http://127.0.0.1:18080")
REDIS_URL = os.getenv("VP_REDIS_URL", "redis://127.0.0.1:6380/0")


@pytest.mark.skipif(not STRICT, reason="set VP_GO_PHASE6_STRICT=1 for live Go orchestrator tests")
def test_go_api_creates_and_completes_go_owned_job(tmp_path: Path) -> None:
    asset_id = upload_video_asset(tmp_path)
    pipeline_id = create_go_pipeline(asset_id)
    created = requests.post(f"{GO_API}/api/v1/jobs", json={"pipeline_id": pipeline_id, "inputs": {}}, timeout=20)
    assert created.status_code == 201, created.text
    body = created.json()
    assert body["orchestrator_owner"] == "go"
    job_id = body["id"]

    terminal = wait_for_terminal_job(GO_API, job_id)
    assert terminal["status"] == "SUCCEEDED", terminal
    worker_nodes = [n for n in terminal["node_executions"] if n["node_type"] != "source"]
    assert worker_nodes
    assert all("ffmpeg_go-worker@" in (n["worker_id"] or "") for n in worker_nodes)
    assert any(n["output_artifact_id"] for n in terminal["node_executions"])

    python_view = requests.get(f"{PY_API}/api/v1/jobs/{job_id}", timeout=10)
    assert python_view.status_code == 200
    assert python_view.json()["status"] == terminal["status"]

    assert pending_count("vp:events:go", "orchestrator-go") == 0
    assert pending_count("vp:tasks:ffmpeg_go", "ffmpeg_go-workers") == 0


@pytest.mark.skipif(not STRICT, reason="set VP_GO_PHASE6_STRICT=1 for live Go orchestrator tests")
def test_non_eligible_pipeline_rejected_without_job(tmp_path: Path) -> None:
    asset_id = upload_video_asset(tmp_path)
    pipeline_id = create_pipeline(asset_id, "smart_trim")
    before = job_count()
    response = requests.post(f"{GO_API}/api/v1/jobs", json={"pipeline_id": pipeline_id, "inputs": {}}, timeout=20)
    assert response.status_code == 501, response.text
    assert "Python-owned" in response.json()["detail"]
    assert job_count() == before


def pending_count(stream: str, group: str) -> int:
    output = subprocess.check_output(["redis-cli", "-u", REDIS_URL, "XPENDING", stream, group], text=True)
    return int(output.splitlines()[0].strip())
```

Also include helper functions copied from `tests/go_migration/test_go_worker_nodes.py` for asset upload and pipeline definition, adjusted so job creation uses `GO_API`.

- [ ] **Step 3: Run Docker rebuild**

Run:

```bash
docker compose up -d --build api api-go ffmpeg-worker-go
curl -fsS http://127.0.0.1:18081/readyz
```

Expected:

```text
Go API readyz returns status ready.
```

- [ ] **Step 4: Run live Phase 6 tests**

Run:

```bash
VP_GO_PHASE6_STRICT=1 VP_GO_API_URL=http://127.0.0.1:18081 VP_PYTHON_API=http://127.0.0.1:18080 VP_REDIS_URL=redis://127.0.0.1:6380/0 python3 -m pytest tests/go_migration/test_go_orchestrator_phase6.py -q
```

Expected:

```text
2 passed
```

- [ ] **Step 5: Commit**

Run:

```bash
git add docker-compose.yml tests/go_migration/test_go_orchestrator_phase6.py
git commit -m "test: verify go orchestrator phase 6 live path"
```

## Task 9: Phase 6 Acceptance Runner And Evidence

**Owner:** assistant

**Files:**
- Create: `scripts/go_phase6_acceptance.py`
- Modify: `docs/go-migration-acceptance/README.md`

- [ ] **Step 1: Create acceptance runner**

Create `scripts/go_phase6_acceptance.py` that:

- uploads one generated video asset through Python API or Go asset API;
- creates a representative pure ffmpeg pipeline through Go API;
- creates jobs only through `api-go POST /api/v1/jobs`;
- waits for terminal success;
- checks `orchestrator_owner == "go"`;
- checks worker ids contain `ffmpeg_go-worker@`;
- checks final artifact download succeeds;
- checks `XPENDING vp:events:go orchestrator-go == 0`;
- checks `XPENDING vp:tasks:ffmpeg_go ffmpeg_go-workers == 0`;
- creates one non-eligible pipeline and proves Go API rejects it without creating a job.

Runner output must be JSON:

```json
{
  "jobs_completed": 20,
  "go_event_pending": 0,
  "go_task_pending": 0,
  "wrong_owner": 0,
  "wrong_worker": 0,
  "missing_final_artifact": 0,
  "non_eligible_rejected": true
}
```

- [ ] **Step 2: Run acceptance smoke**

Run:

```bash
python3 scripts/go_phase6_acceptance.py --help
python3 -m py_compile scripts/go_phase6_acceptance.py
python3 scripts/go_phase6_acceptance.py --api-go-url http://127.0.0.1:18081 --python-api-url http://127.0.0.1:18080 --redis-url redis://127.0.0.1:6380/0 --count 1
```

Expected:

```text
help prints options.
py_compile passes.
count=1 emits JSON with jobs_completed=1 and pending counts 0.
```

- [ ] **Step 3: Run production-style acceptance**

Run:

```bash
python3 scripts/go_phase6_acceptance.py --api-go-url http://127.0.0.1:18081 --python-api-url http://127.0.0.1:18080 --redis-url redis://127.0.0.1:6380/0 --count 20
```

Expected:

```text
jobs_completed=20
go_event_pending=0
go_task_pending=0
wrong_owner=0
wrong_worker=0
missing_final_artifact=0
non_eligible_rejected=true
```

- [ ] **Step 4: Update evidence doc**

Append to `docs/go-migration-acceptance/README.md`:

```markdown
## Phase 6 Go Orchestrator Acceptance

Commands:

```bash
docker compose up -d --build api api-go ffmpeg-worker-go
VP_GO_PHASE6_STRICT=1 VP_GO_API_URL=http://127.0.0.1:18081 VP_PYTHON_API=http://127.0.0.1:18080 VP_REDIS_URL=redis://127.0.0.1:6380/0 python3 -m pytest tests/go_migration/test_go_orchestrator_phase6.py -q
python3 scripts/go_phase6_acceptance.py --api-go-url http://127.0.0.1:18081 --python-api-url http://127.0.0.1:18080 --redis-url redis://127.0.0.1:6380/0 --count 20
```

Observed:

```text
Go API created Go-owned jobs.
Go worker emitted events to vp:events:go.
Go listener finalized jobs.
Python API agreed on terminal status.
Redis pending counts were zero.
Non-eligible pipeline was rejected without fallback.
```
```

- [ ] **Step 5: Commit**

Run:

```bash
git add scripts/go_phase6_acceptance.py docs/go-migration-acceptance/README.md
git commit -m "test: add go orchestrator phase 6 acceptance runner"
```

## Task 10: Final Verification

**Owner:** assistant

**Files:**
- Read all changed files.
- Modify docs only if verification evidence differs from expected output.

- [ ] **Step 1: Run required checks**

Run:

```bash
go test ./...
go vet ./...
cd backend && python3 -m pytest
cd backend && python3 -m ruff check . || true
cd backend && python3 -m mypy app || true
```

Expected:

```text
go test passes.
go vet passes.
backend pytest passes.
ruff/mypy either pass or report the known missing module output if the local environment still lacks them.
```

- [ ] **Step 2: Run migration strict gates**

Run:

```bash
VP_GO_PARITY_STRICT=1 VP_GO_API=http://127.0.0.1:18081 VP_PYTHON_API=http://127.0.0.1:18080 python3 -m pytest tests/go_migration/test_go_api_parity.py tests/go_migration/test_go_api_read_parity.py tests/go_migration/test_go_registry_parity.py tests/go_migration/test_go_validator_parity.py -q
VP_GO_WORKER_SMOKE_STRICT=1 VP_REDIS_URL=redis://127.0.0.1:6380/0 VP_PYTHON_API=http://127.0.0.1:18080 python3 -m pytest tests/go_migration/test_go_trim_worker_smoke.py -q
VP_GO_WORKER_NODE_STRICT=1 VP_REDIS_URL=redis://127.0.0.1:6380/0 VP_PYTHON_API=http://127.0.0.1:18080 python3 -m pytest tests/go_migration/test_go_worker_nodes.py -q
VP_GO_WRITE_STRICT=1 VP_GO_API_URL=http://127.0.0.1:18081 python3 -m pytest tests/go_migration/test_go_api_write_parity.py -q
VP_GO_PHASE6_STRICT=1 VP_GO_API_URL=http://127.0.0.1:18081 VP_PYTHON_API=http://127.0.0.1:18080 VP_REDIS_URL=redis://127.0.0.1:6380/0 python3 -m pytest tests/go_migration/test_go_orchestrator_phase6.py -q
```

Expected:

```text
All strict gates pass.
```

- [ ] **Step 3: Run Phase 6 acceptance**

Run:

```bash
python3 scripts/go_phase6_acceptance.py --api-go-url http://127.0.0.1:18081 --python-api-url http://127.0.0.1:18080 --redis-url redis://127.0.0.1:6380/0 --count 20
redis-cli -u redis://127.0.0.1:6380/0 XPENDING vp:events:go orchestrator-go
redis-cli -u redis://127.0.0.1:6380/0 XPENDING vp:tasks:ffmpeg_go ffmpeg_go-workers
```

Expected:

```text
Acceptance JSON reports jobs_completed=20 and all error counters zero.
Both XPENDING commands report 0.
```

- [ ] **Step 4: Commit final evidence if changed**

Run:

```bash
git status --short
git diff --check
```

If docs changed:

```bash
git add docs/go-migration-acceptance/README.md
git commit -m "docs: record go orchestrator phase 6 evidence"
```

Expected:

```text
Working tree clean after final evidence commit.
```
