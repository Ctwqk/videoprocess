# Go Sidecar Production Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the Go sidecar production contract milestone by closing Go API Phase 1 read gaps and Go worker Phase 3 runtime gaps before migrating any additional node type.

**Architecture:** Python remains the authoritative API write path, orchestrator, event listener, and rollback worker. Go API serves only selected read-only parity routes, and Go worker continues to consume only `vp:tasks:ffmpeg_go` for the existing `trim` cutover. The work is contract-first: every behavior change starts with a focused failing test and ends with Go/Python parity or mixed-mode verification.

**Tech Stack:** Go 1.24+, `chi`, `pgx`, `go-redis`, `miniredis`, Python `pytest`/`httpx`, Docker Compose services `api`, `api-go`, `ffmpeg-worker`, `ffmpeg-worker-go`, `postgres`, `redis`, and `minio`.

---

## Scope Check

This plan intentionally contains two tracks because the approved design defines one production-contract milestone:

- Go API Phase 1 read-only parity.
- Go worker Phase 3 production semantics for the already-routed `trim` node.

The plan does not migrate new node types, does not add API write ownership to Go, and does not remove Python code.

## File Structure

API files:

- Create `internal/store/details.go`: detail-row structs and `GetPipeline`, `GetAsset`, `GetArtifactDetail`, `GetJobDetail` queries.
- Create `internal/store/schedule.go`: schedule status row and query matching Python `build_video_schedule_status`.
- Create `internal/httpapi/details.go`: HTTP handlers for pipeline, asset, artifact, and job detail routes.
- Create `internal/httpapi/schedule.go`: real schedule status handler.
- Modify `internal/httpapi/router.go`: register detail routes.
- Modify `internal/httpapi/jobs.go`: remove the fixed schedule handler after `schedule.go` owns it.
- Modify `internal/httpapi/httpapi_test.go`: fail-closed and schedule fake-OPEN regression tests.
- Modify `tests/go_migration/test_go_api_read_parity.py`: live parity for detail routes and schedule status.

Worker files:

- Modify `internal/worker/worker.go`: add production runtime config fields and env parsing.
- Modify `internal/worker/consumer.go`: add bounded concurrency, no-ack-on-event-publish-failure, graceful shutdown hooks, heartbeat, reclaim, and affinity calls.
- Create `internal/worker/reclaim.go`: PEL reclaim with `XAUTOCLAIM`.
- Create `internal/worker/heartbeat.go`: active-message heartbeat with `XCLAIM`.
- Create `internal/worker/affinity.go`: `preferred_hosts` defer/bounce logic.
- Create `internal/worker/cancel.go`: cancellation watcher helpers.
- Modify `internal/worker/runtime.go`: during-execution cancellation integration.
- Modify `internal/worker/consumer_test.go`: Redis contract tests.
- Modify `internal/worker/runtime_test.go`: cancellation watcher test.
- Modify `tests/go_migration/test_go_trim_worker_smoke.py`: pending-state and worker-id assertions.

Verification docs and scripts:

- Modify `docs/superpowers/specs/2026-05-19-go-sidecar-production-contract-design.md` only if implementation discoveries require an approved design correction.
- No frontend files should change.

---

### Task 1: API Detail Store Queries

**Files:**
- Create: `internal/store/details.go`
- Test: `internal/store/store_test.go`

- [ ] **Step 1: Add compile-time shape tests for detail structs**

Append this test to `internal/store/store_test.go`:

```go
func TestDetailRowsExposePythonCompatibleJSONKeys(t *testing.T) {
	pipeline := PipelineRow{ID: "p1", TemplateTags: nil}
	asset := AssetRow{ID: "a1"}
	artifact := ArtifactDetailRow{ID: "art1", Kind: "INTERMEDIATE"}
	job := JobDetailRow{JobRow: JobRow{ID: "j1"}, NodeExecutions: []NodeExecutionRow{}}

	assertJSONKey := func(name string, value any, key string) {
		t.Helper()
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("%s marshal: %v", name, err)
		}
		var payload map[string]any
		if err := json.Unmarshal(data, &payload); err != nil {
			t.Fatalf("%s unmarshal: %v", name, err)
		}
		if _, ok := payload[key]; !ok {
			t.Fatalf("%s missing json key %q in %s", name, key, string(data))
		}
	}

	assertJSONKey("pipeline", pipeline, "template_tags")
	assertJSONKey("asset", asset, "original_name")
	assertJSONKey("artifact", artifact, "node_execution_id")
	assertJSONKey("job", job, "node_executions")
}
```

Also add imports to `internal/store/store_test.go`:

```go
import (
	"encoding/json"
	"testing"
)
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
go test ./internal/store -run TestDetailRowsExposePythonCompatibleJSONKeys -count=1
```

Expected: FAIL because `ArtifactDetailRow`, `JobDetailRow`, and `NodeExecutionRow` are undefined.

- [ ] **Step 3: Add detail row structs and DB methods**

Create `internal/store/details.go`:

```go
package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type ArtifactDetailRow struct {
	ID              string    `json:"id"`
	JobID           string    `json:"job_id"`
	NodeExecutionID string    `json:"node_execution_id"`
	Kind            string    `json:"kind"`
	Filename        string    `json:"filename"`
	MimeType        *string   `json:"mime_type"`
	FileSize        *int64    `json:"file_size"`
	CreatedAt       time.Time `json:"created_at"`
}

type NodeExecutionRow struct {
	ID                      string     `json:"id"`
	NodeID                  string     `json:"node_id"`
	NodeType                string     `json:"node_type"`
	NodeLabel               string     `json:"node_label"`
	Status                  string     `json:"status"`
	Progress                int        `json:"progress"`
	WorkerID                *string    `json:"worker_id"`
	QueuedAt                *time.Time `json:"queued_at"`
	StartedAt               *time.Time `json:"started_at"`
	CompletedAt             *time.Time `json:"completed_at"`
	ErrorMessage            *string    `json:"error_message"`
	InputArtifactIDs        []string   `json:"input_artifact_ids"`
	OutputArtifactID        *string    `json:"output_artifact_id"`
	OutputArtifactFilename  *string    `json:"output_artifact_filename"`
	OutputArtifactMediaInfo any        `json:"output_artifact_media_info"`
}

type JobDetailRow struct {
	JobRow
	PipelineSnapshot any                `json:"pipeline_snapshot"`
	ExecutionPlan    any                `json:"execution_plan"`
	NodeExecutions   []NodeExecutionRow `json:"node_executions"`
}

func (s *Store) GetPipeline(ctx context.Context, id string) (PipelineRow, error) {
	var row PipelineRow
	var uuid [16]byte
	err := s.Pool.QueryRow(ctx, `
		SELECT id, name, description, definition, is_template, template_tags,
		       created_at, updated_at, version
		FROM pipelines
		WHERE id = $1
	`, id).Scan(&uuid, &row.Name, &row.Description, &row.Definition, &row.IsTemplate,
		&row.TemplateTags, &row.CreatedAt, &row.UpdatedAt, &row.Version)
	if err != nil {
		return row, err
	}
	row.ID = uuidString(uuid)
	if row.TemplateTags == nil {
		row.TemplateTags = []string{}
	}
	return row, nil
}

func (s *Store) GetAssetDetail(ctx context.Context, id string) (AssetRow, error) {
	var row AssetRow
	var uuid [16]byte
	err := s.Pool.QueryRow(ctx, `
		SELECT id, filename, original_name, mime_type, file_size, media_info, uploaded_at
		FROM assets
		WHERE id = $1
	`, id).Scan(&uuid, &row.Filename, &row.OriginalName, &row.MimeType, &row.FileSize, &row.MediaInfo, &row.UploadedAt)
	if err != nil {
		return row, err
	}
	row.ID = uuidString(uuid)
	return row, nil
}

func (s *Store) GetArtifactDetail(ctx context.Context, id string) (ArtifactDetailRow, error) {
	var row ArtifactDetailRow
	var uuid [16]byte
	var jobUUID [16]byte
	var nodeUUID [16]byte
	err := s.Pool.QueryRow(ctx, `
		SELECT id, job_id, node_execution_id, kind::text, filename, mime_type, file_size, created_at
		FROM artifacts
		WHERE id = $1
	`, id).Scan(&uuid, &jobUUID, &nodeUUID, &row.Kind, &row.Filename, &row.MimeType, &row.FileSize, &row.CreatedAt)
	if err != nil {
		return row, err
	}
	row.ID = uuidString(uuid)
	row.JobID = uuidString(jobUUID)
	row.NodeExecutionID = uuidString(nodeUUID)
	return row, nil
}

func (s *Store) GetJobDetail(ctx context.Context, id string) (JobDetailRow, error) {
	var row JobDetailRow
	var jobUUID [16]byte
	var pipelineUUID [16]byte
	err := s.Pool.QueryRow(ctx, `
		SELECT id, pipeline_id, status::text, submitted_at, started_at, completed_at,
		       error_message, submitted_by, retry_count, pipeline_snapshot, execution_plan
		FROM jobs
		WHERE id = $1
	`, id).Scan(&jobUUID, &pipelineUUID, &row.Status, &row.SubmittedAt, &row.StartedAt,
		&row.CompletedAt, &row.ErrorMessage, &row.SubmittedBy, &row.RetryCount,
		&row.PipelineSnapshot, &row.ExecutionPlan)
	if err != nil {
		return row, err
	}
	row.ID = uuidString(jobUUID)
	row.PipelineID = uuidString(pipelineUUID)

	nodes, err := s.listNodeExecutions(ctx, row.ID)
	if err != nil {
		return row, err
	}
	row.NodeExecutions = nodes
	return row, nil
}

func (s *Store) listNodeExecutions(ctx context.Context, jobID string) ([]NodeExecutionRow, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT ne.id, ne.node_id, ne.node_type, ne.node_label, ne.status::text, ne.progress,
		       ne.worker_id, ne.queued_at, ne.started_at, ne.completed_at, ne.error_message,
		       ne.input_artifact_ids, ne.output_artifact_id, a.filename, a.media_info
		FROM node_executions ne
		LEFT JOIN artifacts a ON a.id = ne.output_artifact_id
		WHERE ne.job_id = $1
		ORDER BY ne.id ASC
	`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]NodeExecutionRow, 0)
	for rows.Next() {
		var row NodeExecutionRow
		var uuid [16]byte
		var inputUUIDs []pgtype.UUID
		var outputUUID pgtype.UUID
		if err := rows.Scan(&uuid, &row.NodeID, &row.NodeType, &row.NodeLabel, &row.Status,
			&row.Progress, &row.WorkerID, &row.QueuedAt, &row.StartedAt, &row.CompletedAt,
			&row.ErrorMessage, &inputUUIDs, &outputUUID, &row.OutputArtifactFilename,
			&row.OutputArtifactMediaInfo); err != nil {
			return nil, err
		}
		row.ID = uuidString(uuid)
		row.InputArtifactIDs = make([]string, 0, len(inputUUIDs))
		for _, inputUUID := range inputUUIDs {
			if inputUUID.Valid {
				row.InputArtifactIDs = append(row.InputArtifactIDs, uuidString(inputUUID.Bytes))
			}
		}
		if outputUUID.Valid {
			outputID := uuidString(outputUUID.Bytes)
			row.OutputArtifactID = &outputID
		}
		items = append(items, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func IsNotFound(err error) bool {
	return err == pgx.ErrNoRows
}
```

- [ ] **Step 4: Run the focused test and verify it passes**

Run:

```bash
go test ./internal/store -run TestDetailRowsExposePythonCompatibleJSONKeys -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit Task 1**

Run:

```bash
git add internal/store/details.go internal/store/store_test.go
git commit -m "feat: add go store detail read models"
```

---

### Task 2: API Detail Routes And Fail-Closed Behavior

**Files:**
- Create: `internal/httpapi/details.go`
- Modify: `internal/httpapi/router.go`
- Modify: `internal/httpapi/httpapi_test.go`

- [ ] **Step 1: Write failing route tests**

Append to `internal/httpapi/httpapi_test.go`:

```go
func TestDetailEndpointsFailClosedWhenStubStoreDisabled(t *testing.T) {
	cases := []string{
		"/api/v1/pipelines/00000000-0000-0000-0000-000000000001",
		"/api/v1/assets/00000000-0000-0000-0000-000000000002",
		"/api/v1/artifacts/00000000-0000-0000-0000-000000000003",
		"/api/v1/jobs/00000000-0000-0000-0000-000000000004",
	}
	for _, path := range cases {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()

		NewServerWithOptions(nil, ServerOptions{AllowStubStore: false}).Router().ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s status = %d body=%s", path, rec.Code, rec.Body.String())
		}
		var payload map[string]string
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("%s payload: %v", path, err)
		}
		if payload["detail"] != "database unavailable" {
			t.Fatalf("%s payload = %#v", path, payload)
		}
	}
}
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
go test ./internal/httpapi -run TestDetailEndpointsFailClosedWhenStubStoreDisabled -count=1
```

Expected: FAIL because the detail routes are not registered and return 404.

- [ ] **Step 3: Implement detail route handlers**

Create `internal/httpapi/details.go`:

```go
package httpapi

import (
	"net/http"

	"github.com/Ctwqk/videoprocess/internal/store"
	"github.com/go-chi/chi/v5"
)

func (s *Server) getPipeline(w http.ResponseWriter, r *http.Request) {
	s.withStore(w, func(st *store.Store) {
		row, err := st.GetPipeline(r.Context(), chi.URLParam(r, "pipelineID"))
		s.writeDetailResult(w, row, err, "Pipeline not found")
	})
}

func (s *Server) getAsset(w http.ResponseWriter, r *http.Request) {
	s.withStore(w, func(st *store.Store) {
		row, err := st.GetAssetDetail(r.Context(), chi.URLParam(r, "assetID"))
		s.writeDetailResult(w, row, err, "Asset not found")
	})
}

func (s *Server) getArtifact(w http.ResponseWriter, r *http.Request) {
	s.withStore(w, func(st *store.Store) {
		row, err := st.GetArtifactDetail(r.Context(), chi.URLParam(r, "artifactID"))
		s.writeDetailResult(w, row, err, "Artifact not found")
	})
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	s.withStore(w, func(st *store.Store) {
		row, err := st.GetJobDetail(r.Context(), chi.URLParam(r, "jobID"))
		s.writeDetailResult(w, row, err, "Job not found")
	})
}

func (s *Server) withStore(w http.ResponseWriter, fn func(*store.Store)) {
	if s.store == nil {
		if !s.allowStubStore {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"detail": "database unavailable"})
			return
		}
		writeJSON(w, http.StatusNotFound, map[string]string{"detail": "Not found"})
		return
	}
	fn(s.store)
}

func (s *Server) writeDetailResult(w http.ResponseWriter, row any, err error, notFoundDetail string) {
	if err != nil {
		if store.IsNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]string{"detail": notFoundDetail})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, row)
}
```

- [ ] **Step 4: Register detail routes**

Modify the `/api/v1` block in `internal/httpapi/router.go` so it contains these registrations:

```go
r.Get("/node-types", s.listNodeTypes)
r.Get("/node-types/{typeName}", s.getNodeType)
r.Get("/pipelines", s.listPipelines)
r.Get("/pipelines/{pipelineID}", s.getPipeline)
r.Get("/templates", s.listTemplates)
r.Get("/assets", s.listAssets)
r.Get("/assets/{assetID}", s.getAsset)
r.Get("/artifacts/{artifactID}", s.getArtifact)
r.Get("/jobs", s.listJobs)
r.Get("/jobs/{jobID}", s.getJob)
```

- [ ] **Step 5: Run route tests**

Run:

```bash
go test ./internal/httpapi -run 'TestDetailEndpointsFailClosedWhenStubStoreDisabled|TestListEndpointsFailClosedWhenStubStoreDisabled' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit Task 2**

Run:

```bash
git add internal/httpapi/details.go internal/httpapi/router.go internal/httpapi/httpapi_test.go
git commit -m "feat: add go api detail read routes"
```

---

### Task 3: Real Schedule Status Route

**Files:**
- Create: `internal/store/schedule.go`
- Create: `internal/httpapi/schedule.go`
- Modify: `internal/httpapi/jobs.go`
- Modify: `internal/httpapi/httpapi_test.go`

- [ ] **Step 1: Write a regression test that rejects fixed fake OPEN**

Append to `internal/httpapi/httpapi_test.go`:

```go
func TestScheduleStatusFailsClosedWithoutStore(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/internal/schedule/video/status", nil)
	rec := httptest.NewRecorder()

	NewServerWithOptions(nil, ServerOptions{AllowStubStore: false}).Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["detail"] != "database unavailable" {
		t.Fatalf("payload = %#v", payload)
	}
}
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
go test ./internal/httpapi -run TestScheduleStatusFailsClosedWithoutStore -count=1
```

Expected: FAIL because the current handler returns `200 {"state":"OPEN"}`.

- [ ] **Step 3: Implement the store schedule query**

Create `internal/store/schedule.go`:

```go
package store

import (
	"context"
	"time"
)

const VideoScheduleServiceName = "videoprocess"

type VideoScheduleStatusRow struct {
	ServiceName  string     `json:"service_name"`
	State        string     `json:"state"`
	WaitingJobs  int        `json:"waiting_jobs"`
	ActiveJobs   int        `json:"active_jobs"`
	QueuedNodes  int        `json:"queued_nodes"`
	RunningNodes int        `json:"running_nodes"`
	UpdatedAt    *time.Time `json:"updated_at"`
	UpdatedBy    *string    `json:"updated_by"`
	ReleasedJobs int        `json:"released_jobs"`
}

func (s *Store) GetVideoScheduleStatus(ctx context.Context) (VideoScheduleStatusRow, error) {
	var row VideoScheduleStatusRow
	err := s.Pool.QueryRow(ctx, `
		SELECT service_name, state, updated_at, updated_by
		FROM runtime_schedules
		WHERE service_name = $1
	`, VideoScheduleServiceName).Scan(&row.ServiceName, &row.State, &row.UpdatedAt, &row.UpdatedBy)
	if err != nil {
		return row, err
	}
	if err := s.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM jobs WHERE status = 'WAITING_WINDOW'
	`).Scan(&row.WaitingJobs); err != nil {
		return row, err
	}
	if err := s.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM jobs WHERE status IN ('PENDING', 'VALIDATING', 'PLANNING', 'RUNNING')
	`).Scan(&row.ActiveJobs); err != nil {
		return row, err
	}
	if err := s.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM node_executions WHERE status = 'QUEUED'
	`).Scan(&row.QueuedNodes); err != nil {
		return row, err
	}
	if err := s.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM node_executions WHERE status = 'RUNNING'
	`).Scan(&row.RunningNodes); err != nil {
		return row, err
	}
	row.ReleasedJobs = 0
	return row, nil
}
```

- [ ] **Step 4: Replace the fixed schedule handler**

Create `internal/httpapi/schedule.go`:

```go
package httpapi

import "net/http"

func (s *Server) scheduleStatus(w http.ResponseWriter, r *http.Request) {
	if s.store == nil {
		if !s.allowStubStore {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"detail": "database unavailable"})
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"detail": "schedule store unavailable"})
		return
	}
	row, err := s.store.GetVideoScheduleStatus(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, row)
}
```

Remove the old `scheduleStatus` function from `internal/httpapi/jobs.go`.

- [ ] **Step 5: Run focused tests**

Run:

```bash
go test ./internal/httpapi -run 'TestScheduleStatusFailsClosedWithoutStore|TestListEndpointsShapeMatchesPython' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit Task 3**

Run:

```bash
git add internal/store/schedule.go internal/httpapi/schedule.go internal/httpapi/jobs.go internal/httpapi/httpapi_test.go
git commit -m "feat: serve real go schedule status"
```

---

### Task 4: Live Go API Read Parity Expansion

**Files:**
- Modify: `tests/go_migration/test_go_api_read_parity.py`

- [ ] **Step 1: Add failing live parity tests**

Replace `tests/go_migration/test_go_api_read_parity.py` with:

```python
from __future__ import annotations

import os
from typing import Any

import httpx
import pytest


PYTHON_API = os.environ.get("VP_PYTHON_API", "http://127.0.0.1:18080")
GO_API = os.environ.get("VP_GO_API", "http://127.0.0.1:18081")
STRICT = os.environ.get("VP_GO_PARITY_STRICT", "").lower() in {"1", "true", "yes", "on"}


def request_json(base_url: str, path: str) -> tuple[int, Any]:
    try:
        response = httpx.get(f"{base_url}{path}", timeout=10)
    except httpx.HTTPError as exc:
        if STRICT:
            raise
        pytest.skip(f"{base_url} unavailable for Go parity: {exc}")
    try:
        payload = response.json()
    except ValueError:
        payload = response.text
    return response.status_code, payload


def get_json(base_url: str, path: str) -> Any:
    status, payload = request_json(base_url, path)
    if status >= 400:
        raise AssertionError(f"{base_url}{path} returned {status}: {payload}")
    return payload


def assert_page_shape(payload: Any) -> None:
    assert isinstance(payload, dict)
    assert isinstance(payload.get("items"), list)
    assert isinstance(payload.get("total"), int)


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/pipelines?skip=0&limit=50",
        "/api/v1/templates?skip=0&limit=50",
        "/api/v1/assets?skip=0&limit=50",
        "/api/v1/jobs?skip=0&limit=50",
    ],
)
def test_read_page_shape_matches_python_contract(path: str) -> None:
    assert_page_shape(get_json(PYTHON_API, path))
    assert_page_shape(get_json(GO_API, path))


@pytest.mark.parametrize(
    "list_path, id_key, detail_path",
    [
        ("/api/v1/pipelines?skip=0&limit=1", "id", "/api/v1/pipelines/{id}"),
        ("/api/v1/assets?skip=0&limit=1", "id", "/api/v1/assets/{id}"),
        ("/api/v1/jobs?skip=0&limit=1", "id", "/api/v1/jobs/{id}"),
    ],
)
def test_detail_shape_matches_python_for_live_records(list_path: str, id_key: str, detail_path: str) -> None:
    py_page = get_json(PYTHON_API, list_path)
    go_page = get_json(GO_API, list_path)
    assert_page_shape(py_page)
    assert_page_shape(go_page)
    if not py_page["items"]:
        pytest.skip(f"no live rows for {list_path}")
    record_id = py_page["items"][0][id_key]
    py_detail = get_json(PYTHON_API, detail_path.format(id=record_id))
    go_detail = get_json(GO_API, detail_path.format(id=record_id))
    assert set(go_detail.keys()) == set(py_detail.keys())


def test_artifact_detail_shape_matches_python_when_job_has_output() -> None:
    jobs = get_json(PYTHON_API, "/api/v1/jobs?skip=0&limit=20")
    assert_page_shape(jobs)
    artifact_id = None
    for job in jobs["items"]:
        detail = get_json(PYTHON_API, f"/api/v1/jobs/{job['id']}")
        for node in detail.get("node_executions", []):
            if node.get("output_artifact_id"):
                artifact_id = node["output_artifact_id"]
                break
        if artifact_id:
            break
    if artifact_id is None:
        pytest.skip("no live output artifact available for artifact detail parity")

    py_detail = get_json(PYTHON_API, f"/api/v1/artifacts/{artifact_id}")
    go_detail = get_json(GO_API, f"/api/v1/artifacts/{artifact_id}")
    assert set(go_detail.keys()) == set(py_detail.keys())


def test_unknown_detail_ids_match_python_status_and_error_shape() -> None:
    missing = "00000000-0000-0000-0000-000000000000"
    for path in [
        f"/api/v1/pipelines/{missing}",
        f"/api/v1/assets/{missing}",
        f"/api/v1/artifacts/{missing}",
        f"/api/v1/jobs/{missing}",
    ]:
        py_status, py_payload = request_json(PYTHON_API, path)
        go_status, go_payload = request_json(GO_API, path)
        assert go_status == py_status
        assert set(go_payload.keys()) == set(py_payload.keys()) == {"detail"}


def test_go_schedule_status_is_not_fixed_fake_open() -> None:
    py_status, py_payload = request_json(PYTHON_API, "/internal/schedule/video/status")
    go_status, go_payload = request_json(GO_API, "/internal/schedule/video/status")
    assert go_status == py_status
    assert set(go_payload.keys()) == set(py_payload.keys())
    assert go_payload["state"] == py_payload["state"]


def test_go_readyz_reports_dependencies() -> None:
    payload = get_json(GO_API, "/readyz")
    assert payload["status"] in {"ready", "not_ready"}
    assert "postgres" in payload
```

- [ ] **Step 2: Run strict parity against current services and verify the expected missing-route failures**

Run with compose services running:

```bash
VP_GO_PARITY_STRICT=1 python3 -m pytest tests/go_migration/test_go_api_read_parity.py -q
```

Expected before Tasks 1-3 are present in the running `api-go` container: FAIL on detail or schedule routes. Expected after rebuilding `api-go`: PASS or SKIP only when the live DB has no rows for a detail category.

- [ ] **Step 3: Run the test after rebuilding api-go**

Run:

```bash
PLATFORM_UPLOAD_ROOT=/home/taiwei/Constructure-repos/constructure-platform-upload PLATFORM_UPLOAD_RUNTIME_ROOT=/home/taiwei/Constructure-repos/constructure-platform-upload docker compose up -d --build api-go
VP_GO_PARITY_STRICT=1 python3 -m pytest tests/go_migration/test_go_api_read_parity.py -q
```

Expected: PASS, with SKIP allowed only for missing live artifact rows.

- [ ] **Step 4: Commit Task 4**

Run:

```bash
git add tests/go_migration/test_go_api_read_parity.py
git commit -m "test: expand go api read parity"
```

---

### Task 5: Worker Runtime Config

**Files:**
- Modify: `internal/worker/worker.go`
- Test: `internal/worker/worker_test.go`

- [ ] **Step 1: Add config parsing tests**

Append to `internal/worker/worker_test.go`:

```go
func TestLoadConfigProductionRuntimeDefaults(t *testing.T) {
	t.Setenv("WORKER_TYPE", "")
	t.Setenv("WORKER_CONCURRENCY", "")
	t.Setenv("WORKER_PEL_MIN_IDLE_MS", "")
	t.Setenv("WORKER_PEL_RECLAIM_INTERVAL_SECONDS", "")
	t.Setenv("WORKER_HEARTBEAT_INTERVAL_SECONDS", "")
	t.Setenv("WORKER_AFFINITY_WAIT_SECONDS", "")
	t.Setenv("WORKER_AFFINITY_MAX_BOUNCES", "")
	t.Setenv("WORKER_SHUTDOWN_GRACE_SECONDS", "")
	t.Setenv("WORKER_CANCEL_POLL_SECONDS", "")

	cfg := LoadConfig()

	if cfg.WorkerType != "ffmpeg_go" {
		t.Fatalf("WorkerType = %q", cfg.WorkerType)
	}
	if cfg.Concurrency != 2 {
		t.Fatalf("Concurrency = %d", cfg.Concurrency)
	}
	if cfg.PELMinIdle != 15*time.Minute {
		t.Fatalf("PELMinIdle = %s", cfg.PELMinIdle)
	}
	if cfg.HeartbeatInterval != 15*time.Second {
		t.Fatalf("HeartbeatInterval = %s", cfg.HeartbeatInterval)
	}
	if cfg.AffinityMaxBounces != 6 {
		t.Fatalf("AffinityMaxBounces = %d", cfg.AffinityMaxBounces)
	}
}

func TestLoadConfigProductionRuntimeOverrides(t *testing.T) {
	t.Setenv("WORKER_CONCURRENCY", "4")
	t.Setenv("WORKER_PEL_MIN_IDLE_MS", "120000")
	t.Setenv("WORKER_PEL_RECLAIM_INTERVAL_SECONDS", "5")
	t.Setenv("WORKER_HEARTBEAT_INTERVAL_SECONDS", "3")
	t.Setenv("WORKER_AFFINITY_WAIT_SECONDS", "7")
	t.Setenv("WORKER_AFFINITY_MAX_BOUNCES", "2")
	t.Setenv("WORKER_SHUTDOWN_GRACE_SECONDS", "9")
	t.Setenv("WORKER_CANCEL_POLL_SECONDS", "1")

	cfg := LoadConfig()

	if cfg.Concurrency != 4 || cfg.PELMinIdle != 2*time.Minute || cfg.PELReclaimInterval != 5*time.Second {
		t.Fatalf("config = %#v", cfg)
	}
	if cfg.HeartbeatInterval != 3*time.Second || cfg.AffinityWait != 7*time.Second {
		t.Fatalf("config = %#v", cfg)
	}
	if cfg.ShutdownGracePeriod != 9*time.Second || cfg.CancelPollInterval != time.Second {
		t.Fatalf("config = %#v", cfg)
	}
}
```

Add `time` to the import list.

- [ ] **Step 2: Run the focused tests and verify they fail**

Run:

```bash
go test ./internal/worker -run TestLoadConfigProductionRuntime -count=1
```

Expected: FAIL because the new config fields do not exist.

- [ ] **Step 3: Add config fields and parsers**

Modify `internal/worker/worker.go`:

```go
import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)
```

Extend `Config`:

```go
type Config struct {
	WorkerType           string
	WorkerID             string
	RedisURL             string
	DatabaseURL          string
	StorageBackend       string
	StorageLocalRoot     string
	Concurrency          int
	PELMinIdle           time.Duration
	PELReclaimInterval   time.Duration
	HeartbeatInterval    time.Duration
	AffinityWait         time.Duration
	AffinityMaxBounces   int
	ShutdownGracePeriod  time.Duration
	CancelPollInterval   time.Duration
}
```

Set these fields in `LoadConfig()`:

```go
Concurrency:         intEnv("WORKER_CONCURRENCY", 2),
PELMinIdle:          durationMillisEnv("WORKER_PEL_MIN_IDLE_MS", 15*time.Minute),
PELReclaimInterval:  durationSecondsEnv("WORKER_PEL_RECLAIM_INTERVAL_SECONDS", 60*time.Second),
HeartbeatInterval:   durationSecondsEnv("WORKER_HEARTBEAT_INTERVAL_SECONDS", 15*time.Second),
AffinityWait:        durationSecondsEnv("WORKER_AFFINITY_WAIT_SECONDS", 20*time.Second),
AffinityMaxBounces:  intEnv("WORKER_AFFINITY_MAX_BOUNCES", 6),
ShutdownGracePeriod: durationSecondsEnv("WORKER_SHUTDOWN_GRACE_SECONDS", 30*time.Second),
CancelPollInterval:  durationSecondsEnv("WORKER_CANCEL_POLL_SECONDS", 2*time.Second),
```

Add helper functions:

```go
func intEnv(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func durationSecondsEnv(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return time.Duration(parsed) * time.Second
}

func durationMillisEnv(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return time.Duration(parsed) * time.Millisecond
}
```

- [ ] **Step 4: Run config tests**

Run:

```bash
go test ./internal/worker -run TestLoadConfigProductionRuntime -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit Task 5**

Run:

```bash
git add internal/worker/worker.go internal/worker/worker_test.go
git commit -m "feat: add go worker production runtime config"
```

---

### Task 6: Event Publication Ack Semantics

**Files:**
- Modify: `internal/worker/consumer.go`
- Modify: `internal/worker/consumer_test.go`

- [ ] **Step 1: Add a no-ack-on-event-failure test**

Append to `internal/worker/consumer_test.go`:

```go
func TestConsumerLeavesValidTaskPendingWhenEventPublishFails(t *testing.T) {
	client, mr := newRedis(t)
	cfg := Config{WorkerType: "ffmpeg_go", WorkerID: "test-event-failure"}
	handler := &fakeHandler{node: "trim"}
	consumer := NewConsumer(client, cfg, handler)
	consumer.BlockTimeout = 50 * time.Millisecond

	withGroup(t, consumer)
	enqueueTrim(t, client, cfg.WorkerType)
	mr.SetError("forced redis write failure")
	runOneTick(t, consumer)
	mr.SetError("")

	stream := redisstream.TaskStream(cfg.WorkerType)
	pending, err := client.XPending(context.Background(), stream, consumer.ConsumerGroup).Result()
	if err != nil {
		t.Fatalf("xpending: %v", err)
	}
	if pending.Count != 1 {
		t.Fatalf("pending after publish failure = %d; want 1", pending.Count)
	}
}
```

- [ ] **Step 2: Run the focused test and verify it fails**

Run:

```bash
go test ./internal/worker -run TestConsumerLeavesValidTaskPendingWhenEventPublishFails -count=1
```

Expected: FAIL because `handleMessage` currently acks even when publish fails.

- [ ] **Step 3: Change publish failure handling**

Modify `handleMessage` in `internal/worker/consumer.go` so the success and valid-task failure branches look like:

```go
case err == nil:
	if strings.TrimSpace(result.OutputArtifactID) == "" {
		if pubErr := c.publishFailed(ctx, task, "handler succeeded without output_artifact_id"); pubErr != nil {
			c.log.Error("publish failed event failed; leaving message pending", "msg_id", msg.ID, "error", pubErr)
			return
		}
		c.ack(ctx, msg.ID)
		return
	}
	if pubErr := c.publishCompleted(ctx, task, result.OutputArtifactID); pubErr != nil {
		c.log.Error("publish completed event failed; leaving message pending", "msg_id", msg.ID, "error", pubErr)
		return
	}
	c.ack(ctx, msg.ID)
```

And the default branch:

```go
default:
	c.log.Error("handler failed", "msg_id", msg.ID, "node_id", task.NodeID, "error", err)
	if pubErr := c.publishFailed(ctx, task, err.Error()); pubErr != nil {
		c.log.Error("publish failed event failed; leaving message pending", "msg_id", msg.ID, "error", pubErr)
		return
	}
	c.ack(ctx, msg.ID)
```

Keep confirmed cancellation as ack without event.

- [ ] **Step 4: Run consumer tests**

Run:

```bash
go test ./internal/worker -run 'TestConsumer.*(Event|Failure|Success|Cancellation|OutputArtifact)' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit Task 6**

Run:

```bash
git add internal/worker/consumer.go internal/worker/consumer_test.go
git commit -m "fix: leave go worker tasks pending on event publish failure"
```

---

### Task 7: PEL Reclaim And Heartbeat

**Files:**
- Create: `internal/worker/reclaim.go`
- Create: `internal/worker/heartbeat.go`
- Modify: `internal/worker/consumer.go`
- Modify: `internal/worker/consumer_test.go`

- [ ] **Step 1: Add reclaim and heartbeat tests**

Append to `internal/worker/consumer_test.go`:

```go
func TestReclaimPendingClaimsStaleMessages(t *testing.T) {
	client, _ := newRedis(t)
	cfg := Config{WorkerType: "ffmpeg_go", WorkerID: "claimer", PELMinIdle: time.Millisecond}
	consumer := NewConsumer(client, cfg, &fakeHandler{node: "trim"})
	withGroup(t, consumer)
	enqueueTrim(t, client, cfg.WorkerType)

	other := "other-worker"
	stream := redisstream.TaskStream(cfg.WorkerType)
	if _, err := client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group: consumer.ConsumerGroup, Consumer: other, Streams: []string{stream, ">"}, Count: 1,
	}).Result(); err != nil {
		t.Fatalf("xreadgroup: %v", err)
	}
	time.Sleep(5 * time.Millisecond)

	claimed, err := consumer.ReclaimPending(context.Background())
	if err != nil {
		t.Fatalf("ReclaimPending: %v", err)
	}
	if claimed == 0 {
		t.Fatal("expected at least one reclaimed message")
	}
}

func TestHeartbeatRefreshesPendingOwnership(t *testing.T) {
	client, _ := newRedis(t)
	cfg := Config{WorkerType: "ffmpeg_go", WorkerID: "heartbeat-worker", HeartbeatInterval: time.Millisecond}
	consumer := NewConsumer(client, cfg, &fakeHandler{node: "trim"})
	withGroup(t, consumer)
	msgID := enqueueTrim(t, client, cfg.WorkerType)
	stream := redisstream.TaskStream(cfg.WorkerType)
	if _, err := client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group: consumer.ConsumerGroup, Consumer: cfg.WorkerID, Streams: []string{stream, ">"}, Count: 1,
	}).Result(); err != nil {
		t.Fatalf("xreadgroup: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := consumer.StartHeartbeat(ctx, msgID)
	time.Sleep(5 * time.Millisecond)
	cancel()
	<-done

	pending, err := client.XPendingExt(context.Background(), &redis.XPendingExtArgs{
		Stream: stream, Group: consumer.ConsumerGroup, Start: "-", End: "+", Count: 10,
	}).Result()
	if err != nil {
		t.Fatalf("xpendingext: %v", err)
	}
	if len(pending) != 1 || pending[0].Consumer != cfg.WorkerID {
		t.Fatalf("pending = %#v", pending)
	}
}
```

- [ ] **Step 2: Run focused tests and verify they fail**

Run:

```bash
go test ./internal/worker -run 'TestReclaimPendingClaimsStaleMessages|TestHeartbeatRefreshesPendingOwnership' -count=1
```

Expected: FAIL because `ReclaimPending` and `StartHeartbeat` are undefined.

- [ ] **Step 3: Implement reclaim**

Create `internal/worker/reclaim.go`:

```go
package worker

import (
	"context"
	"time"

	"github.com/Ctwqk/videoprocess/internal/redisstream"
	"github.com/redis/go-redis/v9"
)

func (c *Consumer) ReclaimPending(ctx context.Context) (int, error) {
	minIdle := c.cfg.PELMinIdle
	if minIdle <= 0 {
		minIdle = 15 * time.Minute
	}
	stream := redisstream.TaskStream(c.WorkerType)
	messages, _, err := c.Redis.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    c.ConsumerGroup,
		Consumer: c.WorkerID,
		MinIdle:  minIdle,
		Start:    "0-0",
		Count:    100,
	}).Result()
	if err != nil {
		return 0, err
	}
	for _, msg := range messages {
		c.handleMessage(ctx, msg)
	}
	return len(messages), nil
}
```

- [ ] **Step 4: Implement heartbeat**

Create `internal/worker/heartbeat.go`:

```go
package worker

import (
	"context"
	"time"

	"github.com/Ctwqk/videoprocess/internal/redisstream"
	"github.com/redis/go-redis/v9"
)

func (c *Consumer) StartHeartbeat(ctx context.Context, msgID string) <-chan struct{} {
	done := make(chan struct{})
	interval := c.cfg.HeartbeatInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		stream := redisstream.TaskStream(c.WorkerType)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := c.Redis.XClaim(ctx, &redis.XClaimArgs{
					Stream:   stream,
					Group:    c.ConsumerGroup,
					Consumer: c.WorkerID,
					MinIdle:  0,
					Messages: []string{msgID},
				}).Err(); err != nil && err != redis.Nil {
					c.log.Warn("worker heartbeat failed", "msg_id", msgID, "error", err)
				}
			}
		}
	}()
	return done
}
```

- [ ] **Step 5: Wire config into Consumer and start heartbeat around handler execution**

Modify `Consumer` in `internal/worker/consumer.go`:

```go
cfg Config
```

Set fields in `NewConsumer`:

```go
cfg: cfg,
```

Wrap handler execution in `handleMessage`:

```go
taskCtx, taskCancel := context.WithCancel(ctx)
heartbeatDone := c.StartHeartbeat(taskCtx, msg.ID)
result, err := handler.Execute(taskCtx, task)
taskCancel()
<-heartbeatDone
```

- [ ] **Step 6: Run worker package tests**

Run:

```bash
go test ./internal/worker -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit Task 7**

Run:

```bash
git add internal/worker/reclaim.go internal/worker/heartbeat.go internal/worker/consumer.go internal/worker/consumer_test.go
git commit -m "feat: add go worker reclaim and heartbeat"
```

---

### Task 8: Host Affinity

**Files:**
- Create: `internal/worker/affinity.go`
- Modify: `internal/worker/consumer.go`
- Modify: `internal/worker/consumer_test.go`

- [ ] **Step 1: Add affinity tests**

Append to `internal/worker/consumer_test.go`:

```go
func TestAffinityDefersAndRequeuesForPreferredHost(t *testing.T) {
	client, _ := newRedis(t)
	cfg := Config{WorkerType: "ffmpeg_go", WorkerID: "ffmpeg_go-worker@wrong-host:1", AffinityWait: time.Minute, AffinityMaxBounces: 6}
	consumer := NewConsumer(client, cfg, &fakeHandler{node: "trim"})

	withGroup(t, consumer)
	stream := redisstream.TaskStream(cfg.WorkerType)
	configJSON, _ := json.Marshal(map[string]any{"duration": "1"})
	msgID, err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{
			"job_id": "job-1", "node_execution_id": "ne-1", "node_id": "trim_1", "node_type": "trim",
			"config": string(configJSON), "input_artifacts": "{}", "preferred_hosts": `["right-host"]`,
			"affinity_enqueued_at": time.Now().UTC().Format(time.RFC3339Nano), "affinity_bounces": "0",
		},
	}).Result()
	if err != nil {
		t.Fatal(err)
	}

	task := TaskMessage{
		JobID: "job-1", NodeExecutionID: "ne-1", NodeID: "trim_1", NodeType: "trim",
		Config: map[string]any{"duration": "1"}, InputArtifacts: map[string]any{},
		PreferredHosts: []string{"right-host"}, AffinityEnqueuedAt: time.Now().UTC().Format(time.RFC3339Nano), AffinityBounces: "0",
	}
	if !consumer.shouldDeferForAffinity(task, time.Now().UTC()) {
		t.Fatal("expected non-preferred host to defer")
	}
	if err := consumer.deferForAffinity(context.Background(), redis.XMessage{ID: msgID}, task); err != nil {
		t.Fatalf("deferForAffinity: %v", err)
	}

	pending, _ := client.XPending(context.Background(), stream, consumer.ConsumerGroup).Result()
	if pending.Count != 0 {
		t.Fatalf("deferred message must be acked, pending = %d", pending.Count)
	}
	length, _ := client.XLen(context.Background(), stream).Result()
	if length < 2 {
		t.Fatalf("expected re-enqueued message, stream length = %d", length)
	}
}

func TestAffinityRelaxesAfterBounceBudget(t *testing.T) {
	cfg := Config{WorkerType: "ffmpeg_go", WorkerID: "ffmpeg_go-worker@wrong-host:1", AffinityWait: time.Minute, AffinityMaxBounces: 1}
	consumer := NewConsumer(nil, cfg, &fakeHandler{node: "trim"})
	task := TaskMessage{
		NodeType: "trim", PreferredHosts: []string{"right-host"},
		AffinityEnqueuedAt: time.Now().UTC().Format(time.RFC3339Nano), AffinityBounces: "1",
	}
	if consumer.shouldDeferForAffinity(task, time.Now().UTC()) {
		t.Fatal("expected worker to process locally after bounce budget is exhausted")
	}
}
```

- [ ] **Step 2: Run focused tests and verify they fail**

Run:

```bash
go test ./internal/worker -run 'TestAffinityDefersAndRequeuesForPreferredHost|TestAffinityRelaxesAfterBounceBudget' -count=1
```

Expected: FAIL because affinity is parsed but not enforced.

- [ ] **Step 3: Implement affinity helpers**

Create `internal/worker/affinity.go`:

```go
package worker

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/Ctwqk/videoprocess/internal/redisstream"
	"github.com/redis/go-redis/v9"
)

func (c *Consumer) shouldDeferForAffinity(task TaskMessage, now time.Time) bool {
	if len(task.PreferredHosts) == 0 {
		return false
	}
	host := workerHostFromID(c.WorkerID)
	for _, preferred := range task.PreferredHosts {
		if strings.EqualFold(strings.TrimSpace(preferred), host) {
			return false
		}
	}
	bounces, _ := strconv.Atoi(task.AffinityBounces)
	maxBounces := c.cfg.AffinityMaxBounces
	if maxBounces <= 0 {
		maxBounces = 6
	}
	if bounces >= maxBounces {
		return false
	}
	enqueuedAt := parseAffinityTime(task.AffinityEnqueuedAt, now)
	wait := c.cfg.AffinityWait
	if wait <= 0 {
		wait = 20 * time.Second
	}
	return now.Sub(enqueuedAt) < wait
}

func (c *Consumer) deferForAffinity(ctx context.Context, msg redis.XMessage, task TaskMessage) error {
	task.AffinityBounces = strconv.Itoa(parseIntDefault(task.AffinityBounces, 0) + 1)
	if strings.TrimSpace(task.AffinityEnqueuedAt) == "" {
		task.AffinityEnqueuedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	values, err := encodeTask(task)
	if err != nil {
		return err
	}
	stream := redisstream.TaskStream(c.WorkerType)
	if err := c.Redis.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: values}).Err(); err != nil {
		return err
	}
	c.ack(ctx, msg.ID)
	return nil
}

func workerHostFromID(workerID string) string {
	parts := strings.Split(workerID, "@")
	if len(parts) != 2 {
		return workerID
	}
	hostPID := parts[1]
	if idx := strings.LastIndex(hostPID, ":"); idx >= 0 {
		return hostPID[:idx]
	}
	return hostPID
}

func parseAffinityTime(raw string, fallback time.Time) time.Time {
	if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw)); err == nil {
		return parsed
	}
	if seconds, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64); err == nil && seconds > 0 {
		return time.Unix(seconds, 0).UTC()
	}
	return fallback
}

func parseIntDefault(raw string, fallback int) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return fallback
	}
	return parsed
}
```

- [ ] **Step 4: Add task encoding and wire affinity before handler lookup**

Add to `internal/worker/consumer.go`:

```go
func encodeTask(task TaskMessage) (map[string]any, error) {
	if task.Config == nil {
		task.Config = map[string]any{}
	}
	if task.InputArtifacts == nil {
		task.InputArtifacts = map[string]any{}
	}
	if task.PreferredHosts == nil {
		task.PreferredHosts = []string{}
	}
	config, err := json.Marshal(task.Config)
	if err != nil {
		return nil, err
	}
	inputArtifacts, err := json.Marshal(task.InputArtifacts)
	if err != nil {
		return nil, err
	}
	preferredHosts, err := json.Marshal(task.PreferredHosts)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"job_id":               task.JobID,
		"node_execution_id":    task.NodeExecutionID,
		"node_id":              task.NodeID,
		"node_type":            task.NodeType,
		"config":               string(config),
		"input_artifacts":      string(inputArtifacts),
		"preferred_hosts":      string(preferredHosts),
		"affinity_enqueued_at": task.AffinityEnqueuedAt,
		"affinity_bounces":     task.AffinityBounces,
	}, nil
}
```

In `handleMessage`, immediately after successful `decodeTask`:

```go
if c.shouldDeferForAffinity(task, time.Now().UTC()) {
	if err := c.deferForAffinity(ctx, msg, task); err != nil {
		c.log.Warn("affinity defer failed; leaving message pending", "msg_id", msg.ID, "error", err)
	}
	return
}
```

- [ ] **Step 5: Run affinity tests**

Run:

```bash
go test ./internal/worker -run 'TestAffinityDefersAndRequeuesForPreferredHost|TestAffinityRelaxesAfterBounceBudget' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit Task 8**

Run:

```bash
git add internal/worker/affinity.go internal/worker/consumer.go internal/worker/consumer_test.go
git commit -m "feat: add go worker host affinity"
```

---

### Task 9: Bounded Concurrency And Graceful Shutdown

**Files:**
- Modify: `internal/worker/consumer.go`
- Modify: `internal/worker/consumer_test.go`

- [ ] **Step 1: Add concurrency limit test**

Append to `internal/worker/consumer_test.go`:

```go
type blockingHandler struct {
	node       string
	started    chan struct{}
	release    chan struct{}
	active     atomic.Int32
	maxActive  atomic.Int32
	invocation atomic.Int32
}

func (h *blockingHandler) NodeType() string { return h.node }

func (h *blockingHandler) Execute(ctx context.Context, task TaskMessage) (NodeResult, error) {
	current := h.active.Add(1)
	for {
		old := h.maxActive.Load()
		if current <= old || h.maxActive.CompareAndSwap(old, current) {
			break
		}
	}
	h.invocation.Add(1)
	h.started <- struct{}{}
	select {
	case <-h.release:
	case <-ctx.Done():
		h.active.Add(-1)
		return NodeResult{}, ctx.Err()
	}
	h.active.Add(-1)
	return NodeResult{OutputArtifactID: "artifact-1"}, nil
}

func TestConsumerHonorsConcurrencyLimit(t *testing.T) {
	client, _ := newRedis(t)
	cfg := Config{WorkerType: "ffmpeg_go", WorkerID: "test-concurrency", Concurrency: 2, HeartbeatInterval: time.Hour}
	handler := &blockingHandler{node: "trim", started: make(chan struct{}, 4), release: make(chan struct{})}
	consumer := NewConsumer(client, cfg, handler)
	consumer.BlockTimeout = 10 * time.Millisecond

	withGroup(t, consumer)
	for i := 0; i < 4; i++ {
		enqueueTrim(t, client, cfg.WorkerType)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	go func() { _ = consumer.Run(ctx) }()
	<-handler.started
	<-handler.started
	time.Sleep(30 * time.Millisecond)
	if got := handler.maxActive.Load(); got != 2 {
		t.Fatalf("maxActive = %d; want 2", got)
	}
	close(handler.release)
}
```

Add imports:

```go
import "sync/atomic"
```

- [ ] **Step 2: Run focused test and verify it fails**

Run:

```bash
go test ./internal/worker -run TestConsumerHonorsConcurrencyLimit -count=1
```

Expected: FAIL because `Run` processes one message synchronously.

- [ ] **Step 3: Implement bounded dispatch and graceful shutdown**

Modify `Run` in `internal/worker/consumer.go` to use a semaphore and wait group:

```go
func (c *Consumer) Run(ctx context.Context) error {
	if err := c.EnsureGroup(ctx); err != nil {
		return err
	}
	if _, err := c.ReclaimPending(ctx); err != nil {
		c.log.Warn("initial pending reclaim failed", "error", err)
	}
	stream := redisstream.TaskStream(c.WorkerType)
	concurrency := c.cfg.Concurrency
	if concurrency <= 0 {
		concurrency = 2
	}
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	reclaimTicker := time.NewTicker(c.reclaimInterval())
	defer reclaimTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return c.waitForActive(ctx, &wg)
		case <-reclaimTicker.C:
			if _, err := c.ReclaimPending(ctx); err != nil {
				c.log.Warn("periodic pending reclaim failed", "error", err)
			}
			continue
		case sem <- struct{}{}:
		}

		res, err := c.Redis.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: c.ConsumerGroup, Consumer: c.WorkerID, Streams: []string{stream, ">"}, Block: c.BlockTimeout, Count: 1,
		}).Result()
		if err != nil {
			<-sem
			if errors.Is(err, context.Canceled) {
				return c.waitForActive(ctx, &wg)
			}
			if errors.Is(err, redis.Nil) {
				continue
			}
			c.log.Warn("xreadgroup failed", "error", err)
			time.Sleep(time.Second)
			continue
		}
		dispatched := false
		for _, streamResult := range res {
			for _, msg := range streamResult.Messages {
				dispatched = true
				wg.Add(1)
				go func(m redis.XMessage) {
					defer wg.Done()
					defer func() { <-sem }()
					c.handleMessage(ctx, m)
				}(msg)
			}
		}
		if !dispatched {
			<-sem
		}
	}
}
```

Add helpers:

```go
func (c *Consumer) reclaimInterval() time.Duration {
	if c.cfg.PELReclaimInterval > 0 {
		return c.cfg.PELReclaimInterval
	}
	return 60 * time.Second
}

func (c *Consumer) waitForActive(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	timeout := c.cfg.ShutdownGracePeriod
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return ctx.Err()
	case <-timer.C:
		c.log.Warn("worker shutdown grace period expired")
		return ctx.Err()
	}
}
```

Add `sync` to the imports.

- [ ] **Step 4: Run worker tests**

Run:

```bash
go test ./internal/worker -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit Task 9**

Run:

```bash
git add internal/worker/consumer.go internal/worker/consumer_test.go
git commit -m "feat: add go worker concurrency and graceful shutdown"
```

---

### Task 10: During-Execution Cancellation

**Files:**
- Create: `internal/worker/cancel.go`
- Modify: `internal/worker/runtime.go`
- Modify: `internal/worker/runtime_test.go`

- [ ] **Step 1: Add a runtime cancellation watcher test**

Append to `internal/worker/runtime_test.go`:

```go
type cancelAfterRunningStore struct {
	fakeTaskStore
	loads atomic.Int32
}

func (f *cancelAfterRunningStore) LoadExecutionState(ctx context.Context, nodeExecutionID string) (store.ExecutionState, error) {
	count := f.loads.Add(1)
	if count >= 2 {
		state := f.state
		state.NodeStatus = contracts.NodeStatusCancelled
		return state, nil
	}
	return f.state, nil
}

type blockingMediaHandler struct {
	cancelled chan struct{}
}

func (h *blockingMediaHandler) NodeType() string { return "trim" }

func (h *blockingMediaHandler) Execute(ctx context.Context, inputPath string, outputPath string, config map[string]any) error {
	<-ctx.Done()
	close(h.cancelled)
	return ctx.Err()
}

func TestMediaTaskHandlerCancelsDuringExecutionWhenStateChanges(t *testing.T) {
	root := t.TempDir()
	inputPath := filepath.Join(root, "input.mp4")
	if err := os.WriteFile(inputPath, []byte("input"), 0o644); err != nil {
		t.Fatal(err)
	}
	storeFake := &cancelAfterRunningStore{
		fakeTaskStore: fakeTaskStore{
			state: store.ExecutionState{
				JobID: "00000000-0000-0000-0000-000000000101", NodeExecutionID: "00000000-0000-0000-0000-000000000201",
				JobStatus: contracts.JobStatusRunning, NodeStatus: contracts.NodeStatusQueued,
			},
			input: store.ArtifactRow{ID: "00000000-0000-0000-0000-000000000301", Filename: "input.mp4", StorageBackend: "local", StoragePath: inputPath},
		},
	}
	media := &blockingMediaHandler{cancelled: make(chan struct{})}
	handler := NewMediaTaskHandler(RuntimeEnv{
		Store: storeFake, Storage: storage.LocalBackend{Root: root}, StorageBackend: "local", LocalRoot: root,
		WorkerID: "ffmpeg_go-worker@test:1", CancelPollInterval: time.Millisecond,
	}, media)

	_, err := handler.Execute(context.Background(), TaskMessage{
		JobID: "00000000-0000-0000-0000-000000000101", NodeExecutionID: "00000000-0000-0000-0000-000000000201",
		NodeType: "trim", Config: map[string]any{"duration": "1"}, InputArtifacts: map[string]any{"input": "00000000-0000-0000-0000-000000000301"},
	})
	if !errors.Is(err, ErrConfirmedCancellation) {
		t.Fatalf("err = %v; want ErrConfirmedCancellation", err)
	}
	select {
	case <-media.cancelled:
	case <-time.After(time.Second):
		t.Fatal("media handler context was not cancelled")
	}
}
```

Add imports to `internal/worker/runtime_test.go`:

```go
import (
	"errors"
	"sync/atomic"
	"time"
)
```

- [ ] **Step 2: Run focused test and verify it fails**

Run:

```bash
go test ./internal/worker -run TestMediaTaskHandlerCancelsDuringExecutionWhenStateChanges -count=1
```

Expected: FAIL because `RuntimeEnv.CancelPollInterval` and during-execution cancellation are not implemented.

- [ ] **Step 3: Add cancellation helper**

Create `internal/worker/cancel.go`:

```go
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
```

- [ ] **Step 4: Add cancel poll field and wrap media execution**

Modify `RuntimeEnv` in `internal/worker/runtime.go`:

```go
CancelPollInterval time.Duration
```

Add `time` to the imports.

Replace direct media execution:

```go
if err := h.media.Execute(ctx, inputPath, outputLocalPath, task.Config); err != nil {
	return NodeResult{}, err
}
```

with:

```go
execCtx, cancel := context.WithCancel(ctx)
cancelled := make(chan struct{}, 1)
watchDone := make(chan struct{})
go func() {
	defer close(watchDone)
	h.watchCancellation(execCtx, cancel, task.NodeExecutionID, cancelled)
}()
err = h.media.Execute(execCtx, inputPath, outputLocalPath, task.Config)
cancel()
<-watchDone
if err != nil {
	select {
	case <-cancelled:
		return NodeResult{}, ErrConfirmedCancellation
	default:
		return NodeResult{}, err
	}
}
```

Keep the existing pre-start cancellation check.

- [ ] **Step 5: Run runtime tests**

Run:

```bash
go test ./internal/worker -run 'TestMediaTaskHandlerCreatesArtifactResult|TestMediaTaskHandlerCancelsDuringExecutionWhenStateChanges' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit Task 10**

Run:

```bash
git add internal/worker/cancel.go internal/worker/runtime.go internal/worker/runtime_test.go
git commit -m "feat: cancel go media tasks during execution"
```

---

### Task 11: Mixed-Mode Smoke Gate Hardening

**Files:**
- Modify: `tests/go_migration/test_go_trim_worker_smoke.py`

- [ ] **Step 1: Add Redis pending and Go worker-id assertions**

Modify `tests/go_migration/test_go_trim_worker_smoke.py` so it imports Redis support:

```python
import redis
```

Add helper functions after `get_json`:

```python
def redis_client() -> redis.Redis:
    url = os.environ.get("VP_REDIS_URL", "redis://127.0.0.1:6380/0")
    return redis.Redis.from_url(url, decode_responses=True)


def pending_count() -> int:
    pending = redis_client().xpending("vp:tasks:ffmpeg_go", "ffmpeg_go-workers")
    if isinstance(pending, dict):
        return int(pending.get("pending", 0))
    return int(pending["pending"])
```

Extend the final assertions:

```python
assert trim_nodes[0]["worker_id"]
assert "ffmpeg_go-worker@" in trim_nodes[0]["worker_id"]
assert pending_count() == 0
```

- [ ] **Step 2: Run smoke test without strict and verify it skips**

Run:

```bash
python3 -m pytest tests/go_migration/test_go_trim_worker_smoke.py -q
```

Expected: SKIP because `VP_GO_WORKER_SMOKE_STRICT=1` is not set.

- [ ] **Step 3: Run strict smoke with Docker services**

Run:

```bash
PLATFORM_UPLOAD_ROOT=/home/taiwei/Constructure-repos/constructure-platform-upload PLATFORM_UPLOAD_RUNTIME_ROOT=/home/taiwei/Constructure-repos/constructure-platform-upload docker compose up -d --build api api-go ffmpeg-worker ffmpeg-worker-go redis postgres minio
VP_GO_WORKER_SMOKE_STRICT=1 VP_REDIS_URL=redis://127.0.0.1:6380/0 python3 -m pytest tests/go_migration/test_go_trim_worker_smoke.py -q
```

Expected: PASS.

- [ ] **Step 4: Commit Task 11**

Run:

```bash
git add tests/go_migration/test_go_trim_worker_smoke.py
git commit -m "test: harden go trim mixed-mode smoke"
```

---

### Task 12: Full Verification And Final Gate

**Files:**
- No code file changes expected in this task.

- [ ] **Step 1: Run Go tests**

Run:

```bash
go test ./...
```

Expected: PASS.

- [ ] **Step 2: Run Go vet**

Run:

```bash
go vet ./...
```

Expected: PASS.

- [ ] **Step 3: Run backend tests**

Run:

```bash
cd backend && python3 -m pytest
```

Expected: PASS.

- [ ] **Step 4: Run optional backend linters**

Run:

```bash
cd backend && python3 -m ruff check . || true
cd backend && python3 -m mypy app || true
```

Expected: command output is recorded. Missing `ruff` or `mypy` is acceptable only because AGENTS marks them `|| true`.

- [ ] **Step 5: Rebuild Go sidecars and Python services**

Run:

```bash
PLATFORM_UPLOAD_ROOT=/home/taiwei/Constructure-repos/constructure-platform-upload PLATFORM_UPLOAD_RUNTIME_ROOT=/home/taiwei/Constructure-repos/constructure-platform-upload docker compose up -d --build api api-go ffmpeg-worker ffmpeg-worker-go redis postgres minio
```

Expected: all listed services are running.

- [ ] **Step 6: Verify API readiness**

Run:

```bash
curl -fsS http://127.0.0.1:18080/health
curl -fsS http://127.0.0.1:18081/health
curl -fsS http://127.0.0.1:18081/readyz
curl -fsS http://127.0.0.1:18081/internal/schedule/video/status
```

Expected:

```text
Python /health returns {"status":"ok"}
Go /health returns {"status":"ok"}
Go /readyz includes postgres
Go schedule status includes service_name, state, waiting_jobs, active_jobs, queued_nodes, running_nodes
```

- [ ] **Step 7: Run strict Go API parity**

Run:

```bash
VP_GO_PARITY_STRICT=1 python3 -m pytest tests/go_migration/test_go_api_parity.py tests/go_migration/test_go_api_read_parity.py -q
```

Expected: PASS, with the existing registry coverage xfail allowed if it remains explicitly marked xfail.

- [ ] **Step 8: Run strict Go worker smoke**

Run:

```bash
VP_GO_WORKER_SMOKE_STRICT=1 VP_REDIS_URL=redis://127.0.0.1:6380/0 python3 -m pytest tests/go_migration/test_go_trim_worker_smoke.py -q
```

Expected: PASS.

- [ ] **Step 9: Verify the running worker is the Go binary**

Run:

```bash
docker compose exec -T ffmpeg-worker-go sh -lc 'ps -o comm= -p 1 && printenv WORKER_TYPE'
```

Expected:

```text
vp-ffmpeg-worker-go
ffmpeg_go
```

- [ ] **Step 10: Verify Redis pending is clean**

Run:

```bash
redis-cli -u redis://127.0.0.1:6380 XPENDING vp:tasks:ffmpeg_go ffmpeg_go-workers
```

Expected:

```text
0
```

- [ ] **Step 11: Commit any verification-doc updates if present**

If no files changed during verification, do not create an empty commit.

Run:

```bash
git status --short
```

Expected: clean worktree.
