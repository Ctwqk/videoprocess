# VideoProcess Go Partial Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the existing Go sidecars into a safe read-only API parity sidecar and a real `trim` worker on `ffmpeg_go`, while Python remains the authoritative API, orchestrator, event listener, schema owner, and rollback path.

**Architecture:** Keep the strangler-sidecar model from `/home/taiwei/Constructure-repos/videoprocess/docs/videoprocess-go-partial-migration-spec.md`: Go API handles low-risk reads with explicit readiness, and Go worker executes only selected pure ffmpeg nodes on its own Redis stream. The first executable migration target is `trim`, and registry cutover happens only after artifact creation, event publication, cancellation, and mixed-mode tests are proven.

**Tech Stack:** Go 1.25, `net/http`, `github.com/go-chi/chi/v5`, `github.com/jackc/pgx/v5/pgxpool`, `github.com/redis/go-redis/v9`, `github.com/minio/minio-go/v7`, `log/slog`, Python pytest, Docker Compose.

---

## Scope And Stop Rules

This plan implements the milestone "Go trim worker + read-only API safety/parity".

Do not migrate these surfaces in this plan:

- Go orchestrator ownership of `vp:events`.
- AutoFlow graph planner, LLM, ASR, TTS, platform search, or public upload routes.
- Full frontend proxy switch to `api-go`.
- Any node registry switch except the final gated `trim` switch.
- Any Postgres schema or Alembic ownership change.

Hard cutover rule:

- Do not change `backend/app/node_registry/builtin/trim.py` to `ffmpeg_go` until Task 8 passes.

## File Structure

- Create `docs/superpowers/specs/2026-05-19-videoprocess-go-partial-migration-spec.md`: local copy of the approved canonical spec.
- Modify `internal/config/config.go`: add Go API stub-store and worker concurrency/cancel config.
- Modify `cmd/vp-api/main.go`: wire production-safe stub-store behavior and readiness probes.
- Modify `internal/httpapi/router.go`: add `/readyz` and middleware.
- Create `internal/httpapi/readiness.go`: readiness response shape and probe execution.
- Create `internal/httpapi/middleware.go`: request logging and panic recovery.
- Modify `internal/httpapi/httpapi_test.go`: cover `/readyz`, stub behavior, and panic recovery.
- Modify `internal/store/store.go`: add `Ping`.
- Create `internal/store/artifacts.go`: artifact row reads and creation.
- Create `internal/store/node_executions.go`: node/job execution state and running updates.
- Modify `internal/worker/worker.go`: extend worker config.
- Modify `internal/worker/consumer.go`: return `NodeResult`, require `output_artifact_id`, ack confirmed cancellation, keep shutdown cancellation pending.
- Create `internal/worker/runtime.go`: task-level media runtime adapter.
- Create `internal/worker/artifacts.go`: output path, mime, temp file, and storage helpers.
- Modify `internal/worker/consumer_test.go`: update success, failure, cancellation, and missing artifact tests.
- Modify `internal/worker/handlers/trim.go`: keep path-level behavior and expose `NodeType`.
- Create `internal/worker/runtime_test.go`: fake store/storage tests for artifact/event behavior.
- Modify `cmd/vp-ffmpeg-worker/main.go`: open store/storage and register `trim`.
- Create `tests/go_migration/test_go_api_read_parity.py`: read-only API parity smoke.
- Create `tests/go_migration/test_go_trim_worker_smoke.py`: opt-in mixed-mode trim smoke.
- Modify `docs/go-migration-runbook.md`: record the new readiness, trim worker, cutover, and rollback commands.
- Modify `backend/app/node_registry/builtin/trim.py`: switch `worker_type` to `ffmpeg_go` only in the final cutover task.

## Task 1: Preserve The Approved Spec In This Worktree

**Files:**
- Create: `docs/superpowers/specs/2026-05-19-videoprocess-go-partial-migration-spec.md`

- [ ] **Step 1: Copy the approved canonical spec into the worktree**

Run:

```bash
mkdir -p docs/superpowers/specs
cp /home/taiwei/Constructure-repos/videoprocess/docs/videoprocess-go-partial-migration-spec.md docs/superpowers/specs/2026-05-19-videoprocess-go-partial-migration-spec.md
```

Expected:

```text
docs/superpowers/specs/2026-05-19-videoprocess-go-partial-migration-spec.md exists
```

- [ ] **Step 2: Verify the baseline Go suite**

Run:

```bash
go test ./...
```

Expected:

```text
ok  	github.com/Ctwqk/videoprocess/internal/worker
ok  	github.com/Ctwqk/videoprocess/internal/worker/handlers
```

- [ ] **Step 3: Verify the Python contract fixture still matches the current registry**

Run:

```bash
cd backend
python3 -m pytest tests/test_go_contract_fixtures.py -q
```

Expected:

```text
2 passed
```

- [ ] **Step 4: Commit the spec copy**

Run:

```bash
git add docs/superpowers/specs/2026-05-19-videoprocess-go-partial-migration-spec.md
git commit -m "docs: add go partial migration spec"
```

## Task 2: Add API Readiness And Fail-Closed Stub Behavior

**Files:**
- Modify: `internal/config/config.go`
- Modify: `cmd/vp-api/main.go`
- Modify: `internal/httpapi/router.go`
- Create: `internal/httpapi/readiness.go`
- Modify: `internal/httpapi/httpapi_test.go`
- Modify: `internal/store/store.go`

- [ ] **Step 1: Add failing config tests**

Add to `internal/config/config_test.go`:

```go
func TestAPIGoAllowStubStoreDefaultsFalse(t *testing.T) {
	t.Setenv("VP_API_GO_ALLOW_STUB_STORE", "")

	cfg := Load()

	if cfg.APIGoAllowStubStore {
		t.Fatal("APIGoAllowStubStore must default false so production read APIs fail closed")
	}
}

func TestAPIGoAllowStubStoreReadsTruthyValues(t *testing.T) {
	t.Setenv("VP_API_GO_ALLOW_STUB_STORE", "true")

	cfg := Load()

	if !cfg.APIGoAllowStubStore {
		t.Fatal("APIGoAllowStubStore should read true")
	}
}
```

- [ ] **Step 2: Run the failing config tests**

Run:

```bash
go test ./internal/config -run 'TestAPIGoAllowStubStore' -count=1
```

Expected:

```text
cfg.APIGoAllowStubStore undefined
```

- [ ] **Step 3: Add config fields**

In `internal/config/config.go`, add this field to `Config`:

```go
APIGoAllowStubStore bool
```

In `Load()`, add:

```go
APIGoAllowStubStore: boolEnv("VP_API_GO_ALLOW_STUB_STORE", false),
```

- [ ] **Step 4: Add readiness implementation**

Create `internal/httpapi/readiness.go`:

```go
package httpapi

import (
	"context"
	"net/http"
	"time"
)

type ReadinessProbe func(context.Context) error

type ReadinessDeps struct {
	Postgres ReadinessProbe
	Redis    ReadinessProbe
	Storage  ReadinessProbe
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	payload := map[string]string{"status": "ready"}
	status := http.StatusOK
	check := func(name string, probe ReadinessProbe) {
		if probe == nil {
			return
		}
		if err := probe(ctx); err != nil {
			payload["status"] = "not_ready"
			payload[name] = "error"
			status = http.StatusServiceUnavailable
			return
		}
		payload[name] = "ok"
	}

	check("postgres", s.readiness.Postgres)
	check("redis", s.readiness.Redis)
	check("storage", s.readiness.Storage)

	writeJSON(w, status, payload)
}
```

- [ ] **Step 5: Extend server options and fail closed when store is absent**

In `internal/httpapi/router.go`, replace the `Server` constructors with:

```go
type Server struct {
	store          *store.Store
	readiness      ReadinessDeps
	allowStubStore bool
}

type ServerOptions struct {
	Readiness      ReadinessDeps
	AllowStubStore bool
}

func NewServer() *Server {
	return &Server{allowStubStore: true}
}

func NewServerWithStore(s *store.Store) *Server {
	return &Server{store: s, allowStubStore: true}
}

func NewServerWithOptions(st *store.Store, opts ServerOptions) *Server {
	return &Server{
		store:          st,
		readiness:      opts.Readiness,
		allowStubStore: opts.AllowStubStore,
	}
}
```

In `Router()`, add:

```go
r.Get("/readyz", s.readyz)
```

In `internal/httpapi/pipelines.go`, update the nil-store branch in `respondPipelineList`:

```go
if s.store == nil {
	if !s.allowStubStore {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"detail": "database unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, emptyPage())
	return
}
```

Apply the same nil-store guard to `listJobs` and `listAssets`.

- [ ] **Step 6: Add store ping**

In `internal/store/store.go`, add:

```go
func (s *Store) Ping(ctx context.Context) error {
	if s == nil || s.Pool == nil {
		return pgx.ErrNoRows
	}
	return s.Pool.Ping(ctx)
}
```

- [ ] **Step 7: Wire readiness and stub policy in the API main**

In `cmd/vp-api/main.go`, replace the server construction branch with:

```go
var server *httpapi.Server
var pgProbe httpapi.ReadinessProbe
if err != nil {
	slog.Error("vp-api-go: database unavailable", "error", err)
	dbErr := err
	pgProbe = func(context.Context) error { return dbErr }
	server = httpapi.NewServerWithOptions(nil, httpapi.ServerOptions{
		AllowStubStore: cfg.APIGoAllowStubStore,
		Readiness: httpapi.ReadinessDeps{
			Postgres: pgProbe,
		},
	})
} else {
	defer st.Close()
	pgProbe = st.Ping
	server = httpapi.NewServerWithOptions(st, httpapi.ServerOptions{
		AllowStubStore: cfg.APIGoAllowStubStore,
		Readiness: httpapi.ReadinessDeps{
			Postgres: pgProbe,
		},
	})
}
```

- [ ] **Step 8: Add readiness and fail-closed tests**

Add to `internal/httpapi/httpapi_test.go`:

```go
func TestReadyzReportsHealthyDependencies(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	srv := NewServerWithOptions(nil, ServerOptions{
		AllowStubStore: true,
		Readiness: ReadinessDeps{
			Postgres: func(context.Context) error { return nil },
		},
	})

	srv.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["status"] != "ready" || payload["postgres"] != "ok" {
		t.Fatalf("payload = %#v", payload)
	}
}

func TestReadyzFailsWhenDependencyFails(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()
	srv := NewServerWithOptions(nil, ServerOptions{
		Readiness: ReadinessDeps{
			Postgres: func(context.Context) error { return errors.New("down") },
		},
	})

	srv.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestListEndpointsFailClosedWhenStubStoreDisabled(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/pipelines", nil)
	rec := httptest.NewRecorder()

	NewServerWithOptions(nil, ServerOptions{AllowStubStore: false}).Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
}
```

Add imports:

```go
import (
	"context"
	"errors"
)
```

- [ ] **Step 9: Verify API safety**

Run:

```bash
go test ./internal/config ./internal/httpapi ./cmd/vp-api
go test ./...
```

Expected:

```text
ok  	github.com/Ctwqk/videoprocess/internal/config
ok  	github.com/Ctwqk/videoprocess/internal/httpapi
```

- [ ] **Step 10: Commit**

Run:

```bash
git add cmd/vp-api internal/config internal/httpapi internal/store
git commit -m "feat: add go api readiness gate"
```

## Task 3: Add HTTP Recovery And Request Logging

**Files:**
- Create: `internal/httpapi/middleware.go`
- Modify: `internal/httpapi/router.go`
- Modify: `internal/httpapi/httpapi_test.go`

- [ ] **Step 1: Add middleware tests**

Add to `internal/httpapi/httpapi_test.go`:

```go
func TestRecoveryMiddlewareReturnsFastAPIStyleError(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/panic-test", nil)
	rec := httptest.NewRecorder()
	r := chi.NewRouter()
	r.Use(recoverPanic)
	r.Get("/panic-test", func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})

	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["detail"] != "internal server error" {
		t.Fatalf("payload = %#v", payload)
	}
}
```

Add import:

```go
import "github.com/go-chi/chi/v5"
```

- [ ] **Step 2: Run the failing middleware test**

Run:

```bash
go test ./internal/httpapi -run TestRecoveryMiddlewareReturnsFastAPIStyleError -count=1
```

Expected:

```text
undefined: recoverPanic
```

- [ ] **Step 3: Implement middleware**

Create `internal/httpapi/middleware.go`:

```go
package httpapi

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"
)

func recoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				slog.Error("http panic", "path", r.URL.Path, "panic", rec, "stack", string(debug.Stack()))
				writeJSON(w, http.StatusInternalServerError, map[string]string{"detail": "internal server error"})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		slog.Info("http request", "method", r.Method, "path", r.URL.Path, "duration_ms", time.Since(start).Milliseconds())
	})
}
```

- [ ] **Step 4: Wire middleware**

In `internal/httpapi/router.go`, before route registration:

```go
r.Use(recoverPanic)
r.Use(logRequests)
```

- [ ] **Step 5: Verify middleware**

Run:

```bash
go test ./internal/httpapi -count=1
go test ./...
```

Expected:

```text
ok  	github.com/Ctwqk/videoprocess/internal/httpapi
```

- [ ] **Step 6: Commit**

Run:

```bash
git add internal/httpapi
git commit -m "feat: harden go api middleware"
```

## Task 4: Change Worker Handler Contract To Return Output Artifact IDs

**Files:**
- Modify: `internal/worker/consumer.go`
- Modify: `internal/worker/consumer_test.go`
- Modify: `internal/worker/worker.go`

- [ ] **Step 1: Update consumer tests first**

In `internal/worker/consumer_test.go`, change `fakeHandler.Execute` to:

```go
func (f *fakeHandler) Execute(ctx context.Context, task TaskMessage) (NodeResult, error) {
	f.seen = append(f.seen, task)
	if f.err != nil {
		return NodeResult{}, f.err
	}
	return NodeResult{OutputArtifactID: "artifact-1"}, nil
}
```

Add a dedicated fake for missing artifacts:

```go
type emptyArtifactHandler struct{}

func (h emptyArtifactHandler) NodeType() string { return "trim" }
func (h emptyArtifactHandler) Execute(context.Context, TaskMessage) (NodeResult, error) {
	return NodeResult{}, nil
}
```

Add this test:

```go
func TestConsumerRejectsSuccessWithoutOutputArtifactID(t *testing.T) {
	client, _ := newRedis(t)
	cfg := Config{WorkerType: "ffmpeg_go", WorkerID: "test-empty-artifact"}
	consumer := NewConsumer(client, cfg, emptyArtifactHandler{})
	consumer.BlockTimeout = 50 * time.Millisecond

	withGroup(t, consumer)
	enqueueTrim(t, client, cfg.WorkerType)
	runOneTick(t, consumer)

	events, _ := client.XRange(context.Background(), redisstream.EventStream, "-", "+").Result()
	if len(events) != 1 || events[0].Values["event"] != "node_failed" {
		t.Fatalf("events = %#v", events)
	}
	if got, _ := events[0].Values["error"].(string); !strings.Contains(got, "output_artifact_id") {
		t.Fatalf("error = %q", got)
	}
}
```

Add import:

```go
import "strings"
```

- [ ] **Step 2: Run failing worker tests**

Run:

```bash
go test ./internal/worker -run 'TestConsumer(Success|Rejects)' -count=1
```

Expected:

```text
undefined: NodeResult
```

- [ ] **Step 3: Update the handler contract**

In `internal/worker/consumer.go`, replace the `Handler` interface with:

```go
type NodeResult struct {
	OutputArtifactID string
}

type Handler interface {
	NodeType() string
	Execute(ctx context.Context, task TaskMessage) (NodeResult, error)
}
```

Add:

```go
var ErrConfirmedCancellation = errors.New("confirmed cancellation")
```

Update the success branch in `handleMessage`:

```go
result, err := handler.Execute(ctx, task)
switch {
case err == nil:
	if strings.TrimSpace(result.OutputArtifactID) == "" {
		_ = c.publishFailed(ctx, task, "handler succeeded without output_artifact_id")
		c.ack(ctx, msg.ID)
		return
	}
	_ = c.publishCompleted(ctx, task, result.OutputArtifactID)
	c.ack(ctx, msg.ID)
case errors.Is(err, ErrConfirmedCancellation):
	c.log.Info("task cancelled by recorded job/node state, acking without event", "msg_id", msg.ID, "node_id", task.NodeID)
	c.ack(ctx, msg.ID)
case errors.Is(err, context.Canceled):
	c.log.Info("worker context cancelled, leaving message pending", "msg_id", msg.ID, "node_id", task.NodeID)
default:
	c.log.Error("handler failed", "msg_id", msg.ID, "node_id", task.NodeID, "error", err)
	_ = c.publishFailed(ctx, task, err.Error())
	c.ack(ctx, msg.ID)
}
```

Change `publishCompleted` to:

```go
func (c *Consumer) publishCompleted(ctx context.Context, task TaskMessage, artifactID string) error {
	return redisstream.PublishNodeCompleted(ctx, c.Redis, redisstream.NodeEvent{
		JobID:            task.JobID,
		NodeExecutionID:  task.NodeExecutionID,
		OutputArtifactID: artifactID,
	})
}
```

- [ ] **Step 4: Update cancellation test semantics**

Change `TestConsumerCancellationLeavesMessagePending` into:

```go
func TestConsumerConfirmedCancellationAcksWithoutEvent(t *testing.T) {
	client, _ := newRedis(t)
	cfg := Config{WorkerType: "ffmpeg_go", WorkerID: "test-3"}
	handler := &fakeHandler{node: "trim", err: ErrConfirmedCancellation}
	consumer := NewConsumer(client, cfg, handler)
	consumer.BlockTimeout = 50 * time.Millisecond

	withGroup(t, consumer)
	enqueueTrim(t, client, cfg.WorkerType)
	runOneTick(t, consumer)

	stream := redisstream.TaskStream(cfg.WorkerType)
	pending, err := client.XPending(context.Background(), stream, consumer.ConsumerGroup).Result()
	if err != nil {
		t.Fatalf("xpending: %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("confirmed cancelled task should be acked, pending = %d", pending.Count)
	}

	events, _ := client.XRange(context.Background(), redisstream.EventStream, "-", "+").Result()
	if len(events) != 0 {
		t.Fatalf("confirmed cancellation must not publish events, got %#v", events)
	}
}
```

- [ ] **Step 5: Verify worker contract**

Run:

```bash
go test ./internal/worker -count=1
go test ./...
```

Expected:

```text
ok  	github.com/Ctwqk/videoprocess/internal/worker
```

- [ ] **Step 6: Commit**

Run:

```bash
git add internal/worker
git commit -m "feat: require go worker artifact results"
```

## Task 5: Add Store Methods For Worker Runtime

**Files:**
- Create: `internal/store/artifacts.go`
- Create: `internal/store/node_executions.go`
- Create: `internal/store/store_test.go`

- [ ] **Step 1: Add unit tests for pure helper behavior**

Create `internal/store/store_test.go`:

```go
package store

import "testing"

func TestMimeForExtension(t *testing.T) {
	cases := map[string]string{
		".mp4": "video/mp4",
		".mkv": "video/x-matroska",
		".wav": "audio/wav",
		".srt": "application/x-subrip",
		".bin": "video/mp4",
	}
	for ext, want := range cases {
		if got := GuessMime(ext); got != want {
			t.Fatalf("GuessMime(%q) = %q; want %q", ext, got, want)
		}
	}
}
```

- [ ] **Step 2: Run the failing store helper test**

Run:

```bash
go test ./internal/store -run TestMimeForExtension -count=1
```

Expected:

```text
undefined: GuessMime
```

- [ ] **Step 3: Add artifact row types and helper**

Create `internal/store/artifacts.go`:

```go
package store

import (
	"context"
	"time"

	"github.com/Ctwqk/videoprocess/internal/contracts"
)

type ArtifactRow struct {
	ID              string
	JobID           string
	NodeExecutionID string
	Filename        string
	MimeType        *string
	FileSize        *int64
	StorageBackend  string
	StoragePath     string
	MediaInfo       any
	CreatedAt       time.Time
}

type CreateArtifactInput struct {
	JobID           string
	NodeExecutionID string
	Kind            contracts.ArtifactKind
	Filename        string
	MimeType        string
	FileSize        int64
	StorageBackend  string
	StoragePath     string
	MediaInfo       any
}

func (s *Store) GetArtifact(ctx context.Context, id string) (ArtifactRow, error) {
	var row ArtifactRow
	var uuid [16]byte
	var jobUUID [16]byte
	var nodeUUID [16]byte
	err := s.Pool.QueryRow(ctx, `
		SELECT id, job_id, node_execution_id, filename, mime_type, file_size,
		       storage_backend, storage_path, media_info, created_at
		FROM artifacts
		WHERE id = $1
	`, id).Scan(&uuid, &jobUUID, &nodeUUID, &row.Filename, &row.MimeType, &row.FileSize, &row.StorageBackend, &row.StoragePath, &row.MediaInfo, &row.CreatedAt)
	if err != nil {
		return row, err
	}
	row.ID = uuidString(uuid)
	row.JobID = uuidString(jobUUID)
	row.NodeExecutionID = uuidString(nodeUUID)
	return row, nil
}

func (s *Store) CreateIntermediateArtifact(ctx context.Context, in CreateArtifactInput) (string, error) {
	var id [16]byte
	err := s.Pool.QueryRow(ctx, `
		INSERT INTO artifacts (
			job_id, node_execution_id, kind, filename, mime_type, file_size,
			storage_backend, storage_path, media_info
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id
	`, in.JobID, in.NodeExecutionID, in.Kind, in.Filename, in.MimeType, in.FileSize, in.StorageBackend, in.StoragePath, in.MediaInfo).Scan(&id)
	if err != nil {
		return "", err
	}
	return uuidString(id), nil
}

func GuessMime(ext string) string {
	switch ext {
	case ".mp4":
		return "video/mp4"
	case ".mkv":
		return "video/x-matroska"
	case ".json":
		return "application/json"
	case ".webm":
		return "video/webm"
	case ".avi":
		return "video/x-msvideo"
	case ".mov":
		return "video/quicktime"
	case ".srt":
		return "application/x-subrip"
	case ".wav":
		return "audio/wav"
	case ".mp3":
		return "audio/mpeg"
	default:
		return "video/mp4"
	}
}
```

- [ ] **Step 4: Add node execution state queries**

Create `internal/store/node_executions.go`:

```go
package store

import (
	"context"
	"time"

	"github.com/Ctwqk/videoprocess/internal/contracts"
)

type ExecutionState struct {
	JobID           string
	NodeExecutionID string
	JobStatus       contracts.JobStatus
	NodeStatus      contracts.NodeStatus
}

func (s *Store) LoadExecutionState(ctx context.Context, nodeExecutionID string) (ExecutionState, error) {
	var state ExecutionState
	var jobUUID [16]byte
	var nodeUUID [16]byte
	var jobStatus string
	var nodeStatus string
	err := s.Pool.QueryRow(ctx, `
		SELECT j.id, ne.id, j.status::text, ne.status::text
		FROM node_executions ne
		JOIN jobs j ON j.id = ne.job_id
		WHERE ne.id = $1
	`, nodeExecutionID).Scan(&jobUUID, &nodeUUID, &jobStatus, &nodeStatus)
	if err != nil {
		return state, err
	}
	state.JobID = uuidString(jobUUID)
	state.NodeExecutionID = uuidString(nodeUUID)
	state.JobStatus = contracts.JobStatus(jobStatus)
	state.NodeStatus = contracts.NodeStatus(nodeStatus)
	return state, nil
}

func (s *Store) MarkNodeRunning(ctx context.Context, nodeExecutionID string, workerID string) error {
	_, err := s.Pool.Exec(ctx, `
		UPDATE node_executions
		SET status = 'RUNNING', started_at = $2, worker_id = $3
		WHERE id = $1
	`, nodeExecutionID, time.Now().UTC(), workerID)
	return err
}
```

- [ ] **Step 5: Verify store package**

Run:

```bash
go test ./internal/store -count=1
go test ./...
```

Expected:

```text
ok  	github.com/Ctwqk/videoprocess/internal/store
```

- [ ] **Step 6: Commit**

Run:

```bash
git add internal/store
git commit -m "feat: add go worker store queries"
```

## Task 6: Add Worker Runtime Adapter For Path-Level Media Handlers

**Files:**
- Create: `internal/worker/runtime.go`
- Create: `internal/worker/artifacts.go`
- Create: `internal/worker/runtime_test.go`
- Modify: `internal/worker/handlers/trim.go`

- [ ] **Step 1: Add a runtime test with fake store and local storage**

Create `internal/worker/runtime_test.go`:

```go
package worker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Ctwqk/videoprocess/internal/contracts"
	"github.com/Ctwqk/videoprocess/internal/store"
)

type fakeTaskStore struct {
	state        store.ExecutionState
	input        store.ArtifactRow
	createdInput store.CreateArtifactInput
	runningNode  string
}

func (f *fakeTaskStore) LoadExecutionState(context.Context, string) (store.ExecutionState, error) {
	return f.state, nil
}
func (f *fakeTaskStore) MarkNodeRunning(_ context.Context, nodeExecutionID string, _ string) error {
	f.runningNode = nodeExecutionID
	return nil
}
func (f *fakeTaskStore) GetArtifact(context.Context, string) (store.ArtifactRow, error) {
	return f.input, nil
}
func (f *fakeTaskStore) CreateIntermediateArtifact(_ context.Context, in store.CreateArtifactInput) (string, error) {
	f.createdInput = in
	return "00000000-0000-0000-0000-000000000777", nil
}

type fakeMediaHandler struct {
	seenInput  string
	seenOutput string
}

func (h *fakeMediaHandler) NodeType() string { return "trim" }
func (h *fakeMediaHandler) Execute(ctx context.Context, inputPath string, outputPath string, config map[string]any) error {
	h.seenInput = inputPath
	h.seenOutput = outputPath
	return os.WriteFile(outputPath, []byte("media"), 0o644)
}

func TestMediaTaskHandlerCreatesArtifactResult(t *testing.T) {
	root := t.TempDir()
	inputPath := filepath.Join(root, "input.mp4")
	if err := os.WriteFile(inputPath, []byte("input"), 0o644); err != nil {
		t.Fatal(err)
	}
	storeFake := &fakeTaskStore{
		state: store.ExecutionState{
			JobID:           "00000000-0000-0000-0000-000000000101",
			NodeExecutionID: "00000000-0000-0000-0000-000000000201",
			JobStatus:       contracts.JobStatusRunning,
			NodeStatus:      contracts.NodeStatusQueued,
		},
		input: store.ArtifactRow{
			ID:             "00000000-0000-0000-0000-000000000301",
			Filename:       "input.mp4",
			StorageBackend: "local",
			StoragePath:    inputPath,
		},
	}
	media := &fakeMediaHandler{}
	handler := NewMediaTaskHandler(RuntimeEnv{
		Store:          storeFake,
		StorageBackend: "local",
		LocalRoot:      root,
		WorkerID:       "ffmpeg_go-worker@test:1",
	}, media)

	result, err := handler.Execute(context.Background(), TaskMessage{
		JobID:           "00000000-0000-0000-0000-000000000101",
		NodeExecutionID: "00000000-0000-0000-0000-000000000201",
		NodeType:        "trim",
		Config:          map[string]any{"duration": "1", "output_format": "mp4"},
		InputArtifacts:  map[string]any{"input": "00000000-0000-0000-0000-000000000301"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.OutputArtifactID == "" {
		t.Fatal("OutputArtifactID must be populated")
	}
	if storeFake.runningNode != "00000000-0000-0000-0000-000000000201" {
		t.Fatalf("running node = %q", storeFake.runningNode)
	}
	if storeFake.createdInput.StorageBackend != "local" || storeFake.createdInput.StoragePath == "" {
		t.Fatalf("created artifact = %#v", storeFake.createdInput)
	}
	if media.seenInput != inputPath {
		t.Fatalf("input path = %q", media.seenInput)
	}
}
```

- [ ] **Step 2: Run the failing runtime test**

Run:

```bash
go test ./internal/worker -run TestMediaTaskHandlerCreatesArtifactResult -count=1
```

Expected:

```text
undefined: NewMediaTaskHandler
```

- [ ] **Step 3: Define runtime interfaces**

Create `internal/worker/runtime.go`:

```go
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/Ctwqk/videoprocess/internal/contracts"
	"github.com/Ctwqk/videoprocess/internal/store"
)

type TaskStore interface {
	LoadExecutionState(ctx context.Context, nodeExecutionID string) (store.ExecutionState, error)
	MarkNodeRunning(ctx context.Context, nodeExecutionID string, workerID string) error
	GetArtifact(ctx context.Context, id string) (store.ArtifactRow, error)
	CreateIntermediateArtifact(ctx context.Context, in store.CreateArtifactInput) (string, error)
}

type RuntimeEnv struct {
	Store          TaskStore
	StorageBackend string
	LocalRoot      string
	WorkerID       string
	Logger         *slog.Logger
}

type MediaHandler interface {
	NodeType() string
	Execute(ctx context.Context, inputPath string, outputPath string, config map[string]any) error
}

type MediaTaskHandler struct {
	env   RuntimeEnv
	media MediaHandler
}

func NewMediaTaskHandler(env RuntimeEnv, media MediaHandler) MediaTaskHandler {
	return MediaTaskHandler{env: env, media: media}
}

func (h MediaTaskHandler) NodeType() string {
	return h.media.NodeType()
}

func (h MediaTaskHandler) Execute(ctx context.Context, task TaskMessage) (NodeResult, error) {
	if h.env.Store == nil {
		return NodeResult{}, errors.New("worker store is required")
	}
	state, err := h.env.Store.LoadExecutionState(ctx, task.NodeExecutionID)
	if err != nil {
		return NodeResult{}, fmt.Errorf("load execution state: %w", err)
	}
	if state.JobStatus == contracts.JobStatusCancelled || state.NodeStatus == contracts.NodeStatusCancelled {
		return NodeResult{}, ErrConfirmedCancellation
	}
	if err := h.env.Store.MarkNodeRunning(ctx, task.NodeExecutionID, h.env.WorkerID); err != nil {
		return NodeResult{}, fmt.Errorf("mark node running: %w", err)
	}
	inputArtifactID, ok := task.InputArtifacts["input"].(string)
	if !ok || inputArtifactID == "" {
		return NodeResult{}, errors.New("missing input artifact on input port")
	}
	input, err := h.env.Store.GetArtifact(ctx, inputArtifactID)
	if err != nil {
		return NodeResult{}, fmt.Errorf("load input artifact: %w", err)
	}
	inputPath, cleanup, err := h.resolveInput(ctx, input)
	if err != nil {
		return NodeResult{}, err
	}
	defer cleanup()

	ext := outputExtension(task.NodeType, task.Config)
	filename := task.NodeExecutionID + ext
	outputStoragePath := filepath.Join("artifacts", task.JobID, filename)
	outputLocalPath := filepath.Join(h.env.LocalRoot, outputStoragePath)
	if err := os.MkdirAll(filepath.Dir(outputLocalPath), 0o755); err != nil {
		return NodeResult{}, err
	}
	if err := h.media.Execute(ctx, inputPath, outputLocalPath, task.Config); err != nil {
		return NodeResult{}, err
	}
	info, err := os.Stat(outputLocalPath)
	if err != nil {
		return NodeResult{}, fmt.Errorf("handler did not produce output: %w", err)
	}
	storageBackend, storagePath := h.outputStorage(outputLocalPath, outputStoragePath)
	artifactID, err := h.env.Store.CreateIntermediateArtifact(ctx, store.CreateArtifactInput{
		JobID:           task.JobID,
		NodeExecutionID: task.NodeExecutionID,
		Kind:            contracts.ArtifactKindIntermediate,
		Filename:        filename,
		MimeType:        store.GuessMime(ext),
		FileSize:        info.Size(),
		StorageBackend:  storageBackend,
		StoragePath:     storagePath,
		MediaInfo:       map[string]any{},
	})
	if err != nil {
		return NodeResult{}, fmt.Errorf("create artifact row: %w", err)
	}
	return NodeResult{OutputArtifactID: artifactID}, nil
}
```

- [ ] **Step 4: Add runtime artifact helpers**

Create `internal/worker/artifacts.go`:

```go
package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Ctwqk/videoprocess/internal/store"
)

func (h MediaTaskHandler) resolveInput(ctx context.Context, artifact store.ArtifactRow) (string, func(), error) {
	if artifact.StorageBackend == "local" {
		return artifact.StoragePath, func() {}, nil
	}
	return "", func() {}, fmt.Errorf("storage backend %q is not wired into runtime input resolution", artifact.StorageBackend)
}

func (h MediaTaskHandler) outputStorage(outputLocalPath string, outputStoragePath string) (string, string) {
	if h.env.StorageBackend == "local" || h.env.StorageBackend == "" {
		return "local", outputLocalPath
	}
	return h.env.StorageBackend, outputStoragePath
}

func outputExtension(nodeType string, config map[string]any) string {
	if nodeType == "transcode" {
		if raw, ok := config["format"].(string); ok && raw != "" {
			return "." + raw
		}
	}
	if raw, ok := config["output_format"].(string); ok && raw != "" {
		return "." + raw
	}
	return ".mp4"
}

func writeTempFile(prefix string, suffix string, data []byte) (string, func(), error) {
	file, err := os.CreateTemp("", prefix+"_*"+suffix)
	if err != nil {
		return "", func() {}, err
	}
	path := file.Name()
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return "", func() {}, err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return "", func() {}, err
	}
	return path, func() { _ = os.Remove(path) }, nil
}
```

Remove the unused `context` or `filepath` import if Go reports it unused after the exact implementation.

- [ ] **Step 5: Add NodeType to TrimHandler**

In `internal/worker/handlers/trim.go`, add:

```go
func (h TrimHandler) NodeType() string {
	return "trim"
}
```

- [ ] **Step 6: Verify runtime adapter**

Run:

```bash
go test ./internal/worker -run TestMediaTaskHandlerCreatesArtifactResult -count=1
go test ./internal/worker ./internal/worker/handlers -count=1
go test ./...
```

Expected:

```text
ok  	github.com/Ctwqk/videoprocess/internal/worker
ok  	github.com/Ctwqk/videoprocess/internal/worker/handlers
```

- [ ] **Step 7: Commit**

Run:

```bash
git add internal/worker internal/worker/handlers
git commit -m "feat: add go media worker runtime"
```

## Task 7: Register Trim In The Go Worker Binary

**Files:**
- Modify: `cmd/vp-ffmpeg-worker/main.go`
- Modify: `internal/worker/worker.go`
- Modify: `docs/go-migration-runbook.md`

- [ ] **Step 1: Extend worker config**

In `internal/worker/worker.go`, add fields:

```go
DatabaseURL    string
StorageBackend string
StorageLocalRoot string
```

In `LoadConfig()`, set:

```go
databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
if databaseURL == "" {
	databaseURL = "postgresql://vp:vp_secret@localhost:5435/videoprocess"
}
storageBackend := strings.TrimSpace(os.Getenv("STORAGE_BACKEND"))
if storageBackend == "" {
	storageBackend = "local"
}
storageLocalRoot := strings.TrimSpace(os.Getenv("STORAGE_LOCAL_ROOT"))
if storageLocalRoot == "" {
	storageLocalRoot = "/tmp/vp_storage"
}
```

Return these fields in `Config`.

- [ ] **Step 2: Add worker config test**

Create or extend `internal/worker/worker_test.go`:

```go
package worker

import "testing"

func TestLoadConfigIncludesDatabaseAndStorage(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://vp:test@localhost:5432/videoprocess")
	t.Setenv("STORAGE_BACKEND", "local")
	t.Setenv("STORAGE_LOCAL_ROOT", "/tmp/vp-test")

	cfg := LoadConfig()

	if cfg.DatabaseURL == "" || cfg.StorageLocalRoot != "/tmp/vp-test" {
		t.Fatalf("cfg = %#v", cfg)
	}
}
```

- [ ] **Step 3: Wire store, storage, runner, and trim handler**

In `cmd/vp-ffmpeg-worker/main.go`, add imports:

```go
import (
	"time"

	"github.com/Ctwqk/videoprocess/internal/store"
	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
	"github.com/Ctwqk/videoprocess/internal/worker/handlers"
)
```

After Redis client creation, add:

```go
openCtx, openCancel := context.WithTimeout(ctx, 10*time.Second)
st, err := store.Open(openCtx, cfg.DatabaseURL)
openCancel()
if err != nil {
	slog.Error("open worker database", "error", err)
	os.Exit(1)
}
defer st.Close()

runtimeEnv := worker.RuntimeEnv{
	Store:          st,
	StorageBackend: cfg.StorageBackend,
	LocalRoot:      cfg.StorageLocalRoot,
	WorkerID:       cfg.WorkerID,
	Logger:         slog.Default(),
}
trim := worker.NewMediaTaskHandler(runtimeEnv, handlers.TrimHandler{Runner: vpffmpeg.NewRunner()})
consumer := worker.NewConsumer(client, cfg, trim)
```

Remove the existing `consumer := worker.NewConsumer(client, cfg /* handlers go here as they land */)` line.

- [ ] **Step 4: Verify build and worker tests**

Run:

```bash
go test ./internal/worker ./cmd/vp-ffmpeg-worker -count=1
go build ./cmd/vp-ffmpeg-worker
rm -f vp-ffmpeg-worker
go test ./...
```

Expected:

```text
ok  	github.com/Ctwqk/videoprocess/internal/worker
```

- [ ] **Step 5: Update the runbook**

In `docs/go-migration-runbook.md`, add under "Worker Sidecar Start":

````markdown
The worker now registers the `trim` task handler. It still consumes only
`vp:tasks:ffmpeg_go`; no live jobs reach it until the Python node registry
switches `trim.worker_type` to `ffmpeg_go`.

Before any registry switch, confirm:

```bash
go test ./internal/worker ./internal/worker/handlers ./cmd/vp-ffmpeg-worker
docker compose up -d --build ffmpeg-worker-go
docker compose logs --tail=100 ffmpeg-worker-go
```
````

- [ ] **Step 6: Commit**

Run:

```bash
git add cmd/vp-ffmpeg-worker internal/worker docs/go-migration-runbook.md
git commit -m "feat: register go trim worker"
```

## Task 8: Add API And Worker Parity Smoke Tests

**Files:**
- Create: `tests/go_migration/test_go_api_read_parity.py`
- Create: `tests/go_migration/test_go_trim_worker_smoke.py`
- Modify: `docs/go-migration-runbook.md`

- [ ] **Step 1: Add read-only API parity test**

Create `tests/go_migration/test_go_api_read_parity.py`:

```python
from __future__ import annotations

import os
from typing import Any

import httpx
import pytest


PYTHON_API = os.environ.get("VP_PYTHON_API", "http://127.0.0.1:18080")
GO_API = os.environ.get("VP_GO_API", "http://127.0.0.1:18081")
STRICT = os.environ.get("VP_GO_PARITY_STRICT", "").lower() in {"1", "true", "yes", "on"}


def get_json(base_url: str, path: str) -> Any:
    try:
        response = httpx.get(f"{base_url}{path}", timeout=10)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        if STRICT:
            raise
        pytest.skip(f"{base_url} unavailable for Go parity: {exc}")
    return response.json()


def assert_page_shape(payload: Any) -> None:
    assert isinstance(payload, dict)
    assert isinstance(payload.get("items"), list)
    assert isinstance(payload.get("total"), int)


@pytest.mark.parametrize("path", [
    "/api/v1/pipelines?skip=0&limit=50",
    "/api/v1/templates?skip=0&limit=50",
    "/api/v1/assets?skip=0&limit=50",
    "/api/v1/jobs?skip=0&limit=50",
])
def test_read_page_shape_matches_python_contract(path: str) -> None:
    assert_page_shape(get_json(PYTHON_API, path))
    assert_page_shape(get_json(GO_API, path))


def test_go_readyz_reports_dependencies() -> None:
    payload = get_json(GO_API, "/readyz")
    assert payload["status"] in {"ready", "not_ready"}
    assert "postgres" in payload
```

- [ ] **Step 2: Add opt-in trim worker smoke skeleton**

Create `tests/go_migration/test_go_trim_worker_smoke.py`:

```python
from __future__ import annotations

import os

import httpx
import pytest


STRICT = os.environ.get("VP_GO_WORKER_SMOKE_STRICT", "").lower() in {"1", "true", "yes", "on"}
PYTHON_API = os.environ.get("VP_PYTHON_API", "http://127.0.0.1:18080")


def require_strict() -> None:
    if not STRICT:
        pytest.skip("set VP_GO_WORKER_SMOKE_STRICT=1 after compose services and fixture media are ready")


def test_trim_worker_mixed_mode_smoke_requires_real_job_completion() -> None:
    require_strict()
    health = httpx.get(f"{PYTHON_API}/health", timeout=10)
    health.raise_for_status()
    assert health.json() == {"status": "ok"}
    pytest.fail(
        "Create a source->trim->export fixture job through Python API after Task 9 switches trim to ffmpeg_go; "
        "assert final job status, artifact row, output_artifact_id event, and empty Redis pending count."
    )
```

The failing marker is intentional in strict mode: it prevents a real cutover run from passing before the mixed-mode job creation code is added with a known fixture asset id.

- [ ] **Step 3: Run non-strict parity tests**

Run:

```bash
python3 -m pytest tests/go_migration/test_go_api_read_parity.py tests/go_migration/test_go_trim_worker_smoke.py -q
```

Expected when services are absent:

```text
skipped
```

- [ ] **Step 4: Run strict API parity with services up**

Run:

```bash
docker compose up -d --build api api-go postgres redis minio
VP_GO_PARITY_STRICT=1 python3 -m pytest tests/go_migration/test_go_api_read_parity.py -q
```

Expected:

```text
passed
```

- [ ] **Step 5: Commit**

Run:

```bash
git add tests/go_migration docs/go-migration-runbook.md
git commit -m "test: add go migration parity smokes"
```

## Task 9: First Trim Cutover Gate

**Files:**
- Modify: `backend/app/node_registry/builtin/trim.py`
- Modify: `tests/go_migration/test_go_trim_worker_smoke.py`
- Modify: `docs/go-migration-runbook.md`

- [ ] **Step 1: Complete the strict mixed-mode smoke with a real fixture job**

Replace the full contents of `tests/go_migration/test_go_trim_worker_smoke.py` with:

```python
from __future__ import annotations

import os
import time
from typing import Any

import httpx
import pytest


STRICT = os.environ.get("VP_GO_WORKER_SMOKE_STRICT", "").lower() in {"1", "true", "yes", "on"}
PYTHON_API = os.environ.get("VP_PYTHON_API", "http://127.0.0.1:18080")


def require_strict() -> None:
    if not STRICT:
        pytest.skip("set VP_GO_WORKER_SMOKE_STRICT=1 after compose services and fixture media are ready")
    if not os.environ.get("VP_GO_SMOKE_ASSET_ID"):
        pytest.fail("VP_GO_SMOKE_ASSET_ID must point to an existing video asset id")


def post_json(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    response = httpx.post(f"{PYTHON_API}{path}", json=payload, timeout=20)
    response.raise_for_status()
    return response.json()


def get_json(path: str) -> dict[str, Any]:
    response = httpx.get(f"{PYTHON_API}{path}", timeout=20)
    response.raise_for_status()
    return response.json()


def wait_for_job(job_id: str) -> dict[str, Any]:
    deadline = time.time() + 180
    last_payload: dict[str, Any] = {}
    while time.time() < deadline:
        last_payload = get_json(f"/api/v1/jobs/{job_id}")
        if last_payload["status"] in {"SUCCEEDED", "FAILED", "CANCELLED", "PARTIALLY_FAILED"}:
            return last_payload
        time.sleep(2)
    pytest.fail(f"job {job_id} did not finish before timeout; last payload={last_payload}")


def test_trim_worker_mixed_mode_smoke_requires_real_job_completion() -> None:
    require_strict()
    asset_id = os.environ["VP_GO_SMOKE_ASSET_ID"]
    pipeline_payload = {
        "name": "go-trim-smoke",
        "description": "Mixed-mode smoke: Python orchestrator dispatches trim to ffmpeg_go.",
        "definition": {
            "nodes": [
                {
                    "id": "source_1",
                    "type": "source",
                    "position": {"x": 0, "y": 0},
                    "data": {
                        "label": "Source",
                        "config": {"asset_id": asset_id, "media_type": "video"},
                        "asset_id": asset_id,
                    },
                },
                {
                    "id": "trim_1",
                    "type": "trim",
                    "position": {"x": 260, "y": 0},
                    "data": {
                        "label": "Trim",
                        "config": {"start_time": "0", "duration": "1", "output_format": "mp4"},
                    },
                },
                {
                    "id": "export_1",
                    "type": "export",
                    "position": {"x": 520, "y": 0},
                    "data": {
                        "label": "Export",
                        "config": {"output_dir": "/tmp/vp_autoflow_exports", "filename": "go-trim-smoke.mp4"},
                    },
                },
            ],
            "edges": [
                {"id": "e1", "source": "source_1", "target": "trim_1", "sourceHandle": "output", "targetHandle": "input"},
                {"id": "e2", "source": "trim_1", "target": "export_1", "sourceHandle": "output", "targetHandle": "input"},
            ],
            "viewport": {"x": 0, "y": 0, "zoom": 1},
        },
        "is_template": False,
        "template_tags": [],
    }

    pipeline = post_json("/api/v1/pipelines", pipeline_payload)
    job = post_json("/api/v1/jobs", {"pipeline_id": pipeline["id"], "inputs": {}})
    final_job = wait_for_job(job["id"])

    assert final_job["status"] == "SUCCEEDED", final_job
    trim_nodes = [node for node in final_job["node_executions"] if node["node_id"] == "trim_1"]
    assert len(trim_nodes) == 1
    assert trim_nodes[0]["status"] == "SUCCEEDED"
    assert trim_nodes[0]["output_artifact_id"]
```

- [ ] **Step 2: Change only trim worker type**

In `backend/app/node_registry/builtin/trim.py`, change:

```python
worker_type="ffmpeg",
```

to:

```python
worker_type="ffmpeg_go",
```

- [ ] **Step 3: Run the full cutover gate**

Run:

```bash
go test ./...
cd backend && python3 -m pytest tests/test_go_contract_fixtures.py -q
cd ..
docker compose up -d --build api ffmpeg-worker ffmpeg-worker-go redis postgres minio
VP_GO_SMOKE_ASSET_ID="$(curl -fsS http://127.0.0.1:18080/api/v1/assets?limit=1 | python3 -c 'import json,sys; p=json.load(sys.stdin); print(p["items"][0]["id"])')"
VP_GO_WORKER_SMOKE_STRICT=1 VP_GO_SMOKE_ASSET_ID="$VP_GO_SMOKE_ASSET_ID" python3 -m pytest tests/go_migration/test_go_trim_worker_smoke.py -q
```

Expected:

```text
go test ./... passes
backend contract fixture passes
strict trim worker smoke passes
```

- [ ] **Step 4: Run rollback drill**

Revert the single registry line locally:

```bash
git diff -- backend/app/node_registry/builtin/trim.py
```

Expected diff before committing:

```diff
-    worker_type="ffmpeg",
+    worker_type="ffmpeg_go",
```

Test rollback by temporarily changing the line back to `worker_type="ffmpeg"`, then run:

```bash
docker compose up -d --build api ffmpeg-worker redis postgres minio
VP_GO_SMOKE_ASSET_ID="$(curl -fsS http://127.0.0.1:18080/api/v1/assets?limit=1 | python3 -c 'import json,sys; p=json.load(sys.stdin); print(p["items"][0]["id"])')"
VP_GO_WORKER_SMOKE_STRICT=1 VP_GO_SMOKE_ASSET_ID="$VP_GO_SMOKE_ASSET_ID" python3 -m pytest tests/go_migration/test_go_trim_worker_smoke.py -q
```

Expected:

```text
strict trim worker smoke passes while only Python ffmpeg-worker is running
```

- [ ] **Step 5: Commit cutover**

Run:

```bash
git add backend/app/node_registry/builtin/trim.py tests/go_migration/test_go_trim_worker_smoke.py docs/go-migration-runbook.md
git commit -m "feat: route trim to go ffmpeg worker"
```

## Final Verification Checklist

Run before calling this milestone complete:

```bash
go test ./...
cd backend
python3 -m pytest tests/test_go_contract_fixtures.py -q
python3 -m ruff check . || true
python3 -m mypy app || true
cd ..
python3 -m pytest tests/go_migration/test_go_api_read_parity.py -q
```

Run only after compose services and a fixture asset are ready:

```bash
VP_GO_PARITY_STRICT=1 python3 -m pytest tests/go_migration/test_go_api_read_parity.py -q
VP_GO_SMOKE_ASSET_ID="$(curl -fsS http://127.0.0.1:18080/api/v1/assets?limit=1 | python3 -c 'import json,sys; p=json.load(sys.stdin); print(p["items"][0]["id"])')"
VP_GO_WORKER_SMOKE_STRICT=1 VP_GO_SMOKE_ASSET_ID="$VP_GO_SMOKE_ASSET_ID" python3 -m pytest tests/go_migration/test_go_trim_worker_smoke.py -q
```

Frontend is unchanged in this milestone. Run frontend checks only if frontend files are changed:

```bash
cd frontend
npm install
npm run build
npm run lint || true
```

## Self-Review Notes

- Spec coverage: Phase 0 maps to Task 1 and Task 2; Phase 1 maps to Task 2, Task 3, and Task 8; Phase 2 maps to Task 4 through Task 9. Phase 3 production semantics are partially addressed by cancellation contract correction, while PEL reclaim, heartbeat, affinity, and concurrency remain outside this milestone and must be planned as the next PR group before multi-node production traffic.
- Type consistency: `NodeResult.OutputArtifactID`, `ErrConfirmedCancellation`, `RuntimeEnv`, `TaskStore`, and `MediaTaskHandler` are introduced before use by `cmd/vp-ffmpeg-worker`.
- Cutover safety: `backend/app/node_registry/builtin/trim.py` is touched only in Task 9 after strict mixed-mode tests exist.
