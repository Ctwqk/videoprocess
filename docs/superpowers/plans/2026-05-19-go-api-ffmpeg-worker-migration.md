# Go API And Ffmpeg Worker Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build Go sidecar services for the VideoProcess API control plane and pure ffmpeg worker path, then migrate traffic through parity-tested cutover gates.

**Architecture:** Add `vp-api-go` and `vp-ffmpeg-worker-go` beside the current Python services. Reuse the existing Postgres schema, Alembic migrations, Redis Streams, storage paths, frontend `/api/v1` contract, and Python vision/ASR/TTS workers. Migrate by contracts first, then read APIs, write APIs, orchestrator, worker runner, and handler batches.

**Tech Stack:** Go 1.24+, `net/http`, `github.com/go-chi/chi/v5`, `github.com/jackc/pgx/v5/pgxpool`, `github.com/redis/go-redis/v9`, `github.com/minio/minio-go/v7`, `log/slog`, Docker Compose, existing Python pytest fixtures.

---

## File Structure

- Create `go.mod`: root Go module for API and worker binaries.
- Create `cmd/vp-api/main.go`: Go API entrypoint.
- Create `cmd/vp-ffmpeg-worker/main.go`: Go ffmpeg worker entrypoint.
- Create `internal/config/config.go`: environment parsing compatible with Python settings.
- Create `internal/contracts/*.go`: HTTP, DB, pipeline, job, artifact, node, and AutoFlow structs.
- Create `internal/store/*.go`: Postgres queries over the existing schema.
- Create `internal/redisstream/*.go`: Redis Streams task/event helpers.
- Create `internal/storage/*.go`: local and MinIO storage implementations.
- Create `internal/pipeline/*.go`: node registry, validation, topological sort, capability manifest.
- Create `internal/httpapi/*.go`: route groups and handlers.
- Create `internal/orchestrator/*.go`: job start, event listener, recovery, dispatch.
- Create `internal/worker/*.go`: worker loop, task processing, cancellation, artifact creation.
- Create `internal/worker/ffmpeg/*.go`: ffmpeg/ffprobe runner and encode argument helpers.
- Create `internal/worker/handlers/*.go`: first-wave pure ffmpeg handlers.
- Create `backend/tests/golden/go_migration/*.json`: fixture inputs/outputs shared by Python and Go parity tests.
- Create `tests/go_migration/test_go_api_parity.py`: Python-side parity smoke tests for Go API sidecar.
- Create `backend/Dockerfile.api-go`: Go API Docker image.
- Create `backend/Dockerfile.ffmpeg-worker-go`: Go ffmpeg worker Docker image.
- Modify `docker-compose.yml`: add sidecar services without replacing Python `api` or `ffmpeg-worker`.
- Modify `docker-compose.gpu.yml`: add optional GPU environment to `ffmpeg-worker-go`.
- Modify `backend/app/node_registry/builtin/*.py`: switch individual node worker types from `ffmpeg` to `ffmpeg_go` only after their Go handlers pass.

## Task 1: Contract Snapshot Fixtures

**Files:**
- Create: `backend/tests/golden/go_migration/pipeline_basic.json`
- Create: `backend/tests/golden/go_migration/pipeline_validation_basic.valid.json`
- Create: `backend/tests/golden/go_migration/job_task_ffmpeg.json`
- Create: `backend/tests/golden/go_migration/node_types_subset.json`
- Create: `backend/tests/test_go_contract_fixtures.py`

- [ ] **Step 1: Write fixture files**

Create `backend/tests/golden/go_migration/pipeline_basic.json`:

```json
{
  "nodes": [
    {
      "id": "source_1",
      "type": "source",
      "position": {"x": 0, "y": 0},
      "data": {
        "label": "Source",
        "config": {"asset_id": "00000000-0000-0000-0000-000000000001", "media_type": "video"},
        "asset_id": "00000000-0000-0000-0000-000000000001"
      }
    },
    {
      "id": "trim_1",
      "type": "trim",
      "position": {"x": 260, "y": 0},
      "data": {
        "label": "Trim",
        "config": {"start_time": "0", "duration": "2", "output_format": "mp4"}
      }
    },
    {
      "id": "export_1",
      "type": "export",
      "position": {"x": 520, "y": 0},
      "data": {
        "label": "Export Preview",
        "config": {"output_dir": "/tmp/vp_autoflow_exports", "filename": "basic.mp4"}
      }
    }
  ],
  "edges": [
    {
      "id": "e-source_1-trim_1",
      "source": "source_1",
      "target": "trim_1",
      "sourceHandle": "output",
      "targetHandle": "input"
    },
    {
      "id": "e-trim_1-export_1",
      "source": "trim_1",
      "target": "export_1",
      "sourceHandle": "output",
      "targetHandle": "input"
    }
  ],
  "viewport": {"x": 0, "y": 0, "zoom": 1}
}
```

Create `backend/tests/golden/go_migration/pipeline_validation_basic.valid.json`:

```json
{
  "valid": true,
  "errors": [],
  "warnings": []
}
```

Create `backend/tests/golden/go_migration/job_task_ffmpeg.json`:

```json
{
  "job_id": "00000000-0000-0000-0000-000000000101",
  "node_execution_id": "00000000-0000-0000-0000-000000000201",
  "node_id": "trim_1",
  "node_type": "trim",
  "config": "{\"start_time\":\"0\",\"duration\":\"2\",\"output_format\":\"mp4\"}",
  "input_artifacts": "{\"input\":\"00000000-0000-0000-0000-000000000301\"}",
  "preferred_hosts": "[]",
  "affinity_enqueued_at": "1779120000",
  "affinity_bounces": "0"
}
```

Create `backend/tests/golden/go_migration/node_types_subset.json`:

```json
[
  {
    "type_name": "trim",
    "display_name": "Trim",
    "category": "transform",
    "worker_type": "ffmpeg"
  },
  {
    "type_name": "smart_trim",
    "display_name": "Smart Trim",
    "category": "ai_transform",
    "worker_type": "vision"
  }
]
```

- [ ] **Step 2: Write Python fixture validation test**

Create `backend/tests/test_go_contract_fixtures.py`:

```python
import json
from pathlib import Path

from app.orchestrator.dag import validate_pipeline
from app.schemas.pipeline import PipelineDefinition


FIXTURE_DIR = Path(__file__).parent / "golden" / "go_migration"


def test_pipeline_basic_fixture_matches_python_validation_contract():
    definition = PipelineDefinition.model_validate_json(
        (FIXTURE_DIR / "pipeline_basic.json").read_text(encoding="utf-8")
    )

    result = validate_pipeline(definition)
    expected = json.loads(
        (FIXTURE_DIR / "pipeline_validation_basic.valid.json").read_text(encoding="utf-8")
    )

    assert result.model_dump(mode="json") == expected


def test_task_fixture_uses_existing_redis_payload_keys():
    task = json.loads((FIXTURE_DIR / "job_task_ffmpeg.json").read_text(encoding="utf-8"))

    assert sorted(task) == [
        "affinity_bounces",
        "affinity_enqueued_at",
        "config",
        "input_artifacts",
        "job_id",
        "node_execution_id",
        "node_id",
        "node_type",
        "preferred_hosts",
    ]
    assert json.loads(task["config"])["output_format"] == "mp4"
    assert json.loads(task["input_artifacts"])["input"] == "00000000-0000-0000-0000-000000000301"
```

- [ ] **Step 3: Run fixture tests**

Run:

```bash
cd backend
python3 -m pytest tests/test_go_contract_fixtures.py -q
```

Expected:

```text
2 passed
```

- [ ] **Step 4: Commit**

```bash
git add backend/tests/golden/go_migration backend/tests/test_go_contract_fixtures.py
git commit -m "test: capture go migration contracts"
```

## Task 2: Go Module And Config

**Files:**
- Create: `go.mod`
- Create: `internal/config/config.go`
- Create: `internal/config/config_test.go`

- [ ] **Step 1: Initialize Go module**

Run:

```bash
go mod init github.com/Ctwqk/videoprocess
go mod edit -go=1.24
```

Expected: `go.mod` contains module `github.com/Ctwqk/videoprocess`.

- [ ] **Step 2: Create config implementation**

Create `internal/config/config.go`:

```go
package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	DeployMode                         string
	DatabaseURL                        string
	RedisURL                           string
	StorageBackend                     string
	StorageLocalRoot                   string
	MinIOEndpoint                      string
	MinIOAccessKey                     string
	MinIOSecretKey                     string
	MinIOBucket                        string
	MinIOSecure                        bool
	APIHost                            string
	APIPort                            int
	ExoWatchdogURL                     string
	YouTubeManagerURL                  string
	PlatformBrowserManagerURL          string
	XPlatformBrowserManagerURL         string
	BilibiliPlatformBrowserManagerURL  string
	XiaohongshuPlatformBrowserManagerURL string
	EmbeddingGatewayURL                string
	QdrantURL                          string
	MaterialQdrantCollection           string
	VisionEmbeddingURL                  string
	SmartTrimDefaultWorkerType         string
	VideoScheduleDefaultState          string
	VideoUseGPU                        bool
	VideoUseVideotoolbox               bool
	VideoGPUFallbackToCPU              bool
}

func Load() Config {
	return Config{
		DeployMode:                         env("DEPLOY_MODE", "shared"),
		DatabaseURL:                        env("DATABASE_URL", "postgresql://vp:vp_secret@localhost:5435/videoprocess"),
		RedisURL:                           env("REDIS_URL", "redis://localhost:6379/0"),
		StorageBackend:                     env("STORAGE_BACKEND", "local"),
		StorageLocalRoot:                   env("STORAGE_LOCAL_ROOT", "/tmp/vp_storage"),
		MinIOEndpoint:                      env("MINIO_ENDPOINT", "localhost:9000"),
		MinIOAccessKey:                     env("MINIO_ACCESS_KEY", "minioadmin"),
		MinIOSecretKey:                     env("MINIO_SECRET_KEY", "minioadmin"),
		MinIOBucket:                        env("MINIO_BUCKET", "videoprocess"),
		MinIOSecure:                        boolEnv("MINIO_SECURE", false),
		APIHost:                            env("API_HOST", "0.0.0.0"),
		APIPort:                            intEnv("API_PORT", 8080),
		ExoWatchdogURL:                     env("EXO_WATCHDOG_URL", "http://localhost:8000"),
		YouTubeManagerURL:                  env("YOUTUBE_MANAGER_URL", "http://localhost:8899"),
		PlatformBrowserManagerURL:          env("PLATFORM_BROWSER_MANAGER_URL", "http://localhost:8898"),
		XPlatformBrowserManagerURL:         env("X_PLATFORM_BROWSER_MANAGER_URL", ""),
		BilibiliPlatformBrowserManagerURL:  env("BILIBILI_PLATFORM_BROWSER_MANAGER_URL", ""),
		XiaohongshuPlatformBrowserManagerURL: env("XIAOHONGSHU_PLATFORM_BROWSER_MANAGER_URL", ""),
		EmbeddingGatewayURL:                env("EMBEDDING_GATEWAY_URL", "http://localhost:8080"),
		QdrantURL:                          env("QDRANT_URL", "http://localhost:6333"),
		MaterialQdrantCollection:           env("MATERIAL_QDRANT_COLLECTION", "videoprocess_material_clips"),
		VisionEmbeddingURL:                 env("VISION_EMBEDDING_URL", ""),
		SmartTrimDefaultWorkerType:         env("SMART_TRIM_DEFAULT_WORKER_TYPE", "vision"),
		VideoScheduleDefaultState:          env("VIDEO_SCHEDULE_DEFAULT_STATE", "OPEN"),
		VideoUseGPU:                        boolEnv("VIDEO_USE_GPU", false),
		VideoUseVideotoolbox:               boolEnv("VIDEO_USE_VIDEOTOOLBOX", false),
		VideoGPUFallbackToCPU:              boolEnv("VIDEO_GPU_FALLBACK_TO_CPU", true),
	}
}

func env(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func boolEnv(key string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if value == "" {
		return fallback
	}
	return value == "1" || value == "true" || value == "yes" || value == "on"
}

func intEnv(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
```

- [ ] **Step 3: Create config tests**

Create `internal/config/config_test.go`:

```go
package config

import "testing"

func TestLoadUsesPythonCompatibleDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("REDIS_URL", "")
	t.Setenv("STORAGE_BACKEND", "")

	cfg := Load()

	if cfg.StorageBackend != "local" {
		t.Fatalf("StorageBackend = %q", cfg.StorageBackend)
	}
	if cfg.StorageLocalRoot != "/tmp/vp_storage" {
		t.Fatalf("StorageLocalRoot = %q", cfg.StorageLocalRoot)
	}
	if cfg.VideoGPUFallbackToCPU != true {
		t.Fatalf("VideoGPUFallbackToCPU = false")
	}
}

func TestBoolEnvAcceptsPythonStyleTruth(t *testing.T) {
	t.Setenv("VIDEO_USE_GPU", "yes")

	cfg := Load()

	if !cfg.VideoUseGPU {
		t.Fatalf("VideoUseGPU = false")
	}
}
```

- [ ] **Step 4: Run tests**

Run:

```bash
go test ./internal/config
```

Expected:

```text
ok  	github.com/Ctwqk/videoprocess/internal/config
```

- [ ] **Step 5: Commit**

```bash
git add go.mod internal/config
git commit -m "feat: add go config foundation"
```

## Task 3: Shared Contracts And Pipeline Validation

**Files:**
- Create: `internal/contracts/pipeline.go`
- Create: `internal/contracts/job.go`
- Create: `internal/contracts/artifact.go`
- Create: `internal/pipeline/registry.go`
- Create: `internal/pipeline/validate.go`
- Create: `internal/pipeline/validate_test.go`

- [ ] **Step 1: Create pipeline contracts**

Create `internal/contracts/pipeline.go`:

```go
package contracts

type PipelineNodeData struct {
	Label   string         `json:"label"`
	Config  map[string]any `json:"config"`
	AssetID *string        `json:"asset_id"`
}

type PipelineNode struct {
	ID       string             `json:"id"`
	Type     string             `json:"type"`
	Position map[string]float64 `json:"position"`
	Data     PipelineNodeData   `json:"data"`
}

type PipelineEdge struct {
	ID           string `json:"id"`
	Source       string `json:"source"`
	Target       string `json:"target"`
	SourceHandle string `json:"sourceHandle"`
	TargetHandle string `json:"targetHandle"`
}

type PipelineDefinition struct {
	Nodes    []PipelineNode     `json:"nodes"`
	Edges    []PipelineEdge     `json:"edges"`
	Viewport map[string]float64 `json:"viewport"`
}

type ValidationError struct {
	Type       string   `json:"type"`
	Message    string   `json:"message"`
	NodeID     *string  `json:"node_id"`
	EdgeID     *string  `json:"edge_id"`
	Nodes      []string `json:"nodes"`
	SourcePort *string  `json:"source_port"`
	TargetPort *string  `json:"target_port"`
	ParamName  *string  `json:"param_name"`
}

type ValidationWarning struct {
	Type    string  `json:"type"`
	Message string  `json:"message"`
	NodeID  *string `json:"node_id"`
}

type ValidationResult struct {
	Valid    bool                `json:"valid"`
	Errors   []ValidationError   `json:"errors"`
	Warnings []ValidationWarning `json:"warnings"`
}
```

Create `internal/contracts/job.go`:

```go
package contracts

type JobStatus string

const (
	JobStatusPending         JobStatus = "PENDING"
	JobStatusValidating      JobStatus = "VALIDATING"
	JobStatusPlanning        JobStatus = "PLANNING"
	JobStatusRunning         JobStatus = "RUNNING"
	JobStatusSucceeded       JobStatus = "SUCCEEDED"
	JobStatusFailed          JobStatus = "FAILED"
	JobStatusCancelled       JobStatus = "CANCELLED"
	JobStatusPartiallyFailed JobStatus = "PARTIALLY_FAILED"
	JobStatusWaitingWindow   JobStatus = "WAITING_WINDOW"
)

type NodeStatus string

const (
	NodeStatusPending   NodeStatus = "PENDING"
	NodeStatusQueued    NodeStatus = "QUEUED"
	NodeStatusRunning   NodeStatus = "RUNNING"
	NodeStatusSucceeded NodeStatus = "SUCCEEDED"
	NodeStatusFailed    NodeStatus = "FAILED"
	NodeStatusSkipped   NodeStatus = "SKIPPED"
	NodeStatusCancelled NodeStatus = "CANCELLED"
)
```

Create `internal/contracts/artifact.go`:

```go
package contracts

type ArtifactKind string

const (
	ArtifactKindIntermediate ArtifactKind = "intermediate"
	ArtifactKindFinal        ArtifactKind = "final"
)
```

- [ ] **Step 2: Create node registry**

Create `internal/pipeline/registry.go`:

```go
package pipeline

type PortType string

const (
	PortVideo         PortType = "video"
	PortAudio         PortType = "audio"
	PortImage         PortType = "image"
	PortSubtitle      PortType = "subtitle"
	PortAnyMedia      PortType = "any_media"
	PortSearchResults PortType = "search_results"
	PortURLValue      PortType = "url_value"
	PortAssetValue    PortType = "asset_value"
)

type PortDefinition struct {
	Name        string   `json:"name"`
	PortType    PortType `json:"port_type"`
	Required    bool     `json:"required"`
	Description string   `json:"description"`
}

type NodeTypeDefinition struct {
	TypeName    string           `json:"type_name"`
	DisplayName string           `json:"display_name"`
	Category    string           `json:"category"`
	Inputs      []PortDefinition `json:"inputs"`
	Outputs     []PortDefinition `json:"outputs"`
	WorkerType  string           `json:"worker_type"`
}

func BuiltinRegistry() map[string]NodeTypeDefinition {
	return map[string]NodeTypeDefinition{
		"source": {
			TypeName: "source", DisplayName: "Source", Category: "source", WorkerType: "none",
			Outputs: []PortDefinition{{Name: "output", PortType: PortAnyMedia, Required: true}},
		},
		"trim": {
			TypeName: "trim", DisplayName: "Trim", Category: "transform", WorkerType: "ffmpeg",
			Inputs: []PortDefinition{{Name: "input", PortType: PortVideo, Required: true}},
			Outputs: []PortDefinition{{Name: "output", PortType: PortVideo, Required: true}},
		},
		"transcode": {
			TypeName: "transcode", DisplayName: "Transcode", Category: "transform", WorkerType: "ffmpeg",
			Inputs: []PortDefinition{{Name: "input", PortType: PortAnyMedia, Required: true}},
			Outputs: []PortDefinition{{Name: "output", PortType: PortAnyMedia, Required: true}},
		},
		"export": {
			TypeName: "export", DisplayName: "Export", Category: "output", WorkerType: "ffmpeg",
			Inputs: []PortDefinition{{Name: "input", PortType: PortAnyMedia, Required: true}},
			Outputs: []PortDefinition{{Name: "output", PortType: PortAnyMedia, Required: true}},
		},
		"smart_trim": {
			TypeName: "smart_trim", DisplayName: "Smart Trim", Category: "ai_transform", WorkerType: "vision",
			Inputs: []PortDefinition{{Name: "input", PortType: PortVideo, Required: true}},
			Outputs: []PortDefinition{{Name: "output", PortType: PortVideo, Required: true}},
		},
	}
}
```

- [ ] **Step 3: Create validator**

Create `internal/pipeline/validate.go`:

```go
package pipeline

import "github.com/Ctwqk/videoprocess/internal/contracts"

func Validate(def contracts.PipelineDefinition) contracts.ValidationResult {
	errors := make([]contracts.ValidationError, 0)
	warnings := make([]contracts.ValidationWarning, 0)
	registry := BuiltinRegistry()
	nodesByID := map[string]contracts.PipelineNode{}
	inDegree := map[string]int{}
	adjacency := map[string][]string{}

	for _, node := range def.Nodes {
		nodesByID[node.ID] = node
		inDegree[node.ID] = 0
		if _, ok := registry[node.Type]; !ok {
			id := node.ID
			errors = append(errors, contracts.ValidationError{
				Type: "unknown_node_type", NodeID: &id,
				Message: "Unknown node type '" + node.Type + "'",
			})
		}
	}

	for _, edge := range def.Edges {
		source, sourceOK := nodesByID[edge.Source]
		target, targetOK := nodesByID[edge.Target]
		if !sourceOK {
			id := edge.ID
			errors = append(errors, contracts.ValidationError{Type: "invalid_edge", EdgeID: &id, Message: "Edge source '" + edge.Source + "' does not exist"})
			continue
		}
		if !targetOK {
			id := edge.ID
			errors = append(errors, contracts.ValidationError{Type: "invalid_edge", EdgeID: &id, Message: "Edge target '" + edge.Target + "' does not exist"})
			continue
		}
		if !portsCompatible(registry, source.Type, edge.SourceHandle, target.Type, edge.TargetHandle) {
			id := edge.ID
			sourcePort := edge.SourceHandle
			targetPort := edge.TargetHandle
			errors = append(errors, contracts.ValidationError{
				Type: "port_type_mismatch", EdgeID: &id, SourcePort: &sourcePort, TargetPort: &targetPort,
				Message: "Cannot connect '" + edge.SourceHandle + "' to '" + edge.TargetHandle + "' (type mismatch)",
			})
		}
		adjacency[edge.Source] = append(adjacency[edge.Source], edge.Target)
		inDegree[edge.Target]++
	}

	order := topologicalOrder(inDegree, adjacency)
	if len(order) < len(nodesByID) {
		cycleNodes := make([]string, 0)
		seen := map[string]bool{}
		for _, id := range order {
			seen[id] = true
		}
		for id := range nodesByID {
			if !seen[id] {
				cycleNodes = append(cycleNodes, id)
			}
		}
		errors = append(errors, contracts.ValidationError{Type: "cycle_detected", Nodes: cycleNodes, Message: "Cycle detected"})
	}

	return contracts.ValidationResult{Valid: len(errors) == 0, Errors: errors, Warnings: warnings}
}

func topologicalOrder(inDegree map[string]int, adjacency map[string][]string) []string {
	remaining := map[string]int{}
	queue := make([]string, 0)
	for id, degree := range inDegree {
		remaining[id] = degree
		if degree == 0 {
			queue = append(queue, id)
		}
	}
	order := make([]string, 0, len(inDegree))
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		order = append(order, id)
		for _, downstream := range adjacency[id] {
			remaining[downstream]--
			if remaining[downstream] == 0 {
				queue = append(queue, downstream)
			}
		}
	}
	return order
}

func portsCompatible(registry map[string]NodeTypeDefinition, sourceType, sourcePort, targetType, targetPort string) bool {
	src, srcOK := registry[sourceType]
	tgt, tgtOK := registry[targetType]
	if !srcOK || !tgtOK {
		return false
	}
	srcPort, srcFound := findOutput(src, sourcePort)
	tgtPort, tgtFound := findInput(tgt, targetPort)
	if !srcFound || !tgtFound {
		return false
	}
	return tgtPort.PortType == PortAnyMedia || srcPort.PortType == PortAnyMedia || srcPort.PortType == tgtPort.PortType
}

func findOutput(def NodeTypeDefinition, name string) (PortDefinition, bool) {
	for _, port := range def.Outputs {
		if port.Name == name {
			return port, true
		}
	}
	return PortDefinition{}, false
}

func findInput(def NodeTypeDefinition, name string) (PortDefinition, bool) {
	for _, port := range def.Inputs {
		if port.Name == name {
			return port, true
		}
	}
	return PortDefinition{}, false
}
```

- [ ] **Step 4: Create validation test**

Create `internal/pipeline/validate_test.go`:

```go
package pipeline

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/Ctwqk/videoprocess/internal/contracts"
)

func TestValidateMatchesBasicGoldenFixture(t *testing.T) {
	var def contracts.PipelineDefinition
	raw, err := os.ReadFile(filepath.Join("..", "..", "backend", "tests", "golden", "go_migration", "pipeline_basic.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &def); err != nil {
		t.Fatal(err)
	}

	result := Validate(def)

	if !result.Valid {
		t.Fatalf("result.Valid = false, errors = %#v", result.Errors)
	}
	if len(result.Errors) != 0 {
		t.Fatalf("errors = %#v", result.Errors)
	}
	if len(result.Warnings) != 0 {
		t.Fatalf("warnings = %#v", result.Warnings)
	}
}
```

- [ ] **Step 5: Run tests**

Run:

```bash
go test ./internal/contracts ./internal/pipeline
```

Expected:

```text
ok  	github.com/Ctwqk/videoprocess/internal/pipeline
```

- [ ] **Step 6: Commit**

```bash
git add internal/contracts internal/pipeline
git commit -m "feat: add go pipeline contracts"
```

## Task 4: Go API Shell And Read-Only Routes

**Files:**
- Create: `cmd/vp-api/main.go`
- Create: `internal/httpapi/router.go`
- Create: `internal/httpapi/health.go`
- Create: `internal/httpapi/node_types.go`
- Create: `internal/httpapi/pipelines.go`
- Create: `internal/httpapi/jobs.go`
- Create: `internal/httpapi/httpapi_test.go`

- [ ] **Step 1: Create API router**

Create `internal/httpapi/router.go`:

```go
package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type Server struct{}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Get("/health", s.health)
	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/node-types", s.listNodeTypes)
		r.Get("/node-types/{typeName}", s.getNodeType)
		r.Get("/pipelines", s.listPipelines)
		r.Get("/templates", s.listTemplates)
		r.Get("/jobs", s.listJobs)
	})
	r.Route("/internal/schedule/video", func(r chi.Router) {
		r.Get("/status", s.scheduleStatus)
	})
	return r
}
```

Create `internal/httpapi/health.go`:

```go
package httpapi

import (
	"encoding/json"
	"net/http"
)

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
```

Create `internal/httpapi/node_types.go`:

```go
package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/Ctwqk/videoprocess/internal/pipeline"
)

func (s *Server) listNodeTypes(w http.ResponseWriter, r *http.Request) {
	registry := pipeline.BuiltinRegistry()
	items := make([]pipeline.NodeTypeDefinition, 0, len(registry))
	for _, item := range registry {
		items = append(items, item)
	}
	writeJSON(w, http.StatusOK, items)
}

func (s *Server) getNodeType(w http.ResponseWriter, r *http.Request) {
	typeName := chi.URLParam(r, "typeName")
	item, ok := pipeline.BuiltinRegistry()[typeName]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"detail": "Node type not found"})
		return
	}
	writeJSON(w, http.StatusOK, item)
}
```

Create `internal/httpapi/pipelines.go`:

```go
package httpapi

import "net/http"

func (s *Server) listPipelines(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"items": []any{}, "total": 0})
}

func (s *Server) listTemplates(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"items": []any{}, "total": 0})
}
```

Create `internal/httpapi/jobs.go`:

```go
package httpapi

import "net/http"

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"items": []any{}, "total": 0})
}

func (s *Server) scheduleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"state": "OPEN"})
}
```

- [ ] **Step 2: Create API entrypoint**

Create `cmd/vp-api/main.go`:

```go
package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/Ctwqk/videoprocess/internal/config"
	"github.com/Ctwqk/videoprocess/internal/httpapi"
)

func main() {
	cfg := config.Load()
	server := httpapi.NewServer()
	addr := fmt.Sprintf("%s:%d", cfg.APIHost, cfg.APIPort)
	slog.Info("starting vp-api-go", "addr", addr)
	if err := http.ListenAndServe(addr, server.Router()); err != nil {
		slog.Error("vp-api-go stopped", "error", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 3: Create HTTP tests**

Create `internal/httpapi/httpapi_test.go`:

```go
package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealth(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	NewServer().Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["status"] != "ok" {
		t.Fatalf("status payload = %#v", payload)
	}
}

func TestNodeTypesIncludesTrim(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/node-types/trim", nil)
	rec := httptest.NewRecorder()

	NewServer().Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if !json.Valid(rec.Body.Bytes()) {
		t.Fatalf("invalid JSON: %s", rec.Body.String())
	}
}
```

- [ ] **Step 4: Run tests and build**

Run:

```bash
go test ./internal/httpapi ./cmd/vp-api
go build ./cmd/vp-api
```

Expected:

```text
ok  	github.com/Ctwqk/videoprocess/internal/httpapi
```

- [ ] **Step 5: Commit**

```bash
git add cmd/vp-api internal/httpapi
git commit -m "feat: add go api shell"
```

## Task 5: Store, Storage, And Redis Stream Foundations

**Files:**
- Create: `internal/store/store.go`
- Create: `internal/storage/storage.go`
- Create: `internal/redisstream/streams.go`
- Create: `internal/redisstream/streams_test.go`

- [ ] **Step 1: Create store interface**

Create `internal/store/store.go`:

```go
package store

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	Pool *pgxpool.Pool
}

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{Pool: pool}, nil
}

func (s *Store) Close() {
	if s != nil && s.Pool != nil {
		s.Pool.Close()
	}
}
```

- [ ] **Step 2: Create storage interface**

Create `internal/storage/storage.go`:

```go
package storage

import (
	"context"
	"os"
	"path/filepath"
)

type Backend interface {
	Read(ctx context.Context, path string) ([]byte, error)
	Save(ctx context.Context, path string, data []byte) error
	Exists(ctx context.Context, path string) (bool, error)
	LocalPath(path string) (string, bool)
}

type LocalBackend struct {
	Root string
}

func (b LocalBackend) fullPath(path string) string {
	return filepath.Join(b.Root, path)
}

func (b LocalBackend) Read(ctx context.Context, path string) ([]byte, error) {
	return os.ReadFile(b.fullPath(path))
}

func (b LocalBackend) Save(ctx context.Context, path string, data []byte) error {
	full := b.fullPath(path)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return err
	}
	return os.WriteFile(full, data, 0o644)
}

func (b LocalBackend) Exists(ctx context.Context, path string) (bool, error) {
	_, err := os.Stat(b.fullPath(path))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (b LocalBackend) LocalPath(path string) (string, bool) {
	return b.fullPath(path), true
}
```

- [ ] **Step 3: Create Redis stream constants**

Create `internal/redisstream/streams.go`:

```go
package redisstream

import (
	"context"

	"github.com/redis/go-redis/v9"
)

const EventStream = "vp:events"

func TaskStream(workerType string) string {
	return "vp:tasks:" + workerType
}

type NodeEvent struct {
	Event           string
	JobID           string
	NodeExecutionID string
	OutputArtifactID string
	Error           string
}

func PublishNodeCompleted(ctx context.Context, client *redis.Client, event NodeEvent) error {
	return client.XAdd(ctx, &redis.XAddArgs{
		Stream: EventStream,
		Values: map[string]any{
			"event": "node_completed",
			"job_id": event.JobID,
			"node_execution_id": event.NodeExecutionID,
			"output_artifact_id": event.OutputArtifactID,
		},
	}).Err()
}

func PublishNodeFailed(ctx context.Context, client *redis.Client, event NodeEvent) error {
	errorText := event.Error
	if len(errorText) > 2000 {
		errorText = errorText[:2000]
	}
	return client.XAdd(ctx, &redis.XAddArgs{
		Stream: EventStream,
		Values: map[string]any{
			"event": "node_failed",
			"job_id": event.JobID,
			"node_execution_id": event.NodeExecutionID,
			"error": errorText,
		},
	}).Err()
}
```

- [ ] **Step 4: Create stream tests**

Create `internal/redisstream/streams_test.go`:

```go
package redisstream

import "testing"

func TestTaskStream(t *testing.T) {
	if got := TaskStream("ffmpeg_go"); got != "vp:tasks:ffmpeg_go" {
		t.Fatalf("TaskStream = %q", got)
	}
}
```

- [ ] **Step 5: Run tests**

Run:

```bash
go test ./internal/store ./internal/storage ./internal/redisstream
```

Expected: all packages pass or report no test files except `internal/redisstream`.

- [ ] **Step 6: Commit**

```bash
git add internal/store internal/storage internal/redisstream
git commit -m "feat: add go persistence foundations"
```

## Task 6: Orchestrator Parity Slice

**Files:**
- Create: `internal/orchestrator/dag.go`
- Create: `internal/orchestrator/dispatch.go`
- Create: `internal/orchestrator/dag_test.go`

- [ ] **Step 1: Implement dependency helpers**

Create `internal/orchestrator/dag.go`:

```go
package orchestrator

import "github.com/Ctwqk/videoprocess/internal/contracts"

func LeafNodeIDs(def contracts.PipelineDefinition) map[string]bool {
	hasOutgoing := map[string]bool{}
	for _, edge := range def.Edges {
		hasOutgoing[edge.Source] = true
	}
	leaf := map[string]bool{}
	for _, node := range def.Nodes {
		if !hasOutgoing[node.ID] {
			leaf[node.ID] = true
		}
	}
	return leaf
}

func DependencyMap(def contracts.PipelineDefinition) map[string][]string {
	deps := map[string][]string{}
	for _, node := range def.Nodes {
		deps[node.ID] = []string{}
	}
	for _, edge := range def.Edges {
		deps[edge.Target] = append(deps[edge.Target], edge.Source)
	}
	return deps
}
```

Create `internal/orchestrator/dispatch.go`:

```go
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
		"job_id": p.JobID,
		"node_execution_id": p.NodeExecutionID,
		"node_id": p.NodeID,
		"node_type": p.NodeType,
		"config": p.ConfigJSON,
		"input_artifacts": p.InputArtifactsJSON,
		"preferred_hosts": p.PreferredHostsJSON,
		"affinity_enqueued_at": p.AffinityEnqueuedAt,
		"affinity_bounces": p.AffinityBounces,
	}
}
```

- [ ] **Step 2: Test Redis payload keys**

Create `internal/orchestrator/dag_test.go`:

```go
package orchestrator

import "testing"

func TestTaskPayloadRedisValuesUsesPythonKeys(t *testing.T) {
	values := TaskPayload{
		JobID: "job", NodeExecutionID: "node-exec", NodeID: "trim_1", NodeType: "trim",
		ConfigJSON: "{}", InputArtifactsJSON: "{}", PreferredHostsJSON: "[]",
		AffinityEnqueuedAt: "1779120000", AffinityBounces: "0",
	}.RedisValues()

	for _, key := range []string{"job_id", "node_execution_id", "node_id", "node_type", "config", "input_artifacts", "preferred_hosts", "affinity_enqueued_at", "affinity_bounces"} {
		if _, ok := values[key]; !ok {
			t.Fatalf("missing key %s", key)
		}
	}
}
```

- [ ] **Step 3: Run tests**

Run:

```bash
go test ./internal/orchestrator
```

Expected:

```text
ok  	github.com/Ctwqk/videoprocess/internal/orchestrator
```

- [ ] **Step 4: Commit**

```bash
git add internal/orchestrator
git commit -m "feat: add go orchestrator contracts"
```

## Task 7: Go Ffmpeg Runner

**Files:**
- Create: `internal/worker/ffmpeg/runner.go`
- Create: `internal/worker/ffmpeg/encode.go`
- Create: `internal/worker/ffmpeg/encode_test.go`

- [ ] **Step 1: Implement encode argument parity**

Create `internal/worker/ffmpeg/encode.go`:

```go
package ffmpeg

import "strconv"

type EncodeConfig struct {
	UseGPU          bool
	UseVideotoolbox bool
	Codec           string
	Preset          string
	CRF             int
	Bitrate         string
	MP4Compatible   bool
}

func VideoEncodeArgs(cfg EncodeConfig) []string {
	codec := preferredCodec(cfg)
	args := []string{"-c:v", codec}
	if codec == "libx264" || codec == "libx265" {
		args = append(args, "-crf", itoa(cfg.CRF), "-preset", defaultString(cfg.Preset, "medium"))
	}
	if codec == "h264_nvenc" || codec == "hevc_nvenc" {
		args = append(args, "-rc:v", "vbr", "-cq:v", itoa(cfg.CRF), "-preset", defaultString(cfg.Preset, "medium"))
	}
	if codec == "h264_videotoolbox" || codec == "hevc_videotoolbox" {
		args = append(args, "-b:v", defaultString(cfg.Bitrate, "6M"))
	}
	if cfg.Bitrate != "" && codec != "h264_videotoolbox" && codec != "hevc_videotoolbox" {
		args = append(args, "-b:v", cfg.Bitrate)
	}
	if cfg.MP4Compatible {
		args = append(args, "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-color_primaries", "bt709", "-color_trc", "bt709", "-colorspace", "bt709")
	}
	return args
}

func preferredCodec(cfg EncodeConfig) string {
	codec := defaultString(cfg.Codec, "libx264")
	if cfg.UseGPU {
		if codec == "libx264" {
			return "h264_nvenc"
		}
		if codec == "libx265" {
			return "hevc_nvenc"
		}
	}
	if cfg.UseVideotoolbox {
		if codec == "libx264" {
			return "h264_videotoolbox"
		}
		if codec == "libx265" {
			return "hevc_videotoolbox"
		}
	}
	return codec
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func itoa(value int) string {
	if value == 0 {
		value = 20
	}
	return strconv.Itoa(value)
}
```

- [ ] **Step 2: Implement runner**

Create `internal/worker/ffmpeg/runner.go`:

```go
package ffmpeg

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
)

type Runner struct {
	Binary string
}

func NewRunner() Runner {
	return Runner{Binary: "ffmpeg"}
}

func (r Runner) Run(ctx context.Context, args []string) (string, error) {
	binary := r.Binary
	if binary == "" {
		binary = "ffmpeg"
	}
	fullArgs := append([]string{"-y", "-hide_banner"}, args...)
	cmd := exec.CommandContext(ctx, binary, fullArgs...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return stderr.String(), fmt.Errorf("ffmpeg failed: %w: %s", err, tail(stderr.String(), 2000))
	}
	return stderr.String(), nil
}

func tail(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[len(value)-max:]
}
```

- [ ] **Step 3: Test encode args**

Create `internal/worker/ffmpeg/encode_test.go`:

```go
package ffmpeg

import (
	"reflect"
	"testing"
)

func TestVideoEncodeArgsCPU(t *testing.T) {
	got := VideoEncodeArgs(EncodeConfig{Codec: "libx264", Preset: "medium", CRF: 20, MP4Compatible: true})
	want := []string{"-c:v", "libx264", "-crf", "20", "-preset", "medium", "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-color_primaries", "bt709", "-color_trc", "bt709", "-colorspace", "bt709"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v", got)
	}
}

func TestVideoEncodeArgsNVENC(t *testing.T) {
	got := VideoEncodeArgs(EncodeConfig{UseGPU: true, Codec: "libx264", Preset: "medium", CRF: 20, MP4Compatible: true})
	want := []string{"-c:v", "h264_nvenc", "-rc:v", "vbr", "-cq:v", "20", "-preset", "medium", "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-color_primaries", "bt709", "-color_trc", "bt709", "-colorspace", "bt709"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v", got)
	}
}
```

- [ ] **Step 4: Run tests**

Run:

```bash
go test ./internal/worker/ffmpeg
```

Expected:

```text
ok  	github.com/Ctwqk/videoprocess/internal/worker/ffmpeg
```

- [ ] **Step 5: Commit**

```bash
git add internal/worker/ffmpeg
git commit -m "feat: add go ffmpeg runner"
```

## Task 8: Go Worker Loop And First Handler

**Files:**
- Create: `cmd/vp-ffmpeg-worker/main.go`
- Create: `internal/worker/worker.go`
- Create: `internal/worker/handlers/trim.go`
- Create: `internal/worker/handlers/trim_test.go`

- [ ] **Step 1: Implement trim handler**

Create `internal/worker/handlers/trim.go`:

```go
package handlers

import (
	"context"

	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
)

type TrimHandler struct {
	Runner vpffmpeg.Runner
}

func (h TrimHandler) Args(inputPath, outputPath string, config map[string]any) []string {
	start := stringValue(config["start_time"], "00:00:00")
	end := stringValue(config["end_time"], "")
	duration := stringValue(config["duration"], "")

	args := []string{}
	if start != "" {
		args = append(args, "-ss", start)
	}
	args = append(args, "-i", inputPath)
	if end != "" {
		args = append(args, "-to", end)
	} else if duration != "" {
		args = append(args, "-t", duration)
	}
	args = append(args, "-map", "0:v:0", "-map", "0:a?")
	args = append(args, vpffmpeg.VideoEncodeArgs(vpffmpeg.EncodeConfig{
		Codec:         "libx264",
		Preset:        "slow",
		CRF:           18,
		MP4Compatible: true,
	})...)
	args = append(args, "-c:a", "aac", outputPath)
	return args
}

func (h TrimHandler) Execute(ctx context.Context, inputPath, outputPath string, config map[string]any) error {
	runner := h.Runner
	if runner.Binary == "" {
		runner = vpffmpeg.NewRunner()
	}
	_, err := runner.Run(ctx, h.Args(inputPath, outputPath, config))
	return err
}

func stringValue(value any, fallback string) string {
	if raw, ok := value.(string); ok {
		return raw
	}
	return fallback
}
```

- [ ] **Step 2: Test trim args**

Create `internal/worker/handlers/trim_test.go`:

```go
package handlers

import (
	"reflect"
	"testing"

	vpffmpeg "github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
)

func TestTrimArgsMatchPythonHandler(t *testing.T) {
	handler := TrimHandler{Runner: vpffmpeg.Runner{Binary: "ffmpeg"}}
	got := handler.Args("/input.mp4", "/output.mp4", map[string]any{"start_time": "1.250", "duration": "2.500"})
	want := []string{"-ss", "1.250", "-i", "/input.mp4", "-t", "2.500", "-map", "0:v:0", "-map", "0:a?", "-c:v", "libx264", "-crf", "18", "-preset", "slow", "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-color_primaries", "bt709", "-color_trc", "bt709", "-colorspace", "bt709", "-c:a", "aac", "/output.mp4"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v", got)
	}
}
```

- [ ] **Step 3: Add worker shell**

Create `internal/worker/worker.go`:

```go
package worker

type Config struct {
	WorkerType string
	WorkerID   string
}

func DefaultWorkerType() string {
	return "ffmpeg_go"
}
```

Create `cmd/vp-ffmpeg-worker/main.go`:

```go
package main

import (
	"log/slog"

	"github.com/Ctwqk/videoprocess/internal/worker"
)

func main() {
	slog.Info("starting vp-ffmpeg-worker-go", "worker_type", worker.DefaultWorkerType())
	select {}
}
```

- [ ] **Step 4: Run tests and build**

Run:

```bash
go test ./internal/worker/... ./cmd/vp-ffmpeg-worker
go build ./cmd/vp-ffmpeg-worker
```

Expected: tests pass and the worker binary builds.

- [ ] **Step 5: Commit**

```bash
git add cmd/vp-ffmpeg-worker internal/worker
git commit -m "feat: add go ffmpeg worker shell"
```

## Task 9: Docker Compose Sidecars

**Files:**
- Create: `backend/Dockerfile.api-go`
- Create: `backend/Dockerfile.ffmpeg-worker-go`
- Modify: `docker-compose.yml`
- Modify: `docker-compose.gpu.yml`

- [ ] **Step 1: Create Go API Dockerfile**

Create `backend/Dockerfile.api-go`:

```dockerfile
FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/vp-api-go ./cmd/vp-api

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/vp-api-go /usr/local/bin/vp-api-go
EXPOSE 8080
CMD ["vp-api-go"]
```

- [ ] **Step 2: Create Go worker Dockerfile**

Create `backend/Dockerfile.ffmpeg-worker-go`:

```dockerfile
FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/vp-ffmpeg-worker-go ./cmd/vp-ffmpeg-worker

FROM nvidia/cuda:12.4.1-cudnn-runtime-ubuntu22.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates ffmpeg && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/vp-ffmpeg-worker-go /usr/local/bin/vp-ffmpeg-worker-go
CMD ["vp-ffmpeg-worker-go"]
```

- [ ] **Step 3: Add compose sidecars**

Add these services to `docker-compose.yml` without removing `api` or `ffmpeg-worker`:

```yaml
  api-go:
    build:
      context: .
      dockerfile: backend/Dockerfile.api-go
    container_name: vp_api_go
    restart: unless-stopped
    environment:
      DEPLOY_MODE: ${DEPLOY_MODE:-shared}
      DATABASE_URL: ${VP_DATABASE_URL_GO:-postgres://vp:${VP_POSTGRES_PASSWORD:-vp_secret}@host.docker.internal:5435/videoprocess}
      REDIS_URL: ${VP_REDIS_URL:-redis://host.docker.internal:6380/0}
      STORAGE_BACKEND: ${STORAGE_BACKEND:-local}
      STORAGE_LOCAL_ROOT: /data/storage
      MINIO_ENDPOINT: ${VP_MINIO_ENDPOINT:-host.docker.internal:9000}
      MINIO_ACCESS_KEY: ${MINIO_ROOT_USER:-minioadmin}
      MINIO_SECRET_KEY: ${MINIO_ROOT_PASSWORD:-minioadmin}
      MINIO_BUCKET: videoprocess
      EXO_WATCHDOG_URL: ${VP_WATCHDOG_URL:-http://host.docker.internal:8000}
      EMBEDDING_GATEWAY_URL: ${VP_EMBEDDING_GATEWAY_URL:-http://host.docker.internal:8080}
      QDRANT_URL: ${VP_QDRANT_URL:-http://host.docker.internal:6333}
      VIDEO_SCHEDULE_DEFAULT_STATE: ${VIDEO_SCHEDULE_DEFAULT_STATE:-OPEN}
      YOUTUBE_MANAGER_URL: ${YOUTUBE_MANAGER_URL:-http://youtube-manager:8899}
      PLATFORM_BROWSER_MANAGER_URL: http://host.docker.internal:8898
      X_PLATFORM_BROWSER_MANAGER_URL: http://host.docker.internal:8898
      XIAOHONGSHU_PLATFORM_BROWSER_MANAGER_URL: http://host.docker.internal:8897
      API_PORT: "8080"
    volumes:
      - ${VP_STORAGE_ROOT:-./k8s-data/storage}:/data/storage
    ports:
      - "${API_GO_PORT:-18081}:8080"
    extra_hosts:
      - "host.docker.internal:host-gateway"
    networks:
      - vp_internal

  ffmpeg-worker-go:
    build:
      context: .
      dockerfile: backend/Dockerfile.ffmpeg-worker-go
    container_name: vp_ffmpeg_worker_go_1
    restart: unless-stopped
    environment:
      DEPLOY_MODE: ${DEPLOY_MODE:-shared}
      REDIS_URL: ${VP_REDIS_URL:-redis://host.docker.internal:6380/0}
      WORKER_TYPE: ffmpeg_go
      WORKER_CONCURRENCY: "2"
      STORAGE_LOCAL_ROOT: /data/storage
      DATABASE_URL: ${VP_DATABASE_URL_GO:-postgres://vp:${VP_POSTGRES_PASSWORD:-vp_secret}@host.docker.internal:5435/videoprocess}
      VIDEO_USE_GPU: ${VIDEO_USE_GPU:-false}
      VIDEO_GPU_FALLBACK_TO_CPU: ${VIDEO_GPU_FALLBACK_TO_CPU:-true}
    volumes:
      - ${VP_STORAGE_ROOT:-./k8s-data/storage}:/data/storage
    extra_hosts:
      - "host.docker.internal:host-gateway"
    networks:
      - vp_internal
```

- [ ] **Step 4: Add GPU override**

Add this service override to `docker-compose.gpu.yml`:

```yaml
  ffmpeg-worker-go:
    gpus: all
    environment:
      VIDEO_USE_GPU: "true"
      VIDEO_GPU_FALLBACK_TO_CPU: "true"
      NVIDIA_VISIBLE_DEVICES: all
      NVIDIA_DRIVER_CAPABILITIES: compute,video,utility
```

- [ ] **Step 5: Build sidecars**

Run:

```bash
docker compose build api-go ffmpeg-worker-go
```

Expected: both images build.

- [ ] **Step 6: Commit**

```bash
git add backend/Dockerfile.api-go backend/Dockerfile.ffmpeg-worker-go docker-compose.yml docker-compose.gpu.yml
git commit -m "feat: add go sidecar containers"
```

## Task 10: Cutover Gates And Verification

**Files:**
- Create: `tests/go_migration/test_go_api_parity.py`
- Create: `docs/go-migration-runbook.md`

- [ ] **Step 1: Create API parity smoke**

Create `tests/go_migration/test_go_api_parity.py`:

```python
import os

import httpx


PYTHON_API = os.environ.get("VP_PYTHON_API", "http://127.0.0.1:18080")
GO_API = os.environ.get("VP_GO_API", "http://127.0.0.1:18081")


def get_json(base_url: str, path: str):
    response = httpx.get(f"{base_url}{path}", timeout=10)
    response.raise_for_status()
    return response.json()


def test_health_parity():
    assert get_json(PYTHON_API, "/health") == get_json(GO_API, "/health")


def test_node_types_trim_exists_in_both_services():
    python_payload = get_json(PYTHON_API, "/api/v1/node-types/trim")
    go_payload = get_json(GO_API, "/api/v1/node-types/trim")
    assert python_payload["type_name"] == go_payload["type_name"]
    assert python_payload["worker_type"] == go_payload["worker_type"]
```

- [ ] **Step 2: Create runbook**

Create `docs/go-migration-runbook.md`:

```markdown
# Go Migration Runbook

## Sidecar Start

```bash
docker compose up -d --build api api-go frontend
curl -fsS http://127.0.0.1:18080/health
curl -fsS http://127.0.0.1:18081/health
```

## Worker Sidecar Start

```bash
docker compose up -d --build ffmpeg-worker ffmpeg-worker-go
docker compose logs --tail=100 ffmpeg-worker-go
```

## GPU Sidecar Start

```bash
docker compose -f docker-compose.yml -f docker-compose.gpu.yml up -d --build ffmpeg-worker-go
docker compose exec ffmpeg-worker-go ffmpeg -hide_banner -encoders | grep -E 'h264_nvenc|hevc_nvenc'
```

## Rollback

```bash
docker compose stop api-go ffmpeg-worker-go
docker compose up -d api ffmpeg-worker
```

## Cutover Rule

Only point frontend/proxy traffic at `api-go` after health, node-types, pipelines, jobs, assets, and deterministic AutoFlow parity tests pass. Only switch a node to `ffmpeg_go` after its fixture media test passes and a visible smoke output is inspectable.
```
```

- [ ] **Step 3: Run parity smoke**

Run after both APIs are running:

```bash
python3 -m pytest tests/go_migration/test_go_api_parity.py -q
```

Expected: parity tests pass for the routes implemented so far.

- [ ] **Step 4: Run required checks**

Run:

```bash
go test ./...
cd backend && python3 -m pytest
cd ../frontend && npm install && npm run build
git diff --check
```

Expected: Go tests pass, Python tests pass, frontend builds, and `git diff --check` has no output.

- [ ] **Step 5: Commit**

```bash
git add tests/go_migration/test_go_api_parity.py docs/go-migration-runbook.md
git commit -m "test: add go migration cutover gates"
```

## Task 11: First Node Worker-Type Switch

**Files:**
- Modify: `backend/app/node_registry/builtin/trim.py`
- Test: `backend/tests/autoflow/test_node_registration.py`

- [ ] **Step 1: Change only `trim` to `ffmpeg_go`**

Modify `backend/app/node_registry/builtin/trim.py` so the definition includes:

```python
worker_type="ffmpeg_go",
```

Keep every input, output, and parameter unchanged.

- [ ] **Step 2: Run node registry tests**

Run:

```bash
cd backend
python3 -m pytest tests/autoflow/test_node_registration.py tests/worker/test_media_quality_args.py -q
```

Expected: registry and worker argument tests pass.

- [ ] **Step 3: Run mixed-mode smoke**

Run:

```bash
docker compose up -d --build api ffmpeg-worker-go frontend
curl -fsS http://127.0.0.1:18080/health
```

Then create and execute a `source -> trim -> export` pipeline through the existing UI or API. Confirm `vp_ffmpeg_worker_go_1` logs show the `trim` node and the resulting artifact downloads.

- [ ] **Step 4: Commit**

```bash
git add backend/app/node_registry/builtin/trim.py
git commit -m "feat: route trim node to go ffmpeg worker"
```

## Self-Review Checklist

- Spec coverage: this plan covers sidecar architecture, contract snapshots, Go config, pipeline validation, API shell, store/storage/Redis foundations, orchestrator contracts, ffmpeg runner, first handler, Docker sidecars, cutover gates, and first node switch.
- Placeholder scan: the plan contains no unfinished placeholder sections, and every code-changing task includes concrete file paths and code blocks.
- Type consistency: package names use `config`, `contracts`, `pipeline`, `httpapi`, `store`, `storage`, `redisstream`, `orchestrator`, `worker`, `ffmpeg`, and `handlers` consistently.
- Scope check: this plan intentionally stops after the first worker-type switch. Additional tasks should repeat the tested pattern for the remaining pure ffmpeg handlers before migrating network/platform handlers, AutoFlow graph planning, or Channel Agent.
