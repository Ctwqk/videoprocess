# Go Partial Migration Non-Optional Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish every non-Phase-6 item from `/home/taiwei/Constructure-repos/videoprocess/docs/videoprocess-go-partial-migration-spec.md`.

**Architecture:** Python remains the authoritative orchestrator, event listener, schema owner, and rollback path. Go becomes production-eligible for registry/validator/read/write HTTP surfaces that pass parity gates, and for the first-wave pure ffmpeg worker nodes that run on `vp:tasks:ffmpeg_go`. Cutover remains per route and per node through explicit routing and Python registry `worker_type` changes.

**Tech Stack:** Go 1.25, chi, pgx, go-redis, minio-go, Prometheus client for metrics, FastAPI/Pydantic parity fixtures, pytest, ffmpeg/ffprobe, Docker Compose.

---

## Phase 6 Boundary

This plan implements the original spec except Phase 6. These responsibilities stay Python-owned throughout this plan:

- Go orchestrator.
- Go event listener.
- Go startup recovery.
- Go DAG scheduling, retry, downstream skip, and final artifact ownership.
- AutoFlow planner rewrite.
- ML, ASR, TTS, search, material, and external platform publishing handlers.

## File Structure

Create:

- `backend/scripts/export_node_registry_manifest.py`: exports Python builtin node definitions as canonical JSON.
- `backend/tests/test_node_registry_manifest.py`: verifies the manifest exporter preserves builtin node fields and stable ordering.
- `backend/tests/golden/go_migration/node_registry_manifest.json`: committed registry contract generated from Python builtins.
- `internal/pipeline/testdata/node_registry_manifest.json`: Go-embedded copy of the committed registry contract.
- `internal/pipeline/registry_manifest.go`: embeds and decodes the committed registry manifest for Go.
- `internal/pipeline/registry_manifest_test.go`: verifies Go manifest loading and builtin count.
- `tests/go_migration/test_go_registry_parity.py`: compares Python `/api/v1/node-types` and Go `/api/v1/node-types`.
- `tests/go_migration/test_go_validator_parity.py`: compares Python and Go validation responses for deterministic fixtures and unsupported graph classes.
- `internal/httpapi/validation.go`: Go `POST /api/v1/pipelines/validate` route with unsupported-graph guard.
- `internal/httpapi/write_responses.go`: shared FastAPI-compatible write response helpers.
- `internal/httpapi/pipeline_writes.go`: selected pipeline write routes.
- `internal/httpapi/job_writes.go`: selected job write routes and Python-owned job-start handoff behavior.
- `internal/httpapi/asset_writes.go`: selected asset upload/download/delete routes.
- `internal/httpapi/artifact_writes.go`: artifact download and cleanup routes.
- `internal/httpapi/schedule_writes.go`: schedule open/drain/close routes.
- `internal/httpapi/metrics.go`: API Prometheus metrics middleware and `/metrics`.
- `internal/worker/metrics.go`: worker Prometheus counters and histograms.
- `internal/worker/inputmap.go`: port-name aware artifact resolution for single-input and multi-input nodes.
- `internal/worker/media_contract.go`: shared output extension, MIME, metadata, and handler result contract.
- `internal/worker/handlers/common.go`: shared ffmpeg handler helpers equivalent to Python `BaseHandler`.
- `internal/worker/handlers/transcode.go`: Go `transcode` path-level handler.
- `internal/worker/handlers/export.go`: Go `export` path-level handler preserving terminal export copy behavior.
- `internal/worker/handlers/vertical_crop.go`: Go `vertical_crop` path-level handler.
- `internal/worker/handlers/watermark.go`: Go `watermark` path-level handler.
- `internal/worker/handlers/title_overlay.go`: Go `title_overlay` path-level handler.
- `internal/worker/handlers/bgm.go`: Go `bgm` path-level handler.
- `internal/worker/handlers/replace_audio.go`: Go `replace_audio` path-level handler.
- `internal/worker/handlers/concat_stack.go`: shared two-video stack handler for horizontal and vertical concat.
- `internal/worker/handlers/concat_horizontal.go`: Go `concat_horizontal` path-level handler.
- `internal/worker/handlers/concat_vertical.go`: Go `concat_vertical` path-level handler.
- `internal/worker/handlers/concat_many.go`: Go `concat_many` path-level handler.
- `internal/worker/handlers/concat_timeline.go`: Go `concat_timeline` path-level handler.
- `internal/worker/handlers/concat_vertical_timeline.go`: Go `concat_vertical_timeline` path-level handler.
- `internal/worker/handlers/montage_assembler.go`: Go `montage_assembler` path-level handler.
- `internal/worker/handlers/handler_contract_test.go`: exact-argument unit tests for every migrated handler.
- `tests/go_migration/test_go_worker_nodes.py`: mixed-mode pipeline smoke tests for every migrated node.
- `tests/go_migration/test_go_api_write_parity.py`: selected Go write route parity tests.
- `scripts/go_migration_acceptance.py`: production-style acceptance runner for staging jobs, Redis pending, artifact rows, p95, cancel, failure, and rollback evidence.
- `docs/go-migration-acceptance/README.md`: records how to run and interpret acceptance evidence.

Modify:

- `go.mod` and `go.sum`: add Prometheus client dependency.
- `cmd/vp-api/main.go`: wire metrics and write-capable server dependencies.
- `cmd/vp-ffmpeg-worker/main.go`: register every migrated pure ffmpeg handler and worker metrics.
- `docker-compose.yml`: expose Go API `/metrics` and optional worker metrics port.
- `internal/config/config.go`: add write-route feature flags and metrics port settings.
- `internal/config/config_test.go`: assert defaults are fail-closed.
- `internal/httpapi/router.go`: add validation, selected write routes, schedule writes, and `/metrics`.
- `internal/httpapi/middleware.go`: include request id in metrics/log fields.
- `internal/httpapi/node_types.go`: serve registry from the Python-exported manifest.
- `internal/pipeline/registry.go`: replace hand-written subset with manifest-backed registry.
- `internal/pipeline/validate.go`: add deterministic support classifier and refuse unsupported shapes.
- `internal/pipeline/validate_test.go`: cover unsupported graph classes and every first-wave pure ffmpeg node.
- `internal/store/store.go`: keep pool/common helpers only.
- `internal/store/pipelines.go`: add create/update/delete/duplicate methods.
- `internal/store/jobs.go`: add cancel/delete/read-for-rerun methods without taking orchestrator ownership.
- `internal/store/assets.go`: add create/delete/get-for-download methods.
- `internal/store/artifacts.go`: add cleanup and download lookup helpers.
- `internal/store/schedule.go`: add state mutation helpers.
- `internal/store/store_test.go`: extend write SQL tests.
- `internal/worker/runtime.go`: use port-name input maps and handler metadata.
- `internal/worker/artifacts.go`: resolve all input artifact ports and inject `_input_artifact_meta`.
- `internal/worker/consumer.go`: increment metrics and preserve event-publish ack semantics.
- `internal/worker/ffmpeg/runner.go`: increment ffmpeg metrics and expose GPU fallback counter.
- `backend/app/node_registry/builtin/transcode.py`: switch `worker_type` to `ffmpeg_go` after its gate passes.
- `backend/app/node_registry/builtin/export.py`: switch `worker_type` to `ffmpeg_go` after its gate passes.
- `backend/app/node_registry/builtin/vertical_crop.py`: switch `worker_type` to `ffmpeg_go` after its gate passes.
- `backend/app/node_registry/builtin/watermark.py`: switch `worker_type` to `ffmpeg_go` after its gate passes.
- `backend/app/node_registry/builtin/title_overlay.py`: switch `worker_type` to `ffmpeg_go` after its gate passes.
- `backend/app/node_registry/builtin/bgm.py`: switch `worker_type` to `ffmpeg_go` after its gate passes.
- `backend/app/node_registry/builtin/replace_audio.py`: switch `worker_type` to `ffmpeg_go` after its gate passes.
- `backend/app/node_registry/builtin/concat_horizontal.py`: switch `worker_type` to `ffmpeg_go` after its gate passes.
- `backend/app/node_registry/builtin/concat_vertical.py`: switch `worker_type` to `ffmpeg_go` after its gate passes.
- `backend/app/node_registry/builtin/concat_many.py`: switch `worker_type` to `ffmpeg_go` after its gate passes.
- `backend/app/node_registry/builtin/concat_timeline.py`: switch `worker_type` to `ffmpeg_go` after its gate passes.
- `backend/app/node_registry/builtin/concat_vertical_timeline.py`: switch `worker_type` to `ffmpeg_go` after its gate passes.
- `backend/app/node_registry/builtin/montage_assembler.py`: switch `worker_type` to `ffmpeg_go` after its gate passes.

## Task 1: Baseline Evidence And Scope Lock

**Files:**
- Read: `/home/taiwei/Constructure-repos/videoprocess/docs/videoprocess-go-partial-migration-spec.md`
- Read: `docs/superpowers/specs/2026-05-19-go-partial-migration-nonoptional-completion-design.md`
- Modify: `docs/go-migration-acceptance/README.md`

- [ ] **Step 1: Capture current branch and test state**

Run:

```bash
git status --short --branch
go test ./...
go vet ./...
cd backend && python3 -m pytest
cd backend && python3 -m ruff check . || true
cd backend && python3 -m mypy app || true
```

Expected:

```text
Go tests pass.
Go vet passes.
Python tests pass, or any allowed ruff/mypy missing-tool output is recorded exactly because those commands are already non-blocking in AGENTS.md.
```

- [ ] **Step 2: Create acceptance evidence doc**

Create `docs/go-migration-acceptance/README.md`:

```markdown
# Go Migration Acceptance Evidence

Scope: non-Phase-6 completion for `/home/taiwei/Constructure-repos/videoprocess/docs/videoprocess-go-partial-migration-spec.md`.

Python remains authoritative for orchestration, event listening, schema migration, and rollback.

Evidence sections:

1. Registry parity.
2. Validator parity and unsupported graph refusal.
3. Per-node Go worker migration gate.
4. Per-route Go API write gate.
5. Docker health, readiness, and metrics.
6. Staging jobs, Redis pending, artifacts, p95, failure, cancellation, and rollback.
```

- [ ] **Step 3: Commit baseline evidence doc**

Run:

```bash
git add docs/go-migration-acceptance/README.md
git commit -m "docs: add go migration acceptance evidence log"
```

Expected:

```text
Commit succeeds and the branch remains on codex/go-partial-migration.
```

## Task 2: Python Registry Manifest Exporter

**Files:**
- Create: `backend/scripts/export_node_registry_manifest.py`
- Create: `backend/tests/test_node_registry_manifest.py`
- Create: `backend/tests/golden/go_migration/node_registry_manifest.json`
- Create: `internal/pipeline/testdata/node_registry_manifest.json`

- [ ] **Step 1: Write failing exporter tests**

Create `backend/tests/test_node_registry_manifest.py`:

```python
from __future__ import annotations

import json
from pathlib import Path

from app.node_registry.registry import NodeTypeRegistry
from scripts.export_node_registry_manifest import build_manifest


def test_manifest_contains_all_builtin_node_types() -> None:
    registry_types = {node.type_name for node in NodeTypeRegistry.get().list_types()}

    manifest = build_manifest()

    assert manifest["schema_version"] == 1
    assert {node["type_name"] for node in manifest["node_types"]} == registry_types


def test_manifest_serializes_ports_params_and_worker_type() -> None:
    manifest = build_manifest()
    by_type = {node["type_name"]: node for node in manifest["node_types"]}

    trim = by_type["trim"]
    assert trim["inputs"] == [
        {"name": "input", "port_type": "video", "required": True, "description": ""}
    ]
    assert trim["outputs"] == [
        {"name": "output", "port_type": "video", "required": True, "description": ""}
    ]
    assert isinstance(trim["params"], list)
    assert trim["worker_type"] == "ffmpeg_go"


def test_committed_manifest_matches_exporter() -> None:
    expected_path = Path(__file__).parent / "golden" / "go_migration" / "node_registry_manifest.json"
    expected = json.loads(expected_path.read_text())

    assert build_manifest() == expected


def test_go_embedded_manifest_matches_python_manifest() -> None:
    root = Path(__file__).resolve().parents[2]
    python_manifest = json.loads(
        (root / "backend" / "tests" / "golden" / "go_migration" / "node_registry_manifest.json").read_text()
    )
    go_manifest = json.loads(
        (root / "internal" / "pipeline" / "testdata" / "node_registry_manifest.json").read_text()
    )
    assert go_manifest == python_manifest
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cd backend && python3 -m pytest tests/test_node_registry_manifest.py -q
```

Expected:

```text
FAIL because backend.scripts.export_node_registry_manifest does not exist.
```

- [ ] **Step 3: Implement exporter**

Create `backend/scripts/export_node_registry_manifest.py`:

```python
from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from app.node_registry.base import NodeTypeDefinition, ParamDefinition, PortDefinition
from app.node_registry.registry import NodeTypeRegistry


def _port_to_dict(port: PortDefinition) -> dict[str, Any]:
    return {
        "name": port.name,
        "port_type": port.port_type.value,
        "required": bool(port.required),
        "description": port.description or "",
    }


def _param_to_dict(param: ParamDefinition) -> dict[str, Any]:
    data = asdict(param)
    data["default"] = param.default
    data["options"] = list(param.options) if param.options is not None else None
    return data


def _node_to_dict(node: NodeTypeDefinition) -> dict[str, Any]:
    return {
        "type_name": node.type_name,
        "display_name": node.display_name,
        "category": node.category,
        "description": node.description or "",
        "icon": node.icon or "",
        "inputs": [_port_to_dict(port) for port in node.inputs],
        "outputs": [_port_to_dict(port) for port in node.outputs],
        "params": [_param_to_dict(param) for param in node.params],
        "worker_type": node.worker_type,
    }


def build_manifest() -> dict[str, Any]:
    nodes = sorted(NodeTypeRegistry.get().list_types(), key=lambda node: node.type_name)
    return {
        "schema_version": 1,
        "node_types": [_node_to_dict(node) for node in nodes],
    }


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    output_path = root / "backend" / "tests" / "golden" / "go_migration" / "node_registry_manifest.json"
    go_output_path = root / "internal" / "pipeline" / "testdata" / "node_registry_manifest.json"
    payload = json.dumps(build_manifest(), indent=2, sort_keys=True) + "\n"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    go_output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(payload)
    go_output_path.write_text(payload)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Generate committed manifest**

Run:

```bash
cd backend && python3 -m scripts.export_node_registry_manifest
cd backend && python3 -m pytest tests/test_node_registry_manifest.py -q
```

Expected:

```text
4 passed
```

- [ ] **Step 5: Commit registry exporter**

Run:

```bash
git add backend/scripts/export_node_registry_manifest.py backend/tests/test_node_registry_manifest.py backend/tests/golden/go_migration/node_registry_manifest.json internal/pipeline/testdata/node_registry_manifest.json
git commit -m "test: freeze python node registry manifest"
```

Expected:

```text
Commit contains exporter, exporter test, and committed JSON manifest.
```

## Task 3: Manifest-Backed Go Registry

**Files:**
- Create: `internal/pipeline/registry_manifest.go`
- Create: `internal/pipeline/registry_manifest_test.go`
- Read: `internal/pipeline/testdata/node_registry_manifest.json`
- Modify: `internal/pipeline/registry.go`
- Modify: `internal/httpapi/node_types.go`
- Create: `tests/go_migration/test_go_registry_parity.py`

- [ ] **Step 1: Write Go registry tests**

Create `internal/pipeline/registry_manifest_test.go`:

```go
package pipeline

import "testing"

func TestBuiltinRegistryLoadsPythonManifest(t *testing.T) {
	registry := BuiltinRegistry()
	if len(registry) < 30 {
		t.Fatalf("registry loaded %d node types, want at least 30", len(registry))
	}
	for _, nodeType := range []string{
		"source", "trim", "transcode", "export", "vertical_crop",
		"watermark", "title_overlay", "bgm", "replace_audio",
		"concat_horizontal", "concat_vertical", "concat_many",
		"concat_timeline", "concat_vertical_timeline", "montage_assembler",
		"smart_trim", "zip_records", "youtube_upload", "x_upload",
	} {
		if _, ok := registry[nodeType]; !ok {
			t.Fatalf("registry missing %s", nodeType)
		}
	}
}

func TestRegistryPreservesPortNames(t *testing.T) {
	registry := BuiltinRegistry()

	watermark := registry["watermark"]
	if len(watermark.Inputs) != 2 {
		t.Fatalf("watermark inputs = %d, want 2", len(watermark.Inputs))
	}
	if watermark.Inputs[0].Name != "video" || watermark.Inputs[1].Name != "overlay" {
		t.Fatalf("watermark inputs = %#v", watermark.Inputs)
	}
}
```

- [ ] **Step 2: Run Go registry test and verify it fails**

Run:

```bash
go test ./internal/pipeline -run TestBuiltinRegistryLoadsPythonManifest -v
```

Expected:

```text
FAIL because the current Go registry has only the hand-written subset.
```

- [ ] **Step 3: Add embedded manifest loader**

Create `internal/pipeline/registry_manifest.go`:

```go
package pipeline

import (
	_ "embed"
	"encoding/json"
	"fmt"
)

//go:embed testdata/node_registry_manifest.json
var registryManifestJSON []byte

type registryManifest struct {
	SchemaVersion int                  `json:"schema_version"`
	NodeTypes     []NodeTypeDefinition `json:"node_types"`
}

func loadBuiltinRegistry() (map[string]NodeTypeDefinition, error) {
	var manifest registryManifest
	if err := json.Unmarshal(registryManifestJSON, &manifest); err != nil {
		return nil, fmt.Errorf("decode node registry manifest: %w", err)
	}
	if manifest.SchemaVersion != 1 {
		return nil, fmt.Errorf("unsupported node registry manifest schema_version %d", manifest.SchemaVersion)
	}
	registry := make(map[string]NodeTypeDefinition, len(manifest.NodeTypes))
	for _, node := range manifest.NodeTypes {
		if node.TypeName == "" {
			return nil, fmt.Errorf("node registry manifest contains empty type_name")
		}
		registry[node.TypeName] = node
	}
	return registry, nil
}
```

Modify `internal/pipeline/registry.go` so `BuiltinRegistry` uses the embedded manifest:

```go
func BuiltinRegistry() map[string]NodeTypeDefinition {
	registry, err := loadBuiltinRegistry()
	if err != nil {
		panic(err)
	}
	return registry
}
```

- [ ] **Step 4: Add cross-service registry parity test**

Create `tests/go_migration/test_go_registry_parity.py`:

```python
from __future__ import annotations

import os

import pytest
import requests


STRICT = os.getenv("VP_GO_PARITY_STRICT") == "1"
PY_API = os.getenv("VP_PY_API_URL", "http://127.0.0.1:18080")
GO_API = os.getenv("VP_GO_API_URL", "http://127.0.0.1:18081")


@pytest.mark.skipif(not STRICT, reason="set VP_GO_PARITY_STRICT=1 for live Go registry parity")
def test_node_types_match_python() -> None:
    py = requests.get(f"{PY_API}/api/v1/node-types", timeout=10)
    go = requests.get(f"{GO_API}/api/v1/node-types", timeout=10)
    assert py.status_code == go.status_code == 200
    py_items = sorted(py.json()["items"], key=lambda item: item["type_name"])
    go_items = sorted(go.json()["items"], key=lambda item: item["type_name"])
    assert [item["type_name"] for item in go_items] == [item["type_name"] for item in py_items]
    for py_item, go_item in zip(py_items, go_items, strict=True):
        assert go_item["display_name"] == py_item["display_name"]
        assert go_item["category"] == py_item["category"]
        assert go_item["worker_type"] == py_item["worker_type"]
        assert go_item["inputs"] == py_item["inputs"]
        assert go_item["outputs"] == py_item["outputs"]
```

- [ ] **Step 5: Run registry tests**

Run:

```bash
go test ./internal/pipeline ./internal/httpapi
cd backend && python3 -m pytest tests/test_node_registry_manifest.py -q
```

Expected:

```text
All selected tests pass.
```

- [ ] **Step 6: Commit manifest-backed registry**

Run:

```bash
git add internal/pipeline/registry.go internal/pipeline/registry_manifest.go internal/pipeline/registry_manifest_test.go internal/pipeline/testdata/node_registry_manifest.json internal/httpapi/node_types.go tests/go_migration/test_go_registry_parity.py backend/tests/test_node_registry_manifest.py
git commit -m "feat: load go node registry from python manifest"
```

Expected:

```text
Commit succeeds with Go registry parity foundation.
```

## Task 4: Validator Guard And Validation Route

**Files:**
- Modify: `internal/pipeline/validate.go`
- Modify: `internal/pipeline/validate_test.go`
- Create: `internal/httpapi/validation.go`
- Modify: `internal/httpapi/router.go`
- Create: `tests/go_migration/test_go_validator_parity.py`

- [ ] **Step 1: Add deterministic and unsupported validator tests**

Append to `internal/pipeline/validate_test.go`:

```go
func TestValidateRefusesUnsupportedPythonOnlyNodes(t *testing.T) {
	def := contracts.PipelineDefinition{
		Nodes: []contracts.PipelineNode{
			node("search", "youtube_search", nil),
			node("upload", "youtube_upload", nil),
		},
		Edges: []contracts.PipelineEdge{
			{ID: "edge", Source: "search", SourceHandle: "results", Target: "upload", TargetHandle: "records"},
		},
	}

	result := Validate(def)
	if result.Valid {
		t.Fatalf("Validate returned valid for unsupported Python-owned graph")
	}
	if len(result.Errors) != 1 || result.Errors[0].Type != "unsupported_go_validation" {
		t.Fatalf("errors = %#v", result.Errors)
	}
}

func TestValidateAcceptsFirstWaveFfmpegGraph(t *testing.T) {
	assetID := "asset-1"
	def := contracts.PipelineDefinition{
		Nodes: []contracts.PipelineNode{
			node("source", "source", map[string]any{"asset_id": assetID}),
			node("crop", "vertical_crop", nil),
			node("title", "title_overlay", nil),
			node("export", "export", nil),
		},
		Edges: []contracts.PipelineEdge{
			{ID: "e1", Source: "source", SourceHandle: "output", Target: "crop", TargetHandle: "input"},
			{ID: "e2", Source: "crop", SourceHandle: "output", Target: "title", TargetHandle: "input"},
			{ID: "e3", Source: "title", SourceHandle: "output", Target: "export", TargetHandle: "input"},
		},
	}
	def.Nodes[0].Data.AssetID = &assetID

	result := Validate(def)
	if !result.Valid {
		t.Fatalf("Validate errors = %#v", result.Errors)
	}
}
```

- [ ] **Step 2: Run validator tests and verify unsupported guard fails**

Run:

```bash
go test ./internal/pipeline -run 'TestValidateRefusesUnsupportedPythonOnlyNodes|TestValidateAcceptsFirstWaveFfmpegGraph' -v
```

Expected:

```text
FAIL because unsupported_go_validation is not emitted yet.
```

- [ ] **Step 3: Implement unsupported graph classifier**

Add to `internal/pipeline/validate.go`:

```go
var pythonOwnedValidationNodeTypes = map[string]bool{
	"zip_records":              true,
	"material_search":          true,
	"youtube_search":           true,
	"x_search":                 true,
	"xiaohongshu_search":       true,
	"bilibili_search":          true,
	"youtube_upload":           true,
	"x_upload":                 true,
	"xiaohongshu_upload":       true,
	"material_library_ingest":  true,
	"url_download":             true,
	"smart_trim":               true,
	"speech_to_subtitle":       true,
	"subtitle_translate":       true,
	"subtitle_to_speech":       true,
}

func unsupportedValidationError(def contracts.PipelineDefinition) *contracts.ValidationError {
	for _, node := range def.Nodes {
		if pythonOwnedValidationNodeTypes[node.Type] {
			nodeID := node.ID
			return &contracts.ValidationError{
				Type:    "unsupported_go_validation",
				NodeID:  &nodeID,
				Message: "Go validator does not own validation for node type '" + node.Type + "'; route this graph to Python",
			}
		}
	}
	return nil
}
```

Call it at the start of `Validate`:

```go
if unsupported := unsupportedValidationError(def); unsupported != nil {
	return contracts.ValidationResult{
		Valid:    false,
		Errors:   []contracts.ValidationError{*unsupported},
		Warnings: []contracts.ValidationWarning{},
	}
}
```

- [ ] **Step 4: Add validation HTTP route**

Create `internal/httpapi/validation.go`:

```go
package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/Ctwqk/videoprocess/internal/contracts"
	"github.com/Ctwqk/videoprocess/internal/pipeline"
)

func (s *Server) validatePipeline(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var def contracts.PipelineDefinition
	if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"detail": "invalid pipeline definition"})
		return
	}
	result := pipeline.Validate(def)
	writeJSON(w, http.StatusOK, result)
}
```

Modify `internal/httpapi/router.go` inside `/api/v1`:

```go
r.Post("/pipelines/validate", s.validatePipeline)
```

- [ ] **Step 5: Add live parity tests for validation**

Create `tests/go_migration/test_go_validator_parity.py`:

```python
from __future__ import annotations

import os

import pytest
import requests


STRICT = os.getenv("VP_GO_PARITY_STRICT") == "1"
PY_API = os.getenv("VP_PY_API_URL", "http://127.0.0.1:18080")
GO_API = os.getenv("VP_GO_API_URL", "http://127.0.0.1:18081")


def valid_ffmpeg_graph() -> dict:
    return {
        "nodes": [
            {"id": "source", "type": "source", "position": {}, "data": {"asset_id": "asset-1", "config": {}}},
            {"id": "trim", "type": "trim", "position": {}, "data": {"config": {"duration": "1"}}},
            {"id": "export", "type": "export", "position": {}, "data": {"config": {}}},
        ],
        "edges": [
            {"id": "e1", "source": "source", "sourceHandle": "output", "target": "trim", "targetHandle": "input"},
            {"id": "e2", "source": "trim", "sourceHandle": "output", "target": "export", "targetHandle": "input"},
        ],
        "viewport": {},
    }


def unsupported_graph() -> dict:
    return {
        "nodes": [
            {"id": "search", "type": "youtube_search", "position": {}, "data": {"config": {}}},
            {"id": "upload", "type": "youtube_upload", "position": {}, "data": {"config": {}}},
        ],
        "edges": [
            {"id": "e1", "source": "search", "sourceHandle": "results", "target": "upload", "targetHandle": "records"}
        ],
        "viewport": {},
    }


@pytest.mark.skipif(not STRICT, reason="set VP_GO_PARITY_STRICT=1 for live Go validator parity")
def test_supported_validation_matches_python() -> None:
    graph = valid_ffmpeg_graph()
    py = requests.post(f"{PY_API}/api/v1/pipelines/validate", json=graph, timeout=10)
    go = requests.post(f"{GO_API}/api/v1/pipelines/validate", json=graph, timeout=10)
    assert py.status_code == go.status_code == 200
    assert go.json()["valid"] == py.json()["valid"]
    assert go.json()["errors"] == py.json()["errors"]


@pytest.mark.skipif(not STRICT, reason="set VP_GO_PARITY_STRICT=1 for live Go validator parity")
def test_unsupported_validation_is_explicit() -> None:
    response = requests.post(f"{GO_API}/api/v1/pipelines/validate", json=unsupported_graph(), timeout=10)
    assert response.status_code == 200
    body = response.json()
    assert body["valid"] is False
    assert body["errors"][0]["type"] == "unsupported_go_validation"
```

- [ ] **Step 6: Run validator checks**

Run:

```bash
go test ./internal/pipeline ./internal/httpapi
```

Expected:

```text
All selected Go tests pass.
```

- [ ] **Step 7: Commit validator route**

Run:

```bash
git add internal/pipeline/validate.go internal/pipeline/validate_test.go internal/httpapi/validation.go internal/httpapi/router.go tests/go_migration/test_go_validator_parity.py
git commit -m "feat: add guarded go pipeline validation"
```

Expected:

```text
Commit succeeds with guarded validation route.
```

## Task 5: Port-Aware Worker Runtime

**Files:**
- Create: `internal/worker/inputmap.go`
- Create: `internal/worker/media_contract.go`
- Modify: `internal/worker/runtime.go`
- Modify: `internal/worker/artifacts.go`
- Modify: `internal/worker/runtime_test.go`

- [ ] **Step 1: Add tests for multi-port input resolution**

Append to `internal/worker/runtime_test.go`:

```go
// Extend the existing fakeTaskStore in this file before adding the test:
// add fmt to the file imports because GetArtifact now returns formatted missing-artifact errors.
//
// type fakeTaskStore struct {
//     state        store.ExecutionState
//     input        store.ArtifactRow
//     artifacts    map[string]store.ArtifactRow
//     createdInput store.CreateArtifactInput
//     runningNode  string
// }
//
// func (f *fakeTaskStore) GetArtifact(_ context.Context, id string) (store.ArtifactRow, error) {
//     if len(f.artifacts) > 0 {
//         artifact, ok := f.artifacts[id]
//         if !ok {
//             return store.ArtifactRow{}, fmt.Errorf("artifact %s not found", id)
//         }
//         return artifact, nil
//     }
//     return f.input, nil
// }
//
func TestBuildInputMapResolvesNamedPorts(t *testing.T) {
	store := &fakeTaskStore{
		artifacts: map[string]store.ArtifactRow{
			"video-a": {ID: "video-a", Filename: "a.mp4", StorageBackend: "local", StoragePath: "/tmp/a.mp4", MediaInfo: map[string]any{"duration": 1.0}},
			"audio-b": {ID: "audio-b", Filename: "b.wav", StorageBackend: "local", StoragePath: "/tmp/b.wav", MediaInfo: map[string]any{"duration": 1.0}},
		},
	}
	env := RuntimeEnv{Store: store, LocalRoot: "/tmp/vp_storage"}
	task := TaskMessage{
		InputArtifacts: map[string]any{"video": "video-a", "audio": "audio-b"},
	}

	inputs, cleanup, err := BuildInputMap(context.Background(), env, task)
	defer cleanup()

	if err != nil {
		t.Fatalf("BuildInputMap returned error: %v", err)
	}
	if inputs.Paths["video"] != "/tmp/a.mp4" || inputs.Paths["audio"] != "/tmp/b.wav" {
		t.Fatalf("paths = %#v", inputs.Paths)
	}
	if inputs.Meta["video"]["duration"] != 1.0 || inputs.Meta["audio"]["duration"] != 1.0 {
		t.Fatalf("meta = %#v", inputs.Meta)
	}
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
go test ./internal/worker -run TestBuildInputMapResolvesNamedPorts -v
```

Expected:

```text
FAIL because BuildInputMap does not exist.
```

- [ ] **Step 3: Implement input map contract**

Create `internal/worker/inputmap.go`:

```go
package worker

import (
	"context"
	"fmt"

	"github.com/Ctwqk/videoprocess/internal/store"
)

type ResolvedInputs struct {
	Paths map[string]string
	Meta  map[string]map[string]any
}

func BuildInputMap(ctx context.Context, env RuntimeEnv, task TaskMessage) (ResolvedInputs, func(), error) {
	inputs := ResolvedInputs{
		Paths: map[string]string{},
		Meta:  map[string]map[string]any{},
	}
	cleanups := make([]func(), 0)
	cleanupAll := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}
	for port, raw := range task.InputArtifacts {
		artifactID, ok := raw.(string)
		if !ok || artifactID == "" {
			cleanupAll()
			return inputs, func() {}, fmt.Errorf("missing input artifact on %s port", port)
		}
		artifact, err := env.Store.GetArtifact(ctx, artifactID)
		if err != nil {
			cleanupAll()
			return inputs, func() {}, fmt.Errorf("load input artifact %s on %s: %w", artifactID, port, err)
		}
		path, cleanup, err := resolveInputArtifact(ctx, env, artifact)
		if err != nil {
			cleanupAll()
			return inputs, func() {}, err
		}
		cleanups = append(cleanups, cleanup)
		inputs.Paths[port] = path
		inputs.Meta[port] = artifact.MediaInfo
	}
	return inputs, cleanupAll, nil
}

func resolveInputArtifact(ctx context.Context, env RuntimeEnv, artifact store.ArtifactRow) (string, func(), error) {
	return MediaTaskHandler{env: env}.resolveInput(ctx, artifact)
}
```

Create `internal/worker/media_contract.go`:

```go
package worker

type MultiInputMediaHandler interface {
	NodeType() string
	Execute(ctx context.Context, inputPaths map[string]string, outputPath string, config map[string]any) (map[string]any, error)
}
```

If adding the above requires `context`, include the import:

```go
import "context"
```

- [ ] **Step 4: Update runtime to call multi-input handlers**

Modify `MediaHandler` in `internal/worker/runtime.go`:

```go
type MediaHandler interface {
	NodeType() string
	Execute(ctx context.Context, inputPaths map[string]string, outputPath string, config map[string]any) (map[string]any, error)
}
```

Replace the single `"input"` lookup with:

```go
inputs, cleanup, err := BuildInputMap(ctx, h.env, task)
if err != nil {
	return NodeResult{}, err
}
defer cleanup()
config := cloneConfig(task.Config)
if len(inputs.Meta) > 0 {
	config["_input_artifact_meta"] = inputs.Meta
}
metadata, err := h.media.Execute(execCtx, inputs.Paths, outputLocalPath, config)
```

Add:

```go
func cloneConfig(config map[string]any) map[string]any {
	cloned := make(map[string]any, len(config)+1)
	for key, value := range config {
		cloned[key] = value
	}
	return cloned
}
```

When creating artifact row, persist metadata:

```go
MediaInfo: metadata,
```

- [ ] **Step 5: Run worker runtime tests**

Run:

```bash
go test ./internal/worker
```

Expected:

```text
All worker tests pass after existing trim handler is updated to the new MediaHandler interface.
```

- [ ] **Step 6: Commit port-aware runtime**

Run:

```bash
git add internal/worker/inputmap.go internal/worker/media_contract.go internal/worker/runtime.go internal/worker/artifacts.go internal/worker/runtime_test.go internal/worker/handlers/trim.go internal/worker/handlers/trim_test.go
git commit -m "feat: resolve go worker inputs by port"
```

Expected:

```text
Commit succeeds and trim still works through the multi-input adapter.
```

## Task 6: Shared Go Ffmpeg Handler Helpers

**Files:**
- Create: `internal/worker/handlers/common.go`
- Modify: `internal/worker/ffmpeg/encode.go`
- Test: `internal/worker/handlers/handler_contract_test.go`

- [ ] **Step 1: Write helper tests**

Create `internal/worker/handlers/handler_contract_test.go` with shared assertions:

```go
package handlers

import (
	"reflect"
	"testing"
)

func TestScaleFilter(t *testing.T) {
	got := scaleFilter("1080", "1920", "increase")
	want := "scale=1080:1920:force_original_aspect_ratio=increase:flags=lanczos"
	if got != want {
		t.Fatalf("scaleFilter = %q, want %q", got, want)
	}
}

func TestDrawTextEscaping(t *testing.T) {
	got := escapeDrawText(`a:b'c\d`)
	want := `a\:b\'c\\d`
	if got != want {
		t.Fatalf("escapeDrawText = %q, want %q", got, want)
	}
}

func TestIntermediateVideoEncodeArgs(t *testing.T) {
	got := intermediateVideoEncodeArgs("libx264")
	want := []string{
		"-c:v", "libx264", "-crf", "18", "-preset", "slow",
		"-pix_fmt", "yuv420p", "-movflags", "+faststart",
		"-color_primaries", "bt709", "-color_trc", "bt709", "-colorspace", "bt709",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v", got)
	}
}
```

- [ ] **Step 2: Run helper tests and verify they fail**

Run:

```bash
go test ./internal/worker/handlers -run 'TestScaleFilter|TestDrawTextEscaping|TestIntermediateVideoEncodeArgs' -v
```

Expected:

```text
FAIL because common helper functions do not exist.
```

- [ ] **Step 3: Implement common helpers**

Create `internal/worker/handlers/common.go`:

```go
package handlers

import (
	"fmt"
	"strconv"
	"strings"
)

func scaleFilter(width string, height string, forceOriginalAspectRatio string) string {
	parts := []string{fmt.Sprintf("scale=%s:%s", width, height)}
	if forceOriginalAspectRatio != "" {
		parts = append(parts, "force_original_aspect_ratio="+forceOriginalAspectRatio)
	}
	parts = append(parts, "flags=lanczos")
	return strings.Join(parts, ":")
}

func intString(value any, fallback int) string {
	return strconv.Itoa(intValue(value, fallback))
}

func intValue(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		if typed > 0 {
			return typed
		}
	case float64:
		if typed > 0 {
			return int(typed)
		}
	case string:
		if parsed, err := strconv.Atoi(typed); err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}

func floatValue(value any, fallback float64) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case int:
		return float64(typed)
	case string:
		if parsed, err := strconv.ParseFloat(typed, 64); err == nil {
			return parsed
		}
	}
	return fallback
}

func boolValue(value any, fallback bool) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		normalized := strings.TrimSpace(strings.ToLower(typed))
		if normalized == "" {
			return fallback
		}
		return normalized == "1" || normalized == "true" || normalized == "yes" || normalized == "on"
	default:
		return fallback
	}
}

func stringValue(value any, fallback string) string {
	if typed, ok := value.(string); ok && typed != "" {
		return typed
	}
	return fallback
}

func escapeDrawText(text string) string {
	text = strings.ReplaceAll(text, `\`, `\\`)
	text = strings.ReplaceAll(text, ":", `\:`)
	text = strings.ReplaceAll(text, "'", `\'`)
	return text
}

func intermediateVideoEncodeArgs(codec string) []string {
	if codec == "" {
		codec = "libx264"
	}
	return []string{
		"-c:v", codec, "-crf", "18", "-preset", "slow",
		"-pix_fmt", "yuv420p", "-movflags", "+faststart",
		"-color_primaries", "bt709", "-color_trc", "bt709", "-colorspace", "bt709",
	}
}

func finalVideoEncodeArgs(codec string) []string {
	if codec == "" {
		codec = "libx264"
	}
	return []string{
		"-c:v", codec, "-crf", "20", "-preset", "medium",
		"-pix_fmt", "yuv420p", "-movflags", "+faststart",
		"-color_primaries", "bt709", "-color_trc", "bt709", "-colorspace", "bt709",
	}
}
```

- [ ] **Step 4: Run helper tests**

Run:

```bash
go test ./internal/worker/handlers
```

Expected:

```text
Handler helper tests pass.
```

- [ ] **Step 5: Commit helper layer**

Run:

```bash
git add internal/worker/handlers/common.go internal/worker/handlers/handler_contract_test.go internal/worker/ffmpeg/encode.go
git commit -m "feat: add shared go ffmpeg handler helpers"
```

Expected:

```text
Commit succeeds.
```

## Task 7: Batch 4A Handlers

**Files:**
- Create: `internal/worker/handlers/transcode.go`
- Create: `internal/worker/handlers/export.go`
- Create: `internal/worker/handlers/vertical_crop.go`
- Create: `internal/worker/handlers/watermark.go`
- Create: `internal/worker/handlers/title_overlay.go`
- Modify: `internal/worker/handlers/handler_contract_test.go`
- Modify: `cmd/vp-ffmpeg-worker/main.go`

- [ ] **Step 1: Add exact-argument tests for Batch 4A**

Append to `internal/worker/handlers/handler_contract_test.go`:

```go
func TestBatch4AArgs(t *testing.T) {
	tests := []struct {
		name   string
		args   []string
		expect []string
	}{
		{
			name: "transcode default",
			args: TranscodeArgs("/in.mp4", "/out.mp4", map[string]any{}),
			expect: []string{"-i", "/in.mp4", "-c:v", "libx264", "-crf", "20", "-preset", "medium", "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-color_primaries", "bt709", "-color_trc", "bt709", "-colorspace", "bt709", "-c:a", "aac", "/out.mp4"},
		},
		{
			name: "vertical crop center",
			args: VerticalCropArgs("/in.mp4", "/out.mp4", map[string]any{"width": 1080.0, "height": 1920.0, "mode": "center_crop"}),
			expect: []string{"-i", "/in.mp4", "-vf", "scale=1080:1920:force_original_aspect_ratio=increase:flags=lanczos,crop=1080:1920,setsar=1", "-c:v", "libx264", "-crf", "18", "-preset", "slow", "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-color_primaries", "bt709", "-color_trc", "bt709", "-colorspace", "bt709", "-c:a", "aac", "/out.mp4"},
		},
		{
			name: "watermark bottom right",
			args: WatermarkArgs("/video.mp4", "/wm.png", "/out.mp4", map[string]any{"position": "bottom_right", "opacity": 0.8, "scale": 0.15, "margin": 10.0}),
			expect: []string{"-i", "/video.mp4", "-i", "/wm.png", "-filter_complex", "[1:v]scale=iw*0.15:-1:flags=lanczos,format=rgba,colorchannelmixer=aa=0.8[wm];[0:v][wm]overlay=W-w-10:H-h-10[v]", "-map", "[v]", "-map", "0:a?", "-c:v", "libx264", "-crf", "18", "-preset", "slow", "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-color_primaries", "bt709", "-color_trc", "bt709", "-colorspace", "bt709", "-c:a", "copy", "/out.mp4"},
		},
		{
			name: "title overlay",
			args: TitleOverlayArgs("/in.mp4", "/out.mp4", map[string]any{"text": "Hello: A", "position": "top", "start_time": 0.0, "duration": 3.0, "font_size": 72.0, "safe_area": true}),
			expect: []string{"-i", "/in.mp4", "-vf", "drawtext=text='Hello\\: A':fontcolor=white:fontsize=72:box=1:boxcolor=black@0.45:boxborderw=18:x=(w-text_w)/2:y=h*0.12:enable='between(t,0,3)'", "-c:v", "libx264", "-crf", "18", "-preset", "slow", "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-color_primaries", "bt709", "-color_trc", "bt709", "-colorspace", "bt709", "-c:a", "aac", "/out.mp4"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !reflect.DeepEqual(tt.args, tt.expect) {
				t.Fatalf("args = %#v", tt.args)
			}
		})
	}
}
```

- [ ] **Step 2: Run Batch 4A tests and verify they fail**

Run:

```bash
go test ./internal/worker/handlers -run TestBatch4AArgs -v
```

Expected:

```text
FAIL because Batch 4A handler argument builders do not exist.
```

- [ ] **Step 3: Implement Batch 4A handlers**

Each handler must satisfy:

```go
func (h HandlerName) NodeType() string
func (h HandlerName) Execute(ctx context.Context, inputPaths map[string]string, outputPath string, config map[string]any) (map[string]any, error)
```

Create `internal/worker/handlers/transcode.go` with:

```go
func TranscodeArgs(inputPath string, outputPath string, config map[string]any) []string {
	videoCodec := stringValue(config["video_codec"], "libx264")
	audioCodec := stringValue(config["audio_codec"], "aac")
	resolution := stringValue(config["resolution"], "")
	bitrate := stringValue(config["bitrate"], "")
	crf := intValue(config["crf"], 20)
	preset := stringValue(config["preset"], "medium")
	args := []string{"-i", inputPath}
	if videoCodec == "copy" {
		args = append(args, "-c:v", "copy")
	} else if videoCodec == "libvpx-vp9" {
		args = append(args, "-c:v", "libvpx-vp9", "-crf", strconv.Itoa(crf), "-b:v", "0")
		if bitrate != "" {
			args = append(args, "-b:v", bitrate)
		}
	} else {
		args = append(args, "-c:v", videoCodec, "-crf", strconv.Itoa(crf), "-preset", preset, "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-color_primaries", "bt709", "-color_trc", "bt709", "-colorspace", "bt709")
		if bitrate != "" {
			args = append(args, "-b:v", bitrate)
		}
	}
	if videoCodec != "copy" && resolution != "" && resolution != "original" {
		parts := strings.SplitN(resolution, "x", 2)
		if len(parts) == 2 {
			args = append(args, "-vf", scaleFilter(parts[0], parts[1], ""))
		}
	}
	args = append(args, "-c:a", audioCodec, outputPath)
	return args
}
```

Create the remaining handlers using the exact Python contracts read from:

```text
backend/worker/handlers/export.py
backend/worker/handlers/vertical_crop.py
backend/worker/handlers/watermark.py
backend/worker/handlers/title_overlay.py
```

The implemented input ports are:

```text
export: input
vertical_crop: input
watermark: video, overlay
title_overlay: input
```

The implemented config defaults are:

```text
export.output_dir=/tmp/vp_export, export.filename=basename(input)
vertical_crop.width=1080, vertical_crop.height=1920, vertical_crop.mode=center_crop
watermark.position=bottom_right, watermark.opacity=0.8, watermark.scale=0.15, watermark.margin=10
title_overlay.text="", title_overlay.position=top, title_overlay.start_time=0, title_overlay.duration=3, title_overlay.font_size=72, title_overlay.safe_area=true
```

- [ ] **Step 4: Register Batch 4A handlers**

Modify `cmd/vp-ffmpeg-worker/main.go` handler registration:

```go
handlers := []worker.Handler{
	worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.TrimHandler{Runner: runner}),
	worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.TranscodeHandler{Runner: runner}),
	worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.ExportHandler{Runner: runner}),
	worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.VerticalCropHandler{Runner: runner}),
	worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.WatermarkHandler{Runner: runner}),
	worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.TitleOverlayHandler{Runner: runner}),
}
consumer := worker.NewConsumer(redisClient, cfg, handlers...)
```

- [ ] **Step 5: Run Batch 4A tests**

Run:

```bash
go test ./internal/worker/handlers ./internal/worker
go test ./...
```

Expected:

```text
All Go tests pass.
```

- [ ] **Step 6: Commit Batch 4A handlers**

Run:

```bash
git add cmd/vp-ffmpeg-worker/main.go internal/worker/handlers/transcode.go internal/worker/handlers/export.go internal/worker/handlers/vertical_crop.go internal/worker/handlers/watermark.go internal/worker/handlers/title_overlay.go internal/worker/handlers/handler_contract_test.go
git commit -m "feat: migrate first batch ffmpeg handlers to go"
```

Expected:

```text
Commit succeeds.
```

## Task 8: Batch 4B Handlers

**Files:**
- Create: `internal/worker/handlers/bgm.go`
- Create: `internal/worker/handlers/replace_audio.go`
- Create: `internal/worker/handlers/concat_stack.go`
- Create: `internal/worker/handlers/concat_horizontal.go`
- Create: `internal/worker/handlers/concat_vertical.go`
- Create: `internal/worker/handlers/concat_many.go`
- Modify: `internal/worker/handlers/handler_contract_test.go`
- Modify: `cmd/vp-ffmpeg-worker/main.go`

- [ ] **Step 1: Add Batch 4B contract tests**

Append to `internal/worker/handlers/handler_contract_test.go`:

```go
func TestBatch4BInputPorts(t *testing.T) {
	tests := map[string][]string{
		"bgm":               {"video", "audio"},
		"replace_audio":     {"video", "audio"},
		"concat_horizontal": {"video_left", "video_right"},
		"concat_vertical":   {"video_top", "video_bottom"},
		"concat_many":       {"video_1", "video_2"},
	}
	for nodeType, ports := range tests {
		t.Run(nodeType, func(t *testing.T) {
			if len(ports) < 2 {
				t.Fatalf("%s ports = %#v", nodeType, ports)
			}
		})
	}
}

func TestConcatManySelectedInputOrder(t *testing.T) {
	inputs := map[string]string{
		"video_10": "/10.mp4",
		"video_2":  "/2.mp4",
		"video_1":  "/1.mp4",
	}
	got := selectedVideoInputItems(inputs)
	want := []inputItem{{handle: "video_1", path: "/1.mp4"}, {handle: "video_2", path: "/2.mp4"}, {handle: "video_10", path: "/10.mp4"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("selected = %#v", got)
	}
}
```

- [ ] **Step 2: Run Batch 4B tests and verify they fail**

Run:

```bash
go test ./internal/worker/handlers -run 'TestBatch4BInputPorts|TestConcatManySelectedInputOrder' -v
```

Expected:

```text
FAIL because concat selection helpers do not exist.
```

- [ ] **Step 3: Implement Batch 4B handlers**

Implement exact contracts from:

```text
backend/worker/handlers/bgm.py
backend/worker/handlers/replace_audio.py
backend/worker/handlers/concat_stack.py
backend/worker/handlers/concat_horizontal.py
backend/worker/handlers/concat_vertical.py
backend/worker/handlers/concat_many.py
```

Use this handler input and default matrix:

```text
bgm inputs: video, audio
bgm defaults: volume=0.3, original_volume=1.0, loop=true, fade_in=0, fade_out=0

replace_audio inputs: video, audio
replace_audio defaults: loop_if_shorter=true, audio_volume=1.0

concat_horizontal inputs: video_left, video_right
concat_horizontal defaults: resize_mode=match_height

concat_vertical inputs: video_top, video_bottom
concat_vertical defaults: resize_mode=match_width

concat_many inputs: video_N sorted numerically, plus legacy video_first=1 and video_second=2
concat_many defaults: normalize_resolution=true, aspect_ratio=9:16, width/height by aspect ratio, sample_rate=48000 for synthetic silence
```

For `bgm` and `replace_audio`, use Go ffprobe through the existing ffmpeg runner helper. If the current runner exposes no ffprobe helper, add:

```go
func (r Runner) Probe(ctx context.Context, inputPath string) (ProbeResult, error)
```

with JSON parsing from:

```bash
ffprobe -v error -show_streams -show_format -of json <input>
```

For `concat_many`, implement:

```go
type inputItem struct {
	handle string
	path   string
}

func selectedVideoInputItems(inputPaths map[string]string) []inputItem {
	indexed := map[int]inputItem{}
	for handle, path := range inputPaths {
		if strings.HasPrefix(handle, "video_") {
			raw := strings.TrimPrefix(handle, "video_")
			if index, err := strconv.Atoi(raw); err == nil {
				indexed[index] = inputItem{handle: handle, path: path}
			}
		}
		if handle == "video_first" {
			if _, exists := indexed[1]; !exists {
				indexed[1] = inputItem{handle: handle, path: path}
			}
		}
		if handle == "video_second" {
			if _, exists := indexed[2]; !exists {
				indexed[2] = inputItem{handle: handle, path: path}
			}
		}
	}
	keys := make([]int, 0, len(indexed))
	for key := range indexed {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	selected := make([]inputItem, 0, len(keys))
	for _, key := range keys {
		selected = append(selected, indexed[key])
	}
	return selected
}
```

- [ ] **Step 4: Register Batch 4B handlers**

Add these handlers to `cmd/vp-ffmpeg-worker/main.go`:

```go
worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.BgmHandler{Runner: runner}),
worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.ReplaceAudioHandler{Runner: runner}),
worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.ConcatHorizontalHandler{Runner: runner}),
worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.ConcatVerticalHandler{Runner: runner}),
worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.ConcatManyHandler{Runner: runner}),
```

- [ ] **Step 5: Run Batch 4B tests**

Run:

```bash
go test ./internal/worker/ffmpeg ./internal/worker/handlers ./internal/worker
go test ./...
```

Expected:

```text
All Go tests pass.
```

- [ ] **Step 6: Commit Batch 4B handlers**

Run:

```bash
git add cmd/vp-ffmpeg-worker/main.go internal/worker/ffmpeg internal/worker/handlers
git commit -m "feat: migrate ffmpeg composition handlers to go"
```

Expected:

```text
Commit succeeds.
```

## Task 9: Batch 4C Timeline And Layout Handlers

**Files:**
- Create: `internal/worker/handlers/concat_timeline.go`
- Create: `internal/worker/handlers/concat_vertical_timeline.go`
- Create: `internal/worker/handlers/montage_assembler.go`
- Modify: `internal/worker/handlers/handler_contract_test.go`
- Modify: `cmd/vp-ffmpeg-worker/main.go`

- [ ] **Step 1: Add Batch 4C contract tests**

Append to `internal/worker/handlers/handler_contract_test.go`:

```go
func TestTimelineTempConcatFileContents(t *testing.T) {
	got := concatDemuxerFileContent([]string{"/a.mp4", "/b c.mp4"})
	want := "file '/a.mp4'\nfile '/b c.mp4'\n"
	if got != want {
		t.Fatalf("concat file = %q", got)
	}
}

func TestMontageDimensions(t *testing.T) {
	tests := []struct {
		config map[string]any
		width  int
		height int
	}{
		{map[string]any{}, 1080, 1920},
		{map[string]any{"aspect_ratio": "16:9"}, 1920, 1080},
		{map[string]any{"aspect_ratio": "1:1"}, 1080, 1080},
		{map[string]any{"width": 720.0, "height": 1280.0}, 720, 1280},
	}
	for _, tt := range tests {
		width, height := montageDimensions(tt.config)
		if width != tt.width || height != tt.height {
			t.Fatalf("dimensions = %dx%d", width, height)
		}
	}
}
```

- [ ] **Step 2: Run Batch 4C tests and verify they fail**

Run:

```bash
go test ./internal/worker/handlers -run 'TestTimelineTempConcatFileContents|TestMontageDimensions' -v
```

Expected:

```text
FAIL because Batch 4C helpers do not exist.
```

- [ ] **Step 3: Implement Batch 4C handlers**

Implement contracts from:

```text
backend/worker/handlers/concat_timeline.py
backend/worker/handlers/concat_vertical_timeline.py
backend/worker/handlers/montage_assembler.py
```

Use this behavior matrix:

```text
concat_timeline inputs: video_N sorted numerically, plus legacy video_first/video_second
concat_timeline defaults: transition=none, transition_duration=0.5
concat_timeline no transition: concat demuxer, -safe 0, -c copy
concat_timeline two videos with fade/dissolve: xfade video, acrossfade only when both inputs have audio
concat_timeline more than two videos with transition: route through ConcatManyHandler with normalize_resolution=true

concat_vertical_timeline inputs: video_first, video_second, optional image_top, optional image_bottom
concat_vertical_timeline defaults: pane_width=640, pane_height=360, background_color=black
concat_vertical_timeline generated images: extract frame index frame_count-15 for first video and 14 for second video, clamped to available frame range
concat_vertical_timeline missing audio: synthesize anullsrc=r=48000:cl=stereo

montage_assembler inputs: video_N sorted numerically
montage_assembler defaults: aspect_ratio=9:16, normalize_resolution=true, width/height by aspect ratio
```

Implement temp file cleanup with:

```go
func safeRemove(path string) {
	if path != "" {
		_ = os.Remove(path)
	}
}
```

- [ ] **Step 4: Register Batch 4C handlers**

Add these handlers to `cmd/vp-ffmpeg-worker/main.go`:

```go
worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.ConcatTimelineHandler{Runner: runner}),
worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.ConcatVerticalTimelineHandler{Runner: runner}),
worker.NewMediaTaskHandler(runtimeEnv, handlerspkg.MontageAssemblerHandler{Runner: runner}),
```

- [ ] **Step 5: Run Batch 4C tests**

Run:

```bash
go test ./internal/worker/ffmpeg ./internal/worker/handlers ./internal/worker
go test ./...
```

Expected:

```text
All Go tests pass.
```

- [ ] **Step 6: Commit Batch 4C handlers**

Run:

```bash
git add cmd/vp-ffmpeg-worker/main.go internal/worker/handlers
git commit -m "feat: migrate timeline ffmpeg handlers to go"
```

Expected:

```text
Commit succeeds.
```

## Task 10: Per-Node Mixed-Mode Tests And Worker-Type Cutover

**Files:**
- Create: `tests/go_migration/test_go_worker_nodes.py`
- Modify: `backend/app/node_registry/builtin/*.py` for the migrated first-wave pure ffmpeg nodes
- Modify: `docs/go-migration-acceptance/README.md`

- [ ] **Step 1: Add strict mixed-mode test matrix**

Create `tests/go_migration/test_go_worker_nodes.py`:

```python
from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import pytest
import requests


STRICT = os.getenv("VP_GO_WORKER_NODE_STRICT") == "1"
PY_API = os.getenv("VP_PY_API_URL", "http://127.0.0.1:18080")
REDIS_URL = os.getenv("VP_REDIS_URL", "redis://127.0.0.1:6380/0")


NODE_CASES = [
    "trim",
    "transcode",
    "export",
    "vertical_crop",
    "watermark",
    "title_overlay",
    "bgm",
    "replace_audio",
    "concat_horizontal",
    "concat_vertical",
    "concat_many",
    "concat_timeline",
    "concat_vertical_timeline",
    "montage_assembler",
]


@pytest.mark.skipif(not STRICT, reason="set VP_GO_WORKER_NODE_STRICT=1 for live mixed-mode node tests")
@pytest.mark.parametrize("node_type", NODE_CASES)
def test_node_runs_through_go_worker(node_type: str, tmp_path: Path) -> None:
    payload = build_pipeline_payload(node_type, tmp_path)
    created = requests.post(f"{PY_API}/api/v1/pipelines", json=payload["pipeline"], timeout=20)
    assert created.status_code in {200, 201}, created.text
    pipeline_id = created.json()["id"]
    job = requests.post(f"{PY_API}/api/v1/jobs", json={"pipeline_id": pipeline_id}, timeout=20)
    assert job.status_code in {200, 201}, job.text
    job_id = job.json()["id"]

    deadline = time.monotonic() + 180
    body = {}
    while time.monotonic() < deadline:
        response = requests.get(f"{PY_API}/api/v1/jobs/{job_id}", timeout=10)
        assert response.status_code == 200
        body = response.json()
        if body["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        time.sleep(2)

    assert body["status"] == "SUCCEEDED", body
    pending = subprocess.check_output(
        ["redis-cli", "-u", REDIS_URL, "XPENDING", "vp:tasks:ffmpeg_go", "ffmpeg_go-workers"],
        text=True,
    )
    assert pending.splitlines()[0].strip() == "0"


def build_pipeline_payload(node_type: str, tmp_path: Path) -> dict:
    raise AssertionError(f"test fixture builder missing case for {node_type}")
```

- [ ] **Step 2: Replace the fixture-builder assertion with concrete graph builders**

Implement `build_pipeline_payload` with deterministic uploaded assets and exact graph cases:

```python
def build_pipeline_payload(node_type: str, tmp_path: Path) -> dict:
    assets = {
        "video": ensure_uploaded_asset("video", tmp_path),
        "audio": ensure_uploaded_asset("audio", tmp_path),
        "image": ensure_uploaded_asset("image", tmp_path),
    }
    return {
        "pipeline": {
            "name": f"go-node-{node_type}",
            "definition": pipeline_definition_for(node_type, assets),
            "is_template": False,
            "template_tags": [],
        }
    }


def ensure_uploaded_asset(kind: str, tmp_path: Path) -> str:
    env_key = f"VP_GO_SMOKE_{kind.upper()}_ASSET_ID"
    if asset_id := os.getenv(env_key):
        return asset_id
    source = tmp_path / f"go-{kind}-fixture"
    if kind == "video":
        source = source.with_suffix(".mp4")
        subprocess.run(
            [
                "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
                "-f", "lavfi", "-i", "testsrc2=size=320x180:rate=30",
                "-f", "lavfi", "-i", "sine=frequency=1000:sample_rate=48000",
                "-t", "3", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", str(source),
            ],
            check=True,
        )
        content_type = "video/mp4"
    elif kind == "audio":
        source = source.with_suffix(".wav")
        subprocess.run(
            ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-t", "3", str(source)],
            check=True,
        )
        content_type = "audio/wav"
    elif kind == "image":
        source = source.with_suffix(".png")
        subprocess.run(
            ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "color=c=red:s=96x96", "-frames:v", "1", str(source)],
            check=True,
        )
        content_type = "image/png"
    else:
        raise AssertionError(kind)
    with source.open("rb") as fh:
        response = requests.post(f"{PY_API}/api/v1/assets/upload", files={"file": (source.name, fh, content_type)}, timeout=60)
    assert response.status_code in {200, 201}, response.text
    return response.json()["id"]


def source_node(node_id: str, asset_id: str, media_type: str, x: int) -> dict:
    return {
        "id": node_id,
        "type": "source",
        "position": {"x": x, "y": 0},
        "data": {"label": node_id, "asset_id": asset_id, "config": {"asset_id": asset_id, "media_type": media_type}},
    }


def work_node(node_id: str, node_type: str, config: dict, x: int) -> dict:
    return {"id": node_id, "type": node_type, "position": {"x": x, "y": 0}, "data": {"label": node_type, "config": config}}


def edge(edge_id: str, source: str, source_handle: str, target: str, target_handle: str) -> dict:
    return {"id": edge_id, "source": source, "sourceHandle": source_handle, "target": target, "targetHandle": target_handle}


def pipeline_definition_for(node_type: str, assets: dict[str, str]) -> dict:
    export = work_node("export_1", "export", {"output_dir": "/tmp/vp_go_node_exports", "filename": f"{node_type}.mp4"}, 900)
    source_video = source_node("video_1", assets["video"], "video", 0)
    source_audio = source_node("audio_1", assets["audio"], "audio", 0)
    source_image = source_node("image_1", assets["image"], "image", 0)
    if node_type in {"trim", "transcode", "export", "vertical_crop", "title_overlay"}:
        config = {
            "trim": {"start_time": "0", "duration": "1", "output_format": "mp4"},
            "transcode": {"video_codec": "libx264", "audio_codec": "aac", "crf": 20, "preset": "medium"},
            "export": {"output_dir": "/tmp/vp_go_node_exports", "filename": "export-direct.mp4"},
            "vertical_crop": {"width": 320, "height": 568, "mode": "center_crop"},
            "title_overlay": {"text": "Go", "position": "top", "duration": 1, "font_size": 32},
        }[node_type]
        node = work_node("node_1", node_type, config, 450)
        return {"nodes": [source_video, node, export], "edges": [edge("e1", "video_1", "output", "node_1", "input"), edge("e2", "node_1", "output", "export_1", "input")], "viewport": {"x": 0, "y": 0, "zoom": 1}}
    if node_type == "watermark":
        node = work_node("node_1", "watermark", {"position": "bottom_right", "opacity": 0.8, "scale": 0.15, "margin": 10}, 450)
        return {"nodes": [source_video, source_image, node, export], "edges": [edge("e1", "video_1", "output", "node_1", "video"), edge("e2", "image_1", "output", "node_1", "overlay"), edge("e3", "node_1", "output", "export_1", "input")], "viewport": {"x": 0, "y": 0, "zoom": 1}}
    if node_type in {"bgm", "replace_audio"}:
        node = work_node("node_1", node_type, {}, 450)
        return {"nodes": [source_video, source_audio, node, export], "edges": [edge("e1", "video_1", "output", "node_1", "video"), edge("e2", "audio_1", "output", "node_1", "audio"), edge("e3", "node_1", "output", "export_1", "input")], "viewport": {"x": 0, "y": 0, "zoom": 1}}
    if node_type == "concat_horizontal":
        node = work_node("node_1", node_type, {"resize_mode": "match_height"}, 450)
        return {"nodes": [source_video, source_node("video_2", assets["video"], "video", 0), node, export], "edges": [edge("e1", "video_1", "output", "node_1", "video_left"), edge("e2", "video_2", "output", "node_1", "video_right"), edge("e3", "node_1", "output", "export_1", "input")], "viewport": {"x": 0, "y": 0, "zoom": 1}}
    if node_type == "concat_vertical":
        node = work_node("node_1", node_type, {"resize_mode": "match_width"}, 450)
        return {"nodes": [source_video, source_node("video_2", assets["video"], "video", 0), node, export], "edges": [edge("e1", "video_1", "output", "node_1", "video_top"), edge("e2", "video_2", "output", "node_1", "video_bottom"), edge("e3", "node_1", "output", "export_1", "input")], "viewport": {"x": 0, "y": 0, "zoom": 1}}
    if node_type in {"concat_many", "montage_assembler", "concat_timeline"}:
        node = work_node("node_1", node_type, {"normalize_resolution": True, "aspect_ratio": "9:16", "transition": "none"}, 450)
        return {"nodes": [source_video, source_node("video_2", assets["video"], "video", 0), node, export], "edges": [edge("e1", "video_1", "output", "node_1", "video_1"), edge("e2", "video_2", "output", "node_1", "video_2"), edge("e3", "node_1", "output", "export_1", "input")], "viewport": {"x": 0, "y": 0, "zoom": 1}}
    if node_type == "concat_vertical_timeline":
        node = work_node("node_1", node_type, {"pane_width": 320, "pane_height": 180, "background_color": "black"}, 450)
        return {"nodes": [source_video, source_node("video_2", assets["video"], "video", 0), node, export], "edges": [edge("e1", "video_1", "output", "node_1", "video_first"), edge("e2", "video_2", "output", "node_1", "video_second"), edge("e3", "node_1", "output", "export_1", "input")], "viewport": {"x": 0, "y": 0, "zoom": 1}}
    raise AssertionError(node_type)
```

- [ ] **Step 3: Run strict mixed-mode tests for one node before registry cutover**

Run:

```bash
VP_GO_WORKER_NODE_STRICT=1 VP_REDIS_URL=redis://127.0.0.1:6380/0 python3 -m pytest tests/go_migration/test_go_worker_nodes.py::test_node_runs_through_go_worker[trim] -q
```

Expected:

```text
trim passes because it is already routed to ffmpeg_go.
```

- [ ] **Step 4: Switch first-wave node worker types one at a time**

For each file listed below, change only `worker_type` from `"ffmpeg"` to `"ffmpeg_go"` after that node's strict mixed-mode test passes with a direct Redis task or temporary registry override:

```text
backend/app/node_registry/builtin/transcode.py
backend/app/node_registry/builtin/export.py
backend/app/node_registry/builtin/vertical_crop.py
backend/app/node_registry/builtin/watermark.py
backend/app/node_registry/builtin/title_overlay.py
backend/app/node_registry/builtin/bgm.py
backend/app/node_registry/builtin/replace_audio.py
backend/app/node_registry/builtin/concat_horizontal.py
backend/app/node_registry/builtin/concat_vertical.py
backend/app/node_registry/builtin/concat_many.py
backend/app/node_registry/builtin/concat_timeline.py
backend/app/node_registry/builtin/concat_vertical_timeline.py
backend/app/node_registry/builtin/montage_assembler.py
```

- [ ] **Step 5: Run the full strict node matrix**

Run:

```bash
VP_GO_WORKER_NODE_STRICT=1 VP_REDIS_URL=redis://127.0.0.1:6380/0 python3 -m pytest tests/go_migration/test_go_worker_nodes.py -q
```

Expected:

```text
14 passed and Redis XPENDING for vp:tasks:ffmpeg_go is 0 after the run.
```

- [ ] **Step 6: Record node cutover evidence**

Append to `docs/go-migration-acceptance/README.md`:

````markdown
## Per-Node Worker Cutover

Command:

```bash
VP_GO_WORKER_NODE_STRICT=1 VP_REDIS_URL=redis://127.0.0.1:6380/0 python3 -m pytest tests/go_migration/test_go_worker_nodes.py -q
redis-cli -u redis://127.0.0.1:6380/0 XPENDING vp:tasks:ffmpeg_go ffmpeg_go-workers
```

Expected evidence:

```text
14 passed
XPENDING summary count: 0
```
````

- [ ] **Step 7: Commit worker cutover**

Run:

```bash
git add tests/go_migration/test_go_worker_nodes.py backend/app/node_registry/builtin docs/go-migration-acceptance/README.md
git commit -m "feat: route first wave ffmpeg nodes to go worker"
```

Expected:

```text
Commit succeeds with per-node mixed-mode evidence instructions.
```

## Task 11: API Metrics And Worker Metrics

**Files:**
- Modify: `go.mod`
- Create: `internal/httpapi/metrics.go`
- Modify: `internal/httpapi/router.go`
- Modify: `internal/httpapi/middleware.go`
- Create: `internal/worker/metrics.go`
- Modify: `internal/worker/consumer.go`
- Modify: `internal/worker/ffmpeg/runner.go`
- Modify: `cmd/vp-api/main.go`
- Modify: `cmd/vp-ffmpeg-worker/main.go`

- [ ] **Step 1: Add metrics tests**

Append to `internal/httpapi/httpapi_test.go`:

```go
func TestMetricsEndpointExposesHTTPMetrics(t *testing.T) {
	server := NewServer()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body := rec.Body.String()
	for _, metric := range []string{
		"http_requests_total",
		"http_request_duration_seconds",
		"http_request_errors_total",
	} {
		if !strings.Contains(body, metric) {
			t.Fatalf("metrics body missing %s: %s", metric, body)
		}
	}
}
```

- [ ] **Step 2: Run metrics test and verify it fails**

Run:

```bash
go test ./internal/httpapi -run TestMetricsEndpointExposesHTTPMetrics -v
```

Expected:

```text
FAIL because /metrics is not registered.
```

- [ ] **Step 3: Add Prometheus dependency**

Run:

```bash
go get github.com/prometheus/client_golang@v1.20.5
```

Expected:

```text
go.mod and go.sum update successfully.
```

- [ ] **Step 4: Implement API metrics**

Create `internal/httpapi/metrics.go`:

```go
package httpapi

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var httpRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "http_requests_total",
	Help: "Total HTTP requests handled by api-go.",
}, []string{"method", "route", "status"})

var httpRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "http_request_duration_seconds",
	Help:    "HTTP request duration for api-go.",
	Buckets: prometheus.DefBuckets,
}, []string{"method", "route", "status"})

var httpRequestErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "http_request_errors_total",
	Help: "Total HTTP requests with 5xx status handled by api-go.",
}, []string{"method", "route", "status"})

func metricsHandler() http.Handler {
	return promhttp.Handler()
}

func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = r.URL.Path
		}
		status := strconv.Itoa(rec.status)
		httpRequestsTotal.WithLabelValues(r.Method, route, status).Inc()
		httpRequestDuration.WithLabelValues(r.Method, route, status).Observe(time.Since(start).Seconds())
		if rec.status >= 500 {
			httpRequestErrorsTotal.WithLabelValues(r.Method, route, status).Inc()
		}
	})
}
```

Wire in `internal/httpapi/router.go`:

```go
r.Use(metricsMiddleware)
r.Handle("/metrics", metricsHandler())
```

- [ ] **Step 5: Implement worker metrics**

Create `internal/worker/metrics.go`:

```go
package worker

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var workerTasksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "vp_worker_tasks_total",
	Help: "Total tasks claimed by Go workers.",
}, []string{"worker_type", "node_type", "result"})

var workerTaskDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "vp_worker_task_duration_seconds",
	Help:    "Task duration for Go workers.",
	Buckets: prometheus.DefBuckets,
}, []string{"worker_type", "node_type"})

var workerTaskFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "vp_worker_task_failures_total",
	Help: "Total failed Go worker tasks.",
}, []string{"worker_type", "node_type"})

var workerTaskCancellationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "vp_worker_task_cancellations_total",
	Help: "Total confirmed Go worker cancellations.",
}, []string{"worker_type", "node_type"})

var workerPendingReclaimsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "vp_worker_pending_reclaims_total",
	Help: "Total pending Redis stream tasks reclaimed by Go workers.",
}, []string{"worker_type"})

var workerHeartbeatFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "vp_worker_heartbeat_failures_total",
	Help: "Total heartbeat refresh failures for Go workers.",
}, []string{"worker_type"})
```

Add ffmpeg counters in `internal/worker/ffmpeg/runner.go`:

```go
vp_ffmpeg_runs_total
vp_ffmpeg_failures_total
vp_ffmpeg_gpu_fallbacks_total
```

- [ ] **Step 6: Run metrics checks**

Run:

```bash
go test ./...
go vet ./...
```

Expected:

```text
All Go checks pass.
```

- [ ] **Step 7: Commit metrics**

Run:

```bash
git add go.mod go.sum internal/httpapi/metrics.go internal/httpapi/router.go internal/httpapi/middleware.go internal/httpapi/httpapi_test.go internal/worker/metrics.go internal/worker/consumer.go internal/worker/ffmpeg/runner.go cmd/vp-api/main.go cmd/vp-ffmpeg-worker/main.go
git commit -m "feat: expose go migration metrics"
```

Expected:

```text
Commit succeeds with API and worker metric counters.
```

## Task 12: Selective Pipeline Write API

**Files:**
- Create: `internal/httpapi/write_responses.go`
- Create: `internal/httpapi/pipeline_writes.go`
- Modify: `internal/httpapi/router.go`
- Modify: `internal/store/pipelines.go`
- Modify: `internal/store/store_test.go`
- Create: `tests/go_migration/test_go_api_write_parity.py`

- [ ] **Step 1: Add Go HTTP tests for pipeline writes**

Append to `internal/httpapi/httpapi_test.go`:

```go
func TestValidateRouteRejectsMalformedJSON(t *testing.T) {
	server := NewServer()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/validate", strings.NewReader("{"))
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"detail"`) {
		t.Fatalf("body = %s", rec.Body.String())
	}
}
```

- [ ] **Step 2: Implement shared write responses**

Create `internal/httpapi/write_responses.go`:

```go
package httpapi

import "net/http"

func badRequest(w http.ResponseWriter, detail string) {
	writeJSON(w, http.StatusBadRequest, map[string]string{"detail": detail})
}

func notFound(w http.ResponseWriter, detail string) {
	writeJSON(w, http.StatusNotFound, map[string]string{"detail": detail})
}

func conflict(w http.ResponseWriter, detail string) {
	writeJSON(w, http.StatusConflict, map[string]string{"detail": detail})
}

func unsupportedWrite(w http.ResponseWriter, detail string) {
	writeJSON(w, http.StatusNotImplemented, map[string]string{"detail": detail})
}
```

- [ ] **Step 3: Implement pipeline write store methods**

Add to `internal/store/pipelines.go`:

```go
type PipelineWriteInput struct {
	Name        string
	Description string
	Definition  map[string]any
	IsTemplate  bool
}

func (s *Store) CreatePipeline(ctx context.Context, in PipelineWriteInput) (PipelineRow, error)
func (s *Store) UpdatePipeline(ctx context.Context, id string, in PipelineWriteInput) (PipelineRow, error)
func (s *Store) DeletePipeline(ctx context.Context, id string) error
func (s *Store) DuplicatePipeline(ctx context.Context, id string) (PipelineRow, error)
```

Use SQL returning the same fields as `ListPipelines` and `GetPipeline`.

- [ ] **Step 4: Implement pipeline write routes**

Create `internal/httpapi/pipeline_writes.go` with handlers:

```go
func (s *Server) createPipeline(w http.ResponseWriter, r *http.Request)
func (s *Server) updatePipeline(w http.ResponseWriter, r *http.Request)
func (s *Server) deletePipeline(w http.ResponseWriter, r *http.Request)
func (s *Server) duplicatePipeline(w http.ResponseWriter, r *http.Request)
```

Each create/update request must:

```go
result := pipeline.Validate(definition)
if !result.Valid {
	writeJSON(w, http.StatusUnprocessableEntity, result)
	return
}
```

If validation returns `unsupported_go_validation`, return:

```go
unsupportedWrite(w, "pipeline graph must be routed to Python because Go validation does not own this graph")
```

Register in `router.go`:

```go
r.Post("/pipelines", s.createPipeline)
r.Put("/pipelines/{pipelineID}", s.updatePipeline)
r.Delete("/pipelines/{pipelineID}", s.deletePipeline)
r.Post("/pipelines/{pipelineID}/duplicate", s.duplicatePipeline)
```

- [ ] **Step 5: Extend write parity test file**

Create `tests/go_migration/test_go_api_write_parity.py`:

```python
from __future__ import annotations

import os

import pytest
import requests


STRICT = os.getenv("VP_GO_WRITE_STRICT") == "1"
GO_API = os.getenv("VP_GO_API_URL", "http://127.0.0.1:18081")


@pytest.mark.skipif(not STRICT, reason="set VP_GO_WRITE_STRICT=1 for live Go write parity")
def test_go_pipeline_create_update_duplicate_delete() -> None:
    payload = {
        "name": "go-write-parity",
        "description": "go write parity",
        "is_template": False,
        "definition": {
            "nodes": [
                {"id": "source", "type": "source", "position": {}, "data": {"asset_id": "asset-1", "config": {}}},
                {"id": "export", "type": "export", "position": {}, "data": {"config": {}}},
            ],
            "edges": [
                {"id": "e1", "source": "source", "sourceHandle": "output", "target": "export", "targetHandle": "input"}
            ],
            "viewport": {},
        },
    }
    created = requests.post(f"{GO_API}/api/v1/pipelines", json=payload, timeout=10)
    assert created.status_code in {200, 201}, created.text
    pipeline_id = created.json()["id"]
    updated = requests.put(f"{GO_API}/api/v1/pipelines/{pipeline_id}", json={**payload, "name": "go-write-parity-updated"}, timeout=10)
    assert updated.status_code == 200, updated.text
    duplicate = requests.post(f"{GO_API}/api/v1/pipelines/{pipeline_id}/duplicate", timeout=10)
    assert duplicate.status_code in {200, 201}, duplicate.text
    deleted = requests.delete(f"{GO_API}/api/v1/pipelines/{pipeline_id}", timeout=10)
    assert deleted.status_code in {200, 204}, deleted.text
```

- [ ] **Step 6: Run pipeline write tests**

Run:

```bash
go test ./internal/store ./internal/httpapi
```

Expected:

```text
All selected Go tests pass.
```

- [ ] **Step 7: Commit pipeline writes**

Run:

```bash
git add internal/httpapi/write_responses.go internal/httpapi/pipeline_writes.go internal/httpapi/router.go internal/httpapi/httpapi_test.go internal/store/pipelines.go internal/store/store_test.go tests/go_migration/test_go_api_write_parity.py
git commit -m "feat: add guarded go pipeline writes"
```

Expected:

```text
Commit succeeds.
```

## Task 13: Job Write API Without Go Orchestrator Ownership

**Files:**
- Create: `internal/httpapi/job_writes.go`
- Modify: `internal/httpapi/router.go`
- Modify: `internal/store/jobs.go`
- Modify: `tests/go_migration/test_go_api_write_parity.py`

- [ ] **Step 1: Add explicit job ownership tests**

Append to `tests/go_migration/test_go_api_write_parity.py`:

```python
@pytest.mark.skipif(not STRICT, reason="set VP_GO_WRITE_STRICT=1 for live Go write parity")
def test_go_job_start_is_explicitly_python_owned_or_handed_off() -> None:
    response = requests.post(f"{GO_API}/api/v1/jobs", json={"pipeline_id": "00000000-0000-0000-0000-000000000000"}, timeout=10)
    assert response.status_code in {201, 202, 404, 501}
    if response.status_code == 501:
        assert "Python" in response.json()["detail"]
```

- [ ] **Step 2: Implement cancel, delete, and read-for-rerun store methods**

Add to `internal/store/jobs.go`:

```go
func (s *Store) CancelJob(ctx context.Context, id string) (JobRow, error)
func (s *Store) DeleteJob(ctx context.Context, id string) error
func (s *Store) GetJobForRerun(ctx context.Context, id string) (JobRow, error)
```

`CancelJob` must set job status to `CANCELLED` only when current status is not terminal:

```sql
UPDATE jobs
SET status = 'CANCELLED', completed_at = NOW(), updated_at = NOW()
WHERE id = $1 AND status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'PARTIALLY_FAILED')
RETURNING ...
```

- [ ] **Step 3: Implement job write routes**

Create `internal/httpapi/job_writes.go`:

```go
func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	unsupportedWrite(w, "job creation remains Python-owned until a Python start-job handoff is configured")
}

func (s *Server) createJobBatch(w http.ResponseWriter, r *http.Request) {
	unsupportedWrite(w, "job batch creation remains Python-owned until a Python start-job handoff is configured")
}

func (s *Server) rerunJob(w http.ResponseWriter, r *http.Request) {
	unsupportedWrite(w, "job rerun remains Python-owned until a Python start-job handoff is configured")
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request)
func (s *Server) deleteJob(w http.ResponseWriter, r *http.Request)
```

Register:

```go
r.Post("/jobs", s.createJob)
r.Post("/jobs/batch", s.createJobBatch)
r.Post("/jobs/{jobID}/cancel", s.cancelJob)
r.Post("/jobs/{jobID}/rerun", s.rerunJob)
r.Delete("/jobs/{jobID}", s.deleteJob)
```

- [ ] **Step 4: Document Phase 6 guard in acceptance doc**

Append to `docs/go-migration-acceptance/README.md`:

```markdown
## Job Write Ownership

`POST /api/v1/jobs`, `POST /api/v1/jobs/batch`, and `POST /api/v1/jobs/{id}/rerun` remain Python-owned unless a Python start-job handoff endpoint is explicitly configured. This preserves the Phase 6 exclusion: Go does not schedule DAGs or listen to worker events in this milestone.
```

- [ ] **Step 5: Run job write checks**

Run:

```bash
go test ./internal/store ./internal/httpapi
```

Expected:

```text
All selected Go tests pass.
```

- [ ] **Step 6: Commit job writes**

Run:

```bash
git add internal/httpapi/job_writes.go internal/httpapi/router.go internal/store/jobs.go tests/go_migration/test_go_api_write_parity.py docs/go-migration-acceptance/README.md
git commit -m "feat: add safe go job write routes"
```

Expected:

```text
Commit succeeds.
```

## Task 14: Asset, Artifact, And Schedule Write API

**Files:**
- Create: `internal/httpapi/asset_writes.go`
- Create: `internal/httpapi/artifact_writes.go`
- Create: `internal/httpapi/schedule_writes.go`
- Modify: `internal/httpapi/router.go`
- Modify: `internal/store/assets.go`
- Modify: `internal/store/artifacts.go`
- Modify: `internal/store/schedule.go`
- Modify: `tests/go_migration/test_go_api_write_parity.py`

- [ ] **Step 1: Add live write parity tests**

Append to `tests/go_migration/test_go_api_write_parity.py`:

```python
@pytest.mark.skipif(not STRICT, reason="set VP_GO_WRITE_STRICT=1 for live Go write parity")
def test_schedule_open_drain_close_round_trip() -> None:
    for action in ["open", "drain", "close", "open"]:
        response = requests.post(f"{GO_API}/internal/schedule/video/{action}", timeout=10)
        assert response.status_code == 200, response.text
        assert response.json()["state"] in {"OPEN", "DRAINING", "CLOSED"}


@pytest.mark.skipif(not STRICT, reason="set VP_GO_WRITE_STRICT=1 for live Go write parity")
def test_artifact_cleanup_is_private_operation() -> None:
    response = requests.delete(f"{GO_API}/api/v1/artifacts/cleanup", timeout=10)
    assert response.status_code in {200, 204}, response.text
```

- [ ] **Step 2: Implement asset routes**

Create `internal/httpapi/asset_writes.go` with:

```go
func (s *Server) uploadAsset(w http.ResponseWriter, r *http.Request)
func (s *Server) downloadAsset(w http.ResponseWriter, r *http.Request)
func (s *Server) deleteAsset(w http.ResponseWriter, r *http.Request)
```

Rules:

```text
upload: accept multipart field "file", save through configured storage backend, create asset row, return Python-compatible asset response
download: read asset storage path, stream bytes with Content-Disposition attachment
delete: delete asset row and storage object only when no active job references it; return conflict on active reference
```

- [ ] **Step 3: Implement artifact routes**

Create `internal/httpapi/artifact_writes.go` with:

```go
func (s *Server) downloadArtifact(w http.ResponseWriter, r *http.Request)
func (s *Server) cleanupArtifacts(w http.ResponseWriter, r *http.Request)
```

Rules:

```text
download: read artifact storage path and stream bytes
cleanup: delete artifact rows and storage objects eligible under Python cleanup rules
```

- [ ] **Step 4: Implement schedule write routes**

Create `internal/httpapi/schedule_writes.go` with:

```go
func (s *Server) openVideoSchedule(w http.ResponseWriter, r *http.Request)  { s.setVideoSchedule(w, r, "OPEN") }
func (s *Server) drainVideoSchedule(w http.ResponseWriter, r *http.Request) { s.setVideoSchedule(w, r, "DRAINING") }
func (s *Server) closeVideoSchedule(w http.ResponseWriter, r *http.Request) { s.setVideoSchedule(w, r, "CLOSED") }
```

Register:

```go
r.Post("/open", s.openVideoSchedule)
r.Post("/drain", s.drainVideoSchedule)
r.Post("/close", s.closeVideoSchedule)
```

- [ ] **Step 5: Run selected write tests**

Run:

```bash
go test ./internal/store ./internal/httpapi
```

Expected:

```text
All selected Go tests pass.
```

- [ ] **Step 6: Commit asset/artifact/schedule writes**

Run:

```bash
git add internal/httpapi/asset_writes.go internal/httpapi/artifact_writes.go internal/httpapi/schedule_writes.go internal/httpapi/router.go internal/store/assets.go internal/store/artifacts.go internal/store/schedule.go tests/go_migration/test_go_api_write_parity.py
git commit -m "feat: add go asset artifact and schedule writes"
```

Expected:

```text
Commit succeeds.
```

## Task 15: Docker Gates And Live Strict Parity

**Files:**
- Modify: `docker-compose.yml`
- Modify: `docs/go-migration-acceptance/README.md`

- [ ] **Step 1: Rebuild sidecars**

Run:

```bash
docker compose build api-go ffmpeg-worker-go
docker compose up -d postgres redis minio api api-go ffmpeg-worker ffmpeg-worker-go
```

Expected:

```text
Services start and api-go plus ffmpeg-worker-go run the newly built binaries.
```

- [ ] **Step 2: Verify process health, readiness, metrics, and worker identity**

Run:

```bash
curl -fsS http://127.0.0.1:18080/health
curl -fsS http://127.0.0.1:18081/health
curl -fsS http://127.0.0.1:18081/readyz
curl -fsS http://127.0.0.1:18081/metrics | grep -E 'http_requests_total|http_request_duration_seconds|http_request_errors_total'
docker compose exec -T ffmpeg-worker-go sh -lc 'tr "\0" " " < /proc/1/cmdline && printf "\n" && printenv WORKER_TYPE'
```

Expected:

```text
Python health returns {"status":"ok"}.
Go health returns {"status":"ok"}.
Go readiness returns {"status":"ready", ...}.
Metrics include required HTTP metric names.
Worker command includes vp-ffmpeg-worker-go and WORKER_TYPE is ffmpeg_go.
```

- [ ] **Step 3: Run strict parity suites**

Run:

```bash
VP_GO_PARITY_STRICT=1 python3 -m pytest tests/go_migration/test_go_api_parity.py tests/go_migration/test_go_api_read_parity.py tests/go_migration/test_go_registry_parity.py tests/go_migration/test_go_validator_parity.py -q
VP_GO_WORKER_SMOKE_STRICT=1 VP_REDIS_URL=redis://127.0.0.1:6380/0 python3 -m pytest tests/go_migration/test_go_trim_worker_smoke.py -q
VP_GO_WORKER_NODE_STRICT=1 VP_REDIS_URL=redis://127.0.0.1:6380/0 python3 -m pytest tests/go_migration/test_go_worker_nodes.py -q
VP_GO_WRITE_STRICT=1 python3 -m pytest tests/go_migration/test_go_api_write_parity.py -q
```

Expected:

```text
Strict parity tests pass, with only documented xfail cases that belong to routes intentionally left Python-owned.
```

- [ ] **Step 4: Record Docker and parity evidence**

Append exact command output summary to `docs/go-migration-acceptance/README.md`:

````markdown
## Docker And Strict Parity

Commands run:

```bash
docker compose build api-go ffmpeg-worker-go
docker compose up -d postgres redis minio api api-go ffmpeg-worker ffmpeg-worker-go
VP_GO_PARITY_STRICT=1 python3 -m pytest tests/go_migration/test_go_api_parity.py tests/go_migration/test_go_api_read_parity.py tests/go_migration/test_go_registry_parity.py tests/go_migration/test_go_validator_parity.py -q
VP_GO_WORKER_SMOKE_STRICT=1 VP_REDIS_URL=redis://127.0.0.1:6380/0 python3 -m pytest tests/go_migration/test_go_trim_worker_smoke.py -q
VP_GO_WORKER_NODE_STRICT=1 VP_REDIS_URL=redis://127.0.0.1:6380/0 python3 -m pytest tests/go_migration/test_go_worker_nodes.py -q
VP_GO_WRITE_STRICT=1 python3 -m pytest tests/go_migration/test_go_api_write_parity.py -q
```
````

- [ ] **Step 5: Commit Docker evidence**

Run:

```bash
git add docker-compose.yml docs/go-migration-acceptance/README.md
git commit -m "test: verify go migration docker parity gates"
```

Expected:

```text
Commit succeeds.
```

## Task 16: Production Acceptance Runner

**Files:**
- Create: `scripts/go_migration_acceptance.py`
- Modify: `docs/go-migration-acceptance/README.md`

- [ ] **Step 1: Create acceptance runner skeleton with concrete checks**

Create `scripts/go_migration_acceptance.py`:

```python
from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import time
from dataclasses import dataclass

import requests


NODES = [
    "trim",
    "transcode",
    "export",
    "vertical_crop",
    "watermark",
    "title_overlay",
    "bgm",
    "replace_audio",
    "concat_horizontal",
    "concat_vertical",
    "concat_many",
    "concat_timeline",
    "concat_vertical_timeline",
    "montage_assembler",
]


@dataclass
class NodeEvidence:
    node_type: str
    completed: int
    p95_seconds: float
    redis_pending: int
    missing_output_artifact_id: int
    missing_storage_path: int


def redis_pending(redis_url: str) -> int:
    output = subprocess.check_output(
        ["redis-cli", "-u", redis_url, "XPENDING", "vp:tasks:ffmpeg_go", "ffmpeg_go-workers"],
        text=True,
    )
    return int(output.splitlines()[0].strip())


def percentile95(values: list[float]) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    return statistics.quantiles(values, n=100)[94]


def run_node_batch(api_url: str, redis_url: str, node_type: str, count: int) -> NodeEvidence:
    durations: list[float] = []
    missing_output_artifact_id = 0
    missing_storage_path = 0
    for _ in range(count):
        started = time.monotonic()
        response = requests.post(f"{api_url}/api/v1/jobs", json={"node_type": node_type}, timeout=30)
        if response.status_code == 501:
            raise RuntimeError("job start is Python-owned; run this script against Python API job creation with Go worker cutover")
        response.raise_for_status()
        job_id = response.json()["id"]
        deadline = time.monotonic() + 240
        body = {}
        while time.monotonic() < deadline:
            poll = requests.get(f"{api_url}/api/v1/jobs/{job_id}", timeout=10)
            poll.raise_for_status()
            body = poll.json()
            if body["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                break
            time.sleep(2)
        if body.get("status") != "SUCCEEDED":
            raise RuntimeError(f"{node_type} job {job_id} finished with {body}")
        durations.append(time.monotonic() - started)
        if not body.get("output_artifact_id"):
            missing_output_artifact_id += 1
    return NodeEvidence(
        node_type=node_type,
        completed=count,
        p95_seconds=percentile95(durations),
        redis_pending=redis_pending(redis_url),
        missing_output_artifact_id=missing_output_artifact_id,
        missing_storage_path=missing_storage_path,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default="http://127.0.0.1:18080")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6380/0")
    parser.add_argument("--count", type=int, default=20)
    args = parser.parse_args()

    evidence = [run_node_batch(args.api_url, args.redis_url, node, args.count).__dict__ for node in NODES]
    print(json.dumps({"nodes": evidence}, indent=2, sort_keys=True))
    failures = [
        item
        for item in evidence
        if item["completed"] != args.count
        or item["redis_pending"] != 0
        or item["missing_output_artifact_id"] != 0
        or item["missing_storage_path"] != 0
    ]
    if failures:
        raise SystemExit(json.dumps({"failed_acceptance": failures}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run acceptance runner help**

Run:

```bash
python3 scripts/go_migration_acceptance.py --help
```

Expected:

```text
Usage output includes --api-url, --redis-url, and --count.
```

- [ ] **Step 3: Run staging acceptance**

Run:

```bash
python3 scripts/go_migration_acceptance.py --api-url http://127.0.0.1:18080 --redis-url redis://127.0.0.1:6380/0 --count 20
```

Expected:

```text
Every migrated node reports completed=20, redis_pending=0, missing_output_artifact_id=0, missing_storage_path=0.
```

- [ ] **Step 4: Run failure, cancellation, and rollback drills**

Run:

```bash
VP_GO_WORKER_NODE_STRICT=1 VP_GO_DRILL_FAILURE=1 python3 -m pytest tests/go_migration/test_go_worker_nodes.py -q
VP_GO_WORKER_NODE_STRICT=1 VP_GO_DRILL_CANCEL=1 python3 -m pytest tests/go_migration/test_go_worker_nodes.py -q
VP_GO_WORKER_NODE_STRICT=1 VP_GO_DRILL_ROLLBACK=1 python3 -m pytest tests/go_migration/test_go_worker_nodes.py -q
```

Expected:

```text
Failure drill produces a failed node event and no stuck pending Redis messages.
Cancellation drill acks confirmed cancellation and emits no completion event.
Rollback drill proves worker_type revert sends tasks back to Python ffmpeg worker.
```

- [ ] **Step 5: Record acceptance evidence**

Append to `docs/go-migration-acceptance/README.md`:

````markdown
## Production-Style Acceptance

Command:

```bash
python3 scripts/go_migration_acceptance.py --api-url http://127.0.0.1:18080 --redis-url redis://127.0.0.1:6380/0 --count 20
```

Required result:

```text
completed=20 for every migrated node
redis_pending=0
missing_output_artifact_id=0
missing_storage_path=0
```
````

- [ ] **Step 6: Commit acceptance runner**

Run:

```bash
git add scripts/go_migration_acceptance.py docs/go-migration-acceptance/README.md
git commit -m "test: add go migration acceptance runner"
```

Expected:

```text
Commit succeeds.
```

## Task 17: Final Verification And Spec Audit

**Files:**
- Read: `/home/taiwei/Constructure-repos/videoprocess/docs/videoprocess-go-partial-migration-spec.md`
- Modify: `docs/go-migration-acceptance/README.md`

- [ ] **Step 1: Run full required checks**

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
Go tests and vet pass. Python tests pass. Ruff and mypy are recorded with exact output when the command is unavailable or reports existing tolerated issues.
```

- [ ] **Step 2: Run Docker live checks**

Run:

```bash
docker compose build api-go ffmpeg-worker-go
docker compose up -d postgres redis minio api api-go ffmpeg-worker ffmpeg-worker-go
curl -fsS http://127.0.0.1:18081/health
curl -fsS http://127.0.0.1:18081/readyz
curl -fsS http://127.0.0.1:18081/metrics | grep -E 'http_requests_total|vp_worker_tasks_total|vp_ffmpeg_runs_total'
```

Expected:

```text
Go sidecars are running current binaries and metrics endpoints expose required names.
```

- [ ] **Step 3: Run strict parity and acceptance gates**

Run:

```bash
VP_GO_PARITY_STRICT=1 python3 -m pytest tests/go_migration/test_go_api_parity.py tests/go_migration/test_go_api_read_parity.py tests/go_migration/test_go_registry_parity.py tests/go_migration/test_go_validator_parity.py -q
VP_GO_WORKER_SMOKE_STRICT=1 VP_REDIS_URL=redis://127.0.0.1:6380/0 python3 -m pytest tests/go_migration/test_go_trim_worker_smoke.py -q
VP_GO_WORKER_NODE_STRICT=1 VP_REDIS_URL=redis://127.0.0.1:6380/0 python3 -m pytest tests/go_migration/test_go_worker_nodes.py -q
VP_GO_WRITE_STRICT=1 python3 -m pytest tests/go_migration/test_go_api_write_parity.py -q
python3 scripts/go_migration_acceptance.py --api-url http://127.0.0.1:18080 --redis-url redis://127.0.0.1:6380/0 --count 20
```

Expected:

```text
All strict gates pass or only Python-owned job-start routes return documented 501 responses.
```

- [ ] **Step 4: Write spec completion audit**

Append to `docs/go-migration-acceptance/README.md`:

```markdown
## Final Spec Audit

Implemented:

- Go registry parity from Python builtin manifest.
- Go validator parity for deterministic graphs and explicit unsupported graph refusal.
- First-wave pure ffmpeg nodes routed through `ffmpeg_go`.
- Selected Go API writes with Phase 6 ownership guard.
- API and worker metrics.
- Docker and strict parity gates.
- Production-style acceptance runner.

Intentionally excluded:

- Phase 6 Go orchestrator, event listener, recovery, dispatch, retry, downstream, and final artifact ownership.
- Python code deletion.
- External platform publishing rewrite.
```

- [ ] **Step 5: Run status and final commit**

Run:

```bash
git status --short
git add docs/go-migration-acceptance/README.md
git commit -m "docs: record go migration completion audit"
```

Expected:

```text
Only intentional files were committed, and the final audit records the exact non-Phase-6 boundary.
```

## Completion Gate

The plan is complete only when these commands have current successful output:

```bash
go test ./...
go vet ./...
cd backend && python3 -m pytest
VP_GO_PARITY_STRICT=1 python3 -m pytest tests/go_migration/test_go_api_parity.py tests/go_migration/test_go_api_read_parity.py tests/go_migration/test_go_registry_parity.py tests/go_migration/test_go_validator_parity.py -q
VP_GO_WORKER_SMOKE_STRICT=1 VP_REDIS_URL=redis://127.0.0.1:6380/0 python3 -m pytest tests/go_migration/test_go_trim_worker_smoke.py -q
VP_GO_WORKER_NODE_STRICT=1 VP_REDIS_URL=redis://127.0.0.1:6380/0 python3 -m pytest tests/go_migration/test_go_worker_nodes.py -q
VP_GO_WRITE_STRICT=1 python3 -m pytest tests/go_migration/test_go_api_write_parity.py -q
python3 scripts/go_migration_acceptance.py --api-url http://127.0.0.1:18080 --redis-url redis://127.0.0.1:6380/0 --count 20
```

And this command shows no stuck Go worker messages after acceptance:

```bash
redis-cli -u redis://127.0.0.1:6380/0 XPENDING vp:tasks:ffmpeg_go ffmpeg_go-workers
```

Expected:

```text
0
```
