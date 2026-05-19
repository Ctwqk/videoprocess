# Go Migration Runbook

This runbook keeps the Go API and Go ffmpeg worker as sidecars until route and media-output parity is proven.

## Sidecar Start

```bash
docker compose up -d --build api api-go frontend
curl -fsS http://127.0.0.1:18080/health
curl -fsS http://127.0.0.1:18081/health
```

Run API parity in strict mode after both services are healthy:

```bash
VP_GO_PARITY_STRICT=1 python3 -m pytest tests/go_migration/test_go_api_parity.py -q
```

CI must use strict mode. Without `VP_GO_PARITY_STRICT=1` an unreachable service is reported as `skip`, which is convenient for local runs but silently hides regressions in CI. The strict guard belongs in whatever script invokes the parity suite.

The parity suite enforces:

- `/health` exact-equality.
- list endpoint envelopes (`{items, total}`) for `/pipelines`, `/templates`, `/jobs`, `/assets`.
- `/internal/schedule/video/status` shape.
- `/api/v1/node-types/trim` field-level overlap.
- Registry coverage: any node type present in Python but missing from Go is reported as `xfail` with the missing list so the migration gap is visible at a glance.

## Go Unit Tests

```bash
go test ./...
```

This must pass before merging any change under `cmd/` or `internal/`. The suite covers ArtifactKind enum casing, MinIO/Local storage backends, list endpoint shapes, validation parity, ffmpeg encode args, runner cancellation/GPU fallback, and the Redis Streams consumer (under miniredis).

## Worker Sidecar Start

```bash
docker compose up -d --build ffmpeg-worker ffmpeg-worker-go
docker compose logs --tail=100 ffmpeg-worker-go
```

Healthy startup logs `starting vp-ffmpeg-worker-go worker_type=ffmpeg_go worker_id=...`. The worker creates the `ffmpeg_go-workers` consumer group on first run. Until a node registry entry switches to `ffmpeg_go`, the Go worker idles on an empty stream — that is expected, not a bug.

## GPU Sidecar Start

```bash
docker compose -f docker-compose.yml -f docker-compose.gpu.yml up -d --build ffmpeg-worker-go
docker compose exec ffmpeg-worker-go ffmpeg -hide_banner -encoders | grep -E 'h264_nvenc|hevc_nvenc'
```

If NVENC reports `OpenEncodeSessionEx failed` or similar capacity error in the worker logs, the ffmpeg runner detects the fragment via `IsGPUCapacityError` and the caller is expected to fall back via `RewriteHardwareArgsForCPU`. The mapping rule is `cq:v X -> crf max(18, X-2)`.

## Cutover Rule

Point frontend or proxy traffic at `api-go` only after these tests pass:

- `go test ./...` is green.
- `VP_GO_PARITY_STRICT=1 pytest tests/go_migration` is green and the registry coverage test does not `xfail` (or you have explicitly accepted the remaining gap for the frontend's needs).
- A `GET /api/v1/pipelines?limit=1` against `api-go` returns real rows from Postgres (not the stub `{items:[],total:0}` fallback that fires when DB is unreachable).

Switch a node registry entry to `ffmpeg_go` only after that node has a Go handler registered in `cmd/vp-ffmpeg-worker/main.go`, the consumer test in `internal/worker/consumer_test.go` covers it, fixture media tests pass, and a visible smoke output is inspectable.

## Rollback

```bash
docker compose stop api-go ffmpeg-worker-go
docker compose up -d api ffmpeg-worker
```

Rollback is always safe: the Go API does not write rows the Python API cannot read (ArtifactKind casing is now uppercase, matching the Python `Enum` constraint), and Go workers consume their own `vp:tasks:ffmpeg_go` stream so no Python task can be stranded by Go absence.
