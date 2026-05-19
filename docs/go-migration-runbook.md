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

## Cutover Rule

Point frontend or proxy traffic at `api-go` only after health, node-types, pipelines, jobs, assets, and deterministic AutoFlow parity tests pass.

Switch a node registry entry to `ffmpeg_go` only after that node has a Go handler, a Redis worker loop consumes `ffmpeg_go` tasks, fixture media tests pass, and a visible smoke output is inspectable.

## Rollback

```bash
docker compose stop api-go ffmpeg-worker-go
docker compose up -d api ffmpeg-worker
```
