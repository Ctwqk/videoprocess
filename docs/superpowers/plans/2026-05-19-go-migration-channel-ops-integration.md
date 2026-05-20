# Go Migration ChannelOps Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Merge the remaining ChannelOps branch into the completed Go partial migration branch, deploy the result in Docker, generate a real multi-node video that exercises the Go trim worker, and upload that artifact to YouTube as private.

**Architecture:** Keep ChannelOps hardening in the Python channel-agent surface, because that branch does not move the API/worker contracts that the Go sidecars own. Add one root docs pointer and one Docker integration smoke runner that drives the public API/YouTubeManager APIs end to end, so deployment verification is reproducible instead of manual.

**Tech Stack:** Git, Python/FastAPI backend, Go sidecar API/worker, Redis Streams, Postgres, Docker Compose, ffmpeg/ffprobe, YouTubeManager HTTP API.

---

## File Structure

- Modify: `backend/app/channel_agent/runner.py`, `backend/app/channel_agent/scheduler.py`, `backend/app/channel_agent/service.py`, `backend/app/config.py`, and ChannelOps tests through the merge from `codex/channel-ops-remaining-sprints`.
- Create: `docs/videoprocess-go-partial-migration-spec.md` as a root docs pointer to the committed Superpowers migration spec.
- Create: `scripts/go_channel_ops_integration_smoke.py` as the reusable Docker/live integration runner.
- Create: `backend/tests/go_migration/test_integration_smoke_builder.py` to validate the smoke pipeline builder against `validate_pipeline()`.
- Use existing: `docs/go-migration-runbook.md`, `docker-compose.yml`, `backend/tests/smoke_test.py`, `tests/go_migration/test_go_trim_worker_smoke.py`.

## Task 1: Merge ChannelOps Branch and Classify Changes

**Files:**
- Modify via merge: `backend/app/channel_agent/runner.py`
- Modify via merge: `backend/app/channel_agent/scheduler.py`
- Modify via merge: `backend/app/channel_agent/service.py`
- Modify via merge: `backend/app/config.py`
- Modify via merge: `backend/tests/channel_agent/test_runner.py`
- Modify via merge: `backend/tests/channel_agent/test_scheduler.py`
- Modify via merge: `backend/tests/channel_agent/test_service.py`
- Add via merge: `docs/superpowers/plans/2026-05-19-channel-ops-follow-up-hardening.md`
- Add via merge: `docs/superpowers/specs/2026-05-19-channel-ops-follow-up-hardening-design.md`

- [ ] **Step 1: Confirm clean branch state**

Run:

```bash
git status --short --branch
```

Expected: branch is `codex/go-partial-migration` and no uncommitted files are present before merge execution.

- [ ] **Step 2: Merge the ChannelOps branch**

Run:

```bash
git merge --no-ff codex/channel-ops-remaining-sprints
```

Expected: merge succeeds or reports concrete conflict files.

- [ ] **Step 3: If conflicts appear, classify each conflicted file**

Use this boundary:

```text
Go-owned: cmd/, internal/, backend/Dockerfile.api-go, backend/Dockerfile.ffmpeg-worker-go, ffmpeg_go stream behavior, /readyz, trim registry mapping.
Python-owned: backend/app/channel_agent/, ChannelOps tests, PDS handling, scheduler loops, publication orchestration.
Shared contract: docker-compose.yml, env vars, Redis stream names, artifact payload shape, storage paths, node registry worker_type fields.
```

Expected: keep Python-owned ChannelOps changes in Python; only mirror into Go if a shared contract changed.

- [ ] **Step 4: Inspect merged diff for accidental Go contract drift**

Run:

```bash
git diff --stat HEAD^..HEAD
git diff --name-only HEAD^..HEAD
```

Expected: changed files match the ChannelOps scope plus any intentional integration files added in later tasks.

## Task 2: Add Root Migration Spec Pointer

**Files:**
- Create: `docs/videoprocess-go-partial-migration-spec.md`

- [ ] **Step 1: Add the root docs pointer**

Create `docs/videoprocess-go-partial-migration-spec.md` with this content:

```markdown
# VideoProcess Go Partial Migration Spec

The implementation spec for the partial Go migration is maintained at:

`docs/superpowers/specs/2026-05-19-videoprocess-go-partial-migration-spec.md`

This root file exists because operational tasks and follow-up prompts refer to
`docs/videoprocess-go-partial-migration-spec.md` directly.
```

- [ ] **Step 2: Verify the pointer path**

Run:

```bash
test -f docs/superpowers/specs/2026-05-19-videoprocess-go-partial-migration-spec.md
test -f docs/videoprocess-go-partial-migration-spec.md
```

Expected: both commands exit with status 0.

## Task 3: Add Reproducible Docker Video Smoke Runner

**Files:**
- Create: `scripts/go_channel_ops_integration_smoke.py`
- Create: `backend/tests/go_migration/test_integration_smoke_builder.py`

- [ ] **Step 1: Implement the smoke runner units**

Create `scripts/go_channel_ops_integration_smoke.py` with these responsibilities:

```text
1. Generate local fixture media under /tmp/vp_go_channel_ops_smoke:
   - source video mp4 with testsrc2 + sine audio
   - audio wav/mp3 fixture for bgm
   - png image fixture for watermark
   - srt subtitle fixture
2. Upload all fixtures through POST /api/v1/assets/upload.
3. Build a valid pipeline:
   source_video -> trim -> title_overlay -> subtitle -> watermark -> vertical_crop -> bgm -> transcode -> export
   plus source_audio/source_image/source_subtitle feeding bgm/watermark/subtitle.
4. Validate the pipeline through POST /api/v1/pipelines/validate.
5. Create the pipeline, submit a job, and wait for terminal state.
6. Assert job status is SUCCEEDED.
7. Assert the trim node worker_id starts with ffmpeg_go-worker@ and has an output_artifact_id.
8. Pick the export node artifact when available, otherwise the transcode artifact.
9. Download the final artifact, run ffprobe, and require duration > 1.0 seconds.
10. Upload the final artifact to YouTubeManager POST /api/upload with privacy_status=private.
11. Wait for upload task completion and print JSON containing artifact_path, job_id, trim_worker_id, youtube_video_id, and youtube_url.
```

Expose pure helper functions so tests can validate the graph without live Docker:

```python
from collections.abc import Sequence


def build_pipeline_definition(asset_ids: dict[str, str]) -> dict:
    """Return the pipeline DAG dict used by the live smoke."""


def final_artifact_node_ids() -> Sequence[str]:
    """Return artifact-producing node ids in preference order."""
```

- [ ] **Step 2: Add the builder test**

Create `backend/tests/go_migration/test_integration_smoke_builder.py`:

```python
from __future__ import annotations

import sys
from pathlib import Path

from app.orchestrator.dag import validate_pipeline
from app.schemas.pipeline import PipelineDefinition

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from scripts.go_channel_ops_integration_smoke import build_pipeline_definition, final_artifact_node_ids


def test_integration_smoke_pipeline_is_valid() -> None:
    definition = build_pipeline_definition(
        {
            "video": "00000000-0000-0000-0000-000000000001",
            "audio": "00000000-0000-0000-0000-000000000002",
            "image": "00000000-0000-0000-0000-000000000003",
            "subtitle": "00000000-0000-0000-0000-000000000004",
        }
    )

    result = validate_pipeline(PipelineDefinition(**definition))

    assert result.valid, result.errors
    assert "export_1" in final_artifact_node_ids()
    assert any(node["id"] == "trim_1" and node["type"] == "trim" for node in definition["nodes"])
```

- [ ] **Step 3: Run the new focused test**

Run:

```bash
cd backend && python3 -m pytest tests/go_migration/test_integration_smoke_builder.py -q
```

Expected: test passes.

## Task 4: Run Source-Level Verification

**Files:**
- Verify merged Python and Go code.

- [ ] **Step 1: Run Go tests**

Run:

```bash
go test ./...
```

Expected: all Go packages pass.

- [ ] **Step 2: Run backend tests**

Run:

```bash
cd backend && python3 -m pytest
```

Expected: backend suite passes.

- [ ] **Step 3: Run optional linters without blocking on missing local tools**

Run:

```bash
cd backend && python3 -m ruff check . || true
cd backend && python3 -m mypy app || true
```

Expected: command output is recorded. Missing `ruff` or `mypy` is reported but does not block this task.

- [ ] **Step 4: Run strict Go parity tests against Docker only after services start**

Defer these exact commands to Task 5 after Docker health checks:

```bash
VP_GO_PARITY_STRICT=1 python3 -m pytest tests/go_migration/test_go_api_parity.py tests/go_migration/test_go_api_read_parity.py -q
VP_GO_WORKER_SMOKE_STRICT=1 python3 -m pytest tests/go_migration/test_go_trim_worker_smoke.py -q
```

Expected: strict Go tests pass or document an expected xfail for registry coverage only.

## Task 5: Deploy Docker Services and Run Live Video Smoke

**Files:**
- Use: `docker-compose.yml`
- Use: `scripts/go_channel_ops_integration_smoke.py`

- [ ] **Step 1: Rebuild and recreate the needed services**

Run:

```bash
docker compose up -d --build api api-go ffmpeg-worker ffmpeg-worker-go youtube-manager frontend
```

If a standalone dependency is not already reachable, start it with non-conflicting ports:

```bash
VP_STANDALONE_POSTGRES_PORT=5435 VP_STANDALONE_REDIS_PORT=6380 VP_STANDALONE_MINIO_API_PORT=19000 VP_STANDALONE_MINIO_CONSOLE_PORT=19001 docker compose --profile standalone up -d postgres redis minio
```

Expected: API, Go API, Python ffmpeg worker, Go ffmpeg worker, YouTubeManager, and frontend are running.

- [ ] **Step 2: Verify health endpoints**

Run:

```bash
curl -fsS http://127.0.0.1:18080/health
curl -fsS http://127.0.0.1:18081/health
curl -fsS http://127.0.0.1:18081/readyz
curl -fsS http://127.0.0.1:3001/youtube/api/auth/status
```

Expected: Python API health is ok, Go API health is ok, Go readyz is ready, and YouTube auth status returns JSON.

- [ ] **Step 3: Run strict Go parity and worker smokes**

Run:

```bash
VP_GO_PARITY_STRICT=1 python3 -m pytest tests/go_migration/test_go_api_parity.py tests/go_migration/test_go_api_read_parity.py -q
VP_GO_WORKER_SMOKE_STRICT=1 python3 -m pytest tests/go_migration/test_go_trim_worker_smoke.py -q
```

Expected: parity and worker smokes pass, with only the known registry-coverage xfail accepted if present.

- [ ] **Step 4: Run the maximal integration smoke with private upload**

Run:

```bash
VP_API_BASE=http://127.0.0.1:18080/api/v1 \
VP_YT_BASE=http://127.0.0.1:3001/youtube/api \
python3 scripts/go_channel_ops_integration_smoke.py --upload-youtube --privacy private
```

Expected: command prints JSON with `"ok": true`, a playable `artifact_path`, `trim_worker_id` beginning with `ffmpeg_go-worker@`, and a YouTube video id or URL.

- [ ] **Step 5: Verify Redis has no stuck Go trim tasks**

Run:

```bash
redis-cli -u redis://127.0.0.1:6380 XPENDING vp:tasks:ffmpeg_go ffmpeg_go-workers
```

Expected: pending count is 0 after the smoke job finishes.

## Task 6: Commit and Report

**Files:**
- Commit the merge, root docs pointer, smoke runner, and tests after verification.

- [ ] **Step 1: Check final diff**

Run:

```bash
git status --short
git diff --check
```

Expected: no whitespace errors; only intended files are modified.

- [ ] **Step 2: Commit the integration work**

Run:

```bash
git add docs/videoprocess-go-partial-migration-spec.md scripts/go_channel_ops_integration_smoke.py backend/tests/go_migration/test_integration_smoke_builder.py
git commit -m "test: add go channel ops integration smoke"
```

If the merge command created a merge commit already, do not squash it. Keep the smoke/doc commit separate.

- [ ] **Step 3: Final report**

Report:

```text
merged branch: codex/channel-ops-remaining-sprints
Go conversion decision: ChannelOps remained Python unless shared-contract changes were found
Docker services: list running service names and health results
Verification commands: list pass/fail/xfail results
Artifact path: absolute local path
YouTube privacy: private
YouTube result: video id and URL when available
```
