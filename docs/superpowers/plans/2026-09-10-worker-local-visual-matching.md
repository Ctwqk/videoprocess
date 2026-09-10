# Worker-Local Visual Matching Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable real visual scoring on the already managed 150 vision worker.

**Architecture:** Keep remote visual scoring compatible. When no URL is set,
use an explicitly configured, image-baked Chinese-CLIP model in a bounded child
process of the existing worker; use the same validated similarity matrix.

**Tech Stack:** Python 3.12, asyncio subprocess, Transformers ChineseCLIPModel,
CPU PyTorch, Pillow, existing Docker worker and CI-gated Swarm deployment.

**Spec:** `docs/superpowers/specs/2026-09-10-worker-local-visual-matching-design.md`

## Global Constraints

- Existing default publication and explicit human-review rules remain unchanged.
- Model revision `f4a64596bbcf9a2a94591b74b9dc39b2e4e77e3e`, safetensors only.
- No remote model code or inference-time downloads; CPU-only, two threads.
- Maximum 240 frames, two texts, 512 characters per text; batches of four.
- Child deadline 120 seconds; every exit path reaps the child.
- Raw normalized cosine matrix; no softmax or threshold changes.
- Preserve remote URL priority; no silent local fallback after remote failure.
- 150 hosts vision; 127 executes/exports; 126 remains excluded.

---

### Task 1: Pinned Offline Model Command

**Files:**
- Create: `backend/worker/visual_embedding_model.py`
- Create: `backend/worker/visual_embedding_cli.py`
- Create: `backend/requirements-vision.txt`
- Create: `backend/tests/worker/test_visual_embedding_cli.py`

**Interfaces:**
- `score_images(model_path: str, texts: list[str], image_paths: list[str]) -> list[list[float]]`
- CLI stdin: `{"model_path": "...", "texts": ["..."], "image_paths": ["..."]}`
- CLI stdout: `{"similarities": [[0.4]], "model_revision": "f4a64596bbcf9a2a94591b74b9dc39b2e4e77e3e"}`
- Download command: `python worker/visual_embedding_model.py --download DIRECTORY`

- [x] Write tests rejecting empty/oversized inputs and missing local models
  before model import/download; subprocess exit must be nonzero, stdout must not
  contain a fabricated successful matrix. Example:
  ```python
  result = subprocess.run([sys.executable, "-m", "worker.visual_embedding_cli"],
                          input=json.dumps({"model_path": "/missing",
                              "texts": ["blue"], "image_paths": ["/missing.jpg"]}),
                          text=True, capture_output=True)
  assert result.returncode != 0
  assert '"similarities"' not in result.stdout
  ```
- [x] Run focused tests and record missing-command/behavior RED evidence.
- [x] Implement validation before lazy heavy imports, official processor/model
  offline loading, bounded batches, and normalized output:
  ```python
  with torch.inference_mode():
      output = model(**processor(text=texts, images=images,
                                 return_tensors="pt", padding=True))
      matrix = output.image_embeds @ output.text_embeds.T
  ```
  Validate no truncation beyond 512 characters; use tokenizer truncation at its
  supported length when token count exceeds the model's positional capacity.
  Decode images with Pillow context managers; close them after each batch.
- [x] Implement build-only snapshot download with an explicit allowlist of
  model/config/tokenizer files, fixed revision, and streaming SHA-256 validation
  of `model.safetensors`; reject a mismatch. Preserve upstream provenance.
- [x] Pin visual dependencies separately from API/test dependencies. Verify
  the command with the real model on 150 in an isolated container with no network
  during inference, Chinese positive fixtures, singleton query, and batch order.
- [x] Run focused tests and commit only the task files.

### Task 2: Smart Trim Child Lifecycle and Matrix Validation

**Files:**
- Modify: `backend/app/config.py`
- Modify: `backend/worker/handlers/smart_trim.py`
- Modify: `backend/tests/worker/test_smart_trim_handler.py`
- Create: `backend/tests/worker/test_smart_trim_visual_process.py`

**Interfaces:**
- Add `vision_embedding_model_path: str = ""` setting.
- Add `_score_local_frames(model_path, frames, config)` using the Task 1 JSON CLI.
- Keep `_score_frames(endpoint, frames, config)` as the remote implementation;
  both use one exact-shape numeric cosine validator/scorer.

- [x] Add RED tests for missing/extra rows/columns, NaN/infinity, booleans,
  strings, and out-of-range similarities. Include a literal valid negative
  penalty case: `[[0.8, 0.5]]` produces score `0.6`.
- [x] Add selection tests proving URL priority, opt-in local path, existing
  missing-provider diagnostic, local invalid-output warning, and no-match stop.
- [x] Add real-child lifecycle tests with a tiny Python fixture replacing only
  process argv construction: successful JSON, nonzero exit, timeout, task cancel,
  handler cancel before spawn and during execution. Check no live child remains.
- [x] Implement a per-handler child in `self._proc`, awaiting communication under
  the 120-second deadline and killing/awaiting it in a `finally` block. Propagate
  asyncio and handler cancellation before the visual-warning catch; prevent
  spawning after handler cancellation. Use argv, never a shell.
- [x] Implement provider selection and shared validation without changing ASR,
  match thresholds, publication policy, or deterministic workflow builders.
- [x] Run focused tests, full backend pytest, Ruff and mypy; commit task files.

### Task 3: Existing Automatic Deployment and Real Render

**Files:**
- Modify: `backend/Dockerfile.worker`
- Modify: `deploy/swarm/deploy-sync-extension.sh`
- Modify: `tests/test_vp_deploy_sync_extension.sh`
- Update: the spec's operational evidence and this checklist.

**Interfaces:**
- Worker image contains model at `/usr/local/share/videoprocess/chinese-clip`.
- `vp_vision_worker_env` passes
  `VISION_EMBEDDING_MODEL_PATH=/usr/local/share/videoprocess/chinese-clip`.
- Existing `VISION_EMBEDDING_URL` override remains higher priority in the worker.

- [x] Add a shell behavior check by invoking the deployment environment function:
  ```bash
  output="$(vp_vision_worker_env vp-ffmpeg-worker-python:deploy-0123456789ab)"
  grep -Fx 'VISION_EMBEDDING_MODEL_PATH=/usr/local/share/videoprocess/chinese-clip' <<<"$output"
  ```
  Use the existing registration fixture required by that function. Verify other
  worker environment functions do not opt into the local model.
- [x] Add Docker install/download layers before application source copying;
  use the separate visual requirements and CPU Torch wheel index. Download and
  hash-check before service rotation. Keep the directory read-only to UID 10001.
- [ ] Build an isolated candidate on 150 and run the real CLI offline, then run
  existing deployment tests and an independent change review.
- [ ] Push the reviewed commit under the user's prior approval; observe exact-SHA
  CI and the normal deployment timer without bypassing admission/rollback gates.
- [ ] Run one guarded, owned-source, visual-only production preview. Confirm
  150 visual work, 127 transcode/export, actual non-placeholder video, zero upload
  operations, closed intake, no active jobs, and healthy current worker leases.
- [ ] Record actual results and remaining soak/public-phase gaps. Do not count
  this no-upload preview as a clean YouTube publication canary.
