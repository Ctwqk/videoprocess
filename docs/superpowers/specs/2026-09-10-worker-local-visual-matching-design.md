# Worker-Local Visual Matching

## Scope and Approval

Complete the missing visual scoring path of the existing Smart Trim worker.
The user preapproved ideas/specifications/plans and requested prompt closeout.
This design does not authorize additional YouTube uploads or public publication
of external-platform assets without human review.

## Evidence and Alternatives

The existing 150 shared embedding gateway is text-only. A constructure-runtime
push runs Redis deployment, not an embedding gateway rebuild. Changing that
shared service would introduce unrelated deployment ownership and rollout risk.

An isolated Chinese-CLIP CPU probe on 150 correctly ranked three colored-image
fixtures in 1.27 seconds with two CPU threads. This establishes basic inference
feasibility, not general content quality or threshold calibration.

Considered approaches:

1. Extend the shared gateway: rejected because it affects other consumers and
   currently lacks a scoped automatic deployment path.
2. Add a VP visual HTTP service: viable but adds a service, port, and independent
   rollback surface that this single existing worker does not need.
3. Run the model in a bounded child of the existing vision worker: selected.
   The worker already owns frame extraction, job cancellation, and scoring.
   A child process keeps model loading and CPU work off its registration loop,
   and releases model memory after each job without a background inference task.

## Runtime Contract

- Preserve the existing remote `/embed-image-text` protocol and URL setting.
- Add optional `vision_embedding_model_path`, empty by default. When the remote
  URL is configured, use it exclusively; errors must not silently switch models.
- On 150, deployment configures the baked model path for the vision worker only.
  127 remains the CPU execution/export host. 126 is not involved.
- The local command is the current Python executable in isolated mode running
  `worker.visual_embedding_cli`. Input JSON contains only extracted local frame
  paths, one or two query strings, and the configured model directory.
- Load only local safetensors with `trust_remote_code=False`. No runtime model
  downloads. Fix the model revision and verify the weight SHA-256 during build.
- Inference is CPU-only with two Torch threads, batches of four, at most 240
  images, two texts, and 512 characters per text. Use the official processor.
- Output raw normalized image/text cosine similarities, not a softmax across
  candidate texts. Keep existing threshold and negative-prompt penalty behavior.
- Validate an exact images-by-texts matrix, finite numeric values in [-1, 1],
  and reject booleans, strings, missing rows, or extra rows/columns. Both remote
  and local paths pass through this check before any windows are accepted.
- Use a 120-second child deadline. Cancellation, timeout, failure, and normal
  completion all reap the child before frame-directory cleanup. Handler cancel
  must never leave inference alive. Cancellation is not a scoring warning.
- Missing/invalid model or provider output reports unavailable visual scoring;
  existing ASR fallback remains possible. A visual-only nonmatch stops without
  generating a placeholder unless explicitly requested for preview.

## Packaging and Deployment

The existing `backend/Dockerfile.worker` installs pinned visual-only dependencies
and bakes the pinned model into `/usr/local/share/videoprocess/chinese-clip`.
Build-time download and validation occur before worker rotation. The model is
read-only to the existing non-root worker user. Other images do not acquire Torch.
Existing registered-worker CI, admission, rollback, and normal timer deployment
remain authoritative. No direct production image/container mutation is needed.

## Verification

- Unit regressions for matrix validation and unchanged scoring math.
- Real subprocess tests for successful JSON, failure, deadline, task cancellation,
  and handler cancellation; prove child termination, not just mock calls.
- Real pinned-model integration: Chinese positive/negative fixtures, singleton
  query not forced to 1, deterministic batch order, finite matrix, missing local
  model failure, no network at inference time.
- Full backend pytest plus advisory Ruff/mypy; existing deployment contracts.
- Exact-SHA CI and ordinary 150/127 rollout, then one guarded owned-source render
  using visual-only scoring, with real 150/127 worker identities, playable media,
  no placeholder, no upload operation, and final queue/registration audit.

## Model Provenance

Model: `OFA-Sys/chinese-clip-vit-base-patch16`.
Safetensors revision: `f4a64596bbcf9a2a94591b74b9dc39b2e4e77e3e`.
Weight SHA-256: `29cc0b2bcf6ff777f2e15742be92b110e4acbdb2068356e862c4637a4b15fe4f`.

Primary references:
- [Official model](https://huggingface.co/OFA-Sys/chinese-clip-vit-base-patch16)
- [Transformers interface](https://huggingface.co/docs/transformers/model_doc/chinese_clip)
- [Upstream code license](https://github.com/OFA-Sys/Chinese-CLIP/blob/master/MIT-LICENSE.txt)

The upstream code repository carries an MIT license; its model card has no
separate license field. Preserve upstream attribution in the internal image.
Do not represent this integration as establishing rights to unrelated footage.
