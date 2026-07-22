# Unlisted Canary Feedback Loop Activation Design

Status: superseded on 2026-07-22 by the atomic intake-pause design.

The 2026-07-12 `halt-after-selection` procedure described below is retained
as historical context only. It is replaced by the 2026-07-22 atomic intake
pause: a successful attempt remains intake-paused for downstream and mature
metrics, while a failed attempt becomes fully halted.

## Context

VideoProcess is deployed continuously across the 150 control/infra host and
the 127 application node. The live YouTubeManager on 150 is authenticated and
can upload, inspect, schedule, and read metrics for real YouTube videos.
Historical controlled smoke/soak runs created real unlisted videos, but the
current managed FFmpeg worker deliberately has no YouTube credentials.

The existing upload path cannot simply be re-enabled:

- `youtube_upload` is routed to the general `ffmpeg` stream.
- The direct Google side effect happens before durable upload evidence is
  written, so a crash and retry can create a second video.
- The old soak channel currently has a halted-test backlog of 42 jobs that
  would all become runnable if the global video window were opened.
- YouTubeManager upload tasks are held in memory, so a manager restart can
  make an accepted task temporarily unverifiable.

This design activates one real, controlled feedback-loop canary without
turning those hazards into unattended publishing behavior.

## Goal

Run exactly one production-shaped flow that:

1. acquires an explicitly owned/generated source asset;
2. builds a deterministic AutoFlow video from that asset;
3. uploads it to YouTube as `unlisted` through the authenticated manager;
4. creates one linked publication record;
5. verifies the live YouTube status and metrics contract;
6. leaves a durable metrics collection item for the normal runner.

## Non-Goals

- No public publication or public scheduling.
- No automatic publication of external-platform assets.
- No bulk release of the historical soak backlog.
- No bandit, Thompson sampling, or automatic policy-weight changes.
- No rewrite of the external YouTubeManager repository in this increment.

## Approaches Considered

### Dedicated publisher stream with a VP operation ledger (selected)

Route `youtube_upload` to `vp:tasks:youtube_publisher`. A dedicated worker
submits media to YouTubeManager, while a Postgres upload-operation row is
created before the external request and records the manager task and final
video ID. Retries resume or return the existing operation; an ambiguous
operation is held and never blindly uploaded again.

This preserves credential isolation, is deployable from the VideoProcess
repository, and gives the canary a fail-closed duplicate boundary.

### Restore credentials on the general FFmpeg worker (rejected)

This is fast but exposes OAuth files to unrelated media handlers and lets the
existing backlog compete for the same queue. It also preserves the current
upload-before-durability retry gap.

### Move all publication control into YouTubeManager (deferred)

A persistent, idempotent YouTubeManager publication API is the preferred
longer-term authority. It requires coordinated cross-repository schema,
deployment, and reconciliation work, so it is too broad for the first safe
canary.

## Architecture

### Worker isolation

- Change the node registry and capability manifest so `youtube_upload` uses
  worker type `youtube_publisher`.
- Deploy `vp-youtube-publisher-swarm` on the 150 manager under the dedicated
  `node.labels.vp.publisher == true` constraint.
- The publisher receives Postgres, Redis, MinIO, and `YOUTUBE_MANAGER_URL`
  settings. It receives no OAuth or YouTube credential mount.
- The general Python FFmpeg worker remains credential-free and cannot consume
  publisher tasks.
- Production admission requires `YOUTUBE_PUBLISH_ENABLED=true`,
  `PUBLIC_PUBLISH_ENABLED=false`, a non-local manager URL, and shared MinIO
  settings for a `youtube_publisher` worker.

### Durable upload operation

Add `youtube_upload_operations` with:

- `id` UUID primary key;
- optional unique `production_task_id`;
- `job_id` and unique `node_execution_id`;
- `input_artifact_id` and `content_sha256`;
- `title` and `privacy`;
- `status`: `reserved`, `submitted`, `succeeded`, `uncertain`, or `failed`;
- optional `manager_task_id`;
- optional unique `platform_video_id`;
- `error_message`, `request_attempted_at`, `completed_at`, and timestamps.

The first execution creates `reserved`. Only the process that created that row
may submit a new manager upload request. It immediately stores the returned
manager task ID and changes the state to `submitted` before polling.

On retry:

- `succeeded` returns the stored video receipt and creates no side effect;
- `submitted` resumes polling the same manager task;
- a pre-existing `reserved`, missing manager task, missing manager task after
  restart, timeout, or transport ambiguity becomes `uncertain` and blocks;
- `failed` remains blocked until an explicit operator retry/reconciliation
  action is designed and approved.

This favors a held task over a duplicate video. It does not claim impossible
exactly-once semantics from the YouTube API.

The same migration first reports any historical conflicts, then adds unique
indexes for `publication_records.production_task_id` and
`publication_records(platform, platform_content_id)`. Migration aborts rather
than guessing which conflicting publication is authoritative.

### YouTubeManager adapter

The handler uses `POST /api/upload` with multipart media and polls
`GET /api/status/{task_id}`. Before submission it verifies
`GET /api/auth/status` reports authenticated and at least 1,600 estimated quota
units remaining. Only `private` and `unlisted` are accepted; this canary always
uses `unlisted`.

### Owned canary material

Create one deterministic 9:16 MP4 locally with generated graphics, narration
or tone, and no third-party media. Upload it through the asset API with
explicit owned/generated provenance in `media_info`.

Manual-seed constraints may provide an `input_asset_id`. ChannelOps forwards
that ID as AutoFlow's top-level `input_asset_id` and uses
`source_strategy=input_video`, `source_policy=owned_only`, and template mode.
This exercises asset acquisition and video generation without relying on the
unsafe historical browser-smoke material library.

### Backlog quarantine

Before opening the video schedule:

1. halt the old `channelops-soak-trial` channel;
2. cancel its non-terminal jobs through the job API;
3. move its non-terminal ChannelOps queue rows to `dead_lettered` with the
   reason `operator_quarantine_before_unlisted_canary`;
4. move linked non-terminal production tasks to `held` with the same guard;
5. retain all rows and write a JSON evidence report containing counts and IDs.

The operation is dry-run by default and idempotent under `--apply`.

### Canary orchestration

Create a new channel with one lane, one publishing account, one format, and one
manual seed. The account and format default to `unlisted`, external asset auto
publish is false, maximum cadence is one per day, and the channel starts in
dry-run mode.

After all preflights pass, switch only this channel out of dry-run and enqueue
one guarded tick. The 2026-07-22 atomic intake pause must create exactly one
production task and pause new intake in the same transaction. With the old
backlog quarantined, open the global video window, wait for the canary job to
start, then drain and close the window. A successful attempt keeps the channel
intake-paused while the ChannelOps runner continues the selected task through
publication, reconciliation, and mature metrics. A failed attempt fully halts
the channel and its active canary work.

## Safety Rules

- `public` is rejected in the publisher regardless of node input.
- New account and lane-format API defaults are `private`.
- One canary production task may own at most one upload operation.
- One platform video ID may appear in at most one upload operation and one
  publication.
- No external-platform asset is eligible for this canary.
- The schedule returns to `CLOSED` in a `finally` guard.
- The old halt-after-selection state is superseded by the atomic intake pause.
- Success leaves the canary channel intake-paused for downstream and mature
  metrics; failure applies the full halt state.

## Feedback

The normal promotion path enqueues durable `collect_metrics` and
`reconcile_publication` work. Acceptance also performs a read-only live status
and metrics request immediately after upload to prove connectivity. The queued
metrics item remains responsible for the age-appropriate persisted snapshot;
the immediate probe is evidence only and is not mislabeled as a 1-hour reward.
The intake pause remains in force so downstream reconciliation and mature
metrics can finish without admitting another task.

## Deployment

The existing scoped 150 deploy controller builds the shared Python worker
image, creates or updates the publisher service, and includes it in health and
rollback snapshots. Deployment validates YouTubeManager auth but never prints
tokens or credential contents.

The current 150/127 placement remains unchanged for all other services, and
126 receives neither a publisher label nor a VideoProcess task.

## Verification

Automated checks cover:

- publisher worker admission and credential rejection;
- registry/manifest routing to `youtube_publisher`;
- public privacy rejection;
- fresh reservation, submitted-task resume, succeeded replay, and uncertain
  operation blocking;
- manager auth, upload, poll, failure, and timeout behavior with fake HTTP;
- manual seed `input_asset_id` propagation;
- quarantine dry-run, apply, and idempotence;
- deploy service placement, env, health, and rollback;
- existing backend, Go, and shell suites.

Live acceptance requires:

- exactly one new production task, upload operation, platform video ID, and
  publication after the canary start timestamp;
- YouTube status `processed` and privacy `unlisted`;
- no `public` publication or queued promotion to public;
- one immediate read-only metrics response and one durable metrics queue item;
- no VP tasks or publisher consumers on 126;
- both Redis worker groups at zero pending after completion;
- final video schedule `CLOSED`.

The next approval is exactly:

```text
批准第四次 unlisted canary
```

## Rollback

Halt the canary channel, close the video schedule, scale
`vp-youtube-publisher-swarm` to zero, and leave the upload-operation ledger and
publication evidence intact. The deployment controller restores the prior
images and dedicated placement constraints. A successfully uploaded unlisted
video is never deleted automatically; removal requires a separate explicit
operator decision.
