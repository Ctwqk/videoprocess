# ChannelOps Soak Guard Design

Status: pre-approved for implementation on 2026-07-19.

## Context

VideoProcess now deploys continuously from GitHub through the scoped deploy
controller on 150. Application services run on 127, shared infrastructure and
the dedicated YouTube publisher run on 150, and 126 is excluded from normal VP
placement.

The existing `channelops-soak-watch.sh` does not share those deployment
properties. It is a hand-maintained file under the deploy controller rather
than a repository asset. It monitors a retired June channel, contains a
database URL fallback, omits the Python worker, YouTube publisher, and
publisher Redis stream, and only prints failures. A VP push cannot update or
recreate it, and a production fault does not halt work.

The next real YouTube attempt still requires separate per-attempt approval.
This increment prepares a fail-closed, review-gated long-running mode without
starting a publication or enabling a channel.

## Goal

Provide a repository-owned soak guard that:

1. is installed and scheduled automatically by the existing scoped VP deploy;
2. is disabled unless an operator supplies an explicit per-channel state file;
3. evaluates database, Swarm, placement, and Redis safety conditions;
4. quarantines only the configured channel and closes the video window when a
   critical condition is found;
5. never resumes a channel, opens the schedule, creates content, or publishes;
6. preserves private/unlisted and human-review constraints;
7. keeps all VP work off 126.

## Non-Goals

- No third canary upload in this increment.
- No public publication or unattended promotion to public.
- No automatic channel activation or resume.
- No automatic retry of a failed or uncertain YouTube upload.
- No replacement of the 150 deploy controller.
- No use of 126 as a worker, publisher, or failover node.

## Approaches Considered

### Repository shell plus Python guard service (selected)

A small host shell script checks Swarm and Redis, then invokes a Python guard
module from the already deployed worker image. The Python module uses the
existing SQLAlchemy models and quarantine service for database checks and
fail-closed mutation.

This keeps host orchestration simple while avoiding a second ad hoc SQL
implementation of task, job, node, and queue state transitions.

### Repository shell with direct SQL mutation (rejected)

This would be quick and independent of the API service, but it would duplicate
the quarantine state machine in shell SQL and could silently drift when model
states or schemas change.

### Read-only watcher plus operator response (rejected)

This avoids automated mutation but leaves a 30-minute cron report as the only
response to unsafe privacy, upload ambiguity, or placement. That does not meet
the fail-closed requirement for unattended soak operation.

## Architecture

### Explicit activation state

The installed watcher reads
`$DEPLOY_GITHUB_SYNC_ROOT/state/vp-soak-watch.env`. Absence of the file,
`VP_SOAK_WATCH_ENABLED` other than `true`, an invalid channel UUID, or an
invalid activation timestamp results in a disabled or configuration-error
check. It never guesses a channel from historical data.

The state file contains no credentials. It identifies one channel, its soak
start time, and conservative thresholds. Deployment credentials remain in the
existing protected deploy environment and are passed to the one-shot guard
container by variable name, not rendered into the installed script or logs.

Creating or enabling this state file is an explicit activation action after a
successful approved canary. A code push may update the watcher but cannot
activate it.

### Host watcher

`deploy/swarm/channelops-soak-watch.sh` runs every 30 minutes on 150 and:

- verifies all required VP services are at desired replica count;
- verifies running VP tasks are not placed on the forbidden 126 node;
- checks pending and lag for `ffmpeg_go`, `ffmpeg`, `youtube_publisher`, and
  event consumer groups;
- discovers the exact image deployed for the dedicated publisher;
- invokes the Python guard once in that image with the configured channel and
  activation timestamp;
- emits concise key/value evidence without secrets or content payloads.

Service, placement, and Redis failures are passed as fixed external condition
codes. Arbitrary command output is never interpolated into database fields.

### Database guard

The Python guard evaluates only rows belonging to the configured channel and
created or updated since soak activation. Critical conditions include:

- missing, disabled, or dry-run channel while soak is enabled;
- any enabled account or lane format configured for `public`;
- external-asset automatic publication enabled;
- any new publication with public or unsupported privacy;
- any new external-asset task that passed the required human-review boundary;
- failed, held, or ambiguous upload work;
- dead-lettered or failed ChannelOps queue work;
- stale reserved/submitted upload operations;
- cadence above the configured daily maximum;
- publication feedback missing beyond its grace period;
- external Swarm, placement, or Redis condition codes from the host watcher.

Healthy output is read-only. A critical result with auto-hold disabled exits
non-zero and records no mutation. A critical result with auto-hold enabled
uses the existing quarantine service to halt the channel, hold non-terminal
tasks, cancel their non-terminal jobs and nodes, and dead-letter runnable queue
items. The same transaction changes the VideoProcess runtime schedule to
`CLOSED`. Existing publications and feedback remain intact.

The guard never resumes work and never changes an existing YouTube video's
privacy or deletion state.

### Deploy integration

The sourced VP deploy extension owns an idempotent operations-asset install
step. After VP services pass health checks, it:

1. validates the watcher with `bash -n`;
2. atomically installs it as
   `$DEPLOY_GITHUB_SYNC_ROOT/bin/channelops-soak-watch.sh` with mode `0755`;
3. replaces only a marked `VIDEOPROCESS SOAK WATCH` crontab block;
4. removes the historical unmarked watcher line while preserving unrelated
   cron entries;
5. schedules the installed watcher every 30 minutes.

Dry-run, sync-only, build-only, and failed service deployments do not mutate
the installed watcher or crontab. PDS remains an independent repository and
service project; a PDS-only push does not need to rewrite VP operations
assets.

## Error Handling

- Missing activation state: log `disabled` and exit successfully.
- Invalid state or missing credentials/image: log a configuration failure,
  perform no database mutation, and exit non-zero.
- Database check failure: perform no speculative mutation and exit non-zero.
- Critical health result: quarantine once, close the schedule, and keep
  returning the already-halted state on later runs.
- Quarantine failure: exit non-zero; never report the channel as protected.
- Unknown Redis group or missing required service: treat as critical once the
  watcher is explicitly enabled.

## Security And Safety

- No credentials, tokens, database URLs, or media metadata are printed.
- The repository script has no credential fallback.
- Only `private` and `unlisted` are accepted by the guard; the publisher's
  independent `PUBLIC_PUBLISH_ENABLED=false` admission remains unchanged.
- External-platform assets require explicit human review.
- The guard's only mutations reduce activity: halt, hold, cancel,
  dead-letter, and schedule close.
- 126 is checked as a forbidden placement and is never targeted by deploy or
  guard commands.

## Testing

Automated coverage will include:

- database assessment for healthy, unsafe privacy, external-asset review,
  upload ambiguity, queue failure, cadence, and feedback timeout cases;
- idempotent quarantine with a custom guard reason and atomic schedule close;
- watcher disabled/configuration-error paths;
- Swarm, forbidden-node, Redis, and guard-container command contracts with
  fake host tools;
- deploy extension installation, managed cron replacement, dry-run behavior,
  and preservation of unrelated cron entries;
- shell syntax, focused backend tests, the full backend suite, Go tests, and
  existing deploy contract tests.

Production verification is read-only and disabled-state only until the next
canary receives explicit approval. It must show the pushed commit installed on
150, a single managed cron entry, no credentials in the script, no VP tasks on
126, no active soak channel, and no new upload/publication rows.

## Rollback

Revert the VP commit and let scoped deployment reinstall the prior operations
asset, or set `VP_SOAK_WATCH_ENABLED=false` in the state file. Disabling the
watcher does not resume a halted channel or reopen the schedule. Those remain
separate explicit operator actions.
