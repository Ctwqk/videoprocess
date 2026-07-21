# ChannelOps Durable Staged Metrics Schedules Design

**Date:** 2026-07-21

## Goal

Give every newly confirmed private or unlisted publication a durable,
auditable metrics plan for `1h`, `6h`, `24h`, `72h`, and `7d`. Collection must
survive runner restarts, remain idempotent, and expose enough database state
for the soak guard to distinguish pending, successful, and expired stages.

This change does not enable a channel, upload media, change publication
privacy, or activate feedback-driven selection.

## Chosen Approach

Use a first-class `publication_metric_schedules` table as the source of truth
and mirror each due stage into the existing ChannelOps queue. Promotion
finalization creates the five schedule rows and queue rows in the same fenced
database transaction.

Alternatives were rejected for the following reasons:

- Queue rows alone disappear from durable coverage calculations after cleanup
  and cannot distinguish a missing schedule from a completed one.
- A periodic publication scanner can self-heal, but repeated scans make the
  due-time authority and retry history less explicit. A repair scanner can be
  added later without changing this schema.

## Data Model

Migration `028_channelops_metric_schedules` adds
`publication_metric_schedules` with:

- `id`: UUID primary key;
- `publication_id`: cascading FK to `publication_records`;
- `snapshot_stage`: one of `1h`, `6h`, `24h`, `72h`, or `7d`;
- `effective_start_at`: the confirmed promotion schedule time;
- `due_at` and `grace_until`: immutable stage timing facts;
- `status`: `pending`, `succeeded`, or `expired`;
- `attempt_count`, `last_attempt_at`, and `completed_at`;
- `available_fields_json`: recognized fields from the successful snapshot;
- `last_error_code`: a normalized code, never a provider response or URL;
- normal `created_at` and `updated_at` timestamps.

The table has a unique constraint on `(publication_id, snapshot_stage)`,
checks for stage/status/attempt validity, and an index on `(status, due_at)`.
The migration creates no queue work and does not backfill the 100 historical
publication rows. Historical repair remains an explicit operator action.

The Python ORM gains a matching `PublicationMetricSchedule` model so services,
tests, and future operator APIs share the same contract.

## Stage Policy

The first implementation uses deterministic, version-one timing:

| Stage | Due after effective start | Grace until |
| --- | ---: | ---: |
| `1h` | 1 hour | 3 hours |
| `6h` | 6 hours | 12 hours |
| `24h` | 24 hours | 30 hours |
| `72h` | 72 hours | 84 hours |
| `7d` | 168 hours | 192 hours |

Retry spacing continues to use `CHANNELOPS_METRICS_POLL_DELAY_MINUTES`, and
`CHANNELOPS_METRICS_POLL_MAX_ATTEMPTS` remains an upper bound. The persisted
grace deadline is the stricter bound.

## Promotion Data Flow

After YouTubeManager confirms the requested private/unlisted promotion, the
existing fenced finalization transaction:

1. updates the publication and production task;
2. inserts or reuses all five metric schedule rows;
3. enqueues one delayed `collect_metrics` row per pending stage;
4. enqueues the existing publication reconciliation row;
5. marks the promotion operation finalized.

Each metrics payload contains `publication_id`, `metric_schedule_id`,
`snapshot_stage`, and `metrics_poll_count`. Its idempotency key is stage- and
attempt-specific. Replaying promotion finalization cannot create duplicate
schedules or queue rows.

## Collection State Machine

For schedule-backed queue work, the handler locks and validates the schedule
against the publication and payload before accepting any metrics.

- Recognized metrics upsert the corresponding `feedback_snapshots` row and
  mark the schedule `succeeded` in the same transaction.
- Missing metrics increment the persisted attempt count. Before the grace
  deadline and attempt cap, the handler enqueues the next attempt and leaves
  the schedule `pending`.
- At the grace deadline or attempt cap, the schedule becomes `expired` with a
  normalized `metrics_unavailable` code. The handler does not fabricate a
  zero-valued success.
- Only a successful `24h` snapshot transitions a production task to
  `measured`. Other successful stages preserve the task state.

Legacy `collect_metrics` rows without `metric_schedule_id` retain the existing
poll/retry path. This keeps old APIs and already queued rows compatible.

## Immediate Canary Feedback

The guarded live canary may collect an immediate, explicitly age-ineligible
snapshot. It uses `snapshot_stage="immediate"`, which is allowed only in
`feedback_snapshots`, not in the five-stage schedule table. The learning query
uses only mature `24h` snapshots, preventing the immediate probe or the other
four stages from multiplying one publication's learning weight.

The canary evidence continues to distinguish immediate feedback from the five
durable age-appropriate schedules.

## Failure And Safety Behavior

- Invalid schedule IDs, publication mismatches, stage mismatches, unsafe
  privacy, or impossible timing fail the queue item without external writes.
- Schedule errors persist only fixed codes. Provider payloads, credentials,
  tokens, URLs, titles, and prompts are never copied into schedule rows.
- Public publication remains rejected by existing planner, worker, promotion,
  and soak-guard controls.
- No migration, deployment, or preflight creates an upload or activates the
  soak watcher.
- Host 126 remains outside all build, runtime, publisher, and watcher paths.

## Verification

Tests cover:

- ORM and migration constraints, upgrade/downgrade/upgrade, and no historical
  queue backfill;
- exact five-stage timing and idempotent promotion replay;
- success, retry, grace expiry, payload mismatch, and 24h-only task transition;
- legacy queue compatibility and immediate-canary stage isolation;
- learning aggregation using only `24h` snapshots;
- complete Go, backend, migration, and deployment contract suites.

Production verification remains read-only until the separately approved live
canary. After deployment, a preflight must show source/deployed commit parity,
schedule `CLOSED`, no runnable backlog, no public rows, and no new metric
schedule rows before a new publication exists.
