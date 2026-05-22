# ChannelOps Go Runner Deploy Closure Design

Date: 2026-05-22
Status: Approved design, pending written-spec review

## 1. Scope Decision

This spec closes the final pre-experimental-deployment gaps for ChannelOps
when the `channelops-go` profile is used.

The chosen scope is option B:

- make `channelops-runner-go` the complete long-running ChannelOps consumer for
  experimental deployment;
- keep FastAPI as the control/read API surface;
- make the FastAPI learning recompute endpoint perform real recompute work
  instead of returning a stub response.

Python ChannelAgent runner code may remain for compatibility, but the Go
deployment profile must not depend on it for alerts, cleanup, learning
recompute, health probes, or metrics.

## 2. Current Gaps

The current Go runner consumes the core production workflow queue kinds but
does not consume `send_alert` or `cleanup_expired`. The Python runner did
consume those kinds, so switching to the Go runner leaves legacy or API-created
alert and cleanup queue items unhandled.

The Go runner also has no HTTP server of its own. Docker and Kubernetes cannot
probe liveness or readiness for the runner, and Prometheus cannot scrape runner
metrics during soak.

Learning recompute exists in Go store code, but nothing schedules it. The
FastAPI endpoint `POST /api/v1/channel-agent/channels/{channel_id}/learning/recompute`
currently acknowledges recompute without writing `learning_states`.

These gaps make the Go deployment incomplete for unattended soak because
operators would miss quota, PDS, token, takedown, and material-supply alerts;
old queue/audit/feedback rows would grow without bound; and Phase D learning
state would not be populated.

## 3. Runtime Boundary

`channelops-runner-go` is the only required long-running ChannelOps worker for
the experimental Go profile. It owns:

- workflow queue consumption;
- alert queue consumption;
- retention cleanup;
- scheduled learning recompute;
- runner liveness, readiness, and metrics endpoints.

FastAPI owns:

- operator APIs;
- read APIs for ChannelOps status and learning state;
- manual learning recompute for local verification and backfill.

This preserves a clean runtime split: the runner advances durable work; FastAPI
controls and observes that work.

## 4. Queue Kinds And Handlers

Add Go constants for:

- `send_alert`
- `cleanup_expired`
- `recompute_learning`

`HandlerService.ClaimableKinds()` must include all three kinds. `Handle()`
dispatches them to narrow handlers.

### `send_alert`

The handler reads the queue payload and calls a Go `AlertService`.

Slack delivery uses `CHANNEL_AGENT_ALERT_SLACK_WEBHOOK_URL`. The payload format
matches the Python alpha behavior:

```text
[ChannelOps:<severity>] <type> <resource_id> - <message>
<detail_key>: <detail_value>
```

If no Slack webhook is configured, the handler records/logs the alert as
`recorded` and succeeds. This lets staging run without a webhook. If a webhook
is configured, non-2xx Slack responses return an error so the existing queue
retry and dead-letter behavior applies.

`CHANNEL_AGENT_ALERT_EMAIL_TO` remains an alpha configuration field without a
delivery transport. The Go service may include it in structured logs or result
metadata, but it must not claim email delivery happened until an email
transport exists.

### `cleanup_expired`

The handler deletes old rows using configurable retention windows:

- `CHANNEL_AGENT_RETENTION_QUEUE_DAYS`, default `30`
- `CHANNEL_AGENT_RETENTION_AUDIT_DAYS`, default `90`
- `CHANNEL_AGENT_RETENTION_FEEDBACK_DAYS`, default `365`

Retention behavior:

- delete `channel_ops_queue_items` only when status is terminal:
  `succeeded`, `dead_lettered`, or `cancelled`;
- delete `agent_tick_audits` older than the audit retention window;
- delete `decision_audit_entries` older than the audit retention window;
- delete `feedback_snapshots` older than the feedback retention window.

The Go constants should include `cancelled` if they do not already.

### `recompute_learning`

The handler reads:

- `payload.channel_id`, required;
- `payload.window_days`, optional, default `7`.

It calls the existing Go `Store.RecomputeLearningState(ctx, channelID,
windowDays)`. Failure returns an error and uses queue retry.

## 5. Scheduling

The existing `Scheduler.RunOnce()` continues to enqueue channel `agent_tick`
items.

Add a small `OpsScheduler` next to the existing scheduler work in the runner.
It keeps maintenance enqueue logic out of handler code.

The operations scheduler enqueues:

- one global `cleanup_expired` item per UTC day, idempotency key
  `cleanup_expired:<YYYY-MM-DD>`;
- one `recompute_learning` item per enabled, non-halted channel per UTC day for
  the 7-day window, idempotency key
  `recompute_learning:<channel_id>:7:<YYYY-MM-DD>`.

Daily recompute is intentional. It avoids high-frequency write amplification
while still creating learning state during soak.

The learning data flow becomes:

```text
collect_metrics
  -> feedback_snapshots
  -> daily recompute_learning
  -> learning_states
  -> FastAPI read APIs and decision-audit context
```

## 6. Health And Metrics Server

The runner starts a lightweight HTTP server when
`CHANNELOPS_METRICS_ADDR` is non-empty. Docker Compose sets this to `:9092` for
the `channelops-go` profile and exposes the port.

Endpoints:

- `/healthz`: returns `200 {"status":"ok"}` when the process is alive.
- `/readyz`: uses a short timeout to check Postgres ping and handler dependency
  configuration. It does not call YouTubeManager or PDS, so probes do not
  amplify external dependency load.
- `/metrics`: exposes Prometheus metrics using the existing Go Prometheus
  dependency.

Expose these operational metrics:

- `vp_channelops_queue_items_total{kind,result}`
- `vp_channelops_queue_item_duration_seconds{kind}`
- `vp_channelops_scheduler_runs_total{scheduler,result}`
- `vp_channelops_alerts_total{type,result}`
- `vp_channelops_retention_deleted_total{table}`
- `vp_channelops_learning_recompute_total{result}`

The runner still exits on unrecoverable startup configuration or database-open
errors. Runtime handler errors should be reflected in queue retry metrics and
queue item state.

## 7. Alert Sources

The Go runner must not only consume `send_alert`; it must also enqueue alerts
for Go-owned failure paths that previously depended on Python service logic.

### PDS Outage

Wrap Go PDS decision calls used by plan approval and publication promotion in a
helper that can enqueue PDS outage alerts.

When `PDS_ENABLED` is true and a PDS call fails or returns fail-policy metadata,
the helper enqueues one hourly deduped alert:

- `type`: `pds_outage`
- `resource_id`: `service:pds`
- `severity`: `critical`
- idempotency key:
  `send_alert:pds_outage:service:pds:<UTC-hour-bucket>`

The core safety behavior remains fail-closed. Alert enqueue should happen
before returning the PDS failure to the handler.

### Account Health

`account_health` should enqueue an alert when YouTube authentication is invalid
and the account is disabled.

If YouTubeManager exposes quota remaining, Go must enqueue a quota-pressure
alert when quota is below 20 percent. If only absolute quota units are
available, use `CHANNEL_AGENT_YOUTUBE_DAILY_QUOTA_UNITS`, default `10000`, to
derive the remaining fraction.

### Publication Reconcile

When `reconcile_publication` detects a severe YouTube status and records or
updates a takedown event, it should enqueue a takedown alert for the related
publication and channel.

### Material Supply

If Go candidate/tick evaluation already detects material supply exhaustion, it
must enqueue the existing material-supply alert shape. If that path does not
yet exist in Go, this deploy-closure pass must not invent a new scoring or
candidate system; it must document the absence and ensure any existing
`send_alert` rows are still consumable.

## 8. FastAPI Manual Learning Recompute

`POST /api/v1/channel-agent/channels/{channel_id}/learning/recompute` must
perform real recompute work.

The Python implementation should mirror the existing Go aggregation semantics:

- require the channel to exist;
- default `window_days` to `7`;
- aggregate from `production_tasks`, `publication_records`, and
  `feedback_snapshots`;
- group by `source` as `dimension_type = "source"`;
- include only snapshots where `metrics_completeness_score >= 0.4`;
- include only rows with non-null `reward_score`;
- delete existing `learning_states` for the same channel, dimension type, and
  window before inserting fresh rows;
- compute the same recommendation thresholds used by Go:
  `insufficient_data`, `observe`, `promote_more`, and `cool_down`.

This endpoint is for manual backfill and soak validation. Automatic recompute
is owned by the Go runner operations scheduler.

## 9. Tests

Add focused Go tests:

- `ClaimableKinds()` includes `send_alert`, `cleanup_expired`, and
  `recompute_learning`.
- `send_alert` succeeds without a webhook, posts Slack payloads with a webhook,
  and returns errors on non-2xx Slack responses.
- retention cleanup deletes only expired terminal queue rows and old audit,
  decision-audit, and feedback rows.
- operations scheduler enqueues one cleanup item per day and one recompute item
  per enabled channel per day.
- `recompute_learning` handler writes `learning_states` through the existing
  store method.
- `/healthz`, `/readyz`, and `/metrics` behave predictably.
- Go candidate evaluation does not enqueue an unsupported legacy queue kind.

Add backend pytest coverage:

- `POST /learning/recompute` writes visible `LearningState` rows instead of
  only returning `{"recomputed": true}`;
- repeated recompute is idempotent for the same channel and window.

Run the repository-required checks for changed areas:

```bash
go test ./cmd/... ./internal/...
cd backend && python3 -m pytest
cd backend && python3 -m ruff check . || true
cd backend && python3 -m mypy app || true
```

Frontend checks are not required unless implementation changes frontend files.

## 10. Acceptance Criteria

The implementation is acceptable when:

- the `channelops-go` profile can run without the Python ChannelAgent runner;
- queued `send_alert`, `cleanup_expired`, and `recompute_learning` items are
  consumed by Go;
- Slack alerts retry on configured webhook failures and do not block staging
  when no webhook is configured;
- retention cleanup prevents unbounded growth of queue, tick-audit,
  decision-audit, and feedback tables;
- daily learning recompute populates `learning_states` after metrics snapshots
  exist;
- manual FastAPI recompute writes the same kind of `LearningState` data;
- `/healthz`, `/readyz`, and `/metrics` are usable by Docker, Kubernetes, and
  Prometheus;
- default publication privacy remains private or unlisted;
- no existing API is removed.

## 11. Out Of Scope

This deploy-closure pass does not:

- add a real email transport;
- change AutoFlow graph construction;
- allow LLM output to define arbitrary workflow graphs;
- change publication privacy defaults;
- turn learning state into a ranking or rejection signal;
- add frontend UI beyond what is already available.
