# ChannelOps Follow-Up Hardening Design

Date: 2026-05-19
Status: Approved for implementation
Parent branch: `codex/channel-ops-remaining-sprints`
Parent specs:

- `docs/superpowers/specs/2026-05-19-channel-ops-live-cutover-design.md`
- `docs/superpowers/specs/2026-05-19-channel-ops-remaining-sprints-batch-design.md`

## 1. Decision

Before merging the ChannelOps remaining-sprints branch, apply a focused
hardening pass that fixes three review findings:

1. Wire the PDS health helper into real PDS decision call paths so PDS outage
   alerts are actually enqueued.
2. Run the internal scheduler independently from queue consumption and make
   `tick_interval_minutes` control the scheduler bucket.
3. Add a final material repetition guard inside `handle_publish_task` before a
   new `PublicationRecord` is created.

These fixes are merge blockers for the current branch because each issue
weakens an explicit safety or self-driving guarantee from the approved specs.
They are narrow corrections, not a new sprint.

## 2. PDS Outage Alert

### Problem

`backend/app/channel_agent/pds_health.py` defines
`should_enqueue_pds_outage_alert`, but no production caller invokes it. The
service currently calls `self.pds_client.decide(...)` directly from candidate
acceptance, plan approval, and publication promotion. If PDS is unreachable,
publish decisions fail closed, but operators do not receive the required
`send_alert` item for `pds_outage`.

### Design

Add PDS health state to `ChannelAgentService`:

- `_pds_health_monitor_enabled: bool`
- `_pds_last_success_at: datetime | None`
- `_pds_last_alert_bucket: str | None`

Add one private async helper, `_decide_pds(db, request)`, and replace direct
`self.pds_client.decide(...)` calls in the service with this helper.

The helper:

1. Calls the injected PDS client.
2. Treats a decision as healthy only when it is not a fail-policy decision.
   Fail-policy decisions are identified from `decision.metadata["warning"]`
   or `decision.metadata["fail_policy"]`, covering `pds_disabled`,
   `pds_unavailable`, and `pds_parse_failed`.
3. Updates `_pds_last_success_at` on healthy decisions.
4. Returns immediately when `_pds_health_monitor_enabled` is false. This keeps
   tests and dry NoopPDSClient paths from producing noisy outage alerts.
5. Calls `should_enqueue_pds_outage_alert` after every monitored PDS decision.
6. When the helper returns `should_alert=True`, enqueues one hourly
   `send_alert` item with:
   - `type`: `pds_outage`
   - `resource_id`: `service:pds`
   - `severity`: `critical`
   - details containing the PDS action type and fail-policy metadata
7. Updates `_pds_last_alert_bucket` only after enqueue succeeds.

This keeps PDS health tracking in the service where the PDS calls happen. The
runner still owns long-lived service process state by constructing one
`ChannelAgentService` instance for the worker loop and passing
`pds_health_monitor_enabled=settings.pds_enabled`.

## 3. Scheduler Independence And Interval Buckets

### Problem

`ChannelAgentRunner.run_once()` currently invokes the scheduler only when
`queue.claim_next()` returns no item. Under sustained queue load, the scheduler
can be starved and autonomous ticks stop being created.

The current scheduler also uses `utc_hour_bucket` for every channel. That makes
`tick_interval_minutes` weaker than its name: values such as 15, 30, 60, and
240 all collapse to at most one tick per hour.

### Design

Keep `ChannelAgentRunner.run_once()` deterministic for tests: it may still run
the scheduler when the queue is empty. Add a separate scheduler loop to
`run_forever()` so normal runtime starts two cooperative loops:

- queue consumer loop: claims and handles queue items;
- scheduler loop: sleeps for a configurable interval and calls
  `ChannelOpsScheduler.run_once()`.

Add a setting:

- `channel_agent_scheduler_poll_seconds: float = 60.0`

Make scheduler buckets interval-aware:

1. Normalize `tick_interval_minutes` with a floor of 15 minutes.
2. Compute an interval bucket from UTC time:
   - intervals below 60 use minute slots within the hour;
   - intervals at or above 60 use hour groups within the day;
   - non-divisor values are floored into deterministic minute slots based on
     elapsed minutes since UTC midnight.
3. Use that bucket in both the queue idempotency key and
   `InternalSchedulerRun.bucket`.

Examples:

- 15 minutes at 10:37 UTC -> `2026-05-19-10-30`
- 30 minutes at 10:37 UTC -> `2026-05-19-10-30`
- 60 minutes at 10:37 UTC -> `2026-05-19-10`
- 240 minutes at 10:37 UTC -> `2026-05-19-08`

Manual `POST /enqueue-tick` remains independent and continues to use its
existing idempotency behavior.

## 4. Publish-Time Repetition Guard

### Problem

The current material repetition guard runs during candidate selection. That is
necessary but not sufficient: two close ticks can select the same material
before the first publication writes its `MaterialUsageLedger` row. When the
second task reaches publish, the ledger may finally contain the first use, but
there is no final guard before creating the second publication.

### Design

Before creating a new `PublicationRecord` in `handle_publish_task`, extract the
selected material references from the same sources used by ledger writing:

- AutoFlow plan payload;
- AutoFlow run payload;
- upload metadata from the queue item.

Run `recent_usage_flags(...)` for the task's channel, lane, and account. If it
is blocked:

- lane-generated tasks are held before a publication is created;
- `task.blocked_by_guard` is set to `cross_account_rejected` or
  `repetition_rejected`;
- `task.failure_reason` explains that the final publish-time material guard
  rejected the task;
- `task.rationale_json["material_usage_guard"]` records guard hits for audit.

Manual seeds keep the already-approved override behavior. If a manual seed
would be blocked at publish time, the service records the guard result in
`rationale_json` and continues. This preserves the distinction between
operator-supplied manual work and autonomous lane work.

Existing publication idempotency is preserved. If a `PublicationRecord`
already exists for the task, the final guard is not re-applied to retroactively
hold the already-created publication.

## 5. Tests

Add or update focused tests:

- PDS health:
  - unavailable PDS decisions enqueue one `send_alert` with type
    `pds_outage`;
  - repeated unavailable decisions in the same hour do not enqueue duplicates;
  - a later healthy decision updates `_pds_last_success_at`;
  - disabled PDS does not produce noisy alerts in tests that inject
    `NoopPDSClient` unless the service is explicitly configured to monitor it.
- Scheduler:
  - `run_forever()` starts scheduler work independently from queue consumer
    work;
  - interval buckets differ for 15, 30, 60, and 240 minute settings;
  - scheduler still enqueues at most one item per channel per interval bucket.
- Publish guard:
  - a lane-generated task with a recently used material is held before
    publication creation;
  - a manual seed with the same hit continues and records the guard result;
  - existing publication idempotency is not broken by a repeated publish item.

Run the existing required checks after implementation:

```bash
cd backend
python3 -m pytest
python3 -m ruff check . || true
python3 -m mypy app || true
cd ../frontend
npm install
npm run build
npm run lint || true
```

Frontend changes are not expected for this hardening pass.

## 6. Non-Goals

This pass does not add a new alert transport, change PDS rule semantics,
introduce public auto-publication, or replace greedy candidate selection. It
also does not move scheduling to an external cron. The goal is to make the
already-approved runtime design match its safety contract before merge.
