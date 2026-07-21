# ChannelOps Durable Staged Metrics Schedules Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist and execute one idempotent `1h`, `6h`, `24h`, `72h`, and `7d` metrics schedule for every newly confirmed private or unlisted publication.

**Architecture:** `publication_metric_schedules` is the durable source of truth, while existing ChannelOps queue rows are its delayed execution mechanism. Promotion finalization creates both in one fenced transaction; collection updates the matching feedback snapshot and schedule atomically, while legacy queue rows remain compatible.

**Tech Stack:** Go 1.24, pgx v5, Python 3.12, SQLAlchemy 2, Alembic, PostgreSQL, pytest.

## Global Constraints

- Do not activate a channel, upload media, open the video schedule, or enable the soak watcher.
- Publication privacy remains `private` or `unlisted`; `public` remains rejected.
- The migration must not enqueue work or backfill the 100 historical publications.
- Persist only normalized metrics error codes, never provider payloads, URLs, credentials, titles, or prompts.
- Keep legacy `collect_metrics` rows without a schedule ID functional.
- Only a mature `24h` snapshot may mark a task measured or enter current learning aggregates.
- Keep all build, deployment, publisher, and watcher work off 126.
- Use test-driven development for every behavior change.

---

### Task 1: Metric Schedule Schema And ORM Contract

**Files:**
- Create: `backend/alembic/versions/028_channelops_metric_schedules.py`
- Modify: `backend/app/models/channel_agent.py`
- Modify: `backend/app/models/__init__.py`
- Create: `backend/tests/migrations/test_channelops_metric_schedules_postgres.py`
- Modify: `backend/tests/channel_agent/test_models_queue.py`

**Interfaces:**
- Produces: `PublicationMetricSchedule` ORM model.
- Produces table: `publication_metric_schedules` with a unique `(publication_id, snapshot_stage)` schedule fact.
- Valid stages: `1h`, `6h`, `24h`, `72h`, `7d`.
- Valid statuses: `pending`, `succeeded`, `expired`.

- [ ] **Step 1: Add failing ORM contract tests**

Add assertions for model defaults and table constraints:

```python
schedule = PublicationMetricSchedule(
    publication_id=uuid.uuid4(),
    snapshot_stage="24h",
    effective_start_at=NOW,
    due_at=NOW + timedelta(hours=24),
    grace_until=NOW + timedelta(hours=30),
)
assert schedule.status is None or schedule.status == "pending"
assert schedule.attempt_count is None or schedule.attempt_count == 0
assert {column.name for column in PublicationMetricSchedule.__table__.columns} >= {
    "publication_id", "snapshot_stage", "effective_start_at", "due_at",
    "grace_until", "status", "attempt_count", "last_attempt_at",
    "completed_at", "available_fields_json", "last_error_code",
}
```

- [ ] **Step 2: Run the focused model test and verify RED**

Run:

```bash
cd backend
.venv/bin/python -m pytest tests/channel_agent/test_models_queue.py -k metric_schedule -q
```

Expected: import or attribute failure because `PublicationMetricSchedule` does not exist.

- [ ] **Step 3: Add the ORM model**

Use this contract:

```python
class PublicationMetricSchedule(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "publication_metric_schedules"
    __table_args__ = (
        UniqueConstraint("publication_id", "snapshot_stage", name="uq_metric_schedule_publication_stage"),
        CheckConstraint("snapshot_stage IN ('1h','6h','24h','72h','7d')", name="ck_metric_schedule_stage"),
        CheckConstraint("status IN ('pending','succeeded','expired')", name="ck_metric_schedule_status"),
        CheckConstraint("attempt_count >= 0", name="ck_metric_schedule_attempt_count"),
        CheckConstraint("due_at >= effective_start_at", name="ck_metric_schedule_due_order"),
        CheckConstraint("grace_until >= due_at", name="ck_metric_schedule_grace_order"),
        Index("ix_metric_schedules_status_due", "status", "due_at"),
    )
```

Map the publication FK with `ondelete="CASCADE"`, use timezone-aware datetime
columns for schedule times, JSON for `available_fields_json`, and safe Python
defaults plus matching migration server defaults for `status`, attempts, and
available fields.

- [ ] **Step 4: Add a failing PostgreSQL migration test**

The test must run upgrade to `028_channelops_metric_schedules`, assert all
columns/checks/indexes, reject an invalid stage/status and a duplicate
publication-stage pair, and assert:

```sql
SELECT count(*) FROM publication_metric_schedules;
SELECT count(*) FROM channel_ops_queue_items
WHERE idempotency_key LIKE 'collect_metrics:%:stage:%';
```

Both counts must remain zero for historical rows seeded before the upgrade.
Then run downgrade to `027_publication_promotion_operations` and upgrade again.

- [ ] **Step 5: Implement migration 028 and pass focused tests**

Run:

```bash
cd backend
.venv/bin/python -m pytest tests/channel_agent/test_models_queue.py -k metric_schedule -q
.venv/bin/python -m pytest tests/migrations/test_channelops_metric_schedules_postgres.py -q
.venv/bin/python -m py_compile \
  alembic/versions/028_channelops_metric_schedules.py app/models/channel_agent.py
```

Expected: all selected tests pass; environments without the configured
PostgreSQL migration fixture may skip only through the repository's existing
fixture policy.

- [ ] **Step 6: Commit schema work**

```bash
git add backend/alembic/versions/028_channelops_metric_schedules.py \
  backend/app/models/channel_agent.py backend/app/models/__init__.py \
  backend/tests/migrations/test_channelops_metric_schedules_postgres.py \
  backend/tests/channel_agent/test_models_queue.py
git commit -m "feat: persist publication metric schedules"
```

### Task 2: Deterministic Five-Stage Creation

**Files:**
- Create: `internal/channelops/metric_schedules.go`
- Create: `internal/channelops/metric_schedules_test.go`
- Modify: `internal/channelops/types.go`
- Modify: `internal/channelops/store_publications.go`
- Modify: `internal/channelops/integration_test.go`

**Interfaces:**
- Produces: `MetricStageSpec{Stage string, DueAfter time.Duration, GraceAfter time.Duration}`.
- Produces: `MetricScheduleRow` matching migration 028.
- Produces: `MetricStageSpecs() []MetricStageSpec`.
- Produces: `(*Store).EnsurePublicationMetricSchedules(ctx, publicationID, channelID, parentQueueItemID string, effectiveStart time.Time) error`.

- [ ] **Step 1: Write failing stage-policy tests**

Require this exact table:

```go
want := []MetricStageSpec{
    {Stage: "1h", DueAfter: time.Hour, GraceAfter: 3 * time.Hour},
    {Stage: "6h", DueAfter: 6 * time.Hour, GraceAfter: 12 * time.Hour},
    {Stage: "24h", DueAfter: 24 * time.Hour, GraceAfter: 30 * time.Hour},
    {Stage: "72h", DueAfter: 72 * time.Hour, GraceAfter: 84 * time.Hour},
    {Stage: "7d", DueAfter: 168 * time.Hour, GraceAfter: 192 * time.Hour},
}
```

Also assert the returned slice cannot mutate package state.

- [ ] **Step 2: Run the unit test and verify RED**

Run:

```bash
go test ./internal/channelops -run 'TestMetricStageSpecs' -count=1
```

Expected: compile failure because the stage types/functions do not exist.

- [ ] **Step 3: Implement immutable stage definitions**

Return a new slice on each call. Keep timing constants in
`metric_schedules.go`; do not make them environment-dependent in this phase.

- [ ] **Step 4: Add failing promotion integration assertions**

Extend the existing successful promotion fixture to assert exactly five
schedule rows and five queue rows. Each queue payload must contain:

```json
{
  "publication_id": "<publication UUID>",
  "metric_schedule_id": "<schedule UUID>",
  "snapshot_stage": "24h",
  "metrics_poll_count": 0
}
```

Assert `run_after == due_at`, the timing table is exact, all rows share the
confirmed `scheduled_at` as `effective_start_at`, and replaying finalization
does not change either count.

- [ ] **Step 5: Replace the single legacy automatic enqueue**

Implement `EnsurePublicationMetricSchedules` with `INSERT ... ON CONFLICT DO
NOTHING`, select the canonical row, and enqueue only pending schedules with:

```go
IdempotencyKey: fmt.Sprintf(
    "collect_metrics:%s:stage:%s:attempt:0", publicationID, spec.Stage,
)
```

Call it from `PromotePublication` after the task transition and before
reconciliation enqueue. Remove only the automatic `poll:0` enqueue; the
legacy handler path remains.

- [ ] **Step 6: Run stage and promotion tests**

Run:

```bash
go test ./internal/channelops -run \
  'TestMetricStageSpecs|TestChannelOpsIntegration' -count=1
```

Expected: exact five-stage assertions pass and existing promotion fencing is
unchanged.

- [ ] **Step 7: Commit schedule creation**

```bash
git add internal/channelops/metric_schedules.go \
  internal/channelops/metric_schedules_test.go internal/channelops/types.go \
  internal/channelops/store_publications.go internal/channelops/integration_test.go
git commit -m "feat: schedule five publication metric stages"
```

### Task 3: Schedule-Backed Collection State Machine

**Files:**
- Modify: `internal/channelops/metric_schedules.go`
- Modify: `internal/channelops/metric_schedules_test.go`
- Modify: `internal/channelops/handlers.go`
- Modify: `internal/channelops/handlers_test.go`
- Modify: `internal/channelops/store_publications.go`
- Modify: `internal/channelops/integration_test.go`

**Interfaces:**
- Produces: `(*Store).LockMetricScheduleForQueue(ctx, item) (MetricScheduleRow, error)`.
- Produces: `(*Store).RequeueOrExpireMetricSchedule(ctx, publication, schedule, item, maxPolls, retryDelay) error`.
- Produces: `(*Store).CompleteMetricSchedule(ctx, publication, schedule, metrics, score, fields, reward, rewardComponents) error`.
- Preserves: `RequeueOrHoldMetrics` for queue rows without `metric_schedule_id`.

- [ ] **Step 1: Write failing validation and retry tests**

Cover schedule/publication mismatch, stage mismatch, invalid UUID, a retry
before grace, expiry at grace, and expiry at the configured attempt cap. A
retry must increment `attempt_count`, set `last_attempt_at`, retain
`status='pending'`, and enqueue:

```go
fmt.Sprintf(
    "collect_metrics:%s:stage:%s:attempt:%d",
    publication.ID, schedule.SnapshotStage, nextAttempt,
)
```

The next `run_after` is `min(now+retryDelay, grace_until)` and schedule errors
contain only fixed codes.

- [ ] **Step 2: Run the selected tests and verify RED**

Run:

```bash
go test ./internal/channelops -run \
  'TestMetricScheduleQueueValidation|TestMetricScheduleRetry|TestMetricScheduleExpiry' \
  -count=1
```

Expected: compile failure for the new store methods.

- [ ] **Step 3: Implement lock, retry, and expiry methods**

`LockMetricScheduleForQueue` must require and parse both
`metric_schedule_id` and `snapshot_stage`, select `FOR UPDATE`, and compare
the row to `publication_id`. Return `ErrQueueAuthorityInvalid` for any
mismatch so the queue is rejected without an external call.

`RequeueOrExpireMetricSchedule` updates the publication's
`last_metrics_polled_at`, persists the schedule attempt, and either enqueues
the next stage attempt or sets:

```text
status=expired
completed_at=now
last_error_code=metrics_unavailable
```

It does not fabricate a feedback snapshot and does not hold the task merely
because a non-primary stage expires.

- [ ] **Step 4: Add failing successful-completion tests**

Assert successful collection atomically:

- upserts exactly one snapshot for the schedule stage;
- sets schedule `succeeded`, `completed_at`, attempts, and sorted recognized fields;
- clears `last_error_code`;
- marks a task `measured` only for `24h`;
- preserves task state for `1h`, `6h`, `72h`, and `7d`;
- replays without duplicate rows.

- [ ] **Step 5: Wire the handler and completion method**

Branch only when `metric_schedule_id` is present:

```go
schedule, err := h.Store.LockMetricScheduleForQueue(ctx, item)
if err != nil { return err }
// Fetch metrics with the existing read-only YouTube client, then complete,
// retry, or expire this exact schedule inside the execution fence.
```

Change `UpsertFeedbackSnapshot` so its existing task transition occurs only
when `stage == "24h"`. `CompleteMetricSchedule` calls it and updates the
schedule in the same fenced transaction. Legacy payloads continue through
the old requeue path.

- [ ] **Step 6: Run focused and full ChannelOps tests**

Run:

```bash
go test ./internal/channelops -run 'TestMetricSchedule|TestHandleCollectMetrics' -count=1
go test ./internal/channelops -count=1
```

Expected: all ChannelOps tests pass, including legacy metrics cases.

- [ ] **Step 7: Commit collection state machine**

```bash
git add internal/channelops/metric_schedules.go \
  internal/channelops/metric_schedules_test.go internal/channelops/handlers.go \
  internal/channelops/handlers_test.go internal/channelops/store_publications.go \
  internal/channelops/integration_test.go
git commit -m "feat: execute durable metric stage schedules"
```

### Task 4: Canary Immediate Stage And Mature Learning Isolation

**Files:**
- Modify: `internal/channelops/metrics.go`
- Modify: `internal/channelops/metrics_test.go`
- Modify: `internal/channelops/learning.go`
- Modify: `internal/channelops/learning_test.go`
- Modify: `scripts/run_vp_unlisted_canary.py`
- Modify: `backend/tests/services/test_unlisted_canary_runner.py`
- Modify: `tests/test_vp_unlisted_canary_scripts.sh`

**Interfaces:**
- Produces: `SnapshotStageFromPayload()` accepts explicit `immediate` while preserving the default `24h` for absent/unknown legacy payloads.
- Produces: learning aggregation includes only `feedback_snapshots.snapshot_stage = '24h'`.
- Produces: canary metrics queue payload `snapshot_stage="immediate"`.

- [ ] **Step 1: Add failing stage and learning tests**

Require:

```go
if got := SnapshotStageFromPayload(map[string]any{"snapshot_stage": "immediate"}); got != "immediate" {
    t.Fatalf("stage = %q, want immediate", got)
}
```

Seed one publication with immediate/1h/24h/72h snapshots carrying different
rewards and assert learning uses exactly the 24h reward once.

- [ ] **Step 2: Run Go tests and verify RED**

Run:

```bash
go test ./internal/channelops -run \
  'TestSnapshotStageFromPayload|TestRecomputeLearningUsesMature24hSnapshot' -count=1
```

Expected: immediate falls back to 24h and/or learning counts multiple stages.

- [ ] **Step 3: Implement stage and learning isolation**

Add `immediate` only to the feedback stage parser. Add this predicate to the
learning query:

```sql
AND f.snapshot_stage = '24h'
```

Do not add `immediate` to migration 028's schedule stage constraint.

- [ ] **Step 4: Add failing canary payload tests**

Assert `enqueue_metrics_probe()` writes:

```python
payload={
    "publication_id": str(publication.id),
    "snapshot_stage": "immediate",
}
```

and evidence continues to classify it as `age_appropriate: false` while
listing the five durable queued stages separately.

- [ ] **Step 5: Implement canary evidence updates**

Include `snapshot_stage` and `metric_schedule_id` in the safe pending metrics
summary. Require the durable list to contain the five expected stages after a
successful promotion, without exposing a title, prompt, URL, or credential.

- [ ] **Step 6: Run canary and learning tests**

Run:

```bash
go test ./internal/channelops -run 'TestSnapshotStage|TestRecomputeLearning' -count=1
cd backend
.venv/bin/python -m pytest tests/services/test_unlisted_canary_runner.py -q
cd ..
bash tests/test_vp_unlisted_canary_scripts.sh
```

Expected: all selected tests pass.

- [ ] **Step 7: Commit semantic isolation**

```bash
git add internal/channelops/metrics.go internal/channelops/metrics_test.go \
  internal/channelops/learning.go internal/channelops/learning_test.go \
  scripts/run_vp_unlisted_canary.py \
  backend/tests/services/test_unlisted_canary_runner.py \
  tests/test_vp_unlisted_canary_scripts.sh
git commit -m "fix: isolate mature metrics from canary probes"
```

### Task 5: Verification, Review, Push, And Disabled Deployment

**Files:**
- Modify only if verification or review exposes a scoped defect.
- Generate: `.runtime/youtube-canary/*.json` read-only preflight evidence.

**Interfaces:**
- Produces: tested commit on `main`, scoped deployment on 127/150, and a new read-only preflight evidence file.
- Does not consume the separate live-canary approval.

- [ ] **Step 1: Run changed-scope verification**

```bash
cd backend
.venv/bin/python -m pytest \
  tests/channel_agent/test_models_queue.py \
  tests/migrations/test_channelops_metric_schedules_postgres.py \
  tests/services/test_unlisted_canary_runner.py -q
.venv/bin/python -m ruff check \
  app/models/channel_agent.py \
  alembic/versions/028_channelops_metric_schedules.py \
  tests/migrations/test_channelops_metric_schedules_postgres.py \
  tests/services/test_unlisted_canary_runner.py \
  ../scripts/run_vp_unlisted_canary.py
cd ..
go test ./internal/channelops -count=1
bash tests/test_vp_unlisted_canary_scripts.sh
python3 -m py_compile scripts/run_vp_unlisted_canary.py
git diff --check
```

Expected: every command exits zero.

- [ ] **Step 2: Run repository checks**

```bash
cd backend
.venv/bin/python -m pytest
.venv/bin/python -m ruff check . || true
.venv/bin/python -m mypy app || true
cd ..
go test ./...
bash tests/test_vp_deploy_sync_extension.sh
bash tests/test_channelops_soak_watch.sh
```

Record the existing full Ruff/mypy baseline separately; changed files must be
clean and all tests/shell contracts must pass.

- [ ] **Step 3: Review and fix verified findings**

Use `superpowers:requesting-code-review`. Re-run the smallest failing test
before and after each correction, then repeat Step 1.

- [ ] **Step 4: Commit, push, and scoped deploy**

Commit only scoped review fixes, push `main`, then run on 150:

```bash
/home/taiwei/deploy-github-sync/bin/deploy-github-sync.sh \
  --apply --project vp-app --project vp-feature-aggregator --project vp-pds
```

Require `vp-app` and `vp-feature-aggregator` at the new commit, independent
PDS at its own source commit, every VP service at desired replicas, normal VP
services on 127, GPU/publisher on 150, and zero VP tasks on 126.

- [ ] **Step 5: Prove disabled read-only production state**

Run the tunnel-backed preflight and require:

- source/deployed commit parity;
- mode `preflight_only`, evidence mode `0600`, and no secrets;
- schedule `CLOSED`, empty DB/Redis backlog, authenticated manager;
- watcher `status=disabled reason=state_missing`;
- `publication_metric_schedules=0` before a new publication exists;
- unchanged uploads/publications/public/promotion counts;
- no leftover SSH forwarding process.

- [ ] **Step 6: Return to the separately approved live gate**

Do not run `--confirm-live-unlisted` unless the operator has supplied the
exact per-attempt phrase already documented in the topology runbook. The live
attempt must then prove one immediate age-ineligible snapshot plus five
durable age-appropriate schedules.
