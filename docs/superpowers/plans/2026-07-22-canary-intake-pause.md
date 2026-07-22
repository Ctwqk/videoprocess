# Canary Intake Pause Implementation Plan

> **For agentic workers:** Follow the test-driven development and verification-before-completion skills for every behavior change.

**Goal:** Replace the deadlocking halt-after-selection canary protocol with an
atomic intake pause that permits exactly one approved unlisted task to finish
and continue collecting feedback.

**Architecture:** Add a durable channel intake-pause state. Enforce it at both
scheduler/claim and transactional handler fences for intake kinds only. Let the
guarded Go tick create exactly one task and pause intake atomically. Keep halt as
the full kill switch and add fresh-connection cleanup recovery to the Python
runner.

**Tech stack:** PostgreSQL/Alembic, SQLAlchemy async, FastAPI/Pydantic, Go 1.24
with pgx, pytest, shell deployment contracts, Docker Swarm.

---

### Task 1: Add the durable intake-pause state

**Files:**
- Create: `backend/alembic/versions/030_channelops_intake_pause.py`
- Modify: `backend/app/models/channel_agent.py`
- Modify: `backend/app/schemas/channel_agent.py`
- Modify: `backend/app/api/channel_agent.py`
- Modify: `backend/app/channel_agent/scheduler.py`
- Modify: `backend/tests/channel_agent/test_scheduler.py`
- Modify: `backend/tests/channel_agent/test_api.py`
- Create: `backend/tests/migrations/test_channelops_intake_pause_postgres.py`

- [ ] Write failing scheduler/API/model tests proving an intake-paused channel
  is visible but cannot enqueue a manual tick or enter discovery ingestion.
- [ ] Write a PostgreSQL migration test proving nullable columns, upgrade data
  preservation, and downgrade removal.
- [ ] Run the focused tests and observe the expected missing-column failures.
- [ ] Add migration 030, ORM/schema fields, response serialization, scheduler
  filter, and API guards.
- [ ] Re-run focused tests to green.

### Task 2: Enforce intake pause in Go scheduling and queue authority

**Files:**
- Modify: `internal/channelops/types.go`
- Modify: `internal/channelops/scheduler.go`
- Modify: `internal/channelops/scheduler_test.go`
- Modify: `internal/channelops/queue.go`
- Modify: `internal/channelops/queue_test.go`
- Modify: `internal/channelops/execution_fence.go`
- Modify: `internal/channelops/integration_test.go`

- [ ] Add failing tests proving intake-paused channels are not scheduled and
  cannot claim or execute `agent_tick`/`ingest_discovery`.
- [ ] Keep tests proving `plan_task`, promotion, reconciliation, and metrics
  remain claimable while intake is paused.
- [ ] Keep existing disabled/halted/global-item tests unchanged and green.
- [ ] Implement the scheduler query, claim predicate, and transactional fence
  checks with one shared intake-kind definition.
- [ ] Run `go test ./internal/channelops` to green.

### Task 3: Make one-task selection and intake pause atomic

**Files:**
- Modify: `internal/channelops/handlers.go`
- Modify: `internal/channelops/handlers_test.go`
- Modify: `internal/channelops/store_tasks.go`
- Modify: `internal/channelops/integration_test.go`

- [ ] Add parser tests for the guarded tick payload: valid boolean/run UUID,
  absent flag, malformed flag, missing/malformed run ID, and missing delay.
- [ ] Add a failing integration test proving one task, its delayed plan row,
  and intake pause commit together.
- [ ] Add rollback tests for zero and multiple selected tasks.
- [ ] Implement a small tick options value and transactional pause update.
- [ ] Re-run focused and full ChannelOps Go tests.

### Task 4: Update the live canary protocol and cleanup recovery

**Files:**
- Modify: `scripts/run_vp_unlisted_canary.py`
- Modify: `backend/tests/services/test_unlisted_canary_runner.py`
- Modify: `tests/test_vp_unlisted_canary_scripts.sh`

- [ ] Add failing runner tests proving the graph requests atomic intake pause,
  preapproval rejects a missing/wrong pause, and preapproval never halts.
- [ ] Add failing open-gate/evidence tests for paused-but-not-halted state.
- [ ] Add a failing cleanup test in which the active session fails and a fresh
  connection completes idempotent cleanup.
- [ ] Implement the new runner contract and sanitized cleanup fallback.
- [ ] Keep public/external-source/reapproval/schedule-close tests green.

### Task 5: Update operator documentation

**Files:**
- Modify: `docs/superpowers/specs/2026-07-12-unlisted-canary-feedback-loop-activation-design.md`
- Modify: `docs/superpowers/plans/2026-07-12-unlisted-canary-feedback-loop-activation.md`
- Modify: `docs/superpowers/plans/2026-07-19-channelops-soak-guard.md`
- Modify: `docs/superpowers/plans/2026-07-21-youtube-discovery-scheduler.md`
- Modify: `deploy/four-machine-topology.md`

- [ ] Mark halt-after-selection as superseded by atomic intake pause.
- [ ] Document that success remains intake-paused for mature metrics while
  failure becomes fully halted.
- [ ] Advance the per-attempt phrase to `批准第四次 unlisted canary`.
- [ ] Preserve 127 application, 150 GPU/publisher/control, and 126 exclusion.

### Task 6: Verify, review, push, and deploy

- [ ] Run focused Python, migration, Go, and shell tests.
- [ ] Run the required backend suite, Go suite, deploy contracts, canary shell
  contract, `git diff --check`, and new-file Ruff checks.
- [ ] Record existing advisory-only Ruff/mypy findings separately from new
  failures.
- [ ] Request independent code review and address confirmed findings using
  tests first.
- [ ] Commit, fast-forward main, push, and require exact-SHA GitHub Actions
  success.
- [ ] Verify automatic deployment of VideoProcess app/aggregator services to
  127/150; do not deploy PDS and do not involve 126.
- [ ] Require migration head 030, exact deployed SHA, expected placement,
  schedule `CLOSED`, zero unsafe/public/upload backlog, and zero Redis pending.
- [ ] Run a fresh read-only canary preflight and persist 0600 evidence.

### Task 7: Run the next live attempt only after fresh approval

- [ ] Do not reuse the failed third-attempt approval.
- [ ] Require the exact phrase `批准第四次 unlisted canary`.
- [ ] Run one owned-only unlisted attempt and verify exactly one task, job,
  upload operation, YouTube video, publication, immediate feedback probe, and
  durable mature-metrics schedule.
- [ ] Leave the channel intake-paused, global schedule `CLOSED`, and Redis
  pending counts at zero.

