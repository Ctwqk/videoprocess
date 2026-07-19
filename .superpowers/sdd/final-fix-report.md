# Final Review Fix Wave Report

## Status

DONE_WITH_CONCERNS

Work was performed only in
`/Users/wenjieliu/videoprocess/.worktrees/channelops-soak-guard` on branch
`codex/channelops-soak-guard`. No deployment, push, watcher activation, activation
state file, YouTube call, upload, or publication was performed. The root user
plan was not edited.

## Commits

- `ef47a84` - `fix: harden soak assessment boundaries`
- `5231b70` - `fix: fence channelops quarantine races`
- `428ce5e` - `fix: preserve verified soak watcher install`
- The report commit is listed in the final task response because this file must
  be written before that commit exists.

## Review Claim Verification

- PostgreSQL timestamp claim: confirmed. The generated asyncpg query bound the
  same aware value to `TIMESTAMP WITHOUT TIME ZONE` inherited timestamp columns
  and timezone-aware task state columns.
- In-flight Go work claim: confirmed. `HandlerService.Handle` had no channel
  transaction fence, claims ignored channel state, and queue completion matched
  only `id`.
- Stale task claim: confirmed. Held task handlers called AutoFlow, PDS, or
  YouTube, and promotion could restore a held task.
- Quarantine publication exemption: confirmed. Nonterminal tasks with a
  publication row were retained with their running jobs and nodes.
- Future activation claim: confirmed. The service, CLI, and watcher accepted an
  arbitrarily future `started_at`.
- Post-commit cleanup claim: confirmed. Cleanup failure changed a verified
  watcher/crontab installation from success to failure.
- Review-gate claim: confirmed. An external-asset task with stale
  `approval_mode=agent` reached PDS and AutoFlow approval.

## Implemented Fixes

- The assessment now maintains aware and naive UTC boundaries. Naive UTC is
  used only for inherited `created_at`/`updated_at`; timezone-aware fields keep
  the aware boundary. Loaded timestamps are normalized for Python comparisons.
- Assessment and CLI reject activation more than 300 seconds in the future.
  The watcher performs the same check using GNU or BSD `date`. Exactly 300
  seconds is accepted.
- Channel-bound production dispatch begins a PostgreSQL transaction, selects
  the `channel_profiles` row `FOR UPDATE`, rejects missing/disabled/halted
  channels, and runs all Store database work through that same transaction
  while holding the lock across external calls. Global items remain supported.
- Claim SQL excludes missing, disabled, and halted channel rows.
- Queue success and retry/dead-letter completion match `id`, running status,
  `locked_by`, and `locked_at`. A zero-row lost lease is benign.
- Plan, execute, observe, publish, promote, reconcile, and metrics handlers
  check task state before external calls or descendant enqueue. Held work is a
  stale no-op. External assets always stop at the human review gate before
  execution even when legacy data says `approval_mode=agent`.
- Quarantine now holds every nonterminal task, including tasks with publication
  rows, and cancels their active jobs/nodes while retaining publication and
  feedback evidence.
- Verified watcher/crontab installation is the cleanup commit point. Cleanup
  failure after it is a warning; pre-commit failure still rolls back and fails.
- Added an opt-in real watcher-image CLI smoke script. It requires an explicit
  test-database attestation, never uses `--apply`, and expects guard exit 20 for
  a deliberately missing channel.
- The runbook now documents the pre-upload external-asset gate, 300-second
  activation tolerance, and real-image smoke command.

## Changed Files

### Assessment and activation

- `backend/app/services/channelops_soak_guard.py`
- `backend/app/channel_agent/soak_guard_cli.py`
- `backend/tests/services/test_channelops_soak_guard.py`
- `backend/tests/channel_agent/test_soak_guard_cli.py`
- `deploy/swarm/channelops-soak-watch.sh`
- `tests/test_channelops_soak_watch.sh`

### Quarantine, handler fence, leases, and stale work

- `backend/app/services/channelops_quarantine.py`
- `backend/tests/services/test_channelops_quarantine.py`
- `internal/channelops/execution_fence.go`
- `internal/channelops/store.go`
- `internal/channelops/queue.go`
- `internal/channelops/runner.go`
- `internal/channelops/handlers.go`
- `internal/channelops/alerts.go`
- `internal/channelops/cleanup.go`
- `internal/channelops/learning.go`
- `internal/channelops/scheduler.go`
- `internal/channelops/store_publications.go`
- `internal/channelops/store_smoke.go`
- `internal/channelops/store_tasks.go`
- `internal/channelops/store_tick.go`
- `internal/channelops/handlers_test.go`
- `internal/channelops/integration_test.go`
- `internal/channelops/queue_test.go`
- `internal/channelops/store_tick_test.go`

### Deployment and documentation

- `deploy/swarm/deploy-sync-extension.sh`
- `tests/test_vp_deploy_sync_extension.sh`
- `tests/test_channelops_soak_image_smoke.sh`
- `docs/channelops-go-live-runner.md`
- `.superpowers/sdd/final-fix-report.md`

## Red-Green Record

### Timestamp and future activation

- RED: future service test reached database access and failed with an attribute
  error instead of rejecting; CLI returned exit 3 `database_error` instead of
  exit 2 `invalid_arguments`.
- RED: real asyncpg assessment failed with
  `can't subtract offset-naive and offset-aware datetimes` for
  `$2::TIMESTAMP WITHOUT TIME ZONE` in the production task recency query.
- GREEN:
  `.venv/bin/python -m pytest tests/services/test_channelops_soak_guard.py::test_started_at_exactly_five_minutes_in_future_is_accepted tests/services/test_channelops_soak_guard.py::test_started_at_over_five_minutes_in_future_is_rejected_before_database_access tests/channel_agent/test_soak_guard_cli.py::test_future_activation_returns_invalid_arguments_without_opening_database -q`
  -> `3 passed`.
- GREEN:
  `CHANNEL_OPS_POSTGRES_TEST_URL=postgresql+asyncpg://vp:vp_test@127.0.0.1:55432/videoprocess .venv/bin/python3 -m pytest tests/services/test_channelops_soak_guard.py::test_postgresql_assessment_accepts_mixed_timestamp_column_contracts -q`
  -> `1 passed`.
- RED: watcher contract reported that a 2099 activation unexpectedly succeeded.
- GREEN: `bash tests/test_channelops_soak_watch.sh` -> PASS.

### Queue admission and lease ownership

- RED: a disabled channel item was claimed. Stale success changed a
  dead-lettered row to `succeeded`; stale retry changed it to `queued`.
- GREEN:
  `DATABASE_URL=postgres://vp:vp_test@127.0.0.1:55432/videoprocess go test ./internal/channelops -run 'TestClaimRejectsDisabledAndHaltedChannelsButKeepsGlobalItems|TestQueueLeasePreventsStaleSuccessAndRetryAfterDeadLetter' -count=1 -v`
  -> all tests and subtests passed.

### Channel fence and stale handlers

- RED: quarantine acquired the channel lock while promotion was blocked in the
  YouTube fake; quarantine-first still called YouTube; an already-held
  promotion called YouTube and restored task state.
- During GREEN implementation, the first transaction design exposed a real
  self-deadlock: queue insert FK locking used another pool connection while the
  fence held `FOR UPDATE`. Store work was moved into the same fence transaction.
- GREEN: both promotion/quarantine orderings passed through the production
  `HandlerService.Handle` path. Handler-first completed before quarantine;
  quarantine-first produced zero YouTube calls, no task reversal, no descendant
  enqueue, and no queue resurrection.
- RED: held plan/execute/observe/publish/reconcile/metrics handlers made 3, 1,
  1, 2, 1, and 1 external calls respectively. External-asset planning made two
  post-plan calls and enqueued execution.
- GREEN: the held-handler matrix made zero external calls and zero descendants;
  external assets stopped in `held` with `human_approval_required`.
- Final focused PostgreSQL command:
  `DATABASE_URL=postgres://vp:vp_test@127.0.0.1:55432/videoprocess go test ./internal/channelops -run 'TestQueueLeasePreventsStaleSuccessAndRetryAfterDeadLetter|TestClaimRejectsDisabledAndHaltedChannelsButKeepsGlobalItems|TestPromotionHandleAndQuarantineSerializeOnChannelFence|TestQuarantineFirstPreventsPromotionSideEffects|TestHeldTaskHandlersAreStaleBeforeExternalCallsOrDescendants|TestExternalAssetPlanRequiresHumanReviewBeforeExecution' -count=1 -v -timeout=90s`
  -> all tests and subtests passed.

### Quarantine evidence retention

- RED: a nonterminal task with publication evidence remained `producing`; its
  job and node remained running.
- GREEN:
  `.venv/bin/python -m pytest tests/services/test_channelops_quarantine.py::test_nonterminal_task_with_publication_is_held_but_evidence_is_retained -q`
  -> `1 passed`; full quarantine file -> `11 passed`.

### Deployment cleanup

- RED: injected post-commit cleanup failure made the verified installation
  return failure.
- GREEN: `bash tests/test_vp_deploy_sync_extension.sh` -> exit 0. The contract
  also proves an injected pre-commit install plus cleanup failure rolls back and
  remains a failure.

## PostgreSQL Environment

- Container: `vp-final-fix-postgres`
- Container ID: `5e142668541f9652fcd722323950ee58387d140ac9adebc569d00aacbc516176`
- Image: `postgres:16-alpine`, local image ID `57c72fd2a128`
- Host mapping: `127.0.0.1:55432 -> 5432`
- Database/user: `videoprocess` / `vp`
- Migration command:
  `DATABASE_URL=postgresql+asyncpg://vp:vp_test@127.0.0.1:55432/videoprocess .venv/bin/alembic upgrade head`
- Applied revisions `001` through `023_youtube_upload_operations`.
- The database was recreated and migrated before final Go verification to
  remove focused-test residue. The `--rm` container was stopped and removed
  after verification.

## Final Verification

- Backend: `.venv/bin/python3 -m pytest` -> `605 passed, 1 skipped, 11 warnings`.
  The skip is the opt-in PostgreSQL assessment when its URL is absent from the
  ordinary suite; it passed separately against the container.
- Go: `DATABASE_URL=postgres://vp:vp_test@127.0.0.1:55432/videoprocess go test ./...`
  -> all packages passed.
- Watcher: `bash tests/test_channelops_soak_watch.sh` -> PASS.
- Deployment: `bash tests/test_vp_deploy_sync_extension.sh` -> exit 0.
- Syntax: both required deployment scripts and the new image smoke script pass
  `bash -n`.
- Image smoke: `bash tests/test_channelops_soak_image_smoke.sh` -> documented
  SKIP because no local `vp-ffmpeg-worker-python` watcher-matching image exists.
- Ruff full tree: 20 existing findings. Ruff on every changed Python file ->
  `All checks passed!`.
- Mypy: 68 existing errors in 25 files. Two are in the unchanged dialect typing
  section of `channelops_quarantine.py`; no broad baseline cleanup was made.
- `git diff --check` passed before commits and is rerun after this report.

## Concerns

- The real watcher-image smoke could not run because no matching Python worker
  or publisher image is available locally. Building `backend/Dockerfile.worker`
  would pull the large CUDA and speech dependency stack, which the brief permits
  avoiding. The opt-in real-image test is present and syntax-checked.
- Full-tree Ruff and mypy remain nonzero because of the recorded pre-existing
  baseline findings.
