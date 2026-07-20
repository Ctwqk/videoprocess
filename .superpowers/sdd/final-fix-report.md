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

## Final Review 2 Fix Wave

### Status And Scope

`DONE_WITH_CONCERNS`. Work remained on `codex/channelops-soak-guard` in the
required worktree. No deployment, push, soak activation, activation-state
change, YouTube interaction, upload, publication, production access, or root
plan edit occurred.

Implementation commit:

- `0b6055c549bead0fcad2a50da798892af3a2a123` -
  `fix: enforce channelops review authority`
- The report-only commit is listed in the final task response because this file
  must be written before that commit can exist.

### Verified Findings And Fixes

- Queue dispatch now resolves authority from the referenced task, publication
  and task, account, or validated payload channel inside the fence transaction.
  Stored non-null metadata must match. Unknown, unresolved, and mismatched work
  fails before external calls or descendants. Only cleanup and channel-less
  alerts are global.
- Revision `024_channelops_human_review_authority` adds the dedicated non-null
  task evidence JSON field. It backfills unambiguous legacy queue channels,
  dead-letters unresolved or mismatched active channel work, clears leases, and
  leaves explicit global work unchanged.
- Human review evidence records scope, actor, server review time, exact plan,
  current `review_approved_at`, optional notes, and publication-specific fields.
  The task release API approves the exact plan and enqueues execution. Go and
  the Python fallback revalidate before external execute, publish, or promotion.
- Manual promotion accepts an optional body with the documented operator
  default, rejects unrelated holds and unsafe channel states, restores only PDS
  holds, preserves plan tokens for external work, remains private/unlisted, and
  still invokes PDS. A terminal prior review attempt creates fresh queue work;
  active attempts remain idempotent.
- Soak assessment validates durable evidence from `planning` onward, including
  legacy snapshot-only external sources. Agent-only, missing, stale,
  mismatched, reset, blocked, and rejected evidence is not accepted.
- The watcher accepts exactly 300 seconds of future skew and rejects any
  positive fractional amount beyond it on both GNU and BSD date paths.

### Changed Files

- Schema and migration: `backend/app/models/channel_agent.py`,
  `backend/alembic/versions/024_channelops_human_review_authority.py`.
- Python review/API/runtime: `backend/app/channel_agent/human_review.py`,
  `backend/app/api/channel_agent.py`, `backend/app/channel_agent/service.py`,
  `backend/app/services/channelops_soak_guard.py`.
- Go authority/runtime: `internal/channelops/execution_fence.go`,
  `internal/channelops/human_review.go`, `internal/channelops/handlers.go`,
  `internal/channelops/store_publications.go`,
  `internal/channelops/store_tasks.go`, `internal/channelops/store_tick.go`,
  `internal/channelops/types.go`.
- Tests: `backend/tests/channel_agent/test_api.py`,
  `backend/tests/channel_agent/test_service.py`,
  `backend/tests/services/test_channelops_soak_guard.py`,
  `backend/tests/services/test_youtube_upload_operations.py`,
  `internal/channelops/integration_test.go`,
  `tests/test_channelops_soak_watch.sh`.
- Watcher and docs: `deploy/swarm/channelops-soak-watch.sh`,
  `docs/channelops-go-live-runner.md`.

### RED Evidence

- Offline migration:
  `cd backend && .venv/bin/python3 -m pytest tests/services/test_youtube_upload_operations.py::test_alembic_upgrade_head_renders_offline_postgresql_sql -q`
  -> `1 failed`; generated SQL lacked
  `ADD COLUMN human_review_evidence_json JSON`.
- Queue authority:
  `DATABASE_URL=postgres://vp:vp_test@127.0.0.1:55433/videoprocess go test ./internal/channelops -run 'TestPromotionAuthorityFencesReferencedChannelWithNullOrMismatchedMetadata|TestQuarantineFirstBlocksNullOrMismatchedPromotionMetadata|TestGlobalCleanupAndAlertDispatchWithoutChannelMetadata' -count=1 -v -timeout=60s`
  -> null and mismatch cases failed because the referenced channel lock was not
  held or YouTube was called before quarantine; global cleanup/alert passed.
- Task release and promotion:
  `cd backend && .venv/bin/python3 -m pytest tests/channel_agent/test_api.py -k 'human_review_release or manual_promotion' -q`
  -> `11 failed, 1 passed`; release returned 404, unrelated holds returned 200,
  and PDS-held tasks were not restored. A test-only missing `select` import was
  corrected before recording this behavioral RED result.
- Soak evidence:
  `cd backend && .venv/bin/python3 -m pytest tests/services/test_channelops_soak_guard.py -k 'external_asset_planning or invalidates_task_human_review_evidence' -q`
  -> `6 failed, 1 passed`; all five invalid evidence modes and an approval-reset
  plan were treated as healthy. The reset fixture was first narrowed to avoid an
  unrelated pipeline rebuild error.
- Go evidence:
  `DATABASE_URL=postgres://vp:vp_test@127.0.0.1:55433/videoprocess go test ./internal/channelops -run 'TestExternalAssetExecuteRejectsInvalidHumanReviewEvidence|TestValidHumanReviewEvidenceReachesExecutePublishAndManualPromotion' -count=1 -v -timeout=60s`
  -> failed because `human_review_evidence_json` did not exist. One promotion
  setup also exposed stale fixture data; the disposable database was recreated
  and auxiliary fixture cleanup was corrected.
- Fractional boundary: `bash tests/test_channelops_soak_watch.sh` -> failed
  because `300.1` seconds reached threshold validation rather than producing
  `future_started_at`.
- Terminal manual retry:
  `cd backend && .venv/bin/python3 -m pytest tests/channel_agent/test_api.py::test_manual_promotion_requeues_after_prior_terminal_review_attempt -q`
  -> `1 failed`; the endpoint returned the prior `succeeded` queue item.

### GREEN Evidence

- API review and promotion:
  `cd backend && .venv/bin/python3 -m pytest tests/channel_agent/test_api.py -k 'human_review_release or manual_promotion' -q`
  -> `15 passed, 18 deselected`.
- Soak evidence:
  `cd backend && .venv/bin/python3 -m pytest tests/services/test_channelops_soak_guard.py -k 'external_asset or human_review_evidence' -q`
  -> `13 passed, 27 deselected`.
- Offline migration command above -> `1 passed`.
- PostgreSQL authority/evidence:
  `DATABASE_URL=postgres://vp:vp_test@127.0.0.1:55433/videoprocess go test ./internal/channelops -run 'TestPromotionAuthorityFencesReferencedChannelWithNullOrMismatchedMetadata|TestQuarantineFirstBlocksNullOrMismatchedPromotionMetadata|TestGlobalCleanupAndAlertDispatchWithoutChannelMetadata|TestExternalAssetExecuteRejectsInvalidHumanReviewEvidence|TestValidHumanReviewEvidenceReachesExecutePublishAndManualPromotion' -count=1 -v -timeout=90s`
  -> every test and subtest passed.
- Full backend: `cd backend && .venv/bin/python3 -m pytest` ->
  `628 passed, 1 skipped, 11 warnings in 65.15s`.
- Full Go:
  `DATABASE_URL=postgres://vp:vp_test@127.0.0.1:55433/videoprocess go test ./... -count=1 -timeout=180s`
  -> all packages passed.
- Watcher: `bash tests/test_channelops_soak_watch.sh` ->
  `PASS: channelops soak watcher contract`.
- Deployment: `bash tests/test_vp_deploy_sync_extension.sh` -> exit 0.
- Syntax: `bash -n deploy/swarm/channelops-soak-watch.sh`,
  `bash -n deploy/swarm/deploy-sync-extension.sh`, and
  `bash -n tests/test_channelops_soak_image_smoke.sh` -> exit 0.
- Image smoke: `bash tests/test_channelops_soak_image_smoke.sh` ->
  `SKIP: set VP_SOAK_SMOKE_IMAGE and VP_SOAK_SMOKE_DATABASE_URL`.
- Changed-file Ruff -> `All checks passed!`; full-tree Ruff retained the same
  20 existing findings. `mypy app` retained the same 68 existing errors in 25
  files; the new review helper and soak modules pass targeted mypy.
- `git diff --check` -> exit 0 before the implementation commit.

### PostgreSQL 16 Migration And Repair

- Container: `vp-final-review-2-postgres`.
- Container ID:
  `ab37967529620f356dd8f104b3b19ea7b38bc11b8dd2290f20d2b21656cf0445`.
- Image: `postgres:16-alpine`; image ID
  `sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777`;
  server version `PostgreSQL 16.14`.
- Host mapping `127.0.0.1:55433 -> 5432`; database/user
  `videoprocess` / `vp`.
- Fresh command:
  `DATABASE_URL=postgresql+asyncpg://vp:vp_test@127.0.0.1:55433/videoprocess .venv/bin/alembic upgrade head`
  applied revisions `001` through `024_channelops_human_review_authority`.
- Repair proof downgraded to `023_youtube_upload_operations`, seeded null,
  mismatch, unresolved, cleanup, and channel-less alert rows, then upgraded to
  head. `repair-null` was backfilled to the referenced task channel and stayed
  queued; `repair-mismatch` and `repair-unresolved` were dead-lettered with
  `queue_authority_unresolved` and cleared leases; global cleanup/alert remained
  queued with null channel metadata. The new task column was `NOT NULL`.
- The database was recreated from empty and migrated again before full Go
  verification. The disposable container was stopped and removed afterward.

### Concerns

- The opt-in real watcher-image smoke did not run because no image and isolated
  smoke URL were supplied; its contract and syntax checks passed.
- Full-tree Ruff and mypy remain nonzero only at the recorded pre-existing
  baselines of 20 findings and 68 errors in 25 files.
