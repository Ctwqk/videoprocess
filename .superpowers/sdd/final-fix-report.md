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

## Final Review 3 Consolidated Fix Wave

### Status And Scope

`DONE_WITH_CONCERNS`. Work remained on `codex/channelops-soak-guard` in the
required worktree, starting from `d5313cf`. No deployment, push, soak
activation, activation-state change, production access, YouTube interaction,
upload, publication, or user-owned root-plan edit occurred.

Implementation commit:

- `85e5a63127df3e0dafcab570e0d182e641ad2f00` -
  `fix: close final review 3 authority gaps`
- The report-only commit is listed in the final task response because this
  section must be written before that commit exists.

### Verified Findings And Fixes

- AutoFlow approval now binds to a SHA-256 hash of the canonical execution
  revision. All direct save and patch paths compare that payload, clear human,
  public, and agent authority on a real change, preserve authority on true
  no-ops, and fail closed for legacy approved rows without a hash.
- Review release and manual promotion lock channel, task, and publication in
  quarantine order, revalidate after locking, and commit approval, evidence,
  task state, and queue work atomically. AutoFlow approval supports a
  flush-only internal path while public endpoint behavior remains unchanged.
- Revision 024 retains the rolling-writer `{}` server default, repairs queue
  ownership consistently for task, publication, account, payload-channel, and
  legacy stored-channel alerts, and dead-letters malformed active rows.
  Revision 025 additively adds nullable revision and execute-key fields plus a
  nullable unique execute-key constraint.
- Go queue claims derive authoritative ownership before channel-state
  filtering. Valid halted/disabled work remains unclaimed; null, unresolved,
  unsupported, or mismatched work is claimed once and terminally rejected with
  its lease cleared and no external call or descendant.
- Python channel alerts include literal `channel_id` in payload and queue
  metadata. Go treats payload channel as authoritative, legacy stored-only
  alerts as channel-bound, and only payload-less/null-metadata alerts as global.
- The watcher uses a Bash 3.2 allowlist parser for literal state records and
  never sources state as code. Operator regex is additive to the immutable
  escaped `CASPERs-Mac-mini`, `colima-swarmbridged`, and `10.0.0.126` baseline.
- Keyed AutoFlow execution reserves the unique run first, flushes pipeline,
  job, plan, usage, and schedule state in one transaction, commits before
  background start, and returns the existing run on replay. Go sends a stable
  task/plan/revision key on every retry. Legacy no-key execution is preserved.

### Changed Files

- Migrations and schemas: `backend/alembic/versions/024_channelops_human_review_authority.py`,
  `backend/alembic/versions/025_autoflow_revision_idempotency.py`,
  `backend/app/models/autoflow.py`, `backend/app/schemas/autoflow.py`.
- Python authority/runtime: `backend/app/api/channel_agent.py`,
  `backend/app/autoflow/service.py`, `backend/app/channel_agent/alerts.py`,
  `backend/app/channel_agent/human_review.py`,
  `backend/app/channel_agent/service.py`, `backend/app/services/job_service.py`,
  `backend/app/services/pipeline_service.py`,
  `backend/app/services/schedule_service.py`.
- Go authority/runtime: `internal/channelops/autoflow_client.go`,
  `internal/channelops/execution_fence.go`, `internal/channelops/human_review.go`,
  `internal/channelops/queue.go`, `internal/channelops/runner.go`,
  `internal/channelops/store_tasks.go`, `internal/channelops/types.go`.
- Python tests: `backend/tests/autoflow/test_autoflow_api.py`,
  `backend/tests/autoflow/test_execute_idempotency_postgres.py`,
  `backend/tests/channel_agent/test_api.py`,
  `backend/tests/channel_agent/test_models_queue.py`,
  `backend/tests/channel_agent/test_operator_quarantine_postgres.py`,
  `backend/tests/channel_agent/test_service.py`,
  `backend/tests/migrations/test_final_review3_postgres.py`,
  `backend/tests/services/test_channelops_soak_guard.py`,
  `backend/tests/services/test_youtube_upload_operations.py`.
- Go and shell tests: `internal/channelops/autoflow_client_test.go`,
  `internal/channelops/integration_test.go`, `internal/channelops/queue_test.go`,
  `tests/test_channelops_soak_watch.sh`.
- Watcher and docs: `deploy/swarm/channelops-soak-watch.sh`,
  `deploy/four-machine-topology.md`, `docs/autoflow/architecture.md`,
  `docs/channelops-go-live-runner.md`.

### RED Evidence

- Exact revision authority:
  `cd backend && .venv/bin/python3 -m pytest tests/autoflow/test_autoflow_api.py -k 'exact_execution_revision_hash or execution_revision_changes or target_platform_and_constraint or true_noop or legacy_approved_plan_without_revision_hash' -q`
  -> `13 failed`; approved hashes were absent, all direct mutation classes and
  target/constraint patches preserved stale approval, and legacy hash-less
  approval executed.
- Rolling migration default:
  `cd backend && .venv/bin/python3 -m pytest tests/services/test_youtube_upload_operations.py::test_alembic_upgrade_head_renders_offline_postgresql_sql -q`
  -> `1 failed`; revision 024 rendered `DROP DEFAULT`.
- Operator/quarantine races:
  `CHANNEL_OPS_POSTGRES_TEST_URL=postgresql+asyncpg://vp:vp_test@127.0.0.1:55434/videoprocess .venv/bin/python3 -m pytest tests/channel_agent/test_operator_quarantine_postgres.py -q`
  -> `4 failed`; release/promotion reached stale completion before expected
  channel/task/publication serialization. Two subsequent failures identified
  test-observation details (`SELECT ... FOR UPDATE` text and typed response)
  before the unchanged four-race suite went green.
- Queue and alert authority:
  `DATABASE_URL=postgres://vp:vp_test@127.0.0.1:55434/videoprocess go test ./internal/channelops -run 'TestRunnerImmediatelyRejectsInvalidQueueAuthority|TestAlertQueueAuthorityScopesLegacyAndGlobalRows' -count=1 -v`
  -> both test groups failed: a stored-halted mismatch stayed queued, an
  unresolved row retried, and a legacy stored-channel alert dispatched.
- Python alerts:
  the focused models/service command for alert payload and PDS outage tests ->
  `3 failed`; `channel_id` was rejected/absent and two channel outages deduped
  into one global row.
- Watcher parser:
  `bash tests/test_channelops_soak_watch.sh` -> failed with
  `state command substitution executed`.
- Execute idempotency:
  `CHANNEL_OPS_POSTGRES_TEST_URL=postgresql+asyncpg://vp:vp_test@127.0.0.1:55434/videoprocess .venv/bin/python3 -m pytest tests/autoflow/test_execute_idempotency_postgres.py -q`
  -> `4 failed, 1 passed`; concurrent and replay calls created distinct runs,
  failure persisted partial work, and cross-plan key reuse was accepted.
- Go execute key:
  the focused HTTP AutoFlow client tests -> `2 failed`; no key was sent and a
  missing approved revision reached the network.
- Go evidence hash:
  the focused external-asset evidence test failed only the
  `revision_mismatched` subtest because an external execute call was made.

### GREEN Evidence

- Exact revision command above -> `13 passed, 22 deselected`.
- Offline migration test -> `1 passed`; full offline
  `alembic upgrade head --sql` also exited 0 through revision 025.
- Deterministic operator races -> `4 passed in 0.92s`.
- Queue/alert focused Go tests -> all subtests passed; full
  `internal/channelops` package later passed.
- Python alert focused tests -> `3 passed`.
- Watcher contract -> `PASS: channelops soak watcher contract` on
  `GNU bash 3.2.57`, including GNU/BSD date paths.
- Execute idempotency PostgreSQL suite -> `6 passed`, covering concurrent
  duplicate, response-loss replay, rollback, cross-plan reuse, legacy no-key,
  and closed-window recovery/no replay start.
- Focused Go client/handler/evidence tests all passed, including response-loss
  handler replay with two identical keys.
- PostgreSQL acceptance bundle:
  `CHANNEL_OPS_POSTGRES_TEST_URL=postgresql+asyncpg://vp:vp_test@127.0.0.1:55434/videoprocess .venv/bin/python3 -m pytest tests/migrations/test_final_review3_postgres.py tests/channel_agent/test_operator_quarantine_postgres.py tests/autoflow/test_execute_idempotency_postgres.py tests/services/test_channelops_soak_guard.py::test_postgresql_assessment_accepts_mixed_timestamp_column_contracts -q`
  -> `12 passed in 3.28s`.
- Full backend: `cd backend && .venv/bin/python3 -m pytest` ->
  `641 passed, 12 skipped, 11 warnings in 65.38s`; opt-in PostgreSQL tests are
  among the ordinary skips and passed separately above.
- Full Go:
  `DATABASE_URL=postgres://vp:vp_test@127.0.0.1:55434/videoprocess go test ./... -count=1`
  -> all packages passed.
- Deployment shell contract: `bash tests/test_vp_deploy_sync_extension.sh` ->
  exit 0. All three required shell files pass `bash -n`.
- Image smoke: `bash tests/test_channelops_soak_image_smoke.sh` ->
  `SKIP: set VP_SOAK_SMOKE_IMAGE and VP_SOAK_SMOKE_DATABASE_URL`.
- Changed-file Ruff -> `All checks passed!`. Full Ruff reports 19 existing
  findings, improving the recorded baseline of 20 by removing an unused import
  from a touched test. `mypy app` remains exactly 68 existing errors in 25
  files. `git diff --check` -> exit 0.

### PostgreSQL 16 Migration And Cleanup

- Container: `vp-final-review-3-postgres`; ID
  `65fa8145b8cd40a3034446e1e7c402cab5648a1623b3ede48623484c06cae342`.
- Image: `postgres:16-alpine`; image ID
  `sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777`;
  server `PostgreSQL 16.14` on port `127.0.0.1:55434`.
- Fresh migration applied revisions 001 through
  `025_autoflow_revision_idempotency`. The isolated migration test then
  downgraded a fresh database to `023_youtube_upload_operations`, seeded null,
  mismatched, unresolved, payload-alert, stored-only alert, and global alert
  rows, and upgraded through head. Null ownership repaired, malformed rows
  dead-lettered with cleared leases, alert scope remained correct, and a legacy
  task insert omitting `human_review_evidence_json` received `{}`.
- The main disposable database was recreated from empty and migrated to head
  before full Go verification. `vp-final-review-3-postgres` was force-removed
  after verification; no test container remains.

### Concerns

- The opt-in real watcher-image smoke did not run because no matching image and
  isolated smoke database URL were supplied. Its script, watcher contract, and
  syntax checks passed.
- Full-tree Ruff and mypy remain nonzero only for the recorded pre-existing
  baselines: 19 Ruff findings and 68 mypy errors in 25 files. Changed-file Ruff
  is clean and no new mypy count was introduced.

## Final Review 4 Consolidated Fix Wave

### Status And Scope

`DONE_WITH_CONCERNS`. Work remained on `codex/channelops-soak-guard` in the
required worktree. No deployment, push, production access, host 126 access,
activation-state change, real PDS/YouTube call, upload, publication, or edit to
the user-owned root `vp_autonomous_production_feedback_loop_plan.md` occurred.

Implementation commit:

- `256c5c613e016bb23177f3854e49819e462c3453` -
  `fix: fence final review 4 authority races`
- The report-only commit is listed in the final task response because this
  section must be written after the implementation commit exists.

### Implemented Fixes

- Persisted AutoFlow mutation and execution now use row locks. Execution owns
  schedule, plan, pipeline, job, run, usage, and plan-state changes in one
  transaction; it revalidates locked plan authority and starts background work
  only after commit. Persistence rejects stale in-memory execution revisions.
- Revision 026 adds database-owned `execution_revision`, approval-bound
  `approved_revision`, and run `request_fingerprint`. Its PostgreSQL trigger
  compares every canonical execution field, excludes approval-only rights
  keys, rotates revision on canonical change, and clears all authority for
  changed, blocked, rejected, or incorrectly revision-bound writes.
- Python and Go review evidence now carry and validate the numeric approved
  revision as well as plan id, timestamp, and SHA-256 revision hash. Go direct
  promotion enters the queue/channel transaction fence when needed, locks task,
  publication, and plan authority before PDS/YouTube, and holds the locks
  through durable promotion writes.
- Manual promotion and publication rejection share channel -> task ->
  publication -> plan operator locking. Quarantine follows channel -> schedule
  -> task/publication/job; AutoFlow follows schedule -> plan -> job; the durable
  starter follows schedule -> job and atomically parks CLOSED jobs without
  dispatch.
- Every persisted execute request uses the same transactional path. Its key is
  bound to canonical JSON for all request behavior fields except the key and
  inline plan. Exact replay succeeds, changed flags conflict, and legacy keyed
  rows without fingerprints fail closed. Review-free owned plans retain their
  existing execution compatibility while review-gated plans require exact
  current authority.

### Changed Files

- Migration/model/schema: `backend/alembic/versions/026_autoflow_authority_fence.py`,
  `backend/app/models/autoflow.py`, `backend/app/schemas/autoflow.py`.
- Python authority/runtime: `backend/app/api/channel_agent.py`,
  `backend/app/autoflow/service.py`,
  `backend/app/channel_agent/human_review.py`,
  `backend/app/orchestrator/engine.py`,
  `backend/app/services/channelops_quarantine.py`,
  `backend/app/services/schedule_service.py`.
- Python tests: `backend/tests/autoflow/test_autoflow_api.py`,
  `backend/tests/autoflow/test_execute_idempotency_postgres.py`,
  `backend/tests/channel_agent/test_api.py`,
  `backend/tests/migrations/test_final_review3_postgres.py`,
  `backend/tests/migrations/test_final_review4_postgres.py`,
  `backend/tests/services/test_channelops_quarantine.py`,
  `backend/tests/services/test_channelops_soak_guard.py`.
- Go runtime: `internal/channelops/autoflow_client.go`,
  `internal/channelops/handlers.go`, `internal/channelops/human_review.go`,
  `internal/channelops/store.go`, `internal/channelops/store_publications.go`,
  `internal/channelops/store_tasks.go`, `internal/channelops/types.go`.
- Go tests: `internal/channelops/autoflow_client_test.go`,
  `internal/channelops/integration_test.go`.

### RED Evidence

- Execute request fingerprint:
  `cd backend && CHANNEL_OPS_POSTGRES_TEST_URL=postgresql+asyncpg://vp:vp_test@127.0.0.1:55435/videoprocess .venv/bin/python3 -m pytest tests/autoflow/test_execute_idempotency_postgres.py -k 'idempotency_key_rejects_execute_flag_change or idempotency_key_rejects_save_as_template_change or exact_request_fingerprint_replay or legacy_keyed_run_without_fingerprint' -q`
  -> `3 failed, 1 passed, 9 deselected`; changed execution/template flags
  silently reused a key and the legacy fingerprint column was absent.
- Python plan and schedule interleavings:
  `cd backend && CHANNEL_OPS_POSTGRES_TEST_URL=postgresql+asyncpg://vp:vp_test@127.0.0.1:55435/videoprocess .venv/bin/python3 -m pytest tests/autoflow/test_execute_idempotency_postgres.py -k 'execute_first_holds_plan_authority or reject_first_revokes_authority or schedule_close_between_creation' -q`
  -> `3 failed, 10 deselected`; patch/reject did not observe the required plan
  lock and the durable starter did not wait for schedule-close authority.
- Review evidence revision:
  `cd backend && .venv/bin/python3 -m pytest tests/channel_agent/test_api.py -k 'human_review_release_approves_exact_plan or manual_promotion_preserves_external_plan_review_token' -q`
  -> `2 failed, 31 deselected`; execution/approved revision was not persisted or
  emitted in evidence.
- Migration authority:
  `cd backend && CHANNEL_OPS_POSTGRES_TEST_URL=postgresql+asyncpg://vp:vp_test@127.0.0.1:55435/videoprocess .venv/bin/python3 -m pytest tests/migrations/test_final_review4_postgres.py -q`
  -> `1 failed`; head remained 025 and no database revision fence existed.
- Go promotion interleavings:
  `DATABASE_URL=postgres://vp:vp_test@127.0.0.1:55435/videoprocess go test ./internal/channelops -run 'TestPlanRejectFirstPreventsDirectPromotionSideEffects|TestDirectPromotionHoldsPlanAuthorityThroughYouTubeAndDurableWrites' -count=1 -v -timeout=60s`
  -> both tests failed; direct promotion and concurrent invalidation completed
  without waiting on plan authority.

### GREEN And Full Verification

- The fingerprint focus above passed all four selected tests; the complete
  PostgreSQL AutoFlow execution file passed `13 passed in 2.07s`.
- Python race focus passed `3 passed, 10 deselected in 0.80s`; API evidence
  focus passed `2 passed, 31 deselected`; migration focus passed `1 passed`.
- The Go promotion focus passed both deterministic tests. Full
  `internal/channelops` later passed, including valid/invalid external evidence,
  direct promotion, retry idempotency, and both invalidation linearization
  orders.
- Final real PostgreSQL acceptance command:
  `cd backend && CHANNEL_OPS_POSTGRES_TEST_URL=postgresql+asyncpg://vp:vp_test@127.0.0.1:55435/videoprocess .venv/bin/python3 -m pytest tests/migrations/test_final_review4_postgres.py tests/migrations/test_final_review3_postgres.py tests/channel_agent/test_operator_quarantine_postgres.py tests/autoflow/test_execute_idempotency_postgres.py tests/channel_agent/test_api.py -q`
  -> `52 passed in 6.17s`.
- Fresh database migration: an empty `videoprocess` database upgraded from 001
  through `026_autoflow_authority_fence`; the migration test additionally
  proved fresh upgrade, literal 025-era canonical update revision rotation,
  literal 025-era approval failure, valid revision-bound approval, and downgrade
  removal of the trigger and three new columns.
- Required backend test command `cd backend && python3 -m pytest` could not load
  pytest from Homebrew Python 3.14. The project environment equivalent,
  `cd backend && .venv/bin/python3 -m pytest`, passed
  `641 passed, 20 skipped, 11 warnings in 65.24s`.
- `cd backend && python3 -m ruff check . || true` likewise could not load Ruff.
  `.venv/bin/python3 -m ruff check .` reports 17 existing repository findings,
  improved from the recorded baseline of 19. Ruff over every changed Python
  file reports `All checks passed!`.
- `cd backend && python3 -m mypy app || true` could not load mypy.
  `.venv/bin/python3 -m mypy app` reports 66 existing errors in 24 files,
  improved from the recorded 68 errors in 25 files; the new shared schedule
  helper has no mypy errors.
- `DATABASE_URL=postgres://vp:vp_test@127.0.0.1:55435/videoprocess go test ./... -count=1`
  -> all Go packages passed against the fresh revision-026 database.
- `bash tests/test_channelops_soak_watch.sh` ->
  `PASS: channelops soak watcher contract`; `bash tests/test_vp_deploy_sync_extension.sh`
  exited 0. Watcher, deploy extension, both contract scripts, and image-smoke
  script all passed `bash -n`.
- `git diff --check` exited 0. Pre-commit `git status --short` contained exactly
  the 25 implementation/test files listed above; no unrelated or root-plan file
  was present.

### PostgreSQL 16 And Cleanup

- Disposable container `vp-final-review-4-postgres`, id
  `0cc4727c42f5f9857358acf4750cc8630d5ff5260eb9d38eb0dcea5c9557ddef`,
  used `postgres:16-alpine` image id
  `sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777`.
- Server was `PostgreSQL 16.14` on `127.0.0.1:55435`. The main database was
  recreated from empty before the final Go suite. The container was force
  removed after verification and its absence was checked.

### Self-Review

- Lock order is acyclic across the changed paths: an existing Go fence owns
  channel first; Python quarantine then owns schedule before task/publication/
  job; AutoFlow owns schedule before plan/job; starters own schedule before job.
- Plan and schedule losing writers wait on real PostgreSQL locks. Commit order
  determines the result: execution/promotion first permits that exact approved
  revision and invalidation follows; invalidation first makes execution or
  promotion fail closed before external effects.
- AutoFlow reservation and all executable rows roll back together on planning
  or job-creation failure. Replays validate plan, approval hash, approved
  revision, and full request fingerprint before returning durable state.
- Go promotion acquires exact manual/external plan evidence before PDS or
  YouTube and keeps the transaction through durable writes. Tests use fakes;
  invalidation-first asserts zero YouTube calls. No verification touched a real
  external platform.

### Concerns

- The exact system-`python3` commands cannot run because that interpreter lacks
  pytest, Ruff, and mypy; all project-environment commands completed as recorded.
- Full-tree Ruff and mypy remain nonzero only for pre-existing baseline findings;
  changed-file Ruff and the newly typed helper are clean.
- Per the binding brief, the deployed-image smoke was not run. It remains the
  post-push check against the exact naturally deployed image; its script syntax
  passed.

## Final Review 5 Consolidated Fix Wave

### Status And Scope

`DONE_WITH_CONCERNS`. Work remained on `codex/channelops-soak-guard` in the
required worktree and started from clean commit
`649e1fcd197315bdc2cd85a544aa110b65fa1317`.

Implementation commit:

- `2e575c1985f713353038e70e7517400dda008eb1` -
  `fix: close final review 5 authority gaps`
- The report-only commit is listed in the final task response because this
  section was written after the implementation commit existed.

No deployment, push, production access, host 126 access, activation-state
write, real PDS/YouTube call, upload, publication, or edit to the user-owned
root `vp_autonomous_production_feedback_loop_plan.md` occurred. Migration 026
was preserved unchanged; no migration was needed because the existing
`production_tasks.rationale_json.autoflow_plan_payload` is the durable task
snapshot.

### Implemented Fixes

- Go approval now captures a typed observation containing exact plan id,
  approved revision hash, and positive approved revision. The planning
  transition persists that authority in task rationale and copies it into the
  durable execute queue payload. Python human-review release writes the same
  snapshot. Task reads no longer derive expected execution authority from a
  live plan join.
- Execute handling cross-checks queue and task snapshots before AutoFlow HTTP.
  Go sends a stable idempotency key and both expected fields. Python validates
  the caller-observed pair under schedule -> plan locks before new work, while
  exact committed replays are checked before live-plan authority so R1 remains
  replayable after R2. A post-schedule key recheck closes the insert visibility
  race, and absent new fields preserve pre-upgrade request fingerprints.
- Every plan-backed promotion, including the automatic owned path, locks the
  exact live plan and compares it with the durable task snapshot before PDS or
  YouTube. The plan remains locked through PDS, scheduling, and durable writes;
  missing, rejected, blocked, stale, or revision-mismatched authority holds or
  rejects before external effects. Existing external/manual evidence checks
  remain additive.
- Queue fences carry their authoritative locked channel into the transactional
  Store. Promotion locks channel -> task -> publication, then revalidates task
  channel and publication ownership before plan/PDS/YouTube, returning
  `ErrQueueAuthorityInvalid` on drift.
- The initial Python job launch reacquires fresh schedule -> job -> node state
  after every authority-releasing commit. The final queued-node check holds
  those locks through Redis dispatch, so quarantine-first cancellation cannot
  be revived or dispatched while dispatch-first may linearize before later
  cancellation. Worker-visible QUEUED state is committed before Redis emission.
- Provided idempotency keys are trimmed and blank/whitespace-only values raise
  before any durable effect. Truly absent keys remain one-shot requests.

### Changed Files

- Python runtime/schema: `backend/app/api/channel_agent.py`,
  `backend/app/autoflow/service.py`, `backend/app/orchestrator/engine.py`, and
  `backend/app/schemas/autoflow.py`.
- Python tests: `backend/tests/autoflow/test_execute_idempotency_postgres.py`,
  `backend/tests/channel_agent/test_api.py`, and
  `backend/tests/channel_agent/test_operator_quarantine_postgres.py`.
- Go runtime: `internal/channelops/autoflow_client.go`,
  `internal/channelops/execution_fence.go`, `internal/channelops/handlers.go`,
  `internal/channelops/human_review.go`, `internal/channelops/store.go`,
  `internal/channelops/store_publications.go`, and
  `internal/channelops/store_tasks.go`.
- Go tests: `internal/channelops/autoflow_client_test.go`,
  `internal/channelops/handlers_test.go`, and
  `internal/channelops/integration_test.go`.

### RED Evidence

- Caller-observed revision, response-loss replay, and whitespace key:
  `cd backend && CHANNEL_OPS_POSTGRES_TEST_URL=postgresql+asyncpg://vp:vp_test@127.0.0.1:55436/videoprocess .venv/bin/python3 -m pytest tests/autoflow/test_execute_idempotency_postgres.py -k 'observed_r1 or r1_response_loss or whitespace_idempotency' -q`
  -> `3 failed, 13 deselected`; R1 silently executed current R2, exact R1 retry
  rejected after R2, and a whitespace key created work.
- Python human-review task snapshot:
  `cd backend && .venv/bin/python3 -m pytest tests/channel_agent/test_api.py::test_human_review_release_approves_exact_plan_and_enqueues_execution -q`
  -> failed with missing `autoflow_plan_payload` authority.
- Initial dispatch race:
  `cd backend && CHANNEL_OPS_POSTGRES_TEST_URL=postgresql+asyncpg://vp:vp_test@127.0.0.1:55436/videoprocess .venv/bin/python3 -m pytest tests/channel_agent/test_operator_quarantine_postgres.py::test_quarantine_after_running_commit_prevents_stale_initial_dispatch -q`
  -> failed because the starter had no post-RUNNING authority boundary and did
  not reach the deterministic pause before stale dispatch.
- Go approval/execute contract:
  `go test ./internal/channelops -run 'TestHTTPAutoFlowApprovePlanPostsReviewNotes|TestHTTPAutoFlowExecuteTaskUsesTaskPlanID' -count=1`
  -> compile-time failure because `ApprovePlan` returned no observation.
- Automatic promotion and channel races:
  `DATABASE_URL=postgres://vp:vp_test@127.0.0.1:55436/videoprocess go test ./internal/channelops -run 'TestAutomaticOwnedPromotionRejectFirstPreventsPDSAndYouTube|TestAutomaticOwnedPromotionHoldsPlanLockThroughDurableWrites|TestPromotionRevalidatesFencedChannelAfterTaskScopeLock' -count=1`
  -> rejection did not block promotion, canonical invalidation did not wait on
  promotion, and enabled/halted channel-B reassignment reached the old path.
- Execute snapshot cross-check:
  `DATABASE_URL=postgres://vp:vp_test@127.0.0.1:55436/videoprocess go test ./internal/channelops -run 'TestHandleExecuteTaskRejectsMissingOrMismatchedDurableAuthorityBeforeAutoFlow' -count=1`
  -> both missing task authority and mismatched queue authority reached AutoFlow.
- Final self-review added two more RED replay regressions. The pre-upgrade
  fingerprint test failed with `AutoFlow execute idempotency key was already
  used for a different request`; the deterministic post-schedule visibility
  test failed with `AutoFlow expected approved revision does not match current
  plan authority` before rechecking the now-visible R1 row.

### GREEN And Full Verification

- Final focused execute command covering observed R1/current R2, committed R1
  after R2, delayed R1 visibility, legacy fingerprints, and whitespace keys ->
  `5 passed, 13 deselected in 1.01s`.
- Full execute file plus quarantine-after-RUNNING race ->
  `19 passed, 1 warning in 3.38s`.
- Focused Go automatic promotion, channel reassignment, execute cross-check,
  approval observation, and stable execute payload command -> passed in
  `0.846s`.
- Final PostgreSQL acceptance command:
  `cd backend && CHANNEL_OPS_POSTGRES_TEST_URL=postgresql+asyncpg://vp:vp_test@127.0.0.1:55436/videoprocess .venv/bin/python3 -m pytest tests/migrations/test_final_review4_postgres.py tests/migrations/test_final_review3_postgres.py tests/channel_agent/test_operator_quarantine_postgres.py tests/autoflow/test_execute_idempotency_postgres.py tests/channel_agent/test_api.py -q`
  -> `58 passed, 1 warning in 7.65s`.
- Full backend: `cd backend && .venv/bin/python3 -m pytest` ->
  `641 passed, 26 skipped, 11 warnings in 65.31s`; opt-in PostgreSQL tests are
  ordinary skips and passed separately above.
- An empty database migrated through `026_autoflow_authority_fence`. Against a
  second fresh migration-head database,
  `DATABASE_URL=postgres://vp:vp_test@127.0.0.1:55436/videoprocess go test ./... -count=1`
  -> all Go packages passed.
- Ruff over every changed Python file -> `All checks passed!`. Full Ruff remains
  the exact pre-existing baseline of 17 findings. `mypy app` remains the exact
  pre-existing baseline of 66 errors in 24 files.
- `bash tests/test_channelops_soak_watch.sh` ->
  `PASS: channelops soak watcher contract`;
  `bash tests/test_vp_deploy_sync_extension.sh` exited 0. The watcher, deploy
  extension, and image-smoke scripts all passed required `bash -n` checks.
- `git diff --check` and staged `git diff --cached --check` exited 0. `gofmt -l`
  returned no files. The staged boundary contained exactly the 17 files listed
  above and excluded the root plan.

### Lock And Replay Review

- Promotion lock order is channel -> task -> publication -> plan. The fenced
  channel is rechecked only after task/publication locks, so a channel-A fence
  cannot authorize a task that moved to enabled or halted channel B. Plan
  invalidation-first makes promotion hold with zero PDS/YouTube; promotion-first
  blocks invalidation until external and durable writes finish.
- New execute work owns schedule -> plan before reservation and effects. Replay
  performs an initial key lookup, then a second lookup after schedule
  serialization and before live-plan validation. Exact stored plan id,
  approved hash/revision, and request fingerprint return the original run;
  changed expected authority or behavior conflicts. Unique-key recovery still
  validates the same stored tuple.
- Initial dispatch owns schedule -> job -> ordered nodes for its final
  eligibility check and Redis emission, then commits immediately. It never
  holds those SQL locks for the video-job lifetime. A CLOSED/deferrable schedule
  parks work, and terminal/cancelled job or node state is never overwritten.

### PostgreSQL 16 And Cleanup

- Disposable container `vp-final-review-5-postgres`, id
  `33290b5ba46fdc07f1e01d1ae81f32582822d14745883eb4c8bd5d98dd5b23e6`,
  used `postgres:16-alpine` image id
  `sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777`.
- Server was PostgreSQL `16.14` on `127.0.0.1:55436`. The database was recreated
  from empty and migrated to head before final Go verification. The container
  was force removed, and an exact-name `docker ps -a` check confirmed absence.

### Concerns

- Full-tree Ruff and mypy remain nonzero only for the recorded pre-existing
  baselines: 17 Ruff findings and 66 mypy errors in 24 files. Changed-file Ruff
  is clean.
- Per the binding brief, the deployed-image smoke was not run before natural
  deployment. Its script syntax and local watcher/deploy contracts passed.

## Final Review 6 Consolidated Fix Wave

### Status And Scope

`DONE_WITH_CONCERNS`. Work remained on `codex/channelops-soak-guard` in the
required worktree and started from clean commit
`7c544459cd302564359d1cb777e2157cf75671d5`.

Implementation commit:

- `fc72056f84c38c9b32c53dfc06520d39f8fa0a5a` -
  `fix: fence multi-root initial dispatch`
- The report-only commit is listed in the final task response because this
  section was written after the implementation commit existed.

No deployment, push, production write, activation-state write, real external
network call, host 126 access, upload, publication, or edit to the user-owned
root `vp_autonomous_production_feedback_loop_plan.md` occurred.

### Implemented Fixes

- Guarded initial dispatch now invokes a per-candidate recheck boundary and
  reacquires schedule -> job -> ordered node locks before considering every
  root. It rebuilds the dependency map and node map from that freshly loaded
  collection before dependency, input, cache, or queue decisions. A closed
  schedule or cancelled/terminal job ends the scan without mutating the next
  root.
- The existing queue commit followed by a final schedule -> job -> node lock
  remains in place before Redis emission. This preserves worker-visible
  ordering and the valid dispatcher-first linearization while preventing a
  quarantine-first root from being revived by an ORM object retained across a
  prior root's commit.
- The four direct completion receives introduced in the reviewed Final Review
  5 Go race tests now use explicit `select` statements with
  `time.After(5 * time.Second)`. Both handler and canonical invalidation waits
  are bounded where applicable. Existing buffered completion channels and the
  bounded release/drain defer remain intact.

### Changed Files

- Runtime: `backend/app/orchestrator/engine.py`.
- PostgreSQL regression:
  `backend/tests/channel_agent/test_operator_quarantine_postgres.py`.
- Bounded Go race-test waits: `internal/channelops/integration_test.go`.

### Deterministic RED Evidence

Before changing runtime code, with PostgreSQL 16 at migration head 026:

`cd backend && CHANNEL_OPS_POSTGRES_TEST_URL=postgresql+asyncpg://vp:vp_test@127.0.0.1:55437/videoprocess .venv/bin/python3 -m pytest tests/channel_agent/test_operator_quarantine_postgres.py::test_quarantine_between_initial_roots_does_not_revive_second_root -q`

Result: `1 failed, 3 warnings in 0.60s`. Root A was the only Redis emission,
quarantine closed the schedule and cancelled the job/task/root B, then the old
loop committed its retained root-B object as `QUEUED`. The exact assertion was
`NodeStatus.QUEUED != NodeStatus.CANCELLED`.

The regression uses two independent roots and `asyncio.Event` boundaries. On
the old path it pauses at the real cache/queue boundary after root A's dispatch;
on the fixed path it pauses before root B reacquires authority. It then runs the
real quarantine transaction and resumes the real dispatcher. No sleeps or
eligibility stubs are used.

### GREEN And Full Verification

- The exact RED command after the runtime fix ->
  `1 passed, 2 warnings in 0.47s`.
- A shell loop reran that exact PostgreSQL regression 10 times -> all 10 runs
  passed (`0.47s` to `0.52s` each).
- Full operator-quarantine PostgreSQL file ->
  `6 passed, 3 warnings in 1.39s`.
- Focused reviewed Go races:
  `DATABASE_URL=postgres://vp:vp_test@127.0.0.1:55437/videoprocess go test ./internal/channelops -run 'TestAutomaticOwnedPromotionRejectFirstPreventsPDSAndYouTube|TestAutomaticOwnedPromotionHoldsPlanLockThroughDurableWrites|TestPromotionRevalidatesFencedChannelAfterTaskScopeLock' -count=1`
  -> passed in `0.811s`.
- Final Review 5 PostgreSQL acceptance set:
  `cd backend && CHANNEL_OPS_POSTGRES_TEST_URL=postgresql+asyncpg://vp:vp_test@127.0.0.1:55437/videoprocess .venv/bin/python3 -m pytest tests/migrations/test_final_review4_postgres.py tests/migrations/test_final_review3_postgres.py tests/channel_agent/test_operator_quarantine_postgres.py tests/autoflow/test_execute_idempotency_postgres.py tests/channel_agent/test_api.py -q`
  -> `59 passed, 3 warnings in 7.81s`.
- Full backend: `cd backend && .venv/bin/python3 -m pytest` ->
  `641 passed, 27 skipped, 11 warnings in 65.31s`. The added PostgreSQL test is
  one of the opt-in skips and passed explicitly above.
- The database was dropped, recreated empty, and migrated through
  `026_autoflow_authority_fence (head)`. Against that fresh database,
  `DATABASE_URL=postgres://vp:vp_test@127.0.0.1:55437/videoprocess go test ./... -count=1`
  -> all Go packages passed; `internal/channelops` completed in `5.494s`.
- Changed-file Ruff -> `All checks passed!`. Full Ruff remains the exact
  pre-existing 17-finding baseline. `mypy app` remains the exact pre-existing
  baseline of 66 errors in 24 files.
- `bash tests/test_channelops_soak_watch.sh` ->
  `PASS: channelops soak watcher contract`;
  `bash tests/test_vp_deploy_sync_extension.sh` exited 0.
- `bash -n` passed for `deploy/swarm/channelops-soak-watch.sh`,
  `deploy/swarm/deploy-sync-extension.sh`, and
  `tests/test_channelops_soak_image_smoke.sh`.
- `gofmt -l internal/channelops/integration_test.go` returned no files.
  `git diff --check` and staged `git diff --cached --check` exited 0, and the
  implementation commit contained exactly the three changed files listed
  above.

### PostgreSQL 16 And Cleanup

- Disposable container `vp-final-review-6-postgres`, id
  `7bcf42f871263927b6bd271da0d40d20c4a0c8b18f16376cc302dc5a53683b35`,
  used `postgres:16-alpine` image id
  `sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777`.
- Server was PostgreSQL `16.14` on `127.0.0.1:55437`. The container was force
  removed, and an exact-name `docker ps -a` check returned `absent`.

### Concerns

- Full-tree Ruff and mypy remain nonzero only for the recorded pre-existing
  baselines: 17 Ruff findings and 66 mypy errors in 24 files. Changed-file Ruff
  is clean.
- Per the binding brief, deployed-image smoke was not run and no deployment or
  external interaction occurred.

## Final Review 7 Test Cleanup Fix Wave

### Status And Scope

`DONE_WITH_CONCERNS`. Work started from clean
`a09e1f707e0bfaf4710dd0888e61cc54f1099d21` on
`codex/channelops-soak-guard`. The focused implementation/test commit is
`f8a8971942a7c6ab5d1f6a28f96ade2b5bb4a72b` (`test: bound channelops race cleanup`).
The report-only commit is recorded in the final task response.

No production, deployment, push, SSH, host 126 access, activation, upload,
publication, external PDS/YouTube call, or edit to the user-owned root plan
occurred.

### TDD Evidence And Fix

- RED, before the helper existed:
  `go test ./internal/channelops -run '^TestCancellableTestOperationCancelsAndDrains$' -count=1`
  failed to build with `undefined: startCancellableTestOperation`.
- GREEN after the smallest helper implementation: the same command passed in
  `0.421s`. The focused test starts a deliberately blocked operation, cancels
  it, proves the operation observed `context.Canceled`, drains its completion,
  and verifies completion within a 100ms test-controlled bound.
- The three reviewed races now launch handler/database work through cancellable
  test-operation contexts. Cleanup registration precedes every failure-capable
  lock/wait helper; it releases the artificial YouTube gate, cancels unfinished
  work, and drains each completion channel with a second five-second bound.
  Fixture cleanup, transaction rollback, and channel-authority restoration use
  fresh bounded cleanup contexts. The acquired invalidation connection is
  released only after both relevant operations drain. An undrained operation
  records a test error and prevents potentially unbounded fixture cleanup.
- `test_quarantine_between_initial_roots_does_not_revive_second_root` now wraps
  the real quarantine transaction in `asyncio.wait_for(..., timeout=5)` while
  retaining its `finally` release and bounded starter await. No orchestrator
  runtime behavior changed.

### Verification

- PostgreSQL 16 was migrated from empty to
  `026_autoflow_authority_fence (head)` in disposable container
  `vp-final-review-7-postgres` (ID
  `0d70b488d90f7debeff41ff3b157894732bfa1013b2af09568904cea3b39ba90`) on
  `127.0.0.1:55438`.
- Focused helper plus three reviewed PostgreSQL Go tests passed in `0.669s`.
  Full `DATABASE_URL=... go test ./internal/channelops -count=1` passed in
  `4.957s`; full `DATABASE_URL=... go test ./... -count=1` passed.
- The two-root Python PostgreSQL regression passed 10 consecutive runs; the
  full operator-quarantine file passed `6 passed, 3 warnings in 1.23s`.
  The Final Review 5/6 PostgreSQL acceptance set passed
  `59 passed, 3 warnings in 7.68s`.
- Full backend with the project virtualenv passed
  `641 passed, 27 skipped, 11 warnings in 65.28s`. Changed-file Ruff passed.
  Full Ruff and mypy remain their pre-existing baselines of 17 findings and 66
  errors in 24 files. The system `python3` is Python 3.14 without pytest,
  Ruff, or mypy installed; the required checks therefore used the repository
  `backend/.venv` after recording those missing-module results.
- `bash tests/test_channelops_soak_watch.sh`,
  `bash tests/test_vp_deploy_sync_extension.sh`, and required Bash syntax
  checks passed. `gofmt -l internal/channelops/integration_test.go` and
  `git diff --check` were clean before the implementation commit.
- The disposable PostgreSQL container was force removed and an exact-name
  `docker ps -a` check confirmed it is absent.

### Changed Paths

- `internal/channelops/integration_test.go`
- `backend/tests/channel_agent/test_operator_quarantine_postgres.py`

### Concerns

- Full Ruff and mypy retain only the documented pre-existing 17/66 baselines.
- The system interpreter lacks the project test/lint/type-check tools; the
  checked-in backend virtualenv supplied the authoritative full backend and
  static-check runs. Deployed-image smoke remains intentionally unrun before a
  natural deployment.

## Final Review 8 Cleanup Path Coverage Fix Wave

### Status And Scope

`DONE_WITH_CONCERNS`. Work started from clean
`d1bce62661df1ed8376b90cc78dd64c74c24a560` on
`codex/channelops-soak-guard` in the required worktree. The focused
implementation/test commit is `ea11a6c` (`test: cover channelops timeout
cleanup paths`). This report is committed separately.

No production, SSH, deploy, push, activation, upload, publication, external
platform call, host 126 access, or root plan edit occurred. The change touches
only test harness and regression files; no production runtime code changed.

### TDD Evidence And Cleanup Sequence

- RED, before the combined helper existed:
  `go test ./internal/channelops -run 'TestCancellableTestOperationTimeout(CancelsAndDrainsBeforeReturning|MarksFixtureCleanupIneligibleWhenUndrained)$' -count=1 -v`
  failed to build with missing `waitOrCancelAndDrain` and
  `fixtureCleanupEligible` symbols.
- GREEN after the smallest helper change:
  `go test ./internal/channelops -run 'TestCancellableTestOperation(CancelsAndDrains|TimeoutCancelsAndDrainsBeforeReturning|TimeoutMarksFixtureCleanupIneligibleWhenUndrained)$' -count=1 -v`
  passed all three tests. The cooperative case blocks during the initial wait,
  observes cancellation, and is drained before the timeout diagnostic returns.
  The cancellation-ignoring case consumes both the 30ms wait and 50ms drain
  bounds, makes fixture cleanup ineligible, then explicitly releases and drains
  its synthetic operation so the unit test leaks nothing.
- `waitOrCancelAndDrain` returns an ordinary operation result on normal
  completion. On the initial timeout it releases the blocker, cancels the
  operation, drains with the independent bound, then returns the initial-timeout
  diagnostic. A drain timeout sets the operation's permanent unsafe state;
  `fixtureCleanupEligible` rejects fixture cleanup after that state.
- The three reviewed PostgreSQL races now use the combined helper for their
  success waits, so cancellation and bounded draining occur before a timeout
  can reach `t.Fatal`. The stored unused operation context was removed. Lock
  waiters now accept a typed completion-probe callback: legacy completion
  channels are explicitly adapted and cancellable operations pass `tryWait`.
  The `any`/unsupported-type panic adapter is gone.
- In the two-root Python regression, the `try/finally` now begins immediately
  after starter task creation and covers the initial event wait, root-A
  assertion, and real bounded quarantine transaction. Every exit releases the
  starter and awaits it through `asyncio.wait_for(..., timeout=5)`, whose timeout
  cancels and awaits the starter task.

### Verification And Cleanup

- PostgreSQL `16.14` ran in isolated disposable container
  `vp-final-review-8-postgres` (`796f628b95a5e27e6a816f8a11884a065611f8edbb9d4874354e0b0dd7c31f7f`),
  bound only to `127.0.0.1:55434`. An empty database was migrated through
  `026_autoflow_authority_fence (head)`, then recreated from empty and migrated
  again before final Go verification.
- The three reviewed PostgreSQL Go races passed. Full
  `DATABASE_URL=... go test ./internal/channelops -count=1` passed in 5.005s;
  migration-head `DATABASE_URL=... go test ./... -count=1` passed.
- The two-root Python PostgreSQL regression passed 10 consecutive runs. The
  full operator-quarantine PostgreSQL file passed `6 passed, 3 warnings`; the
  Final Review 5-7 PostgreSQL acceptance set passed `59 passed, 3 warnings`.
- Full backend passed `641 passed, 27 skipped, 11 warnings` in 65.21s.
  Changed-file Ruff passed. Full Ruff retains 17 pre-existing findings and
  mypy retains 66 pre-existing errors in 24 files.
- Watcher and deploy contracts passed; required Bash syntax checks,
  `gofmt -l internal/channelops/integration_test.go`, and `git diff --check`
  were clean. Frontend `npm install`, build, and lint completed. Build emitted
  existing Lightning CSS, chunk-size, and dependency-audit warnings.
- The disposable container was removed with `docker rm -f`, and an exact-name
  `docker ps -a` check confirmed `vp-final-review-8-postgres` is absent.

### Changed Paths

- `internal/channelops/integration_test.go`
- `backend/tests/channel_agent/test_operator_quarantine_postgres.py`

### Concerns

- Full Ruff and mypy retain the documented pre-existing 17/66 baselines.
- Existing Python deprecation warnings and frontend build/audit warnings remain.
- Deployed-image smoke remains intentionally unrun before natural deployment.

## Final Review 9 Legacy Probe Cleanup Fix Wave

### Status And Scope

`DONE_WITH_CONCERNS`. Work began from clean
`fcb1994dbd992be5edf457c1b9563f65cb58b393` in the required worktree on
`codex/channelops-soak-guard`. The focused test-only commit is
`7a6d467` (`test: bound legacy direct promotion cleanup`). No runtime,
production, SSH, push, deploy, activation, external-platform call, upload,
publication, host-126 access, or root-plan edit occurred.

### Test Change And Focused Evidence

- Only `internal/channelops/integration_test.go` changed. The two named legacy
  direct-promotion races now use `cancellableTestOperation`, register bounded
  fixture cleanup before failure-capable lock waits, use bounded rollback, and
  wait with `waitOrCancelAndDrain(5*time.Second, testOperationCleanupTimeout)`.
- The direct plan-lock/durable-write test cancels and drains the handler and
  invalidation operation before releasing its acquired connection. The real
  PostgreSQL interleaving and all prior semantic assertions remain intact.
- `TestCancellableTestOperationTimeoutMarksFixtureCleanupIneligibleWhenUndrained`
  now verifies that fixture cleanup remains ineligible after the synthetic
  operation is released and drained. This focused test passed immediately:
  the existing helper already keeps `drainTimed` as a permanent unsafe marker,
  so no runtime implementation change or RED failure was appropriate.
- Helper plus both direct PostgreSQL races passed; the three automatic/channel
  races also passed.

### PostgreSQL And Verification

- Disposable `postgres:16-alpine` container `vp-final-review-9-postgres`
  ran PostgreSQL `16.14` on `127.0.0.1:55439`. An empty database migrated to
  `026_autoflow_authority_fence (head)`.
- Full `internal/channelops` passed. Full Go initially overlapped the
  operator-quarantine PostgreSQL suite and an unrelated learning-window test
  failed from shared database state; after recreating the database from empty
  migration HEAD and running in isolation, `go test ./... -count=1` passed.
- The two-root PostgreSQL regression passed 10 consecutive runs; the full
  operator-quarantine file passed `6 passed, 3 warnings`; the acceptance set
  passed `59 passed, 3 warnings`; and the backend virtualenv suite passed
  `641 passed, 27 skipped, 11 warnings`.
- Watcher/deploy contracts and required Bash syntax checks passed. `gofmt -l`
  and `git diff --check` were clean. Frontend `npm install`, build, and lint
  passed.
- Full Ruff retains the pre-existing `17` findings and mypy the pre-existing
  `66` errors in 24 files. System `python3` lacks pytest; the repository
  `backend/.venv` supplied the full backend/static verification.
- The PostgreSQL container was removed with `docker rm -f`; exact-name
  `docker ps -a` verification confirmed it is absent.

### Concerns

- Existing Python deprecation warnings, frontend Lightning CSS/chunk/audit
  warnings, and the documented Ruff/mypy baselines remain.

## Final Review 10 Distributed Side-Effect Closure

### Status And Scope

`DONE_WITH_CONCERNS`. Final Review 10 began at `4ff7f65` and produced the
local commits `298855b` (`fix: bind autoflow execution before start`),
`80bdcac` (`fix: serialize worker execution with quarantine`), and `52898d4`
(`fix: close distributed channelops side effects`). No push, SSH, deployment,
activation, production mutation, PDS write, YouTube call, upload, publication,
host-126 access, or external canary occurred in this wave. Public publication
remains unsupported by the new promotion operation and no canary approval was
inferred from the user's pause instruction.

### Durable Side Effects And Authority

- Alembic head `027_publication_promotion_operations` adds one durable
  operation per publication/queue attempt, with a stable attempt key and the
  states `reserved`, `submitting`, `confirmed`, `finalized`, and `uncertain`.
  Database and Go validation accept only `private` or `unlisted` targets.
- Promotion execution is split into reserve, external submission/reconcile,
  and fenced finalization phases. Retries query observed platform state before
  repeating any manager request. Response loss, contradictory state,
  unavailable status, and severe platform status all fail closed as
  `uncertain`; local finalization can converge without a second manager call.
- YouTubeManager receives the stable `Idempotency-Key` when scheduling. The
  implementation does not assume the external manager enforces it.
- AutoFlow execution now binds the durable run/pipeline/job to its production
  task before launch. Go replays AutoFlow even when the durable link already
  exists, and Python revalidates channel, schedule, task, exact plan revision,
  queue claim, and the exact `locked_by`/`locked_at` lease under row locks.
  Lease fields are excluded from the request fingerprint so a newly claimed
  retry can replay the same durable operation while a stale worker is rejected.
- Initial launch and terminal failure paths hold ordered job/node authority
  through their terminal writes. `RUNNING` jobs with stranded, unclaimed
  `QUEUED` nodes are idempotently redelivered. Launch handoff is awaited and
  shielded from caller cancellation; cancellation is re-raised only after the
  launch completes.
- PostgreSQL race cleanup now bounds task drain, rollback, close, and engine
  disposal. Any timeout permanently marks that fixture unsafe and prevents a
  later disposal or `TRUNCATE` while a connection may still be owned.

### TDD And Review Evidence

- RED tests reproduced: stale queue lease acceptance; lost `RUNNING` start
  handoff; stranded `QUEUED` root; non-awaited launch; rollback/close/dispose
  timeout eligibility; missing Go lease forwarding; severe removed-platform
  reconciliation; exhausted-failure/quarantine interleaving; and promotion
  response-loss/finalization windows.
- The final cancellation regression first failed because caller cancellation
  reached the child launch (`child_cancelled` was set). After shielding, both
  job-runtime tests passed, and the file passed 10 consecutive runs.
- The final recovery/lease/cleanup Python set passed 10 consecutive runs. The
  Go execute-handler/client set passed with `-count=10`. The six core durable
  promotion regressions also passed with `-count=10`.
- The existing reviewer reported no Critical findings, identified the lease,
  `RUNNING` recovery, cleanup timeout, and cancellation windows, then confirmed
  after fixes: `No Critical or Important findings.`

### PostgreSQL And Full Verification

- Disposable container `vp-final-review-10-postgres`
  (`87c2e22fa249`, `postgres:16-alpine`, PostgreSQL `16.14`) ran only on
  `127.0.0.1:55440`. Empty-database migration advanced from `001` through
  `027_publication_promotion_operations (head)`.
- The expanded PostgreSQL migration/authority/API/promotion acceptance bundle
  passed `84 passed, 11 warnings in 13.68s`. An earlier invocation from the
  repository root recorded `49 passed, 1 failed`; that failure was solely an
  existing relative-path contract reading `alembic/...` outside the required
  `backend/` working directory. The correctly rooted rerun passed.
- Final backend verification passed `646 passed, 49 skipped, 8 warnings in
  65.29s`. Changed-file Ruff passed. Full Ruff remains the exact pre-existing
  17-finding baseline; mypy remains 66 errors in 24 files across 144 sources.
- After dropping and recreating the database and migrating from empty to head,
  `DATABASE_URL=... go test ./... -count=1` passed every package;
  `internal/channelops` completed in `6.504s`.
- `tests/test_channelops_soak_watch.sh` and
  `tests/test_vp_deploy_sync_extension.sh` passed. Bash syntax passed for the
  soak watcher, deploy sync extension, and image-smoke scripts. `gofmt -l`
  returned no files, the race-task AST audit found 22 create-task sites with
  zero missing immediate cleanup scopes, and both staged and full-branch
  `git diff --check` passed. No frontend files changed in this wave.
- `docker rm -f vp-final-review-10-postgres` succeeded. An exact-name
  `docker ps -a` check confirmed the disposable container is absent.

### Concerns

- Full Ruff/mypy baselines and existing `datetime.utcnow()` deprecation
  warnings remain outside this scoped closure.
- The implementation is committed locally at `52898d4`; the following
  docs-only commit records this report. Neither commit is pushed or deployed
  while Codex/external actions are paused. Long-running production interaction
  and any further unlisted canary remain pending explicit resume/approval.
