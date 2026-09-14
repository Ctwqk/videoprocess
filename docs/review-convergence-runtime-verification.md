# Review convergence runtime verification

Date: 2026-09-14

This report records local behavioral evidence only. No production deployment or
upload was performed.

## R10: upload timeout recovery

The verified behavior is an at-most-once upload fence. A durable `submitted`
operation with a canonical YouTubeManager task UUID resumes by querying
`GET /api/status/{task_id}` and does not issue another upload `POST`. An upload
whose POST outcome has no canonical task UUID remains `uncertain` and blocked;
manual reconciliation is the safe terminal outcome because there is no stable
provider identity to query.

The prospective recovery fix now preserves automatic query recovery after a
known-ID polling failure:

- `YouTubeUploadOperationStore._action_for()` returns `resume` only for
  `submitted` plus a canonical manager task UUID and returns `block` for every
  `uncertain` operation
  ([youtube_upload_operations.py](../backend/app/services/youtube_upload_operations.py#L1061-L1071)).
- The handler's resume branch validates the canonical UUID and goes directly to
  status polling, with no edge back to preflight, the submission fence, or POST
  ([youtube_upload.py](../backend/worker/handlers/youtube_upload.py#L145-L220)).
- Polling uses a monotonic overall deadline and bounded individual requests
  ([youtube_upload.py](../backend/worker/handlers/youtube_upload.py#L391-L420)),
  and a timeout or non-TLS network failure during the status
  GET now leaves the durable operation `submitted`
  ([youtube_upload.py](../backend/worker/handlers/youtube_upload.py#L422-L463),
  [youtube_upload.py](../backend/worker/handlers/youtube_upload.py#L508-L516)).
- Cancellation takes precedence when it races a recoverable polling failure.
  The same precedence applies when handler or drill cancellation becomes active
  after a successful pending response at the overall deadline.
  TLS failures found in the cycle-safe exception cause/context chain and HTTP
  protocol errors remain terminal `uncertain`
  ([youtube_upload.py](../backend/worker/handlers/youtube_upload.py#L401-L478),
  [youtube_upload.py](../backend/worker/handlers/youtube_upload.py#L615-L649)).
- A fresh claim therefore returns `resume`, rechecks the existing content,
  execution, production-task, and worker-registration authority, and makes only
  the status GET. It cannot enter preflight, submission fencing,
  `mark_attempting`, or the upload POST.

Focused command:

```text
backend/.venv/bin/python -m pytest -q \
  backend/tests/worker/test_youtube_upload_handler.py::test_known_manager_poll_failure_resumes_get_only_with_real_store

backend/.venv/bin/python -m pytest -q \
  backend/tests/worker/test_youtube_upload_handler.py
```

The new real-store regression is
`test_known_manager_poll_failure_resumes_get_only_with_real_store`, parameterized
for a polling deadline and transient GET transport failure. It proves the first
attempt durably reaches `submitted`, a fresh store claim selects `resume`, a
fresh handler completes from the same canonical task UUID, and the complete
request history contains exactly one upload POST. Its RED result was two
failures because both operations became `uncertain`/`block`; its GREEN result
was `2 passed in 0.49s`.

Two combined cancellation-plus-timeout/transport regressions first failed by
raising ordinary uncertainty while leaving `submitted`. Two TLS/protocol
regressions first failed because they also left `submitted`. The six-case GREEN
selection, including both ordinary recoverable cases, passed in `0.51s`. A
further pending-response-at-deadline regression exposed the same precedence gap
for the drill cancellation event; after using the common cancellation predicate
before polling, in the transient catch, and after every response, the
eight-case safety selection passed in `0.52s`. Final review also identified
GET write and close disconnects omitted from the initial retry allowlist.
Both failed the real-store resume assertion before the fix; the final test
covers deadline, read, write, connect, and close failures. The classifier uses
`httpx.NetworkError` with the same SSL exclusion, leaving protocol failures
terminal. The independent final eleven-case review selection passed.

The complete worker handler module result after the safety refinements was
`61 passed, 47 skipped in 0.71s`. The skipped cases are the
separately configured acknowledgment-drill PostgreSQL fixtures.

A focused store regression command covering ordinary submit/resume/replay,
blocked `uncertain`, and canonical manager-task validation passed `41` cases in
`0.59s`.

The known-ID and unknown-ID cases need distinct recovery contracts:

1. Unknown or noncanonical manager task ID: retain `uncertain` and `block`.
   A second POST is unsafe.
2. Canonical manager task ID with no receipt: allow only a freshly authorized
   status query, then persist the ordinary durable success or definite failure.
   Never run preflight, submission fencing, `mark_attempting`, or POST.

Cancellation, explicit interruption, TLS or protocol failure, non-success HTTP
status, malformed status or completion data, content drift, and receipt
persistence failure retain the existing terminal `uncertain` behavior. The
worker module regressions covering those branches remain green, including
`test_poll_failure_after_cancellation_remains_uncertain_and_blocked`,
`test_cancellation_precedes_pending_status_at_overall_deadline`,
`test_security_and_protocol_poll_failures_remain_uncertain_and_blocked`,
`test_cancellation_during_polling_marks_uncertain_without_output`,
`test_completed_result_requires_canonical_watch_url`,
`test_polling_404_remains_uncertain`, and
`test_resume_with_snapshot_hash_mismatch_marks_uncertain_without_http_or_output`.

No migration or `_action_for()` change was made. Known-ID `uncertain` rows that
predate this fix still resolve to `block`, because `mark_succeeded()` and the
registered-worker security-definer transition only accept `submitted`. Those
legacy rows require explicit operator reconciliation. Unknown or noncanonical
manager IDs also remain `uncertain` and blocked, as verified by
`test_ambiguous_submission_marks_uncertain_and_a_retry_never_posts_again` and
the store's canonical-ID regressions.

R10 conclusion: duplicate upload prevention, bounded request handling, and
automatic GET-only retry after prospective known-ID polling timeouts and
transport failures are verified. Legacy `uncertain` rows remain an explicit
manual-reconciliation boundary.

## R11: revision-bound approval and idempotency

The current API regression coverage binds approval to an exact execution
revision, invalidates approval when execution-affecting request, intent,
template, graph, candidates, metadata, validation, rights, target-platform, or
constraint fields change, preserves approval across true no-op writes, and
fails closed for legacy approved plans without a revision hash. Those cases are
in:

- `test_approved_plan_persists_exact_execution_revision_hash`
- `test_save_invalidates_approval_when_execution_revision_changes`
- `test_target_platform_and_constraint_patches_invalidate_exact_approval`
- `test_true_noop_patch_and_save_preserve_exact_approval`
- `test_legacy_approved_plan_without_revision_hash_fails_closed_until_reapproved`

The PostgreSQL concurrency suite adds exact revision and durable idempotency
coverage:

- `test_observed_r1_rejects_first_execute_after_r2_reapproval`
- `test_r1_response_loss_then_r2_exact_retry_returns_r1_execution`
- `test_r1_retry_rechecks_committed_key_before_live_r2_authority`
- `test_idempotency_key_cannot_be_reused_for_different_plan`
- `test_execute_first_holds_plan_authority_until_patch_commits_afterward`
- `test_reject_first_revokes_authority_before_execute_can_claim_it`

The focused local PostgreSQL command selected the first four listed concurrency
cases. Result: `4 skipped, 27 deselected in 0.31s`; each skip reports
`set CHANNEL_OPS_POSTGRES_TEST_URL for PostgreSQL idempotency tests`. The local
API revision and editing regressions also run in the full backend suite recorded
in [the final integration report](review-convergence-verification.md). This run
does not provide a fresh PostgreSQL race result.

## R12: bounded rollback and restart recovery

Focused Python command:

```text
backend/.venv/bin/python -m pytest -q \
  tests/test_failed_control_recovery.py \
  tests/test_registered_runtime_deploy.py \
  tests/test_worker_control_untouched_rollback.py \
  tests/test_worker_admission_transaction.py
```

Final result against integration base `78baa79`: `716 passed, 81 subtests passed
in 116.67s`. This expanded rerun includes the deployment transaction tests for
the independently integrated rollback fix.

The exercised restart and rollback behaviors include:

- `test_native_recovery_preserves_capture_through_real_transition_cas`
- `test_real_apply_failure_before_janitor_capture_reports_installed_baseline`
- `test_shell_reinstall_uses_actual_baseline_with_forward_authority_unchanged`
- `test_existing_rollback_control_fixtures`
- `test_native_capture_accepts_exact_snapshot_baseline_and_versioned_ancestry`
  including restart ancestry
- `test_rollback_rebinds_retained_migration_compatible_image_before_readiness`
- `test_real_owning_shell_lock_and_replay_boundary`
- `test_owning_shell_engine_failure_keeps_pin_and_never_retries`
- `test_atomic_update_acceptance_waits_for_convergence`
- `test_completed_start_first_waits_for_exact_single_healthy_task`
- `test_registered_waiter_does_not_confuse_observation_failure_with_success`
- `test_registered_capture_finishes_cleanup_before_returning_failure`
- `test_unsettled_registered_job_blocks_recovery_before_other_effects`
- `test_pending_signal_prevents_new_attempt_or_success_but_allows_cleanup`

The shell transaction regression also ran:

```text
bash tests/test_worker_admission_rollback.sh
```

Final result: `Ran 14 tests in 13.040s`, `OK`, followed by
`worker admission rollback transaction tests passed`. Its fixtures verify a
fresh-process replay of a durable rollback through retirement, preservation of
a failed rollback generation for restart, and recovery dispatch for
`PREPARING`, `FORWARD_APPLYING`, both rollback phases, all candidate-restore
phases, and `ABORTING`.

The production recovery dispatcher has an explicit maximum of 64 state-machine
iterations
([deploy-sync-extension.sh](../deploy/swarm/deploy-sync-extension.sh#L8270-L8279)).
Exhaustion returns failure for manual intervention. The registered runtime
engine also uses bounded Docker calls and a 120-second convergence deadline
([worker-admission-transaction.py](../deploy/swarm/worker-admission-transaction.py#L5283-L5324)).
This supports the repeated fail, rollback, restart contract: each invocation
replays a durable phase, cannot loop indefinitely in the dispatcher, and fails
closed when it cannot converge.

R12 is guaranteed at the durable state-machine boundary covered by these
fixtures. It is not a whole-script wall-clock guarantee. Some Bash helpers call
external Docker commands directly without a command timeout; for example,
`vp_worker_admission_image_commit()` invokes `docker image inspect`
([deploy-sync-extension.sh](../deploy/swarm/deploy-sync-extension.sh#L8962-L8968)).
The Python test harness timeouts bound the tests, not those production shell
processes. A hung Docker or SSH client can therefore outlive the state-machine
iteration limit. This is the recorded verification boundary and does not
justify a broad deployment refactor in this convergence change.

## Learning influence regression

`internal/channelops/learning_influence_test.go` previously inspected
`tick.go` source text for a pair of strings. It now calls the actual candidate
policy, supplies opposite learning recommendations and rewards, swaps those
learning contexts, and verifies that both accepted-candidate order and
selection remain unchanged.

Focused command:

```text
go test ./internal/channelops \
  -run '^TestLearningStateDoesNotAffectCandidateSelection$' -count=1
```

Result: `ok github.com/Ctwqk/videoprocess/internal/channelops 0.336s`.

Whole-worktree `git diff --check` passed after integration, including the
historical design metadata and these runtime changes.
