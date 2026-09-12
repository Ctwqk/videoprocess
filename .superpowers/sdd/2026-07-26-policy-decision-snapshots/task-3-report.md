# Task3: Atomic Policy And Candidate Snapshot Persistence

## Status And Scope

- Base: `2523d06c1a699501ceb4765acdea4884f2f4ec9c`, initially tracked-clean.
- Worktree: `/Users/wenjieliu/videoprocess/.worktrees/policy-decision-snapshots`.
- Branch: `codex/policy-decision-snapshots`.
- Implementation commit: `606393e9aa62e0bf98a2e75fc929f8e3ded93f05`
  (`feat(channelops): persist complete decision snapshots`).
- This report is committed separately after the implementation commit; it does
  not change the tested runtime tree.
- Task1/schema044 and Task2 builders/build injection were reused, not redone.
- No schema, API, ranking, selection, deployment, CI, Dockerfile, ledger, or
  source outside this worktree was edited. No subagents, external network,
  SSH, Docker, PostgreSQL, Redis, push, deploy, activation, or publication was
  performed by this agent. Existing Go loopback fake HTTP tests were permitted.
- Task4 remains in the parent's independent worktree. Actual PostgreSQL and
  runtime-writer privilege qualification remain parent-owned and unclaimed.

## Implementation

1. `Store` privately captures the binary's `BuildCommitSHA` at construction.
   The actual `prepareTick` path invokes the existing strict baseline builder;
   development/empty identity cannot reach a persisted live tick. Tests use an
   explicit synthetic exact SHA without mutating the package global.
2. Preparation captures detached policy/features before PDS evaluation. All
   final decisions retain the source preparation's features, policy and asOf,
   including owned holds after input changes. Rebinding captured features to
   a different policy or time fails closed.
3. Snapshot asOf alone is truncated to PostgreSQL microsecond precision, once
   at capture. Existing task clocks and scheduling values are untouched. This
   avoids JSON timestamp rounding versus pgx binary timestamp truncation.
4. `ResolvePolicyVersion(ctx, db, policy)` inserts on conflict-do-nothing,
   then compares the complete stored policy content and hash/identity exactly.
   `InsertCandidateSnapshots(ctx, db, tickAuditID, policyID, snapshots)` checks
   uniqueness, hashes, policy/set/asOf identities and inserts every fact.
5. Normal success, dry-run, all-rejected/empty, owned success, owned hold, and
   seventh-item paths write pending audits, snapshots, decisions and unchanged
   tasks/reservations/queue effects in the existing final fenced transaction.
   Task IDs attach before immutable feature links are sealed. Cardinality and
   task/candidate linkage are checked before the final completion UPDATE.
6. Exact completed retries are no-ops only when policy, set, asOf, counts,
   decision/feature hashes, complete links, and summary/guard evidence match.
   Changed asOf, changed facts/decisions, pending collisions and legacy
   collisions fail closed. No historical facts are rebuilt from current inputs.
   The legacy low-level audit APIs remain available; their UPSERT is restricted
   to legacy-to-legacy updates, never completed snapshot rows.
7. Both Go and Python retention filter to `legacy_unreplayable` before deleting.
   Mixed-batch tests retain completed and pending ticks; snapshot rows remain.

### Bounded Evidence Decision

Schema044 has no separate PDS-request column, while Task2's decision hash covers
`PDSRequestJSON` and nullable/raw decision facts not fully represented by legacy
columns. New snapshot ticks add
`decision_summary_json.snapshot_decisions[candidate_id] = CandidateDecisionFacts`.
This immutable summary preserves the exact hash-covered decision evidence,
including owned hold requests with no task. Existing audit fields and task
payloads are unchanged. This additive use of the existing summary column was
raised to the parent during implementation. No schema or API expansion was used.

## Touched Files

- `internal/channelops/store_policy_snapshots.go` (new): persistence, validation,
  source-fact/decision combination, replay matching, sealing and completion.
- `internal/channelops/store_policy_snapshots_test.go` (new): offline boundary
  fakes exercising real preparation/finalization/store methods and failures.
- `internal/channelops/store_tasks.go`: detached preparation and normal finalizer.
- `internal/channelops/store_tick.go`: pending metadata, exact decision summary,
  guarded legacy UPSERT, duplicate/empty decision-ID rejection.
- `internal/channelops/store.go`: private, clone-preserved binary identity.
- `internal/channelops/owned_inventory.go`: success/hold wiring and complete-last.
- `internal/channelops/owned_inventory_test.go`: six owned paths, late failures,
  unchanged no-audit no-op.
- `internal/channelops/owned_inventory_postgres_test.go`: current-runtime head044,
  explicit test identity, snapshot assertions and rollback checks.
- `internal/channelops/integration_test.go`: confirmed disposable fixture cleanup
  and explicit test identity; all old semantic test bodies remain intact.
- `internal/channelops/policy_snapshots_postgres_test.go` (new): disposable-target
  safeguards, pool-close-safe cleanup test and 12-case real PostgreSQL matrix.
- `internal/channelops/cleanup.go`, `internal/channelops/cleanup_test.go`:
  legacy-only retention and mixed legacy/snapshot cases.
- `backend/app/channel_agent/retention.py`,
  `backend/tests/channel_agent/test_retention.py`: selective retention and SQLite
  mixed-batch verification including retained candidate features.
- This report. No ledger edit. `store_tick_test.go` and
  `owned_inventory_admission_test.go` needed no changes.

Two additional secondary-store fixtures were identified and raised:
`owned_inventory_admission_postgres_test.go` and `owned_inventory_python_test.go`.
They were NOT edited. The parent's exact-SHA test-binary ldflags below let those
stores inherit valid identity without scope expansion or package-global mutation.

## Fixture Ownership And Cleanup

`NewChannelOpsFixture` no longer calls `LoadConfig` to discover a default DB.
Before connection it requires an explicit `DATABASE_URL` accepted by one of:

- Existing CI contract: `GITHUB_ACTIONS=true`, `CHANNELOPS_REQUIRE_DATABASE=1`,
  `DATABASE_URL` exactly equals `CHANNEL_OPS_GO_POSTGRES_TEST_URL`, and that URL
  is exactly the postgres/postgresql loopback `postgres:postgres`, port5432,
  database `postgres` service URL already used by CI.
- Local parent contract: numeric loopback `127.0.0.1`, explicit port, no URL query
  or fragment, database prefix `vp_owned_inventory_test_`, exact database name in
  `POLICY_SNAPSHOTS_DISPOSABLE_CONFIRM`, and numeric independently observed
  cluster ID in `POLICY_SNAPSHOTS_DISPOSABLE_SYSTEM_ID`.

After connection it checks `current_database()` and exact migrated head044. For
the local contract it compares `pg_control_system().system_identifier` with the
parent's confirmation. It takes `pg_try_advisory_lock(774403030044)` in a separate
pgx connection. This rejects cooperating concurrent fixtures and does not hold
a Store-pool connection, so existing runner/pool-close tests remain usable.

Cleanup executes checked `TRUNCATE public.channel_profiles,
public.decision_policy_versions CASCADE` under that lock, followed by checked
cleanup of the existing fixed AutoFlow test-plan ID. This is intentionally
whole-disposable-fixture cleanup, NOT per-channel cleanup and NOT suitable for
an ambient/shared database. It does not disable, drop, replace, or relax any
production trigger. TRUNCATE is confined to the explicitly owned test database;
runtime retention still uses selective DELETE. Errors are reported, not ignored.
The parent must grant exclusive ownership for the entire test run; the advisory
lock is cooperative, not proof against unrelated clients that ignore it.

The existing owned fixture still uses its explicit disposable URL/name contract
and preserves facts while disabling its channel and settling mutable queue/task
state at teardown. It now requires044. The historical capacity-specific043 child
fixture remains unchanged. Actual native writer privileges are not inferred
from these disposable-owner tests.

## TDD Evidence

All Go commands below used the clean offline prefix in the next section.

| RED check | Observed RED | GREEN evidence |
| --- | --- | --- |
| `Test(ResolvePolicyVersion|InsertCandidateSnapshots|PreparedSnapshots|SnapshotCompletion)` | Build failed on missing Task3 methods/private identity. An initial duplicate test constant was corrected before the valid RED run. | Included in final focused run. |
| `tests/channel_agent/test_retention.py -q` | 1 failed: protected rows were deleted, `deleted_audits=3`, expected1. | 1 passed,0.20s after selective retention; retained feature row also verified. |
| `TestOwnedTickPersistsPolicySnapshots` | 6 subcases failed: success/seventh lacked pending state; four hold cases lacked completion. | All six pass in final focused/race runs. |
| `TestPreparedSnapshotsUseOnePostgresPrecisionAsOf` | 1 failed on nanosecond asOf versus expected microseconds. | Final focused/race pass. |
| `TestSnapshotAuditRetainsExactDecisionFacts` | 1 failed because hashed PDS request was not persisted. | Final focused/race pass; reconstructed decision hash matches. |
| `TestSnapshotAuditExactReplayAndConflict` | Exact-replay subcase failed because nil summary lost generated evidence. | Both exact/conflict subcases pass. |
| `TestPreparedSnapshotsRejectReboundPolicy` | 1 failed: source features could be rebound to another valid policy. | Final focused/race pass. |
| Normal completion/retention mutation check | Temporarily removing only the normal completion calls and Go legacy filter produced 4 normal-path failures plus the mixed-retention failure. | Calls/filter restored before final focused/full/race checks. |

Normal-path regression tests were added after the first shared writer wiring,
then explicitly proven RED by the bounded mutation check above. Additional
failure-injection tests cover each normal write boundary and owned reservation,
seed exhaustion, seventh-item pause, and completion failures. They assert that
errors propagate and completion is not emitted; actual transaction rollback is
reserved for the parent-run PostgreSQL cases, not claimed from a fake database.

## Final Offline Verification

Private directory `P` is
`/private/var/folders/4l/x_sl6fds6m18bx1tvhnvw22r0000gn/T/vp-capacity-parent.c9JmNz`
(confirmed mode0700). Prefixes used:

```bash
env -i \
  PATH=/Users/wenjieliu/videoprocess/backend/.venv/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin \
  HOME="$P" TMPDIR="$P" \
  GOMODCACHE=/Users/wenjieliu/go/pkg/mod \
  GOCACHE=/Users/wenjieliu/Library/Caches/go-build \
  GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local \
  DATABASE_URL=malformed://no-database \
  /opt/homebrew/bin/go ...
```

Backend commands used `env -i`, the same PATH/HOME/TMPDIR, and
`PYTHONDONTWRITEBYTECODE=1`, with NO DATABASE_URL, REDIS_URL or PG opt-in variables.
They used the existing root backend virtualenv and the worktree backend cwd.

| Command after prefix | Result |
| --- | --- |
| `go test -json -short ./internal/channelops -run 'Test(RunTick.*Snapshot|RunTickRejectsDevelopmentIdentity|OwnedTickPersistsPolicySnapshots|OwnedSnapshot|ResolvePolicyVersion|InsertCandidateSnapshots|PreparedSnapshots|Snapshot|CleanupExpiredPreservesMixed|BuildBaselinePolicy|BuildCandidateSnapshots|CanonicalPolicyHash)' -count=1` | PASS:78 test/subtest passes,29 top-level passes,1 PG fixture skip;0.424s package time. |
| `go test -json -short ./... -count=1` | PASS:1485 test/subtest passes,545 top-level passes,260 skips;14 packages pass,2 have no tests. ChannelOps5.532s. |
| `go test -json -race -short ./internal/channelops -count=1` | PASS:1004 test/subtest passes,287 top-level passes,208 skips;35.720s; no race report. |
| `python3 -m pytest` | PASS:3593 passed,562 skipped,62 warnings;135.93s. One full backend run. |
| `python3 -m ruff check .` | Exit1 advisory:11 pre-existing errors, unchanged count. No unrelated refactor. |
| `python3 -m mypy app` | Exit1 advisory:61 pre-existing errors in21 files,185 files checked; unchanged count. |
| `git diff --check` | PASS, exit0. |

Generated test logs: `$P/task3-go-focused.json`, `$P/task3-go-full.json`,
`$P/task3-go-race.json`, `$P/task3-backend-pytest.log`; advisory logs are recorded
alongside them. Short mode and missing opt-ins intentionally skip real PG/Redis,
contender bridges, live smoke and other integration-only cases.

## Parent PostgreSQL Qualification

NOT RUN BY THIS AGENT. Use only the parent's exclusively owned, already migrated
PG16 database with confirmed prefix/system ID. Do not point these commands at
any live or shared database. No CI/production-trigger changes are required.

From the frozen worktree, with the parent's confirmed values and the same
offline PATH/cache/HOME/TMPDIR prefix, supply:

```bash
DATABASE_URL="$CONFIRMED_DISPOSABLE_URL"
CHANNEL_OPS_GO_POSTGRES_TEST_URL="$CONFIRMED_DISPOSABLE_URL"
CHANNELOPS_REQUIRE_DATABASE=1
POLICY_SNAPSHOTS_DISPOSABLE_CONFIRM="$CONFIRMED_DATABASE_NAME"
POLICY_SNAPSHOTS_DISPOSABLE_SYSTEM_ID="$INDEPENDENTLY_OBSERVED_SYSTEM_ID"
OWNED_INVENTORY_DISPOSABLE_TEST_URL="$CONFIRMED_DISPOSABLE_URL"
OWNED_INVENTORY_DISPOSABLE_TEST_CONFIRM="$CONFIRMED_DATABASE_NAME"
```

Keep DATABASE_URL unset from backend offline invocations. The parent Go command
must omit `-short` and embed its frozen exact code SHA, especially for existing
secondary-store/native-principal/contender fixtures not edited by Task3:

```bash
/opt/homebrew/bin/go test \
  -ldflags "-X github.com/Ctwqk/videoprocess/internal/channelops.BuildCommitSHA=$FROZEN_SHA" \
  ./internal/channelops -count=1 -timeout=10m \
  -run 'Test(PolicySnapshotsPostgresAtomicMatrix|SnapshotFixtureCleanupAfterStoreClose|CleanupExpiredRemovesExpiredRowsAndCascadesDecisionAudit|RunTickWritesDecisionAudit.*|OwnedPG.*)'
```

`TestPolicySnapshotsPostgresAtomicMatrix` subcases:
`success`, `dry-run`, `rejected`, `empty`, `policy-conflict`, `duplicate`,
`missing-link`, `extra-decision`, `late-failure`, `legacy-conflict`, `exact-replay`,
`changed-replay`. These exercise pending/complete status, true stored-content
conflict, duplicate/missing/extra facts, post-sealing rollback, historical
non-rewrite, idempotency, immutable policy/tick/decision/candidate guards, nullable
scores/facts, and microsecond asOf consistency.

Existing owned cases strengthened without dropping semantic assertions include
`TestOwnedPGAtomicContendersAndCommittedResultLossReplay`,
`TestOwnedPGEmptyPlatformAliasDeniedBeforePDS`,
`TestOwnedPGPolicyFailureAndMutationHoldWithoutReplacement`, and
`TestOwnedPGRollbackIncludesReservationTaskSeedAuditAndQueue`.
`TestOwnedPGQueuedHandleAgentTickContenders` and existing owned producer cases
also inherit complete-snapshot assertions through the shared owned fixture.

After focused qualification, run the full existing `./internal/channelops`
functional suite with the same exact-SHA flag/confirmed disposable environment
and no `-run` filter. The parent's prepared `policy-snapshots-go` harness uses
`-v -count=1 -failfast -timeout=25m`, clean/exact-commit and source fingerprints,
and a no-external-network/source-write sandbox; this is the intended full-suite
qualification. Local confirmation does not require or spoof `GITHUB_ACTIONS`.
Keep separate explicitly opted-in native-principal/Redis/
Python bridge qualifications under their existing parent-controlled contracts.
In particular, writer privilege qualification requires the actual runtime role;
the disposable owner passing is not evidence of production role grants.

## Self-Review And Remaining Risks

- Reviewed the full brief/current-code rulings, Task1 migration, Task2 interfaces,
  all three audit write paths, task-link trigger ordering, final early returns,
  replay collisions, retention batching and fixture shutdown interactions.
- No subagent/reviewer was dispatched, per instruction. Parent independent
  review and actual PostgreSQL qualification remain required.
- PostgreSQL SQL/trigger/transaction behavior has NOT been executed here.
  The new opt-in cases compile and skip safely offline; the parent must qualify
  the frozen commit before any use. No live-readiness or writer-rights claim.
- Complete evidence is retained indefinitely in this phase; storage growth is
  intentional. Legacy retention stays unchanged apart from filtering.
- Strict replay equality includes source asOf. A fresh same-bucket preparation
  with a different asOf conflicts instead of silently creating additional tasks
  or relabeling old facts. Owned competing-admission no-audit no-ops are preserved.
- Read APIs, preflight, learned scoring, T12-T15 completion and activation are
  outside Task3 and not claimed.
