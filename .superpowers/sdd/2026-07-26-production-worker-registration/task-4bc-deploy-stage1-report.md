# Task 4B/4C Deploy Track B Stage 1 Report

Date: 2026-07-30

Baseline:
`89e0a43cf3bb8f151c7b592bd9bc83148141c3bb`

Worktree:
`/Users/wenjieliu/videoprocess/.worktrees/worker-registration`

Branch:
`codex/worker-registration`

## Status

Stage 1 implements the shared immutable-identity and durable transaction core,
and closes the Worker Track A I-R5-1 implementation breaker. It does not claim
that I-3 is closed end to end because forward/rollback phase advancement and
full crash reconciliation are explicitly reserved for Stage 2/3.

The marker-control fixture drift found during the initial Stage 1 verification
was corrected in a test-only follow-up. All required Stage 1 shell contracts
now pass.

No SSH, push, deploy, canary, YouTube/publication, remote access, or production
operation was performed. No real Swarm service or secret mutation was
performed.

## A. I-R5-1 Exclusive Ownership

### Root cause

One-shot operation records and bind sentinels had no exclusive live owner.
Normal HUP/INT/TERM cleanup was deferred to a later invocation, while an
overlapping invocation could reconcile and delete the first invocation's live
record and sentinel. The first invocation could then report failure after its
Docker side effect had already succeeded.

### Behavior RED

Commands:

```bash
bash tests/test_worker_admission_deploy.sh
```

Observed failures before implementation:

```text
FAIL: caller-trap TERM returned 1 instead of 143
FAIL: one-shot did not hold the exact regular mode-0600 transaction lock
FAIL: overlapping one-shot invocation did not fail or serialize
FAIL: SIGTERM did not reach the exact Docker boundary
```

The overlap fixture was corrected once to ensure the initial failure came from
concurrency behavior rather than a malformed fixture directory.

### Shared fix

- Added the exact lock
  `$DEPLOY_GITHUB_SYNC_ROOT/state/vp-worker-admission/transaction.lock`.
- The lock is a non-symlink regular file, mode `0600`, with descriptor/path
  identity checks and nonblocking exclusive `flock`.
- The shell keeps FD 19 open from stale reconciliation and operation
  preparation through Docker execution and exact postflight cleanup.
- Reentrant one-shots use the same lock; there is no second lock domain.
- HUP/INT/TERM kill and wait for the exact child, finish the exact
  operation/sentinel under the lock, restore caller traps, and return
  129/130/143.
- The framed payload pipeline records the final Docker boundary PID rather
  than an outer pipeline wrapper, preserves caller `pipefail`, and proves TERM
  reaches Docker before host cleanup and lock release.
- SIGKILL/power-loss residue is reconciled only after the next invocation
  acquires the same exclusive lock.
- A successful Docker side effect is not converted to failure by an overlapping
  cleanup.

### GREEN

```text
worker admission deployment contract tests passed
```

The passing matrix covers immediate HUP/INT/TERM cleanup, zero operation
record/sentinel/host credential residue, caller trap preservation, lock
symlink rejection, two overlapping invocations, and successful-side-effect
reporting.

### Files

- `deploy/swarm/deploy-sync-extension.sh`
- `deploy/swarm/worker-admission-transaction.py`
- `tests/test_worker_admission_deploy.sh`

## B/G. Durable Transaction and I-3 Replay Core

### Root cause

Worker, marker, and control promotion had no durable phase boundary.
Invocation startup could treat a still-active candidate as stale, mint a new
namespace, or attempt retirement after workers were committed but before
marker/control promotion.

### Behavior RED

Initial RED:

```text
FAIL: durable transaction PREPARING begin is unavailable
```

Self-review REDs added for required persistence/schema contracts:

```text
FileNotFoundError: .../<tx-id>/snapshots.json
FAIL: transaction reader accepted a duplicate service identity
```

### Shared fix

Added the stdlib-only `worker-admission-transaction.py` as the sole JSON
writer:

- Fixed `active.json`, `snapshots.json`, transaction directory, and
  `done.json` layout.
- Exact `0700` directories and `0600` regular files with owner, no-symlink,
  and descriptor identity checks.
- `tx-<32 lowercase hex>` IDs from a 128-bit CSPRNG.
- Canonical strict JSON with duplicate-key, unknown-field, enum, collection
  identity, and transition rejection.
- Atomic `O_CREAT|O_EXCL|O_NOFOLLOW` temp write, file fsync, identity-checked
  replace, and directory fsync.
- Reader `lstat`/open/`fstat` identity binding.
- Strict phases, split workers/marker/control promotion flags, retirement
  queue, operation-before-phase intent, deterministic replay plans, and DONE
  archive.
- Active transactions always disable stale cleanup and new candidate minting.
- A `WORKERS_PROMOTED` replay exposes marker promotion only; pending
  retirements remain hidden until `RETIRING`.
- A pending operation replays as exact identity verification rather than
  blindly repeating the mutation.
- Deploy entry acquires the same lock before transaction inspection and only
  begins or exactly resumes `PREPARING`. Any later phase fails before Docker
  with a Stage 2 reconciliation requirement.

### GREEN

```text
worker admission rollback transaction tests passed
```

The pure/fake tests replay the same journal twice and prove:

- no stale cleanup;
- no new candidate namespace;
- no retirement of the still-mounted candidate;
- deterministic next action after workers commit;
- operation intent survives an operation-before-phase crash;
- retirement is visible only in `RETIRING`;
- illegal transitions do not change the journal;
- DONE moves atomically to `done.json`.

### Explicit Stage Boundary

I-3 is not claimed closed end to end. Stage 1 records/resumes `PREPARING` and
provides phase/replay primitives, but does not advance the real forward or
rollback orchestration through those phases. A successful Stage 1 deploy
therefore leaves an active `PREPARING` transaction; another exact invocation
can resume it, while a different target is blocked pending Stage 2
reconciliation. No forward/rollback/cutover ordering was changed.

### Files

- `deploy/swarm/worker-admission-transaction.py`
- `deploy/swarm/deploy-sync-extension.sh`
- `tests/test_worker_admission_rollback.sh`
- `tests/test_vp_deploy_sync_extension.sh`

## C. I-4 Credential Identity and Principal

### Root cause

The four purpose-specific database credentials were validated independently
by pathname and mode, but aliases, hardlinks, and the same effective database
principal were accepted. Bind sites did not compare the live file identity to
the identity validated at transaction start.

### Behavior RED

```text
FAIL: four database purposes accepted one pathname
```

Additional RED fixtures covered a hardlink, a parent-symlink canonical alias,
principal mismatch/duplication, sanitized probe failure, and inode replacement
between validation and bind.

### Shared fix

- Added four required expected-principal settings.
- Captures canonical path, `(st_dev, st_ino)`, and expected principal for each
  purpose.
- Enforces pairwise-distinct canonical paths, inode identities, and
  principals.
- Runs a read-only, capability-dropped, mode-`0400` file-transport database
  probe using `SELECT session_user::text, current_user::text`.
- Requires exact canonical JSON and exact expected values for both returned
  principals.
- Suppresses database/driver error detail and emits only the stable
  `database_principal_probe_failed` error.
- Writes only non-secret purpose identity to `active.json`.
- Rechecks the exact credential identity immediately before every one of the
  four credential bind families.
- Principal mismatch fails before migration, secret creation, or worker
  service mutation.

No URL or credential payload is written to the journal, log, environment, or
container argv.

### GREEN

```text
tests/test_vp_deploy_sync_extension.sh: PASS
```

### Files

- `deploy/swarm/deploy-sync-extension.sh`
- `deploy/swarm/worker-admission-transaction.py`
- `tests/test_vp_deploy_sync_extension.sh`

## D. I-6 Immutable Secret Identity

### Root cause

Creation validated labels, but worker/control manifests and retirement
journals retained only secret names. Destructive cleanup inspected and removed
by name, so name reuse could delete a replacement secret.

### Behavior RED

```text
FAIL: immutable managed-secret removal helper is unavailable
```

The RED matrix covered exact match, label mismatch, immutable-ID mismatch,
same-name replacement, worker v1 hydration, control journal v2, marker secret
identity, and partial cleanup.

### Shared fix

- Added one shared `vp_managed_secret_id` identity reader and
  `vp_remove_managed_secret` destructive primitive.
- Every remaining `docker secret rm` in the extension is inside that helper.
- The helper performs a final inspect by immutable ID, compares ID, name,
  service, generation, and purpose, then immediately runs
  `docker secret rm <ID>`.
- Worker manifests and retirement records are v2 with database/admission
  secret IDs.
- Control manifests and retirement journals are v2 with all seven immutable
  secret IDs.
- Historical worker/control v1 state is upgraded only after strict live
  ID/label hydration; otherwise it is retained and requires manual evidence.
- New marker DB secrets carry exact service/generation/purpose labels and have
  a durable v2 ID manifest.
- Historical unlabeled marker secrets are retained with
  `legacy_unretirable` manual-review evidence; they are never name-deleted.
- Role/grant revocation precedes secret removal.

### GREEN

```text
worker admission deployment contract tests passed
```

Static destructive inventory:

```text
deploy/swarm/deploy-sync-extension.sh: docker secret rm "$secret_id"
```

This single call is the final line of the shared ID-bound helper.

### Files

- `deploy/swarm/deploy-sync-extension.sh`
- `tests/test_worker_admission_deploy.sh`

## E. I-8 Staging Janitor Service Identity

### Root cause

The launcher validated a fixed service name, inspected task state separately,
then removed by name. A replacement could appear between validation and
removal.

### Behavior RED

```text
FAIL: replacement-name race retired the original service
```

### Shared fix

- Initial validation returns the immutable service ID.
- Task inspection is by that ID.
- Terminal handling performs one final full name/labels/generation/spec
  validation and requires the same ID.
- The immediately following destructive operation is
  `docker service rm <ID>`.
- Absence convergence is checked by ID.
- A same-name replacement is retained and the launcher fails closed.
- Existing holder/evidence pin behavior remains unchanged.

### GREEN

```text
staging object janitor launcher tests passed
```

### Files

- `deploy/swarm/staging-object-janitor-run.sh`
- `tests/test_staging_object_janitor_run.sh`

## F. I-10 Retirement Response Binding

### Root cause

The response parser checked field shape and UUID syntax but did not compare
the response's service/generation identity to the request.

### Behavior RED

```text
FAIL: retirement response accepted a mismatched service
```

### Shared fix

The parser receives the expected service and integer generation, validates
every field and every UUID into memory, compares both expected identities, and
only then emits UUIDs. A mismatch has empty stdout and cannot reach
`revoke-registration`.

### GREEN

```text
worker admission deployment contract tests passed
```

### Files

- `deploy/swarm/deploy-sync-extension.sh`
- `tests/test_worker_admission_deploy.sh`

## Contract Preservation

- Worker and janitor secret descriptors remain
  `uid=10001,gid=10001,mode=0400`.
- Input credentials still use stdin to container tmpfs; no durable secret copy
  was restored.
- Controlled directories remain no-follow and exact-identity checked.
- No recursive `chown` was introduced.
- Existing image identity and holder/evidence pin contracts remain blocking.
- Default publication/privacy behavior was not touched.
- No backend worker, Go, Dockerfile, CI, Task 4A API/CLI, or frontend file was
  modified.

## Verification

Fresh check record before report completion:

| Check | Result |
| --- | --- |
| `bash -n` for changed shell production files | PASS |
| `python3 -m py_compile deploy/swarm/worker-admission-transaction.py` | PASS |
| `backend/.venv/bin/ruff check deploy/swarm/worker-admission-transaction.py` | PASS |
| `backend/.venv/bin/mypy deploy/swarm/worker-admission-transaction.py` | PASS |
| `bash tests/test_worker_admission_deploy.sh` | PASS |
| `bash tests/test_worker_admission_rollback.sh` | PASS |
| `bash tests/test_staging_object_janitor_run.sh` | PASS |
| `bash tests/test_vp_deploy_sync_extension.sh` | PASS |
| `bash tests/test_staging_object_janitor_install.sh` | PASS |
| `bash tests/test_worker_redis_marker_control.sh` | PASS after test-only immutable identity fixture follow-up |
| destructive secret inventory | PASS, one ID-bound helper call |
| `git diff --check` | PASS |
| changed-path allowlist | PASS |

The system Python environment does not contain the `ruff` module; the
repository's backend virtual environment was used and passed.

## Marker Fixture Follow-up

### Preserved RED

The coordinator and the initial Stage 1 verification both reproduced:

```text
worker marker database secret creation failed
FAIL: generation-scoped database secrets were not created
```

### Root cause

The fake Docker treated argument three of `docker secret create` as the secret
name. The production command now supplies three `--label` pairs before the
name, so the fake created a name-only empty file, returned no immutable ID on
stdout, and returned no `.ID`, `.Spec.Name`, or `.Spec.Labels` identity from
`docker secret inspect`.

### Test-only fix

- The fake parses the real `docker secret create` option/name/source shape.
- It persists a JSON object with exact Docker API fields:
  `ID`, `Spec.Name`, and `Spec.Labels`.
- The labels must be exact `vp.service`, `vp.generation`, and `vp.purpose`
  values supplied by production.
- Create returns a stable, distinct 32-hex immutable ID on stdout.
- Inspect resolves either name or immutable ID and renders the exact formatted
  identity requested by `vp_managed_secret_id`.
- Remove accepts immutable ID, resolves the saved name for existing lifecycle
  assertions, and deletes only that exact fake object.
- Existing marker readiness, rollback, rotation, partial cleanup, and
  freshness assertions remain unchanged and blocking.
- New adversarial probes tamper the purpose label and immutable ID separately.
  Both return nonzero, retain the secret evidence, and make zero
  `docker secret rm` calls. Restoring the exact JSON identity makes strict
  inspection succeed again.

No production file was changed in this follow-up.

### GREEN

```text
worker Redis marker control tests passed
```

### Files

- `tests/test_worker_redis_marker_control.sh`
- `.superpowers/sdd/2026-07-26-production-worker-registration/task-4bc-deploy-stage1-report.md`

## Unfinished Boundaries

The following work is deliberately not implemented or claimed in Stage 1:

- I-3 end-to-end forward reconciliation and real phase advancement;
- first-deploy/legacy baseline rollback orchestration;
- failed-rollback candidate restoration;
- operation-by-operation worker marker gates;
- vision cutover secret transport and mutation boundaries;
- fixed-name marker job immutable service-ID conversion;
- janitor install recovery;
- full crash replay and transaction snapshot population during real
  forward/rollback mutations;
- production deploy, canary, or cutover.

These are Stage 2/3 concerns and must remain blocked until independently
reviewed and implemented under their own TDD scope.
