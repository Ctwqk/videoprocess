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

## Fix Round 1 (Independent Review I-1 through I-6)

Round start:
`a42322a2d232d1dac67fb76754c6c6d00999bc6d`

The six Important findings in
`task-4bc-deploy-stage1-review.md` were handled independently with
behavior-level REDs before their shared production mechanisms were changed.

### I-1. Inherited Outer-Lock Ownership

Root cause:

The outer deploy's FD 19, lock globals, and flock open-file description were
inherited by subshell/background children. The old reentrant check therefore
could not distinguish same-shell recursion from a child attempting a new
one-shot operation.

RED command and output:

```text
$ bash tests/test_worker_admission_deploy.sh
FAIL: inherited child was accepted as the outer lock owner
```

Shared fix:

- Lock ownership now records the current `BASHPID` and exact lock
  `st_dev:st_ino` token.
- Bash versions without `BASHPID` capture the current shell process through a
  short-lived child's exact `PPID`.
- Same-shell recursion remains depth-counted.
- An inherited child closes its copied FD and clears copied owner state before
  attempting a new lock acquisition. The live parent's exclusive lock then
  blocks it before reconciliation, Docker, record, or sentinel mutation.
- Lock assertion revalidates current-shell ownership and the exact FD/path
  identity.

GREEN command and output:

```text
$ bash tests/test_worker_admission_deploy.sh
worker admission deployment contract tests passed
```

The new fixture holds the production outer deploy lock, starts the first
one-shot, and attempts a nested one-shot from its fake Docker child. The second
call cannot reconcile or remove the first operation's live record/sentinel.

### I-2. Signal Launch Window and Canonical Outer Status

Root cause:

HUP/INT/TERM could run after `docker ... &` but before `$!` was published.
That handler had no child PID to forward/wait. Nested `|| return 1` paths also
collapsed a one-shot's canonical signal status before the real outer deploy
could observe it.

RED command and output:

```text
$ bash tests/test_worker_admission_deploy.sh
FAIL: post-spawn TERM was not forwarded after PID publication
```

Shared fix:

- A pending-signal trap is installed before launch.
- Before PID publication, the first signal records only its canonical status
  and name.
- Immediately after `$!` publication, full forwarding/cleanup traps are
  installed and any pending signal is consumed through the full handler.
- The full handler forwards to and waits for the exact child before
  descriptor/identity-bound operation cleanup.
- The first 129/130/143 status is also written to outer deploy shared state,
  independently of intermediate command-return folding.
- Both one-shot and outer deploy traps preserve and restore caller traps.

GREEN command and output:

```text
$ bash tests/test_worker_admission_deploy.sh
worker admission deployment contract tests passed
```

The GREEN matrix includes deterministic immediate post-spawn delivery and
nested control-role calls under the real outer deploy trap, with canonical
HUP/INT/TERM statuses 129/130/143.

### I-3. Credential Record Binding

Root cause:

The first four-file validation result was discarded. Principal probes and
transaction begin could therefore capture different inodes, allowing a
replacement credential that had never passed the expected-principal probe.

RED command and output:

```text
$ bash tests/test_vp_deploy_sync_extension.sh
FAIL: principal probe accepted an in-flight inode replacement
```

A second RED replaced a path after all probes but before `begin`; the old
implementation accepted the replacement as the new transaction identity.

Shared fix:

- One canonical JSON record set captures each purpose's canonical path,
  device, inode, mode `0400`, and expected principal.
- Pairwise path/inode/principal validation runs once before probes.
- Every probe compares its exact original record both before and after Docker.
- `begin` and `verify-preparing` consume that original record set on stdin and
  verify it in place; neither API recaptures a new authoritative set.
- Transaction-bound credential bind helpers continue to compare against the
  same persisted records.
- Outer deploy clears the operation-local record set on entry and exit.

GREEN command and output:

```text
$ bash tests/test_vp_deploy_sync_extension.sh
<exit 0; no stdout>

$ bash tests/test_worker_admission_rollback.sh
worker admission rollback transaction tests passed
```

No database URL, password, or driver error representation is persisted or
placed in argv/environment.

### I-4. Durable Partial Secret Identity

Root cause:

Worker/control preparation held inspected IDs only in shell locals until every
secret had been created. A later prefix failure left only a name-only v1
manifest, and candidate-capture failure short-circuited rollback entirely.

RED commands and outputs:

```text
$ bash tests/test_worker_admission_deploy.sh
KeyError: 'prepared_secrets'
FAIL: first immutable secret ID was not durably journaled

$ bash tests/test_worker_admission_deploy.sh
FAIL: candidate capture failure short-circuited rollback

$ bash tests/test_worker_admission_deploy.sh
FAIL: worker partial manifest did not resume through durable IDs
```

Shared fix:

- The strict active transaction now contains `prepared_secrets`, with unique
  name, immutable Docker ID, and logical
  `(service,generation,purpose)` identity.
- After each create's adjacent exact inspect, that single reference is
  atomically persisted before the next create starts.
- Replay looks up by the full logical tuple, performs a final inspect by
  immutable ID, and neither mints again nor guesses by name.
- Worker v1 partial candidates resume through the durable transaction record.
- Control retry preserves existing v2 manifest IDs and rejects any replayed
  ID mismatch instead of downgrading the manifest to v1.
- Candidate-capture failure always invokes rollback. Incomplete candidate
  evidence is preserved, while marker/control final retirement is blocked.

GREEN command and output:

```text
$ bash tests/test_worker_admission_deploy.sh
worker admission deployment contract tests passed
```

The fake sequence creates and inspects secret one, fails secret two, observes
secret one's ID in `active.json`, and then resumes without another create or
name lookup for secret one.

### I-5. Relational Transaction Schema

Root cause:

Promotion validation enforced only `control => marker => workers`, not the
exact state legal for each phase. Pending retirements were unique by
retirement UUID and Docker ID, but not by logical schema identity.

RED command and output:

```text
$ bash tests/test_worker_admission_rollback.sh
RED: relational schema accepted preparing-all-promoted
RED: relational schema accepted skipped-marker-promotion
RED: relational schema accepted rollback-promotion-contradiction
RED: relational schema accepted duplicate-logical-retirement
FAIL: relational schema accepted 4 contradictions
```

Shared fix:

- Every forward, rollback, candidate-restore, retirement, and DONE phase has
  one explicit allowed `(workers,marker,control)` tuple.
- The common canonical document validator compares the exact tuple on every
  read. All mutation, intent completion, replay, and archive paths already
  pass through that same validator before writing or acting.
- Pending retirements now require uniqueness by both immutable Docker ID and
  logical `(service,generation,kind,purpose,name)` key.

GREEN command and output:

```text
$ bash tests/test_worker_admission_rollback.sh
worker admission rollback transaction tests passed
```

### I-6. Historical v1 Retirement Hydration

Root cause:

The retirement processor sent exact historical four-field records directly to
the six-field v2 retire parser. It failed closed without attempting strict live
identity hydration, permanently blocking otherwise recoverable journals.

RED command and output:

```text
$ bash tests/test_worker_admission_deploy.sh
FAIL: valid v1 retirement journal did not recover
```

Shared fix:

- The processor first validates the whole journal as exclusively exact
  four-field v1 or exclusively six-field v2.
- For v1, each database/admission name is inspected once with exact
  service/generation/purpose labels.
- All returned immutable IDs must be nonempty and pairwise distinct across the
  entire journal.
- Only after every identity validates does an owner/mode/fd-bound writer
  create a mode-`0600` temporary file, fsync it, identity-check and replace the
  journal, then fsync the directory.
- Current-generation protection and retirement run only from the rewritten
  six-field record set.
- Wrong labels, same-name replacement generation, or duplicate IDs retain the
  original journal byte-for-byte and make zero `docker secret rm` calls.

GREEN command and output:

```text
$ bash tests/test_worker_admission_deploy.sh
worker admission deployment contract tests passed
```

The valid fixture verifies the six-field journal is present before retirement,
then observes final inspect-by-ID immediately followed by rm-by-ID.

### Round 1 Files

- `deploy/swarm/deploy-sync-extension.sh`
- `deploy/swarm/worker-admission-transaction.py`
- `tests/test_worker_admission_deploy.sh`
- `tests/test_worker_admission_rollback.sh`
- `tests/test_vp_deploy_sync_extension.sh`
- `.superpowers/sdd/2026-07-26-production-worker-registration/task-4bc-deploy-stage1-report.md`

No production marker-control, janitor-install, cutover, backend worker, Go,
Dockerfile, CI, Task 4A, or frontend file changed in this round.

### Round 1 Verification

```text
bash tests/test_worker_admission_deploy.sh
  PASS: worker admission deployment contract tests passed
bash tests/test_worker_admission_rollback.sh
  PASS: worker admission rollback transaction tests passed
bash tests/test_staging_object_janitor_run.sh
  PASS: staging object janitor launcher tests passed
bash tests/test_vp_deploy_sync_extension.sh
  PASS: exit 0, no stdout
bash tests/test_staging_object_janitor_install.sh
  PASS: staging object janitor installer tests passed
bash tests/test_worker_redis_marker_control.sh
  PASS: worker Redis marker control tests passed
python3 -m py_compile deploy/swarm/worker-admission-transaction.py
  PASS
backend/.venv/bin/ruff check deploy/swarm/worker-admission-transaction.py
  PASS: All checks passed!
backend/.venv/bin/mypy deploy/swarm/worker-admission-transaction.py
  PASS: Success: no issues found in 1 source file
bash -n <changed production/test shell files>
  PASS
destructive inventory
  PASS: no new docker secret/service rm call in this diff;
        secret removal remains adjacent exact inspect then rm by immutable ID
git diff --check
  PASS
changed-path allowlist
  PASS
```

The system Python still has no `ruff` or `mypy` module; the repository backend
virtual environment supplied both successful checks.

### Round 1 Unfinished Boundary

This round does not wire production forward/rollback phase advancement,
cutover, marker mutation boundaries, first-deploy/failed-rollback
orchestration, or janitor install recovery. In particular, it does not claim
I-3 end-to-end closure. The existing Stage 1 behavior remains: exact
`PREPARING` can begin/resume, while any later active phase blocks with the
Stage 2 reconciliation requirement.

No SSH, push, deploy, remote access, YouTube/canary operation, or real Swarm
service/secret mutation was performed.

## Fix Round 2

Start HEAD: `1c375147ee8ef8748644dd3321602425cb92dcd8`.

### Open Important 1: Legitimate Lock-Owner Calls

Root cause:

The inherited-child guard correctly rejected a different `BASHPID`, but
production retirement called generation-state, retirement-ID, and v1
hydration helpers through command substitutions. Those subshells inherited fd
19 and the lock globals but were not the owner shell, so their nested
`vp_run_python_worker_container` calls failed before retirement could run.

RED command and output:

```text
$ bash tests/test_worker_admission_deploy.sh
FAIL: outer-lock retirement command substitution was rejected
```

Shared fix:

- Generation-state and retirement-ID one-shots now execute directly in the
  lock-owning shell.
- Their stdout goes to a caller-owned regular mode-`0600` file under the
  admission root. An fd-identity-bound parser accepts only canonical,
  exact-schema JSON and publishes sanitized globals; stderr never joins the
  payload.
- Retirement and v1 hydration callers consume explicit globals instead of
  invoking mutating helpers in `$(...)`.
- The production outer-lock fixture exercises real v1 hydration through
  generation-state, retirement candidates, authority revoke, and immutable-ID
  removal. The hostile inherited-background-child fixture remains unchanged
  and still proves zero Docker/record/sentinel mutation.

GREEN command and output:

```text
$ bash tests/test_worker_admission_deploy.sh
worker admission deployment contract tests passed
```

### Open Important 2: Signal Stop Semantics

Root cause:

The wrapper initially installed the full handler, later cleared pending state,
and only then installed the pending launch handler. A signal in that interval
could clean the operation, return to normal control flow, and still launch
Docker. The outer deploy trap recorded a signal but did not gate later phases,
so a validation function that caught a signal and returned zero could continue
into preparation and mutation.

RED commands and outputs:

```text
$ bash tests/test_worker_admission_deploy.sh
FAIL: pre-handler TERM continued into Docker

Focused baseline outer continuation:
deploy_status=143
calls=validate-before-signal,validate-after-signal,prepare-after-signal,mutation-after-signal
```

Shared fix:

- The one-shot wrapper installs a first-signal pending handler before
  admission-root preparation or lock acquisition.
- Once descriptor-bound operation identity exists, it switches once to the
  full handler. Before child PID publication the full handler only records
  pending state; after publication it forwards the first signal, waits for the
  exact child, and performs identity-bound cleanup.
- A common `vp_worker_admission_raise_if_signaled` gate runs before operation
  preparation, immediately before launch, after wait, and at outer validation,
  transaction-preparation, and mutation boundaries.
- Signal state is not reset between full and pending modes. Canonical
  HUP/INT/TERM statuses remain in outer shared state even when nested callers
  use `|| return 1`.
- Caller traps remain installed only outside the wrapper and are restored after
  lock/operation cleanup.

GREEN command and output:

```text
$ bash tests/test_worker_admission_deploy.sh
worker admission deployment contract tests passed
```

The GREEN suite covers pre-launch TERM with zero Docker, post-spawn forwarding,
exact record/sentinel cleanup, nested control-role calls, caller-trap
preservation, and validation-internal HUP/INT/TERM returning canonical
129/130/143 without entering preparation or mutation.

### Open Important 3: Partial-Secret Abort

Root cause:

`prepared_secrets` supported record and exact-ID resume, but PREPARING had no
durable abort phase, operation-before-delete intent, per-secret record
completion, authority cleanup queue, or aborted DONE archive. Permanent
preparation failure therefore retained active state forever; the
`preserve_incomplete=true` restore branch explicitly skipped cleanup.

RED commands and outputs:

```text
$ bash tests/test_worker_admission_rollback.sh
exit 1 (begin-abort/list/intent/remove-record/finish-abort CLI absent)

$ bash tests/test_worker_admission_deploy.sh
tests/test_worker_admission_deploy.sh: line 1874:
  vp_worker_admission_abort_preparing_transaction: command not found
FAIL: control partial-secret prefix 1 did not abort

$ bash tests/test_worker_admission_deploy.sh
FAIL: permanent PREPARING restore did not select durable abort
```

Shared fix:

- The strict transaction schema adds `ABORTING`, `DONE` outcome `aborted`, and
  durable abort evidence containing the failure reason and unique provisioned
  control/runtime authority identities.
- `begin-abort`, `list-abort`, `intent-prepared-secret-removal`,
  `complete-prepared-secret-removal`, `complete-abort-authority`, and
  `finish-abort` all require the shared writer lock, expected revision, common
  relational validator, and atomic active-journal rewrite.
- Prepared secrets are selected in reverse creation order. Before each removal,
  all service IDs are enumerated and their exact mounted SecretIDs checked.
  The final exact ID/name/service/generation/purpose inspect is immediately
  followed by `docker secret rm <immutable-ID>`.
- A successful rm atomically removes its prepared-secret record. If the
  process dies after rm but before record completion, the persisted operation
  permits replay to prove exact-ID absence via manager listing and complete
  the record without a second rm.
- Existing-but-mismatched ID or labels, mounted references, inspect/list
  failures, and rm failures retain active evidence and perform no guessed
  name removal.
- After all secret records are gone, durable authority entries are revoked in
  reverse order through the shared control/runtime revoke primitives. Only an
  empty secret list, empty authority list, and no pending operation may enter
  `DONE(aborted)` and archive `active.json`.
- The permanent PREPARING restore path now invokes this abort even when
  candidate capture is incomplete. Same-commit PREPARING remains resumable;
  same-commit ABORTING or unarchived aborted DONE is completed before any new
  transaction begins.
- One-shot execution no longer clears the original four captured credential
  records; begin/verify-preparing continue to consume the same records.

GREEN commands and outputs:

```text
$ bash tests/test_worker_admission_deploy.sh
worker admission deployment contract tests passed

$ bash tests/test_worker_admission_rollback.sh
worker admission rollback transaction tests passed
```

Behavior coverage includes every control prefix 1 through 7, each worker prefix
1 through 2, reverse deterministic removal, service-unused proof, adjacent
inspect/rm, an rm-before-journal crash and replay with no duplicate rm,
immutable-ID mismatch, exact-label mismatch, mounted-secret fail closed,
authority completion, active release, and rejection of a new transaction until
aborted DONE has been archived.

### Round 2 Files

- `deploy/swarm/deploy-sync-extension.sh`
- `deploy/swarm/worker-admission-transaction.py`
- `tests/test_worker_admission_deploy.sh`
- `tests/test_worker_admission_rollback.sh`
- `.superpowers/sdd/2026-07-26-production-worker-registration/task-4bc-deploy-stage1-report.md`

No marker-control product code, janitor product code, janitor-install recovery,
vision cutover, backend worker, Go, Dockerfile, CI, Task 4A, or frontend file
changed in this round.

### Round 2 Verification

```text
bash tests/test_worker_admission_deploy.sh
  PASS: worker admission deployment contract tests passed
bash tests/test_worker_admission_rollback.sh
  PASS: worker admission rollback transaction tests passed
bash tests/test_staging_object_janitor_run.sh
  PASS: staging object janitor launcher tests passed
bash tests/test_vp_deploy_sync_extension.sh
  PASS: exit 0, no stdout
bash tests/test_staging_object_janitor_install.sh
  PASS: staging object janitor installer tests passed
bash tests/test_worker_redis_marker_control.sh
  PASS: worker Redis marker control tests passed
python3 -m py_compile deploy/swarm/worker-admission-transaction.py
  PASS
backend/.venv/bin/ruff check deploy/swarm/worker-admission-transaction.py
  PASS: All checks passed!
backend/.venv/bin/mypy deploy/swarm/worker-admission-transaction.py
  PASS: Success: no issues found in 1 source file
```

```text
bash -n deploy/swarm/deploy-sync-extension.sh
bash -n tests/test_worker_admission_deploy.sh
bash -n tests/test_worker_admission_rollback.sh
  PASS
destructive inventory
  PASS: one new secret-rm site; it follows service-ID unused proof and an
        exact immutable-ID/name/service/generation/purpose inspect
production one-shot command-substitution inventory
  PASS: no vp_run_python_worker_container call remains inside $(...)
changed-path allowlist
  PASS: exactly the five Round 2 files listed above
generated artifact check
  PASS: no __pycache__ or .pyc remains
git diff --check
  PASS
```

### Round 2 Unfinished Boundary

This round closes only the three Stage 1 review findings. It does not wire
production forward/rollback phase advancement, service reconciliation,
cutover, marker mutation boundaries, first-deploy/failed-rollback
orchestration, or janitor install recovery. It does not claim I-3 end-to-end
closure. No SSH, push, deploy, remote access, YouTube/canary operation, or real
Swarm service/secret mutation was performed.
