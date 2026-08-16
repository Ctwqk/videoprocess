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
backend/.venv/bin/ruff check --no-cache deploy/swarm/worker-admission-transaction.py
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

## Fix Round 3

Start HEAD: `3ede76bdf590b9485bd2a3aaa9fd8a0c5cfc0f98`.

### Open Important 1: Query Output Identity

Root cause:

The query writer used a mode-`0600` pathname for Docker stdout and later
reopened that pathname for parsing. Strict JSON and request binding did not
bind the parser to the inode that received the legitimate one-shot output, so
a same-UID unlink-and-replace could substitute different canonical JSON.

RED command and output:

```text
$ bash tests/test_worker_admission_deploy.sh
FAIL: retirement query accepted replacement pathname JSON
```

Shared fix:

- Query preparation opens fixed, independently tracked read and write
  descriptors for the same mode-`0600`, regular, owner-bound inode.
- It captures the exact device/inode identity, unlinks the pathname, and
  requires link count zero before Docker can write.
- Docker stdout writes only fd 15. The parser seeks and reads only fd 14,
  revalidating exact identity, mode, owner, link count, size, UTF-8,
  canonical JSON, schema, service, and generation.
- A same-UID canonical replacement at the former pathname is never opened.
  The legitimate UUID written to the original fd is accepted.
- Success, schema failure, and canonical signal status `143` all close the
  exact read/write descriptors and clear their identity state.

GREEN command and output:

```text
$ bash tests/test_worker_admission_deploy.sh
worker admission deployment contract tests passed
```

### Open Important 2: Signal Launch Gate

Root cause:

Even after the final signal check, normal shell execution could receive a
first signal before the background Docker command started. The pending signal
was eventually returned correctly, but Docker could still enter before PID
publication and forwarding.

RED commands and outputs:

```text
$ bash tests/test_worker_admission_deploy.sh
FAIL: final-gate TERM continued into Docker

$ bash tests/test_worker_admission_deploy.sh
FAIL: launch-gate failure leaked its waiting supervisor
```

Shared fix:

- Each one-shot creates an operation-owned mode-`0600` FIFO, opens and
  identity-checks fd 16, unlinks the pathname, and generates a 128-bit release
  token.
- The published supervisor initially does nothing except wait on that exact
  descriptor. The parent publishes its PID, consumes pending signal state,
  and releases the token only while first-signal state remains zero.
- A signal before release kills and waits for the waiting supervisor, so the
  Docker callable is never entered. A normal release permits the existing
  Docker path to continue.
- Release or descriptor-verification failure also stops and reaps only the
  exact published supervisor before child identity is cleared. Gate fd and
  identity state are cleared on all tested exits.
- Existing full-handler forwarding, canonical HUP/INT/TERM statuses, outer
  phase gates, caller-trap restoration, and record/sentinel cleanup remain in
  force.

GREEN command and output:

```text
$ bash tests/test_worker_admission_deploy.sh
worker admission deployment contract tests passed
```

### Open Important 3: Fresh ABORTING Reconstruction

Root cause:

The ABORTING shell record carried only authority kind/service/generation.
Runtime revoke still built its operator path from
`VP_WORKER_CONTROL_GENERATION`, which is empty after sourcing the extension in
a fresh shell. Replay therefore produced `control//...` and retained the
active transaction.

RED command and output:

```text
$ bash tests/test_worker_admission_deploy.sh
FAIL: fresh ABORTING replay did not reconstruct authority context

$ bash tests/test_worker_admission_rollback.sh
FAIL: authority WAL accepted an alternate control image
```

Shared fix:

- Every durable authority record now carries exact kind, service, generation,
  state, control image, control generation, and operator reference.
- The strict helper validates the deterministic target-commit relationship,
  fixed control identity, known runtime service/generation shape, unique
  logical identities, and common control image.
- `list-abort` emits those journal fields, and the shell parser requires the
  exact seven-field authority schema.
- Control and runtime revoke calls consume only the persisted authority
  context. Runtime operator paths are built from the strict journal reference,
  never an empty ambient generation global.
- A real helper transaction is advanced to ABORTING, sourced in a fresh shell,
  and converges both runtime and control revocation without a double slash or
  retained `active.json`.

GREEN command and output:

```text
$ bash tests/test_worker_admission_deploy.sh
worker admission deployment contract tests passed

$ bash tests/test_worker_admission_rollback.sh
worker admission rollback transaction tests passed
```

### Open Important 4: Authority Write-Ahead Log

Root cause:

The abort authority queue was inferred from prepared secrets. Provisioning
occurs before the first secret is created and journaled, so a provisioned
control/runtime authority could have zero secret evidence, produce an empty
abort queue, and be archived without revocation.

RED commands and outputs:

```text
$ bash tests/test_worker_admission_rollback.sh
exit 1 (record-authority-intent/mark-authority CLI absent)

$ bash tests/test_worker_admission_deploy.sh
FAIL: fresh ABORTING replay did not reconstruct authority context
```

Shared fix:

- The transaction schema has a top-level, unique authority WAL independent of
  `prepared_secrets`.
- Before every control or runtime provision, production atomically records a
  deterministic `planned` intent and advances it to `provisioning`. Only a
  successful provision may be marked `provisioned`; only then may its first
  secret record be accepted.
- Retry is idempotent for an identical planned/provisioning/provisioned
  authority, while contradictory context and backward state movement fail
  closed.
- `begin-abort` copies every non-revoked authority, including planned,
  provisioning, and provisioned zero-secret records. Completion atomically
  marks the matching top-level record `revoked` and removes it from the abort
  queue.
- The common relational validator requires the abort queue to equal exactly
  the non-revoked WAL records. `DONE(aborted)` requires no prepared secret,
  operation, promotion, retirement, or unrevoked authority evidence.
- Behavior tests drive real control preparation and all four runtime services
  through successful provision followed by first-secret failure. Each has
  zero prepared secrets, still invokes exact authority revoke, retains a
  revoked DONE record, and releases active state only after completion.
- A separate post-provision mark-crash leaves a `provisioning` runtime record,
  sources a fresh shell, uses the persisted control/operator context for
  revoke, and archives only after durable completion.
- The pure helper matrix covers planned, provisioning, and provisioned states
  for control plus every runtime service; premature finish is rejected and
  the DONE archive retains all five revoked records.

GREEN commands and outputs:

```text
$ bash tests/test_worker_admission_deploy.sh
worker admission deployment contract tests passed

$ bash tests/test_worker_admission_rollback.sh
worker admission rollback transaction tests passed
```

### Round 3 Files

- `deploy/swarm/deploy-sync-extension.sh`
- `deploy/swarm/worker-admission-transaction.py`
- `tests/test_worker_admission_deploy.sh`
- `tests/test_worker_admission_rollback.sh`
- `.superpowers/sdd/2026-07-26-production-worker-registration/task-4bc-deploy-stage1-report.md`

No marker-control product code, janitor product code, janitor-install recovery,
vision cutover, backend worker, Go, Dockerfile, CI, Task 4A, or frontend file
changed in this round.

### Round 3 Verification

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
bash -n deploy/swarm/deploy-sync-extension.sh
bash -n tests/test_worker_admission_deploy.sh
bash -n tests/test_worker_admission_rollback.sh
  PASS
destructive inventory
  PASS: no new docker secret/service create, update, or rm site in this round
production one-shot command-substitution inventory
  PASS: no vp_run_python_worker_container call remains inside $(...)
descriptor inventory
  PASS: query output uses only exact fd 14/15 identity; launch release uses
        only exact unlinked FIFO fd 16 identity
changed-path allowlist
  PASS: exactly the five Round 3 files listed above
generated artifact check
  PASS: no __pycache__ or .pyc remains
git diff --check
  PASS
```

Only auto-cleaned local fake Docker/operator fixtures were used. No real
Docker/Swarm object was created, updated, or removed.

### Round 3 Unfinished Boundary

This round closes only the four Stage 1 re-review findings. It does not wire
production forward/rollback phase advancement, service reconciliation,
cutover, marker mutation boundaries, first-deploy/failed-rollback
orchestration, or janitor install recovery. It does not claim I-3 end-to-end
closure. No SSH, push, deploy, remote access, YouTube/canary operation, or real
Swarm service/secret mutation was performed.

## Fix Round 4: Final Stage 1 Important Findings

Review baseline:
`b61a9ca` (prior code baseline `1d13211`).

This round closes only the two Important findings in
`task-4bc-deploy-stage1-rereview-round3.md`. It does not enter Stage 2.

### Round 4 Important 1: Parent-Death Launch Gate and Lock

Root cause:

The release supervisor inherited the parent's read/write FIFO open file
description and transaction-lock fd 19. If the parent died before publishing
the release token, the inherited FIFO writer prevented EOF and fd 19 kept the
exact transaction lock alive. The supervisor could wait indefinitely while a
fresh process could neither acquire the lock nor reconcile the stale
one-shot operation.

Behavior RED:

```text
$ bash tests/test_worker_admission_deploy.sh
FAIL: release-entry parent death retained supervisor, lock, writer, or stale operation
```

Fix:

- The parent creates the unlinked FIFO with fd 16 as the sole read/write
  release descriptor and opens a separate, independently verified read-only
  fd 17 while the FIFO still has a pathname.
- The supervisor immediately closes inherited fd 16 and inherited transaction
  lock fd 19, clears all inherited lock ownership state, verifies fd 17 is the
  exact unlinked FIFO opened read-only, and only then waits for the token.
- A payload producer also closes fd 16, fd 17, and fd 19 plus inherited state
  before streaming. It cannot keep either the release writer or transaction
  lock alive.
- The parent closes its copy of fd 17 after spawn. Normal release writes the
  canonical token through fd 16. Parent SIGKILL or exit closes the only writer,
  so the supervisor reads EOF and exits without invoking Docker.
- Existing strict cleanup was preserved: a live operation's malformed or
  replaced sentinel still fails closed, while a fresh exact lock owner may
  reconcile stale evidence after owner death.

The behavior test sends real SIGKILL at both `release-entry` and
`verified-before-token`. The latter includes a blocked payload producer. On
Linux it audits `/proc/<pid>/fdinfo`; on macOS it uses `lsof -Faf`. Both cases
require:

- supervisor fd 17 is read-only;
- supervisor fd 16 and fd 19 are absent;
- every other direct child has no fd 16, fd 17, or fd 19;
- all children exit within the bounded wait without test-assisted killing;
- a fresh process acquires the exact lock and reconciles the stale operation
  and bind sentinel;
- Docker call count remains zero.

The EXIT cleanup trap kills residual processes only after a failed assertion;
manual child termination is not a passing path.

GREEN:

```text
$ bash tests/test_worker_admission_deploy.sh
worker admission deployment contract tests passed
```

The existing HUP/INT/TERM canonical-status, post-spawn signal, query-fd
identity, bind replacement, and lock-BASHPID cases remain green.

### Round 4 Important 2: Journal Version Compatibility

Root cause:

Round 3 added authority WAL fields while continuing to label active journals
as schema 1. That changed the exact schema 1 field set and made canonical
journals written by the `a0e1afa` helper fail as opaque parse errors. It also
left no stable, typed replay result that could prohibit unsafe inference of
missing authority evidence.

Behavior RED:

The test obtains the historical helper from the repository itself:

```text
git -C "$ROOT_DIR" archive --format=tar a0e1afa \
  deploy/swarm/worker-admission-transaction.py
```

That helper creates exact canonical PREPARING and ABORTING schema 1 journals.
The ABORTING fixture includes two prepared secrets and the historical exact
three-field abort-authority records.

```text
$ bash tests/test_worker_admission_rollback.sh
FAIL: HEAD did not type quarantine legacy PREPARING

$ bash tests/test_worker_admission_rollback.sh
unexpected transaction schema
```

The second RED occurred after adding the legacy read path but before bumping
new active writes.

Fix:

- New active transaction journals now write `schema=2`; the independent
  snapshots journal remains at its own schema 1.
- The schema 2 path retains the complete authority-WAL validator, relational
  checks, mutation matrix, abort handling, credential validation, retirement,
  operation intent, and DONE rules.
- The schema 1 field set is a fixed historical constant. A dedicated validator
  accepts only the old exact canonical document shape and its historical abort
  authority shape. It does not infer or synthesize absent authority WAL.
- Only `replay-plan` may request typed legacy recognition. Every mutation
  reader rejects schema 1, so begin/new namespace, normal phase mutation,
  stale cleanup, retirement, abort mutation, and archive remain unavailable.
- Repeated replay of the same valid legacy bytes returns canonical
  `QUARANTINE_LEGACY_SCHEMA_1` with non-secret transaction id, phase, revision,
  original-byte SHA-256 digest, and stable reason code
  `legacy_schema_1_authority_context_unavailable`.
- This is a read-only quarantine, not an atomic migration. Original
  `active.json` bytes and evidence remain unchanged.

The tests replay both old journals twice and compare byte-identical plans,
verify the digest against the original active bytes, exercise blocked
mutations under the exact lock, invoke the shell admission entry, and require
zero Docker/operator calls. The existing current-schema transaction and abort
matrices continue to run against schema 2.

GREEN:

```text
$ bash tests/test_worker_admission_rollback.sh
worker admission rollback transaction tests passed
```

### Round 4 Files

- `deploy/swarm/deploy-sync-extension.sh`
- `deploy/swarm/worker-admission-transaction.py`
- `tests/test_worker_admission_deploy.sh`
- `tests/test_worker_admission_rollback.sh`
- `.superpowers/sdd/2026-07-26-production-worker-registration/task-4bc-deploy-stage1-report.md`

No Stage 2, CI, backend, Go, Dockerfile, marker-control product, janitor
product, janitor-install recovery, Task 4A, or frontend file changed.

### Round 4 Verification

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
bash -n deploy/swarm/deploy-sync-extension.sh
bash -n tests/test_worker_admission_deploy.sh
bash -n tests/test_worker_admission_rollback.sh
  PASS
destructive Docker mutation inventory and deploy diff
  PASS: no new docker secret/service create, update, or rm site
production one-shot command-substitution inventory and deploy diff
  PASS: no new vp_run_python_worker_container call; existing calls remain
        outside command substitutions
descriptor inventory
  PASS: query output retains exact fd 14/15 identity; launch release uses
        exact unlinked FIFO fd 16 plus independently opened read-only fd 17;
        transaction locking retains exact fd 19 and BASHPID ownership
parent-death process audit
  PASS: no residual supervisor or payload producer
changed-path allowlist
  PASS: exactly the five Round 4 files listed above
generated artifact check
  PASS: no __pycache__ or .pyc remains
git diff --check
  PASS
```

Only auto-cleaned local fake Docker/operator fixtures were used. No SSH,
push, deploy, remote access, YouTube/canary operation, or real Docker/Swarm
service or secret mutation was performed.

### Round 4 Unfinished Boundary

This round closes only the final two Stage 1 review findings. It does not wire
production forward/rollback phase advancement, service reconciliation,
cutover, marker mutation boundaries, first-deploy/failed-rollback
orchestration, or janitor install recovery. It does not claim I-3 end-to-end
closure.

## Fix Round 5: Final Stage 1 Re-review Findings

Start HEAD:
`86c59acef86aee8fa1180efc2d13f8a6b52219a4`.

This round addresses only the three Important findings in
`task-4bc-deploy-stage1-rereview-round4.md`. It does not enter Stage 2.

### Round 5 Important 1: Quarantine Is the First Locked Entry Gate

Root cause:

The real `deploy_vp_app_services` entry acquired the exact transaction lock
but called `vp_validate_deploy_config` before transaction replay
classification. That validation performs one Docker network inspect and four
credential-bearing principal probes. The later preparation helper eventually
quarantined legacy schema 1, but only after those external calls.

Behavior RED:

The full-entry fixture uses four distinct regular mode-`0400` credentials and
a fake Docker implementation that returns the exact network identity and all
four expected principal identities:

```text
$ bash tests/test_worker_admission_rollback.sh
FAIL: full deploy entry touched Docker/operator before legacy PREPARING quarantine (docker=5 operator=0)
```

Fix:

- Added a lock-asserting, local-only Stage 1 entry classifier immediately
  after exact lock acquisition and before `vp_validate_deploy_config`.
- The classifier consumes only canonical `replay-plan` JSON and validates the
  complete absent, current-active, or legacy-quarantine result shape.
- Absent state and current schema-2 `PREPARING` are the only entry states that
  may continue to configuration validation.
- Canonical legacy schema-1 PREPARING/ABORTING returns the stable quarantine
  reason before network inspect, principal probes, Docker, or operator use.
- Every other current active phase returns the existing Stage 2
  reconciliation boundary before external execution.
- The exact lock-root comparison accepts lexical `/var` versus canonical
  `/private/var` only when both paths identify the same directory inode.
- A post-classification signal gate prevents a signal arriving during local
  classification from continuing into configuration or Docker.

GREEN:

```text
$ bash tests/test_worker_admission_rollback.sh
worker admission rollback transaction tests passed

$ bash tests/test_vp_deploy_sync_extension.sh
<exit 0; no stdout>
```

The legacy PREPARING and ABORTING full-entry cases now require exact canonical
quarantine stderr, unchanged active bytes, Docker count zero, and operator
count zero. A current `WORKERS_PROMOTED` full-entry case similarly requires
the exact Stage 2 boundary with zero Docker calls.

### Round 5 Important 2: Exact Integer Schema Discriminators

Root cause:

Legacy active, current active, legacy dispatch, and snapshots used Python
numeric equality for schema versions. JSON floats such as `1.0` and `2.0`
therefore compared equal to integer schema constants; the current path could
replay and mutate a forged schema-`2.0` active journal.

Behavior RED:

```text
$ bash tests/test_worker_admission_rollback.sh
FAIL: legacy active reader accepted non-integer schema 1.0
```

Fix:

- Added one `_require_exact_schema` primitive requiring
  `type(value) is int` and exact version equality.
- Legacy dispatch, the historical schema-1 validator, the current schema-2
  validator, and the independent snapshot schema-1 validator all use that
  primitive.
- Replay, every normal/abort/retirement mutation, and archive continue to use
  the same active reader, so there is no command-specific version bypass.

GREEN:

The matrix writes canonical forged `1.0`, `2.0`, `-0.0`, and `true` schema
values. It requires failure, byte-for-byte preservation, and no archive or
mutation. A valid active schema 2 and valid snapshot schema 1 are separately
required to have exact Python `int` type; tests do not rely on `!=` alone.

### Round 5 Important 3: Child Stderr Survives Inherited Lock Close

Root cause:

`vp_worker_admission_lock_drop_inherited` used the no-command Bash form
`exec 19>&- 2>/dev/null`. The fd19 close was intentional, but the fd2
redirection became permanent in the supervisor or payload-producer shell and
discarded subsequent Docker/container or producer diagnostics.

Behavior RED:

```text
$ bash tests/test_worker_admission_deploy.sh
FAIL: docker-no-payload did not preserve child fd closure, stderr, and status
```

Fix:

The close now runs in a brace group whose `/dev/null` redirection is scoped to
that group. Because the group runs in the current shell, fd19 remains closed;
when the group returns, Bash restores the caller's fd2.

GREEN:

```text
$ bash tests/test_worker_admission_deploy.sh
worker admission deployment contract tests passed
```

The behavior matrix covers Docker stderr without a payload, Docker stderr
with a payload, and payload-producer stderr. It preserves success/failure
status, proves child fd19 is closed, and retains the existing two-boundary
parent-SIGKILL audit with Docker count zero, bounded child exit, stale
reconciliation, and fresh exact-lock acquisition.

### Round 5 Files

- `deploy/swarm/deploy-sync-extension.sh`
- `deploy/swarm/worker-admission-transaction.py`
- `tests/test_worker_admission_deploy.sh`
- `tests/test_worker_admission_rollback.sh`
- `tests/test_vp_deploy_sync_extension.sh`
- `.superpowers/sdd/2026-07-26-production-worker-registration/task-4bc-deploy-stage1-report.md`

The coordinator-owned progress ledger was not modified or reverted. No CI,
backend, Go, Dockerfile, marker-control product, janitor product, Task 4A, or
frontend file changed in this round.

### Round 5 Verification

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
backend/.venv/bin/mypy --no-incremental deploy/swarm/worker-admission-transaction.py
  PASS: Success: no issues found in 1 source file
bash -n changed production/test shell files
  PASS
destructive Docker mutation and one-shot call-site inventory
  PASS: no new production mutation or one-shot execution site
generated artifact inventory
  PASS: no deploy/swarm __pycache__ or .pyc
git diff --check
  PASS
```

Only auto-cleaned local fake-Docker/fake-operator and descriptor fixtures were
used. No SSH, push, deploy, remote access, network access, YouTube/canary
operation, or real Docker/Swarm service or secret mutation was performed.

### Round 5 Unfinished Boundary

This round closes only the three Stage 1 re-review findings. It does not wire
production forward/rollback phase advancement, service reconciliation,
cutover, marker mutation boundaries, first-deploy/failed-rollback
orchestration, or janitor install recovery. It does not claim I-3 end-to-end
closure.

HEAD remains `86c59acef86aee8fa1180efc2d13f8a6b52219a4`. The managed sandbox
exposes `.git` read-only, so no files were staged or committed.

### Round 5 Breaker Closure

The final scoped review found one test-contract regression: the legacy
quarantine credential scan had been nested after an unconditional raise. The
controller restored the scan as an unconditional assertion and added an
explicit credential-bearing negative probe. Fresh verification passed all six
Stage 1 shell contracts, Python compile, Ruff, mypy, shell syntax, and
`git diff --check`.

The original reviewer independently reran the exact credential-bearing
canonical-plan reproducer. It now fails closed with the stable disclosure
error, while the rollback contract remains green. The reviewer returned
**APPROVED** and closed the Stage 1 breaker. Stage 2 wiring was not assessed by
that closure review.

## Stage 2 Post-Review Hardening

Four independently reported Stage 2 risks were reproduced and fixed with
red-green contracts:

1. Promotion precondition and marker-receipt writers no longer use a fixed
   `path.tmp` opened exclusively. They create a unique same-directory 0600
   temporary, fsync the file, replace atomically, fsync the directory, and
   clean only their own temporary on failure. A stale fixed `.tmp` is preserved
   as evidence but cannot block crash replay.
2. App snapshots use the exact four-field record
   `service|service_id|image|spec_digest`. Baseline capture and recovery
   hydration preserve those fields without rebinding by name. Rollback
   validates every record and every current service ID before any mutation;
   ordinary services and the Go worker pass the expected ID into the update
   boundary.
3. Vision cutover has an independent `final-safety` WAL entry and
   `final-safety-database` secret authority. It executes with the deploy-read
   credential and watcher Redis secret after managed vision readiness and
   immediately before legacy container retirement. A nonzero or incomplete
   final job leaves the legacy worker active.
4. Fixed-name marker cleanup resolves `ID|Spec.Name` once. Descriptor
   inspection, task terminal-state inspection, removal, and disappearance
   polling all use the resolved service ID, so a same-name replacement cannot
   be deleted by the old cleanup transaction.

Fresh evidence after these changes:

```text
bash tests/test_worker_admission_deploy.sh
  PASS
bash tests/test_worker_admission_rollback.sh
  PASS
bash tests/test_worker_redis_marker_control.sh
  PASS
bash tests/test_macos_deploy_paths.sh
  PASS
bash tests/test_vp_deploy_sync_extension.sh
  PASS: exit 0 after the full long contract
backend pytest
  PASS: 1426 passed, 125 skipped, 17 warnings
transaction helper Ruff / shell syntax / git diff --check
  PASS
frontend npm run build / npm run lint
  PASS (existing CSS and chunk-size build warnings only)
```

The required full backend Ruff and mypy commands still expose the repository
baseline: 15 Ruff findings and 61 mypy findings. Scoped checks for the modified
vision service/tests and transaction helper are clean.

The fifth unlisted canary is approved but remains unused. This hardening round
performed no push, SSH mutation, remote deployment, Docker/Swarm production
mutation, YouTube upload, or publication action. Commit/push and 150/127
deployment verification remain gated on the final independent review; 126
remains excluded from normal participation.

Read-only production probing after verification confirmed the intended
automation is live: 150 is the active Swarm leader, 127 is Ready/Active with
`vp.runtime=true`, the scoped app/feature cron runs every 15 minutes, and the
independent PDS cron runs at minute 7 plus 15. The clean deployment checkouts
match the last deployed main SHAs. The older `colima-swarmbridged` node remains
joined with only `role=app`; it lacks `vp.runtime` and therefore does not match
VideoProcess runtime placement. Direct local routing to 150 remains unavailable,
while SSH from 127 to 150 succeeds. These probes were read-only.

### Stage 2 Follow-up Corrections

The first Stage 2 final review found three additional replay/identity risks.
They were reproduced with failing contracts and corrected:

1. Full `FORWARD_APPLYING` recovery now aborts all durable vision cutover jobs
   before failed-forward capture and before changing phase to
   `ROLLBACK_PREPARING`. A crash during cleanup therefore remains replayable in
   the forward phase instead of stranding a job behind the rollback boundary.
2. Baseline-absent registered workers are removed from the hydrated durable
   `service|generation|service_id` candidate record after a process restart.
   The record must be unique and valid, the service name must still resolve to
   that ID, and both the ID and name must be absent after removal. Process-local
   worker contracts are used only when no recovery record set exists.
3. Python, vision, and publisher restore helpers accept the immutable baseline
   service ID. They validate it before node or service mutation, reject a
   create fallback when the baseline existed, and revalidate immediately before
   the registered-worker mutation wrapper, which itself targets the exact ID.

Fresh focused evidence is green for worker deployment, worker rollback, Redis
marker control, shell syntax, and `git diff --check`. The full long deploy-sync
contract and the focused independent re-review are still running. The fifth
unlisted canary remains approved and unused; no production or YouTube mutation
was performed by these corrections.

The focused independent re-review of the latest diff returned no P0, P1, P2,
or P3 findings and closed all three follow-up items. It specifically verified
that the durable baseline-absent removal path uses marker freshness plus exact
ID/name/generation labels and does not call the process-local worker contract.
The reviewer noted only layered test-granularity residual risk; the underlying
exact-ID primitives independently enforce each invariant. The restarted full
long deploy-sync contract remains in progress.

### Stage 2 Final Contract Closure

The restarted full deploy-sync contract found one additional integration edge:
hydrating durable recovery state intentionally clears process-local worker
contracts, but an injected service create can mutate Swarm and fail before its
candidate record reaches the durable journal. The immediate rollback therefore
needs a narrowly scoped identity source that fresh-process replay must never
trust.

The deployment wrapper now captures exact in-memory
`service|generation|service_id` records before hydration and passes them only
through the same-process rollback call chain. New forward transactions clear any
stale recovery candidate records first. The removal helper validates both
durable and process records, requires exact agreement when both exist, verifies
marker freshness and immutable service labels, removes by service ID, and proves
both ID and name disappear. Fresh-process recovery receives no process record
and remains durable-only and fail-closed. The full-contract rollback test double
now forwards the fifth transaction argument, and fake Docker retains the
injected partial-create generation so its exact-ID assertion matches real Swarm
labels.

Fresh final-candidate evidence:

```text
bash tests/test_vp_deploy_sync_extension.sh
  PASS: full long contract, exit 0
bash tests/test_worker_admission_deploy.sh
  PASS: worker admission deployment contract tests passed
bash tests/test_worker_admission_rollback.sh
  PASS: worker admission rollback transaction tests passed
bash tests/test_worker_redis_marker_control.sh
  PASS: worker Redis marker control tests passed
bash tests/test_macos_deploy_paths.sh
  PASS
bash tests/test_vp_unlisted_canary_scripts.sh
  PASS
changed-shell bash -n / git diff --check
  PASS
backend pytest
  PASS: 1426 passed, 125 skipped, 17 warnings
modified Python Ruff and mypy
  PASS
frontend npm install / npm run build / npm run lint
  PASS (existing CSS and chunk-size warnings only)
independent final re-review
  PASS: no P0/P1/P2/P3 findings
```

The latest required repository-wide Ruff and mypy runs still expose baseline
debt: 15 Ruff findings and 62 mypy findings, none in the modified Python files.
The fifth unlisted canary approval remains unused. No push, remote deployment,
Swarm mutation, YouTube upload, or publication mutation occurred before this
closure checkpoint.

### First Automatic Deployment Observation

The final candidate was committed as `5c106609a1c196bfe8dd02f941fa217d7033529d`
and fast-forwarded to GitHub `main`. At 17:07Z the independent PDS cron fetched
the new VideoProcess checkout and correctly left the unchanged PDS repository
and service at `6b8f8be32399...`. At 17:15Z the app/feature cron detected the
same commit but stopped before any Swarm mutation because GitHub Actions run
`31960451359` was complete with conclusion `failure`. All VP services remained
on the prior `e51240f` image set; the gate was not bypassed.

The CI evidence identified three test-contract compatibility issues:

1. The long deploy-sync test used `grep | head` under `pipefail`. GNU grep exits
   with SIGPIPE when `head` closes early, while the macOS run had passed. All
   first-line selectors now use `sed -n '1p'`, which consumes the stream. A
   Linux minimal reproducer changed from exit 141 to exit 0.
2. Three PostgreSQL migration tests still asserted revision 033 after the
   worker-registration migration advanced the deployment head to 034. They now
   compare the migrated database with the deploy CLI's
   `EXPECTED_MIGRATION_HEAD` contract.
3. Two PostgreSQL race tests patched JobEngine's session factory but not its new
   durable dispatch reconciler, which therefore attempted the default
   localhost:5435 database. They now construct the real reconciler with the
   same isolated test session factory and expose its idempotent Redis `eval`
   contract.

Fresh local evidence passes the full deploy-sync contract, changed-shell syntax,
scoped Ruff, backend pytest (`1426 passed, 125 skipped, 17 warnings`), and
`git diff --check`. The next GitHub Actions run is the required PostgreSQL 16
verification. The fifth unlisted canary remains approved and unused.
